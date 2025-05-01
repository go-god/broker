package gredis

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/go-god/broker"
	"github.com/go-god/broker/backoff"
)

var _ broker.Broker = (*redisImpl)(nil)

type redisImpl struct {
	client        redis.UniversalClient
	prefix        string
	logger        broker.Logger
	stop          chan struct{}
	gracefulWait  time.Duration
	noDataWaitSec int // no data to handler wait seconds
	keyHandlers   map[string]broker.SubHandler
}

// New create broker interface
func New(opts ...broker.Option) broker.Broker {
	opt := broker.Options{
		Logger:        broker.DummyLogger,
		NoDataWaitSec: 3,               // default:3
		GracefulWait:  5 * time.Second, // graceful exit time
	}
	for _, o := range opts {
		o(&opt)
	}

	if opt.RedisConf == nil {
		panic("redis config is nil")
	}

	obj := &redisImpl{
		client:        initRedisClient(opt.RedisConf),
		prefix:        opt.Prefix,
		logger:        opt.Logger,
		noDataWaitSec: opt.NoDataWaitSec,
		gracefulWait:  5 * time.Second,
		stop:          make(chan struct{}, 1),
	}

	return obj
}

// Publish pub message to topic
func (r *redisImpl) Publish(ctx context.Context, topic string, msg interface{}, opts ...broker.PubOption) error {
	// publish options
	opt := broker.PublishOptions{
		SendTimeout: 30 * time.Second,
	}

	for _, o := range opts {
		o(&opt)
	}

	listName := topic
	if r.prefix != "" {
		listName = strings.Join([]string{r.prefix, listName}, ":")
	}

	payload, err := broker.ParseMessage(msg)
	if err != nil {
		return err
	}

	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, opt.SendTimeout)
	defer cancel()
	err = r.client.LPush(ctx, listName, string(payload)).Err()
	return err
}

// Subscribe subscribe message
func (r *redisImpl) Subscribe(ctx context.Context, topic string, channel string, handler broker.SubHandler,
	opts ...broker.SubOption) error {
	opt := broker.SubscribeOptions{
		PullMsgGoroutines: 1, // default:1
		Name:              channel,
	}

	for _, o := range opts {
		o(&opt)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	r.keyHandlers = opt.KeyHandlers
	r.logger.Printf("subscribe message from redis receive topic:%v channel:%v msg...", topic, opt.Name)
	done := make(chan struct{}, opt.PullMsgGoroutines)
	for i := 0; i < opt.PullMsgGoroutines; i++ {
		go func() {
			defer func() {
				done <- struct{}{}
			}()

			if opt.SubInterval > 0 {
				ticker := time.NewTicker(opt.SubInterval)
				defer ticker.Stop()

				for {
					select {
					case <-ticker.C:
						r.handler(ctx, topic, opt.Name, handler)
					case <-r.stop:
						return
					}
				}
			} else {
				for {
					select {
					case <-r.stop:
						return
					default:
						r.handler(ctx, topic, opt.Name, handler)
					}
				}
			}
		}()
	}

	for i := 0; i < opt.PullMsgGoroutines; i++ {
		<-done
	}

	return nil
}

// Shutdown graceful shutdown broker
func (r *redisImpl) Shutdown(ctx context.Context) error {
	r.gracefulStop(ctx)
	close(r.stop)
	return nil
}

func (r *redisImpl) handler(ctx context.Context, topic string, channel string, handler broker.SubHandler) {
	defer broker.Recovery(r.logger)

	listName := topic
	if r.prefix != "" {
		listName = strings.Join([]string{r.prefix, listName}, ":")
	}

	msgBytes, err := r.client.RPop(ctx, listName).Bytes()
	if err != nil {
		r.logger.Printf("received topic:%s channel:%s handler msg err:%v", topic, channel, err)
		return
	}
	if len(msgBytes) == 0 {
		r.logger.Printf("no data received,wait data publish...")
		backoff.Sleep(r.noDataWaitSec)
		return
	}

	r.logger.Printf("received topic:%v channel:%v\n", topic, channel)
	err = handler(ctx, msgBytes)
	if err != nil {
		r.logger.Printf("received topic:%s channel:%s handler msg err:%v", topic, channel, err)
		return
	}

	// if r.keyHandlers is not nil will handler msg
	for key, fn := range r.keyHandlers {
		err = fn(ctx, msgBytes)
		if err != nil {
			r.logger.Printf("received topic:%s channel:%s key:%s handler msg err:%v", topic, channel, key, err)
		}
	}
}

func (r *redisImpl) gracefulStop(ctx context.Context) {
	defer r.logger.Printf("subscribe msg exit successfully\n")

	if ctx == nil {
		ctx = context.Background()
	}

	// Create a deadline to wait for.
	ctx, cancel := context.WithTimeout(ctx, r.gracefulWait)
	defer cancel()

	// Doesn't block if no service run, but will otherwise wait
	// until the timeout deadline.
	// Optionally, you could run it in a goroutine and block on
	// if your application should wait for other services
	// to finalize based on context cancellation.
	done := make(chan struct{}, 1)
	go func() {
		defer close(done)

		err := r.client.Close()
		if err != nil {
			r.logger.Printf("redis client close err:%v\n", err)
		}
	}()

	select {
	case <-done:
	case <-ctx.Done():
		r.logger.Printf("graceful stop timeout")
	}

	r.logger.Printf("subscribe msg shutting down")
}

func initRedisClient(conf *broker.RedisConf) redis.UniversalClient {
	if conf.ConnMaxLifetime == 0 {
		conf.ConnMaxLifetime = 1800 * time.Second
	}

	if conf.DialTimeout == 0 {
		conf.DialTimeout = 5 * time.Second
	}

	if conf.WriteTimeout == 0 {
		conf.WriteTimeout = 3 * time.Second
	}

	if conf.ReadTimeout == 0 {
		conf.ReadTimeout = 3 * time.Second
	}

	if conf.PoolTimeout == 0 {
		conf.PoolTimeout = conf.ReadTimeout + time.Second
	}

	if conf.ConnMaxIdleTime == 0 {
		conf.ConnMaxIdleTime = 30 * time.Minute
	}

	opt := &redis.Options{
		Addr:            conf.Address,
		Password:        conf.Password,
		DB:              conf.DB, // use default DB
		MaxRetries:      conf.MaxRetries,
		DialTimeout:     conf.DialTimeout,  // Default is 5 seconds
		ReadTimeout:     conf.ReadTimeout,  // Default is 3 seconds
		WriteTimeout:    conf.WriteTimeout, // Default is ReadTimeout
		PoolSize:        conf.PoolSize,
		PoolTimeout:     conf.PoolTimeout,
		MinIdleConns:    conf.MinIdleConns,
		ConnMaxIdleTime: conf.ConnMaxIdleTime,
		ConnMaxLifetime: conf.ConnMaxLifetime,
	}

	return redis.NewClient(opt)
}
