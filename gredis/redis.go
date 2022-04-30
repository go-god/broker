package gredis

import (
	"context"
	"strings"
	"time"

	"github.com/go-redis/redis"

	"github.com/go-god/broker"
	"github.com/go-god/broker/backoff"
)

type redisImpl struct {
	client        *redis.Client
	prefix        string
	logger        broker.Logger
	stop          chan struct{}
	gracefulWait  time.Duration
	noDataWaitSec int
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
		client:        redisClient(opt.RedisConf),
		prefix:        opt.Prefix,
		logger:        opt.Logger,
		noDataWaitSec: opt.NoDataWaitSec,
		gracefulWait:  5 * time.Second,
		stop:          make(chan struct{}, 1),
	}

	return obj
}

// Publish publish message to topic
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

	err = r.client.WithContext(ctx).LPush(listName, string(payload)).Err()
	return err
}

// Subscribe subscribe message
func (r *redisImpl) Subscribe(ctx context.Context, topic string, channel string, handler broker.SubHandler,
	opts ...broker.SubOption) error {
	opt := broker.SubscribeOptions{
		SubType:         broker.Exclusive, // default:Exclusive
		ConcurrencySize: 1,                // default:1
		Name:            channel,
	}

	for _, o := range opts {
		o(&opt)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	r.logger.Printf("subscribe message from redis receive topic:%v channel:%v msg...", topic, opt.Name)
	done := make(chan struct{}, opt.ConcurrencySize)
	for i := 0; i < opt.ConcurrencySize; i++ {
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
						if err := r.handler(ctx, topic, opt.Name, handler); err != nil {
							r.logger.Printf("received topic:%v channel:%v handler msg err:%v", topic, channel, err)
						}
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
						if err := r.handler(ctx, topic, opt.Name, handler); err != nil {
							r.logger.Printf("received topic:%v channel:%v handler msg err:%v", topic, channel, err)
						}
					}
				}
			}
		}()
	}

	for i := 0; i < opt.ConcurrencySize; i++ {
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

func (r *redisImpl) handler(ctx context.Context, topic string, channel string, handler broker.SubHandler) error {
	defer broker.Recovery(r.logger)

	listName := topic
	if r.prefix != "" {
		listName = strings.Join([]string{r.prefix, listName}, ":")
	}

	msgBytes, err := r.client.RPop(listName).Bytes()
	if err != nil && err != redis.Nil {
		return err
	}

	if err == redis.Nil || len(msgBytes) == 0 {
		r.logger.Printf("no data received,wait data publish...")
		backoff.Sleep(r.noDataWaitSec)
		return nil
	}

	r.logger.Printf("received topic:%v channel:%v -- content: '%s'\n", topic, channel, string(msgBytes))

	return handler(ctx, msgBytes)
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
	var err = make(chan error, 1)
	go func() {
		defer close(done)

		err <- r.client.Close()
	}()

	<-done
	<-ctx.Done()

	r.logger.Printf("subscribe msg shutting down,err:%v\n", <-err)
}

func redisClient(conf *broker.RedisConf) *redis.Client {
	if conf.MaxConnAge == 0 {
		conf.MaxConnAge = 30 * 60 * time.Second
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

	opt := &redis.Options{
		Addr:         conf.Address,
		Password:     conf.Password,
		DB:           conf.DB, // use default DB
		MaxRetries:   conf.MaxRetries,
		DialTimeout:  conf.DialTimeout,  // Default is 5 seconds
		ReadTimeout:  conf.ReadTimeout,  // Default is 3 seconds
		WriteTimeout: conf.WriteTimeout, // Default is ReadTimeout
		PoolSize:     conf.PoolSize,
		PoolTimeout:  conf.PoolTimeout,
		MinIdleConns: conf.MinIdleConns,
		IdleTimeout:  conf.IdleTimeout,
		MaxConnAge:   conf.MaxConnAge,
	}

	return redis.NewClient(opt)
}
