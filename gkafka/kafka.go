package gkafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"

	"github.com/go-god/broker"
	"github.com/go-god/broker/backoff"
)

type kafkaImpl struct {
	client       sarama.Client
	logger       broker.Logger
	stop         chan struct{}
	gracefulWait time.Duration
}

var _ broker.Broker = (*kafkaImpl)(nil)

// New create kafka broker
func New(opts ...broker.Option) broker.Broker {
	opt := broker.Options{
		OperationTimeout:  10 * time.Second,
		ConnectionTimeout: 10 * time.Second,
		Logger:            broker.DummyLogger,
		GracefulWait:      5 * time.Second, // graceful exit time
	}

	for _, o := range opts {
		o(&opt)
	}

	if opt.ConsumerAutoCommitInterval == 0 {
		opt.ConsumerAutoCommitInterval = 1 * time.Second
	}

	if len(opt.Address) == 0 {
		panic("kafka address is empty")
	}

	k := &kafkaImpl{
		logger:       opt.Logger,
		stop:         make(chan struct{}, 1),
		gracefulWait: opt.GracefulWait,
	}

	// kafka sarama config
	config := sarama.NewConfig()
	config.Net.DialTimeout = opt.ConnectionTimeout
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Timeout = opt.OperationTimeout

	// consumer config
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = opt.ConsumerAutoCommitInterval
	if opt.User != "" { // user/pwd auth
		config.Net.SASL.Enable = true
		config.Net.SASL.User = opt.User
		config.Net.SASL.Password = opt.Password
	}

	// create kafka client
	var err error
	k.client, err = sarama.NewClient(opt.Address, config)
	if err != nil {
		panic("failed to new kafka client: " + err.Error())
	}

	return k
}

// Publish pub message to topic
func (k *kafkaImpl) Publish(_ context.Context, topic string, msg interface{}, opts ...broker.PubOption) error {
	select {
	case <-k.stop:
		return errors.New("broker has stopped")
	default:
	}

	// publish options
	opt := broker.PublishOptions{}
	for _, o := range opts {
		o(&opt)
	}

	// kafka message
	payload, err := broker.ParseMessage(msg)
	if err != nil {
		return err
	}
	message := &sarama.ProducerMessage{
		Topic: topic, Value: sarama.ByteEncoder(payload),
	}

	if opt.Name != "" {
		// The partitioning key for this message. Pre-existing Encoders include
		// StringEncoder and ByteEncoder.
		message.Key = sarama.StringEncoder(opt.Name)
	}

	// create producer
	var producer sarama.SyncProducer
	producer, err = sarama.NewSyncProducerFromClient(k.client)
	if err != nil {
		k.logger.Printf("new kafka producer err:%v\n", err)
		return err
	}

	defer func() {
		_ = producer.Close()
	}()

	// send message
	var (
		partition int32
		offset    int64
	)
	partition, offset, err = producer.SendMessage(message)
	if err != nil {
		return err
	}

	k.logger.Printf("kafka producer partitionID: %d; offset:%d, value: %s\n", partition, offset, string(payload))

	return nil
}

// Subscribe Sub message from topic + channel
func (k *kafkaImpl) Subscribe(ctx context.Context, topic string, groupID string, handler broker.SubHandler,
	opts ...broker.SubOption) error {
	opt := broker.SubscribeOptions{
		Name:              groupID, // group_id
		Topics:            []string{topic},
		PullMsgGoroutines: 1, // pull msg from broker goroutines
	}

	for _, o := range opts {
		o(&opt)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	k.logger.Printf("start subscribe message from kafka receive topics:%v group_id:%v msg...\n", opt.Topics, opt.Name)
	consumerGroup, err := sarama.NewConsumerGroupFromClient(opt.Name, k.client)
	if err != nil {
		return fmt.Errorf("new kafka consume client for topics:%v group_id:%v err:%v", opt.Topics, opt.Name, err)
	}

	defer func() {
		_ = consumerGroup.Close()
	}()

	done := make(chan struct{}, 1)
	go func() {
		defer broker.Recovery(k.logger)
		defer func() {
			done <- struct{}{}
		}()

		c := &consumerGroupHandler{
			ctx:                  ctx,
			topics:               opt.Topics,
			groupID:              opt.Name,
			commitOffsetBlock:    opt.CommitOffsetBlock,
			logger:               k.logger,
			handler:              handler,
			keyHandlers:          opt.KeyHandlers,
			pullMsgGoroutines:    opt.PullMsgGoroutines,
			enableBuffer:         opt.EnableBuffer,
			bufferSize:           opt.BufferSize,
			consumeMsgGoroutines: opt.ConsumeMsgGoroutines,
			stop:                 k.stop,
		}

		if c.enableBuffer {
			if c.bufferSize == 0 {
				c.bufferSize = 1024
			}
			if c.consumeMsgGoroutines == 0 {
				c.consumeMsgGoroutines = 1
			}

			c.msgBuffer = make(chan *sarama.ConsumerMessage, c.consumeMsgGoroutines)
		}

		for {
			select {
			case <-k.stop:
				return
			case consumeErr := <-consumerGroup.Errors():
				k.logger.Printf("kafka received topics:%v group_id:%v handler msg err:%v\n",
					opt.Topics, opt.Name, consumeErr)
				backoff.Sleep(1)
			default:
				// Consume() should be called continuously in an infinite loop
				// Because Consume() needs to be executed again after each rebalance to restore the connection
				// The Join Group request is not initiated until the Consume starts. If the current consumer
				// becomes the leader of the consumer group after joining, the rebalance process will also be
				// performed to re-allocate
				// The topics and partitions that each consumer group in the group needs to consume,
				// and the consumption starts after the last Sync Group
				consumeErr := consumerGroup.Consume(ctx, opt.Topics, c)
				if consumeErr != nil {
					k.logger.Printf("received topics:%v group_id:%v handler msg err:%v\n",
						opt.Topics, opt.Name, consumeErr)
					continue
				}
			}
		}
	}()

	<-done

	return nil
}

// Shutdown graceful shutdown broker
func (k *kafkaImpl) Shutdown(ctx context.Context) error {
	k.gracefulStop(ctx)
	close(k.stop)
	return nil
}

func (k *kafkaImpl) gracefulStop(ctx context.Context) {
	defer k.logger.Printf("subscribe msg exit successfully\n")

	if ctx == nil {
		ctx = context.Background()
	}

	// Create a deadline to wait for.
	ctx, cancel := context.WithTimeout(ctx, k.gracefulWait)
	defer cancel()

	// Doesn't block if no service run, but will otherwise wait
	// until the timeout deadline.
	// Optionally, you could run it in a goroutine and block on
	// if your application should wait for other services
	// to finalize based on context cancellation.
	done := make(chan struct{}, 1)
	go func() {
		defer close(done)
		err := k.client.Close()
		if err != nil {
			k.logger.Printf("kafka client close err:%v\n", err)
		}
	}()

	select {
	case <-done:
	case <-ctx.Done():
		k.logger.Printf("graceful shutdown timeout")
	}

	k.logger.Printf("subscribe msg shutting down")
}
