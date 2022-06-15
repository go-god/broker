package gkafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Shopify/sarama"

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
		OperationTimeout:        10 * time.Second,
		ConnectionTimeout:       10 * time.Second,
		MaxConnectionsPerBroker: 1,
		Logger:                  broker.DummyLogger,
		GracefulWait:            5 * time.Second, // graceful exit time
	}
	for _, o := range opts {
		o(&opt)
	}

	if len(opt.Addrs) == 0 {
		panic("kafka address is empty")
	}

	k := &kafkaImpl{
		logger:       opt.Logger,
		stop:         make(chan struct{}, 1),
		gracefulWait: opt.GracefulWait,
	}

	// kafka sarama config
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Timeout = opt.OperationTimeout

	// consumer config
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	if opt.User != "" { // user/pwd auth
		config.Net.SASL.Enable = true
		config.Net.SASL.User = opt.User
		config.Net.SASL.Password = opt.Password
	}

	// create kafka client
	var err error
	k.client, err = sarama.NewClient(opt.Addrs, config)
	if err != nil {
		panic("could not connection kafka client:" + err.Error())
	}

	return k
}

// Publish publish message to topic
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
		Topic: topic, Key: sarama.StringEncoder(opt.Name), Value: sarama.ByteEncoder(payload),
	}

	// create producer
	var producer sarama.SyncProducer
	producer, err = sarama.NewSyncProducerFromClient(k.client)
	if err != nil {
		k.logger.Printf("NewSyncProducerFromClient err:%v\n", err)
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

// Subscribe subscribe message from topic + channel
func (k *kafkaImpl) Subscribe(ctx context.Context, topic string, groupID string, handler broker.SubHandler,
	opts ...broker.SubOption) error {
	opt := broker.SubscribeOptions{
		SubType:         broker.Shared, // default:Shared
		ConcurrencySize: 1,             // default:1
		Name:            groupID,       // group_id
	}

	for _, o := range opts {
		o(&opt)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	k.logger.Printf("subscribe message from kafka receive topic:%v channel:%v msg...\n", topic, opt.Name)

	consumerGroup, err := sarama.NewConsumerGroupFromClient(opt.Name, k.client)
	if err != nil {
		panic(fmt.Errorf("new kafka consumer name:%s err:%s", opt.Name, err.Error()))
	}

	defer func() {
		_ = consumerGroup.Close()
	}()

	done := make(chan struct{}, opt.ConcurrencySize)
	topics := []string{topic}
	for i := 0; i < opt.ConcurrencySize; i++ {
		go func() {
			defer broker.Recovery(k.logger)
			defer func() {
				done <- struct{}{}
			}()

			consumerHandler := &consumerGroupHandler{
				ctx:     ctx,
				topic:   topic,
				name:    opt.Name,
				key:     opt.MessageKey, // message key
				logger:  k.logger,
				handler: handler,
			}

			for {
				select {
				case <-k.stop:
					return
				case err := <-consumerGroup.Errors():
					k.logger.Printf("kafka received topic:%v channel:%v handler msg err:%v\n",
						topic, opt.Name, err)
					backoff.Sleep(1)
				default:
					// Consume() should be called continuously in an infinite loop
					// Because Consume() needs to be executed again after each rebalance to restore the connection
					// The Join Group request is not initiated until the Consume starts. If the current consumer
					// becomes the leader of the consumer group after joining, the rebalance process will also be
					// performed to re-allocate
					// The topics and partitions that each consumer group in the group needs to consume,
					// and the consumption starts after the last Sync Group
					err := consumerGroup.Consume(ctx, topics, consumerHandler)
					if err != nil {
						k.logger.Printf("received topic:%v channel:%v handler msg err:%v\n",
							topic, opt.Name, err)
						continue
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
	var err = make(chan error, 1)
	go func() {
		defer close(done)

		err <- k.client.Close()
	}()

	<-done
	<-ctx.Done()

	k.logger.Printf("subscribe msg shutting down,err:%v\n", <-err)
}
