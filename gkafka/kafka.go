package gkafka

import (
	"context"
	"errors"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"

	"github.com/go-god/broker"
	"github.com/go-god/broker/backoff"
)

type kafkaImpl struct {
	client       sarama.Client
	config       *sarama.Config
	addrs        []string
	logger       broker.Logger
	stop         chan struct{}
	gracefulWait time.Duration
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

	var payload []byte
	payload, err := broker.ParseMessage(msg)
	if err != nil {
		return err
	}

	// send msg
	message := &sarama.ProducerMessage{
		Topic: topic, Key: sarama.StringEncoder(opt.Name), Value: sarama.ByteEncoder(payload),
	}
	var (
		partition int32
		offset    int64
	)

	var producer sarama.SyncProducer
	producer, err = sarama.NewSyncProducerFromClient(k.client) // default sync producer
	if err != nil {
		k.logger.Printf("NewSyncProducerFromClient err:%v\n", err)
	}
	defer func() {
		_ = producer.Close()
	}()

	partition, offset, err = producer.SendMessage(message)
	if err != nil {
		return err
	}

	k.logger.Printf("kafka producer partitionID: %d; offset:%d, value: %s\n", partition, offset, string(payload))

	return nil
}

// Subscribe subscribe message from topic + channel
func (k *kafkaImpl) Subscribe(ctx context.Context, topic string, channel string, handler broker.SubHandler,
	opts ...broker.SubOption) error {
	opt := broker.SubscribeOptions{
		SubType:         broker.Shared, // default:Shared
		ConcurrencySize: 1,             // default:1
		Name:            channel,
		Offset:          sarama.OffsetNewest,
	}

	for _, o := range opts {
		o(&opt)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	k.logger.Printf("subscribe message from kafka receive topic:%v channel:%v msg...\n", topic, opt.Name)

	// init (custom) config, set mode to ConsumerModePartitions
	c := cluster.NewConfig()
	c.Group.Mode = cluster.ConsumerModePartitions
	c.Config = *k.config
	c.Consumer.Offsets.Initial = opt.Offset
	c.Consumer.Offsets.AutoCommit.Enable = true
	c.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	c.Consumer.Offsets.CommitInterval = 1 * time.Second

	topics := []string{topic}
	consumer, err := cluster.NewConsumer(k.addrs, opt.Name, topics, c) // init consumer
	if err != nil {
		return err
	}

	defer func() {
		_ = consumer.Close()
	}()

	k.watchNotifications(consumer) // consume notifications

	done := make(chan struct{}, opt.ConcurrencySize)
	for i := 0; i < opt.ConcurrencySize; i++ {
		go func() {
			defer broker.Recovery(k.logger)
			defer func() {
				done <- struct{}{}
			}()

			// consume partitions
			for {
				select {
				case part, ok := <-consumer.Partitions():
					if !ok {
						return
					}

					// start a separate goroutine to consume messages
					go func(pc cluster.PartitionConsumer) {
						defer broker.Recovery(k.logger)
						for msg := range pc.Messages() {
							k.logger.Printf(
								"kafka received topic:%v channel:%v partition:%d offset:%d key:%s -- value:%s\n",
								msg.Topic, opt.Name, msg.Partition, msg.Offset, msg.Key, msg.Value)
							if err := handler(ctx, msg.Value); err != nil {
								k.logger.Printf("received topic:%v channel:%v handler msg err:%v\n",
									topic, opt.Name, err)
								continue
							}

							consumer.MarkOffset(msg, "") // mark message as processed
						}
					}(part)
				case err := <-consumer.Errors():
					k.logger.Printf("kafka received topic:%v channel:%v handler msg err:%v\n",
						topic, opt.Name, err)
					backoff.Sleep(1)
				case <-k.stop:
					return
				}
			}
		}()
	}

	for i := 0; i < opt.ConcurrencySize; i++ {
		<-done
	}

	return nil
}

// consume notifications
func (k *kafkaImpl) watchNotifications(consumer *cluster.Consumer) {
	go func() {
		for ntf := range consumer.Notifications() {
			k.logger.Printf("kafka rebalanced: %+v\n", ntf)
		}
	}()
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

// Shutdown graceful shutdown broker
func (k *kafkaImpl) Shutdown(ctx context.Context) error {
	k.gracefulStop(ctx)
	close(k.stop)
	return nil
}

// New create kafka broker
func New(opts ...broker.Option) broker.Broker {
	opt := broker.Options{
		OperationTimeout:        10 * time.Second,
		ConnectionTimeout:       10 * time.Second,
		MaxConnectionsPerBroker: 1,
		Logger:                  broker.DummyLogger,
		GracefulWait:            5 * time.Second,
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
	if opt.User != "" { // user/pwd auth
		config.Net.SASL.Enable = true
		config.Net.SASL.User = opt.User
		config.Net.SASL.Password = opt.Password
	}
	k.config = config

	// create kafka client
	var err error
	k.client, err = sarama.NewClient(opt.Addrs, k.config)
	if err != nil {
		panic("could not connection kafka client:" + err.Error())
	}

	k.addrs = opt.Addrs

	return k
}
