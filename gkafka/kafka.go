package gkafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
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
		GracefulWait:      5 * time.Second,                // graceful exit time
		Protocol:          "PLAINTEXT",                    // kafka protocol
		SASLMechanism:     "PLAIN",                        // kafka sasl.mechanism
		CompressionLevel:  sarama.CompressionLevelDefault, // kafka compression level
		Compression:       0,                              // no compression
	}

	for _, o := range opts {
		o(&opt)
	}

	opt.Protocol = strings.ToUpper(strings.TrimSpace(opt.Protocol))
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
	config.Producer.CompressionLevel = opt.CompressionLevel
	config.Producer.Compression = sarama.CompressionCodec(opt.Compression)

	// consumer config
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = opt.ConsumerAutoCommitInterval

	// configure kafka protocol: PLAINTEXT, SASL_PLAINTEXT, SASL_SSL
	if opt.Protocol != "PLAINTEXT" {
		if err := configureKafkaSecurity(config, opt); err != nil {
			log.Fatalln("failed to configure kafka security: " + err.Error())
		}
	}

	// create kafka client
	var err error
	k.client, err = sarama.NewClient(opt.Address, config)
	if err != nil {
		log.Fatalln("failed to new kafka client: " + err.Error())
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

	// kafka producer message headers
	if hLen := len(opt.Headers); hLen > 0 {
		message.Headers = make([]sarama.RecordHeader, 0, hLen)
		for key := range opt.Headers {
			message.Headers = append(message.Headers, sarama.RecordHeader{
				Key:   opt.Headers[key].Key,
				Value: opt.Headers[key].Value,
			})
		}
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

	c := &consumerGroupHandler{
		ctx:                        ctx,
		topics:                     opt.Topics,
		groupID:                    opt.Name,
		commitOffsetBlock:          opt.CommitOffsetBlock,
		logger:                     k.logger,
		subHandler:                 handler,
		keyHandlers:                opt.KeyHandlers,
		subMessageHandler:          opt.SubMessageHandler,
		pullMsgGoroutines:          opt.PullMsgGoroutines,
		enableMsgBuffer:            opt.EnableMsgBuffer,
		msgBufferSize:              opt.MsgBufferSize,
		consumeMsgBufferGoroutines: opt.ConsumeMsgBufferGoroutines,
		stop:                       k.stop,
	}

	if c.enableMsgBuffer {
		if c.msgBufferSize == 0 {
			c.msgBufferSize = 1024
		}
		if c.consumeMsgBufferGoroutines == 0 {
			c.consumeMsgBufferGoroutines = 1
		}

		c.msgBuffer = make(chan *sarama.ConsumerMessage, c.msgBufferSize)
	}

	done := make(chan struct{}, 1)
	go func() {
		defer broker.Recovery(k.logger)
		defer func() {
			done <- struct{}{}
		}()

		for {
			select {
			case <-ctx.Done():
				k.logger.Printf("ctx canceled,err:%v\n", ctx.Err())
				return
			case <-k.stop:
				k.logger.Printf("kafka subscribe has stopped\n")
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
		k.logger.Printf("subscribe msg shutting down\n")
	case <-ctx.Done():
		k.logger.Printf("graceful shutdown timeout\n")
	}
}

// configureKafkaSecurity configures SASL and TLS based on Options.
func configureKafkaSecurity(config *sarama.Config, opt broker.Options) error {
	protocol := strings.ToUpper(strings.TrimSpace(opt.Protocol))
	switch protocol {
	case "SASL_PLAINTEXT":
		return configureSASL(config, opt)
	case "SASL_SSL":
		// 设置tls和证书（可选）
		if err := configureTLS(config, opt); err != nil {
			return err
		}

		// 设置 SASL authentication 认证
		return configureSASL(config, opt)
	default:
		return fmt.Errorf("unsupported kafka protocol: %s", opt.Protocol)
	}
}

// configureSASL configures SASL authentication.
func configureSASL(config *sarama.Config, opt broker.Options) error {
	if opt.User == "" {
		return errors.New("kafka SASL user is empty")
	}

	config.Net.SASL.Enable = true
	config.Net.SASL.User = opt.User
	config.Net.SASL.Password = opt.Password
	config.Net.SASL.Handshake = true

	mechanism := strings.ToUpper(strings.TrimSpace(opt.SASLMechanism))
	if mechanism == "" {
		mechanism = sarama.SASLTypePlaintext
	}

	switch mechanism {
	case sarama.SASLTypePlaintext:
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	case sarama.SASLTypeSCRAMSHA256, sarama.SASLTypeSCRAMSHA512:
		config.Net.SASL.Mechanism = sarama.SASLMechanism(mechanism)
		generator, err := newSCRAMClientGenerator(mechanism)
		if err != nil {
			return err
		}

		config.Net.SASL.SCRAMClientGeneratorFunc = generator
	default:
		return fmt.Errorf("unsupported kafka sasl mechanism: %s", opt.SASLMechanism)
	}

	return nil
}

// configureTLS configures TLS for kafka connection.
func configureTLS(config *sarama.Config, opt broker.Options) error {
	config.Net.TLS.Enable = true
	tlsConfig := &tls.Config{
		InsecureSkipVerify: opt.InsecureSkipVerify,
	}

	// 证书不为空，就读取
	if opt.CertPath != "" {
		caCert, err := os.ReadFile(opt.CertPath)
		if err != nil {
			return fmt.Errorf("failed to read kafka cert file:%s err:%w", opt.CertPath, err)
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caCert) {
			return fmt.Errorf("failed to append kafka cert from %s", opt.CertPath)
		}

		tlsConfig.RootCAs = certPool
	}

	config.Net.TLS.Config = tlsConfig
	return nil
}
