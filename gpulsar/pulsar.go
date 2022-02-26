package gpulsar

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/go-god/broker"
)

type pulsarImpl struct {
	client       pulsar.Client
	logger       broker.Logger
	stop         chan struct{}
	gracefulWait time.Duration
}

var _ broker.Broker = (*pulsarImpl)(nil)

// Publish publish message to topic
func (p *pulsarImpl) Publish(ctx context.Context, topic string, msg interface{},
	opts ...broker.PubOption) error {
	// publish options
	opt := broker.PublishOptions{
		SendTimeout: 30 * time.Second,
	}
	for _, o := range opts {
		o(&opt)
	}

	producerOpt := pulsar.ProducerOptions{
		Topic:                   topic,
		SendTimeout:             opt.SendTimeout,
		BatchingMaxPublishDelay: opt.PublishDelay,
	}
	if opt.Name != "" {
		producerOpt.Name = opt.Name
	}

	// create producer
	var (
		producer pulsar.Producer
		err      error
	)
	producer, err = p.client.CreateProducer(producerOpt)
	if err != nil {
		return err
	}

	defer producer.Close()

	// parse message
	var payload []byte
	payload, err = p.parseMessage(msg)
	if err != nil {
		return err
	}

	var msgID pulsar.MessageID
	msgID, err = producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: payload,
	})
	if err != nil {
		return err
	}

	p.logger.Printf("message id:%v partitionIdx:%v", msgID.EntryID(), msgID.PartitionIdx())
	return nil
}

func (p *pulsarImpl) parseMessage(msg interface{}) ([]byte, error) {
	if s, ok := msg.(string); ok {
		return []byte(s), nil
	}

	return json.Marshal(msg)
}

// Subscribe subscribe message
func (p *pulsarImpl) Subscribe(ctx context.Context, topic string, channel string,
	handler broker.SubHandler, opts ...broker.SubOption) error {
	opt := broker.SubscribeOptions{
		SubType:            broker.Shared, // default Shared
		ConcurrencySize:    1,
		MessageChannelSize: 100,
		GracefulWait:       5 * time.Second,
	}

	for _, o := range opts {
		o(&opt)
	}

	p.gracefulWait = opt.GracefulWait
	if channel != "" {
		opt.Name = channel
	}

	consumer, err := p.client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: opt.Name,
		Type:             pulsar.SubscriptionType(opt.SubType),
		RetryEnable:      opt.RetryEnable,
	})

	var msgChannel chan pulsar.ConsumerMessage
	isMsgChannel := opt.MessageChannel && opt.MessageChannelSize > 0
	if isMsgChannel {
		msgChannel = make(chan pulsar.ConsumerMessage, opt.MessageChannelSize)
	}

	if err != nil {
		return err
	}

	defer consumer.Close()

	if ctx == nil {
		ctx = context.Background()
	}

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
						err := p.handler(ctx, topic, opt.Name, consumer, handler)
						p.logger.Printf("received topic:%v channel:%v handler msg err:%v", topic, channel, err)
					case <-p.stop:
						return
					default:
					}
				}
			} else {
				// Receive messages from channel. The channel returns a struct which contains message
				// and the consumer from where the message was received.
				// It's not necessary here since we have 1 single consumer, but the channel could be
				// shared across multiple consumers as well
				if isMsgChannel {
					for {
						select {
						case <-p.stop:
							return
						case cm := <-msgChannel:
							msg := cm.Message
							if err := p.consumerMsg(ctx, topic, opt.Name, msg, handler); err != nil {
								p.logger.Printf("received topic:%v channel:%v handler msg err:%v", topic, channel, err)
								continue
							}
							consumer.Ack(msg)
						}
					}
				} else {
					for {
						select {
						case <-p.stop:
							return
						default:
							err := p.handler(ctx, topic, opt.Name, consumer, handler)
							p.logger.Printf("received topic:%v channel:%v handler msg err:%v", topic, channel, err)
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

func (p *pulsarImpl) handler(ctx context.Context, topic string, channel string,
	consumer pulsar.Consumer, handler broker.SubHandler) error {
	msg, err := consumer.Receive(ctx)
	if err != nil {
		return err
	}

	err = p.consumerMsg(ctx, topic, channel, msg, handler)
	if err != nil {
		return err
	}

	// send ack
	consumer.Ack(msg)

	return nil
}

func (p *pulsarImpl) consumerMsg(ctx context.Context, topic string, channel string,
	msg pulsar.Message, handler broker.SubHandler) error {
	msgBytes := msg.Payload()
	p.logger.Printf("received topic:%v channel:%v message msgId: %#v -- content: '%s'\n", topic, channel,
		msg.ID(), string(msgBytes))
	if err := handler(ctx, msgBytes); err != nil {
		return err
	}

	return nil
}

// gracefulStop stop subscribe msg
func (p *pulsarImpl) gracefulStop(ctx context.Context) {
	defer p.logger.Printf("subscribe msg exit successfully\n")

	if ctx == nil {
		ctx = context.Background()
	}

	// Create a deadline to wait for.
	ctx, cancel := context.WithTimeout(ctx, p.gracefulWait)
	defer cancel()

	// Doesn't block if no service run, but will otherwise wait
	// until the timeout deadline.
	// Optionally, you could run it in a goroutine and block on
	// if your application should wait for other services
	// to finalize based on context cancellation.
	done := make(chan struct{}, 1)
	go func() {
		defer close(done)

		p.client.Close()
	}()

	<-done
	<-ctx.Done()

	p.logger.Printf("subscribe msg shutting down")
}

// Shutdown graceful shutdown broker
func (p *pulsarImpl) Shutdown(ctx context.Context) error {
	p.gracefulStop(ctx)
	close(p.stop)
	return nil
}

// New create broker interface
func New(opts ...broker.Option) broker.Broker {
	opt := broker.Options{
		OperationTimeout:        30 * time.Second,
		ConnectionTimeout:       30 * time.Second,
		MaxConnectionsPerBroker: 1,
		Logger:                  broker.DummyLogger,
	}
	for _, o := range opts {
		o(&opt)
	}

	p := &pulsarImpl{logger: opt.Logger}
	if len(opt.Addrs) == 0 {
		panic("pulsar address is empty")
	}

	clientOpt := pulsar.ClientOptions{
		URL:                     strings.Join(opt.Addrs, ","),
		OperationTimeout:        opt.OperationTimeout,
		ConnectionTimeout:       opt.ConnectionTimeout,
		MaxConnectionsPerBroker: opt.MaxConnectionsPerBroker,
	}
	if opt.ListenerName != "" {
		clientOpt.ListenerName = opt.ListenerName
	}

	// create pulsar client
	client, err := pulsar.NewClient(clientOpt)
	if err != nil {
		panic("could not connection pulsar client:" + err.Error())
	}

	p.client = client

	return p
}
