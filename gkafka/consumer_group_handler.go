package gkafka

import (
	"context"

	"github.com/Shopify/sarama"

	"github.com/go-god/broker"
)

var _ sarama.ConsumerGroupHandler = (*consumerGroupHandler)(nil)

// consumerGroupHandler impl sarama.ConsumerGroupHandler
// consumer groups require Version to be >= V0_10_2_0
type consumerGroupHandler struct {
	ctx     context.Context
	topic   string
	name    string
	logger  broker.Logger
	handler broker.SubHandler
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (c *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
// but before the offsets are committed for the very last time.
func (c *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (c *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {
	defer broker.Recovery(c.logger)

	for msg := range claim.Messages() {
		c.logger.Printf("kafka received topic:%v channel:%v partition:%d offset:%d key:%s -- value:`%s`\n",
			msg.Topic, c.name, msg.Partition, msg.Offset, msg.Key, msg.Value)

		// handler msg
		if err := c.handlerMsg(c.ctx, msg.Value); err != nil {
			c.logger.Printf("received topic:%v channel:%v handler msg err:%v\n",
				c.topic, c.name, err)
			continue
		}

		// mark message as processed
		sess.MarkMessage(msg, "")
		sess.Commit()
	}

	return nil
}

// handlerMsg handler msg
func (c *consumerGroupHandler) handlerMsg(ctx context.Context, msg []byte) error {
	defer broker.Recovery(c.logger)
	return c.handler(ctx, msg)
}
