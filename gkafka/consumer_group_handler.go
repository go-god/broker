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
	ctx           context.Context
	topic         string
	name          string
	key           string // message key
	logger        broker.Logger
	handler       broker.SubHandler
	remainHandler broker.SubHandler
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

	// note: the message key of kafka may be nil,if c.key is not empty,it must be eq msg.key
	for msg := range claim.Messages() {
		if c.key != "" && c.key != string(msg.Key) {
			c.logger.Printf("kafka sub message key:%s invalid,but msg key is empty", c.key)
			if c.remainHandler != nil {
				c.logger.Printf("kafka sub message will handler use remainHandler")
				c.logger.Printf("kafka received topic:%v channel:%v partition:%d offset:%d key:%s -- value:%s\n",
					msg.Topic, c.name, msg.Partition, msg.Offset, msg.Key, msg.Value)

				// handler msg
				if err := c.handlerMessage(c.ctx, c.remainHandler, msg.Value); err != nil {
					c.logger.Printf("received topic:%v channel:%v handler msg err:%v\n",
						c.topic, c.name, err)
					continue
				}

				// mark message as processed
				sess.MarkMessage(msg, "")
				sess.Commit()
			}

			continue
		}

		c.logger.Printf("kafka received topic:%v channel:%v partition:%d offset:%d key:%s -- value:%s\n",
			msg.Topic, c.name, msg.Partition, msg.Offset, msg.Key, msg.Value)

		// handler msg
		if err := c.handlerMessage(c.ctx, c.handler, msg.Value); err != nil {
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

// handlerMessage consumer msg
func (c *consumerGroupHandler) handlerMessage(ctx context.Context, subHandler broker.SubHandler, msg []byte) error {
	defer broker.Recovery(c.logger)

	return subHandler(ctx, msg)
}
