package gkafka

import (
	"context"
	"errors"

	"github.com/Shopify/sarama"
	"github.com/go-god/broker"
)

var _ sarama.ConsumerGroupHandler = (*consumerGroupHandler)(nil)

// ErrSubHandlerInvalid sub handler invalid
var ErrSubHandlerInvalid = errors.New("subHandler is nil")

// consumerGroupHandler impl sarama.ConsumerGroupHandler
// consumer groups require Version to be >= V0_10_2_0
type consumerGroupHandler struct {
	ctx               context.Context
	topic             string
	name              string
	commitOffsetBlock bool
	logger            broker.Logger
	handler           broker.SubHandler
	keyHandlers       map[string]broker.SubHandler
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
		c.logger.Printf("kafka received topic:%v channel:%v partition:%d offset:%d key:%s -- value:%s\n",
			msg.Topic, c.name, msg.Partition, msg.Offset, msg.Key, msg.Value)

		// handler msg
		if handler, exist := c.keyHandlers[string(msg.Key)]; exist && handler != nil {
			if err := c.handlerMessage(c.ctx, handler, msg.Value); err != nil {
				c.logger.Printf("received topic:%v channel:%v handler msg err:%v\n",
					c.topic, c.name, err)
				continue
			}
		} else {
			if c.handler == nil {
				c.logger.Printf("received topic:%v channel:%v handler msg err:%v\n",
					c.topic, c.name, ErrSubHandlerInvalid)
				return ErrSubHandlerInvalid
			}

			if err := c.handlerMessage(c.ctx, c.handler, msg.Value); err != nil {
				c.logger.Printf("received topic:%v channel:%v handler msg err:%v\n",
					c.topic, c.name, err)
				continue
			}
		}

		// mark message as processed
		sess.MarkMessage(msg, "")

		// Commit the offset to the backend for kafka
		// Note: calling Commit performs a blocking synchronous operation.
		if c.commitOffsetBlock {
			sess.Commit()
		}
	}

	return nil
}

// handlerMessage consumer msg
func (c *consumerGroupHandler) handlerMessage(ctx context.Context, subHandler broker.SubHandler, msg []byte) error {
	defer broker.Recovery(c.logger)

	return subHandler(ctx, msg)
}
