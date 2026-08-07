package gkafka

import (
	"context"
	"errors"

	"github.com/IBM/sarama"

	"github.com/go-god/broker"
)

var _ sarama.ConsumerGroupHandler = (*consumerGroupHandler)(nil)

// ErrSubHandlerInvalid sub handler invalid
var ErrSubHandlerInvalid = errors.New("subHandler is nil")

// consumerGroupHandler impl sarama.ConsumerGroupHandler
// consumer groups require Version to be >= V0_10_2_0
type consumerGroupHandler struct {
	ctx               context.Context
	topics            []string
	groupID           string
	commitOffsetBlock bool
	logger            broker.Logger

	// subscribe msg handler with value
	subHandler broker.SubHandler

	// subscribe msg handler with full record
	subMessageHandler broker.SubMessageHandler

	// subscribe msg handler with key
	keyHandlers       map[string]broker.SubHandler
	pullMsgGoroutines int

	// for kafka whether put the message in the buffer pool first when consuming it
	enableMsgBuffer bool
	// msg buffer size
	msgBufferSize int
	// goroutines for consume msg from buffer
	consumeMsgBufferGoroutines int
	// msg buffer channel
	msgBuffer chan *sarama.ConsumerMessage

	stop chan struct{}
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

	if c.enableMsgBuffer {
		c.consumeMsgFromBuffer(c.ctx, sess)
	}

	done := make(chan struct{}, c.pullMsgGoroutines)
	for i := 0; i < c.pullMsgGoroutines; i++ {
		c.logger.Printf("pull msg from topics:%v group_id:%v current goroutine index:%d", c.topics, c.groupID, i)
		go func() {
			defer func() {
				broker.Recovery(c.logger)
				done <- struct{}{}
			}()

			// note: the message key of kafka may be nil,if c.key is not empty,it must be eq msg.key
			for msg := range claim.Messages() {
				select {
				case <-c.stop:
					c.logger.Printf("topics:%v group_id:%v consume msg has stopped", c.topics, c.groupID)
					return
				default:
				}

				c.logger.Printf("kafka received topic:%v group_id:%v partition:%d offset:%d key:%s\n",
					msg.Topic, c.groupID, msg.Partition, msg.Offset, msg.Key)
				if c.enableMsgBuffer {
					c.msgBuffer <- msg
					continue
				}

				// handler msg
				if err := c.consume(c.ctx, msg, sess); err != nil {
					c.logger.Printf("kafka consume topic:%v group_id:%v err:%v\n", msg.Topic, c.groupID, err)
				}
			}
		}()
	}

	for i := 0; i < c.pullMsgGoroutines; i++ {
		<-done
	}

	return nil
}

func (c *consumerGroupHandler) consumeMsgFromBuffer(ctx context.Context, sess sarama.ConsumerGroupSession) {
	for i := 0; i < c.consumeMsgBufferGoroutines; i++ {
		c.logger.Printf(
			"consume msg from buffer topics:%v group_id:%v current goroutine index:%d",
			c.topics, c.groupID, i,
		)

		go func() {
			for msg := range c.msgBuffer {
				select {
				case <-ctx.Done():
					c.logger.Printf("ctx canceled err:%v", ctx.Err())
					return
				case <-c.stop:
					c.logger.Printf("topics:%v group_id:%v consume msg has stopped", c.topics, c.groupID)
					return
				default:
					if err := c.consume(ctx, msg, sess); err != nil {
						c.logger.Printf("kafka consume topic:%v group_id:%v err:%v\n", msg.Topic, c.groupID, err)
					}
				}
			}
		}()
	}
}

func (c *consumerGroupHandler) consume(ctx context.Context, msg *sarama.ConsumerMessage,
	sess sarama.ConsumerGroupSession) error {
	// handler msg with key
	if handler, exist := c.keyHandlers[string(msg.Key)]; exist && handler != nil {
		if err := c.handlerMsg(ctx, handler, msg.Value); err != nil {
			return err
		}

		// mark message as processed
		c.msgAck(sess, msg)

		return nil
	}

	// handler msg with metadata
	if c.subMessageHandler != nil {
		err := c.consumerMessage(ctx, c.subMessageHandler, msg)
		if err != nil {
			return err
		}

		// mark message as processed
		c.msgAck(sess, msg)

		return nil
	}

	// handler msg with value
	if c.subHandler == nil {
		return ErrSubHandlerInvalid
	}

	if err := c.handlerMsg(ctx, c.subHandler, msg.Value); err != nil {
		return err
	}

	// mark message as processed
	c.msgAck(sess, msg)

	return nil
}

// handlerMessage consumer msg
func (c *consumerGroupHandler) handlerMsg(ctx context.Context, subHandler broker.SubHandler, msg []byte) error {
	defer broker.Recovery(c.logger)

	return subHandler(ctx, msg)
}

func (c *consumerGroupHandler) msgAck(sess sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
	// mark message as processed
	sess.MarkMessage(msg, "")

	// Commit the offset to the backend for kafka
	// Note: calling Commit performs a blocking synchronous operation.
	if c.commitOffsetBlock {
		sess.Commit()
	}
}

// consumerMessage consumer msg with msg metadata
func (c *consumerGroupHandler) consumerMessage(ctx context.Context, subHandler broker.SubMessageHandler,
	msg *sarama.ConsumerMessage) error {
	defer broker.Recovery(c.logger)

	m := broker.ConsumerMessage{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       msg.Key,
		Value:     msg.Value,
	}

	if msg.Headers != nil {
		m.Headers = make([]broker.RecordHeader, 0, len(msg.Headers))
		for k := range msg.Headers {
			m.Headers = append(m.Headers, broker.RecordHeader{
				Key:   msg.Headers[k].Key,
				Value: msg.Headers[k].Value,
			})
		}
	}

	return subHandler(ctx, m)
}
