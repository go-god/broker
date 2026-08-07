package broker

import (
	"time"
)

// SubOption subscribe option
type SubOption func(s *SubscribeOptions)

// SubscribeOptions subscribe message option
type SubscribeOptions struct {
	// specifies the consumer name,like kafka consume group id
	Name string

	// sub message with metadata
	SubMessageHandler SubMessageHandler

	// KeyHandlers for kafka consumer message key handler map
	// for redis sub,you can specify different message subscriber functions to handle msg.
	KeyHandlers map[string]SubHandler

	// consume msg from multiple topics
	Topics []string

	// pull msg goroutines for subscribe,default:1
	PullMsgGoroutines int

	// for kafka whether put the message in the buffer pool first when consuming it
	EnableMsgBuffer            bool
	MsgBufferSize              int
	ConsumeMsgBufferGoroutines int

	// for pulsar mq receive messages from channel.
	// The channel returns a struct which contains message and the consumer from where
	// the message was received. It's not necessary here since we have 1 single consumer, but the channel could be
	// shared across multiple consumers as well
	MessageChannel     bool // default:false
	MessageChannelSize int  // default:100

	Offset int64

	// Commit the offset to the backend for kafka
	// Note: calling Commit performs a blocking synchronous operation.
	CommitOffsetBlock bool

	// SubInterval subscribe interval,default:0
	SubInterval time.Duration

	// ===========pulsar mq=======
	// subType specifies the subscription type to be used when subscribing to a topic.
	// Default is `Shared` 1:N
	// Exclusive there can be only 1 consumer on the same topic with the same subscription name
	//
	// Shared 1:N
	// Shared subscription mode, multiple consumer will be able to use the same subscription name
	// and the messages will be dispatched according to
	//
	// Failover subscription mode, multiple consumer will be able to use the same subscription name
	// but only 1 consumer will receive the messages.
	// If that consumer disconnects, one of the other connected consumers will start receiving messages.
	SubType SubscriptionType

	// ReceiverQueueSize sets the size of the consumer receive queue.
	// The consumer receive queue controls how many messages can be accumulated by the `Consumer` before the
	// application calls `Consumer.receive()`. Using a higher value could potentially increase the consumer
	// throughput at the expense of bigger memory utilization.
	// Default value is `1000` messages and should be good for most use cases.
	ReceiverQueueSize int

	// retryEnable for pulsar sub RetryEnable
	RetryEnable bool
}

// WithSubName set sub name
func WithSubName(name string) SubOption {
	return func(s *SubscribeOptions) {
		s.Name = name
	}
}

// WithSubKeyHandlers set sub key => subHandler map
func WithSubKeyHandlers(keyHandlers map[string]SubHandler) SubOption {
	return func(s *SubscribeOptions) {
		s.KeyHandlers = keyHandlers
	}
}

// WithSubTopics set sub topics
func WithSubTopics(topics []string) SubOption {
	return func(s *SubscribeOptions) {
		s.Topics = append(s.Topics, topics...)
	}
}

// WithSubMessageChannel set sub message channel
func WithSubMessageChannel() SubOption {
	return func(s *SubscribeOptions) {
		s.MessageChannel = true
	}
}

// WithSubMessageChannelSize set sub message channel size
func WithSubMessageChannelSize(size int) SubOption {
	return func(s *SubscribeOptions) {
		s.MessageChannelSize = size
	}
}

// WithSubPullMsgGoroutines set goroutines for pull msg,default:1
func WithSubPullMsgGoroutines(size int) SubOption {
	return func(s *SubscribeOptions) {
		s.PullMsgGoroutines = size
	}
}

// WithSubEnableBuffer enable consume msg buffer
func WithSubEnableBuffer() SubOption {
	return func(s *SubscribeOptions) {
		s.EnableMsgBuffer = true
	}
}

// WithSubBufferSize set consume msg buffer size
func WithSubBufferSize(size int) SubOption {
	return func(s *SubscribeOptions) {
		s.MsgBufferSize = size
	}
}

// WithSubConsumeMsgGoroutines set consume msg from buffer goroutines
func WithSubConsumeMsgGoroutines(size int) SubOption {
	return func(s *SubscribeOptions) {
		s.ConsumeMsgBufferGoroutines = size
	}
}

// WithSubOffset set sub offset
func WithSubOffset(offset int64) SubOption {
	return func(s *SubscribeOptions) {
		s.Offset = offset
	}
}

// WithSubInterval set sub interval
func WithSubInterval(t time.Duration) SubOption {
	return func(s *SubscribeOptions) {
		s.SubInterval = t
	}
}

// WithSubType set subType
func WithSubType(t SubscriptionType) SubOption {
	return func(s *SubscribeOptions) {
		s.SubType = t
	}
}

// WithSubRetryEnable set sub retry
func WithSubRetryEnable() SubOption {
	return func(s *SubscribeOptions) {
		s.RetryEnable = true
	}
}

// WithCommitOffsetBlock commit offset block when message consumer.
func WithCommitOffsetBlock() SubOption {
	return func(s *SubscribeOptions) {
		s.CommitOffsetBlock = true
	}
}

// WithSubMessageHandler set SubMessageHandler
func WithSubMessageHandler(handler SubMessageHandler) SubOption {
	return func(s *SubscribeOptions) {
		s.SubMessageHandler = handler
	}
}

// SubscriptionType of subscription supported by Pulsar
type SubscriptionType int

const (
	// Exclusive there can be only 1 consumer on the same topic with the same subscription name
	Exclusive SubscriptionType = iota

	// Shared subscription mode, multiple consumer will be able to use the same subscription name
	// and the messages will be dispatched according to
	// a round-robin rotation between the connected consumers
	Shared

	// Failover subscription mode, multiple consumer will be able to use the same subscription name
	// but only 1 consumer will receive the messages.
	// If that consumer disconnects, one of the other connected consumers will start receiving messages.
	Failover

	// KeyShared subscription mode, multiple consumer will be able to use the same
	// subscription and all messages with the same key will be dispatched to only one consumer
	KeyShared
)
