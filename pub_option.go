package broker

import (
	"time"
)

// PubOption publish option
type PubOption func(p *PublishOptions)

// PublishOptions publish message option
type PublishOptions struct {
	// PublishDelay specifies the time period within which the messages sent will be batched (default: 10ms)
	// if message is enabled. If set to a no zero value, messages will be queued until this time
	// interval or until
	PublishDelay time.Duration

	// Name specifies a name for the producer.
	// if you use pulsar mq,if not assigned, the system will generate
	// a globally unique name which can be access with
	// Producer.ProducerName().
	//
	// for kafka publish message key
	// The partitioning key for this message. Pre-existing Encoders include
	// StringEncoder and ByteEncoder.
	Name string

	// DisableBatching controls whether automatic batching of messages is enabled for the producer.
	// default batching is enabled.
	// When batching is enabled, multiple calls to Producer.sendAsync can result in a single batch to be sent to the
	// broker, leading to better throughput, especially when publishing small messages. If compression is enabled,
	// messages will be compressed at the batch level, leading to a much better compression ratio
	// for similar headers or contents.
	// When enabled default batch delay is set to 1 ms and default batch size is 1000 messages
	// Setting `DisableBatching: true` will make the producer to send messages individually
	DisableBatching bool

	// SendTimeout specifies the timeout for a message that has not been acknowledged by the server since sent.
	// Send and SendAsync returns an error after timeout.
	// Default is 30 seconds, negative such as -1 to disable.
	SendTimeout time.Duration
}

// WithPublishDelay set publish delay time
func WithPublishDelay(t time.Duration) PubOption {
	return func(p *PublishOptions) {
		p.PublishDelay = t
	}
}

// WithPublishName set publish script name
func WithPublishName(name string) PubOption {
	return func(p *PublishOptions) {
		p.Name = name
	}
}

// WithDisableBatching disable batch publish
func WithDisableBatching() PubOption {
	return func(p *PublishOptions) {
		p.DisableBatching = true
	}
}

// WithSendTimeout set publish send msg timeout
func WithSendTimeout(t time.Duration) PubOption {
	return func(p *PublishOptions) {
		p.SendTimeout = t
	}
}
