package broker

import "time"

// Option options functional option
type Option func(o *Options)

// Options broker option
type Options struct {
	Addrs    []string // client connection address list
	Prefix   string   // client mq prefix
	User     string   // user
	Password string   // password

	// ========pulsar mq===============
	// ListenerName Configure the net model for vpc user to connect the pulsar broker
	ListenerName string
	// AuthToken auth token
	AuthToken string
	// OperationTimeout operation timeout
	OperationTimeout time.Duration
	// ConnectionTimeout timeout for the establishment of a TCP connection (default: 10 seconds)
	ConnectionTimeout time.Duration

	// MaxConnectionsPerBroker the max number of connections to a single broker
	// that will keep in the pool. (Default: 1 connection)
	// this param for pulsar connection per broker
	MaxConnectionsPerBroker int

	// =======redis mq================
	RedisConf *RedisConf

	// graceful exit time
	GracefulWait time.Duration

	// no data wait second
	NoDataWaitSec int

	// ConsumerAutoCommitInterval consumer auto commit interval
	ConsumerAutoCommitInterval time.Duration

	// Logger logger
	Logger Logger
}

// WithBrokerAddress set broker address
func WithBrokerAddress(addrs ...string) Option {
	return func(o *Options) {
		o.Addrs = addrs
	}
}

// WithBrokerPrefix set broker prefix
func WithBrokerPrefix(prefix string) Option {
	return func(o *Options) {
		o.Prefix = prefix
	}
}

// WithUser set broker user
func WithUser(user string) Option {
	return func(o *Options) {
		o.User = user
	}
}

// WithPassword set broker password
func WithPassword(pwd string) Option {
	return func(o *Options) {
		o.Password = pwd
	}
}

// WithListenerName set broker listener name
func WithListenerName(name string) Option {
	return func(o *Options) {
		o.ListenerName = name
	}
}

// WithAuthToken set broker token eg:pulsar broker
func WithAuthToken(token string) Option {
	return func(o *Options) {
		o.AuthToken = token
	}
}

// WithOperationTimeout set broker op timeout
func WithOperationTimeout(t time.Duration) Option {
	return func(o *Options) {
		o.OperationTimeout = t
	}
}

// WithConnectionTimeout set broker connection timeout
func WithConnectionTimeout(t time.Duration) Option {
	return func(o *Options) {
		o.ConnectionTimeout = t
	}
}

// WithMaxConnectionsPerBroker set max connection
func WithMaxConnectionsPerBroker(num int) Option {
	return func(o *Options) {
		o.MaxConnectionsPerBroker = num
	}
}

// WithGracefulWait set sub graceful exit time
func WithGracefulWait(t time.Duration) Option {
	return func(s *Options) {
		s.GracefulWait = t
	}
}

// WithNoDataWaitSec no data wait second
func WithNoDataWaitSec(sec int) Option {
	return func(o *Options) {
		o.NoDataWaitSec = sec
	}
}

// WithLogger set broker logger
func WithLogger(logger Logger) Option {
	return func(o *Options) {
		o.Logger = logger
	}
}

// PubOption publish option
type PubOption func(p *PublishOptions)

// PublishOptions publish message option
type PublishOptions struct {
	// PublishDelay specifies the time period within which the messages sent will be batched (default: 10ms)
	// if message is enabled. If set to a non zero value, messages will be queued until this time
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
	// By default batching is enabled.
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

// SubOption subscribe option
type SubOption func(s *SubscribeOptions)

// SubscribeOptions subscribe message option
type SubscribeOptions struct {
	// specifies the consumer name
	Name string

	// KeyHandlers for kafka consumer message key handler map
	// for redis sub,you can specify different message subscriber functions to handle msg.
	KeyHandlers map[string]SubHandler

	// Receive messages from channel. The channel returns a struct which contains message and the consumer from where
	// the message was received. It's not necessary here since we have 1 single consumer, but the channel could be
	// shared across multiple consumers as well
	MessageChannel     bool // default:false
	MessageChannelSize int  // default:100

	// subscribe concurrency count,default:1
	// Note: this param for redis or pulsar consumer message
	ConcurrencySize int

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

// WithMessageChannel set sub message channel
func WithMessageChannel() SubOption {
	return func(s *SubscribeOptions) {
		s.MessageChannel = true
	}
}

// WithMessageChannelSize set sub message channel size
func WithMessageChannelSize(size int) SubOption {
	return func(s *SubscribeOptions) {
		s.MessageChannelSize = size
	}
}

// WithSubConcurrencySize set subscribe size
func WithSubConcurrencySize(size int) SubOption {
	return func(s *SubscribeOptions) {
		s.ConcurrencySize = size
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
