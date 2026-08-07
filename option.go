package broker

import "time"

// Option options functional option
type Option func(o *Options)

// Options broker option
type Options struct {
	Address  []string // client connection address list
	Prefix   string   // client mq prefix
	User     string   // user
	Password string   // password

	// ========kafka mq================
	// kafka protocol,eg:PLAINTEXT,SASL_PLAINTEXT,SASL_SSL 三种协议格式
	// 对于SASL_PLAINTEXT来说，只需要配置user/password即可
	// 对于SASL_SSL来说，需要配置user/password，如果有证书cert路径不为空，就需要设置 ssl.ca.location 读取证书
	Protocol string // default: PLAINTEXT

	// kafka sasl.mechanism,eg:PLAIN,SCRAM-SHA-256,SCRAM-SHA-512
	SASLMechanism string

	// 对于 Protocol=SASL_SSL，如果证书路径不为空，就读取证书
	// 同时，如果insecure_skip_verify参数为true，表示跳过证书,那么enable.ssl.certificate.verification=false，否则为true
	CertPath string // 证书路径，eg:/www/cert.crt

	// InsecureSkipVerify 是否跳过证书验证，仅对 SASL_SSL 生效
	InsecureSkipVerify bool

	// The level of compression to use on messages. The meaning depends
	// on the actual compression type used and defaults to default compression
	// level for the codec.
	// default: sarama.CompressionLevelDefault
	CompressionLevel int

	// The type of compression to use on messages (defaults to no compression).
	// Similar to `compression.codec` setting of the JVM producer.
	// this value for kafka producer
	// default: 0 no compression
	Compression int8

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

	// ConsumerAutoCommitInterval consumer auto commit interval (default: 1s)
	ConsumerAutoCommitInterval time.Duration

	// Logger record logger
	Logger Logger
}

// WithBrokerAddress set broker address
func WithBrokerAddress(address ...string) Option {
	return func(o *Options) {
		o.Address = append(o.Address, address...)
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

// WithConsumerAutoCommitInterval set consumer auto commit interval.
func WithConsumerAutoCommitInterval(interval time.Duration) Option {
	return func(o *Options) {
		o.ConsumerAutoCommitInterval = interval
	}
}

// WithKafkaProtocol set kafka protocol, eg: PLAINTEXT, SASL_PLAINTEXT, SASL_SSL.
func WithKafkaProtocol(protocol string) Option {
	return func(o *Options) {
		o.Protocol = protocol
	}
}

// WithSaslMechanism set kafka sasl mechanism, eg: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512.
func WithSaslMechanism(mechanism string) Option {
	return func(o *Options) {
		o.SASLMechanism = mechanism
	}
}

// WithCertPath set kafka cert path for SASL_SSL.
func WithCertPath(path string) Option {
	return func(o *Options) {
		o.CertPath = path
	}
}

// WithInsecureSkipVerify set whether to skip certificate verification for SASL_SSL.
func WithInsecureSkipVerify(skip bool) Option {
	return func(o *Options) {
		o.InsecureSkipVerify = skip
	}
}

// WithCompressionLevel set producer message compression level
func WithCompressionLevel(level int) Option {
	return func(o *Options) {
		o.CompressionLevel = level
	}
}

// WithCompression set producer msg compression
func WithCompression(c int8) Option {
	return func(o *Options) {
		o.Compression = c
	}
}
