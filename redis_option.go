package broker

import "time"

// RedisConf redis client config
type RedisConf struct {
	// host:port address.
	Address string

	// Optional password. Must match the password specified in the
	// require pass server configuration option.
	Password string

	// Database to be selected after connecting to the server.
	DB int

	// Maximum number of retries before giving up.
	// Default is to not retry failed commands.
	MaxRetries int

	// Dial timeout for establishing new connections.
	// Default is 5 seconds.
	DialTimeout time.Duration

	// Timeout for socket reads. If reached, commands will fail
	// with a timeout instead of blocking. Use value -1 for no timeout and 0 for default.
	// Default is 3 seconds.
	ReadTimeout time.Duration

	// Timeout for socket writes. If reached, commands will fail
	// with a timeout instead of blocking.
	// Default is ReadTimeout.
	WriteTimeout time.Duration

	// Maximum number of socket connections.
	// Default is 10 connections per every CPU as reported by runtime.NumCPU.
	PoolSize int

	// Amount of time client waits for connection if all connections
	// are busy before returning an error.
	// Default is ReadTimeout + 1 second.
	PoolTimeout time.Duration

	// Minimum number of idle connections which is useful when establishing
	// new connection is slow.
	MinIdleConns int

	// Amount of time after which client closes idle connections.
	// Should be less than server's timeout.
	// Default is 5 minutes. -1 disables idle timeout check.
	IdleTimeout time.Duration

	// Connection age at which client retires (closes) the connection.
	// go redis Default is to not close aged connections
	// but 1800s is recommended.
	MaxConnAge time.Duration
}

// WithRedisConf with redis config
func WithRedisConf(conf *RedisConf) Option {
	return func(o *Options) {
		o.RedisConf = conf
	}
}
