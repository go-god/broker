package broker

import (
	"context"
)

// Broker broker interface
type Broker interface {
	// Publish publish message to topic
	Publish(ctx context.Context, topic string, msg interface{}, opts ...PubOption) error
	// Subscribe subscribe message from topic + channel
	Subscribe(ctx context.Context, topic string, channel string, subHandler SubHandler, opts ...SubOption) error
	// Shutdown graceful shutdown broker
	Shutdown(ctx context.Context) error
}

// SubHandler subscribe func
type SubHandler func(ctx context.Context, data []byte) error
