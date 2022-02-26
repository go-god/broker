package gpulsar

import (
	"context"
	"log"
	"testing"
	"time"

	broker "github.com/go-god/broker"
)

func TestPulsarBroker(t *testing.T) {
	b := New(
		broker.WithBrokerAddress("pulsar://127.0.0.1:6650"),
		broker.WithLogger(broker.LoggerFunc(log.Printf)),
	)

	err := b.Publish(context.Background(), "my-topic", "abc", broker.WithSendTimeout(30*time.Second))
	log.Printf("publish err:%v", err)
}

func TestPulsarBrokerConsumer(t *testing.T) {
	b := New(
		broker.WithBrokerAddress("pulsar://127.0.0.1:6650"),
		broker.WithLogger(broker.LoggerFunc(log.Printf)),
	)

	_ = b.Subscribe(context.Background(), "my-topic", "group-1", func(ctx context.Context, data []byte) error {
		log.Println("data: ", string(data))
		return nil
	})
}
