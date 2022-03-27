package gpulsar

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/go-god/broker"
)

func TestPulsarBrokerPublish(t *testing.T) {
	b := New(
		broker.WithBrokerAddress("pulsar://127.0.0.1:6650"),
		broker.WithLogger(broker.LoggerFunc(log.Printf)),
	)

	err := b.Publish(context.Background(), "my-topic", `{"data":"hello"}`, broker.WithSendTimeout(30*time.Second))
	log.Printf("publish err:%v", err)
	_ = b.Shutdown(context.Background())
}

func TestPulsarBrokerConsumer(t *testing.T) {
	b := New(
		broker.WithBrokerAddress("pulsar://127.0.0.1:6650"),
		broker.WithLogger(broker.LoggerFunc(log.Printf)),
		broker.WithNoDataWaitSec(3),
		broker.WithGracefulWait(3*time.Second),
	)

	_ = b.Subscribe(context.Background(), "my-topic", "group-1", func(ctx context.Context, data []byte) error {
		log.Println("data: ", string(data))
		return nil
	})
}
