package gpulsar

import (
	"context"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func TestPulsarPublish(t *testing.T) {
	// create pulsar client
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://127.0.0.1:6650",
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("could not connection pulsar client:%v", err)
	}

	defer client.Close()

	// create producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "my-topic",
	})

	defer producer.Close()
	// send message
	for i := 0; i < 100; i++ {
		sendMsg(producer, i)
	}
}

func sendMsg(producer pulsar.Producer, i int) error {
	msgID, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: []byte("hello,world:" + strconv.Itoa(i)),
	})
	if err != nil {
		return err
	}

	log.Printf("message id:%v partitionIdx:%v", msgID.EntryID(), msgID.PartitionIdx())
	return nil
}

func TestPulsarConsumer(t *testing.T) {
	// create pulsar client
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://127.0.0.1:6650",
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("could not connection pulsar client:%v", err)
	}

	defer client.Close()

	// sub
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            "my-topic",
		SubscriptionName: "my-topic-sub",
		Type:             pulsar.Shared,
	})
	defer consumer.Close()

	for {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatalf("consumer error:%v", err)
		}

		// receive msg
		log.Printf("Received message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer.Ack(msg)
	}
}

func TestPulsarConsumeReader(t *testing.T) {
	// create pulsar client
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://127.0.0.1:6650",
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("could not connection pulsar client:%v", err)
	}

	defer client.Close()

	reader, err := client.CreateReader(pulsar.ReaderOptions{
		Topic:          "my-topic",
		StartMessageID: pulsar.EarliestMessageID(),
	})

	for reader.HasNext() {
		msg, err := reader.Next(context.Background())
		if err != nil {
			log.Fatalf("consumer error:%v", err)
		}

		// receive msg
		log.Printf("Received message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
	}
}
