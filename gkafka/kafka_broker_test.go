package gkafka

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/go-god/broker"
)

func TestKafkaPublish(t *testing.T) {
	b := New(
		broker.WithBrokerAddress("localhost:9092"),
		broker.WithLogger(broker.LoggerFunc(log.Printf)),
	)

	err := b.Publish(context.Background(), "my-topic", "kafka hello", broker.WithSendTimeout(30*time.Second))
	log.Printf("publish err:%v", err)
	_ = b.Shutdown(context.Background())
}

/**
=== RUN   TestKafkaPublish
2022/02/28 10:09:14 kafka producer partitionID: 0; offset:18, value: kafka hello
2022/02/28 10:09:14 publish err:<nil>
2022/02/28 10:09:19 subscribe msg shutting down,err:<nil>
2022/02/28 10:09:19 subscribe msg exit successfully
--- PASS: TestKafkaPublish (5.01s)
PASS
*/

func TestKafkaConsumer(t *testing.T) {
	b := New(
		broker.WithBrokerAddress("localhost:9092"),
		broker.WithLogger(broker.LoggerFunc(log.Printf)),
		broker.WithGracefulWait(3*time.Second),
	)

	_ = b.Subscribe(context.Background(), "my-topic", "group-1", func(ctx context.Context, data []byte) error {
		log.Println("data: ", string(data))
		return nil
	})
}

func TestNilByteEqEmptyStr(t *testing.T) {
	var key []byte
	log.Println(string(key) == "")
}

/**
=== RUN   TestKafkaConsumer
2022/02/27 22:04:29 subscribe message from kafka receive topic:my-topic channel:group-1 msg...
2022/02/27 22:05:09 received topic:my-topic channel:group-1 partition:0 offset:15 key:mytest:hello -- value:hello,daheige
2022/02/27 22:05:09 data:  hello,daheige
2022/02/27 22:05:09 received topic:my-topic channel:group-1 partition:0 offset:16 key:mytest:hello -- value:hello,daheige
2022/02/27 22:05:09 data:  hello,daheige
2022/02/28 10:08:24 received topic:my-topic channel:group-1 partition:0 offset:17 key:mytest:hello -- value:hello,daheige
2022/02/28 10:08:24 data:  hello,daheige
*/
