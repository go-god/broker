package gredis

import (
	"context"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/go-god/broker"
)

func TestRedisPublish(t *testing.T) {
	redisConf := broker.RedisConf{
		Address:     "127.0.0.1:6379",
		Password:    "", // no password set
		DB:          0,  // use default DB
		PoolSize:    10,
		PoolTimeout: 10 * time.Second,
	}

	b := New(
		broker.WithRedisConf(&redisConf),
		broker.WithBrokerPrefix("god"),
		broker.WithLogger(broker.LoggerFunc(log.Printf)), // logger
	)

	for i := 0; i < 10000; i++ {
		err := b.Publish(context.Background(), "my-topic", "hello,world: "+strconv.Itoa(i))
		log.Printf("publish err:%v\n", err)
	}

	_ = b.Shutdown(context.Background())
}

func TestRedisSub(t *testing.T) {
	redisConf := broker.RedisConf{
		Address:     "127.0.0.1:6379",
		Password:    "", // no password set
		DB:          0,  // use default DB
		PoolSize:    10,
		PoolTimeout: 10 * time.Second,
	}

	b := New(
		broker.WithRedisConf(&redisConf),
		broker.WithBrokerPrefix("god"),
		broker.WithLogger(broker.LoggerFunc(log.Printf)), // logger
		broker.WithNoDataWaitSec(2),                      // no data wait second time
		broker.WithGracefulWait(3*time.Second),
	)

	_ = b.Subscribe(context.Background(), "my-topic", "", func(ctx context.Context, data []byte) error {
		log.Printf("data: %v", string(data))
		return nil
	})
}
