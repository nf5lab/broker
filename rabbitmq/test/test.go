package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"time"

	"github.com/nf5lab/broker"
	"github.com/nf5lab/broker/rabbitmq"
)

func main() {
	brk, err := rabbitmq.NewBroker("amqp://guest:guest@localhost:5672/", "test")
	if err != nil {
		log.Fatal(err)
	}
	defer brk.Close()

	handler := func(ctx context.Context, delivery *broker.Delivery) error {
		log.Printf(
			"收到消息, 主题: %s, 内容: %s, 尝试次数: %d",
			delivery.Topic,
			string(delivery.Message.Body),
			delivery.Attempts,
		)

		if delivery.Attempts < 3 {
			return fmt.Errorf("模拟错误, 触发重试")
		}

		if rand.Int()%2 == 0 {
			innerErr := fmt.Errorf("模拟随机错误, 不会重试")
			return broker.NewNonRetryableError(innerErr)
		}

		// 处理成功
		return nil
	}

	for i := 0; i < 4; i++ {
		groupName := fmt.Sprintf("group%d", i)
		subscriptionId, err := brk.Subscribe(context.Background(), "events.*", handler, broker.WithSubscribeGroup(groupName))
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("订阅成功, 订阅ID: %s", subscriptionId)
	}

	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		hello := fmt.Sprintf("hello %d", i)

		msg := &broker.Message{
			Id:   "msg-1",
			Body: []byte(hello),
		}

		err = brk.Publish(context.Background(), "events.hello", msg, broker.WithPublishDelay(5*time.Second))
		if err != nil {
			log.Println(err)
			continue
		}
	}

	select {}
}
