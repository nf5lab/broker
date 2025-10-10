package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/nf5lab/broker"
)

const (
	subscriberGroupPrefix = "subscriber.group" // 订阅组前缀

	delayExchangeSuffix = "delay" // 延迟交换机后缀
	delayQueueSuffix    = "delay" // 延迟队列后缀

	retryExchangeSuffix = "retry" // 重试交换机后缀
	retryQueueSuffix    = "retry" // 重试队列后缀

	deadLetterExchangeSuffix = "dead.letter" // 死信交换机后缀
	deadLetterQueueSuffix    = "dead.letter" // 死信队列后缀
	deadLetterQueueSize      = 1000000       // 死信队列大小
)

// genSubscriberQueueName 生成订阅队列名称
//
// 每个订阅组对应一个独立的队列, 实现组级别的负载均衡和消息隔离
func genSubscriberQueueName(mainExchangeName string, topic string, subscriberGroup string) string {
	return fmt.Sprintf(
		"%s.%s.%s.%s",
		mainExchangeName,
		topic,
		subscriberGroupPrefix,
		subscriberGroup,
	)
}

// genDelayExchangeName 生成延迟交换机名称
func genDelayExchangeName(mainExchangeName string) string {
	return fmt.Sprintf("%s.%s", mainExchangeName, delayExchangeSuffix)
}

// genDelayQueueName 生成延迟队列名称
func genDelayQueueName(mainExchangeName string, topic string, delay time.Duration) string {
	return fmt.Sprintf(
		"%s.%s.%dms.%s",
		mainExchangeName,
		topic,
		normalizeDelay(delay).Milliseconds(),
		delayQueueSuffix,
	)
}

// genRetryExchangeName 生成重试交换机名称
func genRetryExchangeName(mainExchangeName string) string {
	return fmt.Sprintf("%s.%s", mainExchangeName, retryExchangeSuffix)
}

// genRetryQueueName 生成重试队列名称
//
// 每个订阅组的重试队列是隔离的, 实现组级别的重试机制
func genRetryQueueName(mainExchangeName string, topic string, subscriberGroup string, delay time.Duration) string {
	return fmt.Sprintf(
		"%s.%s.%s.%s.%dms.%s",
		mainExchangeName,
		topic,
		subscriberGroupPrefix,
		subscriberGroup,
		normalizeDelay(delay).Milliseconds(),
		retryQueueSuffix,
	)
}

// genDeadLetterExchangeName 生成死信交换机名称
func genDeadLetterExchangeName(mainExchangeName string) string {
	return fmt.Sprintf("%s.%s", mainExchangeName, deadLetterExchangeSuffix)
}

// genDeadLetterQueueName 生成死信队列名称
//
// 每个订阅组的死信队列是隔离的, 实现组级别的死信隔离
func genDeadLetterQueueName(mainExchangeName string, topic string, subscriberGroup string) string {
	return fmt.Sprintf(
		"%s.%s.%s.%s.%s",
		mainExchangeName,
		topic,
		subscriberGroupPrefix,
		subscriberGroup,
		deadLetterQueueSuffix,
	)
}

// declareExchange 声明交换机
func declareExchange(ch *amqp.Channel, name string, kind string, args amqp.Table) error {
	return ch.ExchangeDeclare(name, kind, true, false, false, false, args)
}

// declareQueue 声明队列
func declareQueue(ch *amqp.Channel, name string, args amqp.Table) error {
	_, err := ch.QueueDeclare(name, true, false, false, false, args)
	return err
}

// bindQueue 绑定队列到交换机
func bindQueue(ch *amqp.Channel, queueName string, routingKey string, exchangeName string, args amqp.Table) error {
	return ch.QueueBind(queueName, routingKey, exchangeName, false, args)
}

// declareMainExchange 声明主交换机
func declareMainExchange(ch *amqp.Channel, mainExchangeName string) error {
	if err := declareExchange(ch, mainExchangeName, "topic", nil); err != nil {
		return fmt.Errorf("rabbitmq: 声明主交换机: %s 失败: %w", mainExchangeName, err)
	}
	return nil
}

// declareSubscriberQueue 声明订阅队列
//
// 路由流程: 主交换机(topic) → 订阅队列
func declareSubscriberQueue(ch *amqp.Channel, mainExchangeName string, topic string, subscriberGroup string) error {
	var (
		subscriberQueueName    = genSubscriberQueueName(mainExchangeName, topic, subscriberGroup)
		deadLetterExchangeName = genDeadLetterExchangeName(mainExchangeName)
		deadLetterQueueName    = genDeadLetterQueueName(mainExchangeName, topic, subscriberGroup)
	)

	if err := declareExchange(ch, deadLetterExchangeName, "direct", nil); err != nil {
		return fmt.Errorf("rabbitmq: 声明死信交换机: %s 失败: %w", deadLetterExchangeName, err)
	}

	deadLetterQueueArgs := amqp.Table{
		"x-max-length": deadLetterQueueSize,
		"x-overflow":   "drop-head",
	}

	if err := declareQueue(ch, deadLetterQueueName, deadLetterQueueArgs); err != nil {
		return fmt.Errorf("rabbitmq: 声明死信队列: %s 失败: %w", deadLetterQueueName, err)
	}

	deadLetterRoutingKey := deadLetterQueueName // 注意: 路由键是死信队列的名称
	if err := bindQueue(ch, deadLetterQueueName, deadLetterRoutingKey, deadLetterExchangeName, nil); err != nil {
		return fmt.Errorf("rabbitmq: 绑定死信队列: %s --> %s 失败: %w", deadLetterQueueName, deadLetterExchangeName, err)
	}

	subscriberQueueArgs := amqp.Table{
		"x-dead-letter-exchange":    deadLetterExchangeName,
		"x-dead-letter-routing-key": deadLetterRoutingKey,
		"x-max-priority":            broker.MaxPriorityLimit,
	}

	if err := declareQueue(ch, subscriberQueueName, subscriberQueueArgs); err != nil {
		return fmt.Errorf("rabbitmq: 声明订阅队列: %s 失败: %w", subscriberQueueName, err)
	}

	if err := bindQueue(ch, subscriberQueueName, topic, mainExchangeName, nil); err != nil {
		return fmt.Errorf("rabbitmq: 绑定订阅队列: %s --> %s 失败: %w", subscriberQueueName, mainExchangeName, err)
	}

	return nil
}

// declareDelayQueue 声明延迟队列
//
// 路由流程: 延迟交换机(direct) → 延迟队列(过期) → 主交换机(topic) → 主题队列
func declareDelayQueue(ch *amqp.Channel, mainExchangeName string, topic string, delay time.Duration) error {
	delay = normalizeDelay(delay)

	var (
		delayExchangeName = genDelayExchangeName(mainExchangeName)
		delayQueueName    = genDelayQueueName(mainExchangeName, topic, delay)
	)

	if err := declareExchange(ch, delayExchangeName, "direct", nil); err != nil {
		return fmt.Errorf("rabbitmq: 声明延迟交换机: %s 失败: %w", delayExchangeName, err)
	}

	delayQueueArgs := amqp.Table{
		"x-message-ttl":             delay.Milliseconds(),
		"x-dead-letter-exchange":    mainExchangeName, // 消息过期后, 投递到主交换机
		"x-dead-letter-routing-key": topic,            // 消息过期后, 使用原主题路由
		"x-max-priority":            broker.MaxPriorityLimit,
	}

	if err := declareQueue(ch, delayQueueName, delayQueueArgs); err != nil {
		return fmt.Errorf("rabbitmq: 声明延迟队列: %s 失败: %w", delayQueueName, err)
	}

	delayRoutingKey := delayQueueName // 注意: 路由键就是延迟队列的名称
	if err := bindQueue(ch, delayQueueName, delayRoutingKey, delayExchangeName, nil); err != nil {
		return fmt.Errorf("rabbitmq: 绑定延迟队列: %s --> %s 失败: %w", delayQueueName, delayExchangeName, err)
	}

	return nil
}

// declareRetryQueue 声明重试队列
//
// 路由流程: 重试交换机(direct) → 重试队列(过期) → 重试交换机(direct) → 原订阅队列
//
// 详细说明(逻辑有点绕):
// 第一步: 发布重试消息, 会投递到重试交换机, 然后路由到重试队列
// 第二步: 重试消息过期, 会再次投递到重试交换机, 然后路由到原订阅队列
//
// 特殊处理:
// 为了让重试消息和正常消息主题相同, 重试消息需要携带原主题信息
// 路由层解决不了这个问题, 只能在消息头中携带原主题, 需特殊处理
func declareRetryQueue(ch *amqp.Channel, mainExchangeName string, topic string, subscriberGroup string, delay time.Duration) error {
	delay = normalizeDelay(delay)

	var (
		retryExchangeName = genRetryExchangeName(mainExchangeName)
		retryQueueName    = genRetryQueueName(mainExchangeName, topic, subscriberGroup, delay)
		originalQueueName = genSubscriberQueueName(mainExchangeName, topic, subscriberGroup)
	)

	if err := declareExchange(ch, retryExchangeName, "direct", nil); err != nil {
		return fmt.Errorf("rabbitmq: 声明重试交换机: %s 失败: %w", retryExchangeName, err)
	}

	retryQueueArgs := amqp.Table{
		"x-message-ttl":             delay.Milliseconds(),
		"x-dead-letter-exchange":    retryExchangeName, // 消息过期后, 再次投递到重试交换机
		"x-dead-letter-routing-key": originalQueueName, // 消息过期后, 使用原订阅队列路由
		"x-max-priority":            broker.MaxPriorityLimit,
	}

	if err := declareQueue(ch, retryQueueName, retryQueueArgs); err != nil {
		return fmt.Errorf("rabbitmq: 声明重试队列: %s 失败: %w", retryQueueName, err)
	}

	retryQueueRoutingKey := retryQueueName // 注意: 路由键是重试队列的名称
	if err := bindQueue(ch, retryQueueName, retryQueueRoutingKey, retryExchangeName, nil); err != nil {
		return fmt.Errorf("rabbitmq: 绑定重试队列: %s --> %s 失败: %w", retryQueueName, retryExchangeName, err)
	}

	originalRoutingKey := originalQueueName // 注意: 路由键是原订阅队列的名称
	if err := bindQueue(ch, originalQueueName, originalRoutingKey, retryExchangeName, nil); err != nil {
		return fmt.Errorf("rabbitmq: 绑定订阅队列到重试交换机: %s --> %s 失败: %w", originalQueueName, retryExchangeName, err)
	}

	return nil
}

// publishMaybeConfirm 发布消息, 如果通道处于确认模式, 则等待确认
func publishMaybeConfirm(ctx context.Context, ch *amqp.Channel, exchangeName string, routingKey string, msg amqp.Publishing) error {
	deferredConfirm, err := ch.PublishWithDeferredConfirmWithContext(ctx, exchangeName, routingKey, false, false, msg)
	if err != nil {
		return err
	}

	if deferredConfirm != nil {
		if ack, err := deferredConfirm.WaitContext(ctx); err != nil {
			return err
		} else {
			if !ack {
				return errors.New("rabbitmq: 消息未确认")
			}
		}
	}

	return nil
}

// consumeMessage 消费消息
func consumeMessage(ctx context.Context, ch *amqp.Channel, queueName string, subId string, args amqp.Table) (<-chan amqp.Delivery, error) {
	return ch.ConsumeWithContext(ctx, queueName, subId, false, false, false, false, args)
}

// waitConnClose 等待连接关闭
func waitConnClose(ctx context.Context, conn *amqp.Connection) {
	if conn == nil || conn.IsClosed() {
		return
	}

	healthTicker := time.NewTicker(100 * time.Millisecond)
	defer healthTicker.Stop()

	closeNotify := conn.NotifyClose(make(chan *amqp.Error, 1))
	for {
		select {
		case <-ctx.Done():
			// 正常退出
			return

		case err := <-closeNotify:
			// 异常关闭
			if err != nil {
				slog.Warn("rabbitmq: 连接异常断开", slog.Any("error", err))
			}
			return

		case <-healthTicker.C:
			// 周期性主动检测
			if conn.IsClosed() {
				slog.Warn("rabbitmq: 连接异常断开")
				return
			}
		}
	}
}

// connectBackoff 连接退避函数
func connectBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		return 0
	}

	switch attempt {
	case 1:
		return 5 * time.Second
	case 2:
		return 10 * time.Second
	default:
		return 30 * time.Second
	}
}

// attemptConnect 尝试连接
func attemptConnect(ctx context.Context, connectionUrl string, maxAttempts int) (*amqp.Connection, error) {
	slog.Debug("rabbitmq: 开始尝试连接")

	for attempt := range maxAttempts {
		if attempt > 0 {
			backoff := connectBackoff(attempt)

			select {
			case <-ctx.Done():
				// 正常关闭
				return nil, ctx.Err()

			case <-time.After(backoff):
				// 重连延时
			}
		}

		conn, err := amqp.Dial(connectionUrl)
		if err != nil {
			slog.Warn(
				"rabbitmq: 连接失败，稍后重试",
				slog.Int("attempt", attempt),
				slog.Any("error", err),
			)
			continue
		}

		slog.Debug("rabbitmq: 连接成功")
		return conn, nil
	}

	return nil, fmt.Errorf("rabbitmq: 连接失败, 达到最大重试次数 %d", maxAttempts)
}

// closeConn 关闭连接
func closeConn(conn *amqp.Connection) {
	if conn == nil || conn.IsClosed() {
		return
	}

	if err := conn.Close(); err != nil {
		slog.Warn("rabbitmq: 关闭连接失败", slog.Any("error", err))
	}
}

func closeChannel(channel *amqp.Channel) {
	if channel == nil || channel.IsClosed() {
		return
	}

	if err := channel.Close(); err != nil {
		slog.Warn("rabbitmq: 关闭通道失败", slog.Any("error", err))
	}
}

func ackDelivery(delivery *amqp.Delivery) {
	if err := delivery.Ack(false); err != nil {
		slog.Error(
			"rabbitmq: 确认消息失败",
			slog.String("messageId", delivery.MessageId),
			slog.Any("error", err),
		)
	}
}

func rejectDelivery(delivery *amqp.Delivery, requeue bool) {
	if err := delivery.Reject(requeue); err != nil {
		slog.Error(
			"rabbitmq: 拒绝消息失败",
			slog.String("messageId", delivery.MessageId),
			slog.Any("error", err),
		)
	}
}
