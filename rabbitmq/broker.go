package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/nf5lab/broker"
)

// Broker RabbitMQ 消息代理
//
// 加锁说明:
// connLock            保护 isClosed / connection / publishChannel
// subscriptionLock    保护 subscriptionSpecs / subscriptionTasks
// declaredQueuesLock  保护 declaredQueues
//
// 加锁顺序:
// 如果需要多把锁, 必须按 connLock -> subscriptionLock -> declaredQueuesLock 顺序获取
type Broker struct {
	connectionUrl string
	exchangeName  string

	// 连接管理
	connLock       sync.Mutex
	isClosed       bool
	connection     *amqp.Connection
	publishChannel *amqp.Channel

	// 后台任务
	cancelCtx  context.Context
	cancelFunc context.CancelFunc
	wait       sync.WaitGroup

	// 订阅管理
	subscriptionLock  sync.Mutex
	subscriptionSpecs map[string]*subscriptionSpec
	subscriptionTasks map[string]*subscriptionTask

	// 队列声明
	declaredQueuesLock sync.Mutex
	declaredQueues     map[string]bool
}

// subscriptionSpec 订阅规格
// 表示订阅的静态信息，用于断线后恢复
type subscriptionSpec struct {
	id      string
	topic   string
	group   string
	handler broker.Handler
	options *broker.SubscribeOptions
}

// subscriptionTask 订阅任务
// 表示运行中的订阅信息
type subscriptionTask struct {
	id      string
	topic   string
	group   string
	handler broker.Handler
	options *broker.SubscribeOptions

	channel    *amqp.Channel
	deliveries <-chan amqp.Delivery

	cancelCtx  context.Context
	cancelFunc context.CancelFunc
	wait       sync.WaitGroup
}

// NewBroker 创建代理
func NewBroker(connectionUrl string, exchangeName string) (*Broker, error) {
	if connectionUrl = strings.TrimSpace(connectionUrl); len(connectionUrl) == 0 {
		return nil, fmt.Errorf("rabbitmq: 连接地址不能为空")
	}

	if exchangeName = strings.TrimSpace(exchangeName); len(exchangeName) == 0 {
		return nil, fmt.Errorf("rabbitmq: 交换机名称不能为空")
	}

	cancelCtx, cancelFunc := context.WithCancel(context.Background())

	brk := &Broker{
		connectionUrl:     connectionUrl,
		exchangeName:      exchangeName,
		isClosed:          false,
		cancelCtx:         cancelCtx,
		cancelFunc:        cancelFunc,
		subscriptionSpecs: make(map[string]*subscriptionSpec),
		subscriptionTasks: make(map[string]*subscriptionTask),
		declaredQueues:    make(map[string]bool),
	}

	// 尝试初始连接
	conn, err := attemptConnect(brk.cancelCtx, brk.connectionUrl, 3)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: 初始连接失败: %w", err)
	} else {
		brk.switchToConnected(conn)
	}

	// 启动自动重连协程
	brk.wait.Add(1)
	go brk.autoReconnect()

	slog.Info(
		"rabbitmq: 代理已创建",
		slog.String("connectionUrl", connectionUrl),
		slog.String("exchangeName", exchangeName),
	)
	return brk, nil
}

// ============================== 发布管理 ==============================

// Publish 发布消息
func (brk *Broker) Publish(ctx context.Context, topic string, msg *broker.Message, opts ...broker.PublishOption) error {
	if topic = strings.TrimSpace(topic); len(topic) == 0 {
		return broker.ErrEmptyTopic
	}

	if msg == nil {
		return broker.ErrEmptyMessage
	}

	if err := msg.Validate(); err != nil {
		return err
	}

	options := broker.NewPublishOptions(opts...)
	amqpMsg := amqp.Publishing{
		MessageId:    msg.Id,
		Headers:      make(amqp.Table),
		Body:         msg.Body,
		ContentType:  msg.ContentType,
		Priority:     uint8(options.Priority),
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
	}

	// 复制消息头
	maps.Copy(amqpMsg.Headers, msg.Headers)

	// 额外消息头
	setHeaderOriginalTopic(amqpMsg.Headers, topic) // 原始主题
	setHeaderDeliveryAttempts(amqpMsg.Headers, 1)  // 投递次数

	// 消息过期时间(毫秒)
	if options.Ttl > 0 {
		amqpMsg.Expiration = fmt.Sprintf("%d", options.Ttl.Milliseconds())
	}

	// 获取发布通道
	ch, err := brk.getPublishChannel()
	if err != nil {
		return err
	}

	// 发布延迟消息
	if delay := normalizeDelay(options.Delay); delay > 0 {
		var (
			delayExchangeName = genDelayExchangeName(brk.exchangeName)
			delayQueueName    = genDelayQueueName(brk.exchangeName, topic, delay)
		)

		// 确保延迟队列存在
		if err := brk.ensureDelayQueue(ch, topic, delay); err != nil {
			return err
		}

		routingKey := delayQueueName // 注意: 路由键是延迟队列名称
		return publishMaybeConfirm(ctx, ch, delayExchangeName, routingKey, amqpMsg)
	}

	// 发布常规消息
	routingKey := topic // 注意: 路由键是主题名称
	return publishMaybeConfirm(ctx, ch, brk.exchangeName, routingKey, amqpMsg)
}

// ensureDelayQueue 确保延迟队列存在 (线程安全)
func (brk *Broker) ensureDelayQueue(ch *amqp.Channel, topic string, delay time.Duration) error {
	var delayQueueName = genDelayQueueName(brk.exchangeName, topic, delay)

	brk.declaredQueuesLock.Lock()
	defer brk.declaredQueuesLock.Unlock()

	if brk.declaredQueues[delayQueueName] {
		return nil
	}

	if err := declareDelayQueue(ch, brk.exchangeName, topic, delay); err != nil {
		return err
	}

	brk.declaredQueues[delayQueueName] = true
	return nil
}

// getPublishChannel 获取发布通道 (线程安全)
func (brk *Broker) getPublishChannel() (*amqp.Channel, error) {
	brk.connLock.Lock()
	defer brk.connLock.Unlock()

	if brk.isClosed {
		return nil, errors.New("rabbitmq: 代理已关闭")
	}

	if brk.publishChannel == nil || brk.publishChannel.IsClosed() {
		if brk.connection == nil || brk.connection.IsClosed() {
			return nil, errors.New("rabbitmq: 连接不可用")
		}

		newChannel, err := brk.connection.Channel()
		if err != nil {
			return nil, fmt.Errorf("rabbitmq: 创建发布通道失败: %w", err)
		}

		if err := newChannel.Confirm(false); err != nil {
			closeChannel(newChannel)
			return nil, fmt.Errorf("rabbitmq: 发布通道开启确认模式失败: %w", err)
		}

		if brk.publishChannel != nil {
			closeChannel(brk.publishChannel)
		}
		brk.publishChannel = newChannel
	}

	return brk.publishChannel, nil
}

// ============================== 订阅管理 ==============================

// Subscribe 订阅消息
func (brk *Broker) Subscribe(ctx context.Context, topic string, handler broker.Handler, opts ...broker.SubscribeOption) (string, error) {
	if topic = strings.TrimSpace(topic); len(topic) == 0 {
		return "", broker.ErrEmptyTopic
	}

	if handler == nil {
		return "", broker.ErrEmptyHandler
	}

	options := broker.NewSubscribeOptions(opts...)
	if len(options.Group) == 0 {
		options.Group = broker.DefaultGroupName
	}

	subscriptionId := fmt.Sprintf(
		"%s-%s-%s",
		topic,
		options.Group,
		uuid.NewString(),
	)

	// 创建订阅规格
	spec := &subscriptionSpec{
		id:      subscriptionId,
		topic:   topic,
		group:   options.Group,
		handler: handler,
		options: options,
	}

	conn, err := brk.getConnection()
	if err != nil {
		return "", err
	}

	task, err := brk.startSubscriptionTask(conn, spec)
	if err != nil {
		return "", err
	}

	// 记录订阅信息
	brk.subscriptionLock.Lock()
	brk.subscriptionSpecs[spec.id] = spec
	brk.subscriptionTasks[task.id] = task
	brk.subscriptionLock.Unlock()

	return spec.id, nil
}

// startSubscriptionTask 启动订阅任务
func (brk *Broker) startSubscriptionTask(conn *amqp.Connection, spec *subscriptionSpec) (*subscriptionTask, error) {
	if conn == nil || conn.IsClosed() {
		return nil, errors.New("rabbitmq: 连接不可用")
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: 创建订阅通道失败: %w", err)
	}

	success := false
	defer func() {
		if !success {
			closeChannel(channel)
		}
	}()

	if spec.options.Concurrency > 0 {
		if err := channel.Qos(spec.options.Concurrency, 0, false); err != nil {
			return nil, fmt.Errorf("rabbitmq: 设置订阅通道预取失败: %w", err)
		}
	}

	if err := declareSubscriberQueue(channel, brk.exchangeName, spec.topic, spec.group); err != nil {
		return nil, fmt.Errorf("rabbitmq: 声明订阅队列失败: %w", err)
	}

	taskCtx, taskCancel := context.WithCancel(brk.cancelCtx)
	defer func() {
		if !success {
			taskCancel()
		}
	}()

	queueName := genSubscriberQueueName(brk.exchangeName, spec.topic, spec.group)
	deliveries, err := consumeMessage(taskCtx, channel, queueName, spec.id, nil)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: 开始消费失败: %w", err)
	}

	task := &subscriptionTask{
		id:         spec.id,
		topic:      spec.topic,
		group:      spec.group,
		handler:    spec.handler,
		options:    spec.options,
		channel:    channel,
		deliveries: deliveries,
		cancelCtx:  taskCtx,
		cancelFunc: taskCancel,
	}

	for i := 0; i < spec.options.Concurrency; i++ {
		task.wait.Add(1)
		go brk.subscriptionWorker(task)
	}

	success = true
	return task, nil
}

// subscriptionWorker 订阅工作协程
func (brk *Broker) subscriptionWorker(task *subscriptionTask) {
	defer task.wait.Done()

	for {
		select {
		case <-task.cancelCtx.Done():
			slog.Debug("rabbitmq: 订阅工作协程退出", slog.String("subscriptionId", task.id))
			return

		case delivery, ok := <-task.deliveries:
			if !ok {
				slog.Warn("rabbitmq: 订阅消息通道已关闭", slog.String("subscriptionId", task.id))
				return
			} else {
				brk.processMessage(task, &delivery)
			}
		}
	}
}

func (brk *Broker) processMessage(task *subscriptionTask, amqpDelivery *amqp.Delivery) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error(
				"rabbitmq: 消息处理异常",
				slog.String("messageId", amqpDelivery.MessageId),
				slog.Any("panic", r),
			)

			// 发生异常, 重新入队
			rejectDelivery(amqpDelivery, true)
		}
	}()

	// 转换消息
	delivery := brk.castDelivery(amqpDelivery)

	// 处理消息
	ctx, cancel := context.WithTimeout(task.cancelCtx, 180*time.Second)
	defer cancel()

	if err := task.handler(ctx, delivery); err == nil {
		// 处理成功, 确认原消息
		ackDelivery(amqpDelivery)
		return
	} else if broker.IsNonRetryableError(err) {
		// 不可重试错误, 移至死信队列
		rejectDelivery(amqpDelivery, false)
		return
	} else if delivery.Attempts >= task.options.MaxAttempts {
		slog.Warn(
			"rabbitmq: 达到最大重试次数",
			slog.String("messageId", delivery.Message.Id),
			slog.Int("attempts", delivery.Attempts),
		)

		// 达到最大重试次数, 移至死信队列
		rejectDelivery(amqpDelivery, false)
		return
	}

	// 发布重试消息
	if err := brk.publishRetryMessage(ctx, task, amqpDelivery); err != nil {
		// 重试失败, 重新入队
		rejectDelivery(amqpDelivery, true)
	} else {
		// 重试成功, 确认原消息
		ackDelivery(amqpDelivery)
	}
}

// publishRetryMessage 发布重试消息
func (brk *Broker) publishRetryMessage(ctx context.Context, task *subscriptionTask, amqpDelivery *amqp.Delivery) error {
	var topic string
	if val, ok := getHeaderOriginalTopic(amqpDelivery.Headers); ok {
		topic = val
	} else {
		topic = amqpDelivery.RoutingKey
	}

	attempts, _ := getHeaderDeliveryAttempts(amqpDelivery.Headers)
	if attempts < 1 {
		attempts = 1
	}

	// 创建重试消息
	retryMsg := amqp.Publishing{
		MessageId:    amqpDelivery.MessageId,
		Headers:      maps.Clone(amqpDelivery.Headers),
		Body:         amqpDelivery.Body,
		ContentType:  amqpDelivery.ContentType,
		Priority:     amqpDelivery.Priority,
		DeliveryMode: amqpDelivery.DeliveryMode,
		Timestamp:    time.Now(),
		Expiration:   amqpDelivery.Expiration,
	}

	// 额外消息头
	attempts++
	setHeaderOriginalTopic(retryMsg.Headers, topic)       // 原始主题
	setHeaderDeliveryAttempts(retryMsg.Headers, attempts) // 投递次数

	// 计算重试退避时间
	retryCount := attempts - 1
	backoff := task.options.RetryBackoff(retryCount)
	backoff = normalizeDelay(backoff)
	if backoff <= 0 {
		backoff = 5 * time.Second
	}

	var (
		retryExchangeName = genRetryExchangeName(brk.exchangeName)
		retryQueueName    = genRetryQueueName(brk.exchangeName, topic, task.group, backoff)
	)

	// 确保重试队列存在
	if err := brk.ensureRetryQueue(task.channel, topic, task.group, backoff); err != nil {
		return err
	}

	retryRoutingKey := retryQueueName // 注意: 路由键就是重试队列的名称
	return publishMaybeConfirm(ctx, task.channel, retryExchangeName, retryRoutingKey, retryMsg)
}

// ensureRetryQueue 确保重试队列存在 (线程安全)
func (brk *Broker) ensureRetryQueue(ch *amqp.Channel, topic string, subscriberGroup string, delay time.Duration) error {
	var retryQueueName = genRetryQueueName(brk.exchangeName, topic, subscriberGroup, delay)

	brk.declaredQueuesLock.Lock()
	defer brk.declaredQueuesLock.Unlock()

	if brk.declaredQueues[retryQueueName] {
		return nil
	}

	if err := declareRetryQueue(ch, brk.exchangeName, topic, subscriberGroup, delay); err != nil {
		return err
	}

	brk.declaredQueues[retryQueueName] = true
	return nil
}

// castDelivery 转换 AMQP Delivery -> 通用 Delivery
func (brk *Broker) castDelivery(amqpDelivery *amqp.Delivery) *broker.Delivery {
	message := broker.Message{
		Id:          amqpDelivery.MessageId,
		Headers:     maps.Clone(amqpDelivery.Headers),
		Body:        amqpDelivery.Body,
		ContentType: amqpDelivery.ContentType,
	}

	var topic string
	if val, ok := getHeaderOriginalTopic(amqpDelivery.Headers); ok {
		topic = val
	} else {
		topic = amqpDelivery.RoutingKey
	}

	attempts, _ := getHeaderDeliveryAttempts(amqpDelivery.Headers)
	if attempts < 1 {
		attempts = 1
	}

	return &broker.Delivery{
		Message:     message,
		Topic:       topic,
		Attempts:    attempts,
		ReceiveTime: time.Now(),
	}
}

// getConnection 获取连接 (线程安全)
func (brk *Broker) getConnection() (*amqp.Connection, error) {
	brk.connLock.Lock()
	defer brk.connLock.Unlock()

	if brk.isClosed {
		return nil, errors.New("rabbitmq: 代理已关闭")
	}

	if brk.connection == nil {
		return nil, errors.New("rabbitmq: 连接未建立")
	}

	if brk.connection.IsClosed() {
		return nil, errors.New("rabbitmq: 连接不可用")
	}

	return brk.connection, nil
}

// Unsubscribe 取消订阅
func (brk *Broker) Unsubscribe(ctx context.Context, subscriptionId string) error {
	if subscriptionId = strings.TrimSpace(subscriptionId); len(subscriptionId) == 0 {
		return broker.ErrEmptySubscriptionId
	}

	brk.subscriptionLock.Lock()
	defer brk.subscriptionLock.Unlock()

	if spec, ok := brk.subscriptionSpecs[subscriptionId]; ok {
		delete(brk.subscriptionSpecs, spec.id)
	} else {
		return broker.ErrSubscriptionNotFound
	}

	if task, ok := brk.subscriptionTasks[subscriptionId]; ok {
		delete(brk.subscriptionTasks, task.id)
		brk.stopSubscriptionTask(task)
	}

	slog.Debug("rabbitmq: 取消订阅成功", slog.String("subscriptionId", subscriptionId))
	return nil
}

// stopSubscriptionTask 停止单个订阅任务
func (brk *Broker) stopSubscriptionTask(task *subscriptionTask) {
	if task == nil {
		return
	}

	if task.cancelFunc != nil {
		task.cancelFunc()
	}

	task.wait.Wait()
	if task.channel != nil {
		closeChannel(task.channel)
	}
}

// stopAllSubscriptionTasks 停止所有订阅任务
func (brk *Broker) stopAllSubscriptionTasks() {
	brk.subscriptionLock.Lock()
	tasks := maps.Clone(brk.subscriptionTasks)
	brk.subscriptionTasks = make(map[string]*subscriptionTask)
	brk.subscriptionLock.Unlock()

	for _, task := range tasks {
		brk.stopSubscriptionTask(task)
	}
}

// restartAllSubscriptionTasks 重启所有订阅任务
func (brk *Broker) restartAllSubscriptionTasks() {
	brk.stopAllSubscriptionTasks()

	conn, err := brk.getConnection()
	if err != nil {
		slog.Error("rabbitmq: 重启所有订阅失败", slog.Any("error", err))
		return
	}

	brk.subscriptionLock.Lock()
	defer brk.subscriptionLock.Unlock()

	for id := range brk.subscriptionSpecs {
		task, err := brk.startSubscriptionTask(conn, brk.subscriptionSpecs[id])
		if err != nil {
			slog.Error("rabbitmq: 重启订阅任务失败", slog.String("subscriptionId", id), slog.Any("error", err))
			continue
		}

		brk.subscriptionTasks[task.id] = task
	}
}

// ============================== 生命周期 ==============================

// Close 关闭代理 (可以多次调用)
func (brk *Broker) Close() error {
	// 快速检查
	select {
	case <-brk.cancelCtx.Done():
		// 已关闭
		return nil
	default:
		// 未关闭
	}

	brk.connLock.Lock()
	{
		if brk.cancelFunc != nil {
			brk.cancelFunc()
		}

		if !brk.isClosed {
			brk.isClosed = true
		}
	}
	brk.connLock.Unlock()

	// 停止所有订阅任务
	brk.stopAllSubscriptionTasks()

	// 等待后台任务退出
	brk.wait.Wait()

	brk.connLock.Lock()
	{
		// 关闭发布通道
		if brk.publishChannel != nil {
			closeChannel(brk.publishChannel)
			brk.publishChannel = nil
		}

		// 关闭连接
		if brk.connection != nil {
			closeConn(brk.connection)
			brk.connection = nil
		}
	}
	brk.connLock.Unlock()

	slog.Info("rabbitmq: 代理已关闭")
	return nil
}

// autoReconnect 自动重连
func (brk *Broker) autoReconnect() {
	defer func() {
		slog.Debug("rabbitmq: 退出自动重连")
		brk.wait.Done()
	}()

	for {
		select {
		case <-brk.cancelCtx.Done():
			// 正常退出
			return

		case <-time.After(10 * time.Millisecond):
			// 防止忙等待
		}

		brk.connLock.Lock()
		conn := brk.connection
		brk.connLock.Unlock()

		if conn != nil {
			// 等待连接断开 (阻塞等待)
			waitConnClose(brk.cancelCtx, conn)

			// 运行到这里, 说明连接已经断开
			brk.switchToDisconnected()
		}

		// 尝试重新连接
		newConn, err := attemptConnect(brk.cancelCtx, brk.connectionUrl, 10)
		if err != nil {
			slog.Error("rabbitmq: 尝试重连失败, 稍后继续重连", slog.Any("error", err))
			continue
		}

		// 运行到这里, 说明连接成功
		brk.switchToConnected(newConn)
	}
}

// switchToDisconnected 切换为断开连接状态
func (brk *Broker) switchToDisconnected() {
	slog.Info("rabbitmq: 连接已断开")

	// 关闭连接和通道
	brk.connLock.Lock()
	{
		if brk.publishChannel != nil {
			closeChannel(brk.publishChannel)
			brk.publishChannel = nil
		}

		if brk.connection != nil {
			closeConn(brk.connection)
			brk.connection = nil
		}
	}
	brk.connLock.Unlock()

	// 停止所有订阅任务
	brk.stopAllSubscriptionTasks()

	// 清空已声明队列缓存
	brk.declaredQueuesLock.Lock()
	{
		brk.declaredQueues = make(map[string]bool)
	}
	brk.declaredQueuesLock.Unlock()
}

// switchToConnected 切换为已连接状态
func (brk *Broker) switchToConnected(newConn *amqp.Connection) {
	slog.Info("rabbitmq: 连接已建立")

	var success = false
	defer func() {
		if !success {
			closeConn(newConn)
		}
	}()

	publishChannel, err := newConn.Channel()
	if err != nil {
		slog.Error("rabbitmq: 创建发布通道失败", slog.Any("error", err))
		return
	}
	defer func() {
		if !success {
			closeChannel(publishChannel)
		}
	}()

	if err := publishChannel.Confirm(false); err != nil {
		slog.Error("rabbitmq: 发布通道开启确认模式失败", slog.Any("error", err))
		return
	}

	if err := declareMainExchange(publishChannel, brk.exchangeName); err != nil {
		slog.Error("rabbitmq: 声明主交换机失败", slog.Any("error", err))
		return
	}

	brk.connLock.Lock()
	{
		brk.connection = newConn
		brk.publishChannel = publishChannel
	}
	brk.connLock.Unlock()

	// 重启订阅任务
	brk.restartAllSubscriptionTasks()

	// 设置成功标记
	success = true
}
