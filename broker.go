// Package broker 用于抽象不同的消息队列
package broker

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"time"
)

const (
	// DefaultContentType 默认内容类型
	DefaultContentType = "application/octet-stream"
)

var (
	ErrEmptyTopic           = errors.New("broker: 主题不能为空")
	ErrEmptyMessage         = errors.New("broker: 消息不能为空")
	ErrEmptyMessageId       = errors.New("broker: 消息ID不能为空")
	ErrEmptyMessageBody     = errors.New("broker: 消息体不能为空")
	ErrEmptyHandler         = errors.New("broker: 处理函数不能为空")
	ErrEmptySubscriptionId  = errors.New("broker: 订阅ID不能为空")
	ErrSubscriptionNotFound = errors.New("broker: 订阅不存在")
)

// Message 表示消息队列中的消息
type Message struct {

	// Id 消息ID
	Id string `json:"id"`

	// Headers 消息头
	// 用于存储应用相关的额外信息
	Headers map[string]any `json:"headers"`

	// Body 消息体
	Body []byte `json:"body"`

	// ContentType 消息内容类型
	// 例如 "application/json"
	ContentType string `json:"contentType"`

	// PartitionKey 消息分区键
	//
	// - RabbitMQ: 会忽略该字段
	// - Kafka:    用于消息分区
	PartitionKey string `json:"partitionKey"`
}

// Normalize 规范消息
func (msg *Message) Normalize() {
	if msg == nil {
		return
	}

	msg.Id = strings.TrimSpace(msg.Id)
	msg.ContentType = strings.TrimSpace(msg.ContentType)
	if len(msg.ContentType) == 0 {
		msg.ContentType = DefaultContentType
	}
	msg.PartitionKey = strings.TrimSpace(msg.PartitionKey)
}

// Validate 校验消息
func (msg *Message) Validate() error {
	if msg == nil {
		return ErrEmptyMessage
	} else {
		msg.Normalize()
	}

	if len(msg.Id) == 0 {
		return ErrEmptyMessageId
	}

	if len(msg.Body) == 0 {
		return ErrEmptyMessageBody
	}

	return nil
}

// Clone 克隆消息
func (msg *Message) Clone() *Message {
	if msg == nil {
		return nil
	}

	return &Message{
		Id:           msg.Id,
		Headers:      maps.Clone(msg.Headers),
		Body:         slices.Clone(msg.Body),
		ContentType:  msg.ContentType,
		PartitionKey: msg.PartitionKey,
	}
}

// AddHeader 添加消息头
func (msg *Message) AddHeader(key string, val any) {
	if msg == nil {
		return
	}

	if msg.Headers == nil {
		msg.Headers = make(map[string]any)
	}

	if key = strings.TrimSpace(key); len(key) > 0 {
		msg.Headers[key] = val
	}
}

// GetHeader 获取消息头
func (msg *Message) GetHeader(key string) (any, bool) {
	if msg == nil || len(msg.Headers) == 0 {
		return nil, false
	}

	key = strings.TrimSpace(key)
	if len(key) == 0 {
		return nil, false
	}

	val, ok := msg.Headers[key]
	if !ok {
		return nil, false
	}
	return val, true
}

// DelHeader 删除消息头
func (msg *Message) DelHeader(key string) {
	if msg == nil || len(msg.Headers) == 0 {
		return
	}

	key = strings.TrimSpace(key)
	if len(key) > 0 {
		delete(msg.Headers, key)
	}
}

// AddHeaders 批量添加消息头
func (msg *Message) AddHeaders(headers map[string]any) {
	if msg == nil {
		return
	}

	if msg.Headers == nil {
		msg.Headers = make(map[string]any)
	}

	for key, val := range headers {
		if key = strings.TrimSpace(key); len(key) > 0 {
			msg.Headers[key] = val
		}
	}
}

// ClearHeaders 清空所有消息头
func (msg *Message) ClearHeaders() {
	if msg != nil {
		msg.Headers = nil
	}
}

// AddHeaderString 添加string类型的消息头
func (msg *Message) AddHeaderString(key string, val string) {
	msg.AddHeader(key, val)
}

// GetHeaderString 获取string类型的消息头
func (msg *Message) GetHeaderString(key string) (string, bool) {
	raw, ok := msg.GetHeader(key)
	if !ok {
		return "", false
	}

	if val, ok := raw.(string); !ok {
		return "", false
	} else {
		return val, true
	}
}

// AddHeaderBool 添加bool类型的消息头
func (msg *Message) AddHeaderBool(key string, val bool) {
	msg.AddHeader(key, val)
}

// GetHeaderBool 获取bool类型的消息头
func (msg *Message) GetHeaderBool(key string) (bool, bool) {
	raw, ok := msg.GetHeader(key)
	if !ok {
		return false, false
	}

	if val, ok := raw.(bool); !ok {
		return false, false
	} else {
		return val, true
	}
}

// AddHeaderInteger 添加整数类型的消息头
func (msg *Message) AddHeaderInteger(key string, val int64) {
	msg.AddHeader(key, val)
}

// GetHeaderInteger 获取整数类型的消息头
func (msg *Message) GetHeaderInteger(key string) (int64, bool) {
	raw, ok := msg.GetHeader(key)
	if !ok {
		return 0, false
	}

	switch val := raw.(type) {
	case int:
		return int64(val), true
	case int8:
		return int64(val), true
	case int16:
		return int64(val), true
	case int32:
		return int64(val), true
	case int64:
		return val, true
	default:
		return 0, false
	}
}

// AddHeaderFloat 添加浮点数类型的消息头
func (msg *Message) AddHeaderFloat(key string, val float64) {
	msg.AddHeader(key, val)
}

// GetHeaderFloat 获取浮点数类型的消息头
func (msg *Message) GetHeaderFloat(key string) (float64, bool) {
	raw, ok := msg.GetHeader(key)
	if !ok {
		return 0, false
	}

	switch val := raw.(type) {
	case float32:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
	}
}

// Delivery 表示消息的投递信息
type Delivery struct {

	// Message 投递的消息
	Message Message `json:"message"`

	// Topic 消息的逻辑主题
	//
	// 一般情况(等于原始主题):
	// - 正常消息: 逻辑主题 等于 原始主题
	// - 延时消息: 逻辑主题 等于 原始主题
	// - 重试消息: 逻辑主题 等于 原始主题
	//
	// 特殊情况(不等于原始主题):
	// - 死信消息: 逻辑主题 不等于 原始主题, 不要用于业务处理
	Topic string `json:"topic"`

	// Attempts 消息投递的尝试次数 (包括首次)
	//
	// - 第一次尝试: Attempts = 1
	// - 第二次尝试: Attempts = 2（第1次重试）
	// - 第三次尝试: Attempts = 3（第2次重试）
	// - 以此类推...
	Attempts int `json:"attempts"`

	// ReceiveTime 消息的接收时间
	ReceiveTime time.Time `json:"receiveTime"`
}

// IsFirstAttempt 是否是首次尝试
func (d *Delivery) IsFirstAttempt() bool {
	return d.Attempts <= 1
}

// IsRetry 是否是重试
func (d *Delivery) IsRetry() bool {
	return d.Attempts > 1
}

// RetryCount 获取重试次数
func (d *Delivery) RetryCount() int {
	if d.Attempts <= 1 {
		return 0
	}
	return d.Attempts - 1
}

// ShouldRetry 是否应该继续重试
func (d *Delivery) ShouldRetry(maxAttempts int) bool {
	return d.Attempts < maxAttempts
}

// NonRetryableError 表示不可重试的错误
//
// 当处理函数返回该错误时, 消息不会重试
type NonRetryableError struct {
	inner error
}

func (err *NonRetryableError) Error() string {
	if err == nil || err.inner == nil {
		return "broker: 不可重试的错误"
	}
	return err.inner.Error()
}

func (err *NonRetryableError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.inner
}

// NewNonRetryableError 新建不可重试的错误
func NewNonRetryableError(inner error) error {
	if inner == nil {
		return nil
	}
	return &NonRetryableError{inner: inner}
}

// IsNonRetryableError 是否是不可重试的错误
func IsNonRetryableError(err error) bool {
	var target *NonRetryableError
	return errors.As(err, &target)
}

// Handler 消息处理函数
//
// 参数:
// - delivery: 投递信息
//
// 返回:
// - 处理成功, 返回 nil, 消息将被确认
// - 处理失败, 返回错误
//   - 正常错误, 根据重试策略进行重试
//   - 特殊错误: NonRetryableError 消息不会被重试(被丢弃或移至死信队列)
type Handler func(ctx context.Context, delivery *Delivery) error

// Publisher 表示消息发布者
type Publisher interface {

	// Publish 发布消息
	//
	// 参数:
	// - topic: 逻辑主题
	// - msg:   消息内容
	// - opts:  发布选项
	Publish(ctx context.Context, topic string, msg *Message, opts ...PublishOption) error

	// Close 关闭发布者
	Close() error
}

// Subscriber 表示消息订阅者
type Subscriber interface {

	// Subscribe 订阅消息
	//
	// 参数:
	// - topic:   逻辑主题
	// - handler: 消息处理函数
	// - opts:    订阅选项
	//
	// 返回:
	// - string: 订阅ID
	// - error:  错误信息
	Subscribe(ctx context.Context, topic string, handler Handler, opts ...SubscribeOption) (string, error)

	// Unsubscribe 取消订阅
	Unsubscribe(ctx context.Context, subscriptionId string) error

	// Close 关闭订阅者
	Close() error
}

// Broker 表示消息代理
//
// 消息代理组合了发布者和订阅者
type Broker interface {
	Publisher
	Subscriber
}
