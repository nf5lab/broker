package broker

import (
	"strings"
	"time"
)

const (
	// DefaultPriority 消息默认优先级
	DefaultPriority int = 5

	// MaxPriorityLimit 消息最大优先级限制
	MaxPriorityLimit int = 10
)

const (
	// DefaultGroupName 默认订阅分组名称
	DefaultGroupName = "default"
)

const (
	// DefaultConcurrency 默认并发处理数
	DefaultConcurrency = 1

	// MaxConcurrencyLimit 最大并发处理数限制
	MaxConcurrencyLimit = 64
)

const (
	// DefaultAttempts 消息默认尝试次数
	DefaultAttempts = 10

	// MaxAttemptsLimit 消息最大尝试次数限制
	MaxAttemptsLimit = 32
)

// PublishOptions 发布选项
type PublishOptions struct {

	// Priority 消息优先级
	// 控制消息的优先级, 数值越大优先级越高
	// 注意: 有的消息队列不支持消息优先级, 会忽略该字段
	//
	// - 取值范围: 1 ~ MaxPriorityLimit
	// - 设置为 0, 表示使用默认值 DefaultPriority
	Priority int

	// Ttl 消息存活时间
	// 控制消息在队列中的存活时间
	// 过期消息将被自动删除或移至死信队列
	//
	// - 设置为 0, 表示消息永不过期
	Ttl time.Duration

	// Delay 延迟发送时间
	// 控制消息的延迟投递
	// 适用于定时任务, 延迟通知等场景
	//
	// - 设置为 0, 表示立即发送
	Delay time.Duration
}

// DefaultPublishOptions 默认的发布选项
func DefaultPublishOptions() *PublishOptions {
	return &PublishOptions{
		Priority: DefaultPriority,
		Ttl:      0,
		Delay:    0,
	}
}

// Normalize 规范发布选项
func (opts *PublishOptions) Normalize() {
	if opts == nil {
		return
	}

	if opts.Priority <= 0 {
		opts.Priority = DefaultPriority
	}

	if opts.Priority > MaxPriorityLimit {
		opts.Priority = MaxPriorityLimit
	}

	if opts.Ttl < 0 {
		opts.Ttl = 0
	}

	if opts.Delay < 0 {
		opts.Delay = 0
	}
}

// PublishOption 发布选项的配置函数
type PublishOption func(*PublishOptions)

// NewPublishOptions 新建发布选项
func NewPublishOptions(opts ...PublishOption) *PublishOptions {
	options := DefaultPublishOptions()
	for _, apply := range opts {
		if apply != nil {
			apply(options)
		}
	}
	options.Normalize()
	return options
}

// WithPublishPriority 设置消息优先级
func WithPublishPriority(priority int) PublishOption {
	return func(opts *PublishOptions) {
		opts.Priority = priority
	}
}

// WithPublishTtl 设置消息存活时间
func WithPublishTtl(ttl time.Duration) PublishOption {
	return func(opts *PublishOptions) {
		opts.Ttl = ttl
	}
}

// WithPublishDelay 设置消息延迟发送时间
func WithPublishDelay(delay time.Duration) PublishOption {
	return func(opts *PublishOptions) {
		opts.Delay = delay
	}
}

// RetryBackoff 重试退避函数
// 用于计算重试退避时间
//
// 参数:
// - retryCount: 当前重试次数, 从 0 开始计数
//   - retryCount = 0 表示初始尝试 (不算重试), 永远返回0
//   - retryCount = 1 表示第1次重试
//   - retryCount = 2 表示第2次重试
//   - retryCount = 3 表示第3次重试
//   - 以此类推
//
// 返回值:
// - time.Duration: 重试退避时间
type RetryBackoff func(retryCount int) time.Duration

// DefaultRetryBackoff 默认的重试退避函数
func DefaultRetryBackoff(retryCount int) time.Duration {
	if retryCount <= 0 {
		return 0
	}

	switch retryCount {
	case 1:
		return 5 * time.Second
	case 2:
		return 10 * time.Second
	case 3:
		return 30 * time.Second
	case 4:
		return 60 * time.Second
	default:
		return 120 * time.Second
	}
}

// SubscribeOptions 订阅选项
type SubscribeOptions struct {

	// Group 订阅组
	// 控制消息的负载均衡和广播
	// 消息会投递给每一个订阅组
	// 组内只有一个订阅者会收到消息
	//
	// - 不区分大小写
	// - 空字符串, 表示使用默认值 DefaultGroupName
	Group string

	// Concurrency 并发处理数
	// 控制订阅者的并发处理能力
	//
	// - 取值范围: 1 ~ MaxConcurrencyLimit
	// - 设置为 0, 表示使用默认值 DefaultConcurrency
	Concurrency int

	// MaxAttempts 最大尝试次数 (包括首次)
	// 控制消息的最大尝试次数
	// 超过最大尝试次数的消息将被丢弃或移至死信队列
	//
	// - 设置为 1, 只尝试1次，不重试
	// - 设置为 2, 最多尝试2次（1次初始 + 1次重试）
	// - 设置为 3, 最多尝试3次（1次初始 + 2次重试）
	// - 设置为 0, 表示使用默认值 DefaultAttempts
	MaxAttempts int

	// RetryBackoff 消息重试退避函数
	// 控制消息重试的退避时间
	//
	// - 设置为 nil, 表示使用默认的重试退避函数 DefaultRetryBackoff
	RetryBackoff RetryBackoff
}

// DefaultSubscribeOptions 默认的订阅选项
func DefaultSubscribeOptions() *SubscribeOptions {
	return &SubscribeOptions{
		Group:        DefaultGroupName,
		Concurrency:  DefaultConcurrency,
		MaxAttempts:  DefaultAttempts,
		RetryBackoff: DefaultRetryBackoff,
	}
}

// Normalize 规范订阅选项
func (opts *SubscribeOptions) Normalize() {
	if opts == nil {
		return
	}

	opts.Group = strings.TrimSpace(opts.Group)
	opts.Group = strings.ToLower(opts.Group)
	if len(opts.Group) == 0 {
		opts.Group = DefaultGroupName
	}

	if opts.Concurrency <= 0 {
		opts.Concurrency = DefaultConcurrency
	}

	if opts.Concurrency > MaxConcurrencyLimit {
		opts.Concurrency = MaxConcurrencyLimit
	}

	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = DefaultAttempts
	}

	if opts.MaxAttempts > MaxAttemptsLimit {
		opts.MaxAttempts = MaxAttemptsLimit
	}

	if opts.RetryBackoff == nil {
		opts.RetryBackoff = DefaultRetryBackoff
	}
}

// SubscribeOption 订阅选项的配置函数
type SubscribeOption func(*SubscribeOptions)

// NewSubscribeOptions 新建订阅选项
func NewSubscribeOptions(opts ...SubscribeOption) *SubscribeOptions {
	options := DefaultSubscribeOptions()
	for _, apply := range opts {
		if apply != nil {
			apply(options)
		}
	}
	options.Normalize()
	return options
}

// WithSubscribeGroup 设置订阅组
func WithSubscribeGroup(group string) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.Group = group
	}
}

// WithSubscribeConcurrency 设置并发处理数
func WithSubscribeConcurrency(concurrency int) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.Concurrency = concurrency
	}
}

// WithSubscribeMaxAttempts 设置最大尝试次数
//
// maxAttempts: 最大尝试次数（包括首次）
func WithSubscribeMaxAttempts(maxAttempts int) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.MaxAttempts = maxAttempts
	}
}

// WithSubscribeRetryBackoff 设置消息重试退避函数
func WithSubscribeRetryBackoff(backoff RetryBackoff) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.RetryBackoff = backoff
	}
}
