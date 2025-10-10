package rabbitmq

import (
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// originalTopicKey 消息原始主题键
	originalTopicKey = "x-original-topic"

	// deliveryAttemptsKey 消息投递尝试次数键
	deliveryAttemptsKey = "x-delivery-attempts"
)

// setHeaderString 设置字符串类型的消息头
func setHeaderString(headers amqp.Table, key, value string) {
	if headers == nil {
		return
	}
	headers[key] = value
}

// getHeaderString 获取字符串类型的消息头
func getHeaderString(headers amqp.Table, key string) (string, bool) {
	if headers == nil {
		return "", false
	}

	raw, ok := headers[key]
	if !ok {
		return "", false
	}

	if str, ok := raw.(string); !ok {
		return "", false
	} else {
		return str, true
	}
}

// setHeaderInteger 设置整数类型的消息头
func setHeaderInteger(headers amqp.Table, key string, val int64) {
	if headers == nil {
		return
	}
	headers[key] = val
}

// getHeaderInteger 获取整数类型的消息头
func getHeaderInteger(headers amqp.Table, key string) (int64, bool) {
	if headers == nil {
		return 0, false
	}

	raw, ok := headers[key]
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
	case string:
		if n, err := strconv.ParseInt(val, 10, 64); err == nil {
			return n, true
		}
	}

	return 0, false
}

// setHeaderOriginalTopic 设置消息原始主题
func setHeaderOriginalTopic(headers amqp.Table, originalTopic string) {
	setHeaderString(headers, originalTopicKey, originalTopic)
}

// getHeaderOriginalTopic 获取消息原始主题
func getHeaderOriginalTopic(headers amqp.Table) (string, bool) {
	return getHeaderString(headers, originalTopicKey)
}

// setHeaderDeliveryAttempts 设置消息投递尝试次数
func setHeaderDeliveryAttempts(headers amqp.Table, attempt int) {
	setHeaderInteger(headers, deliveryAttemptsKey, int64(attempt))
}

// getHeaderDeliveryAttempts 获取消息投递尝试次数
func getHeaderDeliveryAttempts(headers amqp.Table) (int, bool) {
	if n, ok := getHeaderInteger(headers, deliveryAttemptsKey); ok {
		return int(n), true
	}
	return 0, false
}
