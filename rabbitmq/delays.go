package rabbitmq

import (
	"time"
)

// supportedDelays 支持的延迟时长
//
// 为了简化部署和运维, 没有使用插件
// 而是使用死信队列, 但会增加队列数量
// 为了限制队列数量, 只支持预定义的延迟时长
var supportedDelays = []time.Duration{

	// 秒级
	1 * time.Second, 5 * time.Second,
	10 * time.Second, 30 * time.Second,

	// 分钟级
	1 * time.Minute, 2 * time.Minute,
	3 * time.Minute, 4 * time.Minute,
	5 * time.Minute, 6 * time.Minute,
	7 * time.Minute, 8 * time.Minute,
	9 * time.Minute, 10 * time.Minute,
	20 * time.Minute, 30 * time.Minute,

	// 小时级
	1 * time.Hour, 2 * time.Hour,
}

// normalizeDelay 规范延迟时长
// 向上取整到最接近的预定义值
func normalizeDelay(delay time.Duration) time.Duration {
	if delay <= 0 {
		return 0
	}

	for _, supportedDelay := range supportedDelays {
		if delay <= supportedDelay {
			return supportedDelay
		}
	}

	return supportedDelays[len(supportedDelays)-1]
}
