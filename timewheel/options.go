package timewheel

import (
	"github.com/lxzan/gws/metrics"
)

type Options struct {
	Tick      int64 // 轮询时间间隔，单位秒
	Timeout   int64 // 心跳超时时间，单位秒
	WheelSize int   // <=0 则使用 Timeout/Tick 的近似值（至少 1）

	// 队列容量
	CmdBuffer     int // add/del 命令队列， 考虑到 add 事件峰值可能很大，建议设置为最大并发的 10%以上
	TimeoutBuffer int // timeout 事件队列（供 worker pool 消费），建议设置为最大并发的 1%

	// Worker pool
	WorkerCount int // 处理 timeout 事件的 worker 数量，建议设置为 CPU 核心数的 2-4 倍

	// 预分配：期望管理的连接数（用于估算每个 slot 的容量）
	ExpectedConnections int
	SlotCapacityFactor  float64 // 默认 1.3，给一些冗余，避免频繁扩容

	// 指标
	Metrics metrics.Recorder
}

func (o *Options) normalize() {
	if o.Tick <= 0 {
		o.Tick = 3 //默认3秒
	}
	if o.Timeout <= 0 {
		o.Timeout = 360 //默认360秒
	}
	if o.WheelSize <= 0 {
		o.WheelSize = int(o.Timeout / o.Tick)
		if o.WheelSize < 1 {
			o.WheelSize = 1
		}
	}

	// 默认值
	if o.CmdBuffer <= 0 {
		o.CmdBuffer = 16 * 1024
	}
	if o.TimeoutBuffer <= 0 {
		o.TimeoutBuffer = 16 * 1024
	}
	if o.WorkerCount <= 0 {
		o.WorkerCount = 8
	}

	if o.ExpectedConnections <= 0 {
		o.ExpectedConnections = 1024
	}

	if o.SlotCapacityFactor <= 0 {
		o.SlotCapacityFactor = 1.3
	}
	if o.Metrics == nil {
		o.Metrics = metrics.NopRecorder{}
	}
}
