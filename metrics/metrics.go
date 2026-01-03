package metrics

import "time"

// Recorder 是指标钩子接口：你可以接 Prometheus、StatsD、OTel 等。
// 该接口刻意保持“小而清晰”，避免强耦合具体指标系统。
type Recorder interface {
	// Tick
	ObserveTickDuration(d time.Duration)
	ObserveBucketSize(n int)

	// Queues
	SetCmdQueueLen(n int)
	SetEventQueueLen(n int)

	// Drops/Backpressure (注意：我们不丢事件，这里统计的是“队列满导致重试”的次数)
	IncCmdQueueBackpressure()   // cmd 发送侧阻塞通常由调用方承担；此处预留
	IncEventQueueFullRetry()    // timeout 事件队列满，触发重试调度
	IncInvalidOrStaleTaskDrop() // 旧任务/无效任务被丢弃次数
}

// NopRecorder 默认空实现，便于在无指标系统时直接使用。
type NopRecorder struct{}

func (NopRecorder) ObserveTickDuration(time.Duration) {}
func (NopRecorder) ObserveBucketSize(int)             {}
func (NopRecorder) SetCmdQueueLen(int)                {}
func (NopRecorder) SetEventQueueLen(int)              {}
func (NopRecorder) IncCmdQueueBackpressure()          {}
func (NopRecorder) IncEventQueueFullRetry()           {}
func (NopRecorder) IncInvalidOrStaleTaskDrop()        {}
