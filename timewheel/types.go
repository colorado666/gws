package timewheel

import (
	"context"
)

// Connection 是 timewheel 需要访问的连接最小能力集合。
// 你可以让 connmgr.connection 实现该接口。
type Connection interface {
	ClientID() string
	ConnID() uint64

	LastRxUnix() int64
	IsClosed() bool
}

// TimeoutCandidate 是 timewheel 产生的“超时事件”。
// 业务侧处理时必须二次校验（当前 connID、lastRx 是否仍超时），防止误判。
type TimeoutCandidate struct {
	ClientID string
	ConnID   uint64
	Now      int64 // 当前时间，单位秒
	Timeout  int64 // 心跳超时时间，单位秒
}

// TimeoutHandler 由业务侧实现：例如 ConnectionManage.HandleTimeoutCandidate。
type TimeoutHandler interface {
	HandleTimeout(ctx context.Context, c TimeoutCandidate)
}

// ---- internal task types ----

type task struct {
	clientID string
	connID   uint64
	conn     Connection

	rounds int32
}
