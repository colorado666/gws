package timewheel

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lxzan/gws/metrics"
)

// 通过向 cmdCh 发送 cmd 的方式，添加/删除 task，实现无锁操作
type cmdType int

const (
	cmdAdd cmdType = iota
	cmdDel
)

type cmd struct {
	typ      cmdType
	clientID string
	connID   uint64
	conn     Connection
	timeout  int64 // 超时时间
}

type Wheel struct {
	opt Options

	// wheel core
	cur       int
	wheelSize int
	slots     [][]*task

	// 一个 client 只允许一个连接：tasks[clientID] 永远指向“当前有效 task”
	// slot 里可能残留旧 task（重连/重复 add），扫描时通过指针比对丢弃
	tasks map[string]*task

	// channels
	cmdCh     chan cmd
	timeoutCh chan TimeoutInfo

	// 池化处理：pools & workers
	taskPool sync.Pool
	workers  *workerPool

	// lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New 创建时间轮。Start 后会启动：
// 1) wheel 主 goroutine（处理 cmd、tick 扫描）
// 2) worker pool（消费 timeout 事件并调用 handler）
func NewWheel(parent context.Context, opt Options, handler TimeoutHandler) (*Wheel, error) {
	if handler == nil {
		return nil, errors.New("handler must not be nil")
	}
	opt.normalize()

	ctx, cancel := context.WithCancel(parent)

	w := &Wheel{
		opt:       opt,
		cur:       0,
		wheelSize: opt.SlotSize,
		slots:     make([][]*task, opt.SlotSize),
		tasks:     make(map[string]*task, opt.ExpectedConnections),
		cmdCh:     make(chan cmd, opt.CmdBuffer),
		timeoutCh: make(chan TimeoutInfo, opt.TimeoutBuffer),
		ctx:       ctx,
		cancel:    cancel,
	}

	// task pool
	w.taskPool.New = func() any { return &task{} }

	// slots 预分配：按 expectedConnections/wheelSize 估算每格容量
	w.preallocateSlots()

	// worker pool
	w.workers = newWorkerPool(ctx, opt.WorkerCount, w.timeoutCh, handler)

	return w, nil
}

func (w *Wheel) preallocateSlots() {
	exp := w.opt.ExpectedConnections
	if exp <= 0 {
		return
	}
	per := float64(exp) / float64(w.wheelSize)    // 一个 slot 平均需要容纳 task 数量
	capPer := int(per * w.opt.SlotCapacityFactor) // 乘以一个系数
	if capPer < 8 {
		capPer = 8
	}
	for i := range w.slots {
		w.slots[i] = make([]*task, 0, capPer)
	}
}

func (w *Wheel) Start() {
	w.wg.Add(1)
	go w.loop()
}

func (w *Wheel) Stop() {
	w.cancel()
	w.wg.Wait()

	// 关闭 eventCh 让 worker 退出
	close(w.timeoutCh)
	w.workers.stop()
}

// AddConnection：添加/替换某 client 的当前连接。
// 注意：调用方可在重连时先 Upsert ConnectionManage，再 AddConnection 到 timewheel。
func (w *Wheel) AddConnection(conn Connection, timeout int64) {
	w.cmdCh <- cmd{
		typ:      cmdAdd,
		clientID: conn.ClientID(),
		connID:   conn.ConnID(),
		conn:     conn,
		timeout:  timeout,
	}
	w.opt.Metrics.SetCmdQueueLen(len(w.cmdCh))
}

// DelConnection：可选（一般不需要）。仅当你希望“立即”让该 client 不再被 timewheel 管理时使用。
func (w *Wheel) DelConnection(clientID string, connID uint64) {
	w.cmdCh <- cmd{typ: cmdDel, clientID: clientID, connID: connID}
	w.opt.Metrics.SetCmdQueueLen(len(w.cmdCh))
}

func (w *Wheel) loop() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("timewheel panic: %v\n", err)
		}
	}()
	defer w.wg.Done()

	ticker := time.NewTicker(time.Duration(w.opt.Tick) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case c := <-w.cmdCh:
			w.handleCmd(c)
			w.opt.Metrics.SetCmdQueueLen(len(w.cmdCh))
		case now := <-ticker.C:
			start := time.Now()
			w.onTick(now)
			w.opt.Metrics.ObserveTickDuration(time.Since(start))
			w.opt.Metrics.SetEventQueueLen(len(w.timeoutCh))
		}
	}
}

func (w *Wheel) handleCmd(c cmd) {
	switch c.typ {
	case cmdAdd:
		old := w.tasks[c.clientID]
		if old != nil {
			// 当旧的存在时，应该删除
			if old.connID == c.connID {
				return
			}
			// connID 不同：重连（或 existing 已关闭），走替换逻辑
		}

		// 生成/复用 task
		t := w.getTask()
		t.clientID = c.clientID
		t.connID = c.connID
		t.conn = c.conn
		t.timeout = c.timeout
		t.rounds = 0

		w.tasks[c.clientID] = t

		// 初次调度：now + timeout
		nowTs := time.Now().Unix()
		w.scheduleAt(t, nowTs+c.timeout, nowTs)

	case cmdDel:
		cur := w.tasks[c.clientID]
		if cur != nil && cur.connID == c.connID {
			delete(w.tasks, c.clientID)
			w.putTask(cur)
		}
	}
}

func (w *Wheel) onTick(now time.Time) {
	// 前进一格
	w.cur = (w.cur + 1) % w.wheelSize

	// 取出当前 bucket 并清空 slot（避免持有旧引用；后续按需 reschedule）
	bucket := w.slots[w.cur]
	w.slots[w.cur] = w.slots[w.cur][:0]

	w.opt.Metrics.ObserveBucketSize(len(bucket))

	nowS := now.Unix()

	for _, t := range bucket {
		// stale：tasks[clientID] 指向的不是该 task，说明已重连/重复 add 覆盖
		if w.tasks[t.clientID] != t {
			w.opt.Metrics.IncInvalidOrStaleTaskDrop()
			w.putTask(t)
			continue
		}

		// rounds
		if t.rounds > 0 {
			t.rounds--
			w.slots[w.cur] = append(w.slots[w.cur], t)
			continue
		}

		// connection validity
		if t.conn == nil || t.conn.IsClosed() || t.conn.ConnID() != t.connID {
			// 当前 task 对应连接已无效：移除
			delete(w.tasks, t.clientID)
			w.opt.Metrics.IncInvalidOrStaleTaskDrop()
			w.putTask(t)
			continue
		}

		lastS := t.conn.LastRx()
		deadlineS := lastS + t.timeout

		if nowS >= deadlineS {
			// 到期：尝试投递 timeout 候选（不阻塞，不丢）
			if w.tryEnqueueTimeout(t, nowS) {
				// 投递成功：timewheel 不再管理此 task，后续由业务侧关闭连接/清理
				delete(w.tasks, t.clientID)
				w.putTask(t)
				continue
			}

			// eventCh 满：下一 tick 重试投递（期间若 lastRx 更新，则会自动回到未超时路径）
			w.scheduleAt(t, nowS+w.opt.Tick, nowS)
			continue
		}

		// 未到期：迁移到 deadline 对应 slot（保证离线发现精度≈tick）
		w.scheduleAt(t, deadlineS, nowS)
	}
}

func (w *Wheel) tryEnqueueTimeout(t *task, now int64) bool {
	cand := TimeoutInfo{
		ClientID: t.clientID,
		ConnID:   t.connID,
		Now:      now,
		Timeout:  t.timeout,
	}

	select {
	case w.timeoutCh <- cand:
		// 投递成功，清掉 pending 标志
		return true
	default:
		// 投递失败，进入下一轮 tick 再处理
		return false
	}
}

func (w *Wheel) scheduleAt(t *task, deadline int64, now int64) {
	diff := deadline - now
	if diff < 0 {
		diff = 0
	}

	// 至少 1 tick，避免安排到当前 slot 造成热循环
	ticks := int(diff / w.opt.Tick)
	if ticks < 1 {
		ticks = 1
	}

	slot := (w.cur + ticks) % w.wheelSize
	rounds := ticks / w.wheelSize

	t.rounds = int32(rounds)
	w.slots[slot] = append(w.slots[slot], t)
}

func (w *Wheel) getTask() *task {
	return w.taskPool.Get().(*task)
}

func (w *Wheel) putTask(t *task) {
	// reset
	t.clientID = ""
	t.connID = 0
	t.conn = nil
	t.rounds = 0
	t.timeout = 0
	w.taskPool.Put(t)
}

// Ensure imports used
var _ metrics.Recorder
