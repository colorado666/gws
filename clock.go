package gws

import (
	"sync"
	"sync/atomic"
	"time"
)

// 全局低精度时间戳，单位秒，作为消息时间戳
var currentSec atomic.Int64
var clockOnce sync.Once

func NowSec() int64 {
	x := currentSec.Load()
	if x == 0 {
		return time.Now().Unix()
	}
	return x
}

func startClock() {
	currentSec.Store(time.Now().Unix())
	t := time.NewTicker(time.Second)
	go func() {
		for range t.C {
			currentSec.Store(time.Now().Unix())
		}
	}()
}

func init() {
	clockOnce.Do(startClock)
}
