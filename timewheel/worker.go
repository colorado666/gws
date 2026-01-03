package timewheel

import (
	"context"
	"sync"
)

type workerPool struct {
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	handler TimeoutHandler

	events <-chan TimeoutInfo
}

func newWorkerPool(parent context.Context, n int, events <-chan TimeoutInfo, h TimeoutHandler) *workerPool {
	ctx, cancel := context.WithCancel(parent)
	p := &workerPool{
		ctx:     ctx,
		cancel:  cancel,
		handler: h,
		events:  events,
	}
	p.wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer p.wg.Done()
			for {
				select {
				case <-p.ctx.Done():
					return
				case c, ok := <-p.events:
					if !ok {
						return
					}
					p.handler.HandleTimeout(p.ctx, c)
				}
			}
		}()
	}
	return p
}

func (p *workerPool) stop() {
	p.cancel()
	p.wg.Wait()
}
