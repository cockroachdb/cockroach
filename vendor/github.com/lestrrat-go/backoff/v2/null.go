package backoff

import (
	"context"
	"sync"
)

// NullPolicy does not do any backoff. It allows the caller
// to execute the desired code once, and no more
type NullPolicy struct{}

func NewNull() *NullPolicy {
	return &NullPolicy{}
}

func (p *NullPolicy) Start(ctx context.Context) Controller {
	return newNullController(ctx)
}

type nullController struct {
	mu   *sync.RWMutex
	ctx  context.Context
	next chan struct{}
}

func newNullController(ctx context.Context) *nullController {
	cctx, cancel := context.WithCancel(ctx)
	c := &nullController{
		mu:   &sync.RWMutex{},
		ctx:  cctx,
		next: make(chan struct{}), // NO BUFFER
	}
	go func(ch chan struct{}, cancel func()) {
		ch <- struct{}{}
		close(ch)
		cancel()
	}(c.next, cancel)
	return c
}

func (c *nullController) Done() <-chan struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ctx.Done()
}

func (c *nullController) Next() <-chan struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.next
}
