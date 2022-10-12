package backoff

import (
	"context"
	"sync"
	"time"
)

type controller struct {
	ctx        context.Context
	cancel     func()
	ig         IntervalGenerator
	maxRetries int
	mu         *sync.RWMutex
	next       chan struct{} // user-facing channel
	resetTimer chan time.Duration
	retries    int
	timer      *time.Timer
}

func newController(ctx context.Context, ig IntervalGenerator, options ...ControllerOption) *controller {
	cctx, cancel := context.WithCancel(ctx) // DO NOT fire this cancel here

	maxRetries := 10
	for _, option := range options {
		switch option.Ident() {
		case identMaxRetries{}:
			maxRetries = option.Value().(int)
		}
	}

	c := &controller{
		cancel:     cancel,
		ctx:        cctx,
		ig:         ig,
		maxRetries: maxRetries,
		mu:         &sync.RWMutex{},
		next:       make(chan struct{}, 1),
		resetTimer: make(chan time.Duration, 1),
		timer:      time.NewTimer(ig.Next()),
	}

	// enqueue a single fake event so the user gets to retry once
	c.next <- struct{}{}

	go c.loop()
	return c
}

func (c *controller) loop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case d := <-c.resetTimer:
			if !c.timer.Stop() {
				select {
				case <-c.timer.C:
				default:
				}
			}
			c.timer.Reset(d)
		case <-c.timer.C:
			select {
			case <-c.ctx.Done():
				return
			case c.next <- struct{}{}:
			}
			if c.maxRetries > 0 {
				c.retries++
			}

			if !c.check() {
				c.cancel()
				return
			}
			c.resetTimer <- c.ig.Next()
		}
	}
}

func (c *controller) check() bool {
	if c.maxRetries > 0 && c.retries >= c.maxRetries {
		return false
	}
	return true
}

func (c *controller) Done() <-chan struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ctx.Done()
}

func (c *controller) Next() <-chan struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.next
}
