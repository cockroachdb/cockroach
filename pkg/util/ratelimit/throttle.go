// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ratelimit

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type Options struct {
	// Interval defines the interval that rate limits the function calls.
	Interval time.Duration
	// Leading executes the function immediately on the first call, then
	// suppresses further calls until the interval has passed
	Leading bool
	// Trailing executes the latest function call after the interval has passed
	// since the last call.
	Trailing bool
	// Clock is used to control the time source for the retry loop. Intended for
	// testing purposes. Should be nil in production code.
	Clock timeutil.TimeSource
}

type Throttler struct {
	mu syncutil.Mutex

	ctx  context.Context
	opts Options

	pendingFn  func()
	onCooldown bool
	timer      timeutil.TimerI
	waitCh     chan<- struct{}
}

func setDefaults(opts Options) Options {
	if opts.Interval == 0 {
		opts.Interval = 100 * time.Millisecond
	}
	if !opts.Leading && !opts.Trailing {
		opts.Trailing = true
	}
	if opts.Clock == nil {
		opts.Clock = timeutil.DefaultTimeSource{}
	}
	return opts
}

func NewThrottler(ctx context.Context, opts Options) *Throttler {
	opts = setDefaults(opts)
	return &Throttler{
		ctx:   ctx,
		opts:  opts,
		timer: opts.Clock.NewTimer(),
	}
}

// Call executes the provided function while respecting to the throttling
// options.
func (t *Throttler) Call(f func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ctx.Err() != nil {
		return
	}

	if !t.onCooldown {
		if t.opts.Leading {
			f()
			t.startCooldown()
		} else if t.opts.Trailing {
			t.pendingFn = f
			t.startCooldown()
		}
		return
	} else if t.opts.Trailing {
		t.pendingFn = f
		return
	}
}

// startCooldown starts the cooldown timer if it is not already running. It does
// not grab the lock, so it should be called from within a locked context.
func (t *Throttler) startCooldown() {
	if t.onCooldown {
		return
	}
	t.timer.Reset(t.opts.Interval)
	t.onCooldown = true

	go func() {
		select {
		case <-t.ctx.Done():
			t.Reset()
		case <-t.timer.Ch():
			t.mu.Lock()
			defer t.mu.Unlock()
			t.onCooldown = false

			if t.opts.Trailing && t.pendingFn != nil {
				t.pendingFn()
				t.pendingFn = nil
				t.startCooldown()
			} else {
				if t.waitCh != nil {
					close(t.waitCh)
					t.waitCh = nil
				}
				t.timer.Stop()
			}
		}
	}()
}

// Reset stops the throttler and clears any pending function.
func (t *Throttler) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.waitCh != nil {
		close(t.waitCh)
		t.waitCh = nil
	}
	t.onCooldown = false
	t.pendingFn = nil
	t.timer.Stop()
}

// WaitForCooldown waits for the throttler to finish any pending function and
// fully cooldown before closing the provided channel.
func (t *Throttler) WaitForCooldown(waitCh chan<- struct{}) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.onCooldown {
		close(waitCh)
	} else {
		t.waitCh = waitCh
	}
}
