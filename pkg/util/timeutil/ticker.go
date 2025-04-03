// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timeutil

import (
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// BroadcastTicker is a wrapper around a time.Ticker that delivers "ticks" of a
// clock to multiple receivers at roughly the same time. This allows a set of
// goroutines to synchronize on the same periodic schedule.
//
// The interface of BroadcastTicker and the receiver handles that it hands out
// is similar to that of time.Ticker, but not identical. In particular, ticker
// "receivers" are created from a parent BroadcastTicker.
//
//	bcastTicker := NewBroadcastTicker(1 * time.Second)
//	defer bcastTicker.Stop()
//	for range N {
//		go func() {
//			ticker := bcastTicker.NewTicker()
//			defer ticker.Stop()
//			for range ticker.C {
//				// Do something...
//			}
//		}()
//	}
type BroadcastTicker struct {
	ticker *time.Ticker
	stopC  chan struct{}
	mu     struct {
		syncutil.Mutex
		chans []chan time.Time
	}
}

// NewBroadcastTicker creates a new BroadcastTicker that broadcasts the current
// time after each tick. The period of the ticks is specified by the duration
// argument. The duration d must be greater than zero; if not, the function will
// panic.
func NewBroadcastTicker(d time.Duration) *BroadcastTicker {
	t := &BroadcastTicker{
		ticker: time.NewTicker(d),
		stopC:  make(chan struct{}),
	}
	go t.run()
	return t
}

func (t *BroadcastTicker) run() {
	for {
		select {
		case now := <-t.ticker.C:
			t.broadcast(now)
		case <-t.stopC:
			return
		}
	}
}

func (t *BroadcastTicker) broadcast(val time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range t.mu.chans {
		select {
		case c <- val:
		default:
		}
	}
}

// Stop turns off a broadcast ticker. After Stop, no more ticks will be sent to
// any of the receivers, even if Reset is later called.
//
// NOTE: this interaction between Stop and Reset could be adjusted if needed, so
// that a stopped ticker could be restarted with a Reset call.
func (t *BroadcastTicker) Stop() {
	t.ticker.Stop()
	close(t.stopC)
}

// Reset stops a broadcast ticker and resets its period to the specified
// duration. The next tick will be broadcast after the new period elapses. The
// duration d must be greater than zero; if not, the function will panic.
func (t *BroadcastTicker) Reset(d time.Duration) {
	t.ticker.Reset(d)
}

// NewTicker creates a new receiver for the broadcast ticker. The receiver
// contains a channel that will receive the current time after each tick.
func (t *BroadcastTicker) NewTicker() *BroadcastTickerReceiver {
	c, r := t.allocNewTicker()
	t.register(c)
	return r
}

func (t *BroadcastTicker) allocNewTicker() (chan time.Time, *BroadcastTickerReceiver) {
	c := make(chan time.Time, 1)
	r := &BroadcastTickerReceiver{
		C: c,
		t: t,
	}
	return c, r
}

func (t *BroadcastTicker) register(c chan time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.chans = append(t.mu.chans, c)
}

func (t *BroadcastTicker) registerLocked(c chan time.Time) {
	t.mu.chans = append(t.mu.chans, c)
}

func (t *BroadcastTicker) unregister(c <-chan time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.chans = slices.DeleteFunc(t.mu.chans, func(ch chan time.Time) bool {
		return ch == c
	})
}

// BroadcastTickerReceiver is a receiver of ticks from a BroadcastTicker. It is
// little more than a wrapper struct around a channel, but is made to look like
// a normal time.Ticker for ease of use.
type BroadcastTickerReceiver struct {
	C <-chan time.Time
	t *BroadcastTicker
}

// Stop stops the receiver from receiving ticks. The method does not stop the
// broadcast ticker itself.
func (r *BroadcastTickerReceiver) Stop() {
	r.t.unregister(r.C)
}
