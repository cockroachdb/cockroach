// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstore

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

func TestSyncWaiterLoop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	w := NewSyncWaiterLoop()
	w.Start(ctx, stopper)

	// Enqueue a waiter while the loop is running.
	c := make(chan struct{})
	wg1 := make(chanSyncWaiter)
	cb1 := funcSyncWaiterCallback(func() { close(c) })
	w.enqueue(ctx, wg1, cb1)

	// Callback is not called before SyncWait completes.
	select {
	case <-c:
		t.Fatal("callback unexpectedly called before SyncWait")
	case <-time.After(5 * time.Millisecond):
	}

	// Callback is called after SyncWait completes.
	close(wg1)
	<-c

	// Enqueue a waiter once the loop is stopped. Enqueuing should not block,
	// regardless of how many times it is called.
	stopper.Stop(ctx)
	wg2 := make(chanSyncWaiter)
	cb2 := funcSyncWaiterCallback(func() { t.Fatalf("callback unexpectedly called") })
	for i := 0; i < 2*cap(w.q); i++ {
		w.enqueue(ctx, wg2, cb2)
	}

	// Callback should not be called, even after SyncWait completes.
	// NB: stopper.Stop waits for the waitLoop to exit.
	time.Sleep(5 * time.Millisecond) // give time to catch bugs
	close(wg2)
	time.Sleep(5 * time.Millisecond) // give time to catch bugs
}

func BenchmarkSyncWaiterLoop(b *testing.B) {
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	w := NewSyncWaiterLoop()
	w.Start(ctx, stopper)

	// Pre-allocate a syncWaiter, notification channel, and callback function that
	// can all be re-used across benchmark iterations so we can isolate the
	// performance of operations inside the SyncWaiterLoop.
	wg := make(chanSyncWaiter)
	c := make(chan struct{})
	cb := funcSyncWaiterCallback(func() { c <- struct{}{} })

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.enqueue(ctx, wg, cb)
		wg <- struct{}{}
		<-c
	}
}

// chanSyncWaiter implements the syncWaiter interface.
type chanSyncWaiter chan struct{}

func (c chanSyncWaiter) SyncWait() error {
	<-c
	return nil
}

func (c chanSyncWaiter) Close() {}

// funcSyncWaiterCallback implements the syncWaiterCallback interface.
type funcSyncWaiterCallback func()

func (f funcSyncWaiterCallback) run() { f() }
