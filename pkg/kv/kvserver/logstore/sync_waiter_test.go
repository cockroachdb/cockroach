// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	w.enqueue(ctx, wg1, func() { close(c) })

	// Callback is not called before SyncWait completes.
	select {
	case <-c:
		t.Fatal("callback unexpectedly called before SyncWait")
	case <-time.After(5 * time.Millisecond):
	}

	// Callback is called after SyncWait completes.
	close(wg1)
	<-c

	// Enqueue a waiter once the loop is stopped. Enqueuing should not block.
	// NB: stopper.Stop waits for the waitLoop to exit.
	stopper.Stop(ctx)
	wg2 := make(chanSyncWaiter)
	w.enqueue(ctx, wg2, func() { t.Fatalf("callback unexpectedly called") })

	// Callback should not be called, even after SyncWait completes.
	time.Sleep(5 * time.Millisecond) // give time to catch bugs
	close(wg2)
	time.Sleep(5 * time.Millisecond) // give time to catch bugs
}

// chanSyncWaiter implements the syncWaiter interface.
type chanSyncWaiter chan struct{}

func (c chanSyncWaiter) SyncWait() error {
	<-c
	return nil
}

func (c chanSyncWaiter) Close() {}
