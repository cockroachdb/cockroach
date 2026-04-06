// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timeutil

import (
	"testing"
	"time"
)

// TestSyncTestClearsTimerPool verifies that SyncTest bypasses the timer
// pool so that bubble code always gets fresh timers. Without the bypass,
// a timer pooled outside the bubble would prevent the bubble's fake
// clock from ever advancing.
func TestSyncTestClearsTimerPool(t *testing.T) {
	// Pool a real-clock timer outside the bubble.
	var outer Timer
	outer.Reset(time.Hour)
	outer.Stop()

	SyncTest(t, func(t *testing.T) {
		var inner Timer
		inner.Reset(time.Second)

		// The pool is bypassed, so inner got a fresh bubble timer.
		// Sleeping advances the bubble's fake clock and the timer fires.
		time.Sleep(time.Second)
		<-inner.C
		inner.Stop()
	})
}
