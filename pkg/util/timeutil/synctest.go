// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timeutil

import (
	"sync"
	"testing"
	"testing/synctest"
)

// SyncTest is a wrapper around synctest.Test that clears the timer pool
// before entering and after exiting the bubble. This prevents non-bubble
// timers from being reused inside the bubble (which would prevent the
// bubble's fake clock from advancing) and prevents bubble timers from
// leaking into the real world afterward.
//
// All synctest usage should go through this function.
func SyncTest(t *testing.T, f func(*testing.T)) {
	timeTimerPool = sync.Pool{}
	synctest.Test(t, func(t *testing.T) {
		t.Cleanup(func() {
			timeTimerPool = sync.Pool{}
		})
		f(t)
	})
}
