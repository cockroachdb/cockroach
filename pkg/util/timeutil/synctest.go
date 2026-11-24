// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timeutil

import (
	"testing"
	"testing/synctest"
)

// SyncTest is a wrapper around synctest.Test that disables the timer
// pool for the duration of the bubble. See activeSynctestBubbles.
//
// All synctest usage should go through this function.
func SyncTest(t *testing.T, f func(*testing.T)) {
	activeSynctestBubbles.Add(1)
	synctest.Test(t, func(t *testing.T) {
		t.Cleanup(func() {
			activeSynctestBubbles.Add(-1)
		})
		f(t)
	})
}
