// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eventagg

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestWindowedFlush(t *testing.T) {
	window := 5 * time.Minute
	now := timeutil.Now()
	// Make sure we're not directly on the window boundary.
	if now.Truncate(window).Equal(now) {
		now = now.Add(1 * time.Minute)
	}
	flush := NewWindowedFlush(window, func() time.Time {
		return now
	})
	// Initially, we should be within the current window.
	require.False(t, flush.shouldFlush())
	// If we fast-forward to the beginning of the next window, we expect
	// a flush to be triggered.
	now = now.Add(window).Truncate(window)
	require.True(t, flush.shouldFlush())
	// Times occurring before the current window's end should not trigger a flush.
	now = now.Add(1 * time.Minute)
	require.False(t, flush.shouldFlush())
}
