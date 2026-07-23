// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	shouldFlush, meta := flush.shouldFlush()
	require.False(t, shouldFlush)
	require.Equal(t, AggInfo{}, meta)
	// If we fast-forward to the beginning of the next window, we expect
	// a flush to be triggered.
	now = now.Add(window).Truncate(window)
	shouldFlush, meta = flush.shouldFlush()
	require.True(t, shouldFlush)
	require.Equal(t, AggInfo{
		Kind:      Windowed,
		StartTime: now.Add(-window).Truncate(window).UnixNano(),
		EndTime:   now.UnixNano(),
	}, meta)
	// Times occurring before the current window's end should not trigger a flush.
	now = now.Add(1 * time.Minute)
	shouldFlush, meta = flush.shouldFlush()
	require.False(t, shouldFlush)
	require.Equal(t, AggInfo{}, meta)
}
