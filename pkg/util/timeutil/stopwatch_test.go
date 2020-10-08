// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package timeutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestStopWatchStart makes sure that consequent calls to Start do not reset
// the internal startedAt time.
func TestStopWatchStart(t *testing.T) {
	timeSource := NewTestTimeSource()
	w := newStopWatch(timeSource.Now)

	w.Start()
	timeSource.Advance()
	w.Start()
	timeSource.Advance()
	w.Stop()

	expected, actual := timeSource.Elapsed(), w.Elapsed()
	require.Equal(t, expected, actual)
}

// TestStopWatchStop makes sure that only the first call to Stop changes the
// state of the stop watch.
func TestStopWatchStop(t *testing.T) {
	timeSource := NewTestTimeSource()
	w := newStopWatch(timeSource.Now)

	w.Start()
	timeSource.Advance()
	w.Stop()

	expected, actual := timeSource.Elapsed(), w.Elapsed()
	require.Equal(t, expected, actual)

	timeSource.Advance()
	w.Stop()
	require.Equal(t, actual, w.Elapsed(), "consequent call to StopWatch.Stop changed the elapsed time")
}

// TestStopWatchElapsed makes sure that the stop watch records the elapsed time
// correctly.
func TestStopWatchElapsed(t *testing.T) {
	timeSource := NewTestTimeSource()
	w := newStopWatch(timeSource.Now)
	expected := time.Duration(10)

	w.Start()
	for i := int64(0); i < int64(expected); i++ {
		timeSource.Advance()
	}
	w.Stop()

	require.Equal(t, expected, w.Elapsed())
}
