// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
