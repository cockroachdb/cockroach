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
	"fmt"
	"testing"

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

	expected, actual := timeSource.Elapsed(), w.Stop()
	require.Equal(t, expected, actual, fmt.Sprintf("durations are not equal: expected %v, but actual %v", expected, actual))
}

// TestStopWatchStop makes sure that first call to Stop returns actual elapsed
// duration while the consequent calls return 0.
func TestStopWatchStop(t *testing.T) {
	timeSource := NewTestTimeSource()
	w := newStopWatch(timeSource.Now)

	w.Start()
	timeSource.Advance()

	expected, actual := timeSource.Elapsed(), w.Stop()
	require.Equal(t, expected, actual, fmt.Sprintf("durations are not equal: expected %v, but actual %v", expected, actual))

	timeSource.Advance()
	secondStop := w.Stop()
	if secondStop != 0 {
		t.Fatal("consequent call to StopWatch.Stop returned non-zero duration")
	}
}
