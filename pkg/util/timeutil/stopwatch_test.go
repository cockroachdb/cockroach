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
)

// TestStopWatchStart makes sure that consequent calls to Start do not reset
// the internal startedAt time.
func TestStopWatchStart(t *testing.T) {
	timeSource := newTestTimeSource()
	w := newStopWatch(timeSource.now)

	w.Start()
	timeSource.advance()
	w.Start()
	timeSource.advance()

	assertEqual(t, timeSource.elapsed(), w.Stop())
}

// TestStopWatchStop makes sure that first call to Stop returns actual elapsed
// duration while the consequent calls return 0.
func TestStopWatchStop(t *testing.T) {
	timeSource := newTestTimeSource()
	w := newStopWatch(timeSource.now)

	w.Start()
	timeSource.advance()

	assertEqual(t, timeSource.elapsed(), w.Stop())

	timeSource.advance()
	secondStop := w.Stop()
	if secondStop != 0 {
		t.Fatal("consequent call to StopWatch.Stop returned non-zero duration")
	}
}

func assertEqual(t *testing.T, expected time.Duration, actual time.Duration) {
	if expected != actual {
		t.Fatalf("durations are not equal: expected %v, but actual %v", expected, actual)
	}
}

type testTimeSource struct {
	initTime time.Time
	counter  int64
}

func newTestTimeSource() *testTimeSource {
	return &testTimeSource{initTime: Now()}
}

func (t *testTimeSource) now() time.Time {
	return t.initTime.Add(time.Duration(t.counter))
}

func (t *testTimeSource) advance() {
	t.counter++
}

func (t *testTimeSource) elapsed() time.Duration {
	return time.Duration(t.counter)
}
