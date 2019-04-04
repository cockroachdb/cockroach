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
	w := NewStopWatch()

	w.Start()
	time.Sleep(250 * time.Millisecond)
	w.Start()
	time.Sleep(250 * time.Millisecond)

	assertWithinHundredMilliseconds(t, 500*time.Millisecond, w.Stop())
}

// TestStopWatchStop makes sure that first call to Stop returns zero
// duration while the first one
func TestStopWatchStop(t *testing.T) {
	w := NewStopWatch()

	w.Start()
	time.Sleep(250 * time.Millisecond)

	assertWithinHundredMilliseconds(t, 250*time.Millisecond, w.Stop())

	time.Sleep(250 * time.Millisecond)
	secondStop := w.Stop()
	if secondStop != 0 {
		t.Fatal("consequent call to StopWatch.Stop returned non-zero duration")
	}
}

// assertWithinHundredMilliseconds asserts that expected and actual differ by no
// more than 100 ms.
func assertWithinHundredMilliseconds(t *testing.T, expected time.Duration, actual time.Duration) {
	diff := expected - actual
	if diff < -100*time.Millisecond || diff > 100*time.Millisecond {
		t.Fatalf("duration difference is more than 100 ms: expected %v, but actual %v", expected, actual)
	}
}
