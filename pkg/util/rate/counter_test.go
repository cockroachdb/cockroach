// Copyright 2018 The Cockroach Authors.
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

package rate

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestCounter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var ctr Counter
	// Add values until we see a rate.
	for ctr.Rate() == 0 {
		ctr.Add(1024)
		time.Sleep(100 * time.Microsecond)
	}

	snapshot := ctr.Rate()

	// Now wait until we see some decay.
	for snapshot == ctr.Rate() {
		time.Sleep(100 * time.Microsecond)
	}

	ctr.Freeze()
	if !ctr.IsFrozen() {
		t.Error("should have been frozen")
	}

	ctr.Reset()
	if ctr.Rate() != 0 {
		t.Error("expected 0 rate")
	}
	if ctr.IsFrozen() {
		t.Error("expected to be unfrozen")
	}
}

func TestChannelCounter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ch := make(chan int)
	defer close(ch)

	ctr := NewChannelRater(ch)
	// Add values until we see a rate.
	for ctr.Rate() == 0 {
		ch <- 1024
		time.Sleep(100 * time.Microsecond)
	}
}
