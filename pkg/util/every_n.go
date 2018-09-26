// Copyright 2017 The Cockroach Authors.
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

package util

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// EveryN provides a way to rate limit spammy events. It tracks how recently a
// given event has occurred so that it can determine whether it's worth
// handling again.
//
// NOTE: If you specifically care about log messages, you should use the
// version of this in the log package, as it integrates with the verbosity
// flags.
type EveryN struct {
	// N is the minimum duration of time between log messages.
	N time.Duration

	syncutil.Mutex
	lastProcessed time.Time
}

// Every is a convenience constructor for an EveryN object that allows a log
// message every n duration.
func Every(n time.Duration) EveryN {
	return EveryN{N: n}
}

// ShouldProcess returns whether it's been more than N time since the last event.
func (e *EveryN) ShouldProcess(now time.Time) bool {
	var shouldProcess bool
	e.Lock()
	if now.Sub(e.lastProcessed) >= e.N {
		shouldProcess = true
		e.lastProcessed = now
	}
	e.Unlock()
	return shouldProcess
}
