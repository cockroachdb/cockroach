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

package log

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// EveryN provides a way to rate limit spammy log messages. It tracks how
// recently a given log message has been emitted so that it can determine
// whether it's worth logging again.
type EveryN struct {
	// N is the minimum duration of time between log messages.
	N time.Duration

	syncutil.Mutex
	lastLog time.Time
}

// Every is a convenience constructor for an EveryN object that allows a log
// message every n duration.
func Every(n time.Duration) EveryN {
	return EveryN{N: n}
}

// ShouldLog returns whether it's been more than N time since the last event.
func (e *EveryN) ShouldLog() bool {
	return e.shouldLog(timeutil.Now())
}

func (e *EveryN) shouldLog(now time.Time) bool {
	if VDepth(2 /* level */, 2 /* depth */) {
		// Always log when high verbosity is desired.
		return true
	}
	var shouldLog bool
	e.Lock()
	if now.Sub(e.lastLog) >= e.N {
		shouldLog = true
		e.lastLog = now
	}
	e.Unlock()
	return shouldLog
}
