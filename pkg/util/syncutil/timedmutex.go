// Copyright 2016 The Cockroach Authors.
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

package syncutil

import (
	"runtime"
	"runtime/debug"
	"time"

	"golang.org/x/net/context"
)

// TimedMutex is a mutex which dumps a stack trace via the supplied callback
// whenever a lock is unlocked after having been held for longer than the
// supplied duration, which must be strictly positive.
type TimedMutex struct {
	mu       *Mutex    // intentionally pointer to make zero value unusable
	lockedAt time.Time // protected by mu

	// Non-mutable fields.
	cb TimingFn
}

// TimingFn is a callback passed to MakeTimedMutex. It is invoked with the
// measured duration of the critical section of the associated mutex after
// each Unlock operation.
type TimingFn func(heldFor time.Duration)

// ThresholdLogger returns a timing function which calls 'record' for each
// measured duration and, for measurements exceeding 'warnDuration', invokes
// 'printf' with the passed context and a detailed stack trace.
func ThresholdLogger(
	ctx context.Context,
	warnDuration time.Duration,
	printf func(context.Context, string, ...interface{}),
	record TimingFn,
) TimingFn {
	return func(heldFor time.Duration) {
		record(heldFor)
		if heldFor > warnDuration {
			// NB: this doesn't use `util/caller.Lookup` because that would result
			// in an import cycle.
			pc, _, _, ok := runtime.Caller(2)
			fun := "?"
			if ok {
				if f := runtime.FuncForPC(pc); f != nil {
					fun = f.Name()
				}
			}

			printf(
				ctx, "mutex held by %s for %s (>%s):\n%s",
				fun, heldFor, warnDuration, debug.Stack(),
			)
		}
	}
}

// MakeTimedMutex creates a TimedMutex which warns when an Unlock happens more
// than warnDuration after the corresponding lock. It will use the supplied
// context and logging callback for the warning message; a nil logger falls
// back to Fprintf to os.Stderr.
func MakeTimedMutex(cb TimingFn) TimedMutex {
	return TimedMutex{
		cb: cb,
		mu: &Mutex{},
	}
}

// Lock implements sync.Locker.
func (tm *TimedMutex) Lock() {
	tm.mu.Lock()
	tm.lockedAt = time.Now()
}

// Unlock implements sync.Locker.
func (tm *TimedMutex) Unlock() {
	lockedAt := tm.lockedAt
	tm.mu.Unlock()
	tm.cb(time.Since(lockedAt))
}
