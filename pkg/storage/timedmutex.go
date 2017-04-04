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

package storage

import (
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"

	"golang.org/x/net/context"
)

var enableTimedMutex = envutil.EnvOrDefaultBool("COCKROACH_ENABLE_TIMED_MUTEX", false)

// timedMutex is a mutex which dumps a stack trace via the supplied callback
// whenever a lock is unlocked after having been held for longer than the
// supplied duration, which must be strictly positive.
type timedMutex struct {
	mu       *syncutil.Mutex // intentionally pointer to make zero value unusable
	lockedAt time.Time       // protected by mu
	isLocked int32           // updated atomically

	// Non-mutable fields.
	cb timingFn
}

// timingFn is a callback passed to MakeTimedMutex. It is invoked with the
// measured duration of the critical section of the associated mutex after
// each Unlock operation.
type timingFn func(heldFor time.Duration)

// thresholdLogger returns a timing function which calls 'record' for each
// measured duration and, for measurements exceeding 'warnDuration', invokes
// 'printf' with the passed context and a detailed stack trace.
func thresholdLogger(
	ctx context.Context,
	warnDuration time.Duration,
	printf func(context.Context, string, ...interface{}),
) timingFn {
	return func(heldFor time.Duration) {
		if heldFor > warnDuration {
			// NB: this doesn't use `util/caller.Lookup` because that would result
			// in an import cycle.
			fun := "?"
			if pc, _, _, ok := runtime.Caller(2); ok {
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

// makeTimedMutex creates a TimedMutex which warns when an Unlock happens more
// than warnDuration after the corresponding lock. If non-nil, it will invoke
// the supplied timingFn callback after each unlock with the elapsed time that
// the mutex was held for.
func makeTimedMutex(cb timingFn) timedMutex {
	if !enableTimedMutex {
		cb = nil
	}
	return timedMutex{
		cb: cb,
		mu: &syncutil.Mutex{},
	}
}

// Lock implements sync.Locker.
func (tm *timedMutex) Lock() {
	tm.mu.Lock()
	atomic.StoreInt32(&tm.isLocked, 1)
	if tm.cb != nil {
		tm.lockedAt = timeutil.Now()
	}
}

// Unlock implements sync.Locker.
func (tm *timedMutex) Unlock() {
	lockedAt := tm.lockedAt
	atomic.StoreInt32(&tm.isLocked, 0)
	tm.mu.Unlock()
	if tm.cb != nil {
		tm.cb(timeutil.Since(lockedAt))
	}
}

// AssertHeld may panic if the mutex is not locked (but it is not
// required to do so). Functions which require that their callers hold
// a particular lock may use this to enforce this requirement more
// directly than relying on the race detector.
//
// Note that we do not require the lock to be held by any particular
// thread, just that some thread holds the lock. This is both more
// efficient and allows for rare cases where a mutex is locked in one
// thread and used in another.
//
// TODO(bdarnell): Add an equivalent method to syncutil.Mutex
// (possibly a no-op depending on a build tag)
func (tm *timedMutex) AssertHeld() {
	isLocked := atomic.LoadInt32(&tm.isLocked)
	if isLocked == 0 {
		panic("mutex is not locked")
	}
}
