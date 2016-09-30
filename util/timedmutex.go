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
package util

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/syncutil"
)

type TimedMutex struct {
	mu       *syncutil.Mutex // intentionally pointer to make zero value unusable
	lockedAt time.Time       // protected by mu

	// Non-mutable fields.
	ctx       context.Context
	threshold time.Duration
}

// MakeTimedMutex returns a mutex which dumps a stack trace whenever a lock is
// unlocked after being held for longer than the supplied duration, which must
// be strictly positive.
func MakeTimedMutex(ctx context.Context, warnDuration time.Duration) TimedMutex {
	if warnDuration <= 0 {
		panic("duration must be positive")
	}
	return TimedMutex{ctx: ctx, threshold: warnDuration, mu: &syncutil.Mutex{}}
}

func (tm *TimedMutex) Lock() {
	tm.mu.Lock()
	tm.lockedAt = time.Now()
}

func (tm *TimedMutex) Unlock() {
	lockedAt := tm.lockedAt
	tm.mu.Unlock()

	if heldFor := time.Since(lockedAt); heldFor >= tm.threshold {
		log.Warningf(
			tm.ctx, "mutex held for %s (>%s):\n%s",
			heldFor, tm.threshold, debug.Stack(),
		)
		if tm.threshold <= 0 {
			log.Fatalf(tm.ctx, "must not use TimedMutex with nonpositive duration")
		}
	}
}
