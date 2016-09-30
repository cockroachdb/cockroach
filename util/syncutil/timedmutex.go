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
	"fmt"
	"os"
	"runtime/debug"
	"time"

	"golang.org/x/net/context"
)

var opts struct {
	mu  Mutex
	log func(context.Context, string, ...interface{})
}

// SetLogger changes the function which will be used by TimedMutex when
// warning about an above-threshold invocation.
func SetLogger(fn func(context.Context, string, ...interface{})) {
	opts.mu.Lock()
	defer opts.mu.Unlock()
	opts.log = fn
}

func init() {
	SetLogger(func(_ context.Context, msg string, args ...interface{}) {
		fmt.Fprintf(os.Stderr, msg, args...)
	})
}

// TimedMutex is a mutex which dumps a stack trace whenever a lock is unlocked
// after being held for longer than the supplied duration, which must be
// strictly positive.
type TimedMutex struct {
	mu       *Mutex    // intentionally pointer to make zero value unusable
	lockedAt time.Time // protected by mu

	// Non-mutable fields.
	ctx       context.Context
	threshold time.Duration
}

// MakeTimedMutex creates a TimedMutex which warns when an Unlock happens more
// than warnDuration after the corresponding lock. It will use the supplied
// context for the warning message; see SetLogger().
func MakeTimedMutex(ctx context.Context, warnDuration time.Duration) TimedMutex {
	if warnDuration <= 0 {
		panic("duration must be positive")
	}
	return TimedMutex{ctx: ctx, threshold: warnDuration, mu: &Mutex{}}
}

// Lock implements sync.Locker
func (tm *TimedMutex) Lock() {
	tm.mu.Lock()
	tm.lockedAt = time.Now()
}

// Unlock implements sync.Locker
func (tm *TimedMutex) Unlock() {
	lockedAt := tm.lockedAt
	tm.mu.Unlock()

	if heldFor := time.Since(lockedAt); heldFor >= tm.threshold {
		opts.mu.Lock()
		log := opts.log
		opts.mu.Unlock()
		log(
			tm.ctx, "mutex held for %s (>%s):\n%s",
			heldFor, tm.threshold, debug.Stack(),
		)
		if tm.threshold <= 0 {
			panic("must not use TimedMutex with nonpositive duration")
		}
	}
}
