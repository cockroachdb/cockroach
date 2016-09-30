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
	"runtime/debug"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine/log"
)

type timedMutex struct {
	Mutex
	lockedAt  time.Time
	threshold time.Duration
}

// NewTimedMutex returns a mutex which dumps a stack trace whenever a lock is
// unlocked after being held for longer than the supplied duration. A zero
// duration returns a syncutil.Mutex without the overhead for the timing
// instrumentation.
func NewTimedMutex(warnDuration time.Duration) sync.Locker {
	if warnDuration == 0 {
		return &Mutex{}
	}
	return &timedMutex{threshold: warnDuration}
}

func (tm *timedMutex) Lock() {
	tm.Mutex.Lock()
	tm.lockedAt = time.Now()
}

func (tm *timedMutex) Unlock() {
	tm.Mutex.Unlock()
	if heldFor := time.Since(tm.lockedAt); heldFor >= tm.threshold {
		stack := debug.Stack()
		log.Warningf(
			context.Background(),
			"mutex held for %s (>%s):\n%s",
			heldFor, tm.threshold, stack,
		)
	}
}
