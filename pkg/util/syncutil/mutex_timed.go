// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package syncutil

import "time"

// TimedMutex is a Mutex that helps record the duration of the critical section
// to a callback.
type TimedMutex struct {
	mu  Mutex
	t   time.Time
	now func() time.Time
	rec func(time.Time)
}

// NewTimedMutex returns a TimedMutex. `rec` will be invoked as part of Unlock
// after having released the mutex, with the timestamp taken right after the
// mutex was acquired. `rec` can perform "expensive" work without delaying
// anyone but the caller of Unlock.
func NewTimedMutex(now func() time.Time, rec func(time.Time)) *TimedMutex {
	return &TimedMutex{
		now: now,
		rec: rec,
	}
}

// Lock locks the mutex.
func (tm *TimedMutex) Lock() {
	tm.mu.Lock()
	tm.t = tm.now()
}

// Unlock unlocks the mutex.
func (tm *TimedMutex) Unlock() {
	tm.mu.Unlock()
	tm.rec(tm.t)
}

// AssertHeld asserts that the mutex is held (by someone, not
// necessarily but usually the caller). See Mutex.AssertHeld.
func (tm *TimedMutex) AssertHeld() {
	tm.mu.AssertHeld()
}
