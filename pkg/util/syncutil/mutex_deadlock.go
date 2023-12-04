// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build deadlock
// +build deadlock

package syncutil

import (
	"time"

	deadlock "github.com/sasha-s/go-deadlock"
)

// DeadlockEnabled is true if the deadlock detector is enabled.
const DeadlockEnabled = true

func init() {
	deadlock.Opts.DeadlockTimeout = 5 * time.Minute
}

// A Mutex is a mutual exclusion lock.
type Mutex struct {
	deadlock.Mutex
}

// TryLock tries to lock m and reports whether it succeeded.
//
// Note that while correct uses of TryLock do exist, they are rare,
// and use of TryLock is often a sign of a deeper problem
// in a particular use of mutexes.
// Note: Deadlock mutex lacks try lock method -- so, revert to regular lock.
func (m *Mutex) TryLock() bool {
	m.Lock()
	return true
}

// AssertHeld is a no-op for deadlock mutexes.
func (m *Mutex) AssertHeld() {
}

// TryLock is a no-op for deadlock mutexes.
func (rw *Mutex) TryLock() bool {
	return false
}

// An RWMutex is a reader/writer mutual exclusion lock.
type RWMutex struct {
	deadlock.RWMutex
}

// TryLock tries to lock m and reports whether it succeeded.
//
// Note that while correct uses of TryLock do exist, they are rare,
// and use of TryLock is often a sign of a deeper problem
// in a particular use of mutexes.
// Note: Deadlock mutex lacks try lock method -- so, revert to regular lock.
func (m *RWMutex) TryLock() bool {
	m.Lock()
	return true
}

// AssertHeld is a no-op for deadlock mutexes.
func (rw *RWMutex) AssertHeld() {
}

// AssertRHeld is a no-op for deadlock mutexes.
func (rw *RWMutex) AssertRHeld() {
}

// TryLock is a no-op for deadlock mutexes.
func (rw *RWMutex) TryLock() bool {
	return false
}
