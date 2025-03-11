// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build deadlock

package syncutil

import (
	"time"

	"github.com/puzpuzpuz/xsync/v3"
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

// TryRLock is a no-op for deadlock mutexes.
func (rw *RWMutex) TryRLock() bool {
	return false
}

// A RBMutex is a reader biased reader/writer mutual exclusion lock.
// It behaves the same in deadlock builds and non-deadlock builds.
type RBMutex struct {
	xsync.RBMutex
}

// AssertHeld may panic if the mutex is not locked for writing (but it is not
// required to do so). Functions which require that their callers hold a
// particular lock may use this to enforce this requirement more directly than
// relying on the race detector.
//
// Note that we do not require the exclusive lock to be held by any particular
// thread, just that some thread holds the lock. This is both more efficient
// and allows for rare cases where a mutex is locked in one thread and used in
// another.
func (rb *RBMutex) AssertHeld() {}

// AssertRHeld may panic if the mutex is not locked for reading (but it is not
// required to do so). If the mutex is locked for writing, it is also considered
// to be locked for reading. Functions which require that their callers hold a
// particular lock may use this to enforce this requirement more directly than
// relying on the race detector.
//
// Note that we do not require the shared lock to be held by any particular
// thread, just that some thread holds the lock. This is both more efficient
// and allows for rare cases where a mutex is locked in one thread and used in
// another.
func (rb *RBMutex) AssertRHeld() {}
