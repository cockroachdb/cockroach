// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build deadlock

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
