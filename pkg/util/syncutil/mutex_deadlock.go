// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build deadlock

package syncutil

import (
	"sync"
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
// NOTE: Due to https://github.com/golang/go/issues/17973, this implementation
// has the behavior of a normal Mutex.
type RWMutex struct {
	deadlock.Mutex
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

// RLock is identical to Lock in the current implementation.
func (rw *RWMutex) RLock() {
	rw.Lock()
}

// RUnlock is identical to Unlock in the current implementation.
func (rw *RWMutex) RUnlock() {
	rw.Unlock()
}

// RLocker returns a [Locker] interface that implements
// the [Locker.Lock] and [Locker.Unlock] methods by calling rw.RLock and rw.RUnlock.
func (rw *RWMutex) RLocker() sync.Locker {
	return (*rlocker)(rw)
}

type rlocker RWMutex

func (r *rlocker) Lock()   { (*RWMutex)(r).RLock() }
func (r *rlocker) Unlock() { (*RWMutex)(r).RUnlock() }
