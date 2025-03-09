// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !deadlock && !race

package syncutil

import "sync"

// DeadlockEnabled is true if the deadlock detector is enabled.
const DeadlockEnabled = false

// A Mutex is a mutual exclusion lock.
type Mutex struct {
	sync.Mutex
}

// AssertHeld may panic if the mutex is not locked (but it is not required to
// do so). Functions which require that their callers hold a particular lock
// may use this to enforce this requirement more directly than relying on the
// race detector.
//
// Note that we do not require the lock to be held by any particular thread,
// just that some thread holds the lock. This is both more efficient and allows
// for rare cases where a mutex is locked in one thread and used in another.
func (m *Mutex) AssertHeld() {
}

// An RWMutex is a reader/writer mutual exclusion lock.
// NOTE: Due to https://github.com/golang/go/issues/17973, this implementation
// has the behavior of a normal Mutex.
type RWMutex struct {
	sync.Mutex
}

// RLock is identical to Lock in the current implementation.
func (rw *RWMutex) RLock() {
	rw.Lock()
}

// RUnlock is identical to Unlock in the current implementation.
func (rw *RWMutex) RUnlock() {
	rw.Unlock()
}

// TryRLock is identical to TryLock in the current implementation.
func (rw *RWMutex) TryRLock() bool {
	return rw.TryLock()
}

// RLocker returns a [Locker] interface that implements
// the [Locker.Lock] and [Locker.Unlock] methods by calling rw.RLock and rw.RUnlock.
func (rw *RWMutex) RLocker() sync.Locker {
	return (*rlocker)(rw)
}

type rlocker RWMutex

func (r *rlocker) Lock()   { (*RWMutex)(r).RLock() }
func (r *rlocker) Unlock() { (*RWMutex)(r).RUnlock() }

// AssertHeld may panic if the mutex is not locked for writing (but it is not
// required to do so). Functions which require that their callers hold a
// particular lock may use this to enforce this requirement more directly than
// relying on the race detector.
//
// Note that we do not require the exclusive lock to be held by any particular
// thread, just that some thread holds the lock. This is both more efficient
// and allows for rare cases where a mutex is locked in one thread and used in
// another.
func (rw *RWMutex) AssertHeld() {
}

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
func (rw *RWMutex) AssertRHeld() {
}
