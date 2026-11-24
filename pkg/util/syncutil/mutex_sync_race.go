// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !deadlock && race

package syncutil

import (
	"sync"
	"sync/atomic"
)

// DeadlockEnabled is true if the deadlock detector is enabled.
const DeadlockEnabled = false

// A Mutex is a mutual exclusion lock.
type Mutex struct {
	mu      sync.Mutex
	wLocked int32 // updated atomically
}

// Lock locks m.
func (m *Mutex) Lock() {
	m.mu.Lock()
	atomic.StoreInt32(&m.wLocked, 1)
}

// TryLock tries to lock m and reports whether it succeeded.
func (m *Mutex) TryLock() bool {
	if !m.mu.TryLock() {
		return false
	}
	atomic.StoreInt32(&m.wLocked, 1)
	return true
}

// Unlock unlocks m.
func (m *Mutex) Unlock() {
	atomic.StoreInt32(&m.wLocked, 0)
	m.mu.Unlock()
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
	if atomic.LoadInt32(&m.wLocked) == 0 {
		panic("mutex is not write locked")
	}
}

// An RWMutex is a reader/writer mutual exclusion lock.
type RWMutex struct {
	mu      sync.RWMutex
	wLocked int32 // updated atomically
	rLocked int32 // updated atomically
}

// Lock locks rw for writing.
func (rw *RWMutex) Lock() {
	rw.mu.Lock()
	atomic.StoreInt32(&rw.wLocked, 1)
}

// TryLock tries to lock rw for writing and reports whether it succeeded.
func (rw *RWMutex) TryLock() bool {
	if !rw.mu.TryLock() {
		return false
	}
	atomic.StoreInt32(&rw.wLocked, 1)
	return true
}

// Unlock unlocks rw for writing.
func (rw *RWMutex) Unlock() {
	atomic.StoreInt32(&rw.wLocked, 0)
	rw.mu.Unlock()
}

// RLock locks m for reading.
func (rw *RWMutex) RLock() {
	rw.mu.RLock()
	atomic.AddInt32(&rw.rLocked, 1)
}

// TryRLock tries to lock rw for reading and reports whether it succeeded.
func (rw *RWMutex) TryRLock() bool {
	if !rw.mu.TryRLock() {
		return false
	}
	atomic.AddInt32(&rw.rLocked, 1)
	return true
}

// RUnlock undoes a single RLock call.
func (rw *RWMutex) RUnlock() {
	atomic.AddInt32(&rw.rLocked, -1)
	rw.mu.RUnlock()
}

// RLocker returns a Locker interface that implements
// the Lock and Unlock methods by calling rw.RLock and rw.RUnlock.
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
	if atomic.LoadInt32(&rw.wLocked) == 0 {
		panic("mutex is not write locked")
	}
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
	if atomic.LoadInt32(&rw.wLocked) == 0 && atomic.LoadInt32(&rw.rLocked) == 0 {
		panic("mutex is not read locked")
	}
}
