// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !deadlock
// +build race

package syncutil

import (
	"sync"
	"sync/atomic"
)

// A Mutex is a mutual exclusion lock.
type Mutex struct {
	mu       sync.Mutex
	isLocked int32 // updated atomically
}

// Lock implements sync.Locker.
func (m *Mutex) Lock() {
	m.mu.Lock()
	atomic.StoreInt32(&m.isLocked, 1)
}

// Unlock implements sync.Locker.
func (m *Mutex) Unlock() {
	atomic.StoreInt32(&m.isLocked, 0)
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
	if atomic.LoadInt32(&m.isLocked) == 0 {
		panic("mutex is not locked")
	}
}

// An RWMutex is a reader/writer mutual exclusion lock.
type RWMutex struct {
	sync.RWMutex
	isLocked int32 // updated atomically
}

// Lock implements sync.Locker.
func (m *RWMutex) Lock() {
	m.RWMutex.Lock()
	atomic.StoreInt32(&m.isLocked, 1)
}

// Unlock implements sync.Locker.
func (m *RWMutex) Unlock() {
	atomic.StoreInt32(&m.isLocked, 0)
	m.RWMutex.Unlock()
}

// AssertHeld may panic if the mutex is not locked for writing (but it is not
// required to do so). Functions which require that their callers hold a
// particular lock may use this to enforce this requirement more directly than
// relying on the race detector.
//
// Note that we do not require the lock to be held by any particular thread,
// just that some thread holds the lock. This is both more efficient and allows
// for rare cases where a mutex is locked in one thread and used in another.
func (m *RWMutex) AssertHeld() {
	if atomic.LoadInt32(&m.isLocked) == 0 {
		panic("mutex is not locked")
	}
}
