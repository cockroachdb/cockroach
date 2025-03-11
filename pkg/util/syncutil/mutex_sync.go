// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !deadlock && !race

package syncutil

import (
	"sync"

	"github.com/puzpuzpuz/xsync/v3"
)

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
type RWMutex struct {
	sync.RWMutex
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

// A RBMutex is a reader biased reader/writer mutual exclusion lock.
type RBMutex struct {
	xsync.RBMutex
}

// RLock locks rb for reading and returns a reader token. The
// token must be used in the later RUnlock call.
//
// Should not be used for recursive read locking; a blocked Lock
// call excludes new readers from acquiring the lock.
func (rb *RBMutex) RLock() *RToken {
	return (*RToken)(rb.RBMutex.RLock())
}

// RUnlock undoes a single RLock call. A reader token obtained from
// the RLock call must be provided. RUnlock does not affect other
// simultaneous readers. A panic is raised if m is not locked for
// reading on entry to RUnlock.
func (rb *RBMutex) RUnlock(token *RToken) {
	rb.RBMutex.RUnlock((*xsync.RToken)(token))
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

func (rb *RBMutex) RLocker() RBLocker {
	return (*rblocker)(rb)
}

type RToken xsync.RToken

type RBLocker interface {
	RLock() *RToken
	RUnlock(token *RToken)
}

type rblocker RBMutex

func (r *rblocker) RLock() *RToken {
	return (*RBMutex)(r).RLock()
}
func (r *rblocker) RUnlock(token *RToken) {
	(*RBMutex)(r).RUnlock(token)
}
