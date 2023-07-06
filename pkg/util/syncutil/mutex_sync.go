// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build !deadlock && !race
// +build !deadlock,!race

package syncutil

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
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
	pcs      []uintptr
	watching *time.Timer // from mtime.AfterFunc
}

func fatalWithStack(pcs []uintptr) {
	var buf strings.Builder
	fs := runtime.CallersFrames(pcs)
	for {
		frame, more := fs.Next()
		if !more {
			break
		}
		_, err := fmt.Fprintf(&buf, "%s:%d %s\n", frame.File, frame.Line, frame.Function)
		if err != nil {
			_, _ = fmt.Fprintf(&buf, "error: %v\n", err)
		}
		if !more {
			break
		}
	}
	_, _ = fmt.Fprintf(os.Stderr, "stuck mutex, acquired at:\n%s", &buf)
	os.Exit(17)
}

func (rw *RWMutex) Lock() {
	rw.RWMutex.Lock()
	if len(rw.pcs) == 0 {
		rw.pcs = make([]uintptr, 8)
	}
	rw.pcs = rw.pcs[:runtime.Callers(2, rw.pcs[:cap(rw.pcs)])]
	rw.watching = time.AfterFunc(25*time.Second, func() {
		fatalWithStack(rw.pcs) // data race but we have bigger problems!
	})
}

func (rw *RWMutex) Unlock() {
	rw.watching.Stop()
	rw.watching = nil
	rw.RWMutex.Unlock()
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
