// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build deadlock

package syncutil

import (
	"time"

	deadlock "github.com/sasha-s/go-deadlock"
)

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
