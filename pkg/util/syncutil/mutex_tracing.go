// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package syncutil

import (
	"context"
	"sync"
	"time"
)

// TracedLock is like Lock, but logs a trace event using the provided context if
// the lock acquisition is slow.
func (m *Mutex) TracedLock(ctx context.Context) { tracedLock(ctx, m) }

// TracedLock is like Lock, but logs a trace event using the provided context if
// the lock acquisition is slow.
func (rw *RWMutex) TracedLock(ctx context.Context) { tracedLock(ctx, rw) }

// TracedRLock is like RLock, but logs a trace event using the provided context
// if the lock acquisition is slow.
func (rw *RWMutex) TracedRLock(ctx context.Context) { tracedLock(ctx, rw.rTryLocker()) }

// rTryLocker returns a tryLocker interface that implements the Lock, Unlock,
// and TryLock methods by calling rw.RLock, rw.RUnlock, and rw.TryRLock,
// respectively.
func (rw *RWMutex) rTryLocker() tryLocker { return (*rTryLocker)(rw) }

type rTryLocker RWMutex

func (r *rTryLocker) Lock()         { (*RWMutex)(r).RLock() }
func (r *rTryLocker) Unlock()       { (*RWMutex)(r).RUnlock() }
func (r *rTryLocker) TryLock() bool { return (*RWMutex)(r).TryRLock() }

// tryLocker extends the sync.Locker interface with a TryLock method.
type tryLocker interface {
	sync.Locker
	TryLock() bool
}

// tracedLock is like l.Lock, but logs a trace event using the provided context
// if the lock acquisition is slow.
//
// Explanation of logic:
//
// The function begins with a fast-path call to TryLock. Most mutexes are
// uncontended and TryLock amounts to a single atomic CAS. If the CAS succeeds,
// no additional work is needed. If the CAS fails, we move on to the slow-path.
//
// On the slow path, we first check if expensive logging is enabled. If not, we
// simply call Lock without checking the time. We only time the acquisition if
// expensive logging is enabled. If we do time the acquisition and it is slow,
// we log a warning message to the logs/trace.
//
// It could be a reasonable choice to switch the order of the TryLock and
// ExpensiveLogEnabled checks. However, we expect that most mutex acquisitions
// will be uncontended and the TryLock check is cheaper than the expensive log
// check.
func tracedLock(ctx context.Context, l tryLocker) {
	if enableTracedLockFastPath && l.TryLock() {
		return // fast-path
	}
	const vLevel = 3
	if !LogExpensiveLogEnabled(ctx, vLevel) {
		l.Lock()
		return
	}
	start := time.Now()
	l.Lock()
	if dur := time.Since(start); dur >= slowLockLogThreshold {
		LogVEventfDepth(ctx, 2 /* depth */, vLevel, "slow mutex acquisition took %s", dur)
	}
}

// enableTracedLockFastPath is used in tests to disable the fast-path of
// tracedLock.
var enableTracedLockFastPath = true

// slowLockLogThreshold is the threshold at which a mutex acquisition is
// considered slow enough to log. It is a variable and not constant so that it
// can be changed in tests.
var slowLockLogThreshold = 500 * time.Microsecond

// LogExpensiveLogEnabled is injected from pkg/util/log to avoid an import
// cycle. This also allows it to be mocked out in tests.
//
// See log.ExpensiveLogEnabled for more details.
var LogExpensiveLogEnabled = func(ctx context.Context, level int32) bool { return false }

// LogVEventfDepth is injected from pkg/util/log to avoid an import
// cycle. This also allows it to be mocked out in tests.
//
// See log.LogVEventfDepth for more details.
var LogVEventfDepth = func(ctx context.Context, depth int, level int32, format string, args ...interface{}) {}
