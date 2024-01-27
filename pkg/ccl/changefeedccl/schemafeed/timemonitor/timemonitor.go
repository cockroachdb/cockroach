// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// Package timemonitor provides a TimeMonitor synchronization construct that is
// useful for synchronizing goroutine access based on a frontier timestamp.
package timemonitor

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// A TimeMonitor is used to synchronize goroutines that advance a monotonically
// increasing frontier timestamp and goroutines that need to wait for the
// frontier to advance past a specific timestamp.
//
// A monotonically decreasing error timestamp can also be tracked and signifies
// the earliest time at which the frontier cannot advance to/past due to some
// external error condition. This external error is recorded so that future
// attempts to wait at a timestamp at or later than the error timestamp will
// return the error.
//
// Similar to a sync.Cond, a TimeMonitor is associated with a sync.Locker,
// which must be held while reading, updating, or waiting for the frontier.
type TimeMonitor struct {
	l        sync.Locker
	frontier hlc.Timestamp
	errTS    hlc.Timestamp
	err      error
	waiters  []timeWaiter
}

// A timeWaiter is created for each goroutine that is blocked waiting for the
// frontier value to advance past a specific timestamp.
type timeWaiter struct {
	ts     hlc.Timestamp
	waitCh chan error
}

// New returns a new TimeMonitor. The sync.Locker passed to New should be locked
// before calling any of TimeMonitor's receivers.
func New(l sync.Locker, opts ...Option) *TimeMonitor {
	m := &TimeMonitor{
		l: l,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Frontier returns the current frontier value.
func (m *TimeMonitor) Frontier() hlc.Timestamp {
	return m.frontier
}

// AdvanceFrontier advances the frontier to the given timestamp.
// The new frontier value must not be earlier than the current frontier,
// nor may it be at or later than the current error timestamp.
func (m *TimeMonitor) AdvanceFrontier(ts hlc.Timestamp) error {
	if m.frontier.IsSet() && ts.Less(m.frontier) {
		return errors.AssertionFailedf(
			"cannot advance frontier to time (%s) earlier than current frontier (%s)", ts, m.frontier)
	}
	if m.errTS.IsSet() && m.errTS.LessEq(ts) {
		return errors.AssertionFailedf(
			"cannot advance frontier to time (%s) equal to or later than error timestamp (%s)", ts, m.errTS)
	}
	m.frontier = ts

	// Signal all waiters that are waiting at or before the new frontier.
	var newWaiters []timeWaiter
	for _, w := range m.waiters {
		if w.ts.LessEq(m.frontier) {
			w.waitCh <- nil
			continue
		}
		newWaiters = append(newWaiters, w)
	}
	m.waiters = newWaiters

	return nil
}

// WaitForFrontier blocks until either the frontier timestamp reaches/passes the
// specified timestamp, the error timestamp recedes to/below the timestamp, or
// the context is canceled. It returns the duration spent waiting.
func (m *TimeMonitor) WaitForFrontier(
	ctx context.Context, ts hlc.Timestamp,
) (time.Duration, error) {
	if ts.LessEq(m.frontier) {
		return 0, nil
	}
	if m.errTS.IsSet() && !ts.Less(m.errTS) {
		return 0, m.err
	}

	waitCh := make(chan error, 1)
	w := timeWaiter{
		ts:     ts,
		waitCh: waitCh,
	}
	m.waiters = append(m.waiters, w)
	m.l.Unlock()
	defer m.l.Lock()

	start := timeutil.Now()
	select {
	case <-ctx.Done():
		return timeutil.Since(start), ctx.Err()
	case err := <-waitCh:
		return timeutil.Since(start), err
	}
}

// ErrorTimestamp returns the current error timestamp value. If the return
// value is non-zero, the error can be retrieved by calling WaitForFrontier
// with the timestamp.
func (m *TimeMonitor) ErrorTimestamp() hlc.Timestamp {
	return m.errTS
}

// SetError will only set the error timestamp and error if the new error
// timestamp is earlier than the current error timestamp (i.e. the error
// timestamp can never advance and may only recede). A non-nil error that
// will be associated with this error timestamp must be provided. The error
// timestamp must not be earlier or equal to the current frontier.
func (m *TimeMonitor) SetError(ts hlc.Timestamp, err error) error {
	if ts.IsEmpty() {
		return errors.AssertionFailedf("cannot set empty error timestamp")
	}
	if err == nil {
		return errors.AssertionFailedf("cannot set nil error")
	}
	if ts.LessEq(m.frontier) {
		return errors.AssertionFailedf(
			"cannot set error timestamp to time (%s) earlier or equal to frontier (%s)", ts, m.frontier)
	}
	if m.errTS.IsSet() && m.errTS.LessEq(ts) {
		return nil
	}
	m.errTS = ts
	m.err = err

	// Signal all waiters that are waiting at or after the new error timestamp.
	var newWaiters []timeWaiter
	for _, w := range m.waiters {
		if !w.ts.Less(m.errTS) {
			w.waitCh <- m.err
			continue
		}
		newWaiters = append(newWaiters, w)
	}
	m.waiters = newWaiters

	return nil
}

// An Option is a functional option for New.
type Option func(*TimeMonitor)

// WithInitialFrontier can be used to set an initial frontier value.
func WithInitialFrontier(ts hlc.Timestamp) Option {
	return func(m *TimeMonitor) {
		m.frontier = ts
	}
}
