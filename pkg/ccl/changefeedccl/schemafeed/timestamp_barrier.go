// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemafeed

import (
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// A timestampBarrier is used to synchronize goroutines that advance a monotonically
// increasing frontier timestamp and goroutines that need to wait for the
// frontier to advance past a specific timestamp.
//
// A monotonically decreasing error timestamp can also be tracked and signifies
// the earliest time at which the frontier cannot advance to/past due to some
// external error condition. This external error is recorded so that future
// attempts to wait at a timestamp at or later than the error timestamp will
// return the error.
//
// The data structure itself is not safe for concurrent use and must be
// protected by a synchronization primitive.
type timestampBarrier struct {
	frontier hlc.Timestamp
	errTS    hlc.Timestamp
	err      error
	waiters  []timestampWaiter
}

// A timestampWaiter is created for each goroutine that is blocked waiting for
// the frontier value to advance past a specific timestamp.
type timestampWaiter struct {
	ts    hlc.Timestamp
	errCh chan<- error
}

// advanceFrontier advances the frontier to the given timestamp.
// The new frontier value must not be earlier than the current frontier,
// nor may it be at or later than the current error timestamp.
func (m *timestampBarrier) advanceFrontier(ts hlc.Timestamp) error {
	if ts.Less(m.frontier) {
		return errors.AssertionFailedf(
			"cannot advance frontier to %s, which is earlier than the current frontier %s", ts, m.frontier)
	}
	if m.errTS.IsSet() && m.errTS.LessEq(ts) {
		return errors.AssertionFailedf(
			"cannot advance frontier to %s, which is equal to or later than the current error timestamp %s", ts, m.errTS)
	}
	m.frontier = ts

	// Signal all waiters that are waiting at or before the new frontier.
	newWaiters := make([]timestampWaiter, 0, len(m.waiters))
	for _, w := range m.waiters {
		if w.ts.LessEq(m.frontier) {
			close(w.errCh)
			continue
		}
		newWaiters = append(newWaiters, w)
	}
	m.waiters = newWaiters

	return nil
}

// noWaitCh is returned by (*timestampBarrier).wait in the common case that
// no waiting is necessary because the specified timestamp is less than or
// equal to the frontier.
var noWaitCh = func() <-chan error {
	c := make(chan error)
	close(c)
	return c
}()

// wait returns a channel that can be used to wait for either the frontier
// timestamp to reach/exceed the specified timestamp or the error timestamp to
// recede to/below the specified timestamp, at which point the channel will be
// closed. In the latter case, the channel will be sent the error associated
// with the error timestamp before being closed. A bool is also returned
// representing whether any waiting is actually required.
func (m *timestampBarrier) wait(ts hlc.Timestamp) (waitCh <-chan error, needToWait bool) {
	if ts.LessEq(m.frontier) {
		return noWaitCh, false
	}

	errCh := make(chan error, 1)
	if m.errTS.IsSet() && m.errTS.LessEq(ts) {
		errCh <- m.err
		close(errCh)
		return errCh, false
	}
	w := timestampWaiter{
		ts:    ts,
		errCh: errCh,
	}
	m.waiters = append(m.waiters, w)
	return errCh, true
}

// setError will only set the error timestamp and error if the new error
// timestamp is earlier than the current error timestamp (i.e. the error
// timestamp can never advance and may only recede). A non-nil error that
// will be associated with this error timestamp must be provided. The error
// timestamp must not be earlier or equal to the current frontier.
func (m *timestampBarrier) setError(ts hlc.Timestamp, err error) error {
	if ts.LessEq(m.frontier) {
		return errors.AssertionFailedf(
			"cannot set error timestamp to %s, which is earlier or equal to the current frontier %s", ts, m.frontier)
	}
	if err == nil {
		return errors.AssertionFailedf("cannot set nil error for timestamp %s", ts)
	}
	if m.errTS.IsSet() && m.errTS.LessEq(ts) {
		return nil
	}
	m.errTS = ts
	m.err = err

	// Signal all waiters that are waiting at or after the new error timestamp.
	newWaiters := make([]timestampWaiter, 0, len(m.waiters))
	for _, w := range m.waiters {
		if !w.ts.Less(m.errTS) {
			w.errCh <- m.err
			close(w.errCh)
			continue
		}
		newWaiters = append(newWaiters, w)
	}
	m.waiters = newWaiters

	return nil
}
