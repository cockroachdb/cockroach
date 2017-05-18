// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package storage

import (
	"errors"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"golang.org/x/net/context"
)

type quotaPool struct {
	syncutil.Mutex

	q        int64
	max      int64
	cond     *sync.Cond
	closed   bool
	closedCh chan struct{}
}

// newQuotaPool returns a new instance of a quota pool initialized with the
// specified quota. The quota pool is capped at this amount.
func newQuotaPool(v int64) *quotaPool {
	qp := &quotaPool{
		q:        v,
		max:      v,
		closedCh: make(chan struct{}),
	}
	qp.cond = sync.NewCond(qp)
	return qp
}

// add returns the specified amount back to the quota pool and is a blocking
// call. We let adds go through on a closed quota pool given subsequent
// acquisitions will not succeed. Safe for concurrent use.
func (qp *quotaPool) add(q int64) {
	qp.Lock()
	q += qp.q
	if q < qp.max {
		qp.q = q
	} else {
		qp.q = qp.max
	}
	qp.cond.Broadcast()
	qp.Unlock()
}

// acquire attempts to acquire the specified amount of quota and blocks
// indefinitely until we have done so. Alternatively if the given context gets
// cancelled or quota pool is closed altogether we return with an error
// specifying so. The lack of an error indicates a successful quota
// acquisition, the caller is responsible for returning the quota back to the
// pool eventually (see quotaPool.add). Safe for concurrent use.
func (qp *quotaPool) acquire(ctx context.Context, v int64) error {
	res := make(chan error, 1)
	done := new(bool)
	go func() {
		qp.acquireInternal(v, done, res)
	}()
	slowTimer := timeutil.NewTimer()
	defer slowTimer.Stop()
	slowTimer.Reset(base.SlowRequestThreshold)

	for {
		select {
		case <-slowTimer.C:
			log.Warningf(ctx, "have been waiting %s attempting to acquire quota",
				base.SlowRequestThreshold)
		case <-ctx.Done():
			// Given we've seen a context cancellation here, we ensure the quota
			// acquisition goroutine runs to completion. We do so by waiting for a
			// result on the 'res' channel. If we end up acquiring quota, we're
			// sure to return it.
			*done = true

			// Wake up the acquisition goroutine to signal it to stop working.
			qp.cond.Broadcast()

			// We've acquired quota, need to release it back because context was
			// cancelled.
			if err := <-res; err == nil {
				qp.add(v)
			}

			return ctx.Err()
		case <-qp.closedCh:
			// Given the quota pool was just closed, we ensure the quota
			// acquisition goroutine runs to completion. We do so by waiting for a
			// result on the 'res' channel. If we end up acquiring quota, we're
			// sure to return it.
			*done = true

			// Wake up the acquisition goroutine to signal it to stop working.
			qp.cond.Broadcast()

			// We acquired quota, we release it back because the quota pool was
			// closed.
			// NB: Strictly speaking this is not necessary given future allocations
			// will fail anyway.
			if err := <-res; err == nil {
				qp.add(v)
			}

			return errors.New("quota pool closed")
		case err := <-res:
			return err
		}
	}
}

func (qp *quotaPool) acquireInternal(v int64, done *bool, res chan<- error) {
	// sync.Cond has the following usage pattern:
	//
	// // Acquire this monitor's lock.
	// c.L.Lock()
	// // While the condition/predicate/assertion that we are waiting for is not true...
	// for !condition() {
	//     // Wait on this monitor's lock and condition variable.
	//     c.Wait()
	// }
	//
	// // Critical section, we have the lock.
	// ...
	//
	// // Wake another waiting thread if appropriate.
	// if signal() {
	//     c.L.Signal()
	// }
	//
	// // Release this monitor's lock.
	// c.L.Unlock()

	qp.Lock()

	for !(v <= qp.q) {
		qp.cond.Wait()
		// If we were signalled it could possibly be because quota was just
		// added to the pool which we check in the next loop iteration.
		// Alternatively we no longer need the result.
		if *done {
			qp.Unlock()
			res <- errors.New("acquisition cancelled")
			return
		}
	}

	// Critical section, we have the lock.
	// While the lock is held, no other go routine is acquiring/decrementing quota.
	qp.q -= v

	qp.Unlock()
	res <- nil
}

// close closes the quota pool and is safe for concurrent use. Any
// ongoing and subsequent acquisitions fail with an error indicating so.
func (qp *quotaPool) close() {
	qp.Lock()
	if !qp.closed {
		close(qp.closedCh)
		qp.closed = true
	}
	qp.Unlock()
}
