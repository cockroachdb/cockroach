// Copyright 2014, Google Inc.
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-grpc.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2017 The Cockroach Authors.
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

// The code below is a modified version of a similar structure found in
// grpc-go (github.com/grpc/grpc-go/blob/b2fae0c/transport/control.go).
// Specifically we allow for arbitrarily sized acquisitions and avoid
// starvation by internally maintaining a FIFO structure.

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type quotaPool struct {
	syncutil.Mutex

	// We service quota acquisitions in a first come, first serve basis. This
	// is done in order to prevent starvations of large acquisitions by a
	// continuous stream of smaller ones. Acquisitions 'register' themselves
	// for a notification that indicates they're now first in line. This is
	// done by appending to the queue the channel they will then wait
	// on. If a goroutine no longer needs to be notified, i.e. their
	// acquisition context has been canceled, the goroutine is responsible for
	// blocking subsequent notifications to the channel by filling up the
	// channel buffer.
	queue []chan struct{}

	// We use a channel to 'park' our quota value for easier composition with
	// context cancellation and quotaPool closing (see quotaPool.acquire).
	//
	// Quota additions push a value into the channel whereas the acquisition
	// first in line waits on the channel itself.
	quota chan int64
	max   int64

	// Ongoing acquisitions listen on quotaPool.done which is closed when the quota
	// pool is closed (see quotaPool.close)
	done   chan struct{}
	closed bool
}

// newQuotaPool returns a new quota pool initialized with a given quota. The quota
// is capped at this amount, meaning that callers may return more quota than they
// acquired without ever making more than the quota capacity available.
func newQuotaPool(q int64) *quotaPool {
	qp := &quotaPool{
		quota: make(chan int64, 1),
		done:  make(chan struct{}),
		max:   q,
	}
	qp.quota <- q
	return qp
}

// add adds the specified quota back to the pool. At no point does the total
// quota in the pool exceed the maximum capacity determined during
// initialization.
//
// Safe for concurrent use.
func (qp *quotaPool) add(v int64) {
	qp.Lock()
	select {
	case q := <-qp.quota:
		v += q
	default:
	}
	if v > qp.max {
		v = qp.max
	}
	qp.quota <- v
	qp.Unlock()
}

// acquire acquires the specified amount of quota from the pool. On success,
// nil is returned and the caller must call add(v) or otherwise arrange for the
// quota to be returned to the pool. If 'v' is greater than the total capacity
// of the pool, we instead try to acquire quota equal to the maximum capacity.
//
// Safe for concurrent use.
func (qp *quotaPool) acquire(ctx context.Context, v int64) error {
	if v > qp.max {
		v = qp.max
	}

	notifyCh := make(chan struct{}, 1)
	qp.Lock()
	qp.queue = append(qp.queue, notifyCh)
	// If we're first in line, we notify ourself immediately.
	if len(qp.queue) == 1 {
		notifyCh <- struct{}{}
	}
	qp.Unlock()
	slowTimer := timeutil.NewTimer()
	defer slowTimer.Stop()
	start := timeutil.Now()

	// Intentionally reset only once, for we care more about the select duration in
	// goroutine profiles than periodic logging.
	slowTimer.Reset(base.SlowRequestThreshold)
	for {
		select {
		case now := <-slowTimer.C:
			slowTimer.Read = true
			log.Warningf(ctx, "have been waiting %s attempting to acquire quota",
				now.Sub(start))
			continue
		case <-ctx.Done():
			qp.Lock()
			// We no longer need to be notified but we need to be careful and check
			// whether or not we're first in queue. If so, we need to notify the
			// next acquisition goroutine and clean up the waiting queue while
			// doing so.
			//
			// Else we simply 'unregister' ourselves from the queue by filling
			// up the channel buffer. This is what is checked when a goroutine
			// wishes to notify the next in line.
			if qp.queue[0] == notifyCh {
				// NB: Notifying the channel before moving it to the head of the
				// queue is safe because the queue itself is guarded by a lock.
				// Goroutines are not a risk of getting notified and finding
				// out they're not first in line.
				qp.notifyNextLocked()
			} else {
				notifyCh <- struct{}{}
			}

			qp.Unlock()
			return ctx.Err()
		case <-qp.done:
			// We don't need to 'unregister' ourselves as in the case when the
			// context is canceled. In fact, we want others waiters to only
			// receive on qp.done and signaling them would work against that.
			return nil
		case <-notifyCh:
		}
		break
	}

	// We're first in line to receive quota, we keep accumulating quota until
	// we've acquired enough or determine we no longer need the acquisition.
	// If we have acquired the quota needed or our context gets canceled,
	// we're sure to remove ourselves from the queue and notify the goroutine
	// next in line (if any).

	var acquired int64
	slowTimer.Reset(base.SlowRequestThreshold)
	for acquired < v {
		select {
		case now := <-slowTimer.C:
			slowTimer.Read = true
			log.Warningf(ctx, "have been waiting %s attempting to acquire quota",
				now.Sub(start))
		case <-ctx.Done():
			if acquired > 0 {
				qp.add(acquired)
			}

			qp.Lock()
			qp.notifyNextLocked()
			qp.Unlock()
			return ctx.Err()
		case <-qp.done:
			// We don't need to release quota back as all ongoing and
			// subsequent acquisitions will succeed immediately.
			return nil
		case q := <-qp.quota:
			acquired += q
		}
	}
	if acquired > v {
		qp.add(acquired - v)
	}

	qp.Lock()
	qp.notifyNextLocked()
	qp.Unlock()
	return nil
}

// notifyNextLocked notifies the waiting acquisition goroutine next in line (if
// any). It requires that qp.Lock is held.
func (qp *quotaPool) notifyNextLocked() {
	// We're at the head of the queue. We traverse until we find a goroutine
	// waiting to be notified, notify the goroutine and truncate our queue so
	// to ensure the said goroutine is at the head of the queue. Normally the
	// next lined up waiter is the one waiting for notification, but if others
	// behind us have also gotten their context canceled, they
	// will leave behind waiters that we would skip below.
	//
	// If we determine there are no goroutines waiting, we simply truncate the
	// queue to reflect this.
	qp.queue = qp.queue[1:]
	for _, ch := range qp.queue {
		select {
		case ch <- struct{}{}:
		default:
			qp.queue = qp.queue[1:]
			continue
		}
		break
	}
}

// approximateQuota will correctly report approximately the amount of quota
// available in the pool. It is accurate only if there are no ongoing
// acquisition goroutines. If there are, the return value can be up to 'v' less
// than actual available quota where 'v' is the value the acquisition goroutine
// first in line is attempting to acquire.
func (qp *quotaPool) approximateQuota() int64 {
	qp.Lock()
	defer qp.Unlock()

	select {
	case q := <-qp.quota:
		qp.quota <- q
		return q
	default:
		return 0
	}
}

// close signals to all ongoing and subsequent acquisitions that they are
// free to return to their callers without error.
//
// Safe for concurrent use.
func (qp *quotaPool) close() {
	qp.Lock()
	if !qp.closed {
		qp.closed = true
		close(qp.done)
	}
	qp.Unlock()
}

// max returns the maximum available quota this pool can contain.
func (qp *quotaPool) maxQuota() int64 {
	return qp.max
}
