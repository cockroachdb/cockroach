// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// feedBudget is memory budget for RangeFeed that wraps BoundAccount
// and provides ability to wait for downstream to release budget and
// to send individual events that exceed budget total budget size.
// feedBudget doesn't provide any fairness when acquiring as it is only
// supposed to be used by a single caller.
// When owning component is destroyed, budget could be closed, in that
// case all budget allocation is returned immediately and no further
// allocations are possible.
// In the typical case processor will get allocations from budget which
// would be in term borrowed from underlying account. Once event is
// processed, allocation would be returned.
// If budget is exhausted, allocations would block until more is available
// or until an optional timeout expires.
// NB: if we are competing with other consumers in the budget pool, then
// we won't see that memory is released.
type feedBudget struct {
	mu struct {
		syncutil.Mutex
		// Bound account that provides budget with resource.
		memBudget *mon.BoundAccount
		// Whenever we pass oversized message this would go to true to prevent
		// other attempts to acquire more memory until the event goes through.
		oversize bool
		// If true, budget was released and no more allocations could take place.
		closed bool
	}
	// Channel to notify that memory was returned to the budget.
	replenishC chan interface{}

	metrics *Metrics
}

func (f *feedBudget) init(budget *mon.BoundAccount, metrics *Metrics) {
	f.mu.memBudget = budget
	if budget != nil {
		f.replenishC = make(chan interface{}, 1)
		f.metrics = metrics
	}
}

// Get allocates amount from budget.
// If budget has less memory than requested it could either wait, if we have any
// in flight allocations, or it could send a single message through regardless
// of its size. In latter case, memory is not requested from underlying budget,
// but we won't be able to do another Get until this message is returned. To
// track such events, feedBudget uses a counter metric.
//
// timeout argument indicates how long we could wait for the budget to replenish
// if there is not enough budget available. Budget is replenished by Return
// calls.
// timeout value of 0 means wait indefinitely.
// Whenever we use timeout, we return its wait chan to caller, so it could be
// used subsequently by the chained wait for other resources thus keeping total
// timeout in check without adding to it on every op. If we didn't initiate
// a wait or if timeout was 0 then wait channel will be nil.
// Returned allocation has its use counter set to 1.
func (f *feedBudget) Get(
	ctx context.Context, amount int64, timeout time.Duration, stoppedC <-chan struct{},
) (sharedBudgetAllocation, <-chan time.Time, error) {
	// Function to allocate from budget or if there's nothing there borrow from
	// emergency ether.
	tryAlloc := func() (sharedBudgetAllocation, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.mu.closed {
			return sharedBudgetAllocation{}, errors.New("budget is already released")
		}
		if f.mu.oversize {
			return sharedBudgetAllocation{}, errors.New("failed to allocate mem budget")
		}
		err := f.mu.memBudget.Grow(ctx, amount)
		if err == nil {
			return sharedBudgetAllocation{size: amount, refCount: 1}, nil
		}
		// If we didn't use any budget yet and not currently sending an oversized
		// event then we can send one.
		if f.mu.memBudget.Used() == 0 {
			f.metrics.RangeFeedOverBudgetEvents.Inc(1)
			f.mu.oversize = true
			// We are in override mode, we return 0 size to avoid returning it to
			// budget later as we didn't borrow anything.
			return sharedBudgetAllocation{size: 0, refCount: 1}, nil
		}
		return sharedBudgetAllocation{}, err
	}

	alloc, err := tryAlloc()
	if err == nil {
		return alloc, nil, nil
	}
	// If we failed to allocate immediately, then we need to wait on replenish
	// channel to track Return calls from consumed events.
	if timeout == 0 {
		for {
			select {
			case <-f.replenishC:
				alloc, err = tryAlloc()
				if err == nil {
					return alloc, nil, nil
				}
			case <-stoppedC:
				// We are already stopped, current allocation is already freed so, do
				// nothing.
				return sharedBudgetAllocation{}, nil, nil
			}
		}
	} else {
		timeC := time.After(timeout)
		for {
			select {
			case <-f.replenishC:
				alloc, err = tryAlloc()
				if err == nil {
					return alloc, timeC, nil
				}
			case <-timeC:
				return sharedBudgetAllocation{}, timeC, errors.New("failed to allocate mem budget")
			case <-stoppedC:
				// We are already stopped, current allocation is already freed so, do
				// nothing.
				return sharedBudgetAllocation{}, timeC, nil
			}
		}
	}
}

// Return returns amount to budget.
func (f *feedBudget) Return(ctx context.Context, x int64) {
	f.mu.Lock()
	if f.mu.closed {
		f.mu.Unlock()
		return
	}
	f.mu.oversize = false
	if x > 0 {
		f.mu.memBudget.Shrink(ctx, x)
	}
	f.mu.Unlock()
	select {
	case f.replenishC <- struct{}{}:
	default:
	}
}

// Close frees up all allocated budget and prevents any further allocations.
func (f *feedBudget) Close(ctx context.Context) {
	f.mu.Lock()
	if !f.mu.closed {
		f.mu.closed = true
		f.mu.memBudget.Close(ctx)
	}
	f.mu.Unlock()
}

// sharedBudgetAllocation is a token that is passed around with range events
// to registrations to maintain RangeFeed memory budget across shared queues.
type sharedBudgetAllocation struct {
	refCount int32
	size     int64
}

func (a *sharedBudgetAllocation) use() {
	if a != nil {
		if atomic.AddInt32(&a.refCount, 1) == 1 {
			panic("unexpected shared memory allocation usage increase after free")
		}
	}
}

// release decreases ref count and returns true if budget could be released.
func (a *sharedBudgetAllocation) release() bool {
	return a != nil && atomic.AddInt32(&a.refCount, -1) == 0
}
