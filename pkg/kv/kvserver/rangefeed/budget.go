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
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

var budgetAllocationSyncPool = sync.Pool{
	New: func() interface{} {
		return new(SharedBudgetAllocation)
	},
}

func getPooledBudgetAllocation(ba SharedBudgetAllocation) *SharedBudgetAllocation {
	b := budgetAllocationSyncPool.Get().(*SharedBudgetAllocation)
	*b = ba
	return b
}

func putPooledBudgetAllocation(ba *SharedBudgetAllocation) {
	*ba = SharedBudgetAllocation{}
	budgetAllocationSyncPool.Put(ba)
}

// FeedBudget is memory budget for RangeFeed that wraps BoundAccount
// and provides ability to wait for downstream to release budget and
// to send individual events that exceed total budget size.
// FeedBudget doesn't provide any fairness when acquiring as it is only
// supposed to be used by a single caller.
// When owning component is destroyed, budget must be closed, in that
// case all budget allocation is returned immediately and no further
// allocations are possible.
// In the typical case processor will get allocations from budget which
// would be in turn borrowed from underlying account. Once event is
// processed, allocation would be returned.
// To use the budget first try to obtain allocation with TryGet and if
// it fails because budget is exhausted use WaitAndGet and use context with
// a deadline to stop. It is not safe to just call WaitAndGet because it
// doesn't check for available budget before waiting.
// NB: Resource release notifications only work within context of a single
// feed. If we start contending for memory with other feeds in the same
// BytesMonitor pool we won't see if memory is released there and will
// time-out if memory is not allocated.
type FeedBudget struct {
	mu struct {
		syncutil.Mutex
		// Bound account that provides budget with resource.
		memBudget *mon.BoundAccount
		// If true, budget was released and no more allocations could take place.
		closed bool
	}
	// Maximum amount of memory to use by feed. We use separate limit here to
	// avoid creating BytesMontior with a limit per feed.
	limit int64
	// Channel to notify that memory was returned to the budget.
	replenishC chan interface{}
	// Budget cancellation request
	stopC chan interface{}

	closed sync.Once
}

// NewFeedBudget creates a FeedBudget to be used with RangeFeed. If nil account
// is passed, function will return nil which is safe to use with RangeFeed as
// it effectively disables memory accounting for that feed.
func NewFeedBudget(budget *mon.BoundAccount, limit int64) *FeedBudget {
	if budget == nil {
		return nil
	}
	// If limit is not specified, use large enough value.
	if limit <= 0 {
		limit = (1 << 63) - 1
	}
	f := &FeedBudget{
		replenishC: make(chan interface{}, 1),
		stopC:      make(chan interface{}),
		limit:      limit,
	}
	f.mu.memBudget = budget
	return f
}

// TryGet allocates amount from budget. If there's not enough budget available
// returns error immediately.
// Returned allocation has its use counter set to 1.
func (f *FeedBudget) TryGet(ctx context.Context, amount int64) (*SharedBudgetAllocation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mu.closed {
		logcrash.ReportOrPanic(ctx, nil, "budget unexpectedly closed")
		return nil, errors.AssertionFailedf("budget unexpectedly closed")
	}
	var err error
	if f.mu.memBudget.Used()+amount > f.limit {
		return nil, errors.Wrap(f.mu.memBudget.Monitor().Resource().NewBudgetExceededError(amount,
			f.mu.memBudget.Used(),
			f.limit), "rangefeed budget")
	}
	if err = f.mu.memBudget.Grow(ctx, amount); err != nil {
		return nil, err
	}
	return getPooledBudgetAllocation(SharedBudgetAllocation{size: amount, refCount: 1, feed: f}), nil
}

// WaitAndGet waits for replenish channel to return any allocations back to the
// budget and then tries to get allocation. Waiting stops when context is
// cancelled. Context should be used to set up a timeout as needed.
func (f *FeedBudget) WaitAndGet(
	ctx context.Context, amount int64,
) (*SharedBudgetAllocation, error) {
	for {
		select {
		case <-f.replenishC:
			alloc, err := f.TryGet(ctx, amount)
			if err == nil {
				return alloc, nil
			}
		case <-ctx.Done():
			// Since we share budget with other components, it is also possible that
			// it was returned and we are not notified by our feed channel, so we try
			// for the last time.
			return f.TryGet(ctx, amount)
		case <-f.stopC:
			// We are already stopped, current allocation is already freed so, do
			// nothing.
			return nil, nil
		}
	}
}

// Return returns amount to budget.
func (f *FeedBudget) returnAllocation(ctx context.Context, amount int64) {
	f.mu.Lock()
	if f.mu.closed {
		f.mu.Unlock()
		return
	}
	if amount > 0 {
		f.mu.memBudget.Shrink(ctx, amount)
	}
	f.mu.Unlock()
	select {
	case f.replenishC <- struct{}{}:
	default:
	}
}

// Close frees up all allocated budget and prevents any further allocations.
// Safe to call on nil budget.
func (f *FeedBudget) Close(ctx context.Context) {
	if f == nil {
		return
	}
	f.closed.Do(func() {
		f.mu.Lock()
		f.mu.closed = true
		f.mu.memBudget.Close(ctx)
		close(f.stopC)
		f.mu.Unlock()
	})
}

// SharedBudgetAllocation is a token that is passed around with range events
// to registrations to maintain RangeFeed memory budget across shared queues.
type SharedBudgetAllocation struct {
	refCount int32
	size     int64
	feed     *FeedBudget
}

// Use increases usage count for the allocation. It should be called by each
// new consumer that plans to retain allocation after returning to a caller
// that passed this allocation.
func (a *SharedBudgetAllocation) Use() {
	if a != nil {
		if atomic.AddInt32(&a.refCount, 1) == 1 {
			panic("unexpected shared memory allocation usage increase after free")
		}
	}
}

// Release decreases ref count and returns true if budget could be released.
func (a *SharedBudgetAllocation) Release(ctx context.Context) {
	if a != nil && atomic.AddInt32(&a.refCount, -1) == 0 {
		a.feed.returnAllocation(ctx, a.size)
		putPooledBudgetAllocation(a)
	}
}
