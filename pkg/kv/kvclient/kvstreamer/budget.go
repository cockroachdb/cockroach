// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvstreamer

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// budget abstracts the memory budget that is provided to the Streamer by its
// client.
//
// This struct is a wrapper on top of mon.BoundAccount because we want to
// support the notion of budget "going in debt". This can occur in a degenerate
// case when a single large row exceeds the provided limit. The Streamer is
// expected to have only a single request in flight in this case.
type budget struct {
	mu struct {
		syncutil.Mutex
		// acc represents the current reservation of this budget against the
		// root memory pool.
		acc *mon.BoundAccount
	}
	// limitBytes is the maximum amount of bytes that this budget should reserve
	// against acc, i.e. acc.Used() should not exceed limitBytes. However, in a
	// degenerate case of a single large row, the budget can go into debt and
	// acc.Used() might exceed limitBytes.
	limitBytes int64
	// waitCh is used by the main loop of the workerCoordinator to block until
	// available() becomes positive (until some release calls occur).
	waitCh chan struct{}
}

// newBudget creates a new budget with the specified limit. The limit determines
// the maximum amount of memory this budget is allowed to use (i.e. it'll be
// used lazily, as needed).
//
// The budget itself is responsible for staying under the limit, so acc should
// be bound to an unlimited memory monitor. This is needed in order to support
// the case of budget going into debt. Note that although it is an "unlimited
// memory monitor", it is still limited by --max-sql-memory in size because all
// monitors are descendants of the root SQL monitor.
//
// The budget takes ownership of the memory account, and the caller is allowed
// to interact with the account only after canceling the Streamer (because
// memory accounts are not thread-safe).
func newBudget(acc *mon.BoundAccount, limitBytes int64) *budget {
	b := budget{
		limitBytes: limitBytes,
		waitCh:     make(chan struct{}),
	}
	b.mu.acc = acc
	return &b
}

// available returns how many bytes are currently available in the budget. The
// answer can be negative, in case the Streamer has used un-budgeted memory
// (e.g. one result was very large).
//
// Note that it's possible that actually available budget is less than the
// number returned - this might occur if --max-sql-memory root pool is almost
// fully used up.
func (b *budget) available() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.limitBytes - b.mu.acc.Used()
}

// consume draws bytes from the available budget. An error is returned if the
// root pool budget is used up such that the budget's limit cannot be fully
// reserved.
// - allowDebt indicates whether the budget is allowed to go into debt on this
// consumption. In other words, if allowDebt is true, then acc's reservation is
// allowed to exceed limitBytes. Note that allowDebt value applies only to this
// consume() call and is not carried forward.
//
// b's mutex should not be held when calling this method.
func (b *budget) consume(ctx context.Context, bytes int64, allowDebt bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.consumeLocked(ctx, bytes, allowDebt)
}

// consumeLocked is the same as consume but assumes that the b's lock is held.
func (b *budget) consumeLocked(ctx context.Context, bytes int64, allowDebt bool) error {
	b.mu.AssertHeld()
	// If we're asked to not exceed the limit (and the limit is greater than
	// five bytes - limits of five bytes or less are treated as a special case
	// for "forced disk spilling" scenarios like in logic tests), we have to
	// check whether we'll stay within the budget.
	if !allowDebt && b.limitBytes > 5 {
		if b.mu.acc.Used()+bytes > b.limitBytes {
			return mon.MemoryResource.NewBudgetExceededError(bytes, b.mu.acc.Used(), b.limitBytes)
		}
	}
	return b.mu.acc.Grow(ctx, bytes)
}

// release returns bytes to the available budget.
func (b *budget) release(ctx context.Context, bytes int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mu.acc.Shrink(ctx, bytes)
	if b.limitBytes > b.mu.acc.Used() {
		// Since we now have some available budget, we non-blockingly send on
		// the wait channel to notify the mainCoordinator about it.
		select {
		case b.waitCh <- struct{}{}:
		default:
		}
	}
}
