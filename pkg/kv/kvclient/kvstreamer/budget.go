// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
)

// budget abstracts the memory budget that is provided to the Streamer by its
// client.
type budget struct {
	mu struct {
		syncutil.Mutex
		// acc represents the current reservation of this budget against the
		// memory pool. acc.Used() will never grow past the limitBytes.
		acc *mon.BoundAccount
		// used is the amount of currently used up bytes of this budget. This
		// number can exceed limitBytes in degenerate cases (e.g. when a single
		// row exceeds the limitBytes), but usually it should not exceed
		// acc.Used().
		used int64
	}
	// limitBytes is the maximum amount of bytes that this budget can reserve
	// against the account.
	limitBytes int64
	// waitCh is used by the main loop of the workerCoordinator to block until
	// available() becomes positive (until some release calls occur).
	waitCh chan struct{}
}

// newBudget creates a new budget with the specified limit. The limit determines
// the maximum amount of memory this budget is allowed to use (i.e. it'll be
// used lazily, as needed), but mon.DefaultPoolAllocationSize is reserved right
// away. An error is returned if the initial reservation is denied.
//
// acc should be bound to an unlimited memory monitor, and the Budget itself is
// responsible for staying under the limit.
//
// The budget takes ownership of the memory account, and the caller is allowed
// to interact with the account only after canceling the Streamer (because
// memory accounts are not thread-safe).
func newBudget(ctx context.Context, acc *mon.BoundAccount, limitBytes int64) (*budget, error) {
	var b budget
	b.mu.acc = acc
	initialBudgetReservation := mon.DefaultPoolAllocationSize
	if err := acc.Grow(ctx, initialBudgetReservation); err != nil {
		return nil, err
	}
	b.limitBytes = limitBytes
	b.waitCh = make(chan struct{}, 1)
	return &b, nil
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
	return b.limitBytes - b.mu.used
}

// consume draws bytes from the available budget. An error is returned if the
// root pool budget is used up such that the budget's limit cannot be fully
// reserved.
// - errorIfAboveLimit indicates whether an error is also returned when
// consuming the specified amount of memory will put the budget's memory usage
// above its limit.
//
// b's mutex should not be held when calling this method.
func (b *budget) consume(ctx context.Context, bytes int64, errorIfAboveLimit bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.consumeLocked(ctx, bytes, errorIfAboveLimit, false /* testingAssertWithinLimit */)
}

// consumeLocked is the same as consume but assumes that the b's lock is held.
// - testingAssertWithinLimit if true will panic if this consumption will put
// the budget's memory usage above its limit.
func (b *budget) consumeLocked(
	ctx context.Context, bytes int64, errorIfAboveLimit bool, testingAssertWithinLimit bool,
) error {
	// If we're asked to not exceed the limit (and the limit is greater than
	// five bytes - limits of five bytes or less are treated as a special case
	// for "forced disk spilling" scenarios), we have to check whether we'll
	// stay under the budget.
	if errorIfAboveLimit && b.limitBytes > 5 {
		if b.mu.used+bytes > b.limitBytes {
			return mon.MemoryResource.NewBudgetExceededError(bytes, b.mu.used, b.limitBytes)
		}
	}
	if buildutil.CrdbTestBuild {
		if testingAssertWithinLimit {
			if b.mu.used+bytes > b.limitBytes {
				panic(mon.MemoryResource.NewBudgetExceededError(bytes, b.mu.used, b.limitBytes))
			}
		}
	}
	// We need to make sure that we have accounted for either the new b.mu.used
	// value or up to the limitBytes. Note that we don't update b.mu.used right
	// away since this resizing can be denied, yet the Streamer might keep on
	// going without emitting the memory reservation denial error to the client.
	accUsed := b.mu.used + bytes
	if accUsed > b.limitBytes {
		accUsed = b.limitBytes
	}
	if b.mu.acc.Used() < accUsed {
		if err := b.mu.acc.ResizeTo(ctx, accUsed); err != nil {
			return err
		}
	}
	b.mu.used += bytes
	return nil
}

// release returns bytes to the available budget.
func (b *budget) release(bytes int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if buildutil.CrdbTestBuild {
		if b.mu.used < bytes {
			panic(errors.AssertionFailedf(
				"want to release %s when only %s is used",
				humanize.Bytes(uint64(bytes)), humanize.Bytes(uint64(b.mu.used)),
			))
		}
	}
	b.mu.used -= bytes
	if b.limitBytes > b.mu.used {
		select {
		case b.waitCh <- struct{}{}:
		default:
		}
	}
}
