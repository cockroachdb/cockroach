// Copyright 2016 The Cockroach Authors.
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
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package mon

import (
	"fmt"

	"golang.org/x/net/context"
)

// MemoryAccount tracks the cumulated allocations for one client of
// MemoryUsageMonitor. This allows a client to release all the memory
// at once when it completes its work.
type MemoryAccount struct {
	mon          *MemoryUsageMonitor
	curAllocated int64
}

// OpenAccount creates a new empty account.
func (mm *MemoryUsageMonitor) OpenAccount(_ context.Context) MemoryAccount {
	// TODO(knz): conditionally track accounts in the memory monitor
	// (#9122).
	return MemoryAccount{mon: mm, curAllocated: 0}
}

// OpenAndInitAccount creates a new account and pre-allocates some
// initial amount of memory.
func (mm *MemoryUsageMonitor) OpenAndInitAccount(
	ctx context.Context, acc *MemoryAccount, initialAllocation int64,
) error {
	*acc = mm.OpenAccount(ctx)
	return acc.Grow(ctx, initialAllocation)
}

// Grow requests a new allocation in an account.
func (acc *MemoryAccount) Grow(
	ctx context.Context, extraSize int64,
) error {
	if err := acc.mon.reserveMemory(ctx, extraSize); err != nil {
		return err
	}
	acc.curAllocated += extraSize
	return nil
}

// Close releases all the cumulated allocations of an account at once.
func (acc *MemoryAccount) Close(ctx context.Context) {
	acc.mon.releaseMemory(ctx, acc.curAllocated)
}

// Clear releases all the cumulated allocations of an account at once
// and primes it for reuse.
func (acc *MemoryAccount) Clear(ctx context.Context) {
	acc.mon.releaseMemory(ctx, acc.curAllocated)
	acc.curAllocated = 0
}

// ResizeItem requests a size change for an object already registered
// in an account. The reservation is not modified if the new allocation is
// refused, so that the caller can keep using the original item
// without an accounting error. This is better than calling Clear
// then Grow because if the Clear succeeds and the Grow fails
// the original item becomes invisible from the perspective of the
// monitor.
func (acc *MemoryAccount) ResizeItem(
	ctx context.Context, oldSize, newSize int64,
) error {
	delta := newSize - oldSize
	mm := acc.mon
	switch {
	case delta > 0:
		if err := mm.reserveMemory(ctx, delta); err != nil {
			return err
		}
	case delta < 0:
		if acc.curAllocated < -delta {
			panic(fmt.Sprintf("no memory in account to release, current %d, free %d", acc.curAllocated, delta))
		}
		mm.releaseMemory(ctx, -delta)
	}
	acc.curAllocated += delta
	return nil
}
