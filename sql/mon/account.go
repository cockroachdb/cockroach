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

import "golang.org/x/net/context"

// MemoryAccount tracks the cumulated allocations for one client of
// MemoryUsageMonitor. This allows a client to release all the memory
// at once when it completes its work.
type MemoryAccount struct {
	curAllocated int64
}

// OpenAccount creates a new empty account.
func (mm *MemoryUsageMonitor) OpenAccount(_ context.Context, acc *MemoryAccount) {
	// TODO(knz): conditionally track accounts in the memory monitor
	// (#9122).
}

// OpenAndInitAccount creates a new account and pre-allocates some
// initial amount of memory.
func (mm *MemoryUsageMonitor) OpenAndInitAccount(
	ctx context.Context, acc *MemoryAccount, init int64,
) error {
	mm.OpenAccount(ctx, acc)
	return mm.GrowAccount(ctx, acc, init)
}

// GrowAccount requests a new allocation in an account.
func (mm *MemoryUsageMonitor) GrowAccount(
	ctx context.Context, acc *MemoryAccount, extraSize int64,
) error {
	if err := mm.reserveMemory(ctx, extraSize); err != nil {
		return err
	}
	acc.curAllocated += extraSize
	return nil
}

// CloseAccount releases all the cumulated allocations of an account at once.
func (mm *MemoryUsageMonitor) CloseAccount(ctx context.Context, acc *MemoryAccount) {
	mm.releaseMemory(ctx, acc.curAllocated)
}

// ClearAccount releases all the cumulated allocations of an account at once
// and primes it for reuse.
func (mm *MemoryUsageMonitor) ClearAccount(ctx context.Context, acc *MemoryAccount) {
	mm.releaseMemory(ctx, acc.curAllocated)
	acc.curAllocated = 0
}

// ClearAccountAndAlloc releases all the cumulated allocations of an account
// at once and primes it for reuse, starting with a first allocation
// of the given size. The account is always closed even if the new
// allocation is refused.
func (mm *MemoryUsageMonitor) ClearAccountAndAlloc(
	ctx context.Context, acc *MemoryAccount, newSize int64,
) error {
	mm.releaseMemory(ctx, acc.curAllocated)
	if err := mm.reserveMemory(ctx, newSize); err != nil {
		return err
	}
	acc.curAllocated = newSize
	return nil
}

// ResizeItem requests a size change for an object already registered
// in an account. The reservation is not modified if the new allocation is
// refused, so that the caller can keep using the original item
// without an accounting error. This is better than calling ClearAccount
// then GrowAccount because if the Clear succeeds and the Grow fails
// the original item becomes invisible from the perspective of the
// monitor.
func (mm *MemoryUsageMonitor) ResizeItem(
	ctx context.Context, acc *MemoryAccount, oldSize, newSize int64,
) error {
	if err := mm.reserveMemory(ctx, newSize); err != nil {
		return err
	}
	mm.releaseMemory(ctx, oldSize)
	acc.curAllocated += newSize - oldSize
	return nil
}
