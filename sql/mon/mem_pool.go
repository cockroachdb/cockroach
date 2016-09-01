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
	"github.com/cockroachdb/cockroach/util/humanizeutil"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/pkg/errors"

	"golang.org/x/net/context"
)

// MemoryPool implements a concurrent reservation system
// for memory.
// The API for this class is:
// - use a MemoryAccount per client to the pool;
// - to allocate memory, use growAccount();
// - to free memory, use shrinkAccount();
// - when finished, use closeAccount().
//
// The main client is MemoryUsageMonitor, see the comments in
// mem_usage.go for details.
type MemoryPool struct {
	// maxBytes is the maximum amount that can be allocated
	// across all clients.
	maxBytes int64

	mu struct {
		syncutil.Mutex
		// curBytes is the current sum of all client accounts.
		curBytes int64
	}
}

// MakeMemoryPool instanciates a fresh pool that supports up to the
// given maximum number of bytes of allocation.
func MakeMemoryPool(maxBytes int64) MemoryPool {
	return MemoryPool{maxBytes: maxBytes}
}

// growAccount attempts to grow the account by the specified extraSize
// bytes. Returns an error if the capacity of this pool is exceeded.
func (p *MemoryPool) growAccount(ctx context.Context, acc *MemoryAccount, extraSize int64) error {
	p.mu.Lock()
	if p.mu.curBytes > p.maxBytes-extraSize {
		err := errors.Errorf("memory budget exceeded: %d bytes requested, %s already allocated",
			extraSize,
			humanizeutil.IBytes(acc.curAllocated))
		if log.V(2) {
			log.Errorf(ctx, "%s, %s global, %s global max",
				err,
				humanizeutil.IBytes(p.mu.curBytes),
				humanizeutil.IBytes(p.maxBytes))
		}
		return err
	}
	p.mu.curBytes += extraSize
	p.mu.Unlock()

	acc.curAllocated += extraSize
	return nil
}

// closeAccount de-allocates the specified account from the pool.
func (p *MemoryPool) closeAccount(acc *MemoryAccount) {
	p.shrink(acc.curAllocated)
}

// shrinkAccount reduces the account down to the new specified
// size. The newSz must be smaller than the current allocated amount
// in this account.
func (p *MemoryPool) shrinkAccount(acc *MemoryAccount, newSz int64) {
	delta := acc.curAllocated - newSz
	if acc.curAllocated < delta {
		panic("not enough memory to release")
	}

	p.shrink(delta)
	acc.curAllocated -= delta
}

// shrink lowers the curBytes in this pool by delta. delta must be
// positive or zero.
func (p *MemoryPool) shrink(delta int64) {
	p.mu.Lock()
	if p.mu.curBytes < delta {
		panic("not enough memory to release")
	}
	p.mu.curBytes -= delta
	p.mu.Unlock()
}
