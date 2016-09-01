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
type MemoryPool struct {
	max int64

	mu struct {
		syncutil.Mutex
		cur int64
	}
}

// MakeMemoryPool instanciates a fresh pool that supports up to the
// given maximum number of bytes of allocation.
func MakeMemoryPool(max int64) MemoryPool {
	return MemoryPool{max: max}
}

// growAccount attempts to grow the account by the specified extraSize
// bytes.  Returns an error if the capacity of this pool is exceeded.
func (p *MemoryPool) growAccount(ctx context.Context, acc *MemoryAccount, extraSize int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.mu.cur > p.max-extraSize {
		err := errors.Errorf("memory budget exceeded: %d requested, %s already allocated",
			extraSize,
			humanizeutil.IBytes(acc.curAllocated))
		if log.V(2) {
			log.Errorf(ctx, "%s, %s global, %s global max",
				err,
				humanizeutil.IBytes(p.mu.cur),
				humanizeutil.IBytes(p.max))
		}
		return err
	}
	p.mu.cur += extraSize
	acc.curAllocated += extraSize
	return nil
}

// closeAccount de-allocates the specified account from the pool.
func (p *MemoryPool) closeAccount(acc *MemoryAccount) {
	p.shrink(acc.curAllocated)
}

// shrinkAccount reduces the account down to the new specified
// size.
func (p *MemoryPool) shrinkAccount(acc *MemoryAccount, newSz int64) {
	delta := acc.curAllocated - newSz
	if acc.curAllocated < delta {
		panic("not enough memory to release")
	}

	p.shrink(delta)
	acc.curAllocated -= delta
}

func (p *MemoryPool) shrink(delta int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mu.cur < delta {
		panic("not enough memory to release")
	}
	p.mu.cur -= delta
}
