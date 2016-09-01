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
	"golang.org/x/net/context"
)

// BoundAccount implements a MemoryAccount attached to a specific
// monitor.
type BoundAccount struct {
	MemoryAccount
	mon *MemoryMonitor
}

// MakeStandaloneBudget creates a BoundAccount suitable for root
// monitors.
func MakeStandaloneBudget(capacity int64) BoundAccount {
	return BoundAccount{MemoryAccount{curAllocated: capacity}, nil}
}

// MakeBoundAccount greates a BoundAccount connected to the given monitor.
func (mm *MemoryMonitor) MakeBoundAccount(
	ctx context.Context, initialAllocation int64,
) (res BoundAccount, err error) {
	if err := mm.OpenAndInitAccount(ctx, &res.MemoryAccount, initialAllocation); err != nil {
		return res, err
	}
	res.mon = mm
	return res, nil
}

// Close is an accessor for b.mon.CloseAccount.
func (b *BoundAccount) Close(ctx context.Context) {
	if b.mon != nil {
		b.mon.CloseAccount(ctx, &b.MemoryAccount)
	}
}
