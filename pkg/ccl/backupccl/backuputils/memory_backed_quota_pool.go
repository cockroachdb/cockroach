// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backuputils

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/redact"
)

// MemoryBackedQuotaPool is an IntPool backed up by a memory monitor.
// Users of MemoryBackedQuotaPool can acquire capacity from the IntPool,
// but the capacity of the IntPool can only be increased by acquiring
// the corresponding amount of memory from the backing memory monitor.
type MemoryBackedQuotaPool struct {
	mon *mon.BytesMonitor
	mem *mon.BoundAccount

	quotaPool *quotapool.IntPool
}

// NewMemoryBackedQuotaPool creates an MemoryBackedQuotaPool from a
// parent monitor m with a limit.
func NewMemoryBackedQuotaPool(
	ctx context.Context, m *mon.BytesMonitor, name redact.RedactableString, limit int64,
) *MemoryBackedQuotaPool {
	q := MemoryBackedQuotaPool{
		quotaPool: quotapool.NewIntPool(fmt.Sprintf("%s-pool", name), 0),
	}

	if m != nil {
		q.mon = mon.NewMonitorInheritWithLimit(name, limit, m)
		q.mon.StartNoReserved(ctx, m)
		mem := q.mon.MakeBoundAccount()
		q.mem = &mem
	}
	return &q
}

// TryAcquireMaybeIncreaseCapacity tries to acquire size from the pool.
// On success, a non-nil alloc is returned and Release() must be called on
// it to return the quota to the pool.
// If the acquire fails because of not enough quota, it will attempt
// to increase the capacity of the pool before immediately returning
// with the error quotapool.ErrNotEnoughQuota.
//
// Safe for concurrent use.
func (q *MemoryBackedQuotaPool) TryAcquireMaybeIncreaseCapacity(
	ctx context.Context, size uint64,
) (*quotapool.IntAlloc, error) {
	return q.quotaPool.TryAcquireFunc(ctx, func(ctx context.Context, p quotapool.PoolInfo) (took uint64, err error) {
		if p.Available >= size {
			return size, nil
		}

		// Not enough quota, attempt to grow the memory to increase quota pool
		// capacity.
		//
		// NB: we can update capacity despite concurrent usage of the quota pool
		// because processing of requests in AbstractPool.Acquire is single threaded.
		q.IncreaseCapacity(ctx, size)

		return 0, quotapool.ErrNotEnoughQuota
	})
}

// Acquire acquires size from the pool. On success, a non-nil alloc is
// returned and Release() must be called on it to return the quota to the pool.
//
// Safe for concurrent use.
func (q *MemoryBackedQuotaPool) Acquire(
	ctx context.Context, size uint64,
) (*quotapool.IntAlloc, error) {
	return q.quotaPool.Acquire(ctx, size)
}

// Release will release allocs back to the pool.
func (q *MemoryBackedQuotaPool) Release(allocs ...*quotapool.IntAlloc) {
	q.quotaPool.Release(allocs...)
}

// IncreaseCapacity will attempt to increase the capacity of the pool.
// Returns true if the increase succeeds and false otherwise.
//
// This is NOT safe for concurrent use.
func (q *MemoryBackedQuotaPool) IncreaseCapacity(ctx context.Context, size uint64) bool {
	if err := q.mem.Grow(ctx, int64(size)); err != nil {
		return false
	}
	q.quotaPool.UpdateCapacity(q.quotaPool.Capacity() + size)
	return true
}

// Capacity returns the capacity of the pool.
func (q *MemoryBackedQuotaPool) Capacity() uint64 {
	return q.quotaPool.Capacity()
}

// Close closes the pool and returns the reserved memory to the backing
// memory monitor.
func (q *MemoryBackedQuotaPool) Close(ctx context.Context) {
	if q.mem != nil {
		q.mem.Close(ctx)
	}

	if q.mon != nil {
		q.mon.Stop(ctx)
	}
}
