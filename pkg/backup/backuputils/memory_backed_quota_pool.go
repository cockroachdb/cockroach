// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backuputils

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// MemoryBackedQuotaPool is an IntPool backed up by a memory monitor. Users of
// MemoryBackedQuotaPool can acquire capacity from the IntPool, but the capacity
// of the IntPool can only be increased by acquiring the corresponding amount of
// memory from the backing memory monitor.
type MemoryBackedQuotaPool struct {
	mon *mon.BytesMonitor
	mem *mon.BoundAccount

	// capacityMu synchronizes operations related to the capacity
	// of quotaPool.
	capacityMu syncutil.Mutex
	quotaPool  *quotapool.IntPool
}

// NewMemoryBackedQuotaPool creates a MemoryBackedQuotaPool from a parent
// monitor m with a limit.
func NewMemoryBackedQuotaPool(
	ctx context.Context, m *mon.BytesMonitor, name redact.SafeString, limit int64,
) *MemoryBackedQuotaPool {
	q := MemoryBackedQuotaPool{
		quotaPool: quotapool.NewIntPool(fmt.Sprintf("%s-pool", name), 0),
	}

	if m != nil {
		q.mon = mon.NewMonitorInheritWithLimit(name, limit, m, false /* longLiving */)
		q.mon.StartNoReserved(ctx, m)
		mem := q.mon.MakeBoundAccount()
		q.mem = &mem
	}
	return &q
}

// TryAcquireMaybeIncreaseCapacity tries to acquire size from the pool. On
// success, a non-nil alloc is returned and Release() must be called on it to
// return the quota to the pool. If the acquire fails because of not enough
// quota, it will repeatedly attempt to increase the capacity of the pool until
// the acquire succeeds. If the capacity increase fails, then the function will
// return with the error quotapool.ErrNotEnoughQuota.
//
// Safe for concurrent use.
func (q *MemoryBackedQuotaPool) TryAcquireMaybeIncreaseCapacity(
	ctx context.Context, size uint64,
) (*quotapool.IntAlloc, error) {
	for {
		alloc, err := q.quotaPool.TryAcquire(ctx, size)
		if err == nil || !errors.Is(err, quotapool.ErrNotEnoughQuota) {
			return alloc, err
		}

		// Not enough quota, attempt to grow the memory to increase quota pool
		// capacity
		if err := q.IncreaseCapacity(ctx, size); err != nil {
			return nil, quotapool.ErrNotEnoughQuota
		}
	}
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

// IncreaseCapacity will attempt to increase the capacity of the pool. Returns
// true if the increase succeeds and false otherwise.
//
// Safe for concurrent use.
func (q *MemoryBackedQuotaPool) IncreaseCapacity(ctx context.Context, size uint64) error {
	q.capacityMu.Lock()
	defer q.capacityMu.Unlock()

	if err := q.mem.Grow(ctx, int64(size)); err != nil {
		c := q.quotaPool.Capacity()
		return errors.Wrapf(err, "failed to increase capacity from %d to %d", c, c+size)
	}

	q.quotaPool.UpdateCapacity(q.quotaPool.Capacity() + size)
	return nil
}

// Capacity returns the capacity of the pool.
func (q *MemoryBackedQuotaPool) Capacity() uint64 {
	q.capacityMu.Lock()
	defer q.capacityMu.Unlock()
	return q.quotaPool.Capacity()
}

// Close closes the pool and returns the reserved memory to the backing memory
// monitor.
func (q *MemoryBackedQuotaPool) Close(ctx context.Context) {
	if q.mem != nil {
		q.mem.Close(ctx)
	}

	if q.mon != nil {
		q.mon.Stop(ctx)
	}
}
