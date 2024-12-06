// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backuputils

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func getMemoryMonitor(limit int64) *mon.BytesMonitor {
	return mon.NewMonitor(mon.Options{
		Name:      mon.MakeMonitorName("test-mon"),
		Limit:     limit,
		Increment: 1,
		Settings:  cluster.MakeTestingClusterSettings(),
	})
}

func TestMemoryBackedQuotaPool(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	t.Run("try-acquire", func(t *testing.T) {
		// Make a quota pool of limit 10.
		qp, cleanup := makeTestingQuotaPool(ctx, 10)
		defer cleanup()

		// Trying to acquire 5 should succeed and increase capacity.
		alloc1, err := qp.TryAcquireMaybeIncreaseCapacity(ctx, 5)
		require.NoError(t, err)
		require.Equal(t, uint64(5), qp.Capacity())

		// Trying to acquire 5 should succeed and increase capacity.
		alloc2, err := qp.TryAcquireMaybeIncreaseCapacity(ctx, 5)
		require.NoError(t, err)
		require.Equal(t, uint64(10), qp.Capacity())
		defer qp.Release(alloc1, alloc2)

		// Trying to acquire 1 should fail since the pool is at limit.
		_, err = qp.TryAcquireMaybeIncreaseCapacity(ctx, 1)
		require.ErrorIs(t, err, quotapool.ErrNotEnoughQuota)
		require.Equal(t, uint64(10), qp.Capacity())
	})

	t.Run("acquire", func(t *testing.T) {
		// Make a quota pool of limit 10.
		qp, cleanup := makeTestingQuotaPool(ctx, 10)
		defer cleanup()

		// Acquire an alloc equal to limit.
		alloc1, err := qp.TryAcquireMaybeIncreaseCapacity(ctx, 10)
		require.NoError(t, err)
		require.Equal(t, uint64(10), qp.Capacity())

		// The second acquire of 1 should wait until the timeout.
		timeoutCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		_, err = qp.Acquire(timeoutCtx, 1)
		require.ErrorIs(t, err, context.DeadlineExceeded)

		// Release the first alloc, the acquire should now succeed.
		qp.Release(alloc1)

		alloc2, err := qp.Acquire(ctx, 1)
		require.NoError(t, err)
		qp.Release(alloc2)
	})

	t.Run("external-mon-user", func(t *testing.T) {
		// Create a quota pool of limit 10.
		const limit = 10
		mm := getMemoryMonitor(limit)
		mm.Start(ctx, nil, mon.NewStandaloneBudget(limit))
		defer mm.Stop(ctx)

		qp := NewMemoryBackedQuotaPool(ctx, mm, "test-qp", limit)
		defer qp.Close(ctx)

		// Create another bound account of the parent monitor and
		// acquire 5.
		externMem := mm.MakeBoundAccount()
		err := externMem.Grow(ctx, 5)
		require.NoError(t, err)

		// The first acquire of 5 should succeed.
		_, err = qp.TryAcquireMaybeIncreaseCapacity(ctx, 5)
		require.NoError(t, err)
		require.Equal(t, uint64(5), qp.Capacity())

		// Acquiring 1 more should error with ErrNotEnoughQuota because the capacity
		// cannot increase anymore despite being under the limit.
		_, err = qp.TryAcquireMaybeIncreaseCapacity(ctx, 1)
		require.ErrorIs(t, err, quotapool.ErrNotEnoughQuota)

		// Increasing the capacity should also fail.
		require.Error(t, qp.IncreaseCapacity(ctx, 1))

		// After shrinking the other bound account, acquiring from the quota pool
		// should succeed.
		externMem.Shrink(ctx, 5)
		defer externMem.Close(ctx)
		_, err = qp.TryAcquireMaybeIncreaseCapacity(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, uint64(6), qp.Capacity())

		// Increasing the capacity should also succeed.
		require.NoError(t, qp.IncreaseCapacity(ctx, 1))
		require.Equal(t, uint64(7), qp.Capacity())
	})
}

// TestMemoryBackedQuotaPoolConcurrent tests the functionality of the
// MemoryBackedQuotaPool in the presence of multiple goroutines acquiring and
// releasing quota, also with multiple goroutines acquiring and releasing memory
// from the parent memory monitor.
func TestMemoryBackedQuotaPoolConcurrent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, quota := range []int64{1, 10, 100, 1000} {
		for _, numGoroutines := range []int{1, 10, 100} {
			quota := quota
			ctx := context.Background()
			mm := getMemoryMonitor(quota)
			mm.Start(ctx, nil, mon.NewStandaloneBudget(quota))
			mem := mm.MakeConcurrentBoundAccount()

			qp := NewMemoryBackedQuotaPool(ctx, mm, "test-qp", quota)
			res := make(chan error, numGoroutines)
			require.NoError(t, qp.IncreaseCapacity(ctx, 1))

			for i := 0; i < numGoroutines; i++ {
				go func() {
					memReq := int64(rand.Intn(int(quota)))
					if err := mem.Grow(ctx, memReq); err != nil {
						return
					}

					defer mem.Shrink(ctx, memReq)
				}()

				go func() {
					alloc, err := qp.TryAcquireMaybeIncreaseCapacity(ctx, 1)
					if err != nil {
						if !errors.Is(err, quotapool.ErrNotEnoughQuota) {
							res <- err
							return
						}

						alloc, err = qp.Acquire(ctx, 1)
						if err != nil {
							res <- err
							return
						}
					}

					defer alloc.Release()
					res <- nil
				}()
			}

			for i := 0; i < numGoroutines; i++ {
				select {
				case <-time.After(time.Minute):
					t.Fatalf("timeout after 1m")
				case err := <-res:
					if err != nil {
						t.Fatal(err)
					}
				}
			}
		}
	}
}

func makeTestingQuotaPool(
	ctx context.Context, limit int64,
) (qp *MemoryBackedQuotaPool, cleanup func()) {
	mm := getMemoryMonitor(limit)
	mm.Start(ctx, nil, mon.NewStandaloneBudget(limit))

	qp = NewMemoryBackedQuotaPool(ctx, mm, "test-qp", limit)
	cleanup = func() {
		qp.Close(ctx)
		mm.Stop(ctx)
	}
	return qp, cleanup
}
