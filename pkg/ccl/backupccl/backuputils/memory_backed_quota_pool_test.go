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
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// TestMemoryBackedQuotaPoolBasic tests the basic functionality
// of the MemoryBackedQuotaPool in the presence of multiple
// goroutines acquiring and releasing quota, also with multiple
// goroutines acquiring and releasing memory from the parent
// memory monitor.
func TestMemoryBackedQuotaPoolBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, quota := range []int64{1, 10, 100, 1000} {
		for _, numGoroutines := range []int{1, 10, 100} {
			quota := quota
			ctx := context.Background()
			mm := mon.NewMonitorWithLimit(
				"test-mon", mon.MemoryResource, quota,
				nil, nil, 1, 0,
				cluster.MakeTestingClusterSettings())
			mm.Start(ctx, nil, mon.NewStandaloneBudget(quota))
			mem := mm.MakeBoundAccount()
			mem.Mu = &syncutil.Mutex{}

			qp := NewMemoryBackedQuotaPool(ctx, mm, "test-qp", quota)
			res := make(chan error, numGoroutines)
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
					return
				}()
			}

			for i := 0; i < numGoroutines; i++ {
				select {
				case <-time.After(5 * time.Second):
					t.Fatalf("timeout after 5s")
				case err := <-res:
					if err != nil {
						t.Fatal(err)
					}
				}
			}
		}
	}
}
