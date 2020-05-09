// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestSpillingQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	rng, _ := randutil.NewPseudoRand()
	for _, rewindable := range []bool{false, true} {
		for _, memoryLimit := range []int64{10 << 10 /* 10 KiB */, 1<<20 + int64(rng.Intn(64<<20)) /* 1 MiB up to 64 MiB */} {
			alwaysCompress := rng.Float64() < 0.5
			diskQueueCacheMode := colcontainer.DiskQueueCacheModeDefault
			// testReuseCache will test the reuse cache modes.
			testReuseCache := rng.Float64() < 0.5
			dequeuedProbabilityBeforeAllEnqueuesAreDone := 0.5
			if testReuseCache {
				dequeuedProbabilityBeforeAllEnqueuesAreDone = 0
				if rng.Float64() < 0.5 {
					diskQueueCacheMode = colcontainer.DiskQueueCacheModeReuseCache
				} else {
					diskQueueCacheMode = colcontainer.DiskQueueCacheModeClearAndReuseCache
				}
			}
			prefix := ""
			if rewindable {
				dequeuedProbabilityBeforeAllEnqueuesAreDone = 0
				prefix = "Rewindable/"
			}
			numBatches := 1 + rng.Intn(1024)
			t.Run(fmt.Sprintf("%sMemoryLimit=%s/DiskQueueCacheMode=%d/AlwaysCompress=%t/NumBatches=%d",
				prefix, humanizeutil.IBytes(memoryLimit), diskQueueCacheMode, alwaysCompress, numBatches), func(t *testing.T) {
				// Create random input.
				batches := make([]coldata.Batch, 0, numBatches)
				op := coldatatestutils.NewRandomDataOp(testAllocator, rng, coldatatestutils.RandomDataOpArgs{
					NumBatches: cap(batches),
					BatchSize:  1 + rng.Intn(coldata.BatchSize()),
					Nulls:      true,
					BatchAccumulator: func(b coldata.Batch, typs []*types.T) {
						batches = append(batches, coldatatestutils.CopyBatch(b, typs, testColumnFactory))
					},
				})
				typs := op.Typs()

				queueCfg.CacheMode = diskQueueCacheMode
				queueCfg.SetDefaultBufferSizeBytesForCacheMode()
				queueCfg.TestingKnobs.AlwaysCompress = alwaysCompress

				// Create queue.
				var q *spillingQueue
				if rewindable {
					q = newRewindableSpillingQueue(
						testAllocator, typs, memoryLimit, queueCfg,
						colexecbase.NewTestingSemaphore(2), coldata.BatchSize(),
						testDiskAcc,
					)
				} else {
					q = newSpillingQueue(
						testAllocator, typs, memoryLimit, queueCfg,
						colexecbase.NewTestingSemaphore(2), coldata.BatchSize(),
						testDiskAcc,
					)
				}

				// Run verification.
				var (
					b   coldata.Batch
					err error
				)
				ctx := context.Background()
				for {
					b = op.Next(ctx)
					require.NoError(t, q.enqueue(ctx, b))
					if b.Length() == 0 {
						break
					}
					if rng.Float64() < dequeuedProbabilityBeforeAllEnqueuesAreDone {
						if b, err = q.dequeue(ctx); err != nil {
							t.Fatal(err)
						} else if b.Length() == 0 {
							t.Fatal("queue incorrectly considered empty")
						}
						coldata.AssertEquivalentBatches(t, batches[0], b)
						batches = batches[1:]
					}
				}
				numReadIterations := 1
				if rewindable {
					numReadIterations = 2
				}
				for i := 0; i < numReadIterations; i++ {
					batchIdx := 0
					for batches[batchIdx].Length() > 0 {
						if b, err = q.dequeue(ctx); err != nil {
							t.Fatal(err)
						} else if b == nil {
							t.Fatal("unexpectedly dequeued nil batch")
						} else if b.Length() == 0 {
							t.Fatal("queue incorrectly considered empty")
						}
						coldata.AssertEquivalentBatches(t, batches[batchIdx], b)
						batchIdx++
					}

					if b, err := q.dequeue(ctx); err != nil {
						t.Fatal(err)
					} else if b.Length() != 0 {
						t.Fatal("queue should be empty")
					}

					if rewindable {
						require.NoError(t, q.rewind())
					}
				}

				// Close queue.
				require.NoError(t, q.close(ctx))

				// Verify no directories are left over.
				directories, err := queueCfg.FS.List(queueCfg.Path)
				require.NoError(t, err)
				require.Equal(t, 0, len(directories))
			})
		}
	}
}
