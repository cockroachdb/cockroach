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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestSpillingQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	rng, _ := randutil.NewPseudoRand()
	for _, rewindable := range []bool{false, true} {
		for _, memoryLimit := range []int64{
			10 << 10,                        /* 10 KiB */
			1<<20 + int64(rng.Intn(63<<20)), /* 1 MiB up to 64 MiB */
			1 << 30,                         /* 1 GiB */
		} {
			alwaysCompress := rng.Float64() < 0.5
			diskQueueCacheMode := colcontainer.DiskQueueCacheModeDefault
			var dequeuedProbabilityBeforeAllEnqueuesAreDone float64
			// testReuseCache will test the reuse cache modes.
			testReuseCache := rng.Float64() < 0.5
			if testReuseCache {
				dequeuedProbabilityBeforeAllEnqueuesAreDone = 0
				if rng.Float64() < 0.5 {
					diskQueueCacheMode = colcontainer.DiskQueueCacheModeReuseCache
				} else {
					diskQueueCacheMode = colcontainer.DiskQueueCacheModeClearAndReuseCache
				}
			} else if rng.Float64() < 0.5 {
				dequeuedProbabilityBeforeAllEnqueuesAreDone = 0.5
			}
			prefix := ""
			if rewindable {
				dequeuedProbabilityBeforeAllEnqueuesAreDone = 0
				prefix = "Rewindable/"
			}
			numBatches := int(spillingQueueInitialItemsLen)*(1+rng.Intn(2)) + rng.Intn(int(spillingQueueInitialItemsLen))
			inputBatchSize := 1 + rng.Intn(coldata.BatchSize())
			const maxNumTuples = 10000
			if numBatches*inputBatchSize > maxNumTuples {
				// When we happen to choose very large value for
				// coldata.BatchSize() and for spillingQueueInitialItemsLen, the
				// test might take non-trivial amount of time, so we'll limit
				// the number of tuples.
				inputBatchSize = maxNumTuples / numBatches
			}
			// Add a limit on the number of batches added to the in-memory
			// buffer of the spilling queue. We will set it to half of the total
			// number of batches which allows us to exercise the case when the
			// spilling to disk queue occurs after some batches were added to
			// the in-memory buffer.
			setInMemEnqueuesLimit := rng.Float64() < 0.5
			log.Infof(context.Background(), "%sMemoryLimit=%s/DiskQueueCacheMode=%d/AlwaysCompress=%t/NumBatches=%d/InMemEnqueuesLimited=%t",
				prefix, humanizeutil.IBytes(memoryLimit), diskQueueCacheMode, alwaysCompress, numBatches, setInMemEnqueuesLimit)
			// Since the spilling queue coalesces tuples to fill-in the batches
			// up to their capacity, we cannot use the batches we get when
			// dequeueing directly. Instead, we are tracking all of the input
			// tuples and will be comparing against a window into them.
			var tuples *appendOnlyBufferedBatch
			// Create random input.
			op := coldatatestutils.NewRandomDataOp(testAllocator, rng, coldatatestutils.RandomDataOpArgs{
				NumBatches: numBatches,
				BatchSize:  inputBatchSize,
				Nulls:      true,
				BatchAccumulator: func(b coldata.Batch, typs []*types.T) {
					if b.Length() == 0 {
						return
					}
					if tuples == nil {
						tuples = newAppendOnlyBufferedBatch(testAllocator, typs, nil /* colsToStore */)
					}
					tuples.append(b, 0 /* startIdx */, b.Length())
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
					&NewSpillingQueueArgs{
						UnlimitedAllocator: testAllocator,
						Types:              typs,
						MemoryLimit:        memoryLimit,
						DiskQueueCfg:       queueCfg,
						FDSemaphore:        colexecbase.NewTestingSemaphore(2),
						DiskAcc:            testDiskAcc,
					},
				)
			} else {
				q = newSpillingQueue(
					&NewSpillingQueueArgs{
						UnlimitedAllocator: testAllocator,
						Types:              typs,
						MemoryLimit:        memoryLimit,
						DiskQueueCfg:       queueCfg,
						FDSemaphore:        colexecbase.NewTestingSemaphore(2),
						DiskAcc:            testDiskAcc,
					},
				)
			}

			if setInMemEnqueuesLimit {
				q.testingKnobs.maxNumBatchesEnqueuedInMemory = numBatches / 2
			}

			// Run verification.
			var (
				b                        coldata.Batch
				err                      error
				numAlreadyDequeuedTuples int
				// Apart from tracking all input tuples we will be tracking all
				// of the dequeued batches and their lengths separately (without
				// deep-copying them).
				// The implementation of dequeue() method is such that if the
				// queue doesn't spill to disk, we can safely keep the
				// references to the dequeued batches because a new batch is
				// allocated whenever it is kept in the in-memory buffer (which
				// is not the case when dequeueing from disk).
				dequeuedBatches      []coldata.Batch
				dequeuedBatchLengths []int
			)

			windowedBatch := coldata.NewMemBatchNoCols(typs, coldata.BatchSize())
			getNextWindowIntoTuples := func(windowLen int) coldata.Batch {
				// makeWindowIntoBatch creates a window into tuples in the range
				// [numAlreadyDequeuedTuples; tuples.length), but we want the
				// range [numAlreadyDequeuedTuples; numAlreadyDequeuedTuples +
				// windowLen), so we'll temporarily set the length of tuples to
				// the desired value and restore it below.
				numTuples := tuples.Length()
				tuples.SetLength(numAlreadyDequeuedTuples + windowLen)
				makeWindowIntoBatch(windowedBatch, tuples, numAlreadyDequeuedTuples, typs)
				tuples.SetLength(numTuples)
				numAlreadyDequeuedTuples += windowLen
				return windowedBatch
			}

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
					coldata.AssertEquivalentBatches(t, getNextWindowIntoTuples(b.Length()), b)
					dequeuedBatches = append(dequeuedBatches, b)
					dequeuedBatchLengths = append(dequeuedBatchLengths, b.Length())
				}
			}
			numDequeuedTuplesBeforeReading := numAlreadyDequeuedTuples
			numDequeuedBatchesBeforeReading := len(dequeuedBatches)
			numReadIterations := 1
			if rewindable {
				numReadIterations = 2
			}
			for i := 0; i < numReadIterations; i++ {
				for {
					if b, err = q.dequeue(ctx); err != nil {
						t.Fatal(err)
					} else if b == nil {
						t.Fatal("unexpectedly dequeued nil batch")
					} else if b.Length() == 0 {
						break
					}
					coldata.AssertEquivalentBatches(t, getNextWindowIntoTuples(b.Length()), b)
					dequeuedBatches = append(dequeuedBatches, b)
					dequeuedBatchLengths = append(dequeuedBatchLengths, b.Length())
				}

				if !q.spilled() {
					// Let's verify that all of the dequeued batches equal to
					// all of the input tuples. We need to unset
					// numAlreadyDequeuedTuples so that we start getting
					// "windows" from the very beginning.
					numAlreadyDequeuedTuples = 0
					for i, b := range dequeuedBatches {
						coldata.AssertEquivalentBatches(t, getNextWindowIntoTuples(dequeuedBatchLengths[i]), b)
					}
				}

				if rewindable {
					require.NoError(t, q.rewind())
					numAlreadyDequeuedTuples = numDequeuedTuplesBeforeReading
					dequeuedBatches = dequeuedBatches[:numDequeuedBatchesBeforeReading]
					dequeuedBatchLengths = dequeuedBatchLengths[:numDequeuedBatchesBeforeReading]
				}
			}

			// Close queue.
			require.NoError(t, q.close(ctx))

			// Verify no directories are left over.
			directories, err := queueCfg.FS.List(queueCfg.GetPather.GetPath(ctx))
			require.NoError(t, err)
			require.Equal(t, 0, len(directories))
		}
	}
}
