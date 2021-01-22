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
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestSpillingQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	ctx := context.Background()
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

			// We need to create a separate unlimited allocator for the spilling
			// queue so that it could measure only its own memory usage
			// (testAllocator might account for other things, thus confusing the
			// spilling queue).
			memAcc := testMemMonitor.MakeBoundAccount()
			defer memAcc.Close(ctx)
			spillingQueueUnlimitedAllocator := colmem.NewAllocator(ctx, &memAcc, testColumnFactory)

			// Create queue.
			var q *spillingQueue
			if rewindable {
				q = newRewindableSpillingQueue(
					&NewSpillingQueueArgs{
						UnlimitedAllocator: spillingQueueUnlimitedAllocator,
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
						UnlimitedAllocator: spillingQueueUnlimitedAllocator,
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

// TestSpillingQueueDidntSpill verifies that in a scenario when every enqueue()
// is followed by dequeue() the non-rewindable spilling queue doesn't actually
// spill to disk.
func TestSpillingQueueDidntSpill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()
	queueCfg.CacheMode = colcontainer.DiskQueueCacheModeDefault
	// We don't expect to spill to disk and we want to give the whole memory
	// limit to the spilling queue's in-memory batch buffer.
	queueCfg.BufferSizeBytes = 1

	rng, _ := randutil.NewPseudoRand()
	numBatches := int(spillingQueueInitialItemsLen)*(1+rng.Intn(4)) + rng.Intn(int(spillingQueueInitialItemsLen))
	op := coldatatestutils.NewRandomDataOp(testAllocator, rng, coldatatestutils.RandomDataOpArgs{
		// TODO(yuzefovich): for some types (e.g. types.MakeArray(types.Int))
		// the memory estimation diverges from 0 after enqueue() / dequeue()
		// sequence. Figure it out.
		DeterministicTyps: []*types.T{types.Int},
		NumBatches:        numBatches,
		BatchSize:         1 + rng.Intn(coldata.BatchSize()),
		Nulls:             true,
	})

	typs := op.Typs()
	// Choose a memory limit such that at most two batches can be kept in the
	// in-memory buffer at a time (single batch is not enough because the queue
	// delays the release of the memory by one batch).
	memoryLimit := int64(2*colmem.EstimateBatchSizeBytes(typs, coldata.BatchSize()) + queueCfg.BufferSizeBytes)
	if memoryLimit < mon.DefaultPoolAllocationSize {
		memoryLimit = mon.DefaultPoolAllocationSize
	}

	// We need to create a separate unlimited allocator for the spilling queue
	// so that it could measure only its own memory usage (testAllocator might
	// account for other things, thus confusing the spilling queue).
	memAcc := testMemMonitor.MakeBoundAccount()
	defer memAcc.Close(ctx)
	spillingQueueUnlimitedAllocator := colmem.NewAllocator(ctx, &memAcc, testColumnFactory)

	q := newSpillingQueue(
		&NewSpillingQueueArgs{
			UnlimitedAllocator: spillingQueueUnlimitedAllocator,
			Types:              typs,
			MemoryLimit:        memoryLimit,
			DiskQueueCfg:       queueCfg,
			FDSemaphore:        colexecbase.NewTestingSemaphore(2),
			DiskAcc:            testDiskAcc,
		},
	)

	for {
		b := op.Next(ctx)
		require.NoError(t, q.enqueue(ctx, b))
		b, err := q.dequeue(ctx)
		require.NoError(t, err)
		if b.Length() == 0 {
			break
		}
	}

	// Ensure that the spilling didn't occur.
	require.False(t, q.spilled())

	// Close queue.
	require.NoError(t, q.close(ctx))

	// Verify no directories are left over.
	directories, err := queueCfg.FS.List(queueCfg.GetPather.GetPath(ctx))
	require.NoError(t, err)
	require.Equal(t, 0, len(directories))
}

// TestSpillingQueueMemoryAccounting is a simple check of the memory accounting
// of the spilling queue that performs a series of enqueue() and dequeue()
// operations and verifies that the reported memory usage is as expected.
//
// Note that this test intentionally doesn't randomize many things (e.g. the
// size of input batches, the types of the vectors) since those randomizations
// would make it hard to compute the expected memory usage (the spilling queue
// has coalescing logic, etc). Thus, the test is more of a sanity check, yet it
// should be sufficient to catch any regressions.
func TestSpillingQueueMemoryAccounting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	rng, _ := randutil.NewPseudoRand()
	typs := []*types.T{types.Int}
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	for _, rewindable := range []bool{false, true} {
		for _, dequeueProbability := range []float64{0, 0.2} {
			if rewindable && dequeueProbability != 0 {
				// For rewindable queues we require that all enqueues occur
				// before any dequeue() call.
				continue
			}
			// We need to create a separate unlimited allocator for the spilling
			// queue so that it could measure only its own memory usage
			// (testAllocator might account for other things, thus confusing the
			// spilling queue).
			memAcc := testMemMonitor.MakeBoundAccount()
			defer memAcc.Close(ctx)
			spillingQueueUnlimitedAllocator := colmem.NewAllocator(ctx, &memAcc, testColumnFactory)

			newQueueArgs := &NewSpillingQueueArgs{
				UnlimitedAllocator: spillingQueueUnlimitedAllocator,
				Types:              typs,
				MemoryLimit:        defaultMemoryLimit,
				DiskQueueCfg:       queueCfg,
				FDSemaphore:        colexecbase.NewTestingSemaphore(2),
				DiskAcc:            testDiskAcc,
			}
			var q *spillingQueue
			if rewindable {
				q = newRewindableSpillingQueue(newQueueArgs)
			} else {
				q = newSpillingQueue(newQueueArgs)
			}

			numInputBatches := int(spillingQueueInitialItemsLen)*(1+rng.Intn(4)) + rng.Intn(int(spillingQueueInitialItemsLen))
			numDequeuedBatches := 0
			batch := coldatatestutils.RandomBatch(testAllocator, rng, typs, coldata.BatchSize(), coldata.BatchSize(), nullProbability)
			batchSize := colmem.GetBatchMemSize(batch)
			getExpectedMemUsage := func(numEnqueuedBatches int) int64 {
				batchesAccountedFor := numEnqueuedBatches
				if !rewindable && numDequeuedBatches > 0 {
					// We release the memory under the dequeued batches only
					// from the non-rewindable queue, and that release is
					// lagging by one batch, so we have -1 here.
					//
					// Note that this logic also works correctly when zero batch
					// has been dequeued once.
					batchesAccountedFor -= numDequeuedBatches - 1
				}
				return int64(batchesAccountedFor) * batchSize
			}
			for numEnqueuedBatches := 1; numEnqueuedBatches <= numInputBatches; numEnqueuedBatches++ {
				require.NoError(t, q.enqueue(ctx, batch))
				if rng.Float64() < dequeueProbability {
					b, err := q.dequeue(ctx)
					require.NoError(t, err)
					coldata.AssertEquivalentBatches(t, batch, b)
					numDequeuedBatches++
				}
				require.Equal(t, getExpectedMemUsage(numEnqueuedBatches), q.unlimitedAllocator.Used())
			}
			require.NoError(t, q.enqueue(ctx, coldata.ZeroBatch))
			for {
				b, err := q.dequeue(ctx)
				require.NoError(t, err)
				numDequeuedBatches++
				require.Equal(t, getExpectedMemUsage(numInputBatches), q.unlimitedAllocator.Used())
				if b.Length() == 0 {
					break
				}
				coldata.AssertEquivalentBatches(t, batch, b)
			}

			// Some sanity checks.
			require.False(t, q.spilled())
			require.NoError(t, q.close(ctx))
			directories, err := queueCfg.FS.List(queueCfg.GetPather.GetPath(ctx))
			require.NoError(t, err)
			require.Equal(t, 0, len(directories))
		}
	}
}
