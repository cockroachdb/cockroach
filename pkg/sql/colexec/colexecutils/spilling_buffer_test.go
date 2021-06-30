// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecutils

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestSpillingBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	ctx := context.Background()
	rng, _ := randutil.NewPseudoRand()

	for _, memoryLimit := range []int64{
		10 << 10,                        /* 10 KiB */
		1<<20 + int64(rng.Intn(63<<20)), /* 1 MiB up to 64 MiB */
		1 << 30,                         /* 1 GiB */
	} {
		alwaysCompress := rng.Float64() < 0.5
		const maxBatches = 20
		numBatches := 1 + rng.Intn(maxBatches)
		inputBatchSize := 1 + rng.Intn(coldata.BatchSize())
		const maxNumTuples = 10000
		if numBatches*inputBatchSize > maxNumTuples {
			// Limit the maximum number of tuples to keep the testing time short.
			inputBatchSize = maxNumTuples / numBatches
		}
		// Generate at least one type.
		const maxCols = 10
		typs := make([]*types.T, 1+rng.Intn(maxCols))
		for i := range typs {
			typs[i] = randgen.RandType(rng)
		}
		const storeColProbability = 0.75
		colsToStore := make([]int, 0, len(typs))
		typesToStore := make([]*types.T, 0, len(typs))
		for i := range typs {
			if rng.Float64() < storeColProbability {
				colsToStore = append(colsToStore, i)
				typesToStore = append(typesToStore, typs[i])
			}
		}
		if len(colsToStore) == 0 {
			// Always store at least one column.
			idx := rng.Intn(len(typs))
			colsToStore = append(colsToStore, idx)
			typesToStore = append(typesToStore, typs[idx])
		}

		// Add a limit on the number of batches added to the in-memory
		// buffer of the spilling queue. We will set it to half of the total
		// number of batches which allows us to exercise the case when the
		// spilling to disk queue occurs after some batches were added to
		// the in-memory buffer.
		setInMemTuplesLimit := rng.Float64() < 0.5
		log.Infof(context.Background(), "MemoryLimit=%s/AlwaysCompress=%t/NumBatches=%d/InMemTuplesLimited=%t",
			humanizeutil.IBytes(memoryLimit), alwaysCompress, numBatches, setInMemTuplesLimit)

		// Since the spilling buffer appends tuples to an AppendOnlyBufferedBatch,
		// we cannot use the batches we get from GetWindowedBatch directly. Instead,
		// we are tracking all of the input tuples and will be comparing against a
		// window into them.
		var tuples *AppendOnlyBufferedBatch
		// Create random input.
		op := coldatatestutils.NewRandomDataOp(testAllocator, rng, coldatatestutils.RandomDataOpArgs{
			NumBatches:        numBatches,
			BatchSize:         inputBatchSize,
			Nulls:             true,
			DeterministicTyps: typs,
			BatchAccumulator: func(_ context.Context, b coldata.Batch, typs []*types.T) {
				if b.Length() == 0 {
					return
				}
				if tuples == nil {
					tuples = NewAppendOnlyBufferedBatch(testAllocator, typs, colsToStore)
				}
				tuples.AppendTuples(b, 0 /* startIdx */, b.Length())
			},
		})
		op.Init(ctx)

		queueCfg.CacheMode = colcontainer.DiskQueueCacheModeClearAndReuseCache
		queueCfg.SetDefaultBufferSizeBytesForCacheMode()
		queueCfg.TestingKnobs.AlwaysCompress = alwaysCompress

		// We need to create a separate unlimited allocator for the spilling
		// buffer so that it measures only its own memory usage (testAllocator might
		// account for other things, thus confusing the spilling buffer).
		memAcc := testMemMonitor.MakeBoundAccount()
		defer memAcc.Close(ctx)
		spillingQueueUnlimitedAllocator := colmem.NewAllocator(ctx, &memAcc, testColumnFactory)

		// Create buffer.
		buf := NewSpillingBuffer(
			spillingQueueUnlimitedAllocator, memoryLimit, queueCfg,
			colexecop.NewTestingSemaphore(2), typs, testDiskAcc, colsToStore...,
		)
		if setInMemTuplesLimit {
			buf.testingKnobs.maxTuplesStoredInMemory = numBatches * inputBatchSize / 2
		}

		// Run verification.
		testBatch := coldata.NewMemBatchNoCols(typesToStore, 0 /* capacity */)
		oracleBatch := coldata.NewMemBatchNoCols(typesToStore, 0 /* capacity */)
		checkWindowAtIndex := func(startIdx int) (nextIdx int) {
			var vec coldata.Vec
			var idx, length int
			for i, colIdx := range colsToStore {
				vec, idx, length = buf.GetVecWithTuple(ctx, i, startIdx)
				if vec.Length() == 0 {
					t.Fatalf("buffer failed to return vector containing index %d", startIdx)
				}
				testBatch.ReplaceCol(vec, i)
				window := tuples.ColVec(colIdx).Window(startIdx-idx, startIdx-idx+length)
				oracleBatch.ReplaceCol(window, i)
			}
			testBatch.SetSelection(false)
			testBatch.SetLength(length)
			oracleBatch.SetSelection(false)
			oracleBatch.SetLength(length)
			coldata.AssertEquivalentBatches(t, oracleBatch, testBatch)

			return startIdx + (length - idx)
		}

		// Until the input is consumed, perform all appends and then verify that the
		// stored tuples are correct, with a random chance to check all tuples
		// stored so far, reset the buffer and oracle batch, and pick up where the
		// input left off.
		for {
			const resetProbability = 0.2
			reset := false
			// Append input tuples.
			var b coldata.Batch
			for {
				b = op.Next()
				if b.Length() == 0 {
					break
				}
				buf.AppendTuples(ctx, b, 0, b.Length())
				if rng.Float64() < resetProbability {
					reset = true
					break
				}
			}
			// Read all stored tuples and check that they are correct.
			require.Equal(t, buf.Length(), tuples.Length())
			var startIdx int
			probabilityCheckRandIndex := 0.5
			for {
				if tuples.Length() > 0 && rng.Float64() < probabilityCheckRandIndex {
					checkWindowAtIndex(rng.Intn(tuples.Length()))
				}
				if startIdx >= tuples.Length() {
					// We have verified that all stored tuples are correct.
					break
				}
				startIdx = checkWindowAtIndex(startIdx)
			}
			if reset {
				// The input has not been fully consumed, but we need to reset the
				// buffer as well as the oracle AppendOnlyBufferedBatch before once
				// again appending and reading.
				buf.Reset(ctx)
				tuples.SetLength(0)
				reset = false
				continue
			}
			// All input tuples have been stored and verified.
			break
		}
	}
}
