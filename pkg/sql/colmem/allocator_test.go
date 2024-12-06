// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colmem_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func init() {
	randutil.SeedForTests()
}

const increment = -1

func getAllocator(increment int64) (_ *colmem.Allocator, _ *mon.BoundAccount, cleanup func()) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	testMemMonitor := mon.NewMonitor(mon.Options{
		Name:      mon.MakeMonitorName("test-mem"),
		Increment: increment,
		Settings:  st,
	})
	testMemMonitor.Start(ctx, nil, mon.NewStandaloneBudget(math.MaxInt64))
	memAcc := testMemMonitor.MakeBoundAccount()
	evalCtx := eval.MakeTestingEvalContext(st)
	testColumnFactory := coldataext.NewExtendedColumnFactory(&evalCtx)
	testAllocator := colmem.NewAllocator(ctx, &memAcc, testColumnFactory)
	cleanup = func() {
		memAcc.Close(ctx)
		testMemMonitor.Stop(ctx)
	}
	return testAllocator, &memAcc, cleanup
}

func TestMaybeAppendColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testAllocator, _, cleanup := getAllocator(increment)
	defer cleanup()
	t.Run("VectorAlreadyPresent", func(t *testing.T) {
		b := testAllocator.NewMemBatchWithMaxCapacity([]*types.T{types.Int})
		b.SetLength(coldata.BatchSize())
		colIdx := 0

		// We expect an error to occur because of a type mismatch.
		err := colexecerror.CatchVectorizedRuntimeError(func() {
			testAllocator.MaybeAppendColumn(b, types.Float, colIdx)
		})
		require.NotNil(t, err)

		// We expect that the old vector is reallocated because the present one
		// is made to be of insufficient capacity.
		b.ReplaceCol(testAllocator.NewVec(types.Int, 1 /* capacity */), colIdx)
		testAllocator.MaybeAppendColumn(b, types.Int, colIdx)
		require.Equal(t, coldata.BatchSize(), b.ColVec(colIdx).Capacity())

		// We expect that Bytes vector is reset when it is being reused (if it
		// isn't, a panic will occur when we try to set at the same positions).
		bytesColIdx := 1
		testAllocator.MaybeAppendColumn(b, types.Bytes, bytesColIdx)
		b.ColVec(bytesColIdx).Bytes().Set(0, []byte{0})
		b.ColVec(bytesColIdx).Bytes().Set(1, []byte{1})
		testAllocator.MaybeAppendColumn(b, types.Bytes, bytesColIdx)
		b.ColVec(bytesColIdx).Bytes().Set(0, []byte{0})
		b.ColVec(bytesColIdx).Bytes().Set(1, []byte{1})
	})

	t.Run("WindowedBatchZeroCapacity", func(t *testing.T) {
		b := testAllocator.NewMemBatchWithFixedCapacity([]*types.T{}, 0 /* capacity */)
		b.SetLength(coldata.BatchSize())
		colIdx := 0

		// We expect that although the batch is of zero capacity, the newly
		// appended vectors are allocated of coldata.BatchSize() capacity.
		testAllocator.MaybeAppendColumn(b, types.Int, colIdx)
		require.Equal(t, 1, b.Width())
		require.Equal(t, coldata.BatchSize(), b.ColVec(colIdx).Length())
		_ = b.ColVec(colIdx).Int64()[0]
	})
}

func TestPerformAppend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Include decimal and geometry columns because PerformAppend differs from
	// PerformOperation for decimal and datum types.
	var typs = []*types.T{types.Int, types.Decimal, types.Geometry}
	const intIdx, decimalIdx, datumIdx = 0, 1, 2
	const maxBatchSize = 100
	const numRows = 1000
	const nullOk = false
	const resetChance = 0.5

	rng, _ := randutil.NewTestRand()
	testAllocator, _, cleanup := getAllocator(increment)
	defer cleanup()

	batch1 := colexecutils.NewAppendOnlyBufferedBatch(testAllocator, typs, nil /* colsToStore */)
	batch2 := colexecutils.NewAppendOnlyBufferedBatch(testAllocator, typs, nil /* colsToStore */)

	getRandomInputBatch := func(count int) coldata.Batch {
		b := testAllocator.NewMemBatchWithFixedCapacity(typs, count)
		for i := 0; i < count; i++ {
			datum := randgen.RandDatum(rng, typs[intIdx], nullOk)
			b.ColVec(intIdx).Int64()[i] = int64(*(datum.(*tree.DInt)))
			datum = randgen.RandDatum(rng, typs[decimalIdx], nullOk)
			b.ColVec(decimalIdx).Decimal()[i] = datum.(*tree.DDecimal).Decimal
			datum = randgen.RandDatum(rng, typs[datumIdx], nullOk)
			b.ColVec(datumIdx).Datum().Set(i, datum)
		}
		b.SetLength(count)
		return b
	}

	rowsLeft := numRows
	for {
		if rowsLeft <= 0 {
			break
		}
		batchSize := rng.Intn(maxBatchSize-1) + 1 // Ensure a nonzero batch size.
		if batchSize > rowsLeft {
			batchSize = rowsLeft
		}
		rowsLeft -= batchSize
		inputBatch := getRandomInputBatch(batchSize)

		beforePerformOperation := testAllocator.Used()
		testAllocator.PerformOperation(batch1.ColVecs(), func() {
			for colIdx, destVec := range batch1.ColVecs() {
				destVec.Append(coldata.SliceArgs{
					Src:       inputBatch.ColVec(colIdx),
					DestIdx:   batch1.Length(),
					SrcEndIdx: inputBatch.Length(),
				})
			}
			batch1.SetLength(batch1.Length() + inputBatch.Length())
		})
		afterPerformOperation := testAllocator.Used()

		beforePerformAppend := afterPerformOperation
		batch2.AppendTuples(inputBatch, 0 /* startIdx */, inputBatch.Length())
		afterPerformAppend := testAllocator.Used()

		performOperationMem := afterPerformOperation - beforePerformOperation
		performAppendMem := afterPerformAppend - beforePerformAppend
		require.Equal(t, performOperationMem, performAppendMem)

		if rng.Float64() < resetChance {
			// Reset the test batches in order to simulate reuse.
			batch1.ResetInternalBatch()
			batch2.ResetInternalBatch()
		}
	}
}

// TestAccountingHelper verifies that the colmem.AccountingHelper makes
// reasonable decisions about when to allocate new batches.
func TestAccountingHelper(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Set the batch size to a fixed small value to make the tests both
	// predictable and concise.
	oldBatchSize := coldata.BatchSize()
	require.NoError(t, coldata.SetBatchSizeForTests(7))
	defer func() {
		require.NoError(t, coldata.SetBatchSizeForTests(oldBatchSize))
	}()

	// Use increment of 1 so that no allocations are "reserved".
	testAllocator, _, cleanup := getAllocator(1 /* increment */)
	defer cleanup()

	// Allocate a scratch bytes value that exceeds the target size in all
	// scenarios.
	v := make([]byte, 10000)
	typs := []*types.T{types.Bytes}

	// overhead returns the overhead of the batch with a single coldata.Bytes
	// vector.
	overhead := func(batchCapacity int) int64 {
		return colmem.SelVectorSize(batchCapacity) +
			coldata.FlatBytesOverhead +
			int64(batchCapacity)*coldata.ElementSize
	}

	// iteration describes a single iteration of using the AccountingHelper.
	type iteration struct {
		// expectedCapacity specifies the expected capacity of the batch after
		// ResetMaybeReallocate call of the current iteration.
		expectedCapacity int
		// expectedReallocated specifies whether the batch is expected to be
		// reallocated during the ResetMaybeReallocate call of the current
		// iteration.
		expectedReallocated bool
		// bytesValueSize determines the footprint of the bytes values that are
		// set during the current iteration. Note that this only affects the
		// call to ResetMaybeReallocate of the **next** iteration.
		bytesValueSize int64
	}
	type testCase struct {
		memoryLimit int64
		// totalTuples, if positive, indicates the number of tuples that are
		// expected to be set throughout this test case.
		totalTuples int
		iterations  []iteration
	}
	errorMessage := func(tc testCase, i iteration) string {
		return fmt.Sprintf(
			"tc: limit=%d, tuples=%d, iterations=%d i: cap=%d, realloc=%t, size=%d",
			tc.memoryLimit, tc.totalTuples, len(tc.iterations), i.expectedCapacity,
			i.expectedReallocated, i.bytesValueSize,
		)
	}

	for _, tc := range []testCase{
		// Verify the dynamic behavior of the helper. We start out without any
		// knowledge on the number of tuples and with an unlimited budget, so
		// we expect the capacity to grow until it reaches the maximum batch
		// size.
		{
			memoryLimit: math.MaxInt64,
			totalTuples: 0,
			iterations: []iteration{
				{1, true, 100},
				{2, true, 200},
				{4, true, 400},
				{7, true, 700},
				{7, false, 700},
				{7, false, 700},
			},
		},
		// A couple of tests for when the total number of tuples is known in
		// advance.
		{
			memoryLimit: math.MaxInt64,
			totalTuples: 4,
			iterations: []iteration{
				{4, true, 400},
			},
		},
		{
			memoryLimit: math.MaxInt64,
			totalTuples: 13,
			iterations: []iteration{
				{7, true, 700},
				{7, false, 600},
			},
		},
		// A test case when the memory limit is exceeded on the third iteration
		// at which point the batch no longer grows but is reused.
		{
			memoryLimit: 300 + overhead(3),
			totalTuples: 0,
			iterations: []iteration{
				{1, true, 100},
				{2, true, 200},
				{4, true, 400},
				{4, false, 400},
				{4, false, 400},
			},
		},
		// A test case with values of different sizes with the memory limit
		// being exceeded even after the batch has shrunk.
		{
			memoryLimit: 300 + overhead(3),
			totalTuples: 0,
			iterations: []iteration{
				{1, true, 100},
				{2, true, 200},
				{4, true, 400},
				// The limit has just been exceeded, but not by too much, so we
				// reuse the same batch.
				{4, false, 1400},
				// Now the limit has been exceeded by too much - a new batch of
				// smaller capacity must be allocated.
				{2, true, 200},
				{2, false, 200},
				{2, false, 1200},
				// Now the limit has been exceeded by too much again - a new
				// batch of smaller capacity must be allocated.
				{1, true, 400},
				// The limit has been exceeded but not by too much, so the batch
				// is reused.
				{1, false, 1100},
				// Now the limit has been exceeded by too much again - a new
				// batch is allocated, but we cannot reduce the capacity since
				// we're at the minimum already.
				{1, true, 100},
			},
		},
		// A test case with values of different sizes with the memory limit
		// being exceeded even after the batch has shrunk when the total number
		// of tuples is known.
		{
			memoryLimit: 300 + overhead(3),
			totalTuples: 17,
			iterations: []iteration{
				{7, true, 700},
				// The limit has been exceeded by too much - a new batch of
				// smaller capacity must be allocated.
				{4, true, 400},
				// The limit has just been exceeded, but not by too much, so we
				// reuse the same batch.
				{4, false, 1400},
				// Now the limit has been exceeded by too much again - a new
				// batch of smaller capacity must be allocated.
				{2, true, 100},
			},
		},
	} {
		// Prime the allocator for reuse.
		testAllocator.ReleaseAll()
		var helper colmem.AccountingHelper
		helper.Init(testAllocator, tc.memoryLimit)
		tuplesToBeSet := tc.totalTuples
		var batch coldata.Batch
		var reallocated bool
		for _, iteration := range tc.iterations {
			batch, reallocated = helper.ResetMaybeReallocate(typs, batch, tuplesToBeSet)
			require.Equal(t, iteration.expectedCapacity, batch.Capacity(), errorMessage(tc, iteration))
			require.Equal(t, iteration.expectedReallocated, reallocated, errorMessage(tc, iteration))
			testAllocator.PerformOperation(batch.ColVecs(), func() {
				batch.ColVec(0).Bytes().Set(0, v[:iteration.bytesValueSize])
				batch.SetLength(batch.Capacity())
			})
			// Since Bytes.Set appends to a byte slice, we don't know the exact
			// capacity that is allocated for the buffer, meaning that the
			// actual footprint can be greater than our target. That's ok since
			// we're still properly accounting for it.
			require.GreaterOrEqual(t, testAllocator.Used(), overhead(batch.Capacity())+iteration.bytesValueSize, errorMessage(tc, iteration))
			if tuplesToBeSet > 0 {
				tuplesToBeSet -= batch.Capacity()
			}
		}
	}
}

// TestSetAccountingHelper verifies that the colmem.SetAccountingHelper
// precisely tracks the memory footprint of the batch across AccountForSet()
// calls.
func TestSetAccountingHelper(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewTestRand()
	testAllocator, _, cleanup := getAllocator(increment)
	defer cleanup()
	numCols := rng.Intn(10) + 1
	typs := make([]*types.T, numCols)
	for i := range typs {
		typs[i] = randgen.RandType(rng)
	}

	var helper colmem.SetAccountingHelper
	helper.Init(testAllocator, math.MaxInt64, typs, false /* alwaysReallocate */)

	numIterations := rng.Intn(10) + 1
	numRows := rng.Intn(coldata.BatchSize()) + 1

	const smallMemSize = 0
	const largeMemSize = math.MaxInt64

	var batch coldata.Batch
	for iteration := 0; iteration < numIterations; iteration++ {
		// We use zero memory limit so that the same batch is used between most
		// iterations.
		maxBatchMemSize := int64(smallMemSize)
		if rng.Float64() < 0.25 {
			// But occasionally we'll use the large mem limit - as a result, a
			// new batch with larger capacity might be allocated.
			maxBatchMemSize = largeMemSize
		}
		helper.TestingUpdateMemoryLimit(maxBatchMemSize)
		batch, _ = helper.ResetMaybeReallocate(typs, batch, numRows)

		for rowIdx := 0; rowIdx < batch.Capacity(); rowIdx++ {
			for vecIdx, typ := range typs {
				switch typ.Family() {
				case types.BytesFamily:
					// For Bytes, insert pretty large values.
					v := make([]byte, rng.Intn(8*coldata.BytesMaxInlineLength))
					_, _ = rng.Read(v)
					batch.ColVec(vecIdx).Bytes().Set(rowIdx, v)
				default:
					datum := randgen.RandDatum(rng, typ, false /* nullOk */)
					converter := colconv.GetDatumToPhysicalFn(typ)
					coldata.SetValueAt(batch.ColVec(vecIdx), converter(datum), rowIdx)
				}
			}
			// The purpose of this test is ensuring that memory accounting is
			// up-to-date, so we ignore the recommendation of the helper whether
			// the batch is done.
			_ = helper.AccountForSet(rowIdx)
		}

		// At this point, we have set all rows in the batch and performed the
		// memory accounting for each set. We no longer have any uninitialized
		// elements, so the memory footprint of the batch must be exactly as
		// what we have accounted for.
		expected := colmem.GetBatchMemSize(batch)
		actual := testAllocator.Used()
		if expected != actual {
			fmt.Printf("iteration = %d numRows = %d\n", iteration, numRows)
			for i := range typs {
				fmt.Printf("%s ", typs[i].SQLString())
			}
			fmt.Println()
			t.Fatal(errors.Newf("expected %d, actual %d", expected, actual))
		}
	}
}

// TestSetAccountingHelperMemoryLimit verifies that colmem.SetAccountingHelper
// reasonably uses the capacity of already allocated batches.
func TestSetAccountingHelperMemoryLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	if coldata.BatchSize() < 4 {
		skip.IgnoreLint(t, "the test assumes coldata.BatchSize() is at least 4")
	}

	// Use increment of 1 so that no allocations are "reserved".
	testAllocator, _, cleanup := getAllocator(1 /* increment */)
	defer cleanup()

	// We need to use at least one type of variable width in order to trigger
	// more interesting path in AccountForSet.
	typs := []*types.T{types.Bytes}

	// We will ask the helper for batches of different sizes ("small" doesn't
	// exceed the memory limit, and "large" exceeds it by too much so it is not
	// actually allocated according to the ask - instead a "medium" batch is
	// allocated).
	largeCap := coldata.BatchSize()
	smallCap := largeCap / 4
	mediumCap := smallCap * 2
	// Use memory limit that is exactly the footprint of the medium batch.
	memoryLimit := colmem.GetBatchMemSize(testAllocator.NewMemBatchWithFixedCapacity(typs, mediumCap))
	testAllocator.ReleaseAll()

	var helper colmem.SetAccountingHelper
	helper.Init(testAllocator, memoryLimit, typs, false /* alwaysReallocate */)

	// Allocate the small batch and ensure that we use all tuples inside of it.
	b, _ := helper.ResetMaybeReallocate(typs, nil /* oldBatch */, smallCap)
	var rowIdx int
	for batchDone := false; !batchDone; {
		batchDone = helper.AccountForSet(rowIdx)
		rowIdx++
	}
	require.Equal(t, smallCap, rowIdx)

	// Attempt to allocate a batch with too large of a capacity.
	b, reallocated := helper.ResetMaybeReallocate(typs, b, largeCap)
	require.True(t, reallocated)
	// We expect that the ask of largeCap is not satisfied and that the batch
	// of exactly mediumCap is allocated.
	require.Equal(t, b.Capacity(), mediumCap)
	// Ensure that the helper still uses the whole capacity.
	rowIdx = 0
	for batchDone := false; !batchDone; {
		batchDone = helper.AccountForSet(rowIdx)
		rowIdx++
	}
	require.Equal(t, b.Capacity(), rowIdx)

	// Increase the limit by a little so that the previously allocated batch is
	// below the new limit. This also unsets the memorized max capacity so that
	// the helper considers allocating a new batch.
	helper.TestingUpdateMemoryLimit(memoryLimit + 1)
	// Now try to allocate a batch of medium capacity plus one, but a new batch
	// won't be allocated because the helper will estimate that it would exceed
	// the (newly-updated) memory limit.
	b, reallocated = helper.ResetMaybeReallocate(typs, b, mediumCap+1)
	require.False(t, reallocated)
	rowIdx = 0
	for batchDone := false; !batchDone; {
		batchDone = helper.AccountForSet(rowIdx)
		rowIdx++
	}
	require.Equal(t, b.Capacity(), rowIdx)
}

// TestEstimateBatchSizeBytes verifies that EstimateBatchSizeBytes returns such
// an estimate that it equals the actual footprint of the newly-created batch
// with no values set.
func TestEstimateBatchSizeBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewTestRand()
	testAllocator, memAcc, cleanup := getAllocator(increment)
	defer cleanup()

	numCols := rng.Intn(10) + 1
	typs := make([]*types.T, numCols)
	for i := range typs {
		for {
			typs[i] = randgen.RandType(rng)
			// We ignore all datum-backed types. This is due to mismatch in how
			// we account for unset elements in EstimateBatchSizeBytes (where we
			// include the estimated implementation size) and datumVec.Size
			// (where unset elements remain nil for which we only include the
			// DatumOverhead). This exception is ok given that we still perform
			// the correct accounting after the actual elements are set.
			if typeconv.TypeFamilyToCanonicalTypeFamily(typs[i].Family()) != typeconv.DatumVecCanonicalTypeFamily {
				break
			}
		}
	}
	const numRuns = 10
	for run := 0; run < numRuns; run++ {
		memAcc.Clear(context.Background())
		numRows := rng.Intn(coldata.BatchSize()) + 1
		batch := testAllocator.NewMemBatchWithFixedCapacity(typs, numRows)
		expected := memAcc.Used()
		actual := colmem.GetBatchMemSize(batch)
		if expected != actual {
			fmt.Printf("run = %d numRows = %d\n", run, numRows)
			for i := range typs {
				fmt.Printf("%s ", typs[i].SQLString())
			}
			fmt.Println()
			t.Fatal(errors.Newf("expected %d, actual %d", expected, actual))
		}
	}
}
