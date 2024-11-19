// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colserde_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func randomBatch(allocator *colmem.Allocator) ([]*types.T, coldata.Batch) {
	const maxTyps = 16
	rng, _ := randutil.NewTestRand()

	typs := make([]*types.T, rng.Intn(maxTyps)+1)
	for i := range typs {
		typs[i] = randgen.RandType(rng)
	}

	capacity := rng.Intn(coldata.BatchSize()) + 1
	length := rng.Intn(capacity)
	args := coldatatestutils.RandomVecArgs{Rand: rng, NullProbability: rng.Float64()}
	b := coldatatestutils.RandomBatch(allocator, args, typs, capacity, length)
	return typs, b
}

func TestArrowBatchConverterRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()

	typs, b := randomBatch(testAllocator)
	c, err := colserde.NewArrowBatchConverter(typs, colserde.BiDirectional, testMemAcc)
	require.NoError(t, err)
	defer c.Close(context.Background())

	// Make a copy of the original batch because the converter modifies and casts
	// data without copying for performance reasons.
	expected := coldatatestutils.CopyBatch(b, typs, testColumnFactory)

	arrowData, err := c.BatchToArrow(context.Background(), b)
	require.NoError(t, err)
	actual := testAllocator.NewMemBatchWithFixedCapacity(typs, b.Length())
	require.NoError(t, c.ArrowToBatch(arrowData, b.Length(), actual))

	coldata.AssertEquivalentBatches(t, expected, actual)
}

// roundTripBatch is a helper function that pushes the source batch through the
// ArrowBatchConverter and RecordBatchSerializer. The result is written to dest.
func roundTripBatch(
	src, dest coldata.Batch, c *colserde.ArrowBatchConverter, r *colserde.RecordBatchSerializer,
) error {
	var buf bytes.Buffer
	arrowDataIn, err := c.BatchToArrow(context.Background(), src)
	if err != nil {
		return err
	}
	_, _, err = r.Serialize(&buf, arrowDataIn, src.Length())
	if err != nil {
		return err
	}

	var arrowDataOut []array.Data
	batchLength, err := r.Deserialize(&arrowDataOut, buf.Bytes())
	if err != nil {
		return err
	}
	return c.ArrowToBatch(arrowDataOut, batchLength, dest)
}

func TestRecordBatchRoundtripThroughBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, _ := randutil.NewTestRand()
	for run := 0; run < 10; run++ {
		var typs []*types.T
		var src coldata.Batch
		if rng.Float64() < 0.1 {
			// In 10% of cases we'll use a zero length schema.
			src = testAllocator.NewMemBatchWithFixedCapacity(typs, rng.Intn(coldata.BatchSize())+1)
			src.SetLength(src.Capacity())
		} else {
			typs, src = randomBatch(testAllocator)
		}
		dest := testAllocator.NewMemBatchWithMaxCapacity(typs)
		c, err := colserde.NewArrowBatchConverter(typs, colserde.BiDirectional, testMemAcc)
		require.NoError(t, err)
		defer c.Close(context.Background())
		r, err := colserde.NewRecordBatchSerializer(typs)
		require.NoError(t, err)

		// Reuse the same destination batch as well as the ArrowBatchConverter
		// and RecordBatchSerializer in order to simulate how these things are
		// used in the production setting.
		for i := 0; i < 10; i++ {
			require.NoError(t, roundTripBatch(src, dest, c, r))

			coldata.AssertEquivalentBatches(t, src, dest)
			// Check that we can actually read each tuple from the destination
			// batch.
			for _, vec := range dest.ColVecs() {
				for tupleIdx := 0; tupleIdx < dest.Length(); tupleIdx++ {
					coldata.GetValueAt(vec, tupleIdx)
				}
			}

			// Generate the new source batch.
			nullProbability := rng.Float64()
			if rng.Float64() < 0.1 {
				// In some cases, make sure that there are no nulls at all.
				nullProbability = 0
			}
			capacity := rng.Intn(coldata.BatchSize()) + 1
			length := rng.Intn(capacity)
			args := coldatatestutils.RandomVecArgs{Rand: rng, NullProbability: nullProbability}
			src = coldatatestutils.RandomBatch(testAllocator, args, typs, capacity, length)
		}
	}
}

// runConversionBenchmarks runs two kinds ("from coldata.Batch" and "to
// coldata.Batch") of conversion benchmarks over different input types and null
// fractions. Both benchmark kinds are represented by a "run" function which is
// expected to perform some optional setup first before resetting the timer and
// then running the benchmark loop.
func runConversionBenchmarks(
	b *testing.B,
	fromBatchBenchName string,
	fromBatch func(*testing.B, coldata.Batch, *types.T),
	toBatchBenchName string,
	toBatch func(*testing.B, coldata.Batch, *types.T),
) {
	const bytesInlinedLen = 16
	const bytesNonInlinedLen = 64

	rng, _ := randutil.NewTestRand()

	// Run a benchmark on every type we care about.
	for _, tc := range []struct {
		t *types.T
		// numBytes specifies how many bytes we are converting on one iteration
		// of the benchmark for the corresponding type.
		numBytes         int64
		bytesFixedLength int
	}{
		{t: types.Bool, numBytes: int64(coldata.BatchSize())},
		{t: types.Bytes, bytesFixedLength: bytesInlinedLen},    // The number of bytes will be set below.
		{t: types.Bytes, bytesFixedLength: bytesNonInlinedLen}, // The number of bytes will be set below.
		{t: types.Decimal}, // The number of bytes will be set below.
		{t: types.Int, numBytes: 8 * int64(coldata.BatchSize())},
		{t: types.Timestamp, numBytes: 3 * 8 * int64(coldata.BatchSize())},
		{t: types.Interval, numBytes: 3 * 8 * int64(coldata.BatchSize())},
	} {
		typ := tc.t
		args := coldatatestutils.RandomVecArgs{Rand: rng, BytesFixedLength: tc.bytesFixedLength}
		batch := coldatatestutils.RandomBatch(testAllocator, args, []*types.T{typ}, coldata.BatchSize(), 0 /* length */)
		if batch.Width() != 1 {
			b.Fatalf("unexpected batch width: %d", batch.Width())
		}
		var typNameSuffix string
		if typ.Identical(types.Bytes) {
			tc.numBytes = int64(tc.bytesFixedLength * coldata.BatchSize())
			if tc.bytesFixedLength == bytesInlinedLen {
				typNameSuffix = "_inlined"
			}
		} else if typ.Identical(types.Decimal) {
			// Decimal is variable length type, so we want to calculate precisely the
			// total size of all decimals in the vector.
			decimals := batch.ColVec(0).Decimal()
			for _, d := range decimals {
				marshaled, err := d.MarshalText()
				require.NoError(b, err)
				tc.numBytes += int64(len(marshaled))
			}
		}
		nullFractions := []float64{0, 0.25, 0.5}
		setNullFraction := func(batch coldata.Batch, nullFraction float64) {
			vec := batch.ColVec(0)
			vec.Nulls().UnsetNulls()
			numNulls := int(nullFraction * float64(batch.Length()))
			// Set the first numNulls elements to null.
			for i := 0; i < batch.Length() && i < numNulls; i++ {
				vec.Nulls().SetNull(i)
			}
		}
		for _, nullFraction := range nullFractions {
			setNullFraction(batch, nullFraction)
			testPrefix := fmt.Sprintf("%s/nullFraction=%0.2f/", typ.String()+typNameSuffix, nullFraction)
			b.Run(testPrefix+fromBatchBenchName, func(b *testing.B) {
				b.SetBytes(tc.numBytes)
				fromBatch(b, batch, typ)
			})
			b.Run(testPrefix+toBatchBenchName, func(b *testing.B) {
				b.SetBytes(tc.numBytes)
				toBatch(b, batch, typ)
			})
		}
	}
}

func BenchmarkArrowBatchConverter(b *testing.B) {
	ctx := context.Background()
	runConversionBenchmarks(
		b,
		"BatchToArrow",
		func(b *testing.B, batch coldata.Batch, typ *types.T) {
			c, err := colserde.NewArrowBatchConverter([]*types.T{typ}, colserde.BiDirectional, testMemAcc)
			require.NoError(b, err)
			defer c.Close(ctx)
			var data []array.Data
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data, _ = c.BatchToArrow(ctx, batch)
				if len(data) != 1 {
					b.Fatal("expected arrow batch of length 1")
				}
				if data[0].Len() != coldata.BatchSize() {
					b.Fatal("unexpected number of elements")
				}
			}
		},
		"ArrowToBatch",
		func(b *testing.B, batch coldata.Batch, typ *types.T) {
			c, err := colserde.NewArrowBatchConverter([]*types.T{typ}, colserde.BiDirectional, testMemAcc)
			require.NoError(b, err)
			defer c.Close(ctx)
			data, err := c.BatchToArrow(ctx, batch)
			dataCopy := make([]array.Data, len(data))
			require.NoError(b, err)
			result := testAllocator.NewMemBatchWithMaxCapacity([]*types.T{typ})
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Since ArrowToBatch eagerly nils things out, we have to make a
				// shallow copy each time.
				copy(dataCopy, data)
				// Using require.NoError here causes large enough allocations to
				// affect the result.
				if err := c.ArrowToBatch(dataCopy, batch.Length(), result); err != nil {
					b.Fatal(err)
				}
				if result.Width() != 1 {
					b.Fatal("expected one column")
				}
				if result.Length() != coldata.BatchSize() {
					b.Fatal("unexpected number of elements")
				}
			}
		},
	)
}
