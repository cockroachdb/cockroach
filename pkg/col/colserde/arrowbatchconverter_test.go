// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colserde_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func randomBatch(allocator *colexec.Allocator) ([]coltypes.T, coldata.Batch) {
	const maxTyps = 16
	rng, _ := randutil.NewPseudoRand()

	typs := make([]coltypes.T, rng.Intn(maxTyps)+1)
	for i := range typs {
		typs[i] = coltypes.AllTypes[rng.Intn(len(coltypes.AllTypes))]
	}

	capacity := rng.Intn(coldata.BatchSize()) + 1
	length := rng.Intn(capacity)
	b := colexec.RandomBatch(allocator, rng, typs, capacity, length, rng.Float64())
	return typs, b
}

func TestArrowBatchConverterRejectsUnsupportedTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	unsupportedTypes := []coltypes.T{coltypes.Unhandled}
	for _, typ := range unsupportedTypes {
		_, err := colserde.NewArrowBatchConverter([]coltypes.T{typ})
		require.Error(t, err)
	}
}

func TestArrowBatchConverterRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()

	typs, b := randomBatch(testAllocator)
	c, err := colserde.NewArrowBatchConverter(typs)
	require.NoError(t, err)

	// Make a copy of the original batch because the converter modifies and casts
	// data without copying for performance reasons.
	expected := colexec.CopyBatch(testAllocator, b)

	arrowData, err := c.BatchToArrow(b)
	require.NoError(t, err)
	actual := coldata.NewMemBatchWithSize(nil, 0)
	require.NoError(t, c.ArrowToBatch(arrowData, actual))

	coldata.AssertEquivalentBatches(t, expected, actual)
}

// roundTripBatch is a helper function that round trips a batch through the
// ArrowBatchConverter and RecordBatchSerializer and asserts that the output
// batch is equal to the input batch. Make sure to copy the input batch before
// passing it to this function to assert equality.
func roundTripBatch(
	b coldata.Batch, c *colserde.ArrowBatchConverter, r *colserde.RecordBatchSerializer,
) (coldata.Batch, error) {
	var buf bytes.Buffer
	arrowDataIn, err := c.BatchToArrow(b)
	if err != nil {
		return nil, err
	}
	_, _, err = r.Serialize(&buf, arrowDataIn)
	if err != nil {
		return nil, err
	}

	var arrowDataOut []*array.Data
	if err := r.Deserialize(&arrowDataOut, buf.Bytes()); err != nil {
		return nil, err
	}
	actual := coldata.NewMemBatchWithSize(nil, 0)
	if err := c.ArrowToBatch(arrowDataOut, actual); err != nil {
		return nil, err
	}
	return actual, nil
}

func TestRecordBatchRoundtripThroughBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for run := 0; run < 10; run++ {
		typs, b := randomBatch(testAllocator)
		c, err := colserde.NewArrowBatchConverter(typs)
		require.NoError(t, err)
		r, err := colserde.NewRecordBatchSerializer(typs)
		require.NoError(t, err)

		// Make a copy of the original batch because the converter modifies and
		// casts data without copying for performance reasons.
		expected := colexec.CopyBatch(testAllocator, b)
		actual, err := roundTripBatch(b, c, r)
		require.NoError(t, err)

		coldata.AssertEquivalentBatches(t, expected, actual)
	}
}

func BenchmarkArrowBatchConverter(b *testing.B) {
	// fixedLen specifies how many bytes we should fit variable length data types
	// to in order to reduce benchmark noise.
	const fixedLen = 64

	rng, _ := randutil.NewPseudoRand()

	typs := []coltypes.T{
		coltypes.Bool,
		coltypes.Bytes,
		coltypes.Decimal,
		coltypes.Int64,
		coltypes.Timestamp,
	}
	// numBytes corresponds 1:1 to typs and specifies how many bytes we are
	// converting on one iteration of the benchmark for the corresponding type in
	// typs.
	numBytes := []int64{
		int64(coldata.BatchSize()),
		fixedLen * int64(coldata.BatchSize()),
		0, // The number of bytes for decimals will be set below.
		8 * int64(coldata.BatchSize()),
		3 * 8 * int64(coldata.BatchSize()),
	}
	// Run a benchmark on every type we care about.
	for typIdx, typ := range typs {
		batch := colexec.RandomBatch(testAllocator, rng, []coltypes.T{typ}, coldata.BatchSize(), 0 /* length */, 0 /* nullProbability */)
		if batch.Width() != 1 {
			b.Fatalf("unexpected batch width: %d", batch.Width())
		}
		if typ == coltypes.Bytes {
			// This type has variable length elements, fit all of them to be fixedLen
			// bytes long so that we can compare results of one benchmark with
			// another. Since we can't overwrite elements in a Bytes, create a new
			// one.
			// TODO(asubiotto): We should probably create some random spec struct that
			//  we pass in to RandomBatch.
			bytes := batch.ColVec(0).Bytes()
			newBytes := coldata.NewBytes(bytes.Len())
			for i := 0; i < bytes.Len(); i++ {
				diff := len(bytes.Get(i)) - fixedLen
				if diff < 0 {
					newBytes.Set(i, append(bytes.Get(i), make([]byte, -diff)...))
				} else if diff >= 0 {
					newBytes.Set(i, bytes.Get(i)[:fixedLen])
				}
			}
			batch.ColVec(0).SetCol(newBytes)
		} else if typ == coltypes.Decimal {
			// Decimal is variable length type, so we want to calculate precisely the
			// total size of all decimals in the vector.
			decimals := batch.ColVec(0).Decimal()
			for _, d := range decimals {
				marshaled, err := d.MarshalText()
				require.NoError(b, err)
				numBytes[typIdx] += int64(len(marshaled))
			}
		}
		c, err := colserde.NewArrowBatchConverter([]coltypes.T{typ})
		require.NoError(b, err)
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
			testPrefix := fmt.Sprintf("%s/nullFraction=%0.2f", typ.String(), nullFraction)
			var data []*array.Data
			b.Run(testPrefix+"/BatchToArrow", func(b *testing.B) {
				b.SetBytes(numBytes[typIdx])
				for i := 0; i < b.N; i++ {
					data, _ = c.BatchToArrow(batch)
					if len(data) != 1 {
						b.Fatal("expected arrow batch of length 1")
					}
					if data[0].Len() != coldata.BatchSize() {
						b.Fatal("unexpected number of elements")
					}
				}
			})
		}
		for _, nullFraction := range nullFractions {
			setNullFraction(batch, nullFraction)
			data, err := c.BatchToArrow(batch)
			require.NoError(b, err)
			testPrefix := fmt.Sprintf("%s/nullFraction=%0.2f", typ.String(), nullFraction)
			result := coldata.NewMemBatch(typs)
			b.Run(testPrefix+"/ArrowToBatch", func(b *testing.B) {
				b.SetBytes(numBytes[typIdx])
				for i := 0; i < b.N; i++ {
					// Using require.NoError here causes large enough allocations to
					// affect the result.
					if err := c.ArrowToBatch(data, result); err != nil {
						b.Fatal(err)
					}
					if result.Width() != 1 {
						b.Fatal("expected one column")
					}
					if result.Length() != coldata.BatchSize() {
						b.Fatal("unexpected number of elements")
					}
				}
			})
		}
	}
}
