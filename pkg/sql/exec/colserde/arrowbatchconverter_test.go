// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colserde

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestArrowBatchConverterRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const maxTyps = 16

	rng, _ := randutil.NewPseudoRand()

	availableTyps := make([]types.T, 0, len(types.AllTypes))
	for _, typ := range types.AllTypes {
		// TODO(asubiotto): We do not support decimal conversion yet.
		if typ == types.Decimal {
			continue
		}
		availableTyps = append(availableTyps, typ)
	}
	typs := make([]types.T, rng.Intn(maxTyps)+1)
	for i := range typs {
		typs[i] = availableTyps[rng.Intn(len(availableTyps))]
	}

	b := exec.RandomBatch(rng, typs, rng.Intn(coldata.BatchSize)+1, rng.Float64())
	c := NewArrowBatchConverter(typs)

	// Make a copy of the original batch because the converter modifies and casts
	// data without copying for performance reasons.
	expectedLength := b.Length()
	expectedWidth := b.Width()
	expectedColVecs := make([]coldata.Vec, len(b.ColVecs()))
	for i := range typs {
		expectedColVecs[i] = coldata.NewMemColumn(typs[i], int(b.Length()))
		expectedColVecs[i].Copy(
			coldata.CopyArgs{
				ColType:   typs[i],
				Src:       b.ColVec(i),
				SrcEndIdx: uint64(b.Length()),
			},
		)
	}

	arrowData, err := c.BatchToArrow(b)
	require.NoError(t, err)
	result, err := c.ArrowToBatch(arrowData)
	require.NoError(t, err)
	if result.Selection() != nil {
		t.Fatal("violated invariant that batches have no selection vectors")
	}
	require.Equal(t, expectedLength, result.Length())
	require.Equal(t, expectedWidth, result.Width())
	for i, typ := range typs {
		// Verify equality of ColVecs (this includes nulls). Since the coldata.Vec
		// backing array is always of coldata.BatchSize due to the scratch batch
		// that the converter keeps around, the coldata.Vec needs to be sliced to
		// the first length elements to match on length, otherwise the check will
		// fail.
		require.Equal(
			t,
			expectedColVecs[i].Slice(typ, 0, uint64(b.Length())),
			result.ColVec(i).Slice(typ, 0, uint64(result.Length())),
		)
	}
}

func BenchmarkArrowBatchConverter(b *testing.B) {
	// fixedLen specifies how many bytes we should fit variable length data types
	// to in order to reduce benchmark noise.
	const fixedLen = 64

	rng, _ := randutil.NewPseudoRand()

	typs := []types.T{types.Bool, types.Bytes, types.Int64}
	// numBytes corresponds 1:1 to typs and specifies how many bytes we are
	// converting on one iteration of the benchmark for the corresponding type in
	// typs.
	numBytes := []int64{coldata.BatchSize, fixedLen * coldata.BatchSize, 8 * coldata.BatchSize}
	// Run a benchmark on every type we care about.
	for typIdx, typ := range typs {
		batch := exec.RandomBatch(rng, []types.T{typ}, coldata.BatchSize, 0 /* nullProbability */)
		if batch.Width() != 1 {
			b.Fatalf("unexpected batch width: %d", batch.Width())
		}
		if typ == types.Bytes {
			// This type has variable length elements, fit all of them to be fixedLen
			// bytes long.
			bytes := batch.ColVec(0).Bytes()
			for i := range bytes {
				diff := len(bytes[i]) - fixedLen
				if diff < 0 {
					bytes[i] = append(bytes[i], make([]byte, -diff)...)
				} else if diff > 0 {
					bytes[i] = bytes[i][:fixedLen]
				}
			}
		}
		c := NewArrowBatchConverter([]types.T{typ})
		nullFractions := []float64{0, 0.25, 0.5}
		setNullFraction := func(batch coldata.Batch, nullFraction float64) {
			vec := batch.ColVec(0)
			vec.Nulls().UnsetNulls()
			numNulls := uint16(int(nullFraction * float64(batch.Length())))
			// Set the first numNulls elements to null.
			for i := uint16(0); i < batch.Length() && i < numNulls; i++ {
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
					if data[0].Len() != coldata.BatchSize {
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
			b.Run(testPrefix+"/ArrowToBatch", func(b *testing.B) {
				b.SetBytes(numBytes[typIdx])
				for i := 0; i < b.N; i++ {
					result, _ := c.ArrowToBatch(data)
					if result.Width() != 1 {
						b.Fatal("expected one column")
					}
					if result.Length() != coldata.BatchSize {
						b.Fatal("unexpected number of elements")
					}
				}
			})
		}
	}
}
