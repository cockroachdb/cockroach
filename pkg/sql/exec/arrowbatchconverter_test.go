// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package exec

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/arrow/array"
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

	// TODO(asubiotto): We do not support decimal conversion yet, so we iterate
	// until we have non-decimal typs.
	typs := randomTypes(rng, rng.Intn(maxTyps)+1)
	for {
		foundDecimal := false
		for _, typ := range typs {
			if typ == types.Decimal {
				foundDecimal = true
				typs = randomTypes(rng, rng.Intn(maxTyps)+1)
				break
			}
		}
		if !foundDecimal {
			break
		}
	}
	b := randomBatch(rng, typs, rng.Intn(coldata.BatchSize)+1, rng.Float64())
	c := MakeArrowBatchConverter(typs)

	result := c.ArrowToBatch(c.BatchToArrow(b))
	if result.Selection() != nil {
		t.Fatal("violated invariant that batches have no selection vectors")
	}
	require.Equal(t, b.Length(), result.Length())
	require.Equal(t, b.Width(), result.Width())
	for i, typ := range typs {
		// Verify equality of ColVecs (this includes nulls). Since the coldata.Vec
		// backing array is always of coldata.BatchSize due to the scratch batch
		// that the converter keeps around, the coldata.Vec needs to be sliced to
		// the first length elements to match on length, otherwise the check will
		// fail.
		require.Equal(
			t,
			b.ColVec(i).Slice(typ, 0, uint64(b.Length())),
			result.ColVec(i).Slice(typ, 0, uint64(result.Length())),
		)
	}
}

func BenchmarkArrowBatchConverter(b *testing.B) {
	// fixedLen specifies how many bytes we should fit variable length data types
	// to to reduce benchmark noise.
	const fixedLen = 64

	rng, _ := randutil.NewPseudoRand()

	typs := []types.T{types.Bool, types.Bytes, types.Int64}
	// numBytes corresponds 1:1 to typs and specifies how many bytes we are
	// converting on one iteration of the benchmark for the corresponding type in
	// typs.
	numBytes := []int64{coldata.BatchSize, fixedLen * coldata.BatchSize, 8 * coldata.BatchSize}
	// Run a benchmark on every type we care about.
	for i, typ := range typs {
		batch := randomBatch(rng, []types.T{typ}, coldata.BatchSize, 0 /* nullProbability */)
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
		c := MakeArrowBatchConverter([]types.T{typ})
		nullFractions := []float64{0, 0.25, 0.5}
		setNullFraction := func(batch coldata.Batch, nullFraction float64) {
			vec := batch.ColVec(0)
			vec.UnsetNulls()
			numNulls := int(nullFraction * float64(batch.Length()))
			// Set the first numNulls elements to null.
			for i, nullsSet := uint16(0), 0; i < batch.Length() && nullsSet < numNulls; i++ {
				vec.SetNull(i)
			}
		}
		for _, nullFraction := range nullFractions {
			setNullFraction(batch, nullFraction)
			testPrefix := fmt.Sprintf("%s/nullFraction=%0.2f", typ.String(), nullFraction)
			var data []*array.Data
			b.Run(fmt.Sprintf("%s/BatchToArrow", testPrefix), func(b *testing.B) {
				b.SetBytes(numBytes[i])
				for i := 0; i < b.N; i++ {
					data = c.BatchToArrow(batch)
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
			data := c.BatchToArrow(batch)
			testPrefix := fmt.Sprintf("%s/nullFraction=%0.2f", typ.String(), nullFraction)
			b.Run(fmt.Sprintf("%s/ArrowToBatch", testPrefix), func(b *testing.B) {
				b.SetBytes(numBytes[i])
				for i := 0; i < b.N; i++ {
					result := c.ArrowToBatch(data)
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
