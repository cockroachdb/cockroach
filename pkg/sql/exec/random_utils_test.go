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
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// maxVarLen specifies a length limit for variable length types (e.g. byte slices).
const maxVarLen = 64

func randomType(rng *rand.Rand) types.T {
	return types.AllTypes[rng.Intn(len(types.AllTypes))]
}

// randomTypes returns an n-length slice of random types.T.
func randomTypes(rng *rand.Rand, n int) []types.T {
	typs := make([]types.T, n)
	for i := range typs {
		typs[i] = randomType(rng)
	}
	return typs
}

// randomVec populates vec with n random values of typ. It is assumed that n is
// in bounds of the given vec.
func randomVec(rng *rand.Rand, typ types.T, vec coldata.Vec, n int, nullProbability float64) {
	switch typ {
	case types.Bool:
		bools := vec.Bool()
		for i := 0; i < n; i++ {
			if rng.Float64() < 0.5 {
				bools[i] = true
			} else {
				bools[i] = false
			}
		}
	case types.Bytes:
		bytes := vec.Bytes()
		for i := 0; i < n; i++ {
			bytes[i] = make([]byte, rng.Intn(maxVarLen))
			// Read always returns len(bytes[i]) and nil.
			_, _ = rand.Read(bytes[i])
		}
	case types.Decimal:
		decs := vec.Decimal()
		for i := 0; i < n; i++ {
			// int64(rng.Uint64()) to get negative numbers, too
			decs[i].SetFinite(int64(rng.Uint64()), int32(rng.Intn(40)-20))
		}
	case types.Int8:
		ints := vec.Int8()
		for i := 0; i < n; i++ {
			ints[i] = int8(rng.Uint64())
		}
	case types.Int16:
		ints := vec.Int16()
		for i := 0; i < n; i++ {
			ints[i] = int16(rng.Uint64())
		}
	case types.Int32:
		ints := vec.Int32()
		for i := 0; i < n; i++ {
			ints[i] = int32(rng.Uint64())
		}
	case types.Int64:
		ints := vec.Int64()
		for i := 0; i < n; i++ {
			ints[i] = int64(rng.Uint64())
		}
	case types.Float32:
		floats := vec.Float32()
		for i := 0; i < n; i++ {
			floats[i] = rng.Float32()
		}
	case types.Float64:
		floats := vec.Float64()
		for i := 0; i < n; i++ {
			floats[i] = rng.Float64()
		}
	default:
		panic(fmt.Sprintf("unhandled type %s", typ))
	}
	vec.UnsetNulls()
	if nullProbability == 0 {
		return
	}

	for i := 0; i < n; i++ {
		if rng.Float64() < nullProbability {
			vec.SetNull(uint16(i))
		}
	}
}

// randomBatch returns an n-length batch of the given typs where each value will
// be null with a probability of nullProbability. The returned batch will have
// no selection vector.
func randomBatch(rng *rand.Rand, typs []types.T, n int, nullProbability float64) coldata.Batch {
	batch := coldata.NewMemBatchWithSize(typs, n)
	for i, typ := range typs {
		randomVec(rng, typ, batch.ColVec(i), n, nullProbability)
	}
	batch.SetLength(uint16(n))
	return batch
}

// randomSel creates a random selection vector up to a given batchSize in
// length. probOfOmitting specifies the probability that a row should be omitted
// from the batch (i.e. whether it should be selected out). So if probOfOmitting
// is 0, then the selection vector will contain all rows, but if it is > 0, then
// some rows might be omitted and the length of the selection vector might be
// less than batchSize.
func randomSel(rng *rand.Rand, batchSize uint16, probOfOmitting float64) []uint16 {
	if probOfOmitting < 0 || probOfOmitting > 1 {
		panic(fmt.Sprintf("probability of omitting a row is %f - outside of [0, 1] range", probOfOmitting))
	}
	sel := make([]uint16, batchSize)
	used := make([]bool, batchSize)
	for i := uint16(0); i < batchSize; i++ {
		if rng.Float64() < probOfOmitting {
			batchSize--
			i--
			continue
		}
		for {
			j := uint16(rng.Intn(int(batchSize)))
			if !used[j] {
				used[j] = true
				sel[i] = j
				break
			}
		}
	}
	return sel[:batchSize]
}

// Suppress unused warnings.
// TODO(asubiotto): Remove this once these functions are actually used.
var (
	_ = randomTypes
	_ = randomBatchWithSel
)

// randomBatchWithSel is equivalent to randomBatch, but will also add a
// selection vector to the batch where each row is selected with probability
// selProbability. If selProbability is 1, all the rows will be selected, if
// selProbability is 0, none will.
func randomBatchWithSel(
	rng *rand.Rand, typs []types.T, n int, nullProbability float64, selProbability float64,
) coldata.Batch {
	batch := randomBatch(rng, typs, n, nullProbability)
	batch.SetSelection(true)
	copy(batch.Selection(), randomSel(rng, uint16(n), 1-selProbability))
	return batch
}
