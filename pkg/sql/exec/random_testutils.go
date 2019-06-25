// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"context"
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

// randomVec populates vec with n random values of typ, setting each value to
// null with a probability of nullProbability. It is assumed that n is in bounds
// of the given vec.
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
	vec.Nulls().UnsetNulls()
	if nullProbability == 0 {
		return
	}

	for i := 0; i < n; i++ {
		if rng.Float64() < nullProbability {
			vec.Nulls().SetNull(uint16(i))
		}
	}
}

// RandomBatch returns an n-length batch of the given typs where each value will
// be null with a probability of nullProbability. The returned batch will have
// no selection vector.
func RandomBatch(rng *rand.Rand, typs []types.T, n int, nullProbability float64) coldata.Batch {
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
// TODO(asubiotto): Remove this once this function is actually used.
var _ = randomTypes

// randomBatchWithSel is equivalent to RandomBatch, but will also add a
// selection vector to the batch where each row is selected with probability
// selProbability. If selProbability is 1, all the rows will be selected, if
// selProbability is 0, none will. The returned batch will have its length set
// to the length of the selection vector, unless selProbability is 0.
func randomBatchWithSel(
	rng *rand.Rand, typs []types.T, n int, nullProbability float64, selProbability float64,
) coldata.Batch {
	batch := RandomBatch(rng, typs, n, nullProbability)
	if selProbability != 0 {
		sel := randomSel(rng, uint16(n), 1-selProbability)
		batch.SetSelection(true)
		copy(batch.Selection(), sel)
		batch.SetLength(uint16(len(sel)))
	}
	return batch
}

const (
	defaultMaxSchemaLength = 8
	defaultBatchSize       = coldata.BatchSize
	defaultNumBatches      = 4
)

// RandomDataOpArgs are arguments passed in to RandomDataOp. All arguments are
// optional (refer to the constants above this struct definition for the
// defaults). Bools are false by default and AvailableTyps defaults to
// types.AllTypes.
type RandomDataOpArgs struct {
	// DeterministicTyps, if set, overrides AvailableTyps and MaxSchemaLength,
	// forcing the RandomDataOp to use this schema.
	DeterministicTyps []types.T
	// AvailableTyps is the pool of types from which the operator's schema will
	// be generated.
	AvailableTyps []types.T
	// MaxSchemaLength is the maximum length of the operator's schema, which will
	// be at least one type.
	MaxSchemaLength int
	// BatchSize is the size of batches returned.
	BatchSize int
	// NumBatches is the number of batches returned before the final, zero batch.
	NumBatches int
	// Selection specifies whether random selection vectors should be generated
	// over the batches.
	Selection bool
	// Nulls specifies whether nulls should be set in batches.
	Nulls bool
	// BatchAccumulator, if set, will be called before returning a coldata.Batch
	// from Next.
	BatchAccumulator func(b coldata.Batch)
}

// RandomDataOp is an operator that generates random data according to
// RandomDataOpArgs. Call GetBuffer to get all data that was returned.
type RandomDataOp struct {
	batchAccumulator func(b coldata.Batch)
	typs             []types.T
	rng              *rand.Rand
	batchSize        int
	numBatches       int
	numReturned      int
	selection        bool
	nulls            bool
}

// NewRandomDataOp creates a new RandomDataOp.
func NewRandomDataOp(rng *rand.Rand, args RandomDataOpArgs) *RandomDataOp {
	var (
		availableTyps   = types.AllTypes
		maxSchemaLength = defaultMaxSchemaLength
		batchSize       = defaultBatchSize
		numBatches      = defaultNumBatches
	)
	if args.AvailableTyps != nil {
		availableTyps = args.AvailableTyps
	}
	if args.MaxSchemaLength > 0 {
		maxSchemaLength = args.MaxSchemaLength
	}
	if args.BatchSize > 0 {
		batchSize = args.BatchSize
	}
	if args.NumBatches > 0 {
		numBatches = args.NumBatches
	}

	typs := args.DeterministicTyps
	if typs == nil {
		// Generate at least one type.
		typs = make([]types.T, 1+rng.Intn(maxSchemaLength))
		for i := range typs {
			typs[i] = availableTyps[rng.Intn(len(availableTyps))]
		}
	}
	return &RandomDataOp{
		batchAccumulator: args.BatchAccumulator,
		typs:             typs,
		rng:              rng,
		batchSize:        batchSize,
		numBatches:       numBatches,
		selection:        args.Selection,
		nulls:            args.Nulls,
	}
}

// Init is part of the Operator interface.
func (o *RandomDataOp) Init() {}

// Next is part of the Operator interface.
func (o *RandomDataOp) Next(ctx context.Context) coldata.Batch {
	if o.numReturned == o.numBatches {
		// Done.
		b := coldata.NewMemBatchWithSize(o.typs, 0)
		b.SetLength(0)
		if o.batchAccumulator != nil {
			o.batchAccumulator(b)
		}
		return b
	}

	var (
		selProbability  float64
		nullProbability float64
	)
	for {
		if o.selection {
			selProbability = o.rng.Float64()
		}
		if o.nulls {
			nullProbability = o.rng.Float64()
		}

		b := randomBatchWithSel(o.rng, o.typs, o.batchSize, nullProbability, selProbability)
		if !o.selection {
			b.SetSelection(false)
		}
		if b.Length() == 0 {
			// Don't return a zero-length batch until we return o.numBatches batches.
			continue
		}
		o.numReturned++
		if o.batchAccumulator != nil {
			o.batchAccumulator(b)
		}
		return b
	}
}
