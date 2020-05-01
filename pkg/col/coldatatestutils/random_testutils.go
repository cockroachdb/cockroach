// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coldatatestutils

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// maxVarLen specifies a length limit for variable length types (e.g. byte slices).
const maxVarLen = 64

var locations []*time.Location

func init() {
	// Load some random time zones.
	for _, locationName := range []string{
		"Africa/Addis_Ababa",
		"America/Anchorage",
		"Antarctica/Davis",
		"Asia/Ashkhabad",
		"Australia/Sydney",
		"Europe/Minsk",
		"Pacific/Palau",
	} {
		loc, err := timeutil.LoadLocation(locationName)
		if err == nil {
			locations = append(locations, loc)
		}
	}
}

// RandomVec populates vec with n random values of typ, setting each value to
// null with a probability of nullProbability. It is assumed that n is in bounds
// of the given vec.
// bytesFixedLength (when greater than zero) specifies the fixed length of the
// bytes slice to be generated. It is used only if typ's canonical type family
// is types.BytesFamily.
func RandomVec(
	rng *rand.Rand, bytesFixedLength int, vec coldata.Vec, n int, nullProbability float64,
) {
	switch vec.CanonicalTypeFamily() {
	case types.BoolFamily:
		bools := vec.Bool()
		for i := 0; i < n; i++ {
			if rng.Float64() < 0.5 {
				bools[i] = true
			} else {
				bools[i] = false
			}
		}
	case types.BytesFamily:
		bytes := vec.Bytes()
		for i := 0; i < n; i++ {
			bytesLen := bytesFixedLength
			if bytesLen <= 0 {
				bytesLen = rng.Intn(maxVarLen)
			}
			randBytes := make([]byte, bytesLen)
			// Read always returns len(bytes[i]) and nil.
			_, _ = rand.Read(randBytes)
			bytes.Set(i, randBytes)
		}
	case types.DecimalFamily:
		decs := vec.Decimal()
		for i := 0; i < n; i++ {
			// int64(rng.Uint64()) to get negative numbers, too
			decs[i].SetFinite(int64(rng.Uint64()), int32(rng.Intn(40)-20))
		}
	case types.IntFamily:
		switch vec.Type().Width() {
		case 16:
			ints := vec.Int16()
			for i := 0; i < n; i++ {
				ints[i] = int16(rng.Uint64())
			}
		case 32:
			ints := vec.Int32()
			for i := 0; i < n; i++ {
				ints[i] = int32(rng.Uint64())
			}
		case 0, 64:
			ints := vec.Int64()
			for i := 0; i < n; i++ {
				ints[i] = int64(rng.Uint64())
			}
		}
	case types.FloatFamily:
		floats := vec.Float64()
		for i := 0; i < n; i++ {
			floats[i] = rng.Float64()
		}
	case types.TimestampTZFamily:
		timestamps := vec.Timestamp()
		for i := 0; i < n; i++ {
			timestamps[i] = timeutil.Unix(rng.Int63n(1000000), rng.Int63n(1000000))
			loc := locations[rng.Intn(len(locations))]
			timestamps[i] = timestamps[i].In(loc)
		}
	case types.IntervalFamily:
		intervals := vec.Interval()
		for i := 0; i < n; i++ {
			intervals[i] = duration.FromFloat64(rng.Float64())
		}
	default:
		panic(fmt.Sprintf("unhandled type %s", vec.Type()))
	}
	vec.Nulls().UnsetNulls()
	if nullProbability == 0 {
		return
	}

	for i := 0; i < n; i++ {
		if rng.Float64() < nullProbability {
			vec.Nulls().SetNull(i)
		}
	}
}

func randomType(rng *rand.Rand) *types.T {
	return typeconv.AllSupportedSQLTypes[rng.Intn(len(typeconv.AllSupportedSQLTypes))]
}

// randomTypes returns an n-length slice of random types.T.
func randomTypes(rng *rand.Rand, n int) []*types.T {
	typs := make([]*types.T, n)
	for i := range typs {
		typs[i] = randomType(rng)
	}
	return typs
}

// RandomBatch returns a batch with a capacity of capacity and a number of
// random elements equal to length (capacity if length is 0). The values will be
// null with a probability of nullProbability.
func RandomBatch(
	allocator *colmem.Allocator,
	rng *rand.Rand,
	typs []*types.T,
	capacity int,
	length int,
	nullProbability float64,
) coldata.Batch {
	batch := allocator.NewMemBatchWithSize(typs, capacity)
	if length == 0 {
		length = capacity
	}
	for _, colVec := range batch.ColVecs() {
		RandomVec(rng, 0 /* bytesFixedLength */, colVec, length, nullProbability)
	}
	batch.SetLength(length)
	return batch
}

// RandomSel creates a random selection vector up to a given batchSize in
// length. probOfOmitting specifies the probability that a row should be omitted
// from the batch (i.e. whether it should be selected out). So if probOfOmitting
// is 0, then the selection vector will contain all rows, but if it is > 0, then
// some rows might be omitted and the length of the selection vector might be
// less than batchSize.
func RandomSel(rng *rand.Rand, batchSize int, probOfOmitting float64) []int {
	if probOfOmitting < 0 || probOfOmitting > 1 {
		colexecerror.InternalError(fmt.Sprintf("probability of omitting a row is %f - outside of [0, 1] range", probOfOmitting))
	}
	sel := make([]int, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		if rng.Float64() < probOfOmitting {
			continue
		}
		sel = append(sel, i)
	}
	return sel
}

// Suppress unused warnings.
// TODO(asubiotto): Remove this once this function is actually used.
var _ = randomTypes

// RandomBatchWithSel is equivalent to RandomBatch, but will also add a
// selection vector to the batch where each row is selected with probability
// selProbability. If selProbability is 1, all the rows will be selected, if
// selProbability is 0, none will. The returned batch will have its length set
// to the length of the selection vector, unless selProbability is 0.
func RandomBatchWithSel(
	allocator *colmem.Allocator,
	rng *rand.Rand,
	typs []*types.T,
	n int,
	nullProbability float64,
	selProbability float64,
) coldata.Batch {
	batch := RandomBatch(allocator, rng, typs, n, 0 /* length */, nullProbability)
	if selProbability != 0 {
		sel := RandomSel(rng, n, 1-selProbability)
		batch.SetSelection(true)
		copy(batch.Selection(), sel)
		batch.SetLength(len(sel))
	}
	return batch
}

const (
	defaultMaxSchemaLength = 8
	defaultNumBatches      = 4
)

// RandomDataOpArgs are arguments passed in to RandomDataOp. All arguments are
// optional (refer to the constants above this struct definition for the
// defaults). Bools are false by default and AvailableTyps defaults to
// typeconv.AllSupportedSQLTypes.
type RandomDataOpArgs struct {
	// DeterministicTyps, if set, overrides AvailableTyps and MaxSchemaLength,
	// forcing the RandomDataOp to use this schema.
	DeterministicTyps []*types.T
	// AvailableTyps is the pool of types from which the operator's schema will
	// be generated.
	AvailableTyps []*types.T
	// MaxSchemaLength is the maximum length of the operator's schema, which will
	// be at least one type.
	MaxSchemaLength int
	// BatchSize() is the size of batches returned.
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
	BatchAccumulator func(b coldata.Batch, typs []*types.T)
}

// RandomDataOp is an operator that generates random data according to
// RandomDataOpArgs. Call GetBuffer to get all data that was returned.
type RandomDataOp struct {
	allocator        *colmem.Allocator
	batchAccumulator func(b coldata.Batch, typs []*types.T)
	typs             []*types.T
	rng              *rand.Rand
	batchSize        int
	numBatches       int
	numReturned      int
	selection        bool
	nulls            bool
}

var _ colexecbase.Operator = &RandomDataOp{}

// NewRandomDataOp creates a new RandomDataOp.
func NewRandomDataOp(
	allocator *colmem.Allocator, rng *rand.Rand, args RandomDataOpArgs,
) *RandomDataOp {
	var (
		availableTyps   = typeconv.AllSupportedSQLTypes
		maxSchemaLength = defaultMaxSchemaLength
		batchSize       = coldata.BatchSize()
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
		typs = make([]*types.T, 1+rng.Intn(maxSchemaLength))
		for i := range typs {
			typs[i] = availableTyps[rng.Intn(len(availableTyps))]
		}
	}
	return &RandomDataOp{
		allocator:        allocator,
		batchAccumulator: args.BatchAccumulator,
		typs:             typs,
		rng:              rng,
		batchSize:        batchSize,
		numBatches:       numBatches,
		selection:        args.Selection,
		nulls:            args.Nulls,
	}
}

// Init is part of the colexec.Operator interface.
func (o *RandomDataOp) Init() {}

// Next is part of the colexec.Operator interface.
func (o *RandomDataOp) Next(ctx context.Context) coldata.Batch {
	if o.numReturned == o.numBatches {
		// Done.
		b := coldata.ZeroBatch
		if o.batchAccumulator != nil {
			o.batchAccumulator(b, o.typs)
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

		b := RandomBatchWithSel(o.allocator, o.rng, o.typs, o.batchSize, nullProbability, selProbability)
		if !o.selection {
			b.SetSelection(false)
		}
		if b.Length() == 0 {
			// Don't return a zero-length batch until we return o.numBatches batches.
			continue
		}
		o.numReturned++
		if o.batchAccumulator != nil {
			o.batchAccumulator(b, o.typs)
		}
		return b
	}
}

// ChildCount implements the execinfra.OpNode interface.
func (o *RandomDataOp) ChildCount(verbose bool) int {
	return 0
}

// Child implements the execinfra.OpNode interface.
func (o *RandomDataOp) Child(nth int, verbose bool) execinfra.OpNode {
	colexecerror.InternalError(fmt.Sprintf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// Typs returns the output types of the RandomDataOp.
func (o *RandomDataOp) Typs() []*types.T {
	return o.typs
}
