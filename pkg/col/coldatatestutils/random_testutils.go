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
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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

// RandomVecArgs is a utility struct that contains arguments to RandomVec call.
type RandomVecArgs struct {
	// Rand is the provided RNG.
	Rand *rand.Rand
	// Vec is the vector to be filled with random values.
	Vec coldata.Vec
	// N is the number of values to be generated.
	N int
	// NullProbability determines the probability of a single value being NULL.
	NullProbability float64

	// BytesFixedLength (when greater than zero) specifies the fixed length of
	// the bytes slice to be generated. It is used only if vec's physical
	// representation is flat bytes.
	BytesFixedLength int
	// IntRange (when greater than zero) determines the range of possible
	// values for integer vectors; namely, all values will be in
	// (-IntRange, +IntRange) interval.
	IntRange int
	// ZeroProhibited determines whether numeric zero values are disallowed to
	// be generated.
	ZeroProhibited bool
}

// RandomVec populates vector with random values, setting each value to null
// with the given probability. It is assumed that N is in bounds of the given
// vector.
func RandomVec(args RandomVecArgs) {
	switch args.Vec.CanonicalTypeFamily() {
	case types.BoolFamily:
		bools := args.Vec.Bool()
		for i := 0; i < args.N; i++ {
			if args.Rand.Float64() < 0.5 {
				bools[i] = true
			} else {
				bools[i] = false
			}
		}
	case types.BytesFamily:
		bytes := args.Vec.Bytes()
		for i := 0; i < args.N; i++ {
			bytesLen := args.BytesFixedLength
			if bytesLen <= 0 {
				bytesLen = args.Rand.Intn(maxVarLen)
			}
			randBytes := make([]byte, bytesLen)
			// Read always returns len(bytes[i]) and nil.
			_, _ = rand.Read(randBytes)
			bytes.Set(i, randBytes)
		}
	case types.DecimalFamily:
		decs := args.Vec.Decimal()
		for i := 0; i < args.N; i++ {
			// int64(args.Rand.Uint64()) to get negative numbers, too
			decs[i].SetFinite(int64(args.Rand.Uint64()), int32(args.Rand.Intn(40)-20))
			if args.ZeroProhibited {
				if decs[i].IsZero() {
					i--
				}
			}
		}
	case types.IntFamily:
		switch args.Vec.Type().Width() {
		case 16:
			ints := args.Vec.Int16()
			for i := 0; i < args.N; i++ {
				ints[i] = int16(args.Rand.Uint64())
				if args.IntRange != 0 {
					ints[i] = ints[i] % int16(args.IntRange)
				}
				if args.ZeroProhibited {
					if ints[i] == 0 {
						i--
					}
				}
			}
		case 32:
			ints := args.Vec.Int32()
			for i := 0; i < args.N; i++ {
				ints[i] = int32(args.Rand.Uint64())
				if args.IntRange != 0 {
					ints[i] = ints[i] % int32(args.IntRange)
				}
				if args.ZeroProhibited {
					if ints[i] == 0 {
						i--
					}
				}
			}
		case 0, 64:
			ints := args.Vec.Int64()
			for i := 0; i < args.N; i++ {
				ints[i] = int64(args.Rand.Uint64())
				if args.IntRange != 0 {
					ints[i] = ints[i] % int64(args.IntRange)
				}
				if args.ZeroProhibited {
					if ints[i] == 0 {
						i--
					}
				}
			}
		}
	case types.FloatFamily:
		floats := args.Vec.Float64()
		for i := 0; i < args.N; i++ {
			floats[i] = args.Rand.Float64()
			if args.ZeroProhibited {
				if floats[i] == 0 {
					i--
				}
			}
		}
	case types.TimestampTZFamily:
		timestamps := args.Vec.Timestamp()
		for i := 0; i < args.N; i++ {
			timestamps[i] = timeutil.Unix(args.Rand.Int63n(1000000), args.Rand.Int63n(1000000))
			loc := locations[args.Rand.Intn(len(locations))]
			timestamps[i] = timestamps[i].In(loc)
		}
	case types.IntervalFamily:
		intervals := args.Vec.Interval()
		for i := 0; i < args.N; i++ {
			intervals[i] = duration.FromFloat64(args.Rand.Float64())
		}
	case types.JsonFamily:
		j := args.Vec.JSON()
		for i := 0; i < args.N; i++ {
			random, err := json.Random(20, args.Rand)
			if err != nil {
				panic(err)
			}
			j.Set(i, random)
		}
	default:
		datums := args.Vec.Datum()
		for i := 0; i < args.N; i++ {
			datums.Set(i, randgen.RandDatum(args.Rand, args.Vec.Type(), false /* nullOk */))
		}
	}
	args.Vec.Nulls().UnsetNulls()
	if args.NullProbability == 0 {
		return
	}

	for i := 0; i < args.N; i++ {
		if args.Rand.Float64() < args.NullProbability {
			setNull(args.Rand, args.Vec, i)
		}
	}
}

// setNull sets ith element in vec to null and might set the actual value (which
// should be ignored) to some garbage.
func setNull(rng *rand.Rand, vec coldata.Vec, i int) {
	vec.Nulls().SetNull(i)
	switch vec.CanonicalTypeFamily() {
	case types.DecimalFamily:
		_, err := vec.Decimal()[i].SetFloat64(rng.Float64())
		if err != nil {
			colexecerror.InternalError(errors.AssertionFailedf("%v", err))
		}
	case types.IntervalFamily:
		vec.Interval()[i] = duration.MakeDuration(rng.Int63(), rng.Int63(), rng.Int63())
	}
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
	batch := allocator.NewMemBatchWithFixedCapacity(typs, capacity)
	if length == 0 {
		length = capacity
	}
	for _, colVec := range batch.ColVecs() {
		RandomVec(RandomVecArgs{
			Rand:            rng,
			Vec:             colVec,
			N:               length,
			NullProbability: nullProbability,
		})
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
		colexecerror.InternalError(errors.AssertionFailedf("probability of omitting a row is %f - outside of [0, 1] range", probOfOmitting))
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
// defaults). Bools are false by default.
type RandomDataOpArgs struct {
	// DeterministicTyps, if set, overrides MaxSchemaLength and disables type
	// randomization, forcing the RandomDataOp to use this schema.
	DeterministicTyps []*types.T
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
	BatchAccumulator func(ctx context.Context, b coldata.Batch, typs []*types.T)
}

// RandomDataOp is an operator that generates random data according to
// RandomDataOpArgs. Call GetBuffer to get all data that was returned.
type RandomDataOp struct {
	ctx              context.Context
	allocator        *colmem.Allocator
	batchAccumulator func(ctx context.Context, b coldata.Batch, typs []*types.T)
	typs             []*types.T
	rng              *rand.Rand
	batchSize        int
	numBatches       int
	numReturned      int
	selection        bool
	nulls            bool
}

var _ colexecop.Operator = &RandomDataOp{}

// NewRandomDataOp creates a new RandomDataOp.
func NewRandomDataOp(
	allocator *colmem.Allocator, rng *rand.Rand, args RandomDataOpArgs,
) *RandomDataOp {
	var (
		maxSchemaLength = defaultMaxSchemaLength
		batchSize       = coldata.BatchSize()
		numBatches      = defaultNumBatches
	)
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
			typs[i] = randgen.RandType(rng)
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

// Init is part of the colexecop.Operator interface.
func (o *RandomDataOp) Init(ctx context.Context) {
	o.ctx = ctx
}

// Next is part of the colexecop.Operator interface.
func (o *RandomDataOp) Next() coldata.Batch {
	if o.numReturned == o.numBatches {
		// Done.
		b := coldata.ZeroBatch
		if o.batchAccumulator != nil {
			o.batchAccumulator(o.ctx, b, o.typs)
		}
		return b
	}

	var (
		selProbability  float64
		nullProbability float64
	)
	if o.selection {
		selProbability = o.rng.Float64()
	}
	if o.nulls && o.rng.Float64() > 0.1 {
		// Even if nulls are desired, in 10% of cases create a batch with no
		// nulls at all.
		nullProbability = o.rng.Float64()
	}
	for {
		b := RandomBatchWithSel(o.allocator, o.rng, o.typs, o.batchSize, nullProbability, selProbability)
		if b.Length() == 0 {
			// Don't return a zero-length batch until we return o.numBatches batches.
			continue
		}
		o.numReturned++
		if o.batchAccumulator != nil {
			o.batchAccumulator(o.ctx, b, o.typs)
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
	colexecerror.InternalError(errors.AssertionFailedf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// Typs returns the output types of the RandomDataOp.
func (o *RandomDataOp) Typs() []*types.T {
	return o.typs
}
