// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stats

import (
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
)

// TestRandomQuantileRoundTrip creates a random histogram of each type, and
// tests that each can be converted to a quantile function and back without
// changing.
func TestRandomQuantileRoundTrip(t *testing.T) {
	colTypes := []*types.T{
		// Types not in types.Scalar.
		types.Int4,
		types.Int2,
		types.Float4,
	}
	colTypes = append(colTypes, types.Scalar...)
	evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	rng, seed := randutil.NewTestRand()
	for _, colType := range colTypes {
		if canMakeQuantile(colType) {
			for i := 0; i < 5; i++ {
				t.Run(fmt.Sprintf("%v/%v", colType.Name(), i), func(t *testing.T) {
					hist, rowCount := randHist(evalCtx, colType, rng)
					qfun, err := makeQuantile(hist, rowCount)
					if err != nil {
						t.Errorf("seed: %v unexpected makeQuantile error: %v", seed, err)
						return
					}
					hist2, err := qfun.toHistogram(evalCtx, colType, rowCount)
					if err != nil {
						t.Errorf("seed: %v unexpected quantile.toHistogram error: %v", seed, err)
						return
					}
					if !reflect.DeepEqual(hist, hist2) {
						t.Errorf("seed: %v incorrect histogram:\n%v\nexpected:\n%v", seed, hist2, hist)
					}
				})
			}
		}
	}
}

// randHist makes a random histogram of the specified type, with [1, 200]
// buckets. Not all types are supported. Every bucket will have NumEq > 0 but
// could have NumRange == 0.
func randHist(evalCtx *eval.Context, colType *types.T, rng *rand.Rand) (histogram, float64) {
	numBuckets := rng.Intn(200) + 1
	buckets := make([]cat.HistogramBucket, numBuckets)
	bounds := randBounds(evalCtx, colType, rng, numBuckets)
	buckets[0].NumEq = float64(rng.Intn(100) + 1)
	buckets[0].UpperBound = bounds[0]
	rowCount := buckets[0].NumEq
	for i := 1; i < len(buckets); i++ {
		buckets[i].NumEq = float64(rng.Intn(100) + 1)
		buckets[i].NumRange = float64(rng.Intn(1000))
		buckets[i].UpperBound = bounds[i]
		rowCount += buckets[i].NumEq + buckets[i].NumRange
	}
	// Adjust counts so that we have a power-of-two rowCount to avoid floating point errors.
	targetRowCount := 1 << int(math.Ceil(math.Log2(rowCount)))
	for rowCount < float64(targetRowCount) {
		rows := float64(rng.Intn(targetRowCount - int(rowCount) + 1))
		bucket := rng.Intn(numBuckets)
		if bucket == 0 || rng.Float32() < 0.1 {
			buckets[bucket].NumEq += rows
		} else {
			buckets[bucket].NumRange += rows
		}
		rowCount += rows
	}
	// Set DistinctRange in all buckets.
	for i := 1; i < len(buckets); i++ {
		lowerBound := getNextLowerBound(evalCtx, buckets[i-1].UpperBound)
		buckets[i].DistinctRange = estimatedDistinctValuesInRange(
			evalCtx, buckets[i].NumRange, lowerBound, buckets[i].UpperBound,
		)
	}
	return histogram{buckets: buckets}, rowCount
}

// randBounds creates an ordered slice of num distinct Datums of the specified
// type. Not all types are supported. This differs from randgen.RandDatum in
// that it generates no "interesting" Datums, and differs from
// randgen.RandDatumSimple in that it generates distinct Datums without repeats.
func randBounds(evalCtx *eval.Context, colType *types.T, rng *rand.Rand, num int) tree.Datums {
	datums := make(tree.Datums, num)

	// randInts creates an ordered slice of num distinct random ints in the closed
	// interval [lo, hi].
	randInts := func(num, lo, hi int) []int {
		vals := make([]int, 0, num)
		set := make(map[int]struct{}, num)
		span := hi - lo + 1
		for len(vals) < num {
			val := rng.Intn(span) + lo
			if _, ok := set[val]; !ok {
				set[val] = struct{}{}
				vals = append(vals, val)
			}
		}
		sort.Ints(vals)
		return vals
	}

	// randFloat64s creates an ordered slice of num distinct random float64s in
	// the half-open interval [lo, hi). If single is true they will also work as
	// float32s.
	randFloat64s := func(num int, lo, hi float64, single bool) []float64 {
		vals := make([]float64, 0, num)
		set := make(map[float64]struct{}, num)
		span := hi - lo
		for len(vals) < num {
			val := rng.Float64()*span + lo
			if single {
				val = float64(float32(val))
			}
			if _, ok := set[val]; !ok {
				set[val] = struct{}{}
				vals = append(vals, val)
			}
		}
		sort.Float64s(vals)
		return vals
	}

	switch colType.Family() {
	case types.IntFamily:
		// First make sure we won't overflow in randInts (i.e. make sure that
		// hi - lo + 1 <= math.MaxInt which requires -2 for hi).
		w := int(bits.UintSize) - 2
		lo := -1 << w
		hi := (1 << w) - 2
		// Then make sure hi and lo are representable by float64.
		if w > 53 {
			w = 53
			lo = -1 << w
			hi = 1 << w
		}
		// Finally make sure hi and low are representable by this type.
		if w > int(colType.Width())-1 {
			w = int(colType.Width()) - 1
			lo = -1 << w
			hi = (1 << w) - 1
		}
		vals := randInts(num, lo, hi)
		for i := range datums {
			datums[i] = tree.NewDInt(tree.DInt(int64(vals[i])))
		}
	case types.FloatFamily:
		var lo, hi float64
		if colType.Width() == 32 {
			lo = -math.MaxFloat32
			hi = math.MaxFloat32
		} else {
			// Divide by 2 to make sure we won't overflow in randFloat64s.
			lo = -math.MaxFloat64 / 2
			hi = math.MaxFloat64 / 2
		}
		vals := randFloat64s(num, lo, hi, colType.Width() == 32)
		for i := range datums {
			datums[i] = tree.NewDFloat(tree.DFloat(vals[i]))
		}
	case types.DateFamily:
		lo := int(pgdate.LowDate.PGEpochDays())
		hi := int(pgdate.HighDate.PGEpochDays())
		vals := randInts(num, lo, hi)
		for i := range datums {
			datums[i] = tree.NewDDate(pgdate.MakeDateFromPGEpochClampFinite(int32(vals[i])))
		}
	case types.TimestampFamily, types.TimestampTZFamily:
		roundTo := tree.TimeFamilyPrecisionToRoundDuration(colType.Precision())
		var lo, hi int
		if quantileMaxTimestampSec < math.MaxInt/2 {
			lo = int(quantileMinTimestampSec)
			hi = int(quantileMaxTimestampSec)
		} else {
			// Make sure we won't overflow in randInts (i.e. make sure that
			// hi - lo + 1 <= math.MaxInt which requires -2 for hi).
			w := int(bits.UintSize) - 2
			lo = -1 << w
			hi = (1 << w) - 2
		}
		secs := randInts(num, lo, hi)
		for i := range datums {
			t := timeutil.Unix(int64(secs[i]), 0)
			var err error
			if colType.Family() == types.TimestampFamily {
				datums[i], err = tree.MakeDTimestamp(t, roundTo)
			} else {
				datums[i], err = tree.MakeDTimestampTZ(t, roundTo)
			}
			if err != nil {
				panic(err)
			}
		}
	default:
		panic(colType.Name())
	}
	return datums
}

// Test basic conversions from histogram to quantile.
func TestMakeQuantile(t *testing.T) {
	// We use all floats here. TestToQuantileValue and TestFromQuantileValue test
	// conversions to other datatypes.
	testCases := []struct {
		hist testHistogram
		rows float64
		qfun quantile
		err  bool
	}{
		{
			hist: nil,
			rows: 0,
			qfun: zeroQuantile,
		},
		{
			hist: testHistogram{},
			rows: 0,
			qfun: zeroQuantile,
		},
		{
			hist: testHistogram{{0, 0, 0, 0}},
			rows: 0,
			qfun: zeroQuantile,
		},
		{
			hist: testHistogram{{0, 0, 0, 100}, {0, 0, 0, 200}},
			rows: 10,
			qfun: zeroQuantile,
		},
		{
			hist: testHistogram{{1, 0, 0, 0}},
			rows: 1,
			qfun: zeroQuantile,
		},
		{
			hist: testHistogram{{2, 0, 0, 0}},
			rows: 2,
			qfun: zeroQuantile,
		},
		{
			hist: testHistogram{{1, 0, 0, 100}},
			rows: 1,
			qfun: quantile{{0, 100}, {1, 100}},
		},
		{
			hist: testHistogram{{1, 0, 0, 100}, {0, 1, 1, 200}},
			rows: 2,
			qfun: quantile{{0, 100}, {0.5, 100}, {1, 200}},
		},
		{
			hist: testHistogram{{1, 0, 0, 100}, {2, 1, 1, 200}},
			rows: 4,
			qfun: quantile{{0, 100}, {0.25, 100}, {0.5, 200}, {1, 200}},
		},
		{
			hist: testHistogram{{0, 0, 0, 100}, {6, 2, 2, 200}},
			rows: 8,
			qfun: quantile{{0, 100}, {0.25, 200}, {1, 200}},
		},
		{
			hist: testHistogram{{0, 0, 0, 100}, {6, 2, 2, 200}},
			rows: 8,
			qfun: quantile{{0, 100}, {0.25, 200}, {1, 200}},
		},
		{
			hist: testHistogram{{2, 0, 0, 100}, {6, 2, 2, 200}, {2, 0, 0, 300}, {0, 4, 4, 400}},
			rows: 16,
			qfun: quantile{{0, 100}, {0.125, 100}, {0.25, 200}, {0.625, 200}, {0.625, 300}, {0.75, 300}, {1, 400}},
		},
		// Cases where we trim leading and trailing zero buckets.
		{
			hist: testHistogram{{0, 0, 0, 0}, {0, 0, 0, 100}, {2, 2, 2, 200}},
			rows: 4,
			qfun: quantile{{0, 100}, {0.5, 200}, {1, 200}},
		},
		{
			hist: testHistogram{{0, 0, 0, 100}, {2, 6, 6, 200}, {0, 0, 0, 300}},
			rows: 8,
			qfun: quantile{{0, 100}, {0.75, 200}, {1, 200}},
		},
		{
			hist: testHistogram{{0, 0, 0, 0}, {4, 0, 0, 100}, {1, 3, 3, 200}, {0, 0, 0, 300}},
			rows: 8,
			qfun: quantile{{0, 100}, {0.5, 100}, {0.875, 200}, {1, 200}},
		},
		// Cases where we clamp p to 1 to fix histogram errors.
		{
			hist: testHistogram{{2, 0, 0, 100}},
			rows: 1,
			qfun: quantile{{0, 100}, {1, 100}},
		},
		{
			hist: testHistogram{{1, 0, 0, 100}, {0, 1, 1, 200}},
			rows: 1,
			qfun: quantile{{0, 100}, {1, 100}, {1, 200}},
		},
		// Error cases.
		{
			hist: testHistogram{},
			rows: math.Inf(1),
			err:  true,
		},
		{
			hist: testHistogram{},
			rows: math.NaN(),
			err:  true,
		},
		{
			hist: testHistogram{},
			rows: -1,
			err:  true,
		},
		{
			hist: testHistogram{{0, 1, 1, 100}},
			rows: 1,
			err:  true,
		},
		{
			hist: testHistogram{{-1, 0, 0, 100}},
			rows: 1,
			err:  true,
		},
		{
			hist: testHistogram{{math.Inf(1), 0, 0, 100}},
			rows: 1,
			err:  true,
		},
		{
			hist: testHistogram{{1, 0, 0, 100}, {1, 0, 0, 99}},
			rows: 2,
			err:  true,
		},
		{
			hist: testHistogram{{1, 0, 0, 100}, {0, 1, 1, 100}},
			rows: 2,
			err:  true,
		},
	}
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			qfun, err := makeQuantile(tc.hist.toHistogram(), tc.rows)
			if err != nil {
				if !tc.err {
					t.Errorf("test case %d unexpected makeQuantile err: %v", i, err)
				}
				return
			}
			if tc.err {
				t.Errorf("test case %d expected makeQuantile err", i)
				return
			}
			if !reflect.DeepEqual(qfun, tc.qfun) {
				t.Errorf("test case %d incorrect quantile %v expected %v", i, qfun, tc.qfun)
			}
		})
	}
}

// Test basic conversions from quantile to histogram.
func TestQuantileToHistogram(t *testing.T) {
	// We use all floats here. TestToQuantileValue and TestFromQuantileValue test
	// conversions to other datatypes.
	testCases := []struct {
		qfun quantile
		rows float64
		hist testHistogram
		err  bool
	}{
		{
			qfun: zeroQuantile,
			rows: 0,
			hist: nil,
		},
		{
			qfun: zeroQuantile,
			rows: 1,
			hist: testHistogram{{1, 0, 0, 0}},
		},
		{
			qfun: zeroQuantile,
			rows: 2,
			hist: testHistogram{{2, 0, 0, 0}},
		},
		{
			qfun: quantile{{0, 100}, {1, 100}},
			rows: 1,
			hist: testHistogram{{1, 0, 0, 100}},
		},
		{
			qfun: quantile{{0, 0}, {0, 100}, {1, 100}},
			rows: 1,
			hist: testHistogram{{1, 0, 0, 100}},
		},
		{
			qfun: quantile{{0, 100}, {1, 100}, {1, 100}},
			rows: 1,
			hist: testHistogram{{1, 0, 0, 100}},
		},
		{
			qfun: quantile{{0, 100}, {1, 100}, {1, 200}},
			rows: 1,
			hist: testHistogram{{1, 0, 0, 100}},
		},
		{
			qfun: quantile{{0, 0}, {1, 100}},
			rows: 1,
			hist: testHistogram{{0, 0, 0, 0}, {0, 1, 1, 100}},
		},
		{
			qfun: quantile{{0, 0}, {0.5, 100}, {1, 100}},
			rows: 2,
			hist: testHistogram{{0, 0, 0, 0}, {1, 1, 1, 100}},
		},
		{
			qfun: quantile{{0, 0}, {0.9, 100}, {1, 100}},
			rows: 10,
			hist: testHistogram{{0, 0, 0, 0}, {1, 9, 9, 100}},
		},
		{
			qfun: quantile{{0, 100}, {0.25, 100}, {0.75, 200}, {1, 200}},
			rows: 16,
			hist: testHistogram{{4, 0, 0, 100}, {4, 8, 8, 200}},
		},
		{
			qfun: quantile{{0, 100}, {0.25, 100}, {0.5, 200}, {0.75, 200}, {0.75, 300}, {1, 300}},
			rows: 16,
			hist: testHistogram{{4, 0, 0, 100}, {4, 4, 4, 200}, {4, 0, 0, 300}},
		},
		{
			qfun: quantile{{0, 500}, {0.125, 500}, {0.25, 600}, {0.5, 600}, {0.75, 800}, {1, 800}},
			rows: 16,
			hist: testHistogram{{2, 0, 0, 500}, {4, 2, 2, 600}, {4, 4, 4, 800}},
		},
		{
			qfun: quantile{{0, 300}, {0, 310}, {0.125, 310}, {0.125, 320}, {0.25, 320}, {0.25, 330}, {0.5, 330}, {0.5, 340}, {0.625, 340}, {0.625, 350}, {0.75, 350}, {0.75, 360}, {0.875, 360}, {0.875, 370}, {1, 370}},
			rows: 32,
			hist: testHistogram{{4, 0, 0, 310}, {4, 0, 0, 320}, {8, 0, 0, 330}, {4, 0, 0, 340}, {4, 0, 0, 350}, {4, 0, 0, 360}, {4, 0, 0, 370}},
		},
		// Cases where we steal a row from NumRange to give to NumEq.
		{
			qfun: quantile{{0, 0}, {1, 100}},
			rows: 2,
			hist: testHistogram{{0, 0, 0, 0}, {1, 1, 1, 100}},
		},
		{
			qfun: quantile{{0, 100}, {0.5, 100}, {1, 200}, {1, 300}},
			rows: 4,
			hist: testHistogram{{2, 0, 0, 100}, {1, 1, 1, 200}},
		},
		{
			qfun: quantile{{0, 0}, {0.875, 87.5}, {1, 100}},
			rows: 8,
			hist: testHistogram{{0, 0, 0, 0}, {1, 6, 6, 87.5}, {0, 1, 1, 100}},
		},
		{
			qfun: quantile{{0, 400}, {0.5, 600}, {0.75, 700}, {1, 800}},
			rows: 16,
			hist: testHistogram{{0, 0, 0, 400}, {1, 7, 7, 600}, {1, 3, 3, 700}, {1, 3, 3, 800}},
		},
		// Error cases.
		{
			qfun: quantile{},
			rows: 1,
			err:  true,
		},
		{
			qfun: quantile{{0, 0}},
			rows: 1,
			err:  true,
		},
		{
			qfun: quantile{{1, 0}, {0, 0}},
			rows: 1,
			err:  true,
		},
		{
			qfun: quantile{{0, 100}, {0, 200}},
			rows: 1,
			err:  true,
		},
		{
			qfun: quantile{{0, 100}, {math.NaN(), 100}, {1, 100}},
			rows: 1,
			err:  true,
		},
		{
			qfun: quantile{{0, 0}, {0.75, 25}, {0.25, 75}, {1, 100}},
			rows: 1,
			err:  true,
		},
		{
			qfun: quantile{{0, 100}, {1, 99}},
			rows: 1,
			err:  true,
		},
	}
	evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			hist, err := tc.qfun.toHistogram(evalCtx, types.Float, tc.rows)
			if err != nil {
				if !tc.err {
					t.Errorf("test case %d unexpected quantile.toHistogram err: %v", i, err)
				}
				return
			}
			if tc.err {
				t.Errorf("test case %d expected quantile.toHistogram err", i)
				return
			}
			hist2 := tc.hist.toHistogram()
			if !reflect.DeepEqual(hist, hist2) {
				t.Errorf("test case %d incorrect histogram %v expected %v", i, hist, hist2)
			}
		})
	}
}

// testHistogram is a float64-only histogram, which is a little more convenient
// to construct than a normal histogram with tree.Datum UpperBounds.
type testHistogram []testBucket

type testBucket struct {
	NumEq, NumRange, DistinctRange, UpperBound float64
}

func (th testHistogram) toHistogram() histogram {
	if th == nil {
		return histogram{}
	}
	h := histogram{buckets: make([]cat.HistogramBucket, len(th))}
	for i := range th {
		h.buckets[i].NumEq = th[i].NumEq
		h.buckets[i].NumRange = th[i].NumRange
		h.buckets[i].DistinctRange = th[i].DistinctRange
		h.buckets[i].UpperBound = tree.NewDFloat(tree.DFloat(th[i].UpperBound))
	}
	return h
}

// Test conversions from datum to quantile value and back.
func TestQuantileValueRoundTrip(t *testing.T) {
	testCases := []struct {
		typ *types.T
		dat tree.Datum
		val float64
		err bool
	}{
		// Integer cases.
		{
			typ: types.Int,
			dat: tree.NewDInt(tree.DInt(0)),
			val: 0,
		},
		{
			typ: types.Int,
			dat: tree.NewDInt(tree.DInt(42)),
			val: 42,
		},
		{
			typ: types.Int,
			dat: tree.NewDInt(tree.DInt(math.MinInt32 - 1)),
			val: math.MinInt32 - 1,
		},
		{
			typ: types.Int,
			dat: tree.NewDInt(tree.DInt(math.MaxInt32 + 1)),
			val: math.MaxInt32 + 1,
		},
		{
			typ: types.Int,
			dat: tree.NewDInt(tree.DInt(math.MinInt64)),
			val: math.MinInt64,
		},
		{
			typ: types.Int,
			dat: tree.NewDInt(tree.DInt(math.MaxInt64)),
			val: math.MaxInt64,
		},
		{
			typ: types.Int4,
			dat: tree.NewDInt(tree.DInt(math.MinInt16 - 1)),
			val: math.MinInt16 - 1,
		},
		{
			typ: types.Int4,
			dat: tree.NewDInt(tree.DInt(math.MaxInt16 + 1)),
			val: math.MaxInt16 + 1,
		},
		{
			typ: types.Int4,
			dat: tree.NewDInt(tree.DInt(math.MinInt32)),
			val: math.MinInt32,
		},
		{
			typ: types.Int4,
			dat: tree.NewDInt(tree.DInt(math.MaxInt32)),
			val: math.MaxInt32,
		},
		{
			typ: types.Int2,
			dat: tree.NewDInt(tree.DInt(math.MinInt16)),
			val: math.MinInt16,
		},
		{
			typ: types.Int2,
			dat: tree.NewDInt(tree.DInt(math.MaxInt16)),
			val: math.MaxInt16,
		},
		// Float cases.
		{
			typ: types.Float,
			dat: tree.DZeroFloat,
			val: 0,
		},
		{
			typ: types.Float,
			dat: tree.NewDFloat(tree.DFloat(-math.MaxFloat32 - 1)),
			val: -math.MaxFloat32 - 1,
		},
		{
			typ: types.Float,
			dat: tree.NewDFloat(tree.DFloat(math.MaxFloat32 + 1)),
			val: math.MaxFloat32 + 1,
		},
		{
			typ: types.Float,
			dat: tree.NewDFloat(tree.DFloat(-math.MaxFloat64)),
			val: -math.MaxFloat64,
		},
		{
			typ: types.Float,
			dat: tree.NewDFloat(tree.DFloat(math.MaxFloat64)),
			val: math.MaxFloat64,
		},
		{
			typ: types.Float,
			dat: tree.NewDFloat(tree.DFloat(math.Pi)),
			val: math.Pi,
		},
		{
			typ: types.Float,
			dat: tree.NewDFloat(tree.DFloat(math.SmallestNonzeroFloat64)),
			val: math.SmallestNonzeroFloat64,
		},
		{
			typ: types.Float,
			dat: tree.DNaNFloat,
			err: true,
		},
		{
			typ: types.Float,
			dat: tree.DNegInfFloat,
			err: true,
		},
		{
			typ: types.Float,
			dat: tree.DPosInfFloat,
			err: true,
		},
		{
			typ: types.Float4,
			dat: tree.NewDFloat(tree.DFloat(-math.MaxFloat32)),
			val: -math.MaxFloat32,
		},
		{
			typ: types.Float4,
			dat: tree.NewDFloat(tree.DFloat(math.MaxFloat32)),
			val: math.MaxFloat32,
		},
		{
			typ: types.Float4,
			dat: tree.NewDFloat(tree.DFloat(float32(math.Pi))),
			val: float64(float32(math.Pi)),
		},
		{
			typ: types.Float4,
			dat: tree.NewDFloat(tree.DFloat(math.SmallestNonzeroFloat32)),
			val: math.SmallestNonzeroFloat32,
		},
		{
			typ: types.Float4,
			dat: tree.DNaNFloat,
			err: true,
		},
		{
			typ: types.Float4,
			dat: tree.DNegInfFloat,
			err: true,
		},
		{
			typ: types.Float4,
			dat: tree.DPosInfFloat,
			err: true,
		},
		// Date cases.
		{
			typ: types.Date,
			dat: tree.NewDDate(pgdate.MakeDateFromPGEpochClampFinite(0)),
			val: 0,
		},
		{
			typ: types.Date,
			dat: tree.NewDDate(pgdate.LowDate),
			val: float64(pgdate.LowDate.PGEpochDays()),
		},
		{
			typ: types.Date,
			dat: tree.NewDDate(pgdate.HighDate),
			val: float64(pgdate.HighDate.PGEpochDays()),
		},
		{
			typ: types.Date,
			dat: tree.NewDDate(pgdate.PosInfDate),
			err: true,
		},
		{
			typ: types.Date,
			dat: tree.NewDDate(pgdate.NegInfDate),
			err: true,
		},
		// Timestamp cases.
		{
			typ: types.Timestamp,
			dat: tree.DZeroTimestamp,
			val: float64(tree.DZeroTimestamp.Unix()),
		},
		{
			typ: types.Timestamp,
			dat: &tree.DTimestamp{Time: quantileMinTimestamp},
			val: quantileMinTimestampSec,
		},
		{
			typ: types.Timestamp,
			dat: &tree.DTimestamp{Time: quantileMaxTimestamp},
			val: quantileMaxTimestampSec,
		},
		{
			typ: types.Timestamp,
			dat: &tree.DTimestamp{Time: pgdate.TimeNegativeInfinity},
			err: true,
		},
		{
			typ: types.Timestamp,
			dat: &tree.DTimestamp{Time: pgdate.TimeInfinity},
			err: true,
		},
		{
			typ: types.TimestampTZ,
			dat: tree.DZeroTimestampTZ,
			val: float64(tree.DZeroTimestampTZ.Unix()),
		},
		{
			typ: types.TimestampTZ,
			dat: &tree.DTimestampTZ{Time: quantileMinTimestamp},
			val: quantileMinTimestampSec,
		},
		{
			typ: types.TimestampTZ,
			dat: &tree.DTimestampTZ{Time: quantileMaxTimestamp},
			val: quantileMaxTimestampSec,
		},
		{
			typ: types.TimestampTZ,
			dat: &tree.DTimestampTZ{Time: pgdate.TimeNegativeInfinity},
			err: true,
		},
		{
			typ: types.TimestampTZ,
			dat: &tree.DTimestampTZ{Time: pgdate.TimeInfinity},
			err: true,
		},
	}
	evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			val, err := toQuantileValue(tc.dat)
			if err != nil {
				if !tc.err {
					t.Errorf("test case %d (%v) unexpected toQuantileValue err: %v", i, tc.typ.Name(), err)
				}
				return
			}
			if tc.err {
				t.Errorf("test case %d (%v) expected toQuantileValue err", i, tc.typ.Name())
				return
			}
			if val != tc.val {
				t.Errorf("test case %d (%v) incorrect val %v expected %v", i, tc.typ.Name(), val, tc.val)
				return
			}
			// Check that we can make the round trip.
			res, err := fromQuantileValue(tc.typ, val)
			if err != nil {
				t.Errorf("test case %d (%v) unexpected fromQuantileValue err: %v", i, tc.typ.Name(), err)
				return
			}
			cmp, err := res.CompareError(evalCtx, tc.dat)
			if err != nil {
				t.Errorf("test case %d (%v) unexpected CompareError err: %v", i, tc.typ.Name(), err)
				return
			}
			if cmp != 0 {
				t.Errorf("test case %d (%v) incorrect datum %v expected %v", i, tc.typ.Name(), res, tc.dat)
			}
		})
	}
}

// Test conversions from quantile value to datum and back.
// TestQuantileValueRoundTrip covers similar ground, so here we focus on cases
// that overflow or underflow and have to clamp.
func TestQuantileValueRoundTripOverflow(t *testing.T) {
	testCases := []struct {
		typ *types.T
		val float64
		dat tree.Datum
		err bool
		res float64
	}{
		// Integer cases.
		{
			typ: types.Int,
			val: math.MinInt64 - 1,
			dat: tree.NewDInt(tree.DInt(math.MinInt64)),
			res: math.MinInt64,
		},
		{
			typ: types.Int,
			val: math.MaxInt64 + 1,
			dat: tree.NewDInt(tree.DInt(math.MaxInt64)),
			res: math.MaxInt64,
		},
		{
			typ: types.Int,
			val: -math.MaxFloat64,
			dat: tree.NewDInt(tree.DInt(math.MinInt64)),
			res: math.MinInt64,
		},
		{
			typ: types.Int,
			val: math.MaxFloat64,
			dat: tree.NewDInt(tree.DInt(math.MaxInt64)),
			res: math.MaxInt64,
		},
		{
			typ: types.Int4,
			val: math.MinInt32 - 1,
			dat: tree.NewDInt(tree.DInt(math.MinInt32)),
			res: math.MinInt32,
		},
		{
			typ: types.Int4,
			val: math.MaxInt32 + 1,
			dat: tree.NewDInt(tree.DInt(math.MaxInt32)),
			res: math.MaxInt32,
		},
		{
			typ: types.Int4,
			val: -math.MaxFloat64,
			dat: tree.NewDInt(tree.DInt(math.MinInt32)),
			res: math.MinInt32,
		},
		{
			typ: types.Int4,
			val: math.MaxFloat64,
			dat: tree.NewDInt(tree.DInt(math.MaxInt32)),
			res: math.MaxInt32,
		},
		{
			typ: types.Int2,
			val: math.MinInt16 - 1,
			dat: tree.NewDInt(tree.DInt(math.MinInt16)),
			res: math.MinInt16,
		},
		{
			typ: types.Int2,
			val: math.MaxInt16 + 1,
			dat: tree.NewDInt(tree.DInt(math.MaxInt16)),
			res: math.MaxInt16,
		},
		{
			typ: types.Int2,
			val: -math.MaxFloat64,
			dat: tree.NewDInt(tree.DInt(math.MinInt16)),
			res: math.MinInt16,
		},
		{
			typ: types.Int2,
			val: math.MaxFloat64,
			dat: tree.NewDInt(tree.DInt(math.MaxInt16)),
			res: math.MaxInt16,
		},
		// Float cases.
		{
			typ: types.Float,
			val: -math.MaxFloat64,
			dat: tree.NewDFloat(tree.DFloat(-math.MaxFloat64)),
			res: -math.MaxFloat64,
		},
		{
			typ: types.Float,
			val: math.MaxFloat64,
			dat: tree.NewDFloat(tree.DFloat(math.MaxFloat64)),
			res: math.MaxFloat64,
		},
		{
			typ: types.Float,
			val: -math.SmallestNonzeroFloat64,
			dat: tree.NewDFloat(tree.DFloat(-math.SmallestNonzeroFloat64)),
			res: -math.SmallestNonzeroFloat64,
		},
		{
			typ: types.Float,
			val: math.SmallestNonzeroFloat64,
			dat: tree.NewDFloat(tree.DFloat(math.SmallestNonzeroFloat64)),
			res: math.SmallestNonzeroFloat64,
		},
		{
			typ: types.Float,
			val: math.NaN(),
			err: true,
		},
		{
			typ: types.Float,
			val: math.Inf(-1),
			err: true,
		},
		{
			typ: types.Float,
			val: math.Inf(+1),
			err: true,
		},
		{
			typ: types.Float4,
			val: -math.MaxFloat32 - 1,
			dat: tree.NewDFloat(tree.DFloat(-math.MaxFloat32)),
			res: -math.MaxFloat32,
		},
		{
			typ: types.Float4,
			val: math.MaxFloat32 + 1,
			dat: tree.NewDFloat(tree.DFloat(math.MaxFloat32)),
			res: math.MaxFloat32,
		},
		{
			typ: types.Float4,
			val: -math.MaxFloat64,
			dat: tree.NewDFloat(tree.DFloat(-math.MaxFloat32)),
			res: -math.MaxFloat32,
		},
		{
			typ: types.Float4,
			val: math.MaxFloat64,
			dat: tree.NewDFloat(tree.DFloat(math.MaxFloat32)),
			res: math.MaxFloat32,
		},
		{
			typ: types.Float4,
			val: math.Pi,
			dat: tree.NewDFloat(tree.DFloat(float32(math.Pi))),
			res: float64(float32(math.Pi)),
		},
		{
			typ: types.Float4,
			val: -math.SmallestNonzeroFloat64,
			dat: tree.DZeroFloat,
			res: 0,
		},
		{
			typ: types.Float4,
			val: math.SmallestNonzeroFloat64,
			dat: tree.DZeroFloat,
			res: 0,
		},
		// Date cases.
		{
			typ: types.Date,
			val: float64(pgdate.LowDate.PGEpochDays()) - 1,
			dat: tree.NewDDate(pgdate.LowDate),
			res: float64(pgdate.LowDate.PGEpochDays()),
		},
		{
			typ: types.Date,
			val: float64(pgdate.HighDate.PGEpochDays()) + 1,
			dat: tree.NewDDate(pgdate.HighDate),
			res: float64(pgdate.HighDate.PGEpochDays()),
		},
		{
			typ: types.Date,
			val: -math.MaxFloat64,
			dat: tree.NewDDate(pgdate.LowDate),
			res: float64(pgdate.LowDate.PGEpochDays()),
		},
		{
			typ: types.Date,
			val: math.MaxFloat64,
			dat: tree.NewDDate(pgdate.HighDate),
			res: float64(pgdate.HighDate.PGEpochDays()),
		},
		// Timestamp cases.
		{
			typ: types.Timestamp,
			val: float64(pgdate.TimeNegativeInfinity.Unix()),
			dat: &tree.DTimestamp{Time: quantileMinTimestamp},
			res: quantileMinTimestampSec,
		},
		{
			typ: types.Timestamp,
			val: float64(pgdate.TimeInfinity.Unix()),
			dat: &tree.DTimestamp{Time: quantileMaxTimestamp},
			res: quantileMaxTimestampSec,
		},
		{
			typ: types.Timestamp,
			val: -math.MaxFloat64,
			dat: &tree.DTimestamp{Time: quantileMinTimestamp},
			res: quantileMinTimestampSec,
		},
		{
			typ: types.Timestamp,
			val: math.MaxFloat64,
			dat: &tree.DTimestamp{Time: quantileMaxTimestamp},
			res: quantileMaxTimestampSec,
		},
		{
			typ: types.TimestampTZ,
			val: float64(pgdate.TimeNegativeInfinity.Unix()),
			dat: &tree.DTimestampTZ{Time: quantileMinTimestamp},
			res: quantileMinTimestampSec,
		},
		{
			typ: types.TimestampTZ,
			val: float64(pgdate.TimeInfinity.Unix()),
			dat: &tree.DTimestampTZ{Time: quantileMaxTimestamp},
			res: quantileMaxTimestampSec,
		},
		{
			typ: types.TimestampTZ,
			val: -math.MaxFloat64,
			dat: &tree.DTimestampTZ{Time: quantileMinTimestamp},
			res: quantileMinTimestampSec,
		},
		{
			typ: types.TimestampTZ,
			val: math.MaxFloat64,
			dat: &tree.DTimestampTZ{Time: quantileMaxTimestamp},
			res: quantileMaxTimestampSec,
		},
	}
	evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			d, err := fromQuantileValue(tc.typ, tc.val)
			if err != nil {
				if !tc.err {
					t.Errorf("test case %d (%v) unexpected fromQuantileValue err: %v", i, tc.typ.Name(), err)
				}
				return
			}
			if tc.err {
				t.Errorf("test case %d (%v) expected fromQuantileValue err", i, tc.typ.Name())
				return
			}
			cmp, err := d.CompareError(evalCtx, tc.dat)
			if err != nil {
				t.Errorf("test case %d (%v) unexpected CompareError err: %v", i, tc.typ.Name(), err)
				return
			}
			if cmp != 0 {
				t.Errorf("test case %d (%v) incorrect datum %v expected %v", i, tc.typ.Name(), d, tc.dat)
				return
			}
			// Check that we can make the round trip with the clamped value.
			res, err := toQuantileValue(d)
			if err != nil {
				t.Errorf("test case %d (%v) unexpected toQuantileValue err: %v", i, tc.typ.Name(), err)
				return
			}
			if res != tc.res {
				t.Errorf("test case %d (%v) incorrect val %v expected %v", i, tc.typ.Name(), res, tc.res)
				return
			}
		})
	}
}
