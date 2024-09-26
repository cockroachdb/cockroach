// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import (
	"context"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"testing"

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
	ctx := context.Background()
	colTypes := []*types.T{
		// Types not in types.Scalar.
		types.Int4,
		types.Int2,
		types.Float4,
	}
	colTypes = append(colTypes, types.Scalar...)
	rng, seed := randutil.NewTestRand()
	for _, colType := range colTypes {
		if canMakeQuantile(HistVersion, colType) {
			for i := 0; i < 5; i++ {
				t.Run(fmt.Sprintf("%v/%v", colType.Name(), i), func(t *testing.T) {
					hist, rowCount := randHist(ctx, colType, rng)
					qfun, err := makeQuantile(hist, rowCount)
					if err != nil {
						t.Errorf("seed: %v unexpected makeQuantile error: %v", seed, err)
						return
					}
					hist2, err := qfun.toHistogram(ctx, colType, rowCount)
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
func randHist(ctx context.Context, colType *types.T, rng *rand.Rand) (histogram, float64) {
	numBuckets := rng.Intn(200) + 1
	buckets := make([]cat.HistogramBucket, numBuckets)
	bounds := randBounds(colType, rng, numBuckets)
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
	var compareCtx *eval.Context
	for i := 1; i < len(buckets); i++ {
		lowerBound := getNextLowerBound(ctx, compareCtx, buckets[i-1].UpperBound)
		buckets[i].DistinctRange = estimatedDistinctValuesInRange(
			ctx, compareCtx, buckets[i].NumRange, lowerBound, buckets[i].UpperBound,
		)
	}
	return histogram{buckets: buckets}, rowCount
}

// randBounds creates an ordered slice of num distinct Datums of the specified
// type. Not all types are supported. This differs from randgen.RandDatum in
// that it generates no "interesting" Datums, and differs from
// randgen.RandDatumSimple in that it generates distinct Datums without repeats.
func randBounds(colType *types.T, rng *rand.Rand, num int) tree.Datums {
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
		if tree.MaxSupportedTimeSec < math.MaxInt/2 {
			lo = int(tree.MinSupportedTimeSec)
			hi = int(tree.MaxSupportedTimeSec)
		} else {
			// Make sure we don't overflow in randInts on 32-bit systems.
			// Specifically, make sure that hi - lo + 1 <= math.MaxInt, which requires subtracting 2 from hi.
			w := bits.UintSize - 2
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

// randQuantile makes a random quantile function with [2, 32] points. The first
// point will have p=0 and the last will have p=1. There is some chance of
// repeated p's and some chance of repeated v's. The quantile will always be
// non-decreasing in p, but might be decreasing in v in places (malformed).
func randQuantile(rng *rand.Rand) quantile {
	// Use [2, 32] points.
	numPoints := rng.Intn(31) + 2
	// Use [2, numPoints] distinct p's. The rest will be repeated.
	numPs := rng.Intn(numPoints-1) + 2
	// Use [1, numPoints] distinct v's The rest will be repeated.
	numVs := rng.Intn(numPoints) + 1
	q := make(quantile, numPoints)
	ps := make([]float64, numPs, numPoints)
	vs := make([]float64, numVs, numPoints)
	ps[0], ps[1] = 0, 1
	for i := 2; i < len(ps); i++ {
		ps[i] = rng.Float64()
	}
	for i := range vs {
		vs[i] = rng.NormFloat64() * 1e4
	}
	// Now fill in the rest of the p's and v's with repeats.
	for i := numPs; i < numPoints; i++ {
		ps = append(ps, ps[rng.Intn(numPs)])
	}
	for i := numVs; i < numPoints; i++ {
		vs = append(vs, vs[rng.Intn(numVs)])
	}
	sort.Float64s(ps)
	// Give ourselves a (roughly) 50% chance of a malformed quantile.
	if rng.Float64() < .5 {
		sort.Float64s(vs)
	}
	for i := range q {
		q[i] = quantilePoint{p: ps[i], v: vs[i]}
	}
	return q
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
			hist: testHistogram{},
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
			qfun: quantile{{0, 0}, {0.9375, 100}, {1, 100}},
			rows: 16,
			hist: testHistogram{{0, 0, 0, 0}, {1, 15, 15, 100}},
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
		// Cases with 0 NumEq.
		{
			qfun: quantile{{0, 0}, {1, 100}},
			rows: 2,
			hist: testHistogram{{0, 0, 0, 0}, {0, 2, 2, 100}},
		},
		{
			qfun: quantile{{0, 100}, {0.5, 100}, {1, 200}, {1, 300}},
			rows: 4,
			hist: testHistogram{{2, 0, 0, 100}, {0, 2, 2, 200}},
		},
		{
			qfun: quantile{{0, 0}, {0.875, 87.5}, {1, 100}},
			rows: 8,
			hist: testHistogram{{0, 0, 0, 0}, {0, 7, 7, 87.5}, {0, 1, 1, 100}},
		},
		{
			qfun: quantile{{0, 400}, {0.5, 600}, {0.75, 700}, {1, 800}},
			rows: 16,
			hist: testHistogram{{0, 0, 0, 400}, {0, 8, 8, 600}, {0, 4, 4, 700}, {0, 4, 4, 800}},
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
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			hist, err := tc.qfun.toHistogram(context.Background(), types.Float, tc.rows)
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
		// Unlike in TableStatistic.setHistogramBuckets and
		// histogram.toHistogramData, we do not round here so that we can test the
		// rounding behavior of those functions.
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
			dat: &tree.DTimestamp{Time: pgdate.TimeInfinity},
			err: true,
		},
		{
			typ: types.Timestamp,
			dat: &tree.DTimestamp{Time: pgdate.TimeNegativeInfinity},
			err: true,
		},
		{
			typ: types.TimestampTZ,
			dat: tree.DZeroTimestampTZ,
			val: float64(tree.DZeroTimestampTZ.Unix()),
		},
		{
			typ: types.TimestampTZ,
			dat: &tree.DTimestampTZ{Time: pgdate.TimeInfinity},
			err: true,
		},
		{
			typ: types.TimestampTZ,
			dat: &tree.DTimestampTZ{Time: pgdate.TimeNegativeInfinity},
			err: true,
		},
	}
	ctx := context.Background()
	var compareCtx *eval.Context
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
			cmp, err := res.Compare(ctx, compareCtx, tc.dat)
			if err != nil {
				t.Errorf("test case %d (%v) unexpected Compare err: %v", i, tc.typ.Name(), err)
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
			err: true,
		},
		{
			typ: types.Timestamp,
			val: float64(pgdate.TimeInfinity.Unix()),
			err: true,
		},
		{
			typ: types.Timestamp,
			val: -math.MaxFloat64,
			err: true,
		},
		{
			typ: types.Timestamp,
			val: math.MaxFloat64,
			err: true,
		},
		// TimestampTZ cases.
		{
			typ: types.TimestampTZ,
			val: float64(pgdate.TimeNegativeInfinity.Unix()),
			err: true,
		},
		{
			typ: types.TimestampTZ,
			val: float64(pgdate.TimeInfinity.Unix()),
			err: true,
		},
		{
			typ: types.TimestampTZ,
			val: -math.MaxFloat64,
			err: true,
		},
		{
			typ: types.TimestampTZ,
			val: math.MaxFloat64,
			err: true,
		},
	}
	ctx := context.Background()
	var compareCtx *eval.Context
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
			cmp, err := d.Compare(ctx, compareCtx, tc.dat)
			if err != nil {
				t.Errorf("test case %d (%v) unexpected Compare err: %v", i, tc.typ.Name(), err)
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

// TestQuantileOps tests basic operations on quantile functions.
func TestQuantileOps(t *testing.T) {
	var r = quantile{{0, 2}, {0.5, 3}, {1, 4}}
	var s = quantile{{0, 11}, {0.125, 11}, {0.125, 13}, {0.25, 13}, {0.25, 15}, {1, 21}}
	testCases := []struct {
		q     quantile
		addR  quantile
		addS  quantile
		subR  quantile
		subS  quantile
		mult  quantile
		intSq float64
		fixed quantile
	}{
		// The zero quantile.
		{
			q:     zeroQuantile,
			addR:  r,
			addS:  s,
			subR:  r.mult(-1),
			subS:  s.mult(-1),
			intSq: 0,
			fixed: zeroQuantile,
		},
		// Quantiles with only positive slope.
		{
			q:     quantile{{0, 100}, {1, 200}},
			addR:  quantile{{0, 102}, {0.5, 153}, {1, 204}},
			addS:  quantile{{0, 111}, {0.125, 123.5}, {0.125, 125.5}, {0.25, 138}, {0.25, 140}, {1, 221}},
			subR:  quantile{{0, 98}, {0.5, 147}, {1, 196}},
			subS:  quantile{{0, 89}, {0.125, 101.5}, {0.125, 99.5}, {0.25, 112}, {0.25, 110}, {1, 179}},
			intSq: 23333.333333333332,
			fixed: quantile{{0, 100}, {1, 200}},
		},
		{
			q:     quantile{{0, 100}, {0.5, 200}, {1, 300}},
			addR:  quantile{{0, 102}, {0.5, 203}, {1, 304}},
			addS:  quantile{{0, 111}, {0.125, 136}, {0.125, 138}, {0.25, 163}, {0.25, 165}, {0.5, 217}, {1, 321}},
			subR:  quantile{{0, 98}, {0.5, 197}, {1, 296}},
			subS:  quantile{{0, 89}, {0.125, 114}, {0.125, 112}, {0.25, 137}, {0.25, 135}, {0.5, 183}, {1, 279}},
			intSq: 43333.333333333336,
			fixed: quantile{{0, 100}, {0.5, 200}, {1, 300}},
		},
		{
			q:     quantile{{0, -100}, {0.5, -50}, {0.625, -40}, {0.75, -30}, {1, 60}},
			addR:  quantile{{0, -98}, {0.5, -47}, {0.625, -36.75}, {0.75, -26.5}, {1, 64}},
			addS:  quantile{{0, -89}, {0.125, -76.5}, {0.125, -74.5}, {0.25, -62}, {0.25, -60}, {0.5, -33}, {0.625, -22}, {0.75, -11}, {1, 81}},
			subR:  quantile{{0, -102}, {0.5, -53}, {0.625, -43.25}, {0.75, -33.5}, {1, 56}},
			subS:  quantile{{0, -111}, {0.125, -98.5}, {0.125, -100.5}, {0.25, -88}, {0.25, -90}, {0.5, -67}, {0.625, -58}, {0.75, -49}, {1, 39}},
			intSq: 3549.9999999999995,
			fixed: quantile{{0, -100}, {0.5, -50}, {0.625, -40}, {0.75, -30}, {1, 60}},
		},
		// Quantiles with vertical and horizontal pieces.
		{
			q:     quantile{{0, 50}, {0, 60}, {1, 60}},
			addR:  quantile{{0, 52}, {0, 62}, {0.5, 63}, {1, 64}},
			addS:  quantile{{0, 61}, {0, 71}, {0.125, 71}, {0.125, 73}, {0.25, 73}, {0.25, 75}, {1, 81}},
			subR:  quantile{{0, 48}, {0, 58}, {0.5, 57}, {1, 56}},
			subS:  quantile{{0, 39}, {0, 49}, {0.125, 49}, {0.125, 47}, {0.25, 47}, {0.25, 45}, {1, 39}},
			intSq: 3600,
			fixed: quantile{{0, 50}, {0, 60}, {1, 60}},
		},
		{
			q:     quantile{{0, -800}, {1, -800}, {1, -700}},
			addR:  quantile{{0, -798}, {0.5, -797}, {1, -796}, {1, -696}},
			addS:  quantile{{0, -789}, {0.125, -789}, {0.125, -787}, {0.25, -787}, {0.25, -785}, {1, -779}, {1, -679}},
			subR:  quantile{{0, -802}, {0.5, -803}, {1, -804}, {1, -704}},
			subS:  quantile{{0, -811}, {0.125, -811}, {0.125, -813}, {0.25, -813}, {0.25, -815}, {1, -821}, {1, -721}},
			intSq: 640000,
			fixed: quantile{{0, -800}, {1, -800}, {1, -700}},
		},
		{
			q:     quantile{{0, 0}, {0.125, 0}, {0.375, 100}, {0.5, 100}, {0.5, 200}, {0.625, 200}, {0.75, 300}, {0.875, 300}, {0.875, 400}, {0.875, 500}, {1, 600}},
			addR:  quantile{{0, 2}, {0.125, 2.25}, {0.375, 102.75}, {0.5, 103}, {0.5, 203}, {0.625, 203.25}, {0.75, 303.5}, {0.875, 303.75}, {0.875, 403.75}, {0.875, 503.75}, {1, 604}},
			addS:  quantile{{0, 11}, {0.125, 11}, {0.125, 13}, {0.25, 63}, {0.25, 65}, {0.375, 116}, {0.5, 117}, {0.5, 217}, {0.625, 218}, {0.75, 319}, {0.875, 320}, {0.875, 420}, {0.875, 520}, {1, 621}},
			subR:  quantile{{0, -2}, {0.125, -2.25}, {0.375, 97.25}, {0.5, 97}, {0.5, 197}, {0.625, 196.75}, {0.75, 296.5}, {0.875, 296.25}, {0.875, 396.25}, {0.875, 496.25}, {1, 596}},
			subS:  quantile{{0, -11}, {0.125, -11}, {0.125, -13}, {0.25, 37}, {0.25, 35}, {0.375, 84}, {0.5, 83}, {0.5, 183}, {0.625, 182}, {0.75, 281}, {0.875, 280}, {0.875, 380}, {0.875, 480}, {1, 579}},
			intSq: 64166.66666666667,
			fixed: quantile{{0, 0}, {0.125, 0}, {0.375, 100}, {0.5, 100}, {0.5, 200}, {0.625, 200}, {0.75, 300}, {0.875, 300}, {0.875, 400}, {0.875, 500}, {1, 600}},
		},
		// Quantiles with repeated points.
		{
			q:     quantile{{0, 20}, {0, 20}, {1, 30}, {1, 30}},
			addR:  quantile{{0, 22}, {0, 22}, {0.5, 28}, {1, 34}, {1, 34}},
			addS:  quantile{{0, 31}, {0, 31}, {0.125, 32.25}, {0.125, 34.25}, {0.25, 35.5}, {0.25, 37.5}, {1, 51}, {1, 51}},
			subR:  quantile{{0, 18}, {0, 18}, {0.5, 22}, {1, 26}, {1, 26}},
			subS:  quantile{{0, 9}, {0, 9}, {0.125, 10.25}, {0.125, 8.25}, {0.25, 9.5}, {0.25, 7.5}, {1, 9}, {1, 9}},
			intSq: 633.3333333333334,
			fixed: quantile{{0, 20}, {0, 20}, {1, 30}, {1, 30}},
		},
		{
			q:     quantile{{0, 100}, {0, 200}, {0, 200}, {0.5, 300}, {0.5, 300}, {0.5, 400}, {1, 400}},
			addR:  quantile{{0, 102}, {0, 202}, {0, 202}, {0.5, 303}, {0.5, 303}, {0.5, 403}, {1, 404}},
			addS:  quantile{{0, 111}, {0, 211}, {0, 211}, {0.125, 236}, {0.125, 238}, {0.25, 263}, {0.25, 265}, {0.5, 317}, {0.5, 317}, {0.5, 417}, {1, 421}},
			subR:  quantile{{0, 98}, {0, 198}, {0, 198}, {0.5, 297}, {0.5, 297}, {0.5, 397}, {1, 396}},
			subS:  quantile{{0, 89}, {0, 189}, {0, 189}, {0.125, 214}, {0.125, 212}, {0.25, 237}, {0.25, 235}, {0.5, 283}, {0.5, 283}, {0.5, 383}, {1, 379}},
			intSq: 111666.66666666667,
			fixed: quantile{{0, 100}, {0, 200}, {0, 200}, {0.5, 300}, {0.5, 300}, {0.5, 400}, {1, 400}},
		},
		// Malformed quantiles that need to be fixed.
		{
			q:     quantile{{0, 1}, {1, 0}},
			addR:  quantile{{0, 3}, {0.5, 3.5}, {1, 4}},
			addS:  quantile{{0, 12}, {0.125, 11.875}, {0.125, 13.875}, {0.25, 13.75}, {0.25, 15.75}, {1, 21}},
			subR:  quantile{{0, -1}, {0.5, -2.5}, {1, -4}},
			subS:  quantile{{0, -10}, {0.125, -10.125}, {0.125, -12.125}, {0.25, -12.25}, {0.25, -14.25}, {1, -21}},
			intSq: 0.3333333333333333,
			fixed: quantile{{0, 0}, {1, 1}},
		},
		{
			q:     quantile{{0, 0}, {0.25, 1}, {0.5, 0}, {0.75, -1}, {1, 0}},
			addR:  quantile{{0, 2}, {0.25, 3.5}, {0.5, 3}, {0.75, 2.5}, {1, 4}},
			addS:  quantile{{0, 11}, {0.125, 11.5}, {0.125, 13.5}, {0.25, 14}, {0.25, 16}, {0.5, 17}, {0.75, 18}, {1, 21}},
			subR:  quantile{{0, -2}, {0.25, -1.5}, {0.5, -3}, {0.75, -4.5}, {1, -4}},
			subS:  quantile{{0, -11}, {0.125, -10.5}, {0.125, -12.5}, {0.25, -12}, {0.25, -14}, {0.5, -17}, {0.75, -20}, {1, -21}},
			intSq: 0.3333333333333333,
			fixed: quantile{{0, -1}, {0.5, 0}, {1, 1}},
		},
		{
			q:     quantile{{0, 1}, {0.25, 0}, {0.5, 0}, {0.5, -1}, {0.75, -1}, {0.75, 0}, {1, 1}},
			addR:  quantile{{0, 3}, {0.25, 2.5}, {0.5, 3}, {0.5, 2}, {0.75, 2.5}, {0.75, 3.5}, {1, 5}},
			addS:  quantile{{0, 12}, {0.125, 11.5}, {0.125, 13.5}, {0.25, 13}, {0.25, 15}, {0.5, 17}, {0.5, 16}, {0.75, 18}, {0.75, 19}, {1, 22}},
			subR:  quantile{{0, -1}, {0.25, -2.5}, {0.5, -3}, {0.5, -4}, {0.75, -4.5}, {0.75, -3.5}, {1, -3}},
			subS:  quantile{{0, -10}, {0.125, -10.5}, {0.125, -12.5}, {0.25, -13}, {0.25, -15}, {0.5, -17}, {0.5, -18}, {0.75, -20}, {0.75, -19}, {1, -20}},
			intSq: 0.41666666666666663,
			fixed: quantile{{0, -1}, {0.25, -1}, {0.25, 0}, {0.5, 0}, {1, 1}},
		},
		{
			q:     quantile{{0, 100}, {0.125, 100}, {0.25, 200}, {0.375, 300}, {0.375, 200}, {0.5, 100}, {0.625, 100}, {0.75, 0}, {0.875, 0}, {0.875, 100}, {0.875, 400}, {1, 100}},
			addR:  quantile{{0, 102}, {0.125, 102.25}, {0.25, 202.5}, {0.375, 302.75}, {0.375, 202.75}, {0.5, 103}, {0.625, 103.25}, {0.75, 3.5}, {0.875, 3.75}, {0.875, 103.75}, {0.875, 403.75}, {1, 104}},
			addS:  quantile{{0, 111}, {0.125, 111}, {0.125, 113}, {0.25, 213}, {0.25, 215}, {0.375, 316}, {0.375, 216}, {0.5, 117}, {0.625, 118}, {0.75, 19}, {0.875, 20}, {0.875, 120}, {0.875, 420}, {1, 121}},
			subR:  quantile{{0, 98}, {0.125, 97.75}, {0.25, 197.5}, {0.375, 297.25}, {0.375, 197.25}, {0.5, 97}, {0.625, 96.75}, {0.75, -3.5}, {0.875, -3.75}, {0.875, 96.25}, {0.875, 396.25}, {1, 96}},
			subS:  quantile{{0, 89}, {0.125, 89}, {0.125, 87}, {0.25, 187}, {0.25, 185}, {0.375, 284}, {0.375, 184}, {0.5, 83}, {0.625, 82}, {0.75, -19}, {0.875, -20}, {0.875, 80}, {0.875, 380}, {1, 79}},
			intSq: 25416.666666666664,
			fixed: quantile{{0, 0}, {0.125, 0}, {0.25, 100}, {0.5, 100}, {0.7916666666666666, 200}, {0.9583333333333334, 300}, {1, 400}},
		},
	}
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			addQ := tc.q.add(tc.q)
			addR := tc.q.add(r)
			if !reflect.DeepEqual(addR, tc.addR) {
				t.Errorf("test case %d incorrect addR %v expected %v", i, addR, tc.addR)
			}
			addS := tc.q.add(s)
			if !reflect.DeepEqual(addS, tc.addS) {
				t.Errorf("test case %d incorrect addS %v expected %v", i, addS, tc.addS)
			}
			addZ := tc.q.add(zeroQuantile)
			if !reflect.DeepEqual(addZ, tc.q) {
				t.Errorf("test case %d incorrect addZ %v expected %v", i, addZ, tc.q)
			}
			subQ := tc.q.sub(tc.q)
			subR := tc.q.sub(r)
			if !reflect.DeepEqual(subR, tc.subR) {
				t.Errorf("test case %d incorrect subR %v expected %v", i, subR, tc.subR)
			}
			subS := tc.q.sub(s)
			if !reflect.DeepEqual(subS, tc.subS) {
				t.Errorf("test case %d incorrect subS %v expected %v", i, subS, tc.subS)
			}
			subZ := tc.q.sub(zeroQuantile)
			if !reflect.DeepEqual(subZ, tc.q) {
				t.Errorf("test case %d incorrect subZ %v expected %v", i, subZ, tc.q)
			}
			zSub := zeroQuantile.sub(tc.q)
			multNeg1 := tc.q.mult(-1)
			if !reflect.DeepEqual(multNeg1, zSub) {
				t.Errorf("test case %d incorrect multNeg1 %v expected %v", i, multNeg1, zSub)
			}
			mult0 := tc.q.mult(0)
			if !reflect.DeepEqual(mult0, subQ) {
				t.Errorf("test case %d incorrect mult0 %v expected %v", i, mult0, subQ)
			}
			mult1 := tc.q.mult(1)
			if !reflect.DeepEqual(mult1, tc.q) {
				t.Errorf("test case %d incorrect mult1 %v expected %v", i, mult1, tc.q)
			}
			mult2 := tc.q.mult(2)
			if !reflect.DeepEqual(mult2, addQ) {
				t.Errorf("test case %d incorrect mult2 %v expected %v", i, mult2, addQ)
			}
			intSq := tc.q.integrateSquared()
			if intSq != tc.intSq {
				t.Errorf("test case %d incorrect intSq %v expected %v", i, intSq, tc.intSq)
			}
			intSqNeg := multNeg1.integrateSquared()
			if intSqNeg != intSq {
				t.Errorf("test case %d incorrect intSqNeg %v expected %v", i, intSqNeg, intSq)
			}
			intSqMult0 := mult0.integrateSquared()
			if intSqMult0 != 0 {
				t.Errorf("test case %d incorrect intSqMult0 %v expected 0", i, intSqMult0)
			}
			fixed := tc.q.fixMalformed()
			if !reflect.DeepEqual(fixed, tc.fixed) {
				t.Errorf("test case %d incorrect fixed %v expected %v", i, fixed, tc.fixed)
			}
			intSqFixed := fixed.integrateSquared()

			// Truncate to 10 decimal places.
			truncatedIntSqFixed := math.Trunc(intSqFixed*1e10) / 1e10
			truncatedIntSq := math.Trunc(intSq*1e10) / 1e10
			if truncatedIntSqFixed != truncatedIntSq {
				t.Errorf("test case %d incorrect truncatedIntSqFixed %v expected %v", i, truncatedIntSqFixed, truncatedIntSq)
			}
		})
	}
}

// TestQuantileOpsRandom tests basic operations on random quantile functions.
func TestQuantileOpsRandom(t *testing.T) {
	const delta = 1e-2
	rng, seed := randutil.NewTestRand()
	for i := 0; i < 5; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			q := randQuantile(rng)
			addQ := q.add(q)
			addZ := q.add(zeroQuantile)
			if !reflect.DeepEqual(addZ, q) {
				t.Errorf("seed %v quantile %v incorrect addZ %v expected %v", seed, q, addZ, q)
			}
			subQ := q.sub(q)
			subZ := q.sub(zeroQuantile)
			if !reflect.DeepEqual(subZ, q) {
				t.Errorf("seed %v quantile %v incorrect subZ %v expected %v", seed, q, subZ, q)
			}
			zSub := zeroQuantile.sub(q)
			multNeg1 := q.mult(-1)
			if !reflect.DeepEqual(multNeg1, zSub) {
				t.Errorf("seed %v quantile %v incorrect multNeg1 %v expected %v", seed, q, multNeg1, zSub)
			}
			mult0 := q.mult(0)
			if !reflect.DeepEqual(mult0, subQ) {
				t.Errorf("seed %v quantile %v incorrect mult0 %v expected %v", seed, q, mult0, subQ)
			}
			mult1 := q.mult(1)
			if !reflect.DeepEqual(mult1, q) {
				t.Errorf("seed %v quantile %v incorrect mult1 %v expected %v", seed, q, mult1, q)
			}
			mult2 := q.mult(2)
			if !reflect.DeepEqual(mult2, addQ) {
				t.Errorf("seed %v quantile %v incorrect mult2 %v expected %v", seed, q, mult2, addQ)
			}
			intSq := q.integrateSquared()
			intSqNeg := multNeg1.integrateSquared()
			if intSqNeg != intSq {
				t.Errorf("seed %v quantile %v incorrect intSqNeg %v expected %v", seed, q, intSqNeg, intSq)
			}
			intSqMult0 := mult0.integrateSquared()
			if intSqMult0 != 0 {
				t.Errorf("seed %v quantile %v incorrect intSqMult0 %v expected 0", seed, q, intSqMult0)
			}
			fixed := q.fixMalformed()
			intSqFixed := fixed.integrateSquared()
			// This seems like it should run into floating point errors, but it hasn't
			// yet, so yay?
			if math.Abs(intSqFixed-intSq) > delta {
				t.Errorf("seed %v quantile %v incorrect intSqFixed %v expected %v", seed, q, intSqFixed, intSq)
			}
		})
	}
}

// TestQuantileInvalid tests the quantile.invalid() method.
func TestQuantileInvalid(t *testing.T) {
	testCases := []struct {
		q       quantile
		invalid bool
	}{
		// Valid quantiles.
		{
			q: zeroQuantile,
		},
		{
			q: quantile{{0, 100}, {1, 200}},
		},
		{
			q: quantile{{0, 100}, {0, 200}, {0, 200}, {0.5, 300}, {0.5, 300}, {0.5, 400}, {1, 400}},
		},
		{
			q: quantile{{0, -811}, {0.125, -811}, {0.125, -813}, {0.25, -813}, {0.25, -815}, {1, -821}, {1, -721}},
		},
		{
			q: quantile{{0, 102}, {0.125, 102.25}, {0.25, 202.5}, {0.375, 302.75}, {0.375, 202.75}, {0.5, 103}, {0.625, 103.25}, {0.75, 3.5}, {0.875, 3.75}, {0.875, 103.75}, {0.875, 403.75}, {1, 104}},
		},
		// Invalid quantiles with NaN.
		{
			q:       quantile{{0, 100}, {1, math.NaN()}},
			invalid: true,
		},
		{
			q:       quantile{{0, math.NaN()}, {1, 200}},
			invalid: true,
		},
		{
			q:       quantile{{0, 100}, {0, 200}, {0, 200}, {0.5, 300}, {0.5, 300}, {0.5, 400}, {1, math.NaN()}},
			invalid: true,
		},
		{
			q:       quantile{{0, 100}, {0, 200}, {0, 200}, {0.5, 300}, {0.5, 300}, {0.5, math.NaN()}, {1, math.NaN()}},
			invalid: true,
		},
		{
			q:       quantile{{0, math.NaN()}, {0.125, -811}, {0.125, -813}, {0.25, -813}, {0.25, -815}, {1, -821}, {1, -721}},
			invalid: true,
		},
		{
			q:       quantile{{0, math.NaN()}, {0.125, math.NaN()}, {0.125, -813}, {0.25, -813}, {0.25, -815}, {1, -821}, {1, -721}},
			invalid: true,
		},
		{
			q:       quantile{{0, 102}, {0.125, 102.25}, {0.25, math.NaN()}, {0.375, 302.75}, {0.375, 202.75}, {0.5, 103}, {0.625, 103.25}, {0.75, 3.5}, {0.875, 3.75}, {0.875, 103.75}, {0.875, 403.75}, {1, 104}},
			invalid: true,
		},
		{
			q:       quantile{{0, 102}, {0.125, 102.25}, {0.25, math.NaN()}, {0.375, 302.75}, {0.375, math.NaN()}, {0.5, 103}, {0.625, math.NaN()}, {0.75, 3.5}, {0.875, 3.75}, {0.875, math.NaN()}, {0.875, 403.75}, {1, 104}},
			invalid: true,
		},
		// Invalid quantiles with Inf.
		{
			q:       quantile{{0, 100}, {1, math.Inf(1)}},
			invalid: true,
		},
		{
			q:       quantile{{0, math.Inf(1)}, {1, 200}},
			invalid: true,
		},
		{
			q:       quantile{{0, 100}, {0, 200}, {0, 200}, {0.5, 300}, {0.5, 300}, {0.5, 400}, {1, math.Inf(1)}},
			invalid: true,
		},
		{
			q:       quantile{{0, 100}, {0, 200}, {0, 200}, {0.5, 300}, {0.5, 300}, {0.5, math.Inf(1)}, {1, math.Inf(1)}},
			invalid: true,
		},
		{
			q:       quantile{{0, 100}, {1, math.Inf(-1)}},
			invalid: true,
		},
		{
			q:       quantile{{0, math.Inf(-1)}, {1, 200}},
			invalid: true,
		},
		{
			q:       quantile{{0, 100}, {0, 200}, {0, 200}, {0.5, 300}, {0.5, 300}, {0.5, 400}, {1, math.Inf(-1)}},
			invalid: true,
		},
		{
			q:       quantile{{0, 100}, {0, 200}, {0, 200}, {0.5, 300}, {0.5, 300}, {0.5, math.Inf(-1)}, {1, math.Inf(-1)}},
			invalid: true,
		},
		// Invalid quantiles with NaN and Inf.
		{
			q:       quantile{{0, math.Inf(-1)}, {0.125, 102.25}, {0.25, math.NaN()}, {0.375, 302.75}, {0.375, math.Inf(1)}, {0.5, 103}, {0.625, 103.25}, {0.75, math.NaN()}, {0.875, math.Inf(1)}, {0.875, math.Inf(-1)}, {0.875, 403.75}, {1, 104}},
			invalid: true,
		},
	}
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			invalid := tc.q.isInvalid()
			if invalid != tc.invalid {
				t.Errorf("test case %d expected invalid to be %v, but was %v", i, tc.invalid, invalid)
			}
		})
	}
}
