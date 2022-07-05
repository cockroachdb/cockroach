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
	"math"
	"reflect"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
)

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

// Test conversions from histogram to quantile.
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
			q, err := makeQuantile(tc.hist.toHistogram(), tc.rows)
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
			if !reflect.DeepEqual(q, tc.qfun) {
				t.Errorf("test case %d incorrect quantile %v expected %v", i, q, tc.qfun)
			}
		})
	}
}

// Test conversions from quantile to histogram.
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
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			h, err := tc.qfun.toHistogram(types.Float, tc.rows)
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
			h2 := tc.hist.toHistogram()
			if !reflect.DeepEqual(h, h2) {
				t.Errorf("test case %d incorrect histogram %v expected %v", i, h, h2)
			}
		})
	}
}

// Test conversions from datum to quantile value and back.
func TestToQuantileValue(t *testing.T) {
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

// Test conversions from quantile value to datum and back. TestToQuantileValue
// covers similar ground, so here we focus on cases that overflow or underflow
// and have to clamp.
func TestFromQuantileValue(t *testing.T) {
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
