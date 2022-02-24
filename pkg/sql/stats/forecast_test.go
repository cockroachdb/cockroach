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
	"strconv"
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
)

func TestForecastTableStatistics(t *testing.T) {
	testCases := []struct {
	}{}
	for i := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
		})
	}
}

func TestForecastColumnStatistics(t *testing.T) {
	testCases := []struct {
	}{}
	for i := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
		})
	}
}

func TestScalarLinearRegression(t *testing.T) {
	testCases := []struct {
		x, y       []float64
		xn, yn, r2 float64
	}{
		{
			x:  []float64{0},
			y:  []float64{0},
			xn: 1,
			yn: 0,
			r2: 1,
		},
		{
			x:  []float64{0, 0},
			y:  []float64{0, 1},
			xn: 1,
			yn: 0.5,
			r2: 0,
		},
		{
			x:  []float64{0, 1},
			y:  []float64{0, 0},
			xn: 2,
			yn: 0,
			r2: 1,
		},
		{
			x:  []float64{0, 1},
			y:  []float64{0, 1},
			xn: 2,
			yn: 2,
			r2: 1,
		},
		{
			x:  []float64{0, 1},
			y:  []float64{1, 0},
			xn: 2,
			yn: -1,
			r2: 1,
		},
		{
			x:  []float64{0, 0, 0},
			y:  []float64{0, 1, 2},
			xn: 1,
			yn: 1,
			r2: 0,
		},
		{
			x:  []float64{0, 1, 2},
			y:  []float64{0, 0, 0},
			xn: 3,
			yn: 0,
			r2: 1,
		},
		{
			x:  []float64{0, 1, 2},
			y:  []float64{0, 1, 2},
			xn: 3,
			yn: 3,
			r2: 1,
		},
		{
			x:  []float64{0, 1, 2},
			y:  []float64{2, 1, 0},
			xn: 3,
			yn: -1,
			r2: 1,
		},
		{
			x:  []float64{0, 1, 2, 3},
			y:  []float64{10, 30, 20, 40},
			xn: 4,
			yn: 45,
			r2: 0.64,
		},
		{
			x:  []float64{0, 1, 1, 3, -1, -2, 4, 10},
			y:  []float64{3, 7, 8, 9, 0, -4, 12, 34},
			xn: 5,
			yn: 17.625,
			r2: 0.9741577594371533,
		},
	}
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			yn, r2 := scalarLinearRegression(tc.x, tc.y, tc.xn)
			if yn != tc.yn {
				t.Errorf("test case %d incorrect yn %v expected %v", i, yn, tc.yn)
			}
			if r2 != tc.r2 {
				t.Errorf("test case %d incorrect r2 %v expected %v", i, r2, tc.r2)
			}
		})
	}
}

func TestQuantileLinearRegression(t *testing.T) {
	testCases := []struct {
		x  []float64
		y  []quantile
		xn float64
		yn quantile
		r2 float64
	}{
		{
			x:  []float64{0},
			y:  []quantile{zeroQuantile},
			xn: 1,
			yn: zeroQuantile,
			r2: 1,
		},
		{
			x:  []float64{9},
			y:  []quantile{{{0, 9}, {0.5, 99}, {1, 999}}},
			xn: 9,
			yn: quantile{{0, 9}, {0.5, 99}, {1, 999}},
			r2: 1,
		},
		{
			x:  []float64{0, 0},
			y:  []quantile{zeroQuantile, {{0, 1}, {1, 1}}},
			xn: 1,
			yn: quantile{{0, 0.5}, {1, 0.5}},
			r2: 0,
		},
		{
			x:  []float64{0, 1},
			y:  []quantile{zeroQuantile, zeroQuantile},
			xn: 2,
			yn: zeroQuantile,
			r2: 1,
		},
		{
			x:  []float64{0, 1},
			y:  []quantile{zeroQuantile, {{0, 1}, {1, 1}}},
			xn: 2,
			yn: quantile{{0, 2}, {1, 2}},
			r2: 1,
		},
		{
			x:  []float64{0, 0, 0},
			y:  []quantile{zeroQuantile, {{0, 1}, {1, 1}}, {{0, 2}, {1, 2}}},
			xn: 1,
			yn: quantile{{0, 1}, {1, 1}},
			r2: 0,
		},
		{
			x:  []float64{0, 1, 2},
			y:  []quantile{zeroQuantile, zeroQuantile, zeroQuantile},
			xn: 3,
			yn: zeroQuantile,
			r2: 1,
		},
		{
			x:  []float64{0, 1, 2},
			y:  []quantile{zeroQuantile, {{0, 1}, {1, 1}}, {{0, 2}, {1, 2}}},
			xn: 3,
			yn: quantile{{0, 3}, {1, 3}},
			r2: 1,
		},
		{
			x:  []float64{0, 1, 2},
			y:  []quantile{{{0, 2}, {1, 2}}, {{0, 1}, {1, 1}}, zeroQuantile},
			xn: 3,
			yn: quantile{{0, -1}, {1, -1}},
			r2: 1,
		},
		{
			x: []float64{0, 1, 2},
			y: []quantile{
				{{0, 0}, {1, 0}},
				{{0, 0}, {1, 10}},
				{{0, 0}, {1, 20}},
			},
			xn: 3,
			yn: quantile{{0, 0}, {1, 30}},
			r2: 1,
		},
		{
			x: []float64{0, 1, 2},
			y: []quantile{
				{{0, 30}, {0.5, 40}, {1, 50}},
				{{0, 20}, {0.5, 30}, {1, 60}},
				{{0, 25}, {0.5, 50}, {1, 70}},
			},
			xn: 1,
			yn: quantile{{0, 25}, {0.5, 40}, {1, 60}},
			r2: 0.484375,
		},
		// A case that produces a malformed quantile which needs fixing.
		{
			x: []float64{0, 1, 2, 3},
			y: []quantile{
				{{0, 400}, {0.5, 419}, {1, 490}},
				{{0, 402}, {0.5, 415}, {1, 490}},
				{{0, 404}, {0.5, 411}, {1, 490}},
				{{0, 406}, {0.5, 407}, {1, 490}},
			},
			xn: 4,
			yn: quantile{{0, 403}, {0.5287356321839081, 408}, {1, 490}},
			r2: 1,
		},
		// Quantiles with horizontal and vertical sections.
		{
			x: []float64{0, 1, 2, 3, 4},
			y: []quantile{
				{{0, 500}, {0, 510}, {0.25, 510}, {0.5, 520}, {0.5, 530}, {0.75, 540}, {0.75, 550}, {1, 550}},
				{{0, 600}, {0.25, 600}, {0.25, 610}, {0.5, 610}, {0.5, 620}, {0.75, 620}, {1, 630}, {1, 640}},
				{{0, 700}, {0.25, 710}, {0.5, 710}, {0.75, 720}, {1, 720}},
				{{0, 800}, {0, 810}, {0.25, 810}, {0.25, 820}, {0.5, 820}, {0.5, 830}, {0.75, 830}, {0.75, 840}, {1, 850}, {1, 860}},
				{{0, 900}, {0.25, 910}, {0.5, 920}, {0.75, 930}, {0.75, 940}, {1, 940}, {1, 950}, {1, 960}},
			},
			xn: -1,
			yn: quantile{{0, 400}, {0, 405}, {0.25, 407}, {0.25, 409}, {0.5, 413}, {0.5, 425}, {0.75, 431}, {0.75, 434}, {1, 438}},
			r2: 0.9973157151167622,
		},
		// A case with varying numbers of points.
		{
			x: []float64{2, 1, 0, -1},
			y: []quantile{
				{{0, 200}, {1, 300}},
				{{0, 200}, {0.5, 275}, {1, 300}},
				{{0, 200}, {0.25, 275}, {0.5, 290}, {0.75, 300}, {1, 300}},
				{{0, 200}, {0.125, 275}, {0.25, 290}, {0.375, 295}, {0.5, 297}, {0.625, 299}, {0.75, 300}, {0.875, 300}, {1, 300}},
			},
			xn: 3,
			yn: quantile{{0, 184.375}, {0.24, 198.75}, {0.25806451612903225, 200}, {0.375, 218.125}, {0.5, 239}, {0.625, 253.62500000000003}, {0.75, 268.75}, {0.875, 284.375}, {1, 300}},
			r2: 0.9507594371452102,
		},
	}
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			yn, r2 := quantileLinearRegression(tc.x, tc.y, tc.xn)
			if !quantilesEqual(yn, tc.yn) {
				t.Errorf("test case %d incorrect yn %v expected %v", i, yn, tc.yn)
			}
			if r2 != tc.r2 {
				t.Errorf("test case %d incorrect r2 %v expected %v", i, r2, tc.r2)
			}
		})
	}
}

// Test conversions from quantile to histogram and back.
func TestQuantileToHistogram(t *testing.T) {
	// We use all floats here. TestQuantileValue and TestHistogramValue test
	// conversions to other datatypes.
	testCases := []struct {
		q             quantile
		rowCount      float64
		distinctCount float64
		err           bool
	}{
		{
			q:             zeroQuantile,
			rowCount:      0,
			distinctCount: 0,
		},
		{
			q:             zeroQuantile,
			rowCount:      1,
			distinctCount: 1,
		},
		{
			q:             zeroQuantile,
			rowCount:      2,
			distinctCount: 1,
		},
		{
			q:             quantile{{0, 0}, {0, 100}, {1, 100}},
			rowCount:      1,
			distinctCount: 1,
		},
		{
			q:             quantile{{0, 0}, {0.5, 100}, {1, 100}},
			rowCount:      2,
			distinctCount: 2,
		},
		{
			q:             quantile{{0, 0}, {0.9, 100}, {1, 100}},
			rowCount:      10,
			distinctCount: 10,
		},
		{
			q:             quantile{{0, 100}, {0.25, 100}, {0.75, 200}, {1, 200}},
			rowCount:      16,
			distinctCount: 10,
		},
		{
			q:             quantile{{0, 100}, {0.25, 100}, {0.5, 200}, {0.75, 200}, {0.75, 300}, {1, 300}},
			rowCount:      16,
			distinctCount: 7,
		},
		{
			q:             quantile{{0, 500}, {0.125, 500}, {0.25, 600}, {0.5, 600}, {0.75, 800}, {1, 800}},
			rowCount:      16,
			distinctCount: 9,
		},
		{
			q:             quantile{{0, 300}, {0, 310}, {0.125, 310}, {0.125, 320}, {0.25, 320}, {0.25, 330}, {0.5, 330}, {0.5, 340}, {0.625, 340}, {0.625, 350}, {0.75, 350}, {0.75, 360}, {0.875, 360}, {0.875, 370}, {1, 370}},
			rowCount:      32,
			distinctCount: 7,
		},
	}
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			h, err := tc.q.toHistogramData(types.Float, tc.rowCount, tc.distinctCount)
			if err != nil {
				if !tc.err {
					t.Errorf("test case %d unexpected toHistogramData err: %v", i, err)
				}
				return
			}
			if tc.err {
				t.Errorf("test case %d expected toHistogramData err", i)
				return
			}
			q, err := makeQuantile(&h, tc.rowCount)
			if err != nil {
				t.Errorf("test case %d unexpected makeQuantile err: %v", i, err)
				return
			}
			if !quantilesEqual(q, tc.q) {
				t.Errorf("test case %d incorrect quantile %v hist %v expected %v", i, q, h, tc.q)
			}
		})
	}
}

// Test conversions from quantile value to datum and back. TestQuantileValue
// covers similar ground, so here we just focus on cases that overflow or
// underflow and have to clamp.
func TestHistogramValue(t *testing.T) {
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
		// Decimal cases.
		{
			typ: types.MakeDecimal(1, 0),
			val: -math.MaxFloat64,
			dat: makeDDecimal("-9"),
			res: -9,
		},
		{
			typ: types.MakeDecimal(1, 0),
			val: math.MaxFloat64,
			dat: makeDDecimal("9"),
			res: 9,
		},
		{
			typ: types.MakeDecimal(10, 4),
			val: -math.MaxFloat64,
			dat: makeDDecimal("-999999.9999"),
			res: -999999.9999,
		},
		{
			typ: types.MakeDecimal(10, 4),
			val: math.MaxFloat64,
			dat: makeDDecimal("999999.9999"),
			res: 999999.9999,
		},
		// Date cases.
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
		// Time cases. For these we modulo instead of clamp on overflow when
		// converting to datums.
		{
			typ: types.Time,
			val: -23 * 60 * 60 * 1000 * 1000,
			dat: tree.MakeDTime(timeofday.FromInt(1 * 60 * 60 * 1000 * 1000)),
			res: 1 * 60 * 60 * 1000 * 1000,
		},
		{
			typ: types.Time,
			val: 25 * 60 * 60 * 1000 * 1000,
			dat: tree.MakeDTime(timeofday.FromInt(1 * 60 * 60 * 1000 * 1000)),
			res: 1 * 60 * 60 * 1000 * 1000,
		},
		// TODO(michae2): timestamp cases. Clamping for timestamps is broken.
		// TODO(michae2): interval cases. Clamping for intervals is broken.
	}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	var a tree.DatumAlloc
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			d, err := histogramValue(tc.typ, tc.val)
			if err != nil {
				if !tc.err {
					t.Errorf("test case %d (%v) unexpected histogramValue err: %v", i, tc.typ.Name(), err)
				}
				return
			}
			if tc.err {
				t.Errorf("test case %d (%v) expected histogramValue err", i, tc.typ.Name())
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
			encoded, err := keyside.Encode(nil, d, encoding.Ascending)
			if err != nil {
				t.Errorf("test case %d (%v) could not encode datum: %v", i, tc.typ.Name(), d)
				return
			}
			res, err := quantileValue(&a, tc.typ, encoded)
			if err != nil {
				t.Errorf("test case %d (%v) unexpected quantileValue err: %v", i, tc.typ.Name(), err)
				return
			}
			if res != tc.res {
				t.Errorf("test case %d (%v) incorrect val %v expected %v", i, tc.typ.Name(), res, tc.res)
				return
			}
		})
	}
}

// Test conversions from datum to quantile value and back.
func TestQuantileValue(t *testing.T) {
	testCases := []struct {
		typ *types.T
		dat tree.Datum
		val float64
		err bool
		res tree.Datum
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
		// Decimal cases.
		{
			typ: types.Decimal,
			dat: tree.DZeroDecimal,
			val: 0,
		},
		{
			typ: types.Decimal,
			dat: makeDDecimal("42"),
			val: 42,
		},
		{
			typ: types.Decimal,
			dat: tree.DNaNDecimal,
			err: true,
		},
		{
			typ: types.Decimal,
			dat: tree.DNegInfDecimal,
			err: true,
		},
		{
			typ: types.Decimal,
			dat: tree.DPosInfDecimal,
			err: true,
		},
		{
			typ: types.Decimal,
			dat: makeDDecimal("-1e1000"),
			err: true,
		},
		{
			typ: types.Decimal,
			dat: makeDDecimal("1e1000"), // What's more than a Googol? A Roachol!
			err: true,
		},
		// Date cases.
		{
			typ: types.Date,
			dat: tree.NewDDate(pgdate.MakeDateFromPGEpochClamp(0)),
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
		// Time cases.
		{
			typ: types.Time,
			dat: tree.DTimeMin,
			val: 0,
		},
		{
			typ: types.Time,
			dat: tree.DTimeMax,
			val: 24 * 60 * 60 * 1000 * 1000,
			// When we convert back the 24:00:00 becomes 00:00:00, which is fine.
			res: tree.DTimeMin,
		},
		{
			typ: types.Time,
			dat: tree.MakeDTime(timeofday.FromInt(42)),
			val: 42,
		},
		{
			typ: types.TimeTZ,
			dat: tree.DZeroTimeTZ,
			val: 0,
		},
		{
			typ: types.TimeTZ,
			dat: tree.DMinTimeTZ,
			val: float64(tree.DMinTimeTZ.ToTime().UnixMicro()),
			// When we convert back the result is in UTC, which is fine.
			res: tree.NewDTimeTZFromTime(tree.DMinTimeTZ.ToTime().UTC()),
		},
		{
			typ: types.TimeTZ,
			dat: tree.DMaxTimeTZ,
			val: float64(tree.DMaxTimeTZ.ToTime().UnixMicro()),
			// When we convert back the result is in UTC, which is fine.
			res: tree.NewDTimeTZFromTime(tree.DMaxTimeTZ.ToTime().UTC()),
		},
		// Timestamp cases.
		{
			typ: types.Timestamp,
			dat: tree.DZeroTimestamp,
			val: float64(tree.DZeroTimestamp.UnixMicro()),
		},
		{
			typ: types.Timestamp,
			dat: &tree.DTimestamp{pgdate.TimeNegativeInfinity},
			err: true,
		},
		{
			typ: types.Timestamp,
			dat: &tree.DTimestamp{pgdate.TimeInfinity},
			err: true,
		},
		{
			typ: types.TimestampTZ,
			dat: tree.DZeroTimestampTZ,
			val: float64(tree.DZeroTimestampTZ.UnixMicro()),
		},
		{
			typ: types.TimestampTZ,
			dat: &tree.DTimestamp{pgdate.TimeNegativeInfinity},
			err: true,
		},
		{
			typ: types.TimestampTZ,
			dat: &tree.DTimestamp{pgdate.TimeInfinity},
			err: true,
		},
		// Interval cases.
		{
			typ: types.Interval,
			dat: tree.DZeroInterval,
			val: 0,
		},
		// Hmm... tree.DMinInterval and tree.DMaxInterval can't be encoded. Skip
		// those for now.
		{
			typ: types.Interval,
			dat: tree.NewDInterval(duration.MakeDuration(-42*1000000000, 0, 0), types.DefaultIntervalTypeMetadata),
			val: -42,
		},
		{
			typ: types.Interval,
			dat: tree.NewDInterval(duration.MakeDuration(42*1000000000, 0, 0), types.DefaultIntervalTypeMetadata),
			val: 42,
		},
	}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	var a tree.DatumAlloc
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			encoded, err := keyside.Encode(nil, tc.dat, encoding.Ascending)
			if err != nil {
				t.Errorf("test case %d (%v) could not encode datum: %v", i, tc.typ.Name(), tc.dat)
				return
			}
			val, err := quantileValue(&a, tc.typ, encoded)
			if err != nil {
				if !tc.err {
					t.Errorf("test case %d (%v) unexpected quantileValue err: %v", i, tc.typ.Name(), err)
				}
				return
			}
			if tc.err {
				t.Errorf("test case %d (%v) expected quantileValue err", i, tc.typ.Name())
				return
			}
			if val != tc.val {
				t.Errorf("test case %d (%v) incorrect val %v expected %v", i, tc.typ.Name(), val, tc.val)
				return
			}
			res, err := histogramValue(tc.typ, val)
			if err != nil {
				t.Errorf("test case %d (%v) unexpected histogramValue err: %v", i, tc.typ.Name(), err)
				return
			}
			if tc.res == nil {
				tc.res = tc.dat
			}
			cmp, err := res.CompareError(evalCtx, tc.res)
			if err != nil {
				t.Errorf("test case %d (%v) unexpected CompareError err: %v", i, tc.typ.Name(), err)
				return
			}
			if cmp != 0 {
				t.Errorf("test case %d (%v) incorrect datum %v expected %v", i, tc.typ.Name(), res, tc.res)
			}
		})
	}
}

// Test basic operations on quantile functions.
func TestQuantileOps(t *testing.T) {
	var r = quantile{{0, 2}, {0.5, 3}, {1, 4}}
	var s = quantile{{0, 11}, {0.125, 11}, {0.125, 13}, {0.25, 13}, {0.25, 15}, {1, 21}}
	testCases := []struct {
		q     quantile
		addR  quantile
		addS  quantile
		subR  quantile
		subS  quantile
		scale quantile
		intSq float64
		fixed quantile
	}{
		// The zero quantile.
		{
			q:     zeroQuantile,
			addR:  r,
			addS:  s,
			subR:  r.scale(-1),
			subS:  s.scale(-1),
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
			fixed: quantile{{0, 20}, {1, 30}},
		},
		{
			q:     quantile{{0, 100}, {0, 200}, {0, 200}, {0.5, 300}, {0.5, 300}, {0.5, 400}, {1, 400}},
			addR:  quantile{{0, 102}, {0, 202}, {0, 202}, {0.5, 303}, {0.5, 303}, {0.5, 403}, {1, 404}},
			addS:  quantile{{0, 111}, {0, 211}, {0, 211}, {0.125, 236}, {0.125, 238}, {0.25, 263}, {0.25, 265}, {0.5, 317}, {0.5, 317}, {0.5, 417}, {1, 421}},
			subR:  quantile{{0, 98}, {0, 198}, {0, 198}, {0.5, 297}, {0.5, 297}, {0.5, 397}, {1, 396}},
			subS:  quantile{{0, 89}, {0, 189}, {0, 189}, {0.125, 214}, {0.125, 212}, {0.25, 237}, {0.25, 235}, {0.5, 283}, {0.5, 283}, {0.5, 383}, {1, 379}},
			intSq: 111666.66666666667,
			fixed: quantile{{0, 100}, {0, 200}, {0.5, 300}, {0.5, 400}, {1, 400}},
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
			if !quantilesEqual(addR, tc.addR) {
				t.Errorf("test case %d incorrect addR %v expected %v", i, addR, tc.addR)
			}
			addS := tc.q.add(s)
			if !quantilesEqual(addS, tc.addS) {
				t.Errorf("test case %d incorrect addS %v expected %v", i, addS, tc.addS)
			}
			addZ := tc.q.add(zeroQuantile)
			if !quantilesEqual(addZ, tc.q) {
				t.Errorf("test case %d incorrect addZ %v expected %v", i, addZ, tc.q)
			}
			subQ := tc.q.sub(tc.q)
			subR := tc.q.sub(r)
			if !quantilesEqual(subR, tc.subR) {
				t.Errorf("test case %d incorrect subR %v expected %v", i, subR, tc.subR)
			}
			subS := tc.q.sub(s)
			if !quantilesEqual(subS, tc.subS) {
				t.Errorf("test case %d incorrect subS %v expected %v", i, subS, tc.subS)
			}
			subZ := tc.q.sub(zeroQuantile)
			if !quantilesEqual(subZ, tc.q) {
				t.Errorf("test case %d incorrect subZ %v expected %v", i, subZ, tc.q)
			}
			zSub := zeroQuantile.sub(tc.q)
			scaleNeg1 := tc.q.scale(-1)
			if !quantilesEqual(scaleNeg1, zSub) {
				t.Errorf("test case %d incorrect scaleNeg1 %v expected %v", i, scaleNeg1, zSub)
			}
			scale0 := tc.q.scale(0)
			if !quantilesEqual(scale0, subQ) {
				t.Errorf("test case %d incorrect scale0 %v expected %v", i, scale0, subQ)
			}
			scale1 := tc.q.scale(1)
			if !quantilesEqual(scale1, tc.q) {
				t.Errorf("test case %d incorrect scale1 %v expected %v", i, scale1, tc.q)
			}
			scale2 := tc.q.scale(2)
			if !quantilesEqual(scale2, addQ) {
				t.Errorf("test case %d incorrect scale2 %v expected %v", i, scale2, addQ)
			}
			intSq := tc.q.integrateSquared()
			if intSq != tc.intSq {
				t.Errorf("test case %d incorrect intSq %v expected %v", i, intSq, tc.intSq)
			}
			intSqNeg := scaleNeg1.integrateSquared()
			if intSqNeg != intSq {
				t.Errorf("test case %d incorrect intSqNeg %v expected %v", i, intSqNeg, intSq)
			}
			intSqScale0 := scale0.integrateSquared()
			if intSqScale0 != 0 {
				t.Errorf("test case %d incorrect intSqScale0 %v expected 0", i, intSqScale0)
			}
			fixed := tc.q.fix()
			if !quantilesEqual(fixed, tc.fixed) {
				t.Errorf("test case %d incorrect fixed %v expected %v", i, fixed, tc.fixed)
			}
			intSqFixed := fixed.integrateSquared()
			// This seems like it should run into floating point errors, but it hasn't
			// yet, so yay?
			if intSqFixed != intSq {
				t.Errorf("test case %d incorrect intSqFixed %v expected %v", i, intSqFixed, intSq)
			}
		})
	}
}

// Check whether all points in q and r are exactly the same. Note that this does
// not account for floating point error, out-of-order points, or quantiles that
// are "semantically" equivalent (e.g. one quantile has a point repeated while
// the other does not). Because of these omissions this function is only used by
// tests.
func quantilesEqual(q, r quantile) bool {
	if len(q) != len(r) {
		return false
	}
	for i := range q {
		if q[i] != r[i] {
			return false
		}
	}
	return true
}

func makeDDecimal(s string) *tree.DDecimal {
	d, _, err := apd.NewFromString(s)
	if err != nil {
		panic(err)
	}
	return &tree.DDecimal{*d}
}
