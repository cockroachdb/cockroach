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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
)

// Test conversions from datum to quantile value and back.
func TestToQuantileValue(t *testing.T) {
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
			dat: &tree.DTimestamp{Time: QuantileMinTimestamp},
			val: QuantileMinTimestampSec,
		},
		{
			typ: types.Timestamp,
			dat: &tree.DTimestamp{Time: QuantileMaxTimestamp},
			val: QuantileMaxTimestampSec,
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
			dat: &tree.DTimestampTZ{Time: QuantileMinTimestamp},
			val: QuantileMinTimestampSec,
		},
		{
			typ: types.TimestampTZ,
			dat: &tree.DTimestampTZ{Time: QuantileMaxTimestamp},
			val: QuantileMaxTimestampSec,
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
			val, err := ToQuantileValue(tc.dat)
			if err != nil {
				if !tc.err {
					t.Errorf("test case %d (%v) unexpected ToQuantileValue err: %v", i, tc.typ.Name(), err)
				}
				return
			}
			if tc.err {
				t.Errorf("test case %d (%v) expected ToQuantileValue err", i, tc.typ.Name())
				return
			}
			if val != tc.val {
				t.Errorf("test case %d (%v) incorrect val %v expected %v", i, tc.typ.Name(), val, tc.val)
				return
			}
			// Check that we can make the round trip.
			res, err := FromQuantileValue(tc.typ, val)
			if err != nil {
				t.Errorf("test case %d (%v) unexpected FromQuantileValue err: %v", i, tc.typ.Name(), err)
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
			val: -math.MaxFloat64,
			dat: &tree.DTimestamp{Time: QuantileMinTimestamp},
			res: QuantileMinTimestampSec,
		},
		{
			typ: types.Timestamp,
			val: math.MaxFloat64,
			dat: &tree.DTimestamp{Time: QuantileMaxTimestamp},
			res: QuantileMaxTimestampSec,
		},
		{
			typ: types.TimestampTZ,
			val: -math.MaxFloat64,
			dat: &tree.DTimestampTZ{Time: QuantileMinTimestamp},
			res: QuantileMinTimestampSec,
		},
		{
			typ: types.TimestampTZ,
			val: math.MaxFloat64,
			dat: &tree.DTimestampTZ{Time: QuantileMaxTimestamp},
			res: QuantileMaxTimestampSec,
		},
	}
	evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			d, err := FromQuantileValue(tc.typ, tc.val)
			if err != nil {
				if !tc.err {
					t.Errorf("test case %d (%v) unexpected FromQuantileValue err: %v", i, tc.typ.Name(), err)
				}
				return
			}
			if tc.err {
				t.Errorf("test case %d (%v) expected FromQuantileValue err", i, tc.typ.Name())
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
			res, err := ToQuantileValue(d)
			if err != nil {
				t.Errorf("test case %d (%v) unexpected ToQuantileValue err: %v", i, tc.typ.Name(), err)
				return
			}
			if res != tc.res {
				t.Errorf("test case %d (%v) incorrect val %v expected %v", i, tc.typ.Name(), res, tc.res)
				return
			}
		})
	}
}
