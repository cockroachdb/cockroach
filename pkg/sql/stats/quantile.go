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
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/errors"
)

// We only create quantiles for types that make sense converted to float64.
// TODO(michae2): Add support for DECIMAL, TIME, TIMETZ, and INTERVAL.
func CanMakeQuantile(colType *types.T) bool {
	if colType.UserDefined() {
		return false
	}
	switch colType.Family() {
	case types.IntFamily,
		types.FloatFamily,
		types.DateFamily,
		types.TimestampFamily,
		types.TimestampTZFamily:
		return true
	default:
		return false
	}
}

// ToQuantileValue converts from a datum to a float suitable for use in a quantile
// function. It differs from eval.PerformCast in a few ways:
// 1. It supports conversions that are not legal casts (e.g. DATE to FLOAT).
// 2. It errors on NaN and infinite values because they will break our model.
// FromQuantileValue is the inverse of this function, and together they should
// support round-trip conversions.
// TODO(michae2): Add support for DECIMAL, TIME, TIMETZ, and INTERVAL.
func ToQuantileValue(d tree.Datum) (float64, error) {
	switch v := d.(type) {
	case *tree.DInt:
		return float64(*v), nil
	case *tree.DFloat:
		if math.IsNaN(float64(*v)) || math.IsInf(float64(*v), 0) {
			return 0, tree.ErrFloatOutOfRange
		}
		return float64(*v), nil
	case *tree.DDate:
		if !v.IsFinite() {
			return 0, tree.ErrFloatOutOfRange
		}
		// We use PG epoch instead of Unix epoch to simplify clamping when
		// converting back.
		return float64(v.PGEpochDays()), nil
	case *tree.DTimestamp:
		if v.Equal(pgdate.TimeInfinity) || v.Equal(pgdate.TimeNegativeInfinity) {
			return 0, tree.ErrFloatOutOfRange
		}
		return float64(v.Unix()) + float64(v.Nanosecond())*1e-9, nil
	case *tree.DTimestampTZ:
		if v.Equal(pgdate.TimeInfinity) || v.Equal(pgdate.TimeNegativeInfinity) {
			return 0, tree.ErrFloatOutOfRange
		}
		return float64(v.Unix()) + float64(v.Nanosecond())*1e-9, nil
	default:
		return 0, errors.Errorf("cannot make quantile value from %v", d)
	}
}

var (
	// QuantileMinTimestamp is an alternative minimum finite DTimestamp value to
	// avoid the problems around TimeNegativeInfinity, see #41564.
	QuantileMinTimestamp    = tree.MinSupportedTime.Add(time.Second)
	QuantileMinTimestampSec = float64(QuantileMinTimestamp.Unix())
	// QuantileMaxTimestamp is an alternative maximum finite DTimestamp value to
	// avoid the problems around TimeInfinity, see #41564.
	QuantileMaxTimestamp    = tree.MaxSupportedTime.Add(-1 * time.Second).Truncate(time.Second)
	QuantileMaxTimestampSec = float64(QuantileMaxTimestamp.Unix())
)

// FromQuantileValue converts from a quantile value back to a datum suitable for
// use in a histogram. It is the inverse of ToQuantileValue. It differs from
// eval.PerformCast in a few ways:
// 1. It supports conversions that are not legal casts (e.g. FLOAT to DATE).
// 2. It errors on NaN and infinite values because they indicate a problem with
//    the regression model rather than valid values.
// 3. On overflow or underflow it clamps to maximum or minimum finite values
//    rather than failing the conversion (and thus the entire histogram).
// TODO(michae2): Add support for DECIMAL, TIME, TIMETZ, and INTERVAL.
func FromQuantileValue(colType *types.T, val float64) (tree.Datum, error) {
	if math.IsNaN(val) || math.IsInf(val, 0) {
		return nil, tree.ErrFloatOutOfRange
	}
	switch colType.Family() {
	case types.IntFamily:
		i := math.Round(val)
		// Clamp instead of truncating.
		switch colType.Width() {
		case 16:
			if i <= math.MinInt16 {
				return tree.NewDInt(tree.DInt(math.MinInt16)), nil
			}
			if i >= math.MaxInt16 {
				return tree.NewDInt(tree.DInt(math.MaxInt16)), nil
			}
		case 32:
			if i <= math.MinInt32 {
				return tree.NewDInt(tree.DInt(math.MinInt32)), nil
			}
			if i >= math.MaxInt16 {
				return tree.NewDInt(tree.DInt(math.MaxInt32)), nil
			}
		default:
			if i <= math.MinInt64 {
				return tree.NewDInt(tree.DInt(math.MinInt64)), nil
			}
			if i >= math.MaxInt64 {
				return tree.NewDInt(tree.DInt(math.MaxInt64)), nil
			}
		}
		return tree.NewDInt(tree.DInt(i)), nil
	case types.FloatFamily:
		switch colType.Width() {
		case 32:
			if val <= -math.MaxFloat32 {
				val = -math.MaxFloat32
			} else if val >= math.MaxFloat32 {
				val = math.MaxFloat32
			} else {
				val = float64(float32(val))
			}
		}
		return tree.NewDFloat(tree.DFloat(val)), nil
	case types.DateFamily:
		days := math.Round(val)
		// First clamp to int32.
		if days <= math.MinInt32 {
			days = math.MinInt32
		} else if days >= math.MaxInt32 {
			days = math.MaxInt32
		}
		// Then clamp to pgdate.Date.
		return tree.NewDDate(pgdate.MakeDateFromPGEpochClampFinite(int32(days))), nil
	case types.TimestampFamily:
		sec, frac := math.Modf(val)
		var t time.Time
		// Clamp to (our alternative finite) DTimestamp bounds.
		if sec <= QuantileMinTimestampSec {
			t = QuantileMinTimestamp
		} else if sec >= QuantileMaxTimestampSec {
			t = QuantileMaxTimestamp
		} else {
			t = timeutil.Unix(int64(sec), int64(frac*1e9))
		}
		roundTo := tree.TimeFamilyPrecisionToRoundDuration(colType.Precision())
		return tree.MakeDTimestamp(t, roundTo)
	case types.TimestampTZFamily:
		sec, frac := math.Modf(val)
		var t time.Time
		// Clamp to (our alternative finite) DTimestamp bounds.
		if sec <= QuantileMinTimestampSec {
			t = QuantileMinTimestamp
		} else if sec >= QuantileMaxTimestampSec {
			t = QuantileMaxTimestamp
		} else {
			t = timeutil.Unix(int64(sec), int64(frac*1e9))
		}
		roundTo := tree.TimeFamilyPrecisionToRoundDuration(colType.Precision())
		return tree.MakeDTimestampTZ(t, roundTo)
	default:
		return nil, errors.Errorf("cannot convert quantile value to type %s", colType.Name())
	}
}
