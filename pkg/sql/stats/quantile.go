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

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/errors"
)

// quantile is a piecewise quantile function with float64 values.
//
// A quantile function is a way of representing a probability distribution. It
// is a function from p to v, over (p=0, p=1], where p is the probability that
// an item in the distribution will have value <= v. The quantile function for a
// probability distribution is the inverse of the cumulative distribution
// function for the same probability distribution. See
// https://en.wikipedia.org/wiki/Quantile_function for more background.
//
// We use quantile functions within our modeling for a few reasons:
// * Unlike histograms, quantile functions are independent of the absolute
//   counts. They are a "shape" not a "size".
// * Unlike cumulative distribution functions or probability density functions,
//   we can always take the definite integral of a quantile function from p=0 to
//   p=1. We use this when performing linear regression over quantiles.
//
// Type quantile represents a piecewise quantile function with float64 values as
// a series of quantilePoints from p=0 (exclusive) to p=1 (inclusive). A
// well-formed quantile is non-decreasing in both p and v. A quantile must have
// at least two points. The first point must have p=0, and the last point must
// have p=1. The pieces of the quantile function are line segments between
// subsequent points (exclusive and inclusive, respectively).
//
// Subsequent points may have the same p (a vertical line, or discontinuity),
// meaning the probability of finding a value > v₁ and <= v₂ is zero. Subsequent
// points may have the same v (a horizontal line), meaning the probability of
// finding exactly that v is p₂ - p₁. To put it in terms of our histograms:
// NumRange = 0 becomes a vertical line, NumRange > 0 becomes a slanted line
// with positive slope, NumEq = 0 goes away, and NumEq > 0 becomes a horizontal
// line.
//
// For example, given this population of 10 values:
//
//  {200, 200, 210, 210, 210, 211, 212, 221, 222, 230}
//
// One possible histogram might be:
//
//   {{UpperBound: 200, NumRange: 0, NumEq: 2},
//    {UpperBound: 210, NumRange: 0, NumEq: 3},
//    {UpperBound: 220, NumRange: 2, NumEq: 0},
//    {UpperBound: 230, NumRange: 2, NumEq: 1}}
//
// And the corresponding quantile function would be:
//
//   {{0, 200}, {0.2, 200}, {0.2, 210}, {0.5, 210}, {0.7, 220}, {0.9, 230}, {1, 230}}
//
//   230 |                 *-*
//       |               /
//   220 |             *
//       |           /
//   210 |   o-----*
//       |
//   200 o---*
//       |
//   190 + - - - - - - - - - -
//       0  .2  .4  .6  .8   1
//
type quantile []quantilePoint

// quantilePoint is an endpoint of a piece (line segment) in a piecewise
// quantile function.
type quantilePoint struct {
	p, v float64
}

// quantileIndex is the ordinal position of a quantilePoint within a
// quantile.
type quantileIndex = int

// zeroQuantile is what we use for empty tables. Technically it says nothing
// about the number of rows in the table / items in the probability
// distribution, only that they all equal the zero value.
var zeroQuantile = quantile{{p: 0, v: 0}, {p: 1, v: 0}}

// If you are introducing a new histogram version, please check whether
// makeQuantile and quantile.toHistogram need to change, and then increase the
// version number in this check.
const _ uint = 1 - uint(histVersion)

// canMakeQuantile returns true if a quantile function can be created for a
// histogram of the given type.
// TODO(michae2): Add support for DECIMAL, TIME, TIMETZ, and INTERVAL.
func canMakeQuantile(colType *types.T) bool {
	if colType.UserDefined() {
		return false
	}
	switch colType.Family() {
	case types.IntFamily,
		types.FloatFamily,
		types.DateFamily,
		types.TimestampFamily,
		types.TimestampTZFamily:
		// TODO(michae2): Even if the column is one of these types, explicit or
		// implicit constraints could make it behave like an ENUM. For example, the
		// hidden shard column of a hash-sharded index is INT8 and yet will only
		// ever contain a few specific values which do not make sense to compare
		// using < or > operators. Histogram prediction will likely not work well
		// for these de facto ENUM columns, so we should skip them.
		//
		// One way to detect these columns would be to watch out for histograms with
		// NumRange == 0 in every bucket.
		return true
	default:
		return false
	}
}

// makeQuantile converts a histogram to a quantile function, or returns an error
// if it cannot. The histogram must not contain a bucket for NULL values, and
// the row count must not include NULL values. The first bucket of the histogram
// must have NumRange == 0.
func makeQuantile(hist histogram, rowCount float64) (quantile, error) {
	if !isValidCount(rowCount) {
		return nil, errors.AssertionFailedf("invalid rowCount: %v", rowCount)
	}

	// Empty table cases.
	if len(hist.buckets) == 0 || rowCount < 1 {
		return zeroQuantile, nil
	}

	// To produce a quantile with first point at p=0 and at least two points, we
	// need the first bucket to have NumRange == 0.
	if hist.buckets[0].NumRange != 0 {
		return nil, errors.AssertionFailedf(
			"histogram with non-zero NumRange in first bucket: %v", hist.buckets[0].NumRange,
		)
	}

	var (
		// qfTrimLo and qfTrimHi are indexes to slice the quantile to when trimming
		// zero-row buckets from the beginning and end of the histogram.
		qfTrimLo, qfTrimHi quantileIndex
		qf                 = make(quantile, 0, len(hist.buckets)*2)
		prevV              = math.Inf(-1)
		p                  float64
	)

	// Add a point counting num rows with value <= v.
	addPoint := func(num, v float64) error {
		if !isValidCount(num) {
			return errors.AssertionFailedf("invalid histogram num: %v", num)
		}
		// Advance p by the proportion of rows counted by num.
		p += num / rowCount
		// Fix any floating point errors or histogram errors (e.g. sum of bucket row
		// counts > total row count) causing p to go above 1.
		if p > 1 {
			p = 1
		}
		qf = append(qf, quantilePoint{p: p, v: v})
		if p == 0 {
			qfTrimLo = len(qf) - 1
		}
		if num > 0 {
			qfTrimHi = len(qf)
		}
		return nil
	}

	// For each histogram bucket, add two points to the quantile: (1) an endpoint
	// for NumRange and (2) an endpoint for NumEq. If NumEq == 0 we can skip the
	// second point, but we must always add the first point even if NumRange == 0.
	for i := range hist.buckets {
		if hist.buckets[i].NumRange < 0 || hist.buckets[i].NumEq < 0 {
			return nil, errors.AssertionFailedf("histogram bucket with negative row count")
		}
		v, err := toQuantileValue(hist.buckets[i].UpperBound)
		if err != nil {
			return nil, err
		}
		if v <= prevV {
			return nil, errors.AssertionFailedf("non-increasing quantile values")
		}
		prevV = v

		if err := addPoint(hist.buckets[i].NumRange, v); err != nil {
			return nil, err
		}
		if hist.buckets[i].NumEq == 0 {
			// Small optimization: skip adding a duplicate point to the quantile.
			continue
		}
		if err := addPoint(hist.buckets[i].NumEq, v); err != nil {
			return nil, err
		}
	}

	if qfTrimHi <= qfTrimLo {
		// In the unlikely case that every bucket had zero rows we simply return the
		// zeroQuantile.
		qf = zeroQuantile
	} else {
		// Trim any zero-row buckets from the beginning and end.
		qf = qf[qfTrimLo:qfTrimHi]
		// Fix any floating point errors or histogram errors (e.g. sum of bucket row
		// counts < total row count) causing p to be below 1 at the end.
		qf[len(qf)-1].p = 1
	}
	return qf, nil
}

// toHistogram converts a quantile into a histogram, using the provided type and
// row count. It returns an error if the conversion fails.
func (qf quantile) toHistogram(
	evalCtx *eval.Context, colType *types.T, rowCount float64,
) (histogram, error) {
	if len(qf) < 2 || qf[0].p != 0 || qf[len(qf)-1].p != 1 {
		return histogram{}, errors.AssertionFailedf("invalid quantile: %v", qf)
	}

	// Empty table case.
	if rowCount < 1 {
		return histogram{}, nil
	}

	hist := histogram{buckets: make([]cat.HistogramBucket, 0, len(qf)-1)}

	var i quantileIndex
	// Skip any leading p=0 points instead of emitting zero-row buckets.
	for qf[i].p == 0 {
		i++
	}

	// Create the first bucket of the histogram. The first bucket must always have
	// NumRange == 0. Sometimes we will emit a zero-row bucket to make this true.
	var currentLowerBound tree.Datum
	currentUpperBound, err := fromQuantileValue(colType, qf[i-1].v)
	if err != nil {
		return histogram{}, err
	}
	currentBucket := cat.HistogramBucket{
		NumEq:         0,
		NumRange:      0,
		DistinctRange: 0,
		UpperBound:    currentUpperBound,
	}

	var pEq float64

	// Set NumEq of the current bucket before creating a new current bucket.
	closeCurrentBucket := func() error {
		numEq := pEq * rowCount
		if !isValidCount(numEq) {
			return errors.AssertionFailedf("invalid histogram NumEq: %v", numEq)
		}
		if numEq < 1 && currentBucket.NumRange+numEq >= 2 {
			// Steal from NumRange so that NumEq is at least 1, if it wouldn't make
			// NumRange 0. This makes the histogram look more like something
			// EquiDepthHistogram would produce.
			currentBucket.NumRange -= 1 - numEq
			numEq = 1
		}
		currentBucket.NumEq = numEq

		// Calculate DistinctRange for this bucket now that NumRange is finalized.
		distinctRange := estimatedDistinctValuesInRange(
			evalCtx, currentBucket.NumRange, currentLowerBound, currentUpperBound,
		)
		if !isValidCount(distinctRange) {
			return errors.AssertionFailedf("invalid histogram DistinctRange: %v", distinctRange)
		}
		currentBucket.DistinctRange = distinctRange

		hist.buckets = append(hist.buckets, currentBucket)
		pEq = 0
		return nil
	}

	// For each point in the quantile, if its value is equal to the current
	// upperBound then add to NumEq of the current bucket. Otherwise close the
	// current bucket and add to NumRange of a new current bucket.
	for ; i < len(qf); i++ {
		upperBound, err := fromQuantileValue(colType, qf[i].v)
		if err != nil {
			return histogram{}, err
		}
		cmp, err := upperBound.CompareError(evalCtx, currentUpperBound)
		if err != nil {
			return histogram{}, err
		}
		if cmp < 0 {
			return histogram{}, errors.AssertionFailedf("decreasing histogram values")
		}
		if cmp == 0 {
			pEq += qf[i].p - qf[i-1].p
		} else {
			if err := closeCurrentBucket(); err != nil {
				return histogram{}, err
			}

			// Start a new current bucket.
			pRange := qf[i].p - qf[i-1].p
			numRange := pRange * rowCount
			if !isValidCount(numRange) {
				return histogram{}, errors.AssertionFailedf("invalid histogram NumRange: %v", numRange)
			}
			currentLowerBound = getNextLowerBound(evalCtx, currentUpperBound)
			currentUpperBound = upperBound
			currentBucket = cat.HistogramBucket{
				NumEq:         0,
				NumRange:      numRange,
				DistinctRange: 0,
				UpperBound:    currentUpperBound,
			}
		}
		// Skip any trailing p=1 points instead of emitting zero-row buckets.
		if qf[i].p == 1 {
			break
		}
	}

	// Close the last bucket.
	if err := closeCurrentBucket(); err != nil {
		return histogram{}, err
	}

	return hist, nil
}

func isValidCount(x float64) bool {
	return x >= 0 && !math.IsInf(x, 0) && !math.IsNaN(x)
}

// toQuantileValue converts from a datum to a float suitable for use in a quantile
// function. It differs from eval.PerformCast in a few ways:
// 1. It supports conversions that are not legal casts (e.g. DATE to FLOAT).
// 2. It errors on NaN and infinite values because they will break our model.
// fromQuantileValue is the inverse of this function, and together they should
// support round-trip conversions.
// TODO(michae2): Add support for DECIMAL, TIME, TIMETZ, and INTERVAL.
func toQuantileValue(d tree.Datum) (float64, error) {
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
		// TIMESTAMPTZ doesn't store a timezone, so this is the same as TIMESTAMP.
		if v.Equal(pgdate.TimeInfinity) || v.Equal(pgdate.TimeNegativeInfinity) {
			return 0, tree.ErrFloatOutOfRange
		}
		return float64(v.Unix()) + float64(v.Nanosecond())*1e-9, nil
	default:
		return 0, errors.Errorf("cannot make quantile value from %v", d)
	}
}

var (
	// quantileMinTimestamp is an alternative minimum finite DTimestamp value to
	// avoid the problems around TimeNegativeInfinity, see #41564.
	quantileMinTimestamp    = tree.MinSupportedTime.Add(time.Second)
	quantileMinTimestampSec = float64(quantileMinTimestamp.Unix())
	// quantileMaxTimestamp is an alternative maximum finite DTimestamp value to
	// avoid the problems around TimeInfinity, see #41564.
	quantileMaxTimestamp    = tree.MaxSupportedTime.Add(-1 * time.Second).Truncate(time.Second)
	quantileMaxTimestampSec = float64(quantileMaxTimestamp.Unix())
)

// fromQuantileValue converts from a quantile value back to a datum suitable for
// use in a histogram. It is the inverse of toQuantileValue. It differs from
// eval.PerformCast in a few ways:
// 1. It supports conversions that are not legal casts (e.g. FLOAT to DATE).
// 2. It errors on NaN and infinite values because they indicate a problem with
//    the regression model rather than valid values.
// 3. On overflow or underflow it clamps to maximum or minimum finite values
//    rather than failing the conversion (and thus the entire histogram).
// TODO(michae2): Add support for DECIMAL, TIME, TIMETZ, and INTERVAL.
func fromQuantileValue(colType *types.T, val float64) (tree.Datum, error) {
	if math.IsNaN(val) || math.IsInf(val, 0) {
		return nil, tree.ErrFloatOutOfRange
	}
	switch colType.Family() {
	case types.IntFamily:
		i := math.Round(val)
		// Clamp instead of truncating.
		switch colType.Width() {
		case 16:
			if i < math.MinInt16 {
				i = math.MinInt16
			} else if i > math.MaxInt16 {
				i = math.MaxInt16
			}
		case 32:
			if i < math.MinInt32 {
				i = math.MinInt32
			} else if i > math.MaxInt32 {
				i = math.MaxInt32
			}
		default:
			if i < math.MinInt64 {
				i = math.MinInt64
			} else if i >= math.MaxInt64 {
				// float64 cannot represent 2^63 - 1 exactly, so cast directly to DInt.
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
		if days < math.MinInt32 {
			days = math.MinInt32
		} else if days > math.MaxInt32 {
			days = math.MaxInt32
		}
		// Then clamp to pgdate.Date.
		return tree.NewDDate(pgdate.MakeDateFromPGEpochClampFinite(int32(days))), nil
	case types.TimestampFamily, types.TimestampTZFamily:
		sec, frac := math.Modf(val)
		var t time.Time
		// Clamp to (our alternative finite) DTimestamp bounds.
		if sec <= quantileMinTimestampSec {
			t = quantileMinTimestamp
		} else if sec >= quantileMaxTimestampSec {
			t = quantileMaxTimestamp
		} else {
			t = timeutil.Unix(int64(sec), int64(frac*1e9))
		}
		roundTo := tree.TimeFamilyPrecisionToRoundDuration(colType.Precision())
		if colType.Family() == types.TimestampFamily {
			return tree.MakeDTimestamp(t, roundTo)
		}
		return tree.MakeDTimestampTZ(t, roundTo)
	default:
		return nil, errors.Errorf("cannot convert quantile value to type %s", colType.Name())
	}
}
