// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import (
	"context"
	"math"
	"sort"

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
//   - Unlike histograms, quantile functions are independent of the absolute
//     counts. They are a "shape" not a "size".
//   - Unlike cumulative distribution functions or probability density functions,
//     we can always take the definite integral of a quantile function from p=0 to
//     p=1. We use this when performing linear regression over quantiles.
//
// Type quantile represents a piecewise quantile function with float64 values as
// a series of quantilePoints. The pieces of the quantile function are line
// segments between subsequent points (exclusive and inclusive, respectively).
// A quantile must have at least two points. The first point must have p=0, and
// the last point must have p=1. All points must be non-decreasing in p. A
// "well-formed" quantile is also non-decreasing in v. A "malformed" quantile
// has decreasing v in one or more pieces. A malformed quantile can be turned
// into a well-formed quantile by calling fixMalformed().
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
//	{200, 200, 210, 210, 210, 211, 212, 221, 222, 230}
//
// One possible histogram might be:
//
//	{{UpperBound: 200, NumRange: 0, NumEq: 2},
//	 {UpperBound: 210, NumRange: 0, NumEq: 3},
//	 {UpperBound: 220, NumRange: 2, NumEq: 0},
//	 {UpperBound: 230, NumRange: 2, NumEq: 1}}
//
// And the corresponding quantile function would be:
//
//	{{0, 200}, {0.2, 200}, {0.2, 210}, {0.5, 210}, {0.7, 220}, {0.9, 230}, {1, 230}}
//
//	230 |                 *-*
//	    |               /
//	220 |             *
//	    |           /
//	210 |   o-----*
//	    |
//	200 o---*
//	    |
//	190 + - - - - - - - - - -
//	    0  .2  .4  .6  .8   1
//
// All quantile functions and methods treat quantiles as immutable. We always
// allocate new quantiles rather than modifying them in-place.
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
// hard-coded number here.
const _ = 3 - uint(HistVersion)

// canMakeQuantile returns true if a quantile function can be created for a
// histogram of the given type. Note that by not supporting BYTES we rule out
// creating quantile functions for histograms of inverted columns.
// TODO(michae2): Add support for DECIMAL, TIME, TIMETZ, and INTERVAL.
func canMakeQuantile(version HistogramVersion, colType *types.T) bool {
	if version > 3 {
		return false
	}

	if colType == nil || colType.UserDefined() {
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
		// qTrimLo and qTrimHi are indexes to slice the quantile to when trimming
		// zero-row buckets from the beginning and end of the histogram.
		qTrimLo, qTrimHi quantileIndex
		q                = make(quantile, 0, len(hist.buckets)*2)
		prevV            = math.Inf(-1)
		p                float64
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
		q = append(q, quantilePoint{p: p, v: v})
		if p == 0 {
			qTrimLo = len(q) - 1
		}
		if num > 0 {
			qTrimHi = len(q)
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

	if qTrimHi <= qTrimLo {
		// In the unlikely case that every bucket had zero rows we simply return the
		// zeroQuantile.
		q = zeroQuantile
	} else {
		// Trim any zero-row buckets from the beginning and end.
		q = q[qTrimLo:qTrimHi]
		// Fix any floating point errors or histogram errors (e.g. sum of bucket row
		// counts < total row count) causing p to be below 1 at the end.
		q[len(q)-1].p = 1
	}
	return q, nil
}

// toHistogram converts a quantile into a histogram, using the provided type and
// row count. It returns an error if the conversion fails. The quantile must be
// well-formed before calling toHistogram.
func (q quantile) toHistogram(
	ctx context.Context, colType *types.T, rowCount float64,
) (histogram, error) {
	if len(q) < 2 || q[0].p != 0 || q[len(q)-1].p != 1 {
		return histogram{}, errors.AssertionFailedf("invalid quantile: %v", q)
	}

	// Empty table case.
	if rowCount < 1 {
		return histogram{buckets: make([]cat.HistogramBucket, 0)}, nil
	}

	// We don't use any session data for conversions or operations on upper
	// bounds, so a nil *eval.Context works as our tree.CompareContext.
	var compareCtx *eval.Context

	hist := histogram{buckets: make([]cat.HistogramBucket, 0, len(q)-1)}

	var i quantileIndex
	// Skip any leading p=0 points instead of emitting zero-row buckets.
	for q[i].p == 0 {
		i++
	}

	// Create the first bucket of the histogram. The first bucket must always have
	// NumRange == 0. Sometimes we will emit a zero-row bucket to make this true.
	var currentLowerBound tree.Datum
	currentUpperBound, err := fromQuantileValue(colType, q[i-1].v)
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
		currentBucket.NumEq = numEq

		// Calculate DistinctRange for this bucket now that NumRange is finalized.
		distinctRange := estimatedDistinctValuesInRange(
			ctx, compareCtx, currentBucket.NumRange, currentLowerBound, currentUpperBound,
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
	for ; i < len(q); i++ {
		upperBound, err := fromQuantileValue(colType, q[i].v)
		if err != nil {
			return histogram{}, err
		}
		cmp, err := upperBound.Compare(ctx, compareCtx, currentUpperBound)
		if err != nil {
			return histogram{}, err
		}
		if cmp < 0 {
			return histogram{}, errors.AssertionFailedf("decreasing histogram values")
		}
		if cmp == 0 {
			pEq += q[i].p - q[i-1].p
		} else {
			if err := closeCurrentBucket(); err != nil {
				return histogram{}, err
			}

			// Start a new current bucket.
			pRange := q[i].p - q[i-1].p
			numRange := pRange * rowCount
			if !isValidCount(numRange) {
				return histogram{}, errors.AssertionFailedf("invalid histogram NumRange: %v", numRange)
			}
			currentLowerBound = getNextLowerBound(ctx, compareCtx, currentUpperBound)
			currentUpperBound = upperBound
			currentBucket = cat.HistogramBucket{
				NumEq:         0,
				NumRange:      numRange,
				DistinctRange: 0,
				UpperBound:    currentUpperBound,
			}
		}
		// Skip any trailing p=1 points instead of emitting zero-row buckets.
		if q[i].p == 1 {
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
//  1. It supports conversions that are not legal casts (e.g. DATE to FLOAT).
//  2. It errors on NaN and infinite values because they will break our model.
//     fromQuantileValue is the inverse of this function, and together they should
//     support round-trip conversions.
//
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
		if v.Before(tree.MinSupportedTime) || v.After(tree.MaxSupportedTime) {
			return 0, tree.ErrFloatOutOfRange
		}
		return float64(v.Unix()) + float64(v.Nanosecond())*1e-9, nil
	case *tree.DTimestampTZ:
		// TIMESTAMPTZ doesn't store a timezone, so this is the same as TIMESTAMP.
		if v.Before(tree.MinSupportedTime) || v.After(tree.MaxSupportedTime) {
			return 0, tree.ErrFloatOutOfRange
		}
		return float64(v.Unix()) + float64(v.Nanosecond())*1e-9, nil
	default:
		return 0, errors.Errorf("cannot make quantile value from %v", d)
	}
}

// fromQuantileValue converts from a quantile value back to a datum suitable for
// use in a histogram. It is the inverse of toQuantileValue. It differs from
// eval.PerformCast in a few ways:
//  1. It supports conversions that are not legal casts (e.g. FLOAT to DATE).
//  2. It errors on NaN and infinite values because they indicate a problem with
//     the regression model rather than valid values.
//  3. On overflow or underflow it clamps to maximum or minimum finite values
//     rather than failing the conversion (and thus the entire histogram).
//
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
		// Return an error for all values outside the supported time range.
		if sec < tree.MinSupportedTimeSec || sec > tree.MaxSupportedTimeSec {
			return nil, tree.ErrFloatOutOfRange
		}
		t := timeutil.Unix(int64(sec), int64(frac*1e9))
		roundTo := tree.TimeFamilyPrecisionToRoundDuration(colType.Precision())
		if colType.Family() == types.TimestampFamily {
			return tree.MakeDTimestamp(t, roundTo)
		}
		return tree.MakeDTimestampTZ(t, roundTo)
	default:
		return nil, errors.Errorf("cannot convert quantile value to type %s", colType.Name())
	}
}

// quantilePiece returns the slope (m) and intercept (b) of the line segment
// with points c and d, defined by the equation: v = mp + b. This must not be
// called on a discontinuity (vertical piece where c.p = d.p).
func quantilePiece(c, d quantilePoint) (m, b float64) {
	m = (d.v - c.v) / (d.p - c.p)
	b = c.v - m*c.p
	return m, b
}

// quantilePieceV returns the v at which the quantile piece with points c and d
// intersects a vertical line at p. This must not be called on a discontinuity
// (vertical piece where c.p = d.p).
func quantilePieceV(c, d quantilePoint, p float64) (v float64) {
	m, b := quantilePiece(c, d)
	return m*p + b
}

// quantilePieceP returns the p at which the quantile piece with points c and d
// intersects a horizontal line at p. This must not be called on a discontinuity
// (vertical piece where c.p = d.p) or a zero-slope piece (horizontal piece
// where c.v = d.v).
func quantilePieceP(c, d quantilePoint, v float64) (p float64) {
	m, b := quantilePiece(c, d)
	return (v - b) / m
}

// binaryOp returns a quantile function representing the result of some binary
// operation on q and r. op is a binary operator that returns a result v given v
// from q and r. This is used for addition and subtraction of quantiles.
func (q quantile) binaryOp(r quantile, op func(qv, rv float64) float64) quantile {
	result := make(quantile, 0, len(q)+len(r))
	result = append(result, quantilePoint{p: 0, v: op(q[0].v, r[0].v)})

	// Walk both quantile functions in p order, creating a point in the result for
	// each distinct p.
	var i, j quantileIndex
	for i, j = 1, 1; i < len(q) && j < len(r); {
		if q[i].p < r[j].p {
			// Find v in the piece of r with right endpoint r[j] at which p = q[i].p.
			// This piece of r will never be a discontinuity. Proof:
			// 1. We visited r[j-1] before q[i], implying r[j-1].p <= q[i].p. For the
			//    i=1 j=1 case, this relies on the assumption that q[0].p = r[0].p
			//    (they should both be 0).
			// 2. Now we're in the q[i].p < r[j].p case.
			// 3. Therefore r[j-1].p < r[j].p meaning this isn't a discontinuity.
			rv := quantilePieceV(r[j-1], r[j], q[i].p)
			result = append(result, quantilePoint{p: q[i].p, v: op(q[i].v, rv)})
			i++
		} else if r[j].p < q[i].p {
			// Find v in the piece of q with right endpoint q[i] at which p = r[j].p.
			// This piece of q will never be a discontinuity (see above).
			qv := quantilePieceV(q[i-1], q[i], r[j].p)
			result = append(result, quantilePoint{p: r[j].p, v: op(qv, r[j].v)})
			j++
		} else {
			result = append(result, quantilePoint{p: q[i].p, v: op(q[i].v, r[j].v)})
			i++
			j++
		}
	}

	// Handle any trailing p=1 points.
	for ; i < len(q); i++ {
		result = append(result, quantilePoint{p: q[i].p, v: op(q[i].v, r[len(r)-1].v)})
	}
	for ; j < len(r); j++ {
		result = append(result, quantilePoint{p: r[j].p, v: op(q[len(q)-1].v, r[j].v)})
	}

	return result
}

// add returns a quantile function which represents the sum of q and r. The
// returned quantile function will usually have more points than either q or r,
// depending on how their points align. The returned quantile function could be
// malformed if either q or r are malformed.
func (q quantile) add(r quantile) quantile {
	return q.binaryOp(r, func(qv, rv float64) float64 { return qv + rv })
}

// sub returns a quantile function which represents q minus r. The returned
// quantile function will usually have more points than either q or r, depending
// on how their points align. The returned quantile function could be malformed,
// even if both q and r are well-formed.
func (q quantile) sub(r quantile) quantile {
	return q.binaryOp(r, func(qv, rv float64) float64 { return qv - rv })
}

// mult returns a quantile function which represents q multiplied by a scalar
// c. The returned quantile function could be malformed if c is negative or q is
// malformed.
func (q quantile) mult(c float64) quantile {
	// Avoid creating a duplicate quantile.
	if c == 1 {
		return q
	}
	prod := make(quantile, len(q))
	for i := range q {
		prod[i] = quantilePoint{p: q[i].p, v: q[i].v * c}
	}
	return prod
}

// integrateSquared returns the definite integral w.r.t. p of the quantile
// function squared.
func (q quantile) integrateSquared() float64 {
	var area float64
	for i := 1; i < len(q); i++ {
		if q[i].p == q[i-1].p {
			// Skip over the discontinuity.
			continue
		}
		// Each continuous piece of the quantile function is a line segment
		// described by:
		//
		//   v = mp + b
		//
		// We're trying to integrate the square of this from p₁ to p₂:
		//
		//   p₂
		//    ∫ (mp + b)² dp
		//   p₁
		//
		// which is equivalent to solving this at both p₁ and p₂:
		//
		//    1            |p₂
		//   --- (mp + b)³ |
		//   3m            |p₁
		//
		// For some pieces, m will be 0. In that case we're solving:
		//
		//   p₂
		//    ∫ b² dp
		//   p₁
		//
		// which is equivalent to solving this at both p₁ and p₂:
		//
		//       |p₂
		//   b²p |
		//       |p₁
		//
		m, b := quantilePiece(q[i-1], q[i])
		if m == 0 {
			bb := b * b
			a2 := bb * q[i].p
			a1 := bb * q[i-1].p
			area += a2 - a1
		} else {
			v2 := m*q[i].p + b
			v1 := m*q[i-1].p + b
			a2 := v2 * v2 * v2 / 3 / m
			a1 := v1 * v1 * v1 / 3 / m
			area += a2 - a1
		}
	}
	return area
}

// fixMalformed returns a quantile function that represents the same
// distribution as q, but is well-formed.
//
// A malformed quantile function has pieces with negative slope (i.e. subsequent
// points with decreasing v). These pieces represent the quantile function
// "folding backward" over parts of the distribution it has already described. A
// well-formed quantile function has zero or positive slope everywhere (i.e. is
// non-decreasing in v).
//
// This function fixes the negative slope pieces by "moving" p from the
// overlapping places to where it should be. No p is lost in the making of these
// calculations.
//
// fixMalformed should not be called without checking that isInvalid() is false.
func (q quantile) fixMalformed() quantile {
	// Check for the happy case where q is already well-formed.
	if q.isWellFormed() {
		return q
	}

	// To fix a malformed quantile function, we recalculate p for each distinct
	// value by summing the total p wherever v <= that value. We do this by
	// drawing a horizontal line across the malformed quantile function at
	// v = that value, finding all of the intersections, and adding the distances
	// between intersections.
	//
	// (This resembles the even-odd algorithm used to solve the point-in-polygon
	// problem, see https://en.wikipedia.org/wiki/Even%E2%80%93odd_rule for more
	// information.)
	//
	// For example, for this malformed quantile function:
	//
	//   {{0, 100}, {0.25, 200}, {0.5, 100}, {0.75, 0}, {1, 100}}
	//
	// to recalculate p for value = 100, we add together the length of these
	// intervals between intersections (the first interval has a length of zero).
	//
	// Intervals with v <= 100:           +       +-------+
	//
	// Malformed quantile function:   200 |   *
	//                                    | /   \
	//                                100 o       *       *
	//                                    |         \   /
	//                                  0 + - - - - - * - -
	//                                    0  .25 .5  .75  1
	//
	// After recalculating p for each distinct value, the well-formed quantile
	// function is:
	//
	//   {{0, 0}, {0.5, 100}, {1, 200}}
	//
	// Which notably has the same (distinct) values and the same definite integral
	// as the original malformed quantile function.

	fixed := make(quantile, 0, len(q))
	// Right endpoints of pieces with one endpoint <= val and one endpoint > val.
	crossingPieces := make([]quantileIndex, 0, len(q)-1)
	// Right endpoints of pieces with both endpoints = val. Allocated here as an
	// optimization.
	equalPieces := make([]quantileIndex, 0, len(q)-1)
	// Exact p's of intersections with line v = val. Allocated here as an
	// optimization.
	intersectionPs := make([]float64, 0, len(q)+1)

	// We'll visit points in (v, p) order.
	pointsByValue := make([]quantileIndex, len(q))
	for i := range pointsByValue {
		pointsByValue[i] = i
	}
	sort.SliceStable(pointsByValue, func(i, j int) bool {
		return q[pointsByValue[i]].v < q[pointsByValue[j]].v
	})

	// For each distinct value "val" in the quantile function, from low to high,
	// calculate intersections with an imaginary line v = val from p=0 to p=1.
	for i := 0; i < len(pointsByValue); {
		val := q[pointsByValue[i]].v

		// First find all equal and crossing pieces.
		equalPieces = equalPieces[:0]

		// As an optimization, we do not need to check every piece to find
		// intersections with v = val. We only need to check (1) pieces that
		// intersected the previous val (the current contents of crossingPieces),
		// and (2) pieces that have the current val as an endpoint.
		//
		// A short proof: every piece that intersects the current val will have one
		// endpoint <= val and one endpoint > val. All pieces with one endpoint <
		// val and one endpoint > val also intersected the previous val and are in
		// (1). All pieces with one endpoint = val and one endpoint > val have the
		// current val as an endpoint and are in (2).

		// Check (1) pieces that intersected the previous val.
		var k int
		for j, qi := range crossingPieces {
			// We already know one endpoint <= prev val < current val, so we just need
			// to check whether one endpoint > val.
			if q[qi-1].v > val || q[qi].v > val {
				crossingPieces[k] = crossingPieces[j]
				k++
			}
		}
		crossingPieces = crossingPieces[:k]

		// Check (2) all pieces that have the current val as an endpoint. Note that this
		// also moves i past all points with the current val.
		for ; i < len(pointsByValue) && q[pointsByValue[i]].v == val; i++ {
			qi := pointsByValue[i]
			// We know q[b].v = val, so we just need to check whether the points to
			// the left and right have v > val.
			if qi > 0 && q[qi-1].v > val {
				crossingPieces = append(crossingPieces, qi)
			}
			if qi+1 < len(q) && q[qi+1].v > val {
				crossingPieces = append(crossingPieces, qi+1)
			}
			// Also look for flat pieces = val. We only need to check to the right.
			if qi+1 < len(q) && q[qi+1].v == val {
				equalPieces = append(equalPieces, qi+1)
			}
		}

		sort.Ints(crossingPieces)

		// Now find exact p's of the intersections with all crossing pieces.
		intersectionPs = intersectionPs[:0]

		// If the first point in the quantile function is > val, we need to add a
		// starting intersection at p=0.
		if q[0].v > val {
			intersectionPs = append(intersectionPs, q[0].p)
		}

		// Add one intersection p for each crossing piece. (We do not need to add
		// intersections for equal pieces, as we count them as fully "below" the v =
		// val line.)
		for _, qi := range crossingPieces {
			if q[qi-1].p == q[qi].p || q[qi-1].v == val {
				// At a discontinuity we simply use the p shared by both points.
				intersectionPs = append(intersectionPs, q[qi-1].p)
			} else if q[qi].v == val {
				intersectionPs = append(intersectionPs, q[qi].p)
			} else {
				// Crossing pieces will never have zero slope because they have one
				// point <= val and one point > val.
				p := quantilePieceP(q[qi-1], q[qi], val)
				intersectionPs = append(intersectionPs, p)
			}
		}

		// If the last point in the quantile function is <= val, we need to add an
		// ending intersection at p=1.
		if q[len(q)-1].v <= val {
			intersectionPs = append(intersectionPs, q[len(q)-1].p)
		}

		// Now add the interval widths to get the new p for val. There will always
		// be an odd number of intersectionPs because the quantile function starts
		// "below" our horizontal line (<= v) and ends "above" our horizontal line
		// (> v) and we always add two intersectionPs for "double roots" touching
		// our line from above (one endpoint > v, one endpoint = v).
		if len(intersectionPs) == 0 {
			return q
		}
		lessEqP := intersectionPs[0]
		for j := 1; j < len(intersectionPs); j += 2 {
			lessEqP += intersectionPs[j+1] - intersectionPs[j]
		}
		var eqP float64
		for _, d := range equalPieces {
			eqP += q[d].p - q[d-1].p
		}
		lessP := lessEqP - eqP

		fixed = append(fixed, quantilePoint{p: lessP, v: val})
		if eqP != 0 {
			fixed = append(fixed, quantilePoint{p: lessEqP, v: val})
		}
	}
	return fixed
}

// isInvalid returns true if q contains NaN or Inf values.
func (q quantile) isInvalid() bool {
	for i := range q {
		if math.IsNaN(q[i].v) || math.IsInf(q[i].v, 0) {
			return true
		}
	}
	return false
}

// isWellFormed returns true if q is well-formed (i.e. is non-decreasing in v).
func (q quantile) isWellFormed() bool {
	for i := 1; i < len(q); i++ {
		if q[i].v < q[i-1].v {
			return false
		}
	}
	return true
}
