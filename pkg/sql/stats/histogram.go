// Copyright 2017 The Cockroach Authors.
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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// HistogramClusterMode controls the cluster setting for enabling
// histogram collection.
var HistogramClusterMode = settings.RegisterBoolSetting(
	"sql.stats.histogram_collection.enabled",
	"histogram collection mode",
	true,
).WithPublic()

// EquiDepthHistogram creates a histogram where each bucket contains roughly
// the same number of samples (though it can vary when a boundary value has
// high frequency).
//
// numRows is the total number of rows from which values were sampled
// (excluding rows that have NULL values on the histogram column).
//
// In addition to building the histogram buckets, EquiDepthHistogram also
// estimates the number of distinct values in each bucket. It distributes the
// known number of distinct values (distinctCount) among the buckets, in
// proportion with the number of rows in each bucket.
func EquiDepthHistogram(
	evalCtx *tree.EvalContext,
	colType *types.T,
	samples tree.Datums,
	numRows, distinctCount int64,
	maxBuckets int,
) (HistogramData, error) {
	numSamples := len(samples)
	if numSamples == 0 {
		return HistogramData{ColumnType: colType}, nil
	}
	if maxBuckets < 2 {
		return HistogramData{}, errors.Errorf("histogram requires at least two buckets")
	}
	if numRows < int64(numSamples) {
		return HistogramData{}, errors.Errorf("more samples than rows")
	}
	for _, d := range samples {
		if d == tree.DNull {
			return HistogramData{}, errors.Errorf("NULL values not allowed in histogram")
		}
	}
	sort.Slice(samples, func(i, j int) bool {
		return samples[i].Compare(evalCtx, samples[j]) < 0
	})
	numBuckets := maxBuckets
	if maxBuckets > numSamples {
		numBuckets = numSamples
	}
	h := HistogramData{
		Buckets: make([]HistogramData_Bucket, 0, numBuckets),
	}
	lowerBound := samples[0]
	h.ColumnType = lowerBound.ResolvedType()
	var distinctCountRange, distinctCountEq float64

	// i keeps track of the current sample and advances as we form buckets.
	for i, b := 0, 0; b < numBuckets && i < numSamples; b++ {
		// num is the number of samples in this bucket. The first bucket has
		// num=1 so the histogram has a clear lower bound.
		num := (numSamples - i) / (numBuckets - b)
		if i == 0 || num < 1 {
			num = 1
		}
		upper := samples[i+num-1]
		// numLess is the number of samples less than upper (in this bucket).
		numLess := 0
		for ; numLess < num-1; numLess++ {
			if c := samples[i+numLess].Compare(evalCtx, upper); c == 0 {
				break
			} else if c > 0 {
				return HistogramData{}, errors.AssertionFailedf("%+v", "samples not sorted")
			}
		}
		// Advance the boundary of the bucket to cover all samples equal to upper.
		for ; i+num < numSamples; num++ {
			if samples[i+num].Compare(evalCtx, upper) != 0 {
				break
			}
		}

		numEq := int64(num-numLess) * numRows / int64(numSamples)
		numRange := int64(numLess) * numRows / int64(numSamples)
		distinctRange := estimatedDistinctValuesInRange(float64(numRange), lowerBound, upper)
		encoded, err := rowenc.EncodeTableKey(nil, upper, encoding.Ascending)
		if err != nil {
			return HistogramData{}, err
		}

		i += num
		h.Buckets = append(h.Buckets, HistogramData_Bucket{
			NumEq:         numEq,
			NumRange:      numRange,
			DistinctRange: distinctRange,
			UpperBound:    encoded,
		})

		// Keep track of the total number of estimated distinct values. This will
		// be used to adjust the distinct count below.
		distinctCountRange += distinctRange
		if numEq > 0 {
			distinctCountEq++
		}

		lowerBound = getNextLowerBound(evalCtx, upper)
	}
	h.adjustDistinctCount(float64(distinctCount), distinctCountRange, distinctCountEq)
	return h, nil
}

// adjustDistinctCount adjusts the number of distinct values per bucket based
// on the total number of distinct values.
func (h *HistogramData) adjustDistinctCount(
	distinctCountTotal, distinctCountRange, distinctCountEq float64,
) {
	if distinctCountRange == 0 {
		return
	}

	adjustmentFactor := (distinctCountTotal - distinctCountEq) / distinctCountRange
	if adjustmentFactor < 0 {
		adjustmentFactor = 0
	}
	for i := range h.Buckets {
		h.Buckets[i].DistinctRange *= adjustmentFactor
	}
}

// estimatedDistinctValuesInRange returns the estimated number of distinct
// values in the range [lowerBound, upperBound), given that the total number
// of values is numRange.
func estimatedDistinctValuesInRange(numRange float64, lowerBound, upperBound tree.Datum) float64 {
	if maxDistinct, ok := maxDistinctValuesInRange(lowerBound, upperBound); ok {
		return expectedDistinctCount(numRange, maxDistinct)
	}
	return numRange
}

// maxDistinctValuesInRange returns the maximum number of distinct values in
// the range [lowerBound, upperBound). It returns ok=false when it is not
// possible to determine a finite value (which is the case for all types other
// than integers and dates).
func maxDistinctValuesInRange(lowerBound, upperBound tree.Datum) (_ float64, ok bool) {
	switch lowerBound.ResolvedType().Family() {
	case types.IntFamily:
		return float64(*upperBound.(*tree.DInt)) - float64(*lowerBound.(*tree.DInt)), true

	case types.DateFamily:
		lower := lowerBound.(*tree.DDate)
		upper := upperBound.(*tree.DDate)
		if lower.IsFinite() && upper.IsFinite() {
			return float64(upper.PGEpochDays()) - float64(lower.PGEpochDays()), true
		}
		return 0, false

	default:
		return 0, false
	}
}

func getNextLowerBound(evalCtx *tree.EvalContext, currentUpperBound tree.Datum) tree.Datum {
	nextLowerBound, ok := currentUpperBound.Next(evalCtx)
	if !ok {
		nextLowerBound = currentUpperBound
	}
	return nextLowerBound
}

// expectedDistinctCount returns the expected number of distinct values
// among k random numbers selected from n possible values. We assume the
// values are chosen using uniform random sampling with replacement.
func expectedDistinctCount(k, n float64) float64 {
	if n == 0 || k == 0 {
		return 0
	}
	// The probability that one specific value (out of the n possible values)
	// does not appear in any of the k selections is:
	//
	//         ⎛ n-1 ⎞ k
	//     p = ⎜-----⎟
	//         ⎝  n  ⎠
	//
	// Therefore, the probability that a specific value appears at least once is
	// 1-p. Over all n values, the expected number that appear at least once is
	// n * (1-p). In other words, the expected distinct count is:
	//
	//                             ⎛     ⎛ n-1 ⎞ k ⎞
	//     E[distinct count] = n * ⎜ 1 - ⎜-----⎟   ⎟
	//                             ⎝     ⎝  n  ⎠   ⎠
	//
	// See https://math.stackexchange.com/questions/72223/finding-expected-
	//   number-of-distinct-values-selected-from-a-set-of-integers for more info.
	count := n * (1 - math.Pow((n-1)/n, k))

	// It's possible that if n is very large, floating point precision errors
	// will cause count to be 0. In that case, just return min(n, k).
	if count == 0 {
		count = k
		if n < k {
			count = n
		}
	}
	return count
}
