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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
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

	// It's possible that distinctCount was calculated to be greater than numRows
	// due to the approximate nature of HyperLogLog. If this is the case, set it
	// equal to numRows.
	if distinctCount > numRows {
		distinctCount = numRows
	}

	sort.Slice(samples, func(i, j int) bool {
		return samples[i].Compare(evalCtx, samples[j]) < 0
	})
	numBuckets := maxBuckets
	if maxBuckets > numSamples {
		numBuckets = numSamples
	}
	h := make(histogram, 0, numBuckets)
	lowerBound := samples[0]

	// Determine the scale factor that will be used to estimate the number of rows
	// equal to each bucket upper bound (numEq) given the number of samples equal
	// to the upper bound (sampEq) as follows:
	//
	//   numEq = sampEq * (scale factor)
	//
	// The scale factor is normally calculated as (numRows / numSamples), but if
	// the distinct count for the column is larger than the sample size, we must
	// divide by distinctCount instead of numSamples to avoid artificially
	// inflating the estimate for numEq.
	//
	// To understand the need for this distinction, consider the case of a unique
	// key column. In this case, there can be at most one row equal to each upper
	// bound, so if numRows >> numSamples, the estimate for numEq would be way too
	// high. Using a scale factor of (numRows / distinctCount) gives the correct
	// estimate in this case.
	// TODO(rytaft): There's probably a more sophisticated formula for this that
	// takes into account the probability of selecting sampEq samples without
	// replacement matching the upper bound given numSamples and distinctCount...
	var scaleFactorNumEq float64
	if distinctCount < int64(numSamples) {
		scaleFactorNumEq = float64(numRows) / float64(numSamples)
	} else {
		scaleFactorNumEq = float64(numRows) / float64(distinctCount)
	}

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

		// Use the scale factor calculated above to estimate the number of rows
		// equal to the upper bound.
		sampEq := float64(num - numLess)
		numEq := sampEq * scaleFactorNumEq

		// Estimate the total number of rows represented by the bucket as well as
		// the number of distinct values in the bucket. If there are no samples less
		// than the upper bound, then the bucket is fully specified by numEq.
		numBucket := numEq
		distinctBucket := float64(1)
		if numLess > 0 {
			numBucket = float64(num) * float64(numRows) / float64(numSamples)
			distinctBucket = estimatedDistinctValuesInBucket(evalCtx, numBucket, lowerBound, upper)
		}

		// Calculate numRange and distinctRange based on the total and distinct
		// counts for the bucket and the upper bound.
		numRange := numBucket - numEq
		distinctRange := distinctBucket - 1

		i += num
		h = append(h, cat.HistogramBucket{
			NumEq:         numEq,
			NumRange:      numRange,
			DistinctRange: distinctRange,
			UpperBound:    upper,
		})

		lowerBound = getNextLowerBound(evalCtx, upper)
	}

	h.adjustCounts(float64(numRows), float64(distinctCount))
	return h.toHistogramData(colType)
}

type histogram []cat.HistogramBucket

// adjustCounts adjusts the row count and number of distinct values per bucket
// based on the total row count and distinct count.
func (h histogram) adjustCounts(rowCountTotal, distinctCountTotal float64) {
	// Calculate the current state of the histogram so we can adjust it as needed.
	// The number of rows and distinct values represented by the histogram should
	// be adjusted so they equal rowCountTotal and distinctCountTotal.
	var rowCountRange, rowCountEq float64
	var distinctCountRange, distinctCountEq float64
	for i := range h {
		rowCountRange += h[i].NumRange
		rowCountEq += h[i].NumEq
		distinctCountRange += h[i].DistinctRange
		if h[i].NumEq > 0 {
			distinctCountEq++
		}
	}

	// If one or more of the counts show that the ranges are empty and therefore
	// the upper bounds account for all values in the histogram, make the
	// histogram consistent by clearing the ranges and adjusting the NumEq values
	// to add up to the row count.
	if rowCountRange == 0 || distinctCountRange == 0 ||
		distinctCountEq >= distinctCountTotal || rowCountEq >= rowCountTotal {
		adjustmentFactorNumEq := rowCountTotal / rowCountEq
		for i := range h {
			h[i].NumRange = 0
			h[i].DistinctRange = 0
			h[i].NumEq *= adjustmentFactorNumEq
		}
		return
	}

	// If the ranges are not empty, adjust the values in the ranges so the row
	// counts and distinct counts add up correctly.
	adjustmentFactorDistinctRange := (distinctCountTotal - distinctCountEq) / distinctCountRange
	adjustmentFactorNumRange := (rowCountTotal - rowCountEq) / rowCountRange
	for i := range h {
		h[i].DistinctRange *= adjustmentFactorDistinctRange
		h[i].NumRange *= adjustmentFactorNumRange
	}
}

// toHistogramData converts a histogram to a HistogramData protobuf with the
// given type.
func (h histogram) toHistogramData(colType *types.T) (HistogramData, error) {
	histogramData := HistogramData{
		Buckets:    make([]HistogramData_Bucket, len(h)),
		ColumnType: colType,
	}

	for i := range h {
		encoded, err := rowenc.EncodeTableKey(nil, h[i].UpperBound, encoding.Ascending)
		if err != nil {
			return HistogramData{}, err
		}

		histogramData.Buckets[i] = HistogramData_Bucket{
			NumEq:         int64(h[i].NumEq),
			NumRange:      int64(h[i].NumRange),
			DistinctRange: h[i].DistinctRange,
			UpperBound:    encoded,
		}
	}

	return histogramData, nil
}

// estimatedDistinctValuesInBucket returns the estimated number of distinct
// values in the bucket with bounds [lowerBound, upperBound], given that the total number
// of values is numBucket.
func estimatedDistinctValuesInBucket(
	evalCtx *tree.EvalContext, numBucket float64, lowerBound, upperBound tree.Datum,
) float64 {
	if maxDistinct, ok := tree.MaxDistinctCount(evalCtx, lowerBound, upperBound); ok {
		return expectedDistinctCount(numBucket, float64(maxDistinct))
	}
	return numBucket
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
