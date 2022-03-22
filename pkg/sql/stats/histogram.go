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
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// DefaultHistogramBuckets is the maximum number of histogram buckets to build
// when creating statistics.
const DefaultHistogramBuckets = 200

// HistogramClusterMode controls the cluster setting for enabling
// histogram collection.
var HistogramClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.stats.histogram_collection.enabled",
	"histogram collection mode",
	true,
).WithPublic()

// HistogramVersion identifies histogram versions.
type HistogramVersion uint32

// histVersion is the current histogram version.
//
// ATTENTION: When updating this field, add a brief description of what
// changed to the version history below.
const histVersion HistogramVersion = 1

/*

**  VERSION HISTORY **

Please add new entries at the top.

- Version: 1
- The histogram creation logic was changed so the number of distinct values in
  the histogram matched the estimated distinct count from the HyperLogLog sketch.

- Version: 0
- Histogram implementations up to and including 21.1.x. The version field is
  omitted on Version 0 histograms.

*/

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
//
// In addition to returning the encoded histogram (HistogramData), it also
// returns the unencoded histogram buckets ([]cat.HistogramBucket) when
// HistogramData.HistogramData_Bucket is non-nil, otherwise a nil
// []cat.HistogramBucket.
func EquiDepthHistogram(
	evalCtx *tree.EvalContext,
	colType *types.T,
	samples tree.Datums,
	numRows, distinctCount int64,
	maxBuckets int,
) (HistogramData, []cat.HistogramBucket, error) {
	numSamples := len(samples)
	if numSamples == 0 {
		return HistogramData{ColumnType: colType}, nil, nil
	}
	if maxBuckets < 2 {
		return HistogramData{}, nil, errors.Errorf("histogram requires at least two buckets")
	}
	if numRows < int64(numSamples) {
		return HistogramData{}, nil, errors.Errorf("more samples than rows")
	}
	if distinctCount == 0 {
		return HistogramData{}, nil, errors.Errorf("histogram requires distinctCount > 0")
	}
	for _, d := range samples {
		if d == tree.DNull {
			return HistogramData{}, nil, errors.Errorf("NULL values not allowed in histogram")
		}
	}

	sort.Slice(samples, func(i, j int) bool {
		return samples[i].Compare(evalCtx, samples[j]) < 0
	})
	numBuckets := maxBuckets
	if maxBuckets > numSamples {
		numBuckets = numSamples
	}
	h := histogram{buckets: make([]cat.HistogramBucket, 0, numBuckets)}
	lowerBound := samples[0]

	// i keeps track of the current sample and advances as we form buckets.
	for i, b := 0, 0; b < numBuckets && i < numSamples; b++ {
		// numSamplesInBucket is the number of samples in this bucket. The first
		// bucket has numSamplesInBucket=1 so the histogram has a clear lower bound.
		numSamplesInBucket := (numSamples - i) / (numBuckets - b)
		if i == 0 || numSamplesInBucket < 1 {
			numSamplesInBucket = 1
		}
		upper := samples[i+numSamplesInBucket-1]
		// numLess is the number of samples less than upper (in this bucket).
		numLess := 0
		for ; numLess < numSamplesInBucket-1; numLess++ {
			if c := samples[i+numLess].Compare(evalCtx, upper); c == 0 {
				break
			} else if c > 0 {
				return HistogramData{}, nil, errors.AssertionFailedf("%+v", "samples not sorted")
			}
		}
		// Advance the boundary of the bucket to cover all samples equal to upper.
		for ; i+numSamplesInBucket < numSamples; numSamplesInBucket++ {
			if samples[i+numSamplesInBucket].Compare(evalCtx, upper) != 0 {
				break
			}
		}

		// Estimate the number of rows equal to the upper bound and less than the
		// upper bound, as well as the number of distinct values less than the upper
		// bound. These estimates may be adjusted later based on the total distinct
		// count.
		numEq := float64(numSamplesInBucket-numLess) * float64(numRows) / float64(numSamples)
		numRange := float64(numLess) * float64(numRows) / float64(numSamples)
		distinctRange := estimatedDistinctValuesInRange(evalCtx, numRange, lowerBound, upper)

		i += numSamplesInBucket
		h.buckets = append(h.buckets, cat.HistogramBucket{
			NumEq:         numEq,
			NumRange:      numRange,
			DistinctRange: distinctRange,
			UpperBound:    upper,
		})

		lowerBound = getNextLowerBound(evalCtx, upper)
	}

	h.adjustCounts(evalCtx, float64(numRows), float64(distinctCount))
	histogramData, err := h.toHistogramData(colType)
	return histogramData, h.buckets, err
}

type histogram struct {
	buckets []cat.HistogramBucket
}

// adjustCounts adjusts the row count and number of distinct values per bucket
// based on the total row count and estimated distinct count.
func (h *histogram) adjustCounts(
	evalCtx *tree.EvalContext, rowCountTotal, distinctCountTotal float64,
) {
	// Calculate the current state of the histogram so we can adjust it as needed.
	// The number of rows and distinct values represented by the histogram should
	// be adjusted so they equal rowCountTotal and distinctCountTotal.
	var rowCountRange, rowCountEq float64
	// Total distinct count for values strictly inside bucket boundaries.
	var distinctCountRange float64
	// Number of bucket boundaries with at least one row on the boundary.
	var distinctCountEq float64
	for i := range h.buckets {
		rowCountRange += h.buckets[i].NumRange
		rowCountEq += h.buckets[i].NumEq
		distinctCountRange += h.buckets[i].DistinctRange
		if h.buckets[i].NumEq > 0 {
			distinctCountEq++
		}
	}

	if rowCountEq <= 0 {
		panic(errors.AssertionFailedf("expected a positive value for rowCountEq"))
	}

	// If the upper bounds account for all distinct values (as estimated by the
	// sketch), make the histogram consistent by clearing the ranges and adjusting
	// the NumEq values to add up to the row count.
	if distinctCountEq >= distinctCountTotal {
		adjustmentFactorNumEq := rowCountTotal / rowCountEq
		for i := range h.buckets {
			h.buckets[i].NumRange = 0
			h.buckets[i].DistinctRange = 0
			h.buckets[i].NumEq *= adjustmentFactorNumEq
		}
		return
	}

	// The upper bounds do not account for all distinct values, so adjust the
	// NumEq values if needed so they add up to less than the row count.
	remDistinctCount := distinctCountTotal - distinctCountEq
	if rowCountEq+remDistinctCount >= rowCountTotal {
		targetRowCountEq := rowCountTotal - remDistinctCount
		adjustmentFactorNumEq := targetRowCountEq / rowCountEq
		for i := range h.buckets {
			h.buckets[i].NumEq *= adjustmentFactorNumEq
		}
		rowCountEq = targetRowCountEq
	}

	// If the ranges do not account for the remaining distinct values, increment
	// them so they add up to the remaining distinct count.
	if remDistinctCount > distinctCountRange {
		remDistinctCount -= distinctCountRange

		// Calculate the maximum possible number of distinct values that can be
		// added to the histogram.
		maxDistinctCountRange := float64(math.MaxInt64)
		lowerBound := h.buckets[0].UpperBound
		upperBound := h.buckets[len(h.buckets)-1].UpperBound
		if maxDistinct, ok := tree.MaxDistinctCount(evalCtx, lowerBound, upperBound); ok {
			// Subtract distinctCountEq to account for the upper bounds of the
			// buckets, along with the current range distinct count which has already
			// been accounted for.
			maxDistinctCountRange = float64(maxDistinct) - distinctCountEq - distinctCountRange
		}

		// Add distinct values into the histogram if there is space. Increment the
		// distinct count of each bucket except the first one.
		if maxDistinctCountRange > 0 {
			if remDistinctCount > maxDistinctCountRange {
				// There isn't enough space in the entire histogram for these distinct
				// values. Add what we can now, and we will add extra buckets below.
				remDistinctCount = maxDistinctCountRange
			}
			avgRemPerBucket := remDistinctCount / float64(len(h.buckets)-1)
			for i := 1; i < len(h.buckets); i++ {
				lowerBound := h.buckets[i-1].UpperBound
				upperBound := h.buckets[i].UpperBound
				maxDistRange, countable := maxDistinctRange(evalCtx, lowerBound, upperBound)

				inc := avgRemPerBucket
				if countable {
					maxDistRange -= h.buckets[i].DistinctRange

					// Set the increment proportional to the remaining number of
					// distinct values in the bucket.
					inc = remDistinctCount * (maxDistRange / maxDistinctCountRange)
				}

				h.buckets[i].NumRange += inc
				h.buckets[i].DistinctRange += inc
				rowCountRange += inc
				distinctCountRange += inc
			}
		}
	}

	// If there are still some distinct values that are unaccounted for, this is
	// probably because the samples did not cover the full domain of possible
	// values. Add buckets above and below the existing buckets to contain these
	// values.
	remDistinctCount = distinctCountTotal - distinctCountRange - distinctCountEq
	if remDistinctCount > 0 {
		h.addOuterBuckets(
			evalCtx, remDistinctCount, &rowCountEq, &distinctCountEq, &rowCountRange, &distinctCountRange,
		)
	}

	// Adjust the values so the row counts and distinct counts add up correctly.
	adjustmentFactorDistinctRange := float64(1)
	if distinctCountRange > 0 {
		adjustmentFactorDistinctRange = (distinctCountTotal - distinctCountEq) / distinctCountRange
	}
	adjustmentFactorRowCount := rowCountTotal / (rowCountRange + rowCountEq)
	for i := range h.buckets {
		h.buckets[i].DistinctRange *= adjustmentFactorDistinctRange
		h.buckets[i].NumRange *= adjustmentFactorRowCount
		h.buckets[i].NumEq *= adjustmentFactorRowCount
	}
}

// addOuterBuckets adds buckets above and below the existing buckets in the
// histogram to include the remaining distinct values in remDistinctCount. It
// also increments the counters rowCountEq, distinctCountEq, rowCountRange, and
// distinctCountRange as needed.
func (h *histogram) addOuterBuckets(
	evalCtx *tree.EvalContext,
	remDistinctCount float64,
	rowCountEq, distinctCountEq, rowCountRange, distinctCountRange *float64,
) {
	var maxDistinctCountExtraBuckets float64
	var addedMin, addedMax bool
	var newBuckets int
	if !h.buckets[0].UpperBound.IsMin(evalCtx) {
		if minVal, ok := h.buckets[0].UpperBound.Min(evalCtx); ok {
			lowerBound := minVal
			upperBound := h.buckets[0].UpperBound
			maxDistRange, _ := maxDistinctRange(evalCtx, lowerBound, upperBound)
			maxDistinctCountExtraBuckets += maxDistRange
			h.buckets = append([]cat.HistogramBucket{{UpperBound: minVal}}, h.buckets...)
			addedMin = true
			newBuckets++
		}
	}
	if !h.buckets[len(h.buckets)-1].UpperBound.IsMax(evalCtx) {
		if maxVal, ok := h.buckets[len(h.buckets)-1].UpperBound.Max(evalCtx); ok {
			lowerBound := h.buckets[len(h.buckets)-1].UpperBound
			upperBound := maxVal
			maxDistRange, _ := maxDistinctRange(evalCtx, lowerBound, upperBound)
			maxDistinctCountExtraBuckets += maxDistRange
			h.buckets = append(h.buckets, cat.HistogramBucket{UpperBound: maxVal})
			addedMax = true
			newBuckets++
		}
	}

	if newBuckets == 0 {
		// No new buckets added.
		return
	}

	// If this is an enum or bool histogram, increment numEq for the upper
	// bounds.
	if typFam := h.buckets[0].UpperBound.ResolvedType().Family(); typFam == types.EnumFamily ||
		typFam == types.BoolFamily {
		if addedMin {
			h.buckets[0].NumEq++
		}
		if addedMax {
			h.buckets[len(h.buckets)-1].NumEq++
		}
		*rowCountEq += float64(newBuckets)
		*distinctCountEq += float64(newBuckets)
		remDistinctCount -= float64(newBuckets)
	}

	if remDistinctCount <= 0 {
		// All distinct values accounted for.
		return
	}

	// Account for the remaining values in the new bucket ranges.
	bucIndexes := make([]int, 0, newBuckets)
	if addedMin {
		// We'll be incrementing the range of the second bucket.
		bucIndexes = append(bucIndexes, 1)
	}
	if addedMax {
		bucIndexes = append(bucIndexes, len(h.buckets)-1)
	}
	avgRemPerBucket := remDistinctCount / float64(newBuckets)
	for _, i := range bucIndexes {
		lowerBound := h.buckets[i-1].UpperBound
		upperBound := h.buckets[i].UpperBound
		maxDistRange, countable := maxDistinctRange(evalCtx, lowerBound, upperBound)

		inc := avgRemPerBucket
		if countable && h.buckets[0].UpperBound.ResolvedType().Family() == types.EnumFamily {
			// Set the increment proportional to the remaining number of
			// distinct values in the bucket. This only really matters for
			// enums.
			inc = remDistinctCount * (maxDistRange / maxDistinctCountExtraBuckets)
		}

		h.buckets[i].NumRange += inc
		h.buckets[i].DistinctRange += inc
		*rowCountRange += inc
		*distinctCountRange += inc
	}
}

// toHistogramData converts a histogram to a HistogramData protobuf with the
// given type.
func (h histogram) toHistogramData(colType *types.T) (HistogramData, error) {
	histogramData := HistogramData{
		Buckets:    make([]HistogramData_Bucket, len(h.buckets)),
		ColumnType: colType,
		Version:    histVersion,
	}

	for i := range h.buckets {
		encoded, err := keyside.Encode(nil, h.buckets[i].UpperBound, encoding.Ascending)
		if err != nil {
			return HistogramData{}, err
		}

		histogramData.Buckets[i] = HistogramData_Bucket{
			NumEq:         int64(math.Round(h.buckets[i].NumEq)),
			NumRange:      int64(math.Round(h.buckets[i].NumRange)),
			DistinctRange: h.buckets[i].DistinctRange,
			UpperBound:    encoded,
		}
	}

	return histogramData, nil
}

// estimatedDistinctValuesInRange returns the estimated number of distinct
// values in the range [lowerBound, upperBound), given that the total number
// of values is numRange.
//
// If lowerBound and upperBound are not countable, the distinct count is just
// equal to numRange. If they are countable, we can estimate the distinct count
// based on the total number of distinct values in the range.
func estimatedDistinctValuesInRange(
	evalCtx *tree.EvalContext, numRange float64, lowerBound, upperBound tree.Datum,
) float64 {
	if numRange == 0 {
		return 0
	}
	rangeUpperBound, ok := upperBound.Prev(evalCtx)
	if !ok {
		rangeUpperBound = upperBound
	}
	if maxDistinct, ok := tree.MaxDistinctCount(evalCtx, lowerBound, rangeUpperBound); ok {
		return expectedDistinctCount(numRange, float64(maxDistinct))
	}
	return numRange
}

func getNextLowerBound(evalCtx *tree.EvalContext, currentUpperBound tree.Datum) tree.Datum {
	nextLowerBound, ok := currentUpperBound.Next(evalCtx)
	if !ok {
		nextLowerBound = currentUpperBound
	}
	return nextLowerBound
}

// maxDistinctRange returns the maximum number of distinct values in the given
// range, excluding both lowerBound and upperBound. Returns countable=true if
// the returned value is countable.
func maxDistinctRange(
	evalCtx *tree.EvalContext, lowerBound, upperBound tree.Datum,
) (_ float64, countable bool) {
	if maxDistinct, ok := tree.MaxDistinctCount(evalCtx, lowerBound, upperBound); ok {
		// Remove 2 for the upper and lower boundaries.
		if maxDistinct < 2 {
			return 0, true
		}
		return float64(maxDistinct - 2), true
	}
	return float64(math.MaxInt64), false
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
