// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/container/heap"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// DefaultHistogramBuckets is the maximum number of histogram buckets to build
// when creating statistics.
var DefaultHistogramBuckets = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.stats.histogram_buckets.count",
	"maximum number of histogram buckets to build during table statistics collection",
	200,
	settings.NonNegativeIntWithMaximum(math.MaxUint32),
	settings.WithPublic)

// HistogramClusterMode controls the cluster setting for enabling
// histogram collection.
var HistogramClusterMode = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.stats.histogram_collection.enabled",
	"histogram collection mode",
	true,
	settings.WithPublic)

// HistogramMCVsClusterMode controls the cluster setting for enabling
// inclusion of the most common values as buckets in the histogram.
var HistogramMCVsClusterMode = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.stats.histogram_buckets.include_most_common_values.enabled",
	"whether to include most common values as histogram buckets",
	true,
	settings.WithPublic)

// MaxFractionHistogramMCVs controls the cluster setting for the maximum
// fraction of buckets in a histogram to use for tracking most common values.
// This setting only matters if HistogramMCVsClusterMode is set to true.
var MaxFractionHistogramMCVs = settings.RegisterFloatSetting(
	settings.ApplicationLevel,
	"sql.stats.histogram_buckets.max_fraction_most_common_values",
	"maximum fraction of histogram buckets to use for most common values",
	0.1,
	settings.NonNegativeFloatWithMaximum(1),
	settings.WithPublic)

// HistogramVersion identifies histogram versions.
type HistogramVersion uint32

// HistVersion is the current histogram version.
//
// ATTENTION: When updating this field, add a brief description of what
// changed to the version history below and introduce new named constant below.
const HistVersion = upperBoundsValueEncodedVersion

/*

**  VERSION HISTORY **

Please add new entries at the top.

- Version: 3
- Introduced in 24.1.
- We now use value-encoding for UpperBounds in the histograms. For some types
  like collated strings this is necessary for correctness, but it also is
  probably beneficial overall since, during sampling, we get value-encoded
  datums for all columns that are not part of the primary key.

- Version: 2
- Introduced in 22.2.
- String columns indexed by an inverted (trigram) index now have two sets of
  statistics created by each statistics collection: one with the normal STRING
  histogram, and one with the inverted BYTES histogram.

- Version: 1
- Introduced in 21.2.
- The histogram creation logic was changed so the number of distinct values in
  the histogram matched the estimated distinct count from the HyperLogLog sketch.

- Version: 0
- Histogram implementations up to and including 21.1.x. The version field is
  omitted on Version 0 histograms.

*/

// upperBoundsValueEncodedVersion is the HistogramVersion from which we started
// using value-encoding for upper bound datums.
const upperBoundsValueEncodedVersion = HistogramVersion(3)

// EncodeUpperBound encodes the upper-bound datum of a histogram bucket.
func EncodeUpperBound(version HistogramVersion, upperBound tree.Datum) ([]byte, error) {
	if version >= upperBoundsValueEncodedVersion || upperBound.ResolvedType().Family() == types.TSQueryFamily {
		// TSQuery doesn't have key-encoding, so we must use value-encoding.
		return valueside.Encode(nil /* appendTo */, valueside.NoColumnID, upperBound)
	}
	return keyside.Encode(nil /* b */, upperBound, encoding.Ascending)
}

// DecodeUpperBound decodes the upper-bound of a histogram bucket into a datum.
func DecodeUpperBound(
	version HistogramVersion, typ *types.T, a *tree.DatumAlloc, upperBound []byte,
) (tree.Datum, error) {
	var datum tree.Datum
	var err error
	if version >= upperBoundsValueEncodedVersion || typ.Family() == types.TSQueryFamily {
		// TSQuery doesn't have key-encoding, so we must have used
		// value-encoding, regardless of the histogram version.
		datum, _, err = valueside.Decode(a, typ, upperBound)
	} else {
		datum, _, err = keyside.Decode(a, typ, upperBound, encoding.Ascending)
	}
	if err != nil {
		err = errors.Wrapf(
			err, "decoding histogram version %d type %v value %v",
			int(version), typ.Family().Name(), hex.EncodeToString(upperBound),
		)
	}
	return datum, err
}

// GetDefaultHistogramBuckets gets the default number of histogram buckets to
// create for the given table.
func GetDefaultHistogramBuckets(sv *settings.Values, desc catalog.TableDescriptor) uint32 {
	if count, ok := desc.HistogramBucketsCount(); ok {
		return count
	}
	return uint32(DefaultHistogramBuckets.Get(sv))
}

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
	ctx context.Context,
	compareCtx tree.CompareContext,
	colType *types.T,
	samples tree.Datums,
	numRows, distinctCount int64,
	maxBuckets int,
	st *cluster.Settings,
) (HistogramData, []cat.HistogramBucket, error) {

	if len(samples) == 0 {
		return HistogramData{
			ColumnType: colType, Buckets: make([]HistogramData_Bucket, 0), Version: HistVersion,
		}, nil, nil
	}

	if distinctCount == 0 {
		return HistogramData{}, nil, errors.Errorf("histogram requires distinctCount > 0")
	}

	h, err := equiDepthHistogramWithoutAdjustment(ctx, compareCtx, samples, numRows, maxBuckets, st)
	if err != nil {
		return HistogramData{}, nil, err
	}

	h.adjustCounts(ctx, compareCtx, colType, float64(numRows), float64(distinctCount))
	histogramData, err := h.toHistogramData(ctx, colType, st)
	return histogramData, h.buckets, err
}

// ConstructExtremesHistogram returns a histogram that are two equi-depth
// histograms stitched together that are each generated by
// equiDepthHistogramWithoutAdjustment. The first histogram covers the range of
// samples up to but not including the value specified by lowerBound and the
// second histogram covers the values up from lowerBound to the maximum. This
// function assumes that the sample only includes values from the extremes of
// the column.
func ConstructExtremesHistogram(
	ctx context.Context,
	compareCtx tree.CompareContext,
	colType *types.T,
	values tree.Datums,
	numRows, distinctCount int64,
	maxBuckets int,
	lowerBound tree.Datum,
	st *cluster.Settings,
) (HistogramData, []cat.HistogramBucket, error) {

	// If there are no new values at the extremes,
	// we just return an empty partial histogram.
	numTotalSamples := int64(values.Len())
	if numTotalSamples == 0 {
		return HistogramData{
			ColumnType: colType, Buckets: make([]HistogramData_Bucket, 0), Version: HistVersion,
		}, nil, nil
	}

	if maxBuckets < 4 {
		return HistogramData{}, nil, errors.Errorf("partial histogram requires at least four buckets")
	}

	if distinctCount == 0 {
		return HistogramData{}, nil, errors.Errorf("histogram requires distinctCount > 0")
	}

	var lowerSamples tree.Datums
	var upperSamples tree.Datums
	for _, val := range values {
		c, err := val.Compare(ctx, compareCtx, lowerBound)
		if err != nil {
			return HistogramData{}, nil, err
		}
		if c == -1 {
			lowerSamples = append(lowerSamples, val)
		} else {
			upperSamples = append(upperSamples, val)
		}
	}

	estNumRowsLower := (int64(lowerSamples.Len()) * numRows) / numTotalSamples
	estNumRowsUpper := (int64(upperSamples.Len()) * numRows) / numTotalSamples
	var lowerHist histogram
	var upperHist histogram
	var err error
	if len(lowerSamples) > 0 {
		lowerHist, err = equiDepthHistogramWithoutAdjustment(ctx, compareCtx, lowerSamples, estNumRowsLower, maxBuckets/2, st)
		if err != nil {
			return HistogramData{}, nil, err
		}
	}
	if len(upperSamples) > 0 {
		upperHist, err = equiDepthHistogramWithoutAdjustment(ctx, compareCtx, upperSamples, estNumRowsUpper, maxBuckets/2, st)
		if err != nil {
			return HistogramData{}, nil, err
		}
	}
	h := histogram{buckets: append(lowerHist.buckets, upperHist.buckets...)}
	h.adjustCounts(ctx, compareCtx, colType, float64(numRows), float64(distinctCount))
	histogramData, err := h.toHistogramData(ctx, colType, st)
	return histogramData, h.buckets, err
}

// equiDepthHistogramWithoutAdjustment performs the core functionality
// described in the comment for EquiDepthHistogram, except the counts
// for each bucket are not adjusted at the end.
func equiDepthHistogramWithoutAdjustment(
	ctx context.Context,
	compareCtx tree.CompareContext,
	samples tree.Datums,
	numRows int64,
	maxBuckets int,
	st *cluster.Settings,
) (histogram, error) {
	numSamples := len(samples)
	if maxBuckets < 2 {
		return histogram{}, errors.Errorf("histogram requires at least two buckets")
	}
	if numRows < int64(numSamples) {
		return histogram{}, errors.Errorf("more samples than rows")
	}
	for _, d := range samples {
		if d == tree.DNull {
			return histogram{}, errors.Errorf("NULL values not allowed in histogram")
		}
	}

	sort.Slice(samples, func(i, j int) bool {
		cmp, err := samples[i].Compare(ctx, compareCtx, samples[j])
		if err != nil {
			panic(err)
		}
		return cmp < 0
	})
	numBuckets := maxBuckets
	if maxBuckets > numSamples {
		numBuckets = numSamples
	}

	// Find the most common values in the set of samples.
	// mcvs contains the indexes in samples of the last instance of each of the
	// most common values (MCVs), in index order.
	// j keeps track of the current MCV and advances as the MCVs are accounted for.
	var mcvs []int
	j := 0
	if HistogramMCVsClusterMode.Get(&st.SV) {
		maxMCVs := getMaxMCVs(st, numBuckets)
		var err error
		mcvs, err = getMCVs(ctx, compareCtx, samples, maxMCVs)
		if err != nil {
			return histogram{}, err
		}
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
		// Use a MCV as the upper bound if it would otherwise be lost in the bucket.
		// As a result, the bucket may be smaller than the target for an equi-depth
		// histogram, but this ensures we have accurate counts for the heavy hitters.
		if j < len(mcvs) && mcvs[j] < i+numSamplesInBucket-1 {
			numSamplesInBucket = mcvs[j] - i + 1
			j++
			// If this would have been the last bucket, we need to add one more bucket
			// to accommodate the rest of the samples.
			if b == numBuckets-1 {
				numBuckets++
			}
		}
		upper := samples[i+numSamplesInBucket-1]
		// numLess is the number of samples less than upper (in this bucket).
		numLess := 0
		for ; numLess < numSamplesInBucket-1; numLess++ {
			if c, err := samples[i+numLess].Compare(ctx, compareCtx, upper); err != nil {
				return histogram{}, err
			} else if c == 0 {
				break
			} else if c > 0 {
				return histogram{}, errors.AssertionFailedf("%+v", "samples not sorted")
			}
		}
		// Advance the boundary of the bucket to cover all samples equal to upper.
		for ; i+numSamplesInBucket < numSamples; numSamplesInBucket++ {
			if c, err := samples[i+numSamplesInBucket].Compare(ctx, compareCtx, upper); err != nil {
				return histogram{}, err
			} else if c != 0 {
				break
			}
		}
		// If we happened to land on a heavy hitter, advance j to mark the MCV as
		// accounted for.
		if j < len(mcvs) && mcvs[j] == i+numSamplesInBucket-1 {
			j++
		}

		// Estimate the number of rows equal to the upper bound and less than the
		// upper bound, as well as the number of distinct values less than the upper
		// bound. These estimates may be adjusted later based on the total distinct
		// count.
		numEq := float64(numSamplesInBucket-numLess) * float64(numRows) / float64(numSamples)
		numRange := float64(numLess) * float64(numRows) / float64(numSamples)
		distinctRange := estimatedDistinctValuesInRange(ctx, compareCtx, numRange, lowerBound, upper)

		i += numSamplesInBucket
		h.buckets = append(h.buckets, cat.HistogramBucket{
			NumEq:         numEq,
			NumRange:      numRange,
			DistinctRange: distinctRange,
			UpperBound:    upper,
		})

		lowerBound = getNextLowerBound(ctx, compareCtx, upper)
	}

	return h, nil
}

// TS represents a timestamp when stats were created. It is like a type union.
// Only one of s and t should be set. Use TSFromTime or TSFromString to create
// a TS.
type TS struct {
	t time.Time
	s string
}

// TSFromTime creates a TS from a time.Time.
func TSFromTime(t time.Time) TS {
	return TS{t: t}
}

// TSFromString creates a TS from a string.
func TSFromString(s string) TS {
	return TS{s: s}
}

// String formats a TS as a string.
func (ts TS) String() string {
	if ts.s != "" {
		return ts.s
	}
	return string(tree.PGWireFormatTimestamp(ts.t, nil /* offset */, nil /* tmp */))
}

// TypeCheck returns an error if the type of the histogram does not match the
// type of the column.
func (histogramData *HistogramData) TypeCheck(
	colType *types.T, table, column string, createdAt TS,
) error {
	if histogramData == nil || histogramData.ColumnType == nil {
		return nil
	}
	// BYTES histograms could be inverted, so they are exempt.
	if histogramData.ColumnType.Family() == types.BytesFamily {
		return nil
	}
	if !histogramData.ColumnType.Equivalent(colType) {
		return errors.Newf(
			"histogram for table %v column %v created_at %s does not match column type %v: %v",
			table, column, createdAt, colType.SQLStringForError(),
			histogramData.ColumnType.SQLStringForError(),
		)
	}
	return nil
}

// histogram is a decoded HistogramData with datums for upper bounds. We use
// nil buckets for error cases, and non-nil zero-length buckets for histograms
// on empty tables.
type histogram struct {
	buckets []cat.HistogramBucket
}

// adjustCounts adjusts the row count and number of distinct values per bucket
// to equal the total row count and estimated distinct count. The total row
// count and estimated distinct count should not include NULL values, and the
// histogram should not contain any buckets for NULL values.
func (h *histogram) adjustCounts(
	ctx context.Context,
	compareCtx tree.CompareContext,
	colType *types.T,
	rowCountTotal, distinctCountTotal float64,
) {
	// Empty table cases.
	if rowCountTotal <= 0 || distinctCountTotal <= 0 {
		h.buckets = make([]cat.HistogramBucket, 0)
		return
	}

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

	// If the histogram only had empty buckets, we can't adjust it.
	if rowCountRange+rowCountEq <= 0 || distinctCountRange+distinctCountEq <= 0 {
		h.buckets = make([]cat.HistogramBucket, 0)
		return
	}

	// If the upper bounds account for all distinct values (as estimated by the
	// sketch), make the histogram consistent by clearing the ranges and adjusting
	// the NumEq values to add up to the row count. This might be the case for
	// low-cardinality types like BOOL and ENUM or other low-cardinality data.
	if distinctCountEq >= distinctCountTotal {
		adjustmentFactorNumEq := rowCountTotal / rowCountEq
		for i := range h.buckets {
			h.buckets[i].NumRange = 0
			h.buckets[i].DistinctRange = 0
			h.buckets[i].NumEq *= adjustmentFactorNumEq
		}
		h.clampNonNegative()
		h.removeZeroBuckets()
		return
	}

	// The upper bounds do not account for all distinct values, so adjust the
	// NumEq values if needed so they add up to less than the row count.
	remDistinctCount := distinctCountTotal - distinctCountEq
	if rowCountEq > 0 && rowCountEq+remDistinctCount > rowCountTotal {
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
		if maxDistinct, ok := tree.MaxDistinctCount(ctx, compareCtx, lowerBound, upperBound); ok {
			// Subtract number of buckets to account for the upper bounds of the
			// buckets, along with the current range distinct count which has already
			// been accounted for.
			maxDistinctCountRange = float64(maxDistinct) - float64(len(h.buckets)) - distinctCountRange
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
				maxDistRange, countable := maxDistinctRange(ctx, compareCtx, lowerBound, upperBound)

				inc := avgRemPerBucket
				if countable {
					maxDistRange -= h.buckets[i].DistinctRange

					// Set the increment proportional to the remaining number of distinct
					// values in the bucket.
					inc = remDistinctCount * (maxDistRange / maxDistinctCountRange)
					// If the bucket has DistinctRange > maxDistRange (a rare but possible
					// occurence, see #93892) then inc will be negative. Prevent this.
					if inc < 0 {
						inc = 0
					}
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
			ctx, compareCtx, colType, remDistinctCount, &rowCountEq, &distinctCountEq, &rowCountRange, &distinctCountRange,
		)
	}

	// At this point rowCountRange + rowCountEq >= distinctCountTotal but not
	// necessarily rowCountTotal, so we've accounted for all distinct values, and
	// any additional rows we add will be duplicate values. We can spread the
	// final adjustment proportionately across both NumRange and NumEq.
	adjustmentFactorDistinctRange := float64(1)
	if distinctCountRange > 0 {
		adjustmentFactorDistinctRange = (distinctCountTotal - distinctCountEq) / distinctCountRange
	}
	adjustmentFactorRowCount := rowCountTotal / (rowCountRange + rowCountEq)
	// TODO(michae2): Consider moving this section above the sections adjusting
	// NumEq and NumRange for distinct counts. This would help the adjustments be
	// less surprising in some cases.
	for i := range h.buckets {
		h.buckets[i].DistinctRange *= adjustmentFactorDistinctRange
		h.buckets[i].NumRange *= adjustmentFactorRowCount
		h.buckets[i].NumEq *= adjustmentFactorRowCount
	}

	h.clampNonNegative()
	h.removeZeroBuckets()
}

// clampNonNegative sets any negative counts to zero.
func (h *histogram) clampNonNegative() {
	for i := 0; i < len(h.buckets); i++ {
		if h.buckets[i].NumEq < 0 {
			h.buckets[i].NumEq = 0
		}
		if h.buckets[i].NumRange < 0 {
			h.buckets[i].NumRange = 0
		}
		if h.buckets[i].DistinctRange < 0 {
			h.buckets[i].DistinctRange = 0
		}
	}
}

// removeZeroBuckets removes any extra zero buckets if we don't need them
// (sometimes we need zero buckets as the lower bound of a range).
func (h *histogram) removeZeroBuckets() {
	if h.buckets == nil {
		return
	}

	var j int
	for i := 0; i < len(h.buckets); i++ {
		if h.buckets[i].NumEq == 0 && h.buckets[i].NumRange == 0 && h.buckets[i].DistinctRange == 0 &&
			(i == len(h.buckets)-1 || h.buckets[i+1].NumRange == 0 && h.buckets[i+1].DistinctRange == 0) {
			continue
		}
		if j != i {
			h.buckets[j] = h.buckets[i]
		}
		j++
	}
	h.buckets = h.buckets[:j]
}

// getMinVal returns the minimum value for the minimum "outer" bucket if the
// value exists. The boolean indicates whether it exists and the bucket needs to
// be created.
func getMinVal(
	ctx context.Context, upperBound tree.Datum, t *types.T, compareCtx tree.CompareContext,
) (tree.Datum, bool) {
	if t.Family() == types.IntFamily {
		// INT2 and INT4 require special handling.
		// TODO(yuzefovich): other types might need it too, but it's less
		// pressing to fix that.
		bound, ok := upperBound.(*tree.DInt)
		if !ok {
			// This shouldn't happen, but we want to be defensive.
			return nil, false
		}
		i := int64(*bound)
		switch t.Width() {
		case 16:
			if i <= math.MinInt16 { // use inequality to be conservative
				return nil, false
			}
			return tree.NewDInt(tree.DInt(math.MinInt16)), true
		case 32:
			if i <= math.MinInt32 { // use inequality to be conservative
				return nil, false
			}
			return tree.NewDInt(tree.DInt(math.MinInt32)), true
		}
	}
	if upperBound.IsMin(ctx, compareCtx) {
		return nil, false
	}
	return upperBound.Min(ctx, compareCtx)
}

// getMaxVal returns the maximum value for the maximum "outer" bucket if the
// value exists. The boolean indicates whether it exists and the bucket needs to
// be created.
func getMaxVal(
	ctx context.Context, upperBound tree.Datum, t *types.T, compareCtx tree.CompareContext,
) (tree.Datum, bool) {
	if t.Family() == types.IntFamily {
		// INT2 and INT4 require special handling.
		// TODO(yuzefovich): other types might need it too, but it's less
		// pressing to fix that.
		bound, ok := upperBound.(*tree.DInt)
		if !ok {
			// This shouldn't happen, but we want to be defensive.
			return nil, false
		}
		i := int64(*bound)
		switch t.Width() {
		case 16:
			if i >= math.MaxInt16 { // use inequality to be conservative
				return nil, false
			}
			return tree.NewDInt(tree.DInt(math.MaxInt16)), true
		case 32:
			if i >= math.MaxInt32 { // use inequality to be conservative
				return nil, false
			}
			return tree.NewDInt(tree.DInt(math.MaxInt32)), true
		}
	}
	if upperBound.IsMax(ctx, compareCtx) {
		return nil, false
	}
	return upperBound.Max(ctx, compareCtx)
}

// addOuterBuckets adds buckets above and below the existing buckets in the
// histogram to include the remaining distinct values in remDistinctCount. It
// also increments the counters rowCountEq, distinctCountEq, rowCountRange, and
// distinctCountRange as needed.
func (h *histogram) addOuterBuckets(
	ctx context.Context,
	compareCtx tree.CompareContext,
	colType *types.T,
	remDistinctCount float64,
	rowCountEq, distinctCountEq, rowCountRange, distinctCountRange *float64,
) {
	var maxDistinctCountExtraBuckets float64
	var addedMin, addedMax bool
	var newBuckets int
	if minVal, ok := getMinVal(ctx, h.buckets[0].UpperBound, colType, compareCtx); ok {
		lowerBound := minVal
		upperBound := h.buckets[0].UpperBound
		maxDistRange, _ := maxDistinctRange(ctx, compareCtx, lowerBound, upperBound)
		maxDistinctCountExtraBuckets += maxDistRange
		h.buckets = append([]cat.HistogramBucket{{UpperBound: minVal}}, h.buckets...)
		addedMin = true
		newBuckets++
	}
	if maxVal, ok := getMaxVal(ctx, h.buckets[len(h.buckets)-1].UpperBound, colType, compareCtx); ok {
		lowerBound := h.buckets[len(h.buckets)-1].UpperBound
		upperBound := maxVal
		maxDistRange, _ := maxDistinctRange(ctx, compareCtx, lowerBound, upperBound)
		maxDistinctCountExtraBuckets += maxDistRange
		h.buckets = append(h.buckets, cat.HistogramBucket{UpperBound: maxVal})
		addedMax = true
		newBuckets++
	}

	if newBuckets == 0 {
		// No new buckets added.
		return
	}

	// If this is an enum or bool histogram, increment numEq for the upper
	// bounds.
	if typFam := colType.Family(); typFam == types.EnumFamily || typFam == types.BoolFamily {
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
		maxDistRange, countable := maxDistinctRange(ctx, compareCtx, lowerBound, upperBound)

		inc := avgRemPerBucket
		if countable {
			// Set the increment proportional to the remaining number of distinct
			// values in the bucket.
			inc = remDistinctCount * (maxDistRange / maxDistinctCountExtraBuckets)
			if inc < 0 {
				inc = 0
			}
		}

		h.buckets[i].NumRange += inc
		h.buckets[i].DistinctRange += inc
		*rowCountRange += inc
		*distinctCountRange += inc
	}
}

// getMaxMCVs returns the maximum number of most common values.
// Postgres uses a more complex formula to determine the number of MCVs,
// (see https://github.com/postgres/postgres/blob/REL_17_STABLE/src/backend/commands/analyze.c#L2934)
// but start simple for now with just a fraction of the buckets defined
// by MaxFractionHistogramMCVs.
func getMaxMCVs(st *cluster.Settings, maxBuckets int) int {
	maxFraction := MaxFractionHistogramMCVs.Get(&st.SV)
	return int(float64(maxBuckets) * maxFraction)
}

// getMCVs returns the indexes in samples of the last instance of each of the
// most common values, in index order. For example, if samples contains
// [ a, a, a, b, c, c ], and maxMCVs is 2, getMCVs returns [ 2, 5 ].
func getMCVs(
	ctx context.Context, compareCtx tree.CompareContext, samples tree.Datums, maxMCVs int,
) ([]int, error) {
	if len(samples) == 0 {
		return nil, errors.AssertionFailedf("empty samples passed to getMCVs")
	}

	// Use a heap to find the most common values.
	h := make(MCVHeap, 0, maxMCVs+1)
	heap.Init[MCV](&h)
	count := 1
	distinctValues := 0
	for i := 1; i < len(samples); i++ {
		if c, err := samples[i].Compare(ctx, compareCtx, samples[i-1]); err != nil {
			return nil, err
		} else if c < 0 {
			return nil, errors.AssertionFailedf("%+v", "samples not sorted")
		} else if c > 0 {
			heap.Push[MCV](&h, MCV{
				idx:   i - 1,
				count: count,
			})
			if len(h) > maxMCVs {
				heap.Pop[MCV](&h)
			}
			count = 1
			distinctValues++
		} else {
			count++
		}
	}
	// Add the last value.
	heap.Push[MCV](&h, MCV{
		idx:   len(samples) - 1,
		count: count,
	})
	if len(h) > maxMCVs {
		heap.Pop[MCV](&h)
	}
	distinctValues++

	// Only keep the values that are actually common. If the frequency of any
	// value is less than or equal to the average sample frequency, remove it.
	expectedCount := len(samples) / distinctValues
	for len(h) > 0 && h[0].count <= expectedCount {
		heap.Pop[MCV](&h)
	}

	// Return just the indexes in increasing order.
	mcvs := make([]int, 0, len(h))
	for i := range h {
		mcvs = append(mcvs, h[i].idx)
	}
	sort.Ints(mcvs)
	return mcvs, nil
}

// toHistogramData converts a histogram to a HistogramData protobuf with the
// given type.
func (h histogram) toHistogramData(
	ctx context.Context, colType *types.T, st *cluster.Settings,
) (HistogramData, error) {
	version := HistVersion
	histogramData := HistogramData{
		Buckets:    make([]HistogramData_Bucket, len(h.buckets)),
		ColumnType: colType,
		Version:    version,
	}

	for i := range h.buckets {
		encoded, err := EncodeUpperBound(version, h.buckets[i].UpperBound)
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

// String prints a histogram to a string.
func (h histogram) String() string {
	var b strings.Builder
	b.WriteString("{[")
	for i, bucket := range h.buckets {
		if i > 0 {
			b.WriteRune(' ')
		}
		fmt.Fprintf(
			&b, "{%v %v %v %v}",
			bucket.NumEq, bucket.NumRange, bucket.DistinctRange, bucket.UpperBound.String(),
		)
	}
	b.WriteString("]}")
	return b.String()
}

// estimatedDistinctValuesInRange returns the estimated number of distinct
// values in the range [lowerBound, upperBound), given that the total number
// of values is numRange.
//
// If lowerBound and upperBound are not countable, the distinct count is just
// equal to numRange. If they are countable, we can estimate the distinct count
// based on the total number of distinct values in the range.
func estimatedDistinctValuesInRange(
	ctx context.Context,
	compareCtx tree.CompareContext,
	numRange float64,
	lowerBound, upperBound tree.Datum,
) float64 {
	if numRange == 0 {
		return 0
	}
	rangeUpperBound, ok := upperBound.Prev(ctx, compareCtx)
	if !ok {
		rangeUpperBound = upperBound
	}
	if maxDistinct, ok := tree.MaxDistinctCount(ctx, compareCtx, lowerBound, rangeUpperBound); ok {
		return expectedDistinctCount(numRange, float64(maxDistinct))
	}
	return numRange
}

func getNextLowerBound(
	ctx context.Context, compareCtx tree.CompareContext, currentUpperBound tree.Datum,
) tree.Datum {
	nextLowerBound, ok := currentUpperBound.Next(ctx, compareCtx)
	if !ok {
		nextLowerBound = currentUpperBound
	}
	return nextLowerBound
}

// maxDistinctRange returns the maximum number of distinct values in the given
// range, excluding both lowerBound and upperBound. Returns countable=true if
// the returned value is countable.
func maxDistinctRange(
	ctx context.Context, compareCtx tree.CompareContext, lowerBound, upperBound tree.Datum,
) (_ float64, countable bool) {
	if maxDistinct, ok := tree.MaxDistinctCount(ctx, compareCtx, lowerBound, upperBound); ok {
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
