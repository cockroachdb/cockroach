// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package props

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/olekukonko/tablewriter"
)

// Histogram captures the distribution of values for a particular column within
// a relational expression.
// Histograms are immutable.
type Histogram struct {
	evalCtx *tree.EvalContext
	col     opt.ColumnID
	buckets []cat.HistogramBucket
}

func (h *Histogram) String() string {
	w := histogramWriter{}
	w.init(h.buckets)
	var buf bytes.Buffer
	w.write(&buf)
	return buf.String()
}

// Init initializes the histogram with data from the catalog.
func (h *Histogram) Init(
	evalCtx *tree.EvalContext, col opt.ColumnID, buckets []cat.HistogramBucket,
) {
	h.evalCtx = evalCtx
	h.col = col
	h.buckets = buckets
}

// copy returns a deep copy of the histogram.
func (h *Histogram) copy() *Histogram {
	buckets := make([]cat.HistogramBucket, len(h.buckets))
	copy(buckets, h.buckets)
	return &Histogram{
		evalCtx: h.evalCtx,
		col:     h.col,
		buckets: buckets,
	}
}

// BucketCount returns the number of buckets in the histogram.
func (h *Histogram) BucketCount() int {
	return len(h.buckets)
}

// Bucket returns a pointer to the ith bucket in the histogram.
// i must be greater than or equal to 0 and less than BucketCount.
func (h *Histogram) Bucket(i int) *cat.HistogramBucket {
	return &h.buckets[i]
}

// ValuesCount returns the total number of values in the histogram. It can
// be used to estimate the selectivity of a predicate by comparing the values
// count before and after calling Filter on the histogram.
func (h *Histogram) ValuesCount() float64 {
	var count float64
	for i := range h.buckets {
		count += h.buckets[i].NumRange
		count += h.buckets[i].NumEq
	}
	return count
}

// DistinctValuesCount returns the estimated number of distinct values in the
// histogram.
func (h *Histogram) DistinctValuesCount() float64 {
	var count float64
	for i := range h.buckets {
		b := &h.buckets[i]
		count += b.DistinctRange
		if b.NumEq > 1 {
			count++
		} else {
			count += b.NumEq
		}
	}
	if maxCount := h.maxDistinctValuesCount(); maxCount < count {
		count = maxCount
	}
	return count
}

// maxDistinctValuesCount estimates the maximum number of distinct values in
// the histogram.
func (h *Histogram) maxDistinctValuesCount() float64 {
	if len(h.buckets) == 0 {
		return 0
	}

	// The first bucket always has a zero value for NumRange, so the lower bound
	// of the histogram is the upper bound of the first bucket.
	if h.Bucket(0).NumRange != 0 {
		panic(errors.AssertionFailedf("the first bucket should have NumRange=0"))
	}
	lowerBound := h.Bucket(0).UpperBound

	var count float64
	for i := range h.buckets {
		b := &h.buckets[i]
		rng, ok := maxDistinctValuesInRange(lowerBound, b.UpperBound)

		if ok && b.NumRange > rng {
			count += rng
		} else {
			count += b.NumRange
		}

		if b.NumEq > 1 {
			count++
		} else {
			count += b.NumEq
		}
		lowerBound = h.getNextLowerBound(b.UpperBound)
	}
	return count
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

// CanFilter returns true if the given constraint can filter the histogram.
// This is the case if the histogram column matches one of the columns in
// the exact prefix of c or the next column immediately after the exact prefix.
// Returns the offset of the matching column in the constraint if found, as
// well as the exact prefix.
func (h *Histogram) CanFilter(c *constraint.Constraint) (colOffset, exactPrefix int, ok bool) {
	exactPrefix = c.ExactPrefix(h.evalCtx)
	constrainedCols := c.ConstrainedColumns(h.evalCtx)
	for i := 0; i < constrainedCols && i <= exactPrefix; i++ {
		if c.Columns.Get(i).ID() == h.col {
			return i, exactPrefix, true
		}
	}
	return 0, exactPrefix, false
}

// Filter filters the histogram according to the given constraint, and returns
// a new histogram with the results. CanFilter should be called first to
// validate that c can filter the histogram.
func (h *Histogram) Filter(c *constraint.Constraint) *Histogram {
	colOffset, exactPrefix, ok := h.CanFilter(c)
	if !ok {
		panic(errors.AssertionFailedf("column mismatch"))
	}

	bucketCount := h.BucketCount()
	filtered := &Histogram{
		evalCtx: h.evalCtx,
		col:     h.col,
		buckets: make([]cat.HistogramBucket, 0, bucketCount),
	}
	if bucketCount == 0 {
		return filtered
	}

	// The first bucket always has a zero value for NumRange, so the lower bound
	// of the histogram is the upper bound of the first bucket.
	if h.Bucket(0).NumRange != 0 {
		panic(errors.AssertionFailedf("the first bucket should have NumRange=0"))
	}

	prefix := make([]tree.Datum, colOffset)
	for i := range prefix {
		prefix[i] = c.Spans.Get(0).StartKey().Value(i)
	}
	desc := c.Columns.Get(colOffset).Descending()
	var iter histogramIter
	iter.init(h, desc)
	spanIndex := 0
	keyCtx := constraint.KeyContext{EvalCtx: h.evalCtx, Columns: c.Columns}

	// Find the first span that may overlap with the histogram.
	firstBucket := makeSpanFromBucket(&iter, prefix)
	spanCount := c.Spans.Count()
	for spanIndex < spanCount {
		span := c.Spans.Get(spanIndex)
		if firstBucket.StartsAfter(&keyCtx, span) {
			spanIndex++
			continue
		}
		break
	}
	if spanIndex == spanCount {
		return filtered
	}

	// Use binary search to find the first bucket that overlaps with the span.
	span := c.Spans.Get(spanIndex)
	bucIndex := sort.Search(bucketCount, func(i int) bool {
		iter.setIdx(i)
		bucket := makeSpanFromBucket(&iter, prefix)
		if desc {
			return span.StartsAfter(&keyCtx, &bucket)
		}
		return !span.StartsAfter(&keyCtx, &bucket)
	})
	if desc {
		bucIndex--
		if bucIndex == -1 {
			return filtered
		}
	} else if bucIndex == bucketCount {
		return filtered
	}
	iter.setIdx(bucIndex)
	if !desc && bucIndex > 0 {
		prevUpperBound := h.Bucket(bucIndex - 1).UpperBound
		filtered.addEmptyBucket(prevUpperBound, desc)
	}

	// For the remaining buckets and spans, use a variation on merge sort.
	for spanIndex < spanCount {
		if spanIndex > 0 && colOffset < exactPrefix {
			// If this column is part of the exact prefix, we don't need to look at
			// the rest of the spans.
			break
		}

		// Convert the bucket to a span in order to take advantage of the
		// constraint library.
		left := makeSpanFromBucket(&iter, prefix)
		right := c.Spans.Get(spanIndex)

		if left.StartsAfter(&keyCtx, right) {
			spanIndex++
			continue
		}

		filteredSpan := left
		if !filteredSpan.TryIntersectWith(&keyCtx, right) {
			filtered.addEmptyBucket(iter.b.UpperBound, desc)
			if ok := iter.next(); !ok {
				break
			}
			continue
		}

		filteredBucket := iter.b
		if filteredSpan.Compare(&keyCtx, &left) != 0 {
			// The bucket was cut off in the middle. Get the resulting filtered
			// bucket.
			filteredBucket = getFilteredBucket(&iter, &keyCtx, &filteredSpan, colOffset)
			if !desc && filteredSpan.CompareStarts(&keyCtx, &left) != 0 {
				// We need to add an empty bucket before the new bucket.
				ub := h.getPrevUpperBound(filteredSpan.StartKey(), filteredSpan.StartBoundary(), colOffset)
				filtered.addEmptyBucket(ub, desc)
			}
		}
		filtered.addBucket(filteredBucket, desc)

		if desc && filteredSpan.CompareEnds(&keyCtx, &left) != 0 {
			// We need to add an empty bucket after the new bucket.
			ub := h.getPrevUpperBound(filteredSpan.EndKey(), filteredSpan.EndBoundary(), colOffset)
			filtered.addEmptyBucket(ub, desc)
		}

		// Skip past whichever span ends first, or skip past both if they have
		// the same endpoint.
		cmp := left.CompareEnds(&keyCtx, right)
		if cmp <= 0 {
			if ok := iter.next(); !ok {
				break
			}
		}
		if cmp >= 0 {
			spanIndex++
		}
	}

	if desc {
		// After we reverse the buckets below, the last bucket will become the
		// first bucket. NumRange of the first bucket must be 0, so add an empty
		// bucket if needed.
		if iter.next() {
			// The remaining buckets from the original histogram have been removed.
			filtered.addEmptyBucket(iter.lb, desc)
		} else if lastBucket := filtered.buckets[len(filtered.buckets)-1]; lastBucket.NumRange != 0 {
			iter.setIdx(0)
			span := makeSpanFromBucket(&iter, prefix)
			ub := h.getPrevUpperBound(span.EndKey(), span.EndBoundary(), colOffset)
			filtered.addEmptyBucket(ub, desc)
		}

		// Reverse the buckets so they are in ascending order.
		for i := 0; i < len(filtered.buckets)/2; i++ {
			j := len(filtered.buckets) - 1 - i
			filtered.buckets[i], filtered.buckets[j] = filtered.buckets[j], filtered.buckets[i]
		}
	}

	return filtered
}

func (h *Histogram) getNextLowerBound(currentUpperBound tree.Datum) tree.Datum {
	nextLowerBound, ok := currentUpperBound.Next(h.evalCtx)
	if !ok {
		nextLowerBound = currentUpperBound
	}
	return nextLowerBound
}

func (h *Histogram) getPrevUpperBound(
	currentLowerBound constraint.Key, boundary constraint.SpanBoundary, colOffset int,
) tree.Datum {
	prevUpperBound := currentLowerBound.Value(colOffset)
	if boundary == constraint.IncludeBoundary {
		if prev, ok := prevUpperBound.Prev(h.evalCtx); ok {
			prevUpperBound = prev
		}
	}
	return prevUpperBound
}

func (h *Histogram) addEmptyBucket(upperBound tree.Datum, desc bool) {
	h.addBucket(&cat.HistogramBucket{UpperBound: upperBound}, desc)
}

func (h *Histogram) addBucket(bucket *cat.HistogramBucket, desc bool) {
	// Check whether we can combine this bucket with the previous bucket.
	if len(h.buckets) != 0 {
		lastBucket := &h.buckets[len(h.buckets)-1]
		lower, higher := lastBucket, bucket
		if desc {
			lower, higher = bucket, lastBucket
		}
		if lower.NumRange == 0 && lower.NumEq == 0 && higher.NumRange == 0 {
			lastBucket.NumEq = higher.NumEq
			lastBucket.UpperBound = higher.UpperBound
			return
		}
		if lastBucket.UpperBound.Compare(h.evalCtx, bucket.UpperBound) == 0 {
			lastBucket.NumEq = lower.NumEq + higher.NumRange + higher.NumEq
			lastBucket.NumRange = lower.NumRange
			return
		}
	}
	h.buckets = append(h.buckets, *bucket)
}

// ApplySelectivity reduces the size of each histogram bucket according to
// the given selectivity, and returns a new histogram with the results.
func (h *Histogram) ApplySelectivity(selectivity float64) *Histogram {
	res := h.copy()
	for i := range res.buckets {
		b := &res.buckets[i]

		// Save n and d for the distinct count formula below.
		n := b.NumRange
		d := b.DistinctRange

		b.NumEq *= selectivity
		b.NumRange *= selectivity

		if d == 0 {
			continue
		}
		// If each distinct value appears n/d times, and the probability of a
		// row being filtered out is (1 - selectivity), the probability that all
		// n/d rows are filtered out is (1 - selectivity)^(n/d). So the expected
		// number of values that are filtered out is d*(1 - selectivity)^(n/d).
		//
		// This formula returns d * selectivity when d=n but is closer to d
		// when d << n.
		b.DistinctRange = d - d*math.Pow(1-selectivity, n/d)
	}
	return res
}

// histogramIter is a helper struct for iterating through the buckets in a
// histogram. It enables iterating both forward and backward through the
// buckets.
type histogramIter struct {
	h    *Histogram
	desc bool
	idx  int
	b    *cat.HistogramBucket
	lb   tree.Datum
	ub   tree.Datum
}

// init initializes a histogramIter to point to the first bucket of the given
// histogram. If desc is true, the iterator starts from the end of the
// histogram and moves backwards.
func (hi *histogramIter) init(h *Histogram, desc bool) {
	hi.idx = -1
	if desc {
		hi.idx = h.BucketCount()
	}
	hi.h = h
	hi.desc = desc
	hi.next()
}

// setIdx updates the histogramIter to point to the ith bucket in the
// histogram.
func (hi *histogramIter) setIdx(i int) {
	hi.idx = i - 1
	if hi.desc {
		hi.idx = i + 1
	}
	hi.next()
}

// next sets the histogramIter to point to the next bucket. If hi.desc is true
// the "next" bucket is actually the previous bucket in the histogram. Returns
// false if there are no more buckets.
func (hi *histogramIter) next() (ok bool) {
	getBounds := func() (lb, ub tree.Datum) {
		hi.b = hi.h.Bucket(hi.idx)
		ub = hi.b.UpperBound
		if hi.idx == 0 {
			lb = ub
		} else {
			lb = hi.h.getNextLowerBound(hi.h.Bucket(hi.idx - 1).UpperBound)
		}
		return lb, ub
	}

	if hi.desc {
		hi.idx--
		if hi.idx < 0 {
			return false
		}
		hi.ub, hi.lb = getBounds()
	} else {
		hi.idx++
		if hi.idx >= hi.h.BucketCount() {
			return false
		}
		hi.lb, hi.ub = getBounds()
	}

	return true
}

func makeSpanFromBucket(iter *histogramIter, prefix []tree.Datum) (span constraint.Span) {
	span.Init(
		constraint.MakeCompositeKey(append(prefix[:len(prefix):len(prefix)], iter.lb)...),
		constraint.IncludeBoundary,
		constraint.MakeCompositeKey(append(prefix[:len(prefix):len(prefix)], iter.ub)...),
		constraint.IncludeBoundary,
	)
	return span
}

// getFilteredBucket filters the histogram bucket according to the given span,
// and returns a new bucket with the results. The span represents the maximum
// range of values that remain in the bucket after filtering. The span must
// be fully contained within the bucket, or else getFilteredBucket will throw
// an error.
//
// For example, suppose a bucket initially has lower bound 0 (inclusive) and
// contains the following data: {NumEq: 5, NumRange: 10, UpperBound: 10} (all
// values are integers).
//
// The following spans will filter the bucket as shown:
//   [/0 - /5]   => {NumEq: 1, NumRange: 5, UpperBound: 5}
//   [/2 - /10]  => {NumEq: 5, NumRange: 8, UpperBound: 10}
//   [/20 - /30] => error
//
// Note that the calculations for NumEq and NumRange depend on the data type.
// For discrete data types such as integers and dates, it is always possible
// to assign a non-zero value for NumEq as long as NumEq and NumRange were
// non-zero in the original bucket. For continuous types such as floats,
// NumEq will be zero unless the filtered bucket includes the original upper
// bound. For example, given the same bucket as in the above example, but with
// floating point values instead of integers:
//
//   [/0 - /5]   => {NumEq: 0, NumRange: 5, UpperBound: 5.0}
//   [/2 - /10]  => {NumEq: 5, NumRange: 8, UpperBound: 10.0}
//   [/20 - /30] => error
//
// For non-numeric types such as strings, it is not possible to estimate
// the size of NumRange if the bucket is cut off in the middle. In this case,
// we use the heuristic that NumRange is reduced by half.
//
func getFilteredBucket(
	iter *histogramIter, keyCtx *constraint.KeyContext, filteredSpan *constraint.Span, colOffset int,
) *cat.HistogramBucket {
	spanLowerBound := filteredSpan.StartKey().Value(colOffset)
	spanUpperBound := filteredSpan.EndKey().Value(colOffset)
	bucketLowerBound := iter.lb
	bucketUpperBound := iter.ub
	b := iter.b

	// Check that the given span is contained in the bucket.
	cmpSpanStartBucketStart := spanLowerBound.Compare(keyCtx.EvalCtx, bucketLowerBound)
	cmpSpanEndBucketEnd := spanUpperBound.Compare(keyCtx.EvalCtx, bucketUpperBound)
	contained := cmpSpanStartBucketStart >= 0 && cmpSpanEndBucketEnd <= 0
	if iter.desc {
		contained = cmpSpanStartBucketStart <= 0 && cmpSpanEndBucketEnd >= 0
	}
	if !contained {
		panic(errors.AssertionFailedf("span must be fully contained in the bucket"))
	}

	// Extract the range sizes before and after filtering. Only numeric and
	// date-time types will have ok=true, since these are the only types for
	// which we can accurately calculate the range size of a non-equality span.
	rangeBefore, rangeAfter, ok := getRangesBeforeAndAfter(
		bucketLowerBound, bucketUpperBound, spanLowerBound, spanUpperBound, iter.desc,
	)

	// Determine whether this span represents an equality condition.
	isEqualityCondition := spanLowerBound.Compare(keyCtx.EvalCtx, spanUpperBound) == 0

	// Determine whether this span includes the original upper bound of the
	// bucket.
	isSpanEndBoundaryInclusive := filteredSpan.EndBoundary() == constraint.IncludeBoundary
	includesOriginalUpperBound := isSpanEndBoundaryInclusive && cmpSpanEndBucketEnd == 0
	if iter.desc {
		isSpanStartBoundaryInclusive := filteredSpan.StartBoundary() == constraint.IncludeBoundary
		includesOriginalUpperBound = isSpanStartBoundaryInclusive && cmpSpanStartBucketStart == 0
	}

	// Calculate the new value for numEq.
	var numEq float64
	if includesOriginalUpperBound {
		numEq = b.NumEq
	} else {
		if isEqualityCondition {
			// This span represents an equality condition with a value in the range
			// of this bucket. Use the distinct count of the bucket to estimate the
			// selectivity of the equality condition.
			selectivity := 1.0
			if b.DistinctRange > 1 {
				selectivity = 1 / b.DistinctRange
			}
			numEq = selectivity * b.NumRange
		} else if ok && rangeBefore > 0 && isDiscrete(bucketLowerBound.ResolvedType()) {
			// If we were successful in finding the ranges before and after filtering
			// and the data type is discrete (e.g., integer, date, or timestamp), we
			// can assign some of the old NumRange to the new NumEq.
			numEq = b.NumRange / rangeBefore
		}
	}

	// Calculate the new value for numRange.
	var numRange float64
	if isEqualityCondition {
		numRange = 0
	} else if ok && rangeBefore > 0 {
		// If we were successful in finding the ranges before and after filtering,
		// calculate the fraction of values that should be assigned to the new
		// bucket.
		numRange = b.NumRange * rangeAfter / rangeBefore
	} else {
		// In the absence of any information, assume we reduced the size of the
		// bucket by half.
		numRange = 0.5 * b.NumRange
	}

	// Calculate the new value for distinctCountRange.
	var distinctCountRange float64
	if b.NumRange > 0 {
		distinctCountRange = b.DistinctRange * numRange / b.NumRange
	}

	upperBound := spanUpperBound
	if iter.desc {
		upperBound = spanLowerBound
	}
	return &cat.HistogramBucket{
		NumEq:         numEq,
		NumRange:      numRange,
		DistinctRange: distinctCountRange,
		UpperBound:    upperBound,
	}
}

// getRangesBeforeAndAfter returns the size of the ranges before and after the
// given bucket is filtered by the given span. If swap is true, the upper and
// lower bounds should be swapped for the bucket and the span. Returns ok=true
// if these range sizes are calculated successfully, and false otherwise.
func getRangesBeforeAndAfter(
	bucketLowerBound, bucketUpperBound, spanLowerBound, spanUpperBound tree.Datum, swap bool,
) (rangeBefore, rangeAfter float64, ok bool) {
	// If the data types don't match, don't bother trying to calculate the range
	// sizes. This should almost never happen, but we want to avoid type
	// assertion errors below.
	typesMatch :=
		bucketLowerBound.ResolvedType().Equivalent(bucketUpperBound.ResolvedType()) &&
			bucketUpperBound.ResolvedType().Equivalent(spanLowerBound.ResolvedType()) &&
			spanLowerBound.ResolvedType().Equivalent(spanUpperBound.ResolvedType())
	if !typesMatch {
		return 0, 0, false
	}

	if swap {
		bucketLowerBound, bucketUpperBound = bucketUpperBound, bucketLowerBound
		spanLowerBound, spanUpperBound = spanUpperBound, spanLowerBound
	}

	// TODO(rytaft): handle more types here.
	// Note: the calculations below assume that bucketLowerBound is inclusive and
	// Span.PreferInclusive() has been called on the span.

	getRange := func(lowerBound, upperBound tree.Datum) (rng float64, ok bool) {
		switch lowerBound.ResolvedType().Family() {
		case types.IntFamily:
			rng = float64(*upperBound.(*tree.DInt)) - float64(*lowerBound.(*tree.DInt))
			return rng, true

		case types.DateFamily:
			lower := lowerBound.(*tree.DDate)
			upper := upperBound.(*tree.DDate)
			if lower.IsFinite() && upper.IsFinite() {
				rng = float64(upper.PGEpochDays()) - float64(lower.PGEpochDays())
				return rng, true
			}
			return 0, false

		case types.DecimalFamily:
			lower, err := lowerBound.(*tree.DDecimal).Float64()
			if err != nil {
				return 0, false
			}
			upper, err := upperBound.(*tree.DDecimal).Float64()
			if err != nil {
				return 0, false
			}
			rng = upper - lower
			return rng, true

		case types.FloatFamily:
			rng = float64(*upperBound.(*tree.DFloat)) - float64(*lowerBound.(*tree.DFloat))
			return rng, true

		case types.TimestampFamily:
			lower := lowerBound.(*tree.DTimestamp).Time
			upper := upperBound.(*tree.DTimestamp).Time
			rng = float64(upper.Sub(lower))
			return rng, true

		case types.TimestampTZFamily:
			lower := lowerBound.(*tree.DTimestampTZ).Time
			upper := upperBound.(*tree.DTimestampTZ).Time
			rng = float64(upper.Sub(lower))
			return rng, true

		default:
			return 0, false
		}
	}

	rangeBefore, okBefore := getRange(bucketLowerBound, bucketUpperBound)
	rangeAfter, okAfter := getRange(spanLowerBound, spanUpperBound)
	ok = okBefore && okAfter

	return rangeBefore, rangeAfter, ok
}

// isDiscrete returns true if the given data type is discrete.
func isDiscrete(typ *types.T) bool {
	switch typ.Family() {
	case types.IntFamily, types.DateFamily, types.TimestampFamily, types.TimestampTZFamily:
		return true
	}
	return false
}

// histogramWriter prints histograms with the following formatting:
//   NumRange1    NumEq1     NumRange2    NumEq2    ....
// <----------- UpperBound1 ----------- UpperBound2 ....
//
// For example:
//   0  1  90  10   0  20
// <--- 0 ---- 100 --- 200
//
// This describes a histogram with 3 buckets. The first bucket contains 1 value
// equal to 0. The second bucket contains 90 values between 0 and 100 and
// 10 values equal to 100. Finally, the third bucket contains 20 values equal
// to 200.
type histogramWriter struct {
	cells     [][]string
	colWidths []int
}

const (
	// These constants describe the two rows that are printed.
	counts = iota
	boundaries
)

func (w *histogramWriter) init(buckets []cat.HistogramBucket) {
	w.cells = [][]string{
		make([]string, len(buckets)*2),
		make([]string, len(buckets)*2),
	}
	w.colWidths = make([]int, len(buckets)*2)

	for i, b := range buckets {
		w.cells[counts][i*2] = fmt.Sprintf(" %.5g ", b.NumRange)
		w.cells[counts][i*2+1] = fmt.Sprintf("%.5g", b.NumEq)
		// TODO(rytaft): truncate large strings.
		w.cells[boundaries][i*2+1] = fmt.Sprintf(" %s ", b.UpperBound.String())
		if width := tablewriter.DisplayWidth(w.cells[counts][i*2]); width > w.colWidths[i*2] {
			w.colWidths[i*2] = width
		}
		if width := tablewriter.DisplayWidth(w.cells[counts][i*2+1]); width > w.colWidths[i*2+1] {
			w.colWidths[i*2+1] = width
		}
		if width := tablewriter.DisplayWidth(w.cells[boundaries][i*2+1]); width > w.colWidths[i*2+1] {
			w.colWidths[i*2+1] = width
		}
	}
}

func (w *histogramWriter) write(out io.Writer) {
	if len(w.cells[counts]) == 0 {
		return
	}

	// Print a space to match up with the "<" character below.
	fmt.Fprint(out, " ")
	for i := range w.cells[counts] {
		fmt.Fprintf(out, "%s", tablewriter.Pad(w.cells[counts][i], " ", w.colWidths[i]))
	}
	fmt.Fprint(out, "\n")
	fmt.Fprint(out, "<")
	for i := range w.cells[boundaries] {
		fmt.Fprintf(out, "%s", tablewriter.Pad(w.cells[boundaries][i], "-", w.colWidths[i]))
	}
}
