// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package props

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/timetz"
	"github.com/cockroachdb/errors"
	"github.com/olekukonko/tablewriter"
)

// Histogram captures the distribution of values for a particular column within
// a relational expression.
// Histograms are immutable.
type Histogram struct {
	evalCtx *eval.Context
	// selectivity tracks the total applied selectivity over time, which will be
	// applied to the histogram when counting values or printing. We use a float64
	// to allow this to go below epsilon, because it could be the product of
	// multiple selectivities.
	selectivity float64
	buckets     []cat.HistogramBucket
	col         opt.ColumnID
}

func (h *Histogram) String() string {
	w := histogramWriter{}
	w.init(h.selectivity, h.buckets)
	var buf bytes.Buffer
	w.write(&buf)
	return buf.String()
}

// Init initializes the histogram with data from the catalog.
func (h *Histogram) Init(evalCtx *eval.Context, col opt.ColumnID, buckets []cat.HistogramBucket) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*h = Histogram{
		evalCtx:     evalCtx,
		col:         col,
		selectivity: 1,
		buckets:     buckets,
	}
}

// bucketCount returns the number of buckets in the histogram.
func (h *Histogram) bucketCount() int {
	return len(h.buckets)
}

// numEq returns NumEq for the ith histogram bucket, with the histogram's
// selectivity applied. i must be greater than or equal to 0 and less than
// bucketCount.
func (h *Histogram) numEq(i int) float64 {
	return h.buckets[i].NumEq * h.selectivity
}

// numRange returns NumRange for the ith histogram bucket, with the histogram's
// selectivity applied. i must be greater than or equal to 0 and less than
// bucketCount.
func (h *Histogram) numRange(i int) float64 {
	return h.buckets[i].NumRange * h.selectivity
}

// distinctRange returns DistinctRange for the ith histogram bucket, with the
// histogram's selectivity applied. i must be greater than or equal to 0 and
// less than bucketCount.
func (h *Histogram) distinctRange(i int) float64 {
	n := h.buckets[i].NumRange
	d := h.buckets[i].DistinctRange

	if d == 0 {
		return 0
	}

	// If each distinct value appears n/d times, and the probability of a row
	// being filtered out is (1 - selectivity), the probability that all n/d rows
	// are filtered out is (1 - selectivity)^(n/d). So the expected number of
	// values that are filtered out is d*(1 - selectivity)^(n/d).
	//
	// This formula returns d * selectivity when d=n but is closer to d when
	// d << n.
	return d - d*math.Pow(1-h.selectivity, n/d)
}

// upperBound returns UpperBound for the ith histogram bucket. i must be
// greater than or equal to 0 and less than bucketCount.
func (h *Histogram) upperBound(i int) tree.Datum {
	return h.buckets[i].UpperBound
}

// ValuesCount returns the total number of values in the histogram. It can
// be used to estimate the selectivity of a predicate by comparing the values
// count before and after calling Filter on the histogram.
func (h *Histogram) ValuesCount() float64 {
	var count float64
	for i := range h.buckets {
		count += h.numRange(i)
		count += h.numEq(i)
	}
	return count
}

// EqEstimate returns the estimated number of rows that equal the given
// datum. If the datum is equal to a bucket's upperbound, it returns the
// bucket's NumEq. If the datum falls in the range of a bucket's upper and lower
// bounds, it returns the bucket's NumRange divided by the bucket's
// DistinctRange. Otherwise, if the datum does not fall into any bucket in the
// histogram or any comparison between the datum and a bucket's upperbound
// results in an error, then it returns the total number of values in the
// histogram divided by the total number of distinct values.
func (h *Histogram) EqEstimate(ctx context.Context, d tree.Datum) float64 {
	// Find the bucket belonging to the datum. It is the first bucket where the
	// datum is less than or equal to the upperbound.
	bucketIdx := binarySearch(len(h.buckets), func(i int) (bool, error) {
		cmp, err := d.Compare(ctx, h.evalCtx, h.upperBound(i))
		return cmp <= 0, err
	})
	if bucketIdx < len(h.buckets) {
		if cmp, err := d.Compare(ctx, h.evalCtx, h.upperBound(bucketIdx)); err == nil {
			if cmp == 0 {
				return h.numEq(bucketIdx)
			}
			if bucketIdx != 0 {
				if h.distinctRange(bucketIdx) == 0 {
					// Avoid dividing by zero.
					return 0
				}
				return h.numRange(bucketIdx) / h.distinctRange(bucketIdx)
			}
			// The value d is less than the upper bound of the first bucket, so
			// it is outside the bounds of the histogram. Fallback to the total
			// number of values divided by the total number of distinct values.
		}
	}
	totalDistinct := h.DistinctValuesCount()
	if totalDistinct == 0 {
		// Avoid dividing by zero.
		return 0
	}
	return h.ValuesCount() / h.DistinctValuesCount()
}

// binarySearch extends sort.Search to allow the search function to return an
// error. It returns the smallest index i in [0, n) at which f(i) is true,
// assuming that on the range [0, n), f(i) == true implies f(i+1) == true. If
// there is no such index, or if f returns an error for any invocation, it
// returns n.
func binarySearch(n int, f func(int) (bool, error)) (idx int) {
	defer func() {
		if r := recover(); r != nil {
			idx = n
		}
	}()
	return sort.Search(n, func(i int) bool {
		res, err := f(i)
		if err != nil {
			panic(err)
		}
		return res
	})
}

// DistinctValuesCount returns the estimated number of distinct values in the
// histogram.
func (h *Histogram) DistinctValuesCount() float64 {
	var count float64
	for i := range h.buckets {
		count += h.distinctRange(i)
		numEq := h.numEq(i)
		if numEq > 1 {
			count++
		} else {
			count += numEq
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
	if h.numRange(0) != 0 {
		panic(errors.AssertionFailedf("the first bucket should have NumRange=0"))
	}
	previousUpperBound := h.upperBound(0)

	var count float64
	for i := range h.buckets {
		upperBound := h.upperBound(i)
		rng, ok := maxDistinctValuesInRange(previousUpperBound, upperBound)

		numRange := h.numRange(i)
		if ok && numRange > rng {
			count += rng
		} else {
			count += numRange
		}

		numEq := h.numEq(i)
		if numEq > 1 {
			count++
		} else {
			count += numEq
		}
		previousUpperBound = upperBound
	}
	return count
}

// MaxFrequency returns the maximum value of NumEq across all histogram buckets.
func (h *Histogram) MaxFrequency() float64 {
	var mf float64
	for i := range h.buckets {
		if numEq := h.numEq(i); numEq > mf {
			mf = numEq
		}
	}
	return mf
}

// maxDistinctValuesInRange returns the maximum number of distinct values in
// the range (lowerBound, upperBound). It returns ok=false when it is not
// possible to determine a finite value (which is the case for all types other
// than integers and dates).
func maxDistinctValuesInRange(lowerBound, upperBound tree.Datum) (n float64, ok bool) {
	// TODO(mgartner): We should probably use tree.MaxDistinctCount here
	// instead. We may have to adjust the result by one or two because it
	// returns a distinct count inclusive of the bounds.
	switch lowerBound.ResolvedType().Family() {
	case types.IntFamily:
		n = float64(*upperBound.(*tree.DInt)) - float64(*lowerBound.(*tree.DInt))

	case types.DateFamily:
		lower := lowerBound.(*tree.DDate)
		upper := upperBound.(*tree.DDate)
		if !lower.IsFinite() || !upper.IsFinite() {
			return 0, false
		}
		n = float64(upper.PGEpochDays()) - float64(lower.PGEpochDays())

	default:
		return 0, false
	}

	// Subtract one to exclude the lower bound value.
	n = n - 1
	if n < 0 {
		n = 0
	}
	return n, true
}

// CanFilter returns true if the given constraint can filter the histogram.
// This is the case if the histogram column matches one of the columns in
// the exact prefix of c or the next column immediately after the exact prefix.
// Returns the offset of the matching column in the constraint if found, as
// well as the exact prefix.
func (h *Histogram) CanFilter(
	ctx context.Context, c *constraint.Constraint,
) (colOffset, exactPrefix int, ok bool) {
	exactPrefix = c.ExactPrefix(ctx, h.evalCtx)
	constrainedCols := c.ConstrainedColumns(h.evalCtx)
	for i := 0; i < constrainedCols && i <= exactPrefix; i++ {
		if c.Columns.Get(i).ID() == h.col {
			return i, exactPrefix, true
		}
	}
	return 0, exactPrefix, false
}

func (h *Histogram) filter(
	ctx context.Context,
	spanCount int,
	getSpan func(int) *constraint.Span,
	desc bool,
	colOffset, exactPrefix int,
	prefix []tree.Datum,
	columns constraint.Columns,
) *Histogram {
	bucketCount := h.bucketCount()
	filtered := &Histogram{
		evalCtx:     h.evalCtx,
		col:         h.col,
		selectivity: h.selectivity,
	}
	if bucketCount == 0 {
		return filtered
	}

	// The first bucket always has a zero value for NumRange, so the lower bound
	// of the histogram is the upper bound of the first bucket.
	if h.numRange(0) != 0 {
		panic(errors.AssertionFailedf("the first bucket should have NumRange=0"))
	}

	var iter histogramIter
	iter.init(h, desc)
	spanIndex := 0
	keyCtx := constraint.KeyContext{Ctx: ctx, EvalCtx: h.evalCtx, Columns: columns}

	// Find the first span that may overlap with the histogram.
	//
	// A span returned from spanBuilder.makeSpanFromBucket is only valid until
	// the next call to the method (see the method for more details). It is safe
	// to reuse the same spanBuilder here and below because the spans are only
	// used for comparison and are not stored, and two spans are never
	// built and referenced simultaneously.
	var sb spanBuilder
	sb.init(prefix)
	{
		// Limit the scope of firstBucket to avoid referencing it below after
		// sb.makeSpanFromBucket has been called again.
		firstBucket := sb.makeSpanFromBucket(ctx, &iter)
		for spanIndex < spanCount {
			span := getSpan(spanIndex)
			if firstBucket.StartsAfter(&keyCtx, span) {
				spanIndex++
				continue
			}
			break
		}
		if spanIndex == spanCount {
			return filtered
		}
	}

	// Use binary search to find the first bucket that overlaps with the span.
	span := getSpan(spanIndex)
	bucIndex := sort.Search(bucketCount, func(i int) bool {
		iter.setIdx(i)
		bucket := sb.makeSpanFromBucket(ctx, &iter)
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

	// In the general case, we'll need the same number of buckets as the
	// existing histogram, minus the buckets that come before the first bucket
	// that overlaps with the spans. In the special, yet common, case where we
	// have a single span that overlaps one bucket, we'll need only two buckets.
	newBucketCount := bucketCount - bucIndex + 1
	if desc {
		newBucketCount = bucIndex + 1
	}
	if spanCount == 1 && bucIndex < bucketCount-1 {
		iter.setIdx(bucIndex + 1)
		bucket := sb.makeSpanFromBucket(ctx, &iter)
		if !desc && bucket.StartsAfter(&keyCtx, span) ||
			desc && !bucket.StartsAfter(&keyCtx, span) {
			newBucketCount = 2
		}
	}
	filtered.buckets = make([]cat.HistogramBucket, 0, newBucketCount)

	if !desc && bucIndex > 0 {
		prevUpperBound := h.upperBound(bucIndex - 1)
		filtered.addEmptyBucket(ctx, prevUpperBound, desc)
	}

	// For the remaining buckets and spans, use a variation on merge sort.
	iter.setIdx(bucIndex)
	for spanIndex < spanCount {
		if spanIndex > 0 && colOffset < exactPrefix {
			// If this column is part of the exact prefix, we don't need to look at
			// the rest of the spans.
			break
		}

		// Convert the bucket to a span in order to take advantage of the
		// constraint library.
		left := sb.makeSpanFromBucket(ctx, &iter)
		right := getSpan(spanIndex)

		if left.StartsAfter(&keyCtx, right) {
			spanIndex++
			continue
		}

		// Copying the span is safe here because the keys within the spans are
		// never mutated.
		filteredSpan := left
		if !filteredSpan.TryIntersectWith(&keyCtx, right) {
			filtered.addEmptyBucket(ctx, iter.b.UpperBound, desc)
			if ok := iter.next(); !ok {
				break
			}
			continue
		}

		filteredBucket := *iter.b
		if filteredSpan.Compare(&keyCtx, &left) != 0 {
			// The bucket was cut off in the middle. Get the resulting filtered
			// bucket.
			//
			// The span generated from the bucket may have an exclusive bound in
			// order to avoid a datum allocation for the majority of cases where
			// the span does not intersect the bucket. If the span does
			// intersect with the bucket, we transform the bucket span into an
			// inclusive one to make it easier to work with. Note that this
			// conversion is best-effort; not all datums support Next or Prev
			// which allow exclusive ranges to be converted to inclusive ones.
			cmpStarts := filteredSpan.CompareStarts(&keyCtx, &left)
			filteredSpan.PreferInclusive(&keyCtx)
			filteredBucket = getFilteredBucket(&iter, &keyCtx, &filteredSpan, colOffset)
			if !desc && cmpStarts != 0 {
				// We need to add an empty bucket before the new bucket.
				ub := h.getPrevUpperBound(ctx, filteredSpan.StartKey(), filteredSpan.StartBoundary(), colOffset)
				filtered.addEmptyBucket(ctx, ub, desc)
			}
		}
		filtered.addBucket(ctx, filteredBucket, desc)

		if desc && filteredSpan.CompareEnds(&keyCtx, &left) != 0 {
			// We need to add an empty bucket after the new bucket.
			ub := h.getPrevUpperBound(ctx, filteredSpan.EndKey(), filteredSpan.EndBoundary(), colOffset)
			filtered.addEmptyBucket(ctx, ub, desc)
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
			filtered.addEmptyBucket(ctx, iter.inclusiveLowerBound(ctx), desc)
		} else if lastBucket := filtered.buckets[len(filtered.buckets)-1]; lastBucket.NumRange != 0 {
			iter.setIdx(0)
			span := sb.makeSpanFromBucket(ctx, &iter)
			ub := h.getPrevUpperBound(ctx, span.EndKey(), span.EndBoundary(), colOffset)
			filtered.addEmptyBucket(ctx, ub, desc)
		}

		// Reverse the buckets so they are in ascending order.
		for i := 0; i < len(filtered.buckets)/2; i++ {
			j := len(filtered.buckets) - 1 - i
			filtered.buckets[i], filtered.buckets[j] = filtered.buckets[j], filtered.buckets[i]
		}
	}

	return filtered
}

// Filter filters the histogram according to the given constraint, and returns
// a new histogram with the results. CanFilter should be called first to
// validate that c can filter the histogram.
func (h *Histogram) Filter(ctx context.Context, c *constraint.Constraint) *Histogram {
	colOffset, exactPrefix, ok := h.CanFilter(ctx, c)
	if !ok {
		panic(errors.AssertionFailedf("column mismatch"))
	}
	prefix := make([]tree.Datum, colOffset)
	for i := range prefix {
		prefix[i] = c.Spans.Get(0).StartKey().Value(i)
	}
	desc := c.Columns.Get(colOffset).Descending()

	return h.filter(ctx, c.Spans.Count(), c.Spans.Get, desc, colOffset, exactPrefix, prefix, c.Columns)
}

// InvertedFilter filters the histogram according to the given inverted
// constraint, and returns a new histogram with the results.
func (h *Histogram) InvertedFilter(ctx context.Context, spans inverted.Spans) *Histogram {
	var columns constraint.Columns
	columns.InitSingle(opt.MakeOrderingColumn(h.col, false /* desc */))
	return h.filter(
		ctx,
		len(spans),
		func(idx int) *constraint.Span {
			return makeSpanFromInvertedSpan(spans[idx])
		},
		false, /* desc */
		0,     /* exactPrefix */
		0,     /* colOffset */
		nil,   /* prefix */
		columns,
	)
}

func makeSpanFromInvertedSpan(invSpan inverted.Span) *constraint.Span {
	var span constraint.Span
	// The statistics use the Bytes type for the encoded key, so we use DBytes
	// here.
	// TODO(mgartner): Use tree.DEncodedKey here instead of tree.DBytes.
	span.Init(
		constraint.MakeKey(tree.NewDBytes(tree.DBytes(invSpan.Start))),
		constraint.IncludeBoundary,
		constraint.MakeKey(tree.NewDBytes(tree.DBytes(invSpan.End))),
		constraint.ExcludeBoundary,
	)
	return &span
}

func (h *Histogram) getNextLowerBound(
	ctx context.Context, currentUpperBound tree.Datum,
) tree.Datum {
	nextLowerBound, ok := currentUpperBound.Next(ctx, h.evalCtx)
	if !ok {
		nextLowerBound = currentUpperBound
	}
	return nextLowerBound
}

func (h *Histogram) getPrevUpperBound(
	ctx context.Context,
	currentLowerBound constraint.Key,
	boundary constraint.SpanBoundary,
	colOffset int,
) tree.Datum {
	prevUpperBound := currentLowerBound.Value(colOffset)
	if boundary == constraint.IncludeBoundary {
		if prev, ok := prevUpperBound.Prev(ctx, h.evalCtx); ok {
			prevUpperBound = prev
		}
	}
	return prevUpperBound
}

func (h *Histogram) addEmptyBucket(ctx context.Context, upperBound tree.Datum, desc bool) {
	h.addBucket(ctx, cat.HistogramBucket{UpperBound: upperBound}, desc)
}

func (h *Histogram) addBucket(ctx context.Context, bucket cat.HistogramBucket, desc bool) {
	// Check whether we can combine this bucket with the previous bucket.
	if len(h.buckets) != 0 {
		lastBucket := &h.buckets[len(h.buckets)-1]
		lower, higher := lastBucket, &bucket
		if desc {
			lower, higher = &bucket, lastBucket
		}
		if lower.NumRange == 0 && lower.NumEq == 0 && higher.NumRange == 0 {
			lastBucket.NumEq = higher.NumEq
			lastBucket.UpperBound = higher.UpperBound
			return
		}
		cmp, err := lastBucket.UpperBound.Compare(ctx, h.evalCtx, bucket.UpperBound)
		if err != nil {
			panic(err)
		} else if cmp == 0 {
			lastBucket.NumEq = lower.NumEq + higher.NumRange + higher.NumEq
			lastBucket.NumRange = lower.NumRange
			return
		}
	}
	h.buckets = append(h.buckets, bucket)
}

// ApplySelectivity returns a histogram with the given selectivity applied. If
// the selectivity was 1 the returned histogram will be the same as before.
func (h *Histogram) ApplySelectivity(selectivity Selectivity) *Histogram {
	if selectivity == ZeroSelectivity {
		return nil
	}
	if selectivity == OneSelectivity {
		return h
	}
	h2 := *h
	h2.selectivity *= selectivity.AsFloat()
	return &h2
}

// histogramIter is a helper struct for iterating through the buckets in a
// histogram. It enables iterating both forward and backward through the
// buckets.
type histogramIter struct {
	h    *Histogram
	desc bool
	idx  int
	b    *cat.HistogramBucket
	elb  tree.Datum // exclusive lower bound
	lb   tree.Datum // inclusive lower bound
	eub  tree.Datum // exclusive upper bound
	ub   tree.Datum // inclusive upper bound
}

// init initializes a histogramIter to point to the first bucket of the given
// histogram. If desc is true, the iterator starts from the end of the
// histogram and moves backwards.
func (hi *histogramIter) init(h *Histogram, desc bool) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*hi = histogramIter{
		idx:  -1,
		h:    h,
		desc: desc,
	}
	if desc {
		hi.idx = h.bucketCount()
	}
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
	getBounds := func() (elb, lb, eub, ub tree.Datum) {
		hi.b = &hi.h.buckets[hi.idx]
		ub = hi.b.UpperBound
		if hi.idx == 0 {
			lb = ub
		} else {
			elb = hi.h.upperBound(hi.idx - 1)
		}
		// We return an exclusive upper bound, eub, even though it is always nil
		// because it makes it easier to handle both the iter.desc=true and
		// iter.desc=false cases below.
		return elb, lb, nil /* eub */, ub
	}

	if hi.desc {
		hi.idx--
		if hi.idx < 0 {
			return false
		}
		// If iter.desc=true, the lower bounds are greater than the upper
		// bounds. Either hi.eub or hi.ub will be set, hi.elb will always be
		// nil, and hi.lb will always be non-nil.
		hi.eub, hi.ub, hi.elb, hi.lb = getBounds()
	} else {
		hi.idx++
		if hi.idx >= hi.h.bucketCount() {
			return false
		}
		// If iter.desc=false, the lower bounds are less than the upper bounds.
		// Either hi.elb or hi.lb will be set, hi.eub will always be nil, and
		// hi.ub will always be non-nil.
		hi.elb, hi.lb, hi.eub, hi.ub = getBounds()
	}

	return true
}

// lowerBound returns the lower bound of the current bucket, and whether the
// bound is inclusive or exclusive. It will never allocate a new datum.
func (hi *histogramIter) lowerBound() (tree.Datum, constraint.SpanBoundary) {
	if hi.lb != nil {
		return hi.lb, constraint.IncludeBoundary
	}
	return hi.elb, constraint.ExcludeBoundary
}

// inclusiveLowerBound returns the inclusive lower bound of the current bucket.
// It may allocate a new datum.
func (hi *histogramIter) inclusiveLowerBound(ctx context.Context) tree.Datum {
	if hi.lb == nil {
		// hi.lb is only nil if iter.desc=false (see histogramIter.next), which
		// means that the lower bounds are less than the upper bounds. So the
		// inclusive lower bound is greater than the exclusive lower bound. For
		// example, the range (/10 - /20] is equivalent to [/11 - /20].
		hi.lb = hi.h.getNextLowerBound(ctx, hi.elb)
	}
	return hi.lb
}

// upperBound returns the upper bound of the current bucket, and whether the
// bound is inclusive or exclusive. It will never allocate a new datum.
func (hi *histogramIter) upperBound() (tree.Datum, constraint.SpanBoundary) {
	if hi.ub != nil {
		return hi.ub, constraint.IncludeBoundary
	}
	return hi.eub, constraint.ExcludeBoundary
}

// inclusiveUpperBound returns the inclusive upper bound of the current bucket.
// It may allocate a new datum.
func (hi *histogramIter) inclusiveUpperBound(ctx context.Context) tree.Datum {
	if hi.ub == nil {
		// hi.ub is only nil if iter.desc=true (see histogramIter.next), which
		// means that the lower bounds are greater than the upper bounds. So the
		// inclusive upper bound is greater than the exclusive upper bound. For
		// example, the range [/20 - /10) is equivalent to [/20 - /11].
		hi.ub = hi.h.getNextLowerBound(ctx, hi.eub)
	}
	return hi.ub
}

type spanBuilder struct {
	startScratch []tree.Datum
	endScratch   []tree.Datum
}

func (sb *spanBuilder) init(prefix []tree.Datum) {
	n := len(prefix) + 1
	d := make([]tree.Datum, 2*n)
	copy(d, prefix)
	copy(d[n:], prefix)
	sb.startScratch = d[:n:n]
	sb.endScratch = d[n:]
}

// makeSpanFromBucket constructs a constraint.Span from iter's current histogram
// bucket.
//
// WARNING: The returned span is only valid until this method is invoked again
// on the same spanBuilder. This is because it reuses scratch slices in the
// spanBuilder to reduce allocations when building span keys.
func (sb *spanBuilder) makeSpanFromBucket(
	ctx context.Context, iter *histogramIter,
) (span constraint.Span) {
	start, startBoundary := iter.lowerBound()
	end, endBoundary := iter.upperBound()
	cmp, err := start.Compare(ctx, iter.h.evalCtx, end)
	if err != nil {
		panic(err)
	}
	if cmp == 0 &&
		(startBoundary == constraint.IncludeBoundary || endBoundary == constraint.IncludeBoundary) {
		// If the start and ends are equal and one of the boundaries is
		// inclusive, the other boundary should be inclusive.
		startBoundary = constraint.IncludeBoundary
		endBoundary = constraint.IncludeBoundary
	}
	sb.startScratch[len(sb.startScratch)-1] = start
	sb.endScratch[len(sb.endScratch)-1] = end
	span.Init(
		constraint.MakeCompositeKey(sb.startScratch...),
		startBoundary,
		constraint.MakeCompositeKey(sb.endScratch...),
		endBoundary,
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
//
//	[/0 - /5]   => {NumEq: 1, NumRange: 5, UpperBound: 5}
//	[/2 - /10]  => {NumEq: 5, NumRange: 8, UpperBound: 10}
//	[/20 - /30] => error
//
// Note that the calculations for NumEq and NumRange depend on the data type.
// For discrete data types such as integers and dates, it is always possible
// to assign a non-zero value for NumEq as long as NumEq and NumRange were
// non-zero in the original bucket. For continuous types such as floats,
// NumEq will be zero unless the filtered bucket includes the original upper
// bound. For example, given the same bucket as in the above example, but with
// floating point values instead of integers:
//
//	[/0 - /5]   => {NumEq: 0, NumRange: 5, UpperBound: 5.0}
//	[/2 - /10]  => {NumEq: 5, NumRange: 8, UpperBound: 10.0}
//	[/20 - /30] => error
//
// For non-numeric types such as strings, it is not possible to estimate
// the size of NumRange if the bucket is cut off in the middle. In this case,
// we use the heuristic that NumRange is reduced by half.
func getFilteredBucket(
	iter *histogramIter, keyCtx *constraint.KeyContext, filteredSpan *constraint.Span, colOffset int,
) cat.HistogramBucket {
	spanLowerBound := filteredSpan.StartKey().Value(colOffset)
	spanUpperBound := filteredSpan.EndKey().Value(colOffset)
	bucketLowerBound := iter.inclusiveLowerBound(keyCtx.Ctx)
	bucketUpperBound := iter.inclusiveUpperBound(keyCtx.Ctx)
	b := iter.b

	// Check that the given span is contained in the bucket.
	cmpSpanStartBucketStart, err := spanLowerBound.Compare(keyCtx.Ctx, keyCtx.EvalCtx, bucketLowerBound)
	if err != nil {
		panic(err)
	}
	cmpSpanEndBucketEnd, err := spanUpperBound.Compare(keyCtx.Ctx, keyCtx.EvalCtx, bucketUpperBound)
	if err != nil {
		panic(err)
	}
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
	cmp, err := spanLowerBound.Compare(keyCtx.Ctx, keyCtx.EvalCtx, spanUpperBound)
	if err != nil {
		panic(err)
	}
	isEqualityCondition := cmp == 0

	// Determine whether this span includes the original upper bound of the
	// bucket.
	var keyLength int
	var keyBoundaryInclusive bool
	if !iter.desc {
		keyLength = filteredSpan.EndKey().Length()
		keyBoundaryInclusive = filteredSpan.EndBoundary() == constraint.IncludeBoundary
		cmp = cmpSpanEndBucketEnd
	} else {
		keyLength = filteredSpan.StartKey().Length()
		keyBoundaryInclusive = filteredSpan.StartBoundary() == constraint.IncludeBoundary
		cmp = cmpSpanStartBucketStart
	}
	includesOriginalUpperBound := cmp == 0 && ((colOffset < keyLength-1) || keyBoundaryInclusive)

	// Calculate the new value for numEq.
	var numEq float64
	if includesOriginalUpperBound {
		numEq = b.NumEq
	} else {
		if isEqualityCondition {
			// This span represents an equality condition with a value in the range
			// of this bucket. Use the distinct count of the bucket to estimate the
			// selectivity of the equality condition.
			selectivity := OneSelectivity
			if b.DistinctRange > 1 {
				selectivity = MakeSelectivity(1 / b.DistinctRange)
			}
			numEq = selectivity.AsFloat() * b.NumRange
		} else if ok && rangeBefore > 0 && isDiscrete(bucketLowerBound.ResolvedType()) {
			// If we were successful in finding the ranges before and after filtering
			// and the data type is discrete (e.g., integer, date, or timestamp), we
			// can assign some of the old NumRange to the new NumEq.
			numEq = b.NumRange / rangeBefore
		}
	}

	// Calculate the new value for numRange.
	var numRange float64
	switch {
	case isEqualityCondition:
		numRange = 0
	case ok && rangeBefore > 0:
		// If we were successful in finding the ranges before and after
		// filtering, calculate the fraction of values that should be assigned
		// to the new bucket.
		numRange = b.NumRange * rangeAfter / rangeBefore
		if !math.IsNaN(numRange) {
			break
		}
		// If the new value is NaN, fallthrough to the default case to estimate
		// the numRange.
		fallthrough
	default:
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
	return cat.HistogramBucket{
		NumEq:         numEq,
		NumRange:      numRange,
		DistinctRange: distinctCountRange,
		UpperBound:    upperBound,
	}
}

// getRangesBeforeAndAfter returns the size of the before and after ranges based
// on the lower and upper bounds provided. If swap is true, the upper and lower
// bounds of both ranges are swapped. Returns ok=true if these range sizes are
// calculated successfully, and false otherwise. The calculations for
// rangeBefore and rangeAfter are datatype dependent.
//
// For numeric types, we can simply find the difference between the lower and
// upper bounds for rangeBefore/rangeAfter.
//
// For non-numeric types, we can convert each bound into sorted key bytes (CRDB
// key representation) to find their range. As we do need a lot of precision in
// our range estimate, we can remove the common prefix between the lower and
// upper bounds, and limit the byte array to 8 bytes. This also simplifies our
// implementation since we won't need to handle an arbitrary length of bounds.
// Following the conversion, we must zero extend the byte arrays to ensure the
// length is uniform between lower and upper bounds. This process is highlighted
// below, where [\bear - \bobcat] represents the before range and
// [\bluejay - \boar] represents the after range.
//
//	bear    := [18  98  101 97  114 0   1          ]
//	        => [101 97  114 0   0   0   0   0      ]
//
//	bluejay := [18  98  108 117 101 106 97  121 0 1]
//	        => [108 117 101 106 97  121 0   0      ]
//
//	boar    := [18  98  111 97  114 0   1          ]
//	        => [111 97  114 0   0   0   0   0      ]
//
//	bobcat  := [18  98  111 98  99  97  116 0   1  ]
//	        => [111 98  99  97  116 0   0   0      ]
//
// We can now find the range before/after by finding the difference between
// the lower and upper bounds:
//
//		 rangeBefore := [111 98  99  97  116 0   1   0] -
//	                 [101 97  114 0   1   0   0   0]
//
//	  rangeAfter  := [111 97  114 0   1   0   0   0] -
//	                 [108 117 101 106 97  121 0   1]
//
// Subtracting the uint64 representations of the byte arrays, the resulting
// rangeBefore and rangeAfter are:
//
//		 rangeBefore := 8,026,086,756,136,779,776 - 7,305,245,414,897,221,632
//	              := 720,841,341,239,558,100
//
//		 rangeAfter := 8,025,821,355,276,500,992 - 7,815,264,235,947,622,400
//	             := 210,557,119,328,878,600
func getRangesBeforeAndAfter(
	beforeLowerBound, beforeUpperBound, afterLowerBound, afterUpperBound tree.Datum, swap bool,
) (rangeBefore, rangeAfter float64, ok bool) {
	// If the data types don't match, don't bother trying to calculate the range
	// sizes. This should almost never happen, but we want to avoid type
	// assertion errors below.
	typesMatch :=
		beforeLowerBound.ResolvedType().Equivalent(beforeUpperBound.ResolvedType()) &&
			beforeUpperBound.ResolvedType().Equivalent(afterLowerBound.ResolvedType()) &&
			afterLowerBound.ResolvedType().Equivalent(afterUpperBound.ResolvedType())
	if !typesMatch {
		return 0, 0, false
	}

	if swap {
		beforeLowerBound, beforeUpperBound = beforeUpperBound, beforeLowerBound
		afterLowerBound, afterUpperBound = afterUpperBound, afterLowerBound
	}

	// The calculations below assume that all bounds are inclusive.
	// TODO(rytaft): handle more types here.
	switch beforeLowerBound.ResolvedType().Family() {
	case types.IntFamily:
		rangeBefore = float64(*beforeUpperBound.(*tree.DInt)) - float64(*beforeLowerBound.(*tree.DInt))
		rangeAfter = float64(*afterUpperBound.(*tree.DInt)) - float64(*afterLowerBound.(*tree.DInt))
		return rangeBefore, rangeAfter, true

	case types.DateFamily:
		lowerBefore := beforeLowerBound.(*tree.DDate)
		upperBefore := beforeUpperBound.(*tree.DDate)
		lowerAfter := afterLowerBound.(*tree.DDate)
		upperAfter := afterUpperBound.(*tree.DDate)
		if lowerBefore.IsFinite() && upperBefore.IsFinite() && lowerAfter.IsFinite() && upperAfter.IsFinite() {
			rangeBefore = float64(upperBefore.PGEpochDays()) - float64(lowerBefore.PGEpochDays())
			rangeAfter = float64(upperAfter.PGEpochDays()) - float64(lowerAfter.PGEpochDays())
			return rangeBefore, rangeAfter, true
		}
		return 0, 0, false

	case types.DecimalFamily:
		lowerBefore, err := beforeLowerBound.(*tree.DDecimal).Float64()
		if err != nil {
			return 0, 0, false
		}
		upperBefore, err := beforeUpperBound.(*tree.DDecimal).Float64()
		if err != nil {
			return 0, 0, false
		}
		lowerAfter, err := afterLowerBound.(*tree.DDecimal).Float64()
		if err != nil {
			return 0, 0, false
		}
		upperAfter, err := afterUpperBound.(*tree.DDecimal).Float64()
		if err != nil {
			return 0, 0, false
		}
		rangeBefore = upperBefore - lowerBefore
		rangeAfter = upperAfter - lowerAfter
		return rangeBefore, rangeAfter, true

	case types.FloatFamily:
		rangeBefore = float64(*beforeUpperBound.(*tree.DFloat)) - float64(*beforeLowerBound.(*tree.DFloat))
		rangeAfter = float64(*afterUpperBound.(*tree.DFloat)) - float64(*afterLowerBound.(*tree.DFloat))
		return rangeBefore, rangeAfter, true

	case types.TimestampFamily:
		lowerBefore := beforeLowerBound.(*tree.DTimestamp).Time
		upperBefore := beforeUpperBound.(*tree.DTimestamp).Time
		lowerAfter := afterLowerBound.(*tree.DTimestamp).Time
		upperAfter := afterUpperBound.(*tree.DTimestamp).Time
		rangeBefore = float64(upperBefore.Sub(lowerBefore))
		rangeAfter = float64(upperAfter.Sub(lowerAfter))
		return rangeBefore, rangeAfter, true

	case types.TimestampTZFamily:
		lowerBefore := beforeLowerBound.(*tree.DTimestampTZ).Time
		upperBefore := beforeUpperBound.(*tree.DTimestampTZ).Time
		lowerAfter := afterLowerBound.(*tree.DTimestampTZ).Time
		upperAfter := afterUpperBound.(*tree.DTimestampTZ).Time
		rangeBefore = float64(upperBefore.Sub(lowerBefore))
		rangeAfter = float64(upperAfter.Sub(lowerAfter))
		return rangeBefore, rangeAfter, true

	case types.TimeFamily:
		lowerBefore := beforeLowerBound.(*tree.DTime)
		upperBefore := beforeUpperBound.(*tree.DTime)
		lowerAfter := afterLowerBound.(*tree.DTime)
		upperAfter := afterUpperBound.(*tree.DTime)
		rangeBefore = float64(*upperBefore) - float64(*lowerBefore)
		rangeAfter = float64(*upperAfter) - float64(*lowerAfter)
		return rangeBefore, rangeAfter, true

	case types.TimeTZFamily:
		// timeTZOffsetSecsRange is the total number of possible values for offset.
		timeTZOffsetSecsRange := timetz.MaxTimeTZOffsetSecs - timetz.MinTimeTZOffsetSecs + 1

		// Find the ranges in microseconds based on the absolute times of the range
		// boundaries.
		lowerBefore := beforeLowerBound.(*tree.DTimeTZ)
		upperBefore := beforeUpperBound.(*tree.DTimeTZ)
		lowerAfter := afterLowerBound.(*tree.DTimeTZ)
		upperAfter := afterUpperBound.(*tree.DTimeTZ)
		rangeBefore = float64(upperBefore.ToTime().Sub(lowerBefore.ToTime()) / time.Microsecond)
		rangeAfter = float64(upperAfter.ToTime().Sub(lowerAfter.ToTime()) / time.Microsecond)

		// Account for the offset.
		rangeBefore *= float64(timeTZOffsetSecsRange)
		rangeAfter *= float64(timeTZOffsetSecsRange)
		rangeBefore += float64(upperBefore.OffsetSecs - lowerBefore.OffsetSecs)
		rangeAfter += float64(upperAfter.OffsetSecs - lowerAfter.OffsetSecs)
		return rangeBefore, rangeAfter, true

	case types.StringFamily, types.BytesFamily, types.UuidFamily, types.INetFamily:
		// For non-numeric types, convert the datums to encoded keys to
		// approximate the range. We utilize an array to reduce repetitive code.
		boundArr := [4]tree.Datum{
			beforeLowerBound, beforeUpperBound, afterLowerBound, afterUpperBound,
		}
		var boundArrByte [4][]byte

		for i := range boundArr {
			var err error
			// Encode each bound value into a sortable byte format.
			boundArrByte[i], err = keyside.Encode(nil, boundArr[i], encoding.Ascending)
			if err != nil {
				return 0, 0, false
			}
		}

		// Remove common prefix.
		ind := getCommonPrefix(boundArrByte)
		for i := range boundArrByte {
			// Fix length of byte arrays to 8 bytes.
			boundArrByte[i] = getFixedLenArr(boundArrByte[i], ind, 8 /* fixLen */)
		}

		rangeBefore = float64(binary.BigEndian.Uint64(boundArrByte[1]) -
			binary.BigEndian.Uint64(boundArrByte[0]))
		rangeAfter = float64(binary.BigEndian.Uint64(boundArrByte[3]) -
			binary.BigEndian.Uint64(boundArrByte[2]))

		return rangeBefore, rangeAfter, true

	default:
		// Range calculations are not supported for the given type family.
		return 0, 0, false
	}
}

// isDiscrete returns true if the given data type is discrete.
func isDiscrete(typ *types.T) bool {
	switch typ.Family() {
	case types.IntFamily, types.DateFamily, types.TimestampFamily, types.TimestampTZFamily:
		return true
	}
	return false
}

// getCommonPrefix returns the first index where the value at said index differs
// across all byte arrays in byteArr. byteArr must contain at least one element
// to compute a common prefix.
func getCommonPrefix(byteArr [4][]byte) int {
	// Checks if the current value at index is the same between all byte arrays.
	currIndMatching := func(ind int) bool {
		for i := 0; i < len(byteArr); i++ {
			if ind >= len(byteArr[i]) || byteArr[0][ind] != byteArr[i][ind] {
				return false
			}
		}
		return true
	}

	ind := 0
	for currIndMatching(ind) {
		ind++
	}

	return ind
}

// getFixedLenArr returns a byte array of size fixLen starting from specified
// index within the original byte array.
func getFixedLenArr(byteArr []byte, ind, fixLen int) []byte {

	if len(byteArr) <= 0 {
		panic(errors.AssertionFailedf("byteArr must have at least one element"))
	}

	if fixLen <= 0 {
		panic(errors.AssertionFailedf("desired fixLen must be greater than 0"))
	}

	if ind < 0 || ind > len(byteArr) {
		panic(errors.AssertionFailedf("ind must be contained within byteArr"))
	}

	// If byteArr is insufficient to hold desired size of byte array (fixLen),
	// allocate new array, else return subarray of size fixLen starting at ind
	if len(byteArr) < ind+fixLen {
		byteArrFix := make([]byte, fixLen)
		copy(byteArrFix, byteArr[ind:])
		return byteArrFix
	}

	return byteArr[ind : ind+fixLen]
}

// histogramWriter prints histograms with the following formatting:
//
//	NumRange1    NumEq1     NumRange2    NumEq2    ....
//
// <----------- UpperBound1 ----------- UpperBound2 ....
//
// For example:
//
//	0  1  90  10   0  20
//
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

func (w *histogramWriter) init(selectivity float64, buckets []cat.HistogramBucket) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*w = histogramWriter{
		cells: [][]string{
			make([]string, len(buckets)*2),
			make([]string, len(buckets)*2),
		},
		colWidths: make([]int, len(buckets)*2),
	}

	for i, b := range buckets {
		w.cells[counts][i*2] = fmt.Sprintf(" %.5g ", b.NumRange*selectivity)
		w.cells[counts][i*2+1] = fmt.Sprintf("%.5g", b.NumEq*selectivity)
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
