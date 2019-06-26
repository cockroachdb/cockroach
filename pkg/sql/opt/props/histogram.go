// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package props

import (
	"bytes"
	"fmt"
	"io"

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
type Histogram struct {
	evalCtx *tree.EvalContext
	col     opt.ColumnID
	buckets []HistogramBucket
}

// HistogramBucket contains the data for a single bucket in a Histogram. Note
// that NumEq and NumRange are floats so the statisticsBuilder can apply
// filters to the histogram.
type HistogramBucket struct {
	// NumEq is the estimated number of values equal to UpperBound.
	NumEq float64

	// NumRange is the estimated number of values in this bucket not equal to
	// UpperBound.
	NumRange float64

	// UpperBound is the largest value in this bucket. The lower bound can be
	// inferred based on the upper bound of the previous bucket in the histogram.
	UpperBound tree.Datum
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
	if len(buckets) == 0 {
		return
	}
	h.buckets = make([]HistogramBucket, len(buckets))
	for i := range buckets {
		h.buckets[i].NumEq = float64(buckets[i].NumEq)
		h.buckets[i].NumRange = float64(buckets[i].NumRange)
		h.buckets[i].UpperBound = buckets[i].UpperBound
	}
}

// Copy returns a deep copy of the histogram.
func (h *Histogram) Copy() *Histogram {
	buckets := make([]HistogramBucket, len(h.buckets))
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
func (h *Histogram) Bucket(i int) *HistogramBucket {
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

// CanFilter returns true if the given constraint can filter the histogram.
// This is the case if there is only one constrained column in c, it is
// ascending, and it matches the column of the histogram.
func (h *Histogram) CanFilter(c *constraint.Constraint) bool {
	if c.ConstrainedColumns(h.evalCtx) != 1 || c.Columns.Get(0).ID() != h.col {
		return false
	}
	if c.Columns.Get(0).Descending() {
		return false
	}
	return true
}

// Filter filters the histogram according to the given constraint, and returns
// a new histogram with the results. CanFilter should be called first to
// validate that c can filter the histogram.
func (h *Histogram) Filter(c *constraint.Constraint) *Histogram {
	// TODO(rytaft): add support for index constraints with multiple ascending
	// or descending columns.
	if c.ConstrainedColumns(h.evalCtx) != 1 && c.Columns.Get(0).ID() != h.col {
		panic(errors.AssertionFailedf("column mismatch"))
	}
	if c.Columns.Get(0).Descending() {
		panic(errors.AssertionFailedf("histogram filter with descending constraint not yet supported"))
	}

	filtered := &Histogram{
		evalCtx: h.evalCtx,
		col:     h.col,
		buckets: make([]HistogramBucket, 0, len(h.buckets)),
	}
	if len(h.buckets) == 0 {
		return filtered
	}

	// The lower bound for the first bucket is the smallest possible value for
	// the data type.
	// TODO(rytaft): Is this necessary? The first bucket should have a zero value
	// for NumRange.
	lowerBound, ok := h.buckets[0].UpperBound.Min(h.evalCtx)
	if !ok {
		lowerBound = h.buckets[0].UpperBound
	}

	// Use variation on merge sort, because both sets of buckets and spans are
	// ordered and non-overlapping.
	// TODO(rytaft): use binary search to find the first bucket.

	bucIndex := 0
	spanIndex := 0
	keyCtx := constraint.KeyContext{EvalCtx: h.evalCtx}
	keyCtx.Columns.InitSingle(opt.MakeOrderingColumn(h.col, false /* descending */))

	for bucIndex < h.BucketCount() && spanIndex < c.Spans.Count() {
		bucket := h.Bucket(bucIndex)
		// Convert the bucket to a span in order to take advantage of the
		// constraint library.
		var left constraint.Span
		left.Init(
			constraint.MakeKey(lowerBound),
			constraint.IncludeBoundary,
			constraint.MakeKey(bucket.UpperBound),
			constraint.IncludeBoundary,
		)

		right := c.Spans.Get(spanIndex)

		if left.StartsAfter(&keyCtx, right) {
			spanIndex++
			continue
		}

		filteredSpan := left
		if !filteredSpan.TryIntersectWith(&keyCtx, right) {
			filtered.addEmptyBucket(bucket.UpperBound)
			lowerBound = h.getNextLowerBound(bucket.UpperBound)
			bucIndex++
			continue
		}

		filteredBucket := bucket
		if filteredSpan.Compare(&keyCtx, &left) != 0 {
			// The bucket was cut off in the middle. Get the resulting filtered
			// bucket.
			filteredBucket = bucket.getFilteredBucket(&keyCtx, &filteredSpan, lowerBound)
			if filteredSpan.CompareStarts(&keyCtx, &left) != 0 {
				// We need to add an empty bucket before the new bucket.
				emptyBucketUpperBound := filteredSpan.StartKey().Value(0)
				if filteredSpan.StartBoundary() == constraint.IncludeBoundary {
					if prev, ok := emptyBucketUpperBound.Prev(h.evalCtx); ok {
						emptyBucketUpperBound = prev
					}
				}
				filtered.addEmptyBucket(emptyBucketUpperBound)
			}
		}
		filtered.addBucket(filteredBucket)

		// Skip past whichever span ends first, or skip past both if they have
		// the same endpoint.
		cmp := left.CompareEnds(&keyCtx, right)
		if cmp <= 0 {
			lowerBound = h.getNextLowerBound(bucket.UpperBound)
			bucIndex++
		}
		if cmp >= 0 {
			spanIndex++
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

func (h *Histogram) addEmptyBucket(upperBound tree.Datum) {
	h.addBucket(&HistogramBucket{UpperBound: upperBound})
}

func (h *Histogram) addBucket(bucket *HistogramBucket) {
	// Check whether we can combine this bucket with the previous bucket.
	if len(h.buckets) != 0 {
		lastBucket := &h.buckets[len(h.buckets)-1]
		if lastBucket.NumRange == 0 && lastBucket.NumEq == 0 && bucket.NumRange == 0 {
			lastBucket.NumEq = bucket.NumEq
			lastBucket.UpperBound = bucket.UpperBound
			return
		}
		if lastBucket.UpperBound.Compare(h.evalCtx, bucket.UpperBound) == 0 {
			lastBucket.NumEq += bucket.NumRange + bucket.NumEq
			return
		}
	}
	h.buckets = append(h.buckets, *bucket)
}

// ApplySelectivity reduces the size of each histogram bucket according to
// the given selectivity.
func (h *Histogram) ApplySelectivity(selectivity float64) {
	for i := range h.buckets {
		h.buckets[i].NumEq *= selectivity
		h.buckets[i].NumRange *= selectivity
	}
}

// getFilteredBucket filters the histogram bucket according to the given span,
// and returns a new bucket with the results.
func (b *HistogramBucket) getFilteredBucket(
	keyCtx *constraint.KeyContext, filteredSpan *constraint.Span, lowerBoundBefore tree.Datum,
) *HistogramBucket {
	spanLowerBound := filteredSpan.StartKey().Value(0)
	spanUpperBound := filteredSpan.EndKey().Value(0)
	numEqUpperBound := float64(0)
	if filteredSpan.EndBoundary() == constraint.IncludeBoundary &&
		b.UpperBound.Compare(keyCtx.EvalCtx, spanUpperBound) == 0 {
		numEqUpperBound = b.NumEq
	}

	var rangeBefore, rangeAfter float64
	var includeUpperBoundInRange bool
	ok := true
	// TODO(rytaft): handle more types here.
	switch spanLowerBound.ResolvedType().Family() {
	case types.IntFamily:
		rangeBefore = float64(*b.UpperBound.(*tree.DInt)) - float64(*lowerBoundBefore.(*tree.DInt))
		rangeAfter = float64(*spanUpperBound.(*tree.DInt)) - float64(*spanLowerBound.(*tree.DInt))
		includeUpperBoundInRange = filteredSpan.EndBoundary() == constraint.IncludeBoundary &&
			b.UpperBound.Compare(keyCtx.EvalCtx, spanUpperBound) != 0

	case types.DateFamily:
		lowerBefore := lowerBoundBefore.(*tree.DDate)
		upperBefore := b.UpperBound.(*tree.DDate)
		lowerAfter := spanLowerBound.(*tree.DDate)
		upperAfter := spanUpperBound.(*tree.DDate)
		if lowerBefore.IsFinite() && upperBefore.IsFinite() &&
			lowerAfter.IsFinite() && upperAfter.IsFinite() {
			rangeBefore = float64(upperBefore.PGEpochDays()) - float64(lowerBefore.PGEpochDays())
			rangeAfter = float64(upperAfter.PGEpochDays()) - float64(lowerAfter.PGEpochDays())
			includeUpperBoundInRange = filteredSpan.EndBoundary() == constraint.IncludeBoundary &&
				b.UpperBound.Compare(keyCtx.EvalCtx, spanUpperBound) != 0
		} else {
			ok = false
		}

	case types.DecimalFamily:
		lowerBefore, err := lowerBoundBefore.(*tree.DDecimal).Float64()
		if err != nil {
			ok = false
			break
		}
		upperBefore, err := b.UpperBound.(*tree.DDecimal).Float64()
		if err != nil {
			ok = false
			break
		}
		lowerAfter, err := spanLowerBound.(*tree.DDecimal).Float64()
		if err != nil {
			ok = false
			break
		}
		upperAfter, err := spanUpperBound.(*tree.DDecimal).Float64()
		if err != nil {
			ok = false
			break
		}
		rangeBefore = upperBefore - lowerBefore
		rangeAfter = upperAfter - lowerAfter

	case types.FloatFamily:
		rangeBefore = float64(*b.UpperBound.(*tree.DFloat)) - float64(*lowerBoundBefore.(*tree.DFloat))
		rangeAfter = float64(*spanUpperBound.(*tree.DFloat)) - float64(*spanLowerBound.(*tree.DFloat))

	case types.TimestampFamily:
		lowerBefore := lowerBoundBefore.(*tree.DTimestamp).Time
		upperBefore := b.UpperBound.(*tree.DTimestamp).Time
		lowerAfter := spanLowerBound.(*tree.DTimestamp).Time
		upperAfter := spanUpperBound.(*tree.DTimestamp).Time
		rangeBefore = float64(upperBefore.Sub(lowerBefore))
		rangeAfter = float64(upperAfter.Sub(lowerAfter))

	case types.TimestampTZFamily:
		lowerBefore := lowerBoundBefore.(*tree.DTimestampTZ).Time
		upperBefore := b.UpperBound.(*tree.DTimestampTZ).Time
		lowerAfter := spanLowerBound.(*tree.DTimestampTZ).Time
		upperAfter := spanUpperBound.(*tree.DTimestampTZ).Time
		rangeBefore = float64(upperBefore.Sub(lowerBefore))
		rangeAfter = float64(upperAfter.Sub(lowerAfter))

	default:
		ok = false
	}

	var numRange float64
	if ok && rangeBefore > 0 {
		if includeUpperBoundInRange {
			numEqUpperBound = b.NumRange / rangeBefore
		}
		numRange = b.NumRange * rangeAfter / rangeBefore
	} else if b.UpperBound.Compare(keyCtx.EvalCtx, spanLowerBound) == 0 {
		// This span represents an equality condition with the upper bound.
		numRange = 0
	} else {
		// In the absence of any information, assume we reduced the size of the
		// bucket by half.
		numRange = 0.5 * b.NumRange
	}

	return &HistogramBucket{
		NumEq:      numEqUpperBound,
		NumRange:   numRange,
		UpperBound: spanUpperBound,
	}
}

// histogramWriter prints histograms with the following formatting:
//   NumRange1    NumEq1     NumRange2    NumEq2    ....
// <----------- UpperBound1 ----------- UpperBound2 ....
//
// For example:
//   0  0  90  10  0  20
// <--- 0 ---- 10 --- 20
//
// This describes a histogram with 3 buckets in which the first bucket is
// empty. There are 90 values between 0 and 10, 10 values equal to 10, and 20
// values equal to 20.
type histogramWriter struct {
	cells     [][]string
	colWidths []int
}

const (
	// These constants describe the two rows that are printed.
	counts = iota
	boundaries
)

func (w *histogramWriter) init(buckets []HistogramBucket) {
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
