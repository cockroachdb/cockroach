// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"bytes"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
)

// RowIndex is used as a set element. It is already deduped.
type RowIndex = rowcontainer.RowIndex

// The abstractions here are geared towards evaluating set expressions over
// an inverted index, which consists of an inverted column followed by the
// primary key of the table. The set expressions involve union and intersection
// over operands. The leaf level operands are sets of primary keys. The evaluator
// below takes as input a set expression where the leaf level operands are represented
// as spans on the first column of the inverted index. The evaluator will be fed
// these set elements as the inverted index is scanned, and will route the
// set element to all the sets to which it belongs (since spans can be overlapping).
// Once the scan is complete, the expression will be evaluated.
//
// Consider the following expression:
// (x @> '{"a":1, "b":2}'::json or x ? 'g') and (x @> '{"a":1, "c":3}'::json or x ? 'h')
// where x is a JSON column that has been indexed, @> is the containment operator and
// the ? operator checks existence as a top-level key in the JSON value.
//
// Abstractly, say the spans are
// A for "a":1
// B for "b":2
// C for "c":3
// G for ? 'g'
// H for ? 'h'
//
// Then the expression to be computed is represented as
// ((A \intersection B) \union G) \intersection ((A \intersection C) \union H)
//
// For convenience of representation and evaluation, the expression is represented
// using reverse polish notation as a []RPExprElement.

// RPExprElement is an element in the Reverse Polish notation expression.
// It is implemented by Key and RPSetOperator.
//
// TODO(sumeer): refactor geoindex/index.go to use RPExprElement and
// RPSetOperator
type RPExprElement interface {
	rpExprElement()
}

// RPSetOperator is a set operator.
type RPSetOperator int

const (
	// RPSetUnion is the union operator.
	RPSetUnion RPSetOperator = iota + 1

	// RPSetIntersection is the intersection operator.
	RPSetIntersection
)

// rpExprElement implements the RPExprElement interface. This allows
// RPSetOperator to be used in RPInvertedIndexExpr.
func (o RPSetOperator) rpExprElement() {}

// encInvertedVal is the encoded form of a value in the inverted column. If
// the inverted column stores an encoded datum, the encoding is
// DatumEncoding_ASCENDING_KEY.
type encInvertedVal []byte

// encInvertedValSpan is a span of encInvertedVals and implements the
// RPExprElement interface.
type encInvertedValSpan struct {
	// [start, end) iff end is not-nil, else represents start.
	start, end encInvertedVal
}

func (e *encInvertedValSpan) rpExprElement() {}

// RPInvertedIndexExpr is a set expression to evaluate over the rows
// retrieved from the inverted index (minus the inverted column) for
// the inverted column spans specified in the operands.
type RPInvertedIndexExpr []RPExprElement

// invertedExprEvaluator evaluates a single expression. It should not be directly
// used -- see batchedInvertedExprEvaluator.
type invertedExprEvaluator struct {
	expr RPInvertedIndexExpr
	// If expr[i] is an operand, the corresponding set elements are in
	// operandSets[i]. These are populated by calls to addIndexRow() as
	// the inverted index is scanned.
	operandSets []setContainer
}

type spanAndOperandIndex struct {
	span         *encInvertedValSpan
	operandIndex int
}

// Spans are not in sorted order and can be overlapping.
func (ev *invertedExprEvaluator) getSpansAndOperandIndexes() []spanAndOperandIndex {
	var rv []spanAndOperandIndex
	for i, e := range ev.expr {
		if s, ok := e.(*encInvertedValSpan); ok {
			rv = append(rv, spanAndOperandIndex{
				span:         s,
				operandIndex: i,
			})
		}
	}
	ev.operandSets = make([]setContainer, len(ev.expr))
	return rv
}

// Adds a row to the given operand. Note that RowIndexes are not added in
// increasing numerical order, nor do they represent any ordering of the
// primary key of the table whose inverted index is being read. Also, the
// same rowIndex could be added repeatedly to an operand.
func (ev *invertedExprEvaluator) addIndexRow(operandIndex int, rowIndex RowIndex) {
	// If duplicates in a set become a memory problem in this build phase, we
	// could do periodic deduplication as we go. For now, we simply append to
	// the slice and dedup at the start of evaluate().
	ev.operandSets[operandIndex] = append(ev.operandSets[operandIndex], rowIndex)
}

// The return value is in increasing order of RowIndex.
func (ev *invertedExprEvaluator) evaluate() []RowIndex {
	// Sort and dedup the sets so that we can efficiently do set operations.
	for i, c := range ev.operandSets {
		if len(c) > 0 {
			sort.Sort(c)
			// Deduplicate
			set := c[:0]
			for j := range c {
				if len(set) > 0 && c[j] == set[len(set)-1] {
					continue
				}
				set = append(set, c[j])
			}
			ev.operandSets[i] = set
		}
	}

	// Evaluate the set expression.
	var stack []setContainer
	for i, elem := range ev.expr {
		switch e := elem.(type) {
		case *encInvertedValSpan:
			stack = append(stack, ev.operandSets[i])
		case RPSetOperator:
			op0, op1 := stack[len(stack)-1], stack[len(stack)-2]
			stack = stack[:len(stack)-2]
			switch e {
			case RPSetIntersection:
				op0 = intersect(op0, op1)
			case RPSetUnion:
				op0 = union(op0, op1)
			}
			stack = append(stack, op0)
		}
	}
	return stack[0]
}

// setContainer is a set of row indexes.
type setContainer []RowIndex

func (s setContainer) Len() int {
	return len(s)
}

func (s setContainer) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s setContainer) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func union(a, b setContainer) setContainer {
	var out setContainer
	var i, j int
	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			out = append(out, a[i])
			i++
		} else if a[i] > b[j] {
			out = append(out, b[j])
			j++
		} else {
			out = append(out, a[i])
			i++
			j++
		}
	}
	for ; i < len(a); i++ {
		out = append(out, a[i])
	}
	for ; j < len(b); j++ {
		out = append(out, b[j])
	}
	return out
}

func intersect(a, b setContainer) setContainer {
	var out setContainer
	var i, j int
	// TODO(sumeer): when one set is much larger than the other
	// it is more efficient to iterate over the smaller set
	// and seek into the larger set.
	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			i++
		} else if a[i] > b[j] {
			j++
		} else {
			out = append(out, a[i])
			i++
			j++
		}
	}
	return out
}

// Supporting struct for invertedValSpanBatch.
type exprAndOperand struct {
	exprNum      int
	operandIndex int
}

// invertedValSpanBatch tracks the expression num and operand index
// that needs rows from the inverted index span. A []invertedValSpanBatch
// with spans that are sorted and non-overlapping is used to route
// an added row to all the expressions and operands that need that row.
type invertedValSpanBatch struct {
	span           *encInvertedValSpan
	exprAndOperand []exprAndOperand
}

type spanBatchSliceSorter []invertedValSpanBatch

func (s spanBatchSliceSorter) Len() int {
	return len(s)
}

func (s spanBatchSliceSorter) Less(i, j int) bool {
	cmp := bytes.Compare(s[i].span.start, s[j].span.start)
	if cmp == 0 {
		cmp = bytes.Compare(s[i].span.end, s[j].span.end)
	}
	return cmp < 0
}

func (s spanBatchSliceSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// batchedInvertedExprEvaluator should be used for both:
// - selecting from an inverted index using an expression involving literals
//   and the indexed column. This will be a batch with a single element in
//   exprs.
// - Joins across two tables using the inverted index of one table.
//
// The batched evaluator can be reused by calling Reset(). In the build phase,
// append expressions directly to exprs. A nil RPInvertedIndexExpr is
// permitted, and is just a placeholder that will result in a nil []RowIndex
// in evaluate(). getSpans() must be called before calls to addIndexRow() even
// if the result is not needed -- it builds the fragmentedSpans used for
// routing the added rows.
type batchedInvertedExprEvaluator struct {
	exprs []RPInvertedIndexExpr
	// The evaluators for all the exprs.
	exprEvals []*invertedExprEvaluator
	// Spans here are in sorted order and non-pverlapping.
	fragmentedSpans []invertedValSpanBatch

	// Temporary state used for constructing fragmentedSpans. All spans here
	// have the same start key. They are not sorted by end key.
	pendingSpans []invertedValSpanBatch
}

func immediateSuccessor(a []byte) []byte {
	return append(a, '\x00')
}

func (b *batchedInvertedExprEvaluator) pendingLenWithSameEnd() int {
	length := 1
	for i := 1; i < len(b.pendingSpans); i++ {
		if bytes.Compare(b.pendingSpans[0].span.end, b.pendingSpans[i].span.end) != 0 {
			break
		}
		length++
	}
	return length
}

// Helper used in building fragmentedSpans using pendingSpans. pendingSpans
// contains spans with the same start key. This fragments and removed all
// spans up to end key fragmentUntil (or all spans if fragmentUntil == nil).
//
// Example 1:
// pendingSpans contains
//    c---g
//    c-----i
//    c--e
//
// And fragmentUntil = i. Since end keys are exclusive we can fragment and
// remove all spans in pendingSpans. These will be:
//    c-e-g
//    c-e-g-i
//    c-e
//
// For the c-e span, all the exprAndOperand slices for these spans are unioned
// since any row in that span needs to be routed to all these expressions and
// operands. For the e-g span only the exprAndOperand slices for the top two
// spans are unioned.
//
// Example 2:
//
// Same pendingSpans, and fragmentUntil = f. The fragments that are generated
// for fragmentedSpans and the remaining spans in pendingSpans are:
//
//    fragments        remaining
//    c-e-f            f-g
//    c-e-f            f-i
//    c-e
func (b *batchedInvertedExprEvaluator) fragmentPendingSpans(fragmentUntil encInvertedVal) {
	// Since the start keys are the same, this sorts in increasing
	// order of end keys.
	// TODO(sumeer): use a different sorter that doesn't do the wasteful
	// comparison of start keys.
	sort.Sort(spanBatchSliceSorter(b.pendingSpans))
	for len(b.pendingSpans) > 0 {
		var removeSize int
		var end encInvertedVal
		var nextStart encInvertedVal
		if b.pendingSpans[0].span.end != nil {
			if fragmentUntil != nil && bytes.Compare(fragmentUntil, b.pendingSpans[0].span.end) < 0 {
				// Can't completely remove any spans from pendingSpans, but a prefix
				// of these spans will be removed
				removeSize = 0
				end = fragmentUntil
				nextStart = end
			} else {
				// We can remove all spans whose end key is the same as span[0].
				// The end of span[0] is also the end key of this fragment.
				removeSize = b.pendingLenWithSameEnd()
				end = b.pendingSpans[0].span.end
				nextStart = end
			}
		} else {
			// We can remove all spans whose ened key is the same as span[0].
			// The end of span[0] is also the end key of this fragment.
			removeSize = b.pendingLenWithSameEnd()
			end = nil
			nextStart = roachpb.BytesNext(b.pendingSpans[0].span.start)
		}
		// The next span to be added to fragmentedSpans.
		nextSpan := invertedValSpanBatch{
			span: &encInvertedValSpan{
				start: b.pendingSpans[0].span.start,
				end:   end,
			},
		}
		for i := 0; i < len(b.pendingSpans); i++ {
			if i >= removeSize {
				// This span is not completely removed so adjust its start.
				b.pendingSpans[i].span.start = nextStart
			}
			// All spans in pendingSpans contribute to exprAndOperand.
			nextSpan.exprAndOperand = append(nextSpan.exprAndOperand, b.pendingSpans[i].exprAndOperand...)
		}
		b.fragmentedSpans = append(b.fragmentedSpans, nextSpan)
		b.pendingSpans = b.pendingSpans[removeSize:]
		if removeSize == 0 {
			break
		}
	}
}

// getSpans fragments the spans for later routing of rows and returns spans
// representing a union of all the spans (for executing the scan).
func (b *batchedInvertedExprEvaluator) getSpans() []encInvertedValSpan {
	if cap(b.exprEvals) < len(b.exprs) {
		b.exprEvals = make([]*invertedExprEvaluator, len(b.exprs))
	} else {
		b.exprEvals = b.exprEvals[:len(b.exprs)]
	}
	// Initial spans fetched from each expression.
	var spans []invertedValSpanBatch
	for i, expr := range b.exprs {
		if expr == nil {
			continue
		}
		b.exprEvals[i] = &invertedExprEvaluator{expr: expr}
		exprSpans := b.exprEvals[i].getSpansAndOperandIndexes()
		for _, s := range exprSpans {
			spans = append(spans,
				invertedValSpanBatch{
					span:           s.span,
					exprAndOperand: []exprAndOperand{{exprNum: i, operandIndex: s.operandIndex}},
				},
			)
		}
	}
	if len(spans) == 0 {
		return nil
	}

	sort.Sort(spanBatchSliceSorter(spans))

	// The union of the spans, which is returned from this function.
	var coveringSpans []encInvertedValSpan
	var currentCoveringSpan encInvertedValSpan
	// This loop does both the union of the spans and fragments the
	// spans.
	for _, span := range spans {
		if len(b.pendingSpans) == 0 {
			currentCoveringSpan = *span.span
		} else {
			if bytes.Compare(b.pendingSpans[0].span.start, span.span.start) < 0 {
				b.fragmentPendingSpans(span.span.start)
				if len(b.pendingSpans) == 0 {
					coveringSpans = append(coveringSpans, currentCoveringSpan)
					currentCoveringSpan = *span.span
				} else if bytes.Compare(currentCoveringSpan.end, span.span.end) < 0 {
					currentCoveringSpan.end = span.span.end
				}
			} else if bytes.Compare(currentCoveringSpan.end, span.span.end) < 0 {
				currentCoveringSpan.end = span.span.end
			}
		}
		// Add this span to the pending list.
		b.pendingSpans = append(b.pendingSpans, span)
	}
	b.fragmentPendingSpans(nil)
	coveringSpans = append(coveringSpans, currentCoveringSpan)
	return coveringSpans
}

// TODO(sumeer): if this will be called in non-decreasing order of enc, can
// use that to optimize the binary search.
func (b *batchedInvertedExprEvaluator) addIndexRow(enc encInvertedVal, rowIndex RowIndex) {
	i := sort.Search(len(b.fragmentedSpans), func(i int) bool {
		return bytes.Compare(b.fragmentedSpans[i].span.start, enc) > 0
	})
	i--
	for _, elem := range b.fragmentedSpans[i].exprAndOperand {
		b.exprEvals[elem.exprNum].addIndexRow(elem.operandIndex, rowIndex)
	}
}

func (b *batchedInvertedExprEvaluator) evaluate() [][]RowIndex {
	result := make([][]RowIndex, len(b.exprs))
	for i := range b.exprEvals {
		if b.exprEvals[i] == nil {
			continue
		}
		result[i] = b.exprEvals[i].evaluate()
	}
	return result
}

func (b *batchedInvertedExprEvaluator) reset() {
	b.exprs = b.exprs[:0]
	b.exprEvals = b.exprEvals[:0]
	b.fragmentedSpans = b.fragmentedSpans[:0]
	b.pendingSpans = b.pendingSpans[:0]
}
