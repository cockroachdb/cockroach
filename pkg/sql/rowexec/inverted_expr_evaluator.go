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

	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
)

// The abstractions in this file help with evaluating (batches of)
// invertedexpr.SpanExpression. The spans in a SpanExpression represent spans
// of an inverted index, which consists of an inverted column followed by the
// primary key of the table. The set expressions involve union and
// intersection over operands. The operands are sets of primary keys contained
// in the corresponding span. Callers should use batchedInvertedExprEvaluator.
// This evaluator does not do the actual scan -- it is fed the set elements as
// the inverted index is scanned, and routes a set element to all the sets to
// which it belongs (since spans can be overlapping). Once the scan is
// complete, the expressions are evaluated.

// KeyIndex is used as a set element. It is already de-duped.
type KeyIndex = int

// setContainer is a set of key indexes in increasing order.
type setContainer []KeyIndex

func (s setContainer) Len() int {
	return len(s)
}

func (s setContainer) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s setContainer) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func unionSetContainers(a, b setContainer) setContainer {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
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

func intersectSetContainers(a, b setContainer) setContainer {
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

// setExpression follows the structure of SpanExpression.
type setExpression struct {
	op invertedexpr.SetOperator
	// The index in invertedExprEvaluator.sets
	unionSetIndex int
	left          *setExpression
	right         *setExpression
}

type invertedSpan = invertedexpr.SpanExpressionProto_Span
type spanExpression = invertedexpr.SpanExpressionProto_Node

// The spans in a SpanExpression.FactoredUnionSpans and the corresponding index
// in invertedExprEvaluator.sets. Only populated when FactoredUnionsSpans is
// non-empty.
type spansAndSetIndex struct {
	spans    []invertedSpan
	setIndex int
}

// invertedExprEvaluator evaluates a single expression. It should not be directly
// used -- see batchedInvertedExprEvaluator.
type invertedExprEvaluator struct {
	setExpr *setExpression
	// These are initially populated by calls to addIndexRow() as
	// the inverted index is scanned.
	sets []setContainer

	spansIndex []spansAndSetIndex
}

func newInvertedExprEvaluator(expr *spanExpression) *invertedExprEvaluator {
	eval := &invertedExprEvaluator{}
	eval.setExpr = eval.initSetExpr(expr)
	return eval
}

func (ev *invertedExprEvaluator) initSetExpr(expr *spanExpression) *setExpression {
	// Assign it an index even if FactoredUnionSpans is empty, since we will
	// need it when evaluating.
	i := len(ev.sets)
	ev.sets = append(ev.sets, nil)
	sx := &setExpression{op: expr.Operator, unionSetIndex: i}
	if len(expr.FactoredUnionSpans) > 0 {
		ev.spansIndex = append(ev.spansIndex,
			spansAndSetIndex{spans: expr.FactoredUnionSpans, setIndex: i})
	}
	if expr.Left != nil {
		sx.left = ev.initSetExpr(expr.Left)
	}
	if expr.Right != nil {
		sx.right = ev.initSetExpr(expr.Right)
	}
	return sx
}

// getSpansAndSetIndex returns the spans and corresponding set indexes for
// this expression. The spans are not in sorted order and can be overlapping.
func (ev *invertedExprEvaluator) getSpansAndSetIndex() []spansAndSetIndex {
	return ev.spansIndex
}

// Adds a row to the given set. KeyIndexes are not added in increasing order,
// nor do they represent any ordering of the primary key of the table whose
// inverted index is being read. Also, the same KeyIndex could be added
// repeatedly to a set.
func (ev *invertedExprEvaluator) addIndexRow(setIndex int, keyIndex KeyIndex) {
	// If duplicates in a set become a memory problem in this build phase, we
	// could do periodic de-duplication as we go. For now, we simply append to
	// the slice and de-dup at the start of evaluate().
	ev.sets[setIndex] = append(ev.sets[setIndex], keyIndex)
}

// Evaluates the expression. The return value is in increasing order
// of KeyIndex.
func (ev *invertedExprEvaluator) evaluate() []KeyIndex {
	// Sort and de-dup the sets so that we can efficiently do set operations.
	for i, c := range ev.sets {
		if len(c) == 0 {
			continue
		}
		sort.Sort(c)
		// De-duplicate
		set := c[:0]
		for j := range c {
			if len(set) > 0 && c[j] == set[len(set)-1] {
				continue
			}
			set = append(set, c[j])
		}
		ev.sets[i] = set
	}
	return ev.evaluateSetExpr(ev.setExpr)
}

func (ev *invertedExprEvaluator) evaluateSetExpr(sx *setExpression) setContainer {
	var left, right setContainer
	if sx.left != nil {
		left = ev.evaluateSetExpr(sx.left)
	}
	if sx.right != nil {
		right = ev.evaluateSetExpr(sx.right)
	}
	var childrenSet setContainer
	switch sx.op {
	case invertedexpr.SetUnion:
		childrenSet = unionSetContainers(left, right)
	case invertedexpr.SetIntersection:
		childrenSet = intersectSetContainers(left, right)
	}
	return unionSetContainers(ev.sets[sx.unionSetIndex], childrenSet)
}

// Supporting struct for invertedSpanRoutingInfo.
type exprAndSetIndex struct {
	// An index into batchedInvertedExprEvaluator.exprEvals.
	exprIndex int
	// An index into batchedInvertedExprEvaluator.exprEvals[exprIndex].sets.
	setIndex int
}

// invertedSpanRoutingInfo contains the list of exprAndSetIndex pairs that
// need rows from the inverted index span. A []invertedSpanRoutingInfo with
// spans that are sorted and non-overlapping is used to route an added row to
// all the expressions and sets that need that row.
type invertedSpanRoutingInfo struct {
	span                invertedSpan
	exprAndSetIndexList []exprAndSetIndex
}

// batchedInvertedExprEvaluator is for evaluating one or more expressions. The
// batched evaluator can be reused by calling reset(). In the build phase,
// append expressions directly to exprs. A nil expression is permitted, and is
// just a placeholder that will result in a nil []KeyIndex in evaluate().
// init() must be called before calls to addIndexRow() -- it builds the
// fragmentedSpans used for routing the added rows.
type batchedInvertedExprEvaluator struct {
	exprs []*invertedexpr.SpanExpressionProto
	// The evaluators for all the exprs.
	exprEvals []*invertedExprEvaluator
	// Spans here are in sorted order and non-overlapping.
	fragmentedSpans []invertedSpanRoutingInfo

	// Temporary state used for constructing fragmentedSpans. All spans here
	// have the same start key. They are not sorted by end key.
	pendingSpans []invertedSpanRoutingInfo
}

// Helper used in building fragmentedSpans using pendingSpans. pendingSpans
// contains spans with the same start key. This fragments and removes all
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
// For the c-e span, all the exprAndSetIndexList slices for these spans are
// appended since any row in that span needs to be routed to all these
// expressions and sets. For the e-g span only the exprAndSetIndexList slices
// for the top two spans are unioned.
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
func (b *batchedInvertedExprEvaluator) fragmentPendingSpans(
	fragmentUntil invertedexpr.EncInvertedVal,
) {
	// The start keys are the same, so this only sorts in increasing
	// order of end keys.
	sort.Slice(b.pendingSpans, func(i, j int) bool {
		return bytes.Compare(b.pendingSpans[i].span.End, b.pendingSpans[j].span.End) < 0
	})
	for len(b.pendingSpans) > 0 {
		if fragmentUntil != nil && bytes.Compare(fragmentUntil, b.pendingSpans[0].span.Start) <= 0 {
			break
		}
		// The prefix of pendingSpans that will be completely consumed when
		// the next fragment is constructed.
		var removeSize int
		// The end of the next fragment.
		var end invertedexpr.EncInvertedVal
		// The start of the fragment after the next fragment.
		var nextStart invertedexpr.EncInvertedVal
		if fragmentUntil != nil && bytes.Compare(fragmentUntil, b.pendingSpans[0].span.End) < 0 {
			// Can't completely remove any spans from pendingSpans, but a prefix
			// of these spans will be removed
			removeSize = 0
			end = fragmentUntil
			nextStart = end
		} else {
			// We can remove all spans whose end key is the same as span[0].
			// The end of span[0] is also the end key of this fragment.
			removeSize = b.pendingLenWithSameEnd()
			end = b.pendingSpans[0].span.End
			nextStart = end
		}
		// The next span to be added to fragmentedSpans.
		nextSpan := invertedSpanRoutingInfo{
			span: invertedSpan{
				Start: b.pendingSpans[0].span.Start,
				End:   end,
			},
		}
		for i := 0; i < len(b.pendingSpans); i++ {
			if i >= removeSize {
				// This span is not completely removed so adjust its start.
				b.pendingSpans[i].span.Start = nextStart
			}
			// All spans in pendingSpans contribute to exprAndSetIndexList.
			nextSpan.exprAndSetIndexList =
				append(nextSpan.exprAndSetIndexList, b.pendingSpans[i].exprAndSetIndexList...)
		}
		b.fragmentedSpans = append(b.fragmentedSpans, nextSpan)
		b.pendingSpans = b.pendingSpans[removeSize:]
		if removeSize == 0 {
			// fragmentUntil was earlier than the smallest End key in the pending
			// spans, so cannot fragment any more.
			break
		}
	}
}

func (b *batchedInvertedExprEvaluator) pendingLenWithSameEnd() int {
	length := 1
	for i := 1; i < len(b.pendingSpans); i++ {
		if !bytes.Equal(b.pendingSpans[0].span.End, b.pendingSpans[i].span.End) {
			break
		}
		length++
	}
	return length
}

// init fragments the spans for later routing of rows and returns spans
// representing a union of all the spans (for executing the scan).
func (b *batchedInvertedExprEvaluator) init() []invertedSpan {
	if cap(b.exprEvals) < len(b.exprs) {
		b.exprEvals = make([]*invertedExprEvaluator, len(b.exprs))
	} else {
		b.exprEvals = b.exprEvals[:len(b.exprs)]
	}
	// Initial spans fetched from all expressions.
	var routingSpans []invertedSpanRoutingInfo
	for i, expr := range b.exprs {
		if expr == nil {
			b.exprEvals[i] = nil
			continue
		}
		b.exprEvals[i] = newInvertedExprEvaluator(&expr.Node)
		exprSpans := b.exprEvals[i].getSpansAndSetIndex()
		for _, spans := range exprSpans {
			for _, span := range spans.spans {
				routingSpans = append(routingSpans,
					invertedSpanRoutingInfo{
						span:                span,
						exprAndSetIndexList: []exprAndSetIndex{{exprIndex: i, setIndex: spans.setIndex}},
					},
				)
			}
		}
	}
	if len(routingSpans) == 0 {
		return nil
	}

	// Sort the routingSpans in increasing order of start key, and for equal
	// start keys in increasing order of end key.
	sort.Slice(routingSpans, func(i, j int) bool {
		cmp := bytes.Compare(routingSpans[i].span.Start, routingSpans[j].span.Start)
		if cmp == 0 {
			cmp = bytes.Compare(routingSpans[i].span.End, routingSpans[j].span.End)
		}
		return cmp < 0
	})

	// The union of the spans, which is returned from this function.
	var coveringSpans []invertedSpan
	currentCoveringSpan := routingSpans[0].span
	b.pendingSpans = append(b.pendingSpans, routingSpans[0])
	// This loop does both the union of the routingSpans and fragments the
	// routingSpans.
	for i := 1; i < len(routingSpans); i++ {
		span := routingSpans[i]
		if bytes.Compare(b.pendingSpans[0].span.Start, span.span.Start) < 0 {
			b.fragmentPendingSpans(span.span.Start)
			if bytes.Compare(currentCoveringSpan.End, span.span.Start) < 0 {
				coveringSpans = append(coveringSpans, currentCoveringSpan)
				currentCoveringSpan = span.span
			} else if bytes.Compare(currentCoveringSpan.End, span.span.End) < 0 {
				currentCoveringSpan.End = span.span.End
			}
		} else if bytes.Compare(currentCoveringSpan.End, span.span.End) < 0 {
			currentCoveringSpan.End = span.span.End
		}
		// Add this span to the pending list.
		b.pendingSpans = append(b.pendingSpans, span)
	}
	b.fragmentPendingSpans(nil)
	coveringSpans = append(coveringSpans, currentCoveringSpan)
	return coveringSpans
}

// TODO(sumeer): if this will be called in non-decreasing order of enc,
// use that to optimize the binary search.
func (b *batchedInvertedExprEvaluator) addIndexRow(
	enc invertedexpr.EncInvertedVal, keyIndex KeyIndex,
) {
	i := sort.Search(len(b.fragmentedSpans), func(i int) bool {
		return bytes.Compare(b.fragmentedSpans[i].span.Start, enc) > 0
	})
	i--
	for _, elem := range b.fragmentedSpans[i].exprAndSetIndexList {
		b.exprEvals[elem.exprIndex].addIndexRow(elem.setIndex, keyIndex)
	}
}

func (b *batchedInvertedExprEvaluator) evaluate() [][]KeyIndex {
	result := make([][]KeyIndex, len(b.exprs))
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
