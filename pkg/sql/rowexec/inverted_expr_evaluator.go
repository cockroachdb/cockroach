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
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/errors"
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
	op inverted.SetOperator
	// The index in invertedExprEvaluator.sets
	unionSetIndex int
	left          *setExpression
	right         *setExpression
}

type invertedSpan = inverted.SpanExpressionProto_Span
type invertedSpans = inverted.SpanExpressionProtoSpans
type spanExpression = inverted.SpanExpressionProto_Node

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
	case inverted.SetUnion:
		childrenSet = unionSetContainers(left, right)
	case inverted.SetIntersection:
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

type exprAndSetIndexSorter []exprAndSetIndex

// Implement sort.Interface. Sorts in increasing order of exprIndex.
func (esis exprAndSetIndexSorter) Len() int      { return len(esis) }
func (esis exprAndSetIndexSorter) Swap(i, j int) { esis[i], esis[j] = esis[j], esis[i] }
func (esis exprAndSetIndexSorter) Less(i, j int) bool {
	return esis[i].exprIndex < esis[j].exprIndex
}

// invertedSpanRoutingInfo contains the list of exprAndSetIndex pairs that
// need rows from the inverted index span. A []invertedSpanRoutingInfo with
// spans that are sorted and non-overlapping is used to route an added row to
// all the expressions and sets that need that row.
type invertedSpanRoutingInfo struct {
	span invertedSpan
	// Sorted in increasing order of exprIndex.
	exprAndSetIndexList []exprAndSetIndex
	// A de-duped and sorted list of exprIndex values from exprAndSetIndexList.
	// Used for pre-filtering, since the pre-filter is applied on a per
	// exprIndex basis.
	exprIndexList []int
}

// invertedSpanRoutingInfosByEndKey is a slice of invertedSpanRoutingInfo that
// implements the sort.Interface interface by sorting infos by their span's end
// key. The (unchecked) assumption is that spans in a slice all have the same
// start key.
type invertedSpanRoutingInfosByEndKey []invertedSpanRoutingInfo

// Implement sort.Interface.
func (s invertedSpanRoutingInfosByEndKey) Len() int      { return len(s) }
func (s invertedSpanRoutingInfosByEndKey) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s invertedSpanRoutingInfosByEndKey) Less(i, j int) bool {
	return bytes.Compare(s[i].span.End, s[j].span.End) < 0
}

// preFilterer is the single method from DatumsToInvertedExpr that is relevant here.
type preFilterer interface {
	PreFilter(enc inverted.EncVal, preFilters []interface{}, result []bool) (bool, error)
}

// batchedInvertedExprEvaluator is for evaluating one or more expressions. The
// batched evaluator can be reused by calling reset(). In the build phase,
// append expressions directly to exprs. A nil expression is permitted, and is
// just a placeholder that will result in a nil []KeyIndex in evaluate().
// init() must be called before calls to {prepare}addIndexRow() -- it builds the
// fragmentedSpans used for routing the added rows.
type batchedInvertedExprEvaluator struct {
	filterer preFilterer
	exprs    []*inverted.SpanExpressionProto

	// The pre-filtering state for each expression. When pre-filtering, this
	// is the same length as exprs.
	preFilterState []interface{}
	// The parameters and result of pre-filtering for an inverted row are
	// kept in this temporary state.
	tempPreFilters      []interface{}
	tempPreFilterResult []bool

	// The evaluators for all the exprs.
	exprEvals []*invertedExprEvaluator
	// The keys that constrain the non-inverted prefix columns, if the index is
	// a multi-column inverted index. For multi-column inverted indexes, these
	// keys are in one-to-one correspondence with exprEvals.
	nonInvertedPrefixes []roachpb.Key
	// Spans here are in sorted order and non-overlapping.
	fragmentedSpans []invertedSpanRoutingInfo
	// The routing index computed by prepareAddIndexRow.
	routingIndex int

	// Temporary state used during initialization.
	routingSpans       []invertedSpanRoutingInfo
	coveringSpans      []invertedSpan
	pendingSpansToSort invertedSpanRoutingInfosByEndKey
}

// Helper used in building fragmentedSpans using pendingSpans. pendingSpans
// contains spans with the same start key. This fragments and removes all
// spans up to end key fragmentUntil (or all spans if fragmentUntil == nil).
// It then returns the remaining pendingSpans.
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
	pendingSpans []invertedSpanRoutingInfo, fragmentUntil inverted.EncVal,
) []invertedSpanRoutingInfo {
	// The start keys are the same, so this only sorts in increasing order of
	// end keys. Assign slice to a field on the receiver before sorting to avoid
	// a heap allocation when the slice header passes through an interface.
	b.pendingSpansToSort = invertedSpanRoutingInfosByEndKey(pendingSpans)
	sort.Sort(&b.pendingSpansToSort)
	for len(pendingSpans) > 0 {
		if fragmentUntil != nil && bytes.Compare(fragmentUntil, pendingSpans[0].span.Start) <= 0 {
			break
		}
		// The prefix of pendingSpans that will be completely consumed when
		// the next fragment is constructed.
		var removeSize int
		// The end of the next fragment.
		var end inverted.EncVal
		// The start of the fragment after the next fragment.
		var nextStart inverted.EncVal
		if fragmentUntil != nil && bytes.Compare(fragmentUntil, pendingSpans[0].span.End) < 0 {
			// Can't completely remove any spans from pendingSpans, but a prefix
			// of these spans will be removed
			removeSize = 0
			end = fragmentUntil
			nextStart = end
		} else {
			// We can remove all spans whose end key is the same as span[0].
			// The end of span[0] is also the end key of this fragment.
			removeSize = b.pendingLenWithSameEnd(pendingSpans)
			end = pendingSpans[0].span.End
			nextStart = end
		}
		// The next span to be added to fragmentedSpans.
		nextSpan := invertedSpanRoutingInfo{
			span: invertedSpan{
				Start: pendingSpans[0].span.Start,
				End:   end,
			},
		}
		for i := 0; i < len(pendingSpans); i++ {
			if i >= removeSize {
				// This span is not completely removed so adjust its start.
				pendingSpans[i].span.Start = nextStart
			}
			// All spans in pendingSpans contribute to exprAndSetIndexList.
			nextSpan.exprAndSetIndexList =
				append(nextSpan.exprAndSetIndexList, pendingSpans[i].exprAndSetIndexList...)
		}
		// Sort the exprAndSetIndexList, since we need to use it to initialize the
		// exprIndexList before we push nextSpan onto b.fragmentedSpans.
		sort.Sort(exprAndSetIndexSorter(nextSpan.exprAndSetIndexList))
		nextSpan.exprIndexList = make([]int, 0, len(nextSpan.exprAndSetIndexList))
		for i := range nextSpan.exprAndSetIndexList {
			length := len(nextSpan.exprIndexList)
			exprIndex := nextSpan.exprAndSetIndexList[i].exprIndex
			if length == 0 || nextSpan.exprIndexList[length-1] != exprIndex {
				nextSpan.exprIndexList = append(nextSpan.exprIndexList, exprIndex)
			}
		}
		b.fragmentedSpans = append(b.fragmentedSpans, nextSpan)
		pendingSpans = pendingSpans[removeSize:]
		if removeSize == 0 {
			// fragmentUntil was earlier than the smallest End key in the pending
			// spans, so cannot fragment any more.
			break
		}
	}
	return pendingSpans
}

func (b *batchedInvertedExprEvaluator) pendingLenWithSameEnd(
	pendingSpans []invertedSpanRoutingInfo,
) int {
	length := 1
	for i := 1; i < len(pendingSpans); i++ {
		if !bytes.Equal(pendingSpans[0].span.End, pendingSpans[i].span.End) {
			break
		}
		length++
	}
	return length
}

// init fragments the spans for later routing of rows and returns spans
// representing a union of all the spans (for executing the scan). The
// returned slice is only valid until the next call to reset.
func (b *batchedInvertedExprEvaluator) init() (invertedSpans, error) {
	if len(b.nonInvertedPrefixes) > 0 && len(b.nonInvertedPrefixes) != len(b.exprs) {
		return nil, errors.AssertionFailedf("length of non-empty nonInvertedPrefixes must equal length of exprs")
	}
	if cap(b.exprEvals) < len(b.exprs) {
		b.exprEvals = make([]*invertedExprEvaluator, len(b.exprs))
	} else {
		b.exprEvals = b.exprEvals[:len(b.exprs)]
	}
	// Initial spans fetched from all expressions.
	for i, expr := range b.exprs {
		if expr == nil {
			b.exprEvals[i] = nil
			continue
		}
		var prefixKey roachpb.Key
		if len(b.nonInvertedPrefixes) > 0 {
			prefixKey = b.nonInvertedPrefixes[i]
		}
		b.exprEvals[i] = newInvertedExprEvaluator(&expr.Node)
		exprSpans := b.exprEvals[i].getSpansAndSetIndex()
		for _, spans := range exprSpans {
			for _, span := range spans.spans {
				if len(prefixKey) > 0 {
					// TODO(mgartner/sumeer): It may be possible to reduce
					// allocations and memory usage by adding a level of
					// indirection for prefix keys (like a map of prefixes to
					// routingSpans), rather than prepending prefix keys to each
					// span.
					span = prefixInvertedSpan(prefixKey, span)
				}
				b.routingSpans = append(b.routingSpans,
					invertedSpanRoutingInfo{
						span:                span,
						exprAndSetIndexList: []exprAndSetIndex{{exprIndex: i, setIndex: spans.setIndex}},
					},
				)
			}
		}
	}
	if len(b.routingSpans) == 0 {
		return nil, nil
	}

	// Sort the routingSpans in increasing order of start key, and for equal
	// start keys in increasing order of end key.
	sort.Slice(b.routingSpans, func(i, j int) bool {
		cmp := bytes.Compare(b.routingSpans[i].span.Start, b.routingSpans[j].span.Start)
		if cmp == 0 {
			cmp = bytes.Compare(b.routingSpans[i].span.End, b.routingSpans[j].span.End)
		}
		return cmp < 0
	})

	// The union of the spans, which is returned from this function.
	currentCoveringSpan := b.routingSpans[0].span
	// Create a slice of pendingSpans to be fragmented by windowing over the
	// full collection of routingSpans. All spans in a given window have the
	// same start key. They are not sorted by end key.
	pendingSpans := b.routingSpans[:1]
	// This loop does both the union of the routingSpans and fragments the
	// routingSpans. The pendingSpans slice contains a subsequence of the
	// routingSpans slice, that when passed to fragmentPendingSpans will be
	// mutated by it.
	for i := 1; i < len(b.routingSpans); i++ {
		span := b.routingSpans[i]
		if bytes.Compare(pendingSpans[0].span.Start, span.span.Start) < 0 {
			pendingSpans = b.fragmentPendingSpans(pendingSpans, span.span.Start)
			if bytes.Compare(currentCoveringSpan.End, span.span.Start) < 0 {
				b.coveringSpans = append(b.coveringSpans, currentCoveringSpan)
				currentCoveringSpan = span.span
			} else if bytes.Compare(currentCoveringSpan.End, span.span.End) < 0 {
				currentCoveringSpan.End = span.span.End
			}
		} else if bytes.Compare(currentCoveringSpan.End, span.span.End) < 0 {
			currentCoveringSpan.End = span.span.End
		}
		// Add this span to the pending list by expanding the window over
		// b.routingSpans.
		pendingSpans = pendingSpans[:len(pendingSpans)+1]
	}
	b.fragmentPendingSpans(pendingSpans, nil)
	b.coveringSpans = append(b.coveringSpans, currentCoveringSpan)
	return b.coveringSpans, nil
}

// prepareAddIndexRow must be called prior to addIndexRow to do any
// pre-filtering. The return value indicates whether addIndexRow should be
// called. encFull should include the entire index key, including non-inverted
// prefix columns. It should be nil if the index is not a multi-column inverted
// index.
// TODO(sumeer): if this will be called in non-decreasing order of enc,
// use that to optimize the binary search.
func (b *batchedInvertedExprEvaluator) prepareAddIndexRow(
	enc inverted.EncVal, encFull inverted.EncVal,
) (bool, error) {
	routingEnc := enc
	if encFull != nil {
		routingEnc = encFull
	}
	// Find the first span that comes after the encoded routing value.
	i := sort.Search(len(b.fragmentedSpans), func(i int) bool {
		return bytes.Compare(b.fragmentedSpans[i].span.Start, routingEnc) > 0
	})
	// Decrement by 1 so that now i tracks the index of the span that might
	// contain the encoded routing value.
	i--
	if i < 0 {
		// Negative index indicates that some assumptions are violated, return
		// an assertion error in this case.
		return false, errors.AssertionFailedf("unexpectedly negative routing index %d", i)
	}
	if bytes.Compare(b.fragmentedSpans[i].span.End, routingEnc) <= 0 {
		return false, errors.AssertionFailedf(
			"unexpectedly the end of the routing span %d is not greater "+
				"than encoded routing value", i,
		)
	}
	b.routingIndex = i
	return b.prefilter(enc)
}

// prefilter applies b.filterer, if it exists, returning true if addIndexRow
// should be called for the row corresponding to the encoded value.
// prepareAddIndexRow or prepareAddMultiColumnIndexRow must be called first.
func (b *batchedInvertedExprEvaluator) prefilter(enc inverted.EncVal) (bool, error) {
	if b.filterer != nil {
		exprIndexList := b.fragmentedSpans[b.routingIndex].exprIndexList
		if len(exprIndexList) > cap(b.tempPreFilters) {
			b.tempPreFilters = make([]interface{}, len(exprIndexList))
			b.tempPreFilterResult = make([]bool, len(exprIndexList))
		} else {
			b.tempPreFilters = b.tempPreFilters[:len(exprIndexList)]
			b.tempPreFilterResult = b.tempPreFilterResult[:len(exprIndexList)]
		}
		for j := range exprIndexList {
			b.tempPreFilters[j] = b.preFilterState[exprIndexList[j]]
		}
		return b.filterer.PreFilter(enc, b.tempPreFilters, b.tempPreFilterResult)
	}
	return true, nil
}

// addIndexRow must be called iff prepareAddIndexRow returned true.
func (b *batchedInvertedExprEvaluator) addIndexRow(keyIndex KeyIndex) error {
	i := b.routingIndex
	if b.filterer != nil {
		exprIndexes := b.fragmentedSpans[i].exprIndexList
		exprSetIndexes := b.fragmentedSpans[i].exprAndSetIndexList
		if len(exprIndexes) != len(b.tempPreFilterResult) {
			return errors.Errorf("non-matching lengths of tempPreFilterResult and exprIndexes")
		}
		// Coordinated iteration over exprIndexes and exprSetIndexes.
		j := 0
		for k := range exprSetIndexes {
			elem := exprSetIndexes[k]
			if elem.exprIndex > exprIndexes[j] {
				j++
				if exprIndexes[j] != elem.exprIndex {
					return errors.Errorf("non-matching expr indexes")
				}
			}
			if b.tempPreFilterResult[j] {
				b.exprEvals[elem.exprIndex].addIndexRow(elem.setIndex, keyIndex)
			}
		}
	} else {
		for _, elem := range b.fragmentedSpans[i].exprAndSetIndexList {
			b.exprEvals[elem.exprIndex].addIndexRow(elem.setIndex, keyIndex)
		}
	}
	return nil
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
	b.preFilterState = b.preFilterState[:0]
	b.exprEvals = b.exprEvals[:0]
	b.fragmentedSpans = b.fragmentedSpans[:0]
	b.routingSpans = b.routingSpans[:0]
	b.coveringSpans = b.coveringSpans[:0]
	b.nonInvertedPrefixes = b.nonInvertedPrefixes[:0]
}

// prefixInvertedSpan returns a new invertedSpan with prefix prepended to the
// input span's Start and End keys. This is similar to the internals of
// rowenc.appendEncDatumsToKey.
func prefixInvertedSpan(prefix roachpb.Key, span invertedSpan) invertedSpan {
	newSpan := invertedSpan{
		Start: make(roachpb.Key, 0, len(prefix)+len(span.Start)),
		End:   make(roachpb.Key, 0, len(prefix)+len(span.End)),
	}
	newSpan.Start = append(newSpan.Start, prefix...)
	newSpan.Start = append(newSpan.Start, span.Start...)
	newSpan.End = append(newSpan.End, prefix...)
	newSpan.End = append(newSpan.End, span.End...)
	return newSpan
}
