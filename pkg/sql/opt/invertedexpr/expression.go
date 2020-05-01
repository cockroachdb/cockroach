// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package invertedexpr

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// EncInvertedVal is the encoded form of a value in the inverted column. If
// the inverted column stores an encoded datum, the encoding is
// DatumEncoding_ASCENDING_KEY, and is performed using
// EncodeTableKey(nil /* prefix */, val tree.Datum, DatumEncoding_ASCENDING_KEY).
// It is used to represent spans of the inverted column.
//
// It would be ideal if the inverted column only contained Datums,
// since we could then work with a Datum here. However, JSON makes this tricky:
// - JSON inverted columns use a custom encoding that uses a special byte
//   jsonInvertedIndex, followed by the bytes produced by the various implementations
//   of the encodeInvertedIndexKey() method in the JSON interface.
//   This could be worked around by using a JSON datum that represents a single
//   path as the start key of the span, and a nil end indicating that the span
//   represents [start, start], and special casing the encoding logic to recognize
//   that it is dealing with JSON (we have similar special path code for JSON
//   elsewhere). But this is insufficient (next bullet).
// - Expressions like x ? 'b' don't have operands that are JSON, but can be
//   represented using a span on the inverted column.
// So we make it the job of the caller of this library to encode the inverted
// column. Note that the second bullet has some similarities with the behavior
// in makeStringPrefixSpan(), except there we can represent the start and end
// keys using the string type.
type EncInvertedVal []byte

// Unlike the logic in the constraints package, the spans of the inverted
// index are not immediately "evaluatable" since they represent sets of
// primary keys that we won't know about until we do the scan. Using a
// simple example: [a, d) \intersection [c, f) is not [c, d) since the
// same primary key K could be found under a and f and be part of the result.
// More precisely, the above expression can be simplified to:
// [c, d) \union ([a, c) \intersection [d, f))
// For regular indexes, since each primary key is indexed in one row of the
// index, we can be sure that the same primary key will not appear in both
// of the non-overlapping spans [a, c) and [d, f), so we can immediately
// throw that part away knowing that it is the empty set. This discarding
// is not possible with inverted indexes, though the factoring can be useful
// for speed of execution (though it does not limit what we need to scan)
// and for selectivity estimation when making optimizer choices.
//
// One could try to construct a general library that handles both the
// cases handled in the constraints package and here, but the complexity seems
// high. Instead, this package is more general than constraints in a few ways
// but simplifies most other things (so overall much simpler):
// - All the inverted spans are [start, end), i.e., there is no support for
//   inclusive end.
// - It primary handles spans only on the inverted column, with a way to
//   plug-in spans generated for the PK columns (more on this below).
//
// The other significant point of departure from the constraints package is
// that this package represents a canonical form for all inverted expressions
// -- it is not just span "constraints" for a scan. The evaluation machinery
// will evaluate this expression. The support to build that canonical form
// expression is independent of how the original expression is represented:
// instead of taking an opt.Expr parameter and traversing it itself, this
// library assumes the caller is doing a traversal. This is partly because I
// am not sure whether the representation of the original expression for the
// single table scan case and the invertedJoiner case are the same: the latter
// starts with an expression with two unspecified rows, and after the left
// side row is bound (partial application), this library needs to be used to
// construct the InvertedExpression.
//
// For costing purposes, this package also allows building an expression for
// the two variable rows case, by implementing the InvertedExpression interface
// (see comment where InvertedExpression is declared for details).
//
// Multi-column constraints and relationship with the constraints package:
//
// Building multi-column constraints is complicated even for the regular
// index case (see idxconstraint and constraints packages). Because the
// constraints code is not generating a full expression and it can immediately
// evaluate intersections, it takes an approach of traversing the expression
// at monotonically increasing column offsets (e.g. makeSpansForAnd() and the
// offset+delta logic). This allows it to build up Key constraints in increasing
// order of the index column (say labelled @1, @2, ...), instead of needing to
// handle an arbitrary order, and then combine them using Constraint.Combine().
// This repeated traversal at different offsets is a simplification and can
// result in spans that are wider than optimal.
//
// Example 1:
// index-constraints vars=(int, int, int) index=(@1 not null, @2 not null, @3 not null)
// ((@1 = 1 AND @3 = 5) OR (@1 = 3 AND @3 = 10)) AND (@2 = 76)
// ----
// [/1/76/5 - /1/76/5]
// [/1/76/10 - /1/76/10]
// [/3/76/5 - /3/76/5]
// [/3/76/10 - /3/76/10]
// Remaining filter: ((@1 = 1) AND (@3 = 5)) OR ((@1 = 3) AND (@3 = 10))
//
// Note that in example 1 we produce the spans with the single key /1/76/10
// and /3/76/5 which are not possible -- this is because the application of
// the @3 constraint happened at the higher level after the @2 constraint had
// been applied, and at that higher level the @3 constraint was now the set
// {5, 10}, so it needed to be applied to both the /1/76 anf /3/76 span.
//
// In contrast example 2 is able to apply the @2 constraint inside each of the
// sub-expressions and results in a tight span.
//
// Example 2:
// index-constraints vars=(int, int, int) index=(@1 not null, @2 not null, @3 not null)
// ((@1 = 1 AND @2 = 5) OR (@1 = 3 AND @2 = 10)) AND (@3 = 76)
// ----
// [/1/5/76 - /1/5/76]
// [/3/10/76 - /3/10/76]
//
// We note that:
// - Working with spans of only the inverted column is much easier for factoring.
// - It is not yet clear how important multi-column constraints are for inverted
//   index performance.
// - We cannot adopt the approach of traversing at monotonically increasing
//   column offsets since we are trying to build an expression. We want to
//   traverse once, to build up the expression tree. One possibility would be
//   to incrementally build the expression tree with the caller traversing once
//   but additionally keep track of the span constraints for each PK column at
//   each node in the already build expression tree. To illustrate, consider
//   an example 1' akin to example 1 where @1 is an inverted column:
//   ((f(@1, 1) AND @3 = 5) OR (f(@1, 3) AND @3 = 10)) AND (@2 = 76)
//   and the functions f(@1, 1) and f(@1, 3) each give a single value for the
//   inverted column (this could be something like f @> '{"a":1}'::json).
//   Say we already have the expression tree built for:
//   ((f(@1, 1) AND @3 = 5) OR (f(@1, 3) AND @3 = 10))
//   When the constraint for (@3 = 76) is anded we traverse this built tree
//   and add this constraint to each node. Note that we are delaying building
//   something akin to a constraint.Key since we are encountering the constraints
//   in arbitrary column order. Then after the full expression tree is built,
//   one traverses and builds the inverted spans and primary key spans (latter
//   could reuse constraint.Span for each node).
// - The previous bullet is doable but complicated, and especially increases the
//   complexity of factoring spans when unioning and intersecting while building
//   up sub-expressions. One needs to either factor taking into account the
//   current per-column PK constraints or delay it until the end (I gave up
//   half-way through writing the code).
//
// In the following we adopt a much simpler approach. The caller generates the
// the inverted index expression and the PK spans separately.
//
// - Generating the inverted index expression: The caller does a single
//   traversal and calls the methods in this package. For every
//   leaf-sub-expression on the non-inverted columns it uses a marker
//   NonInvertedColExpression. Anding a NonInvertedColExpression results in a
//   non-tight inverted expression and Oring a NonInvertedColExpression
//   results in discarding the inverted expression built so far. This package
//   does factoring for ands and ors involving inverted expressions
//   incrementally, and this factoring is straigthtforward since it involves a
//   single column.
//  - Generating the PK spans (optional): The caller can use something like
//   idxconstraint, pretending that the PK columns of the inverted index
//   are the index columns. Every leaf inverted sub-expression is replaced
//   with true.
// - The spans in the inverted index expression can be composed with the
//   spans of the PK columns to narrow wherever possible.
//   Continuing with example 1', the inverted index expression will be
//   v11 \union v13, corresponding to f(@1, 1) and f(@1, 3), where each
//   of v11 and v13 are singleton spans. And this expression is not tight
//   (because of the anding with OtherExpression).
//   The PK spans will be: /76/5, /76/10, which are also singleton spans.
//   This is a favorable example in that we can compose all these singleton
//   spans to get single inverted index rows:
//   /v11/76/5, /v11/76/10, /v13/76/5, /v13/76/10
//   (this is also a contrived example since with such narrow constraints
//   on the PK, we would possibly not use the inverted index).
//
//   If one constructs example 2' (derived from example 2 in the same way
//   we derived example 1'), we would have
//   Inverted index expression:
//   v11 \union v13
//   PK spans:
//   /5/76, /10/76
//   And inverted index rows:
//   /v11/5/76, /v11/10/76, /v13/5/76, /v13/10/76
//   This is worse than example 2 (and resembles example 1 and 1') since
//   we are taking the cross-product.
//   TODO(sumeer): write this straightforward composition code.

// TODO(sumeer): think through how this will change when we have partitioned
// inverted indexes, where some columns of the primary key will appear before the
// inverted column.

// InvertedSpan is a span of the inverted index.
type InvertedSpan struct {
	// [start, end) iff end is not-nil, else represents [start, BytesNext(start)).
	start, end EncInvertedVal
}

type SetOperator int

const (
	None SetOperator = iota
	SetUnion
	SetIntersection
)

// InvertedExpression is the interface representing an expression or sub-expression
// to be evaluated on the inverted index. There are two implementations:
// - SpanExpression: this is the normal expression representing a span of the
//   inverted index.
// - NonInvertedColExpression: this is a marker expression representing the universal
//   span, due to it being over the non inverted column. This only appears in
//   expression trees with a single node, since Anding with such an expression simply
//   changes the tightness to false and Oring with this expression replaces the
//   other expression with a NonInvertedColExpression.
//
// Additionally, for the inverted join case, where the optimizer is working with
// an expression with two variable rows, it can implement InvertedExpression and
// embed some cost information, and pass it to the Or and And methods. An expression
// tree could be a mix of SpanExpressions and the caller implemented InvertedExpression,
// and the optimizer could traverse this to build a cost model. If the tree turns
// out to be a single NonInvertedColExpression, using the inverted index is not
// feasible (this is also true for the single table selection case).
//
// For the evaluation machinery, once the InvertedExpression is built, one can
// be sure that it will be a *SpanExpression, since that is the only case where the
// optimizer would have chosen to use the inverted index.
type InvertedExpression interface {
	Tight() bool
	SetNotTight()
}

type SpanExpression struct {
	tight bool

	// These are the spans to read from the inverted index. Non-overlapping
	// and sorted. If left or right contains a non-SpanExpression, it is
	// not included in the spanningUnion.
	spanningUnion []InvertedSpan

	// Factored spans involved in a union. Non-overlapping and sorted.
	factoredUnionSpans []InvertedSpan

	operator SetOperator
	left     InvertedExpression
	right    InvertedExpression
}

var _ InvertedExpression = (*SpanExpression)(nil)

func (s *SpanExpression) Tight() bool {
	return s.tight
}

func (s *SpanExpression) SetNotTight() {
	s.tight = false
}

type NonInvertedColExpression struct{}

var _ InvertedExpression = NonInvertedColExpression{}

func (n NonInvertedColExpression) Tight() bool {
	return false
}

func (n NonInvertedColExpression) SetNotTight() {}

// ExprForInvertedSpan constructs a leaf-level SpanExpression
// for an inverted expression. Note that these leaf-level
// expressions may also have tight = false. Geospatial functions
// are all non-tight. For JSON, expressions like
// x <@ '{"a":1, "b":2}'::json
// will have tight = false since they will get encoded as
// two calls for ExprForInvertedSpan() with SpanA and SpanB
// respectively (where SpanA, Span B correspond to "a":1 and
// "b":2 respectively), which are then unioned. A tight
// expression would require the following set evaluation:
// Set(SpanA) \union Set(SpanB) - Set(ComplementSpan(SpanA \spanunion SpanB))
// But since ComplementSpan(SpanA \spanunion SpanB) is likely to
// be very wide when SpanA and SpanB are narrow, or vice versa,
// this tight expression would be very costly to evaluate.
func ExprForInvertedSpan(start, end EncInvertedVal, tight bool) *SpanExpression {
	return &SpanExpression{
		tight:              tight,
		spanningUnion:      []InvertedSpan{{start: start, end: end}},
		factoredUnionSpans: []InvertedSpan{{start: start, end: end}},
	}
}

func And(left, right InvertedExpression) InvertedExpression {
	switch l := left.(type) {
	case *SpanExpression:
		switch r := right.(type) {
		case *SpanExpression:
			return intersectSpanExpressions(l, r)
		case NonInvertedColExpression:
			left.SetNotTight()
			return left
		default:
			return opSpanExpressionAndDefault(l, right, SetIntersection)
		}
	case NonInvertedColExpression:
		right.SetNotTight()
		return right
	default:
		switch r := right.(type) {
		case *SpanExpression:
			return opSpanExpressionAndDefault(r, left, SetIntersection)
		case NonInvertedColExpression:
			left.SetNotTight()
			return left
		default:
			return &SpanExpression{
				tight:    left.Tight() && right.Tight(),
				operator: SetIntersection,
				left:     left,
				right:    right,
			}
		}
	}
}

func Or(left, right InvertedExpression) InvertedExpression {
	switch l := left.(type) {
	case *SpanExpression:
		switch r := right.(type) {
		case *SpanExpression:
			return unionSpanExpressions(l, r)
		case NonInvertedColExpression:
			return r
		default:
			return opSpanExpressionAndDefault(l, right, SetUnion)
		}
	case NonInvertedColExpression:
		return left
	default:
		switch r := right.(type) {
		case *SpanExpression:
			return opSpanExpressionAndDefault(r, left, SetUnion)
		case NonInvertedColExpression:
			return right
		default:
			return &SpanExpression{
				tight:    left.Tight() && right.Tight(),
				operator: SetUnion,
				left:     left,
				right:    right,
			}
		}
	}
}

func intersectSpanExpressions(left, right *SpanExpression) *SpanExpression {
	expr := &SpanExpression{
		tight:              left.tight && right.tight,
		spanningUnion:      unionSpans(left.spanningUnion, right.spanningUnion),
		factoredUnionSpans: intersectSpans(left.factoredUnionSpans, left.factoredUnionSpans),
		operator:           SetIntersection,
		left:               left,
		right:              right,
	}
	if expr.factoredUnionSpans != nil {
		left.factoredUnionSpans = subtractSpans(left.factoredUnionSpans, expr.factoredUnionSpans)
		right.factoredUnionSpans = subtractSpans(right.factoredUnionSpans, expr.factoredUnionSpans)
	}
	return expr
}

func unionSpanExpressions(left, right *SpanExpression) *SpanExpression {
	return &SpanExpression{
		tight:              left.tight && right.tight,
		spanningUnion:      unionSpans(left.spanningUnion, right.spanningUnion),
		factoredUnionSpans: unionSpans(left.factoredUnionSpans, left.factoredUnionSpans),
		operator:           SetIntersection,
		left:               left,
		right:              right,
	}
}

func opSpanExpressionAndDefault(
	left *SpanExpression, right InvertedExpression, op SetOperator,
) *SpanExpression {
	expr := &SpanExpression{
		tight:         left.Tight() && right.Tight(),
		spanningUnion: left.spanningUnion,
		operator:      op,
		left:          left,
		right:         right,
	}
	if op == SetUnion {
		expr.factoredUnionSpans = left.factoredUnionSpans
	}
	return expr
}

func unionSpans(left []InvertedSpan, right []InvertedSpan) []InvertedSpan {
	if len(left) == 0 {
		return right
	}
	if len(right) == 0 {
		return left
	}
	var spans []InvertedSpan
	var currSpan InvertedSpan
	var i, j int
	var lastLeft bool
	makeCurrSpan := func() {
		if i == len(left) {
			lastLeft = false
			currSpan = right[j]
			j++
			return
		}
		if j == len(right) {
			lastLeft = true
			currSpan = left[i]
			i++
			return
		}
		if bytes.Compare(left[i].start, right[j].start) < 0 {
			currSpan = left[i]
			i++
			lastLeft = true
		} else {
			currSpan = right[j]
			j++
			lastLeft = false
		}
	}
	makeLastLeft := func() {
		if !lastLeft {
			i, j = j, i
			left, right = right, left
			lastLeft = true
		}
	}
	makeCurrSpan()
	makeLastLeft()
	for j < len(right) {
		if cmpExcEndWithIncStart(currSpan, right[j]) >= 0 {
			if extendSpanEnd(&currSpan, right[j]) {
				lastLeft = false
				j++
				makeLastLeft()
			} else {
				j++
			}
			continue
		}
		spans = append(spans, currSpan)
		makeCurrSpan()
		makeLastLeft()
	}
	spans = append(spans, currSpan)
	spans = append(spans, left[i:]...)
	return spans
}

func intersectSpans(left []InvertedSpan, right []InvertedSpan) []InvertedSpan {
	if len(left) == 0 || len(right) == 0 {
		return nil
	}
	var spans []InvertedSpan
	var i, j int
	var currSpan InvertedSpan
	makeCurrSpanLeft := func() {
		if bytes.Compare(left[i].start, right[j].start) > 0 {
			i, j = j, i
			left, right = right, left
		}
		currSpan = left[i]
	}
	noCurrSpan := true
	for i < len(left) && j < len(right) {
		if noCurrSpan {
			makeCurrSpanLeft()
			noCurrSpan = false
		}
		cmpEndStart := cmpExcEndWithIncStart(currSpan, right[j])
		if cmpEndStart < 0 {
			// Either:
			// - currSpan.end != nil
			// - currSpan.end == nil and right[j].start == currSpan.start
			currSpan.start = right[j].start
			currSpanEnd := currSpan.end
			cmpEnd := cmpEndsWhenEqualStarts(currSpan, right[j])
			if cmpEnd > 0 {
				currSpan.end = right[j].end
			}
			spans = append(spans, currSpan)
			if cmpEnd < 0 {
				// invariant: right[j] must have right[j].end != nil
				i++
				currSpan.start = getExclusiveEnd(currSpan)
				currSpan.end = right[j].end
				i, j = j, i
				left, right = right, left
			} else if cmpEnd == 0 {
				i++
				j++
				noCurrSpan = true
			} else {
				// invariant: currSpanEnd != nil
				j++
				currSpan.start = getExclusiveEnd(currSpan)
				currSpan.end = currSpanEnd
			}
		} else {
			i++
			noCurrSpan = true
		}
	}
	return spans
}

// When right is a subset of left, subtracts right from left
func subtractSpans(left []InvertedSpan, right []InvertedSpan) []InvertedSpan {
	if len(right) == 0 {
		return left
	}
	var currSpan InvertedSpan
	var i, j int
	out := left[:0]
	noCurrSpan := true
	for j < len(right) {
		if noCurrSpan {
			currSpan = left[i]
		}
		cmpEndStart := cmpExcEndWithIncStart(currSpan, right[j])
		if cmpEndStart < 0 {
			// Either:
			// - currSpan.end != nil
			// - currSpan.end == nil and right[j].start == currSpan.start
			if currSpan.end == nil {
				i++
				j++
				noCurrSpan = true
				continue
			}
			cmpStart := bytes.Compare(left[i].start, right[j].start)
			if cmpStart > 0 {
				out = append(out, InvertedSpan{start: left[i].start, end: right[j].start})
				currSpan.start = right[j].start
			}
			// Else cmpStart == 0

			// Invariant: currSpan.start == right[j].start
			cmpEnd := cmpEndsWhenEqualStarts(currSpan, right[j])
			if cmpEnd == 0 {
				i++
				j++
				noCurrSpan = true
				continue
			}
			// Invariant: cmp > 0
			currSpan.start = getExclusiveEnd(right[j])
			j++
		} else {
			// cmpEndStart >= 0
			i++
			noCurrSpan = true
		}
	}
	out = append(out, currSpan)
	i++
	out = append(out, left[i:]...)
	return out
}

func cmpExcEndWithIncStart(left, right InvertedSpan) int {
	if left.end == nil {
		rightLen := len(right.start)
		if rightLen == len(left.start)+1 && right.start[rightLen-1] == '\x00' {
			return bytes.Compare(left.start, right.start[:rightLen-1])

		}
		return bytes.Compare(left.start, right.start)
	}
	return bytes.Compare(left.end, right.start)
}

func extendSpanEnd(left *InvertedSpan, right InvertedSpan) bool {
	if right.end == nil {
		return false
	}
	if left.end == nil {
		left.end = right.end
		return true
	}
	if bytes.Compare(left.end, right.end) < 0 {
		left.end = right.end
		return true
	}
	return false
}

func getExclusiveEnd(s InvertedSpan) EncInvertedVal {
	if s.end == nil {
		return roachpb.BytesNext(s.start)
	}
	return s.end
}

func cmpEndsWhenEqualStarts(left, right InvertedSpan) int {
	if left.end == nil && right.end == nil {
		return 0
	}
	cmpMultiplier := +1
	if left.end == nil {
		cmpMultiplier = -1
		left, right = right, left
	}
	if right.end == nil {
		// left.end = "c\x00", right = [c, c]. We need to return 0 for
		// this case.
		leftLen := len(left.end)
		if leftLen == len(right.start)+1 && left.end[leftLen-1] == '\x00' {
			return cmpMultiplier * bytes.Compare(left.end[:leftLen-1], right.start)
		}
		return cmpMultiplier
	}
	return cmpMultiplier * bytes.Compare(left.end, right.end)
}
