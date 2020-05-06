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

// It would be ideal if the inverted column only contained Datums, since we
// could then work with a Datum here. However, JSON breaks that approach:
// - JSON inverted columns use a custom encoding that uses a special byte
//   jsonInvertedIndex, followed by the bytes produced by the various
//   implementations of the encodeInvertedIndexKey() method in the JSON
//   interface. This could be worked around by using a JSON datum that
//   represents a single path as the start key of the span, and representing
//   [start, start] spans. We would special case the encoding logic to
//   recognize that it is dealing with JSON (we have similar special path code
//   for JSON elsewhere). But this is insufficient (next bullet).
// - Expressions like x ? 'b' don't have operands that are JSON, but can be
//   represented using a span on the inverted column.
//
// So we make it the job of the caller of this library to encode the inverted
// column. Note that the second bullet above has some similarities with the
// behavior in makeStringPrefixSpan(), except there we can represent the start
// and end keys using the string type.
type EncInvertedVal []byte

// Unlike the logic in the constraints package, the spans of the inverted
// index cannot be immediately "evaluated" since they represent sets of
// primary keys that we won't know about until we do the scan. Using a simple
// example: [a, d) \intersection [c, f) is not [c, d) since the same primary
// key K could be found under a and f and be part of the result. More
// precisely, the above expression can be simplified to:
// [c, d) \union ([a, c) \intersection [d, f))
//
// For regular indexes, since each primary key is indexed in one row of the
// index, we can be sure that the same primary key will not appear in both of
// the non-overlapping spans [a, c) and [d, f), so we can immediately throw
// that part away knowing that it is the empty set. This discarding is not
// possible with inverted indexes, though the factoring can be useful for
// speed of execution (it does not limit what we need to scan) and for
// selectivity estimation when making optimizer choices.
//
// One could try to construct a general library that handles both the
// cases handled in the constraints package and here, but the complexity seems
// high. Instead, this package is more general than constraints in a few ways
// but simplifies most other things (so overall much simpler):
// - All the inverted spans are either [start, end) or [start, start].
// - It handles spans only on the inverted column, with a way to plug-in spans
//   generated for the PK columns (more on this below).
//
// The other significant point of departure from the constraints package is
// that this package represents a canonical form for all inverted expressions
// -- it is not just span "constraints" for a scan. The evaluation machinery
// will evaluate this expression. The support to build that canonical form
// expression is independent of how the original expression is represented:
// instead of taking an opt.Expr parameter and traversing it itself, this
// library assumes the caller is doing a traversal. This is partly because the
// representation of the original expression for the single table scan case
// and the invertedJoiner case are not the same: the latter starts with an
// expression with two unspecified rows, and after the left side row is bound
// (partial application), this library needs to be used to construct the
// InvertedExpression.
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
//   When the constraint for (@2 = 76) is anded we traverse this built tree
//   and add this constraint to each node. Note that we are delaying building
//   something akin to a constraint.Key since we are encountering the constraints
//   in arbitrary column order. Then after the full expression tree is built,
//   one traverses and builds the inverted spans and primary key spans (latter
//   could reuse constraint.Span for each node).
// - The previous bullet is doable but complicated, and especially increases the
//   complexity of factoring spans when unioning and intersecting while building
//   up sub-expressions. One needs to either factor taking into account the
//   current per-column PK constraints or delay it until the end (I gave up
//   half-way through writing the code, as it doesn't seem worth the complexity).
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
//   incrementally, and this factoring is straightforward since it involves a
//   single column.
// - Generating the PK spans (optional): The caller can use something like
//   idxconstraint, pretending that the PK columns of the inverted index
//   are the index columns. Every leaf inverted sub-expression is replaced
//   with true. This is because when not representing the inverted column
//   constraint we need the weakest possible constraint on the PK columns.
//   Using example 1' again,
//   ((f(@1, 1) AND @3 = 5) OR (f(@1, 3) AND @3 = 10)) AND (@2 = 76)
//   when generating the PK constraints we would use
//   (@3 = 5 OR @3 = 10) AND (@2 = 76)
//   So the PK spans will be:
//   [/76/5, /76/5], [/76/10, /76/10]
// - The spans in the inverted index expression can be composed with the
//   spans of the PK columns to narrow wherever possible.
//   Continuing with example 1', the inverted index expression will be
//   v11 \union v13, corresponding to f(@1, 1) and f(@1, 3), where each
//   of v11 and v13 are single value spans. And this expression is not tight
//   (because of the anding with NonInvertedColExpression).
//   The PK spans, [/76/5, /76/5], [/76/10, /76/10], are also single key spans.
//   This is a favorable example in that we can compose all these singleton
//   spans to get single inverted index rows:
//   /v11/76/5, /v11/76/10, /v13/76/5, /v13/76/10
//   (this is also a contrived example since with such narrow constraints
//   on the PK, we would possibly not use the inverted index).
//
//   If one constructs example 2' (derived from example 2 in the same way
//   we derived example 1'), we would have
//   ((f(@1, 1) AND @2 = 5) OR (f(@1, 3) AND @2 = 10)) AND (@3 = 76)
//   and the inverted index expression would be:
//   v11 \union v13
//   and the PK spans:
//   [/5/76, /5/76], [/10/76, /10/76]
//   And so the inverted index rows would be:
//   /v11/5/76, /v11/10/76, /v13/5/76, /v13/10/76
//   This is worse than example 2 (and resembles example 1 and 1') since
//   we are taking the cross-product.
//
//   TODO(sumeer): write this composition code.
//
// TODO(sumeer): work out how this will change when we have partitioned
// inverted indexes, where some columns of the primary key will appear before
// the inverted column.

// InvertedSpan is a span of the inverted index.
type InvertedSpan struct {
	// [start, end) iff end != nil, else represents [start, start].
	start, end    EncInvertedVal
	singleKeySpan bool
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
// Optimizer cost estimation
//
// There are two cases:
// - Single table expression: after generating the InvertedExpression, the
//   optimizer will check that it is a *SpanExpression -- if not, it is a
//   NonInvertedColExpression, which implies a full inverted index scan, and
//   definitely not worth using the inverted index. There are two costs for
//   using the inverted index:
//   - The scan cost: this should be estimated by using SpanExpression.spanningUnion.
//   - The cardinality of the output set after evaluating the expression: this
//     requires a traversal of the expression to assign cardinality to the
//     spans in each factoredUnionSpans (this could be done using a mean,
//     or using histograms). The cardinality of a SpanExpression is the
//     cardinality of the union of its factoredUnionSpans and the intersection
//     of its left and right expressions. If the cardinality of the original
//     table is C (i.e., the number of primary keys), and we have two subsets
//     of cardinality C1 and C2, we can assume that each set itself is a
//     drawing without replacement from the original table. This can be
//     used to derive the expected cardinality of the union of the two sets
//     and the intersection of the two sets.
//
// - Join expression: Assigning a cost is hard since there are two
//   parameters, corresponding to the left and right columns. In some cases,
//   like Geospatial, the expression that could be generated is a black-box to
//   the optimizer since the quad-tree traversal is unknown until partial
//   application (when one of the parameters is known). Minimally, we do need to
//   know whether the user expression is going to cause a full inverted index
//   scan due to parts of the expression referring to non-inverted columns.
//   The optimizer will provide its own placeholder implementation of
//   InvertedExpression into which it can embed whatever information it wants.
//   Let's call this the UnknownExpression -- it will only exist at the
//   leaves of the expression tree. It will use this UnknownExpression
//   whenever there is an expression involving both the inverted columns. If
//   the final expression is a NonInvertedColExpression, it is definitely not
//   worth using the inverted index. If the final expression is an
//   UnknownExpression (the tree must be a single node) or a *SpanExpression,
//   the optimizer could either conjure up some magic cost number or try to
//   compose one using costs assigned to each span (as described in the
//   previous bullet) and to each leaf-level UnknownExpression.
//
// Query evaluation
//
// There are two cases:
// - Single table expression: The optimizer will convert the *SpanExpression
//   into a form that is passed to the evaluation machinery, which can recreate
//   the *SpanExpression and evaluate it. The optimizer will have constructed
//   the spans for the evaluation using SpanExpression.spanningUnion, so the
//   expression evaluating code does not need to concern itself with the spans
// - Join expression: The optimizer had an expression tree with the root as
//   a *SpanExpression or an UnknownExpression. Therefore it knows that after
//   partial application the expression will be a *SpanExpression. It passes the
//   inverted expression with two unknowns, as a string, to the join execution
//   machinery. The optimizer provides a way to do partial application for each
//   input row, and returns a *SpanExpression, which is evaluated on the
//   inverted index.
type InvertedExpression interface {
	// IsTight returns whether the inverted expression is tight, i.e., will the
	// original expression not need to be reevaluated on each row output by the
	// query evaluation over the inverted index.
	IsTight() bool
	// SetNotTight sets tight to false.
	SetNotTight()
}

// SpanExpression is an implementation of InvertedExpression.
type SpanExpression struct {
	// Tight mirrors the definition of IsTight().
	Tight bool

	// SpanningUnion are the spans to read from the inverted index
	// to evaluate this SpanExpression. These are non-overlapping
	// and sorted. If left or right contains a non-SpanExpression,
	// it is not included in the spanning union.
	// To illustrate, consider a made up example:
	// [2, 10) \intersection [6, 14)
	// is factored into:
	// [6, 10) \union ([2, 6) \intersection [10, 14))
	// The root expression has a spanning union of [2, 14).
	SpanningUnion []InvertedSpan

	// FactoredUnionSpans are the spans to be unioned. These are
	// non-overlapping and sorted.
	// Using the same example, the FactoredUnionSpans will be
	// [6, 10).
	// If this factored expression is intersected with [5, 8) we would get
	// [6, 8) \union ([5, 6) \intersection ([8, 10) \union ([2, 6) \intersection [10, 14))))
	// The top-level expression has FactoredUnionSpans [6, 8), and the left and
	// right children have factoredUnionSpans [5, 6) and [8, 10) respectively.
	// The SpanningUnion of this top-level expression is still [2, 14) since
	// the intersection with [5, 8) did not add anything to the spans to read.
	FactoredUnionSpans []InvertedSpan

	// Operator is the set operation to apply to Left and Right.
	// When this is union or intersection, both Left and Right are non-nil,
	// else both are nil.
	Operator SetOperator
	Left     InvertedExpression
	Right    InvertedExpression
}

var _ InvertedExpression = (*SpanExpression)(nil)

// IsTight implements the InvertedExpression interface.
func (s *SpanExpression) IsTight() bool {
	return s.Tight
}

// SetNotTight implements the InvertedExpression interface.
func (s *SpanExpression) SetNotTight() {
	s.Tight = false
}

// NonInvertedColExpression is an expression to use for parts of the
// user expression that do not involve the inverted index.
type NonInvertedColExpression struct{}

var _ InvertedExpression = NonInvertedColExpression{}

// IsTight implements the InvertedExpression interface.
func (n NonInvertedColExpression) IsTight() bool {
	return false
}

// SetNotTight implements the InvertedExpression interface.
func (n NonInvertedColExpression) SetNotTight() {}

// ExprForInvertedSpan constructs a leaf-level SpanExpression
// for an inverted expression. Note that these leaf-level
// expressions may also have tight = false. Geospatial functions
// are all non-tight.
//

// For JSON, expressions like x <@ '{"a":1, "b":2}'::json will have
// tight = false. Say SpanA, SpanB correspond to "a":1 and "b":2
// respectively). A tight expression would require the following set
// evaluation:
// Set(SpanA) \union Set(SpanB) - Set(ComplementSpan(SpanA \spanunion SpanB))
// where ComplementSpan(X) is everything in the inverted index
// except for X.
// Since ComplementSpan(SpanA \spanunion SpanB) is likely to
// be very wide when SpanA and SpanB are narrow, or vice versa,
// this tight expression would be very costly to evaluate.
func ExprForInvertedSpan(span InvertedSpan, tight bool) *SpanExpression {
	return &SpanExpression{
		Tight:              tight,
		SpanningUnion:      []InvertedSpan{span},
		FactoredUnionSpans: []InvertedSpan{span},
	}
}

// And of two boolean expressions.
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
				Tight:    left.IsTight() && right.IsTight(),
				Operator: SetIntersection,
				Left:     left,
				Right:    right,
			}
		}
	}
}

// Or of two boolean expressions.
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
				Tight:    left.IsTight() && right.IsTight(),
				Operator: SetUnion,
				Left:     left,
				Right:    right,
			}
		}
	}
}

func opSpanExpressionAndDefault(
	left *SpanExpression, right InvertedExpression, op SetOperator,
) *SpanExpression {
	expr := &SpanExpression{
		Tight:         left.IsTight() && right.IsTight(),
		SpanningUnion: left.SpanningUnion,
		Operator:      op,
		Left:          left,
		Right:         right,
	}
	if op == SetUnion {
		expr.FactoredUnionSpans = left.FactoredUnionSpans
	}
	return expr
}

func intersectSpanExpressions(left, right *SpanExpression) *SpanExpression {
	expr := &SpanExpression{
		Tight:              left.Tight && right.Tight,
		SpanningUnion:      unionSpans(left.SpanningUnion, right.SpanningUnion),
		FactoredUnionSpans: intersectSpans(left.FactoredUnionSpans, right.FactoredUnionSpans),
		Operator:           SetIntersection,
		Left:               left,
		Right:              right,
	}
	if expr.FactoredUnionSpans != nil {
		left.FactoredUnionSpans = subtractSpans(left.FactoredUnionSpans, expr.FactoredUnionSpans)
		right.FactoredUnionSpans = subtractSpans(right.FactoredUnionSpans, expr.FactoredUnionSpans)
	}
	return expr
}

func unionSpanExpressions(left, right *SpanExpression) *SpanExpression {
	expr := &SpanExpression{
		Tight:              left.Tight && right.Tight,
		SpanningUnion:      unionSpans(left.SpanningUnion, right.SpanningUnion),
		FactoredUnionSpans: unionSpans(left.FactoredUnionSpans, left.FactoredUnionSpans),
		Operator:           SetIntersection,
		Left:               left,
		Right:              right,
	}
	left.FactoredUnionSpans = nil
	right.FactoredUnionSpans = nil
	return expr
}

func unionSpans(left []InvertedSpan, right []InvertedSpan) []InvertedSpan {
	if len(left) == 0 {
		return right
	}
	if len(right) == 0 {
		return left
	}
	// Both left and right are non-empty.

	// The output spans.
	var spans []InvertedSpan
	// Contains the current span being merged into.
	var mergeSpan InvertedSpan
	// Indexes into left and right.
	var i, j int

	swapLeftRight := func() {
		i, j = j, i
		left, right = right, left
	}

	// makeMergeSpan is used to initialize mergeSpan. It uses the span from
	// left or right that has an earlier start. Additionally, it swaps left
	// and right if the mergeSpan was initialized using right, so tha mergeSpan
	// is coming from the left.
	// REQUIRES: i < len(left) || j < len(right).
	makeMergeSpan := func() {
		if i >= len(left) || (j < len(right) && bytes.Compare(left[i].start, right[j].start) > 0) {
			swapLeftRight()
		}
		mergeSpan = left[i]
		i++
	}
	makeMergeSpan()
	// We only need to merge spans into mergeSpan while we have more
	// spans from the right. Once the right is exhausted we know that
	// the remaining spans from the left (including mergeSpan) can be
	// appended to the output unchanged.
	for j < len(right) {
		if cmpExcEndWithIncStart(mergeSpan, right[j]) >= 0 {
			if extendSpanEnd(&mergeSpan, right[j]) {
				// The right side extended the span, so now it plays the
				// role of the left.
				j++
				swapLeftRight()
			} else {
				j++
			}
			continue
		}
		// Cannot extend mergeSpan.
		spans = append(spans, mergeSpan)
		makeMergeSpan()
	}
	spans = append(spans, mergeSpan)
	spans = append(spans, left[i:]...)
	return spans
}

func intersectSpans(left []InvertedSpan, right []InvertedSpan) []InvertedSpan {
	if len(left) == 0 || len(right) == 0 {
		return nil
	}

	// Both left and right are non-empty

	// The output spans.
	var spans []InvertedSpan
	// Indexes into left and right.
	var i, j int
	// Contains the current span being intersected.
	var mergeSpan InvertedSpan
	var mergeSpanInitialized bool
	swapLeftRight := func() {
		i, j = j, i
		left, right = right, left
	}
	// Initializes mergeSpan. Additionally, arranges it such that the span has
	// come from left. i continues to refer to the index used to initialize
	// mergeSpan.
	// REQUIRES: i < len(left) && j < len(right)
	makeMergeSpan := func() {
		if bytes.Compare(left[i].start, right[j].start) > 0 {
			swapLeftRight()
		}
		mergeSpan = left[i]
		mergeSpanInitialized = true
	}

	for i < len(left) && j < len(right) {
		if !mergeSpanInitialized {
			makeMergeSpan()
		}
		cmpEndStart := cmpExcEndWithIncStart(mergeSpan, right[j])
		if cmpEndStart > 0 {
			// The intersection of these spans is non-empty. Cases:
			// - mergeSpan.end != nil
			// - mergeSpan.end == nil and right[j].start == currSpan.start
			mergeSpan.start = right[j].start
			mergeSpanEnd := mergeSpan.end
			cmpEnds := cmpEndsWhenEqualStarts(mergeSpan, right[j])
			if cmpEnds > 0 {
				// The right span constrains the end of the intersection.
				// Note that this is only possible if mergeSpan.end != nil.
				// It is possible that right[j].end == nil.
				mergeSpan.end = right[j].end
			}
			// Else the mergeSpan is not constrained by the right span,
			// so it is already ready to be appended to the output.

			// Append to the spans that will be output.
			spans = append(spans, mergeSpan)

			// Now decide whether we should continue intersecting with what
			// is left of the original mergeSpan.
			if cmpEnds < 0 {
				// The mergeSpan constrained the end of the intersection.
				// So nothing left of the original mergeSpan. The rightSpan
				// should become the new mergeSpan since it is guaranteed to
				// have a start <= the next span from the left and it has
				// something leftover.
				// Note that in this case right[j].end != nil
				i++
				mergeSpan.start = getExclusiveEnd(mergeSpan)
				mergeSpan.end = right[j].end
				swapLeftRight()
			} else if cmpEnds == 0 {
				// Both spans end at the same key, so both are consumed.
				i++
				j++
				mergeSpanInitialized = false
			} else {
				// The right span constrained the end of the intersection.
				// So there is something left of the original mergeSpan.
				// Note that mergeSpanEnd != nil.
				j++
				mergeSpan.start = getExclusiveEnd(mergeSpan)
				mergeSpan.end = mergeSpanEnd
			}
		} else {
			// Intersection is empty
			i++
			mergeSpanInitialized = false
		}
	}
	return spans
}

// subtractSpans subtracts right from left, under the assumption that right is a
// subset of left. The output is placed in left.
func subtractSpans(left []InvertedSpan, right []InvertedSpan) []InvertedSpan {
	if len(right) == 0 {
		return left
	}

	// Both left and right are non-empty

	// Contains the current span being subtracted.
	var mergeSpan InvertedSpan
	var mergeSpanInitialized bool
	// Indexes into left and right.
	var i, j int
	out := left[:0]
	for j < len(right) {
		if !mergeSpanInitialized {
			mergeSpan = left[i]
			mergeSpanInitialized = true
		}
		cmpEndStart := cmpExcEndWithIncStart(mergeSpan, right[j])
		if cmpEndStart > 0 {
			// mergeSpan will have some part subtracted by the right span. Cases:
			// - mergeSpan.end != nil
			// - mergeSpan.end == nil and right[j].start == mergeSpan.start and
			//   right[j].end == mergeSpan.end.
			if mergeSpan.end == nil {
				i++
				j++
				mergeSpanInitialized = false
				continue
			}
			cmpStart := bytes.Compare(mergeSpan.start, right[j].start)
			if cmpStart < 0 {
				// There is some part of mergeSpan before the right span starts. Add it
				// to the output.
				out = append(out, InvertedSpan{start: mergeSpan.start, end: right[j].start})
				mergeSpan.start = right[j].start
			}
			// Else cmpStart == 0

			// Invariant: mergeSpan.start == right[j].start
			cmpEnd := cmpEndsWhenEqualStarts(mergeSpan, right[j])
			if cmpEnd == 0 {
				// Both spans end at the same key, so both are consumed.
				i++
				j++
				mergeSpanInitialized = false
				continue
			}

			// Invariant: cmp > 0
			mergeSpan.start = getExclusiveEnd(right[j])
			j++
		} else {
			// Right span starts after mergeSpan ends.
			out = append(out, mergeSpan)
			i++
			mergeSpanInitialized = false
		}
	}
	if mergeSpanInitialized {
		out = append(out, mergeSpan)
		i++
	}
	out = append(out, left[i:]...)
	return out
}

// Compares the exclusive end key of left with the inclusive start key of
// right.
// Examples:
// [a, b), [b, c) == 0
// [a, a], [a, c) == +1
// [a, a\x00), [a, c) == +1
// [a, c), [d, e) == -1
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

// Extends the left span using the right span. Returns true iff
// left was extended. So it will return false if right ends at a key
// <= the end of the left.
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

// Returns the exclusive end key of the span.
// Examples:
// [a, c) returns c
// [a, a] returns a\x00
func getExclusiveEnd(s InvertedSpan) EncInvertedVal {
	if s.end == nil {
		return roachpb.BytesNext(s.start)
	}
	return s.end
}

// Compares the end keys of left and right given that the start
// keys are the same.
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
