// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package opt

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// LogicalKey is a high-level representation of a span boundary.
type LogicalKey struct {
	// Vals is a list of values on consecutive index columns.
	Vals tree.Datums
	// Inclusive is true if the boundary includes Vals, or false otherwise.
	// An Inclusive span can be extended with constraints on more columns.
	// Empty keys (empty vals) are always Inclusive.
	Inclusive bool
}

func (k LogicalKey) String() string {
	if len(k.Vals) == 0 {
		return ""
	}
	var buf bytes.Buffer
	for _, val := range k.Vals {
		fmt.Fprintf(&buf, "/%s", val)
	}
	if k.Inclusive {
		buf.WriteString(" (inclusive)")
	} else {
		buf.WriteString(" (exclusive)")
	}
	return buf.String()
}

// Returns a copy that can be extended independently.
func (k LogicalKey) clone() LogicalKey {
	// Since the only modification we allow is extension, it's ok
	// to alias the slice as long as we limit the capacity.
	return LogicalKey{
		Vals:      k.Vals[:len(k.Vals):len(k.Vals)],
		Inclusive: k.Inclusive,
	}
}

// extend appends the given key.
func (k *LogicalKey) extend(l LogicalKey) {
	if !k.Inclusive {
		panic("extending exclusive logicalKey")
	}
	k.Vals = append(k.Vals, l.Vals...)
	k.Inclusive = l.Inclusive
}

// LogicalSpan is a high-level representation of a span.
// The Vals in Start and End map to a contiguous prefix of index columns.
//
// Note: internally, we also use LogicalKeys that map to a contiguous sequence
// of index columns (starting at a given "depth"). See calcDepth for more
// information.
type LogicalSpan struct {
	// boundaries for the span.
	Start, End LogicalKey
}

// MakeFullSpan creates an unconstrained span.
func MakeFullSpan() LogicalSpan {
	return LogicalSpan{
		Start: LogicalKey{Inclusive: true},
		End:   LogicalKey{Inclusive: true},
	}
}

// String formats a LogicalSpan. Inclusivity/exclusivity is shown using
// brackets/parens. Some examples:
//   [/1 - /2]
//   (/1/1 - /2)
//   [ - /5/6)
//   [/1 - ]
//   [ - ]
func (sp LogicalSpan) String() string {
	var buf bytes.Buffer

	print := func(k LogicalKey) {
		for _, val := range k.Vals {
			fmt.Fprintf(&buf, "/%s", val)
		}
	}
	if sp.Start.Inclusive {
		buf.WriteString("[")
	} else {
		buf.WriteString("(")
	}
	print(sp.Start)
	buf.WriteString(" - ")
	print(sp.End)
	if sp.End.Inclusive {
		buf.WriteString("]")
	} else {
		buf.WriteString(")")
	}
	return buf.String()
}

// LogicalSpans represents index constraints as a list of disjoint,
// ordered LogicalSpans.
//
// A few examples:
//  - no constraints: a single span with no boundaries [ - ]
//  - a constraint on @1 > 1: a single span (/1 - ]
//  - a constraint on @1 = 1 AND @2 > 1: a single span (/1/1 - /1]
//  - a contradiction: empty list.
//
// Note: internally, we also use LogicalSpans that correspond to a
// contiguous sequence of index columns, starting at a given "depth".
type LogicalSpans []LogicalSpan

type indexConstraintCalc struct {
	// types of the columns of the index we are generating constraints for.
	colTypes []types.T

	// andExprs is a set of conjuncts that make up the filter.
	andExprs []*expr

	evalCtx *tree.EvalContext

	// constraints[] is used as the memoization data structure for calcDepth.
	constraints []LogicalSpans
}

func makeIndexConstraintCalc(
	colTypes []types.T, andExprs []*expr, evalCtx *tree.EvalContext,
) indexConstraintCalc {
	return indexConstraintCalc{
		colTypes:    colTypes,
		andExprs:    andExprs,
		evalCtx:     evalCtx,
		constraints: make([]LogicalSpans, len(colTypes)),
	}
}

// compareKeyVals compares two lists of Datums that map to the same
// sequence of index columns. Returns 0 if the lists are equal, or one is a
// prefix of the other.
func (c *indexConstraintCalc) compareKeyVals(a, b tree.Datums) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if cmp := a[i].Compare(c.evalCtx, b[i]); cmp != 0 {
			return cmp
		}
	}
	return 0
}

type comparisonDirection int

const (
	compareStartKeys = +1
	compareEndKeys   = -1
)

// Compares two logicalKeys.
//
// The comparison between two logical keys depends on whether we are comparing
// start keys or end keys.
//
// For example:
//   a = /1/2
//   b = /1
//
// For start keys, a > b: [/1/2 - ...] is tighter than [/1 - ...].
// For end keys, a < b:   [... - /1/2] is tighter than [... - /1].
//
// A similar situation is when the keys have equal values (or one is a prefix of
// the other), and one is exclusive and one is not.
//
// For example:
//   a = /5 (inclusive)
//   b = /5 (exclusive)
//
// If we are comparing start keys, direction must be compareStartKeys (+1).
// If we are comparing end keys, direction must be compareEndKeys (-1).
func (c *indexConstraintCalc) compare(a, b LogicalKey, direction comparisonDirection) int {
	if cmp := c.compareKeyVals(a.Vals, b.Vals); cmp != 0 {
		return cmp
	}
	dirVal := int(direction)
	if len(a.Vals) < len(b.Vals) {
		// a matches a prefix of b.
		if !a.Inclusive {
			// Example:
			//  a = /1 (exclusive)
			//  b = /1/2 (inclusive)
			// Which of these is "smaller" depends on whether we are looking at a
			// start or end key.
			return dirVal
		}
		// Example:
		//   a = /1 (inclusive)
		//   b = /1/2 (inclusive)
		return -dirVal
	}
	// Case symmetric with the above.
	if len(a.Vals) > len(b.Vals) {
		if !b.Inclusive {
			return -dirVal
		}
		return dirVal
	}

	// Equal keys.
	if !a.Inclusive && b.Inclusive {
		// Example:
		//   a = /5 (exclusive)
		//   b = /5 (inclusive)
		return dirVal
	}
	if a.Inclusive && !b.Inclusive {
		return -dirVal
	}
	return 0
}

func (c *indexConstraintCalc) isSpanValid(sp *LogicalSpan) bool {
	a, b := sp.Start, sp.End
	if cmp := c.compareKeyVals(a.Vals, b.Vals); cmp != 0 {
		return cmp < 0
	}
	if len(a.Vals) < len(b.Vals) {
		// a is a prefix of b; a must be inclusive.
		return a.Inclusive
	}
	if len(a.Vals) > len(b.Vals) {
		// b is a prefix of a; b must be inclusive
		return b.Inclusive
	}
	return a.Inclusive && b.Inclusive
}

// checkSpans asserts that the given spans are ordered and non-overlapping.
func (c *indexConstraintCalc) checkSpans(ls LogicalSpans) {
	for i := range ls {
		if !c.isSpanValid(&ls[i]) {
			panic(fmt.Sprintf("invalid span %s", ls[i]))
		}
		if i > 0 {
			cmp := c.compareKeyVals(ls[i-1].End.Vals, ls[i].Start.Vals)
			if cmp > 0 {
				panic(fmt.Sprintf("spans not ordered and disjoint: %s, %s\n", ls[i-1], ls[i]))
			}
			if cmp < 0 {
				continue
			}
			switch {
			case len(ls[i-1].End.Vals) < len(ls[i].Start.Vals):
				// The previous end key is a prefix of the current start key. Only
				// acceptable if the end key is exclusive.
				if ls[i-1].End.Inclusive {
					panic(fmt.Sprintf("spans not ordered and disjoint: %s, %s\n", ls[i-1], ls[i]))
				}

			case len(ls[i-1].End.Vals) > len(ls[i].Start.Vals):
				// The current start key is a prefix of the previous end key. Only
				// acceptable if the start key is exclusive.
				if ls[i].Start.Inclusive {
					panic(fmt.Sprintf("spans not ordered and disjoint: %s, %s\n", ls[i-1], ls[i]))
				}

			default:
				// The previous end key and the current start key have the same values.
				// Only acceptable if they are both exclusive.
				if ls[i-1].End.Inclusive || ls[i].Start.Inclusive {
					panic(fmt.Sprintf("spans not ordered and disjoint: %s, %s\n", ls[i-1], ls[i]))
				}
			}
		}
	}
}

// preferInclusive tries to convert exclusive keys to inclusive keys. This is
// only possible if the type supports Next/Prev.
//
// We prefer inclusive constraints because we can extend inclusive constraints
// with more constraints on columns that follow.
//
// Examples:
//  - for an integer column (/1 - /5)  =>  [/2 - /4].
//  - for a string column, we don't have Prev so
//      (/foo - /qux)  =>  [/foo\x00 - /qux).
//  - for a decimal column, we don't have either Next or Prev so we can't
//    convert anything.
func (c *indexConstraintCalc) preferInclusive(sp *LogicalSpan) {
	if !sp.Start.Inclusive {
		col := len(sp.Start.Vals) - 1
		if nextVal, hasNext := sp.Start.Vals[col].Next(c.evalCtx); hasNext {
			sp.Start.Vals[col] = nextVal
			sp.Start.Inclusive = true
		}
	}

	if !sp.End.Inclusive {
		col := len(sp.End.Vals) - 1
		if prevVal, hasPrev := sp.End.Vals[col].Prev(c.evalCtx); hasPrev {
			sp.End.Vals[col] = prevVal
			sp.End.Inclusive = true
		}
	}
}

// intersectSpan intersects two LogicalSpans and returns
// a new LogicalSpan. If there is no intersection, returns ok=false.
func (c *indexConstraintCalc) intersectSpan(
	a *LogicalSpan, b *LogicalSpan,
) (_ LogicalSpan, ok bool) {
	res := *a
	changed := 0
	if c.compare(a.Start, b.Start, compareStartKeys) < 0 {
		res.Start = b.Start
		changed++
	}
	if c.compare(a.End, b.End, compareEndKeys) > 0 {
		res.End = b.End
		changed++
	}
	// If changed is 0 or 2, the result is identical to one of the input spans.
	if changed == 1 && !c.isSpanValid(&res) {
		return LogicalSpan{}, false
	}
	return LogicalSpan{Start: res.Start.clone(), End: res.End.clone()}, true
}

// intersectSpanSet intersects a span with (the union of) a set of spans. The
// spans in spanSet must be ordered and non-overlapping. The resulting spans are
// guaranteed to be ordered and non-overlapping.
func (c *indexConstraintCalc) intersectSpanSet(a *LogicalSpan, spanSet LogicalSpans) LogicalSpans {
	c.checkSpans(spanSet)

	res := make(LogicalSpans, 0, len(spanSet))
	for i := range spanSet {
		if sp, ok := c.intersectSpan(a, &spanSet[i]); ok {
			res = append(res, sp)
		}
	}
	c.checkSpans(res)
	return res
}

// makeSpansForExpr creates spans for index columns starting at <depth>
// from the given expression.
func (c *indexConstraintCalc) makeSpansForExpr(depth int, e *expr) (LogicalSpans, bool) {
	// Check if the operator is supported.
	switch e.op {
	case eqOp, ltOp, gtOp, leOp, geOp, neOp:
		// We support comparisons when the left-hand side is an indexed var for the
		// current column and the right-hand side is a constant.
		if !isIndexedVar(e.children[0], depth) || e.children[1].op != constOp {
			return nil, false
		}
		datum := e.children[1].private.(tree.Datum)
		sp := MakeFullSpan()
		switch e.op {
		case eqOp:
			sp.Start = LogicalKey{Vals: tree.Datums{datum}, Inclusive: true}
			sp.End = LogicalKey{Vals: tree.Datums{datum}, Inclusive: true}
		case ltOp:
			sp.End = LogicalKey{Vals: tree.Datums{datum}, Inclusive: false}
			c.preferInclusive(&sp)
		case gtOp:
			sp.Start = LogicalKey{Vals: tree.Datums{datum}, Inclusive: false}
			c.preferInclusive(&sp)
		case leOp:
			sp.End = LogicalKey{Vals: tree.Datums{datum}, Inclusive: true}
		case geOp:
			sp.Start = LogicalKey{Vals: tree.Datums{datum}, Inclusive: true}
		case neOp:
			sp.End = LogicalKey{Vals: tree.Datums{datum}, Inclusive: false}
			sp2 := MakeFullSpan()
			sp2.Start = LogicalKey{Vals: tree.Datums{datum}, Inclusive: false}
			return LogicalSpans{sp, sp2}, true
		}
		return LogicalSpans{sp}, true

	default:
		return nil, false
	}
}

// calcDepth calculates constraints for the sequence of index columns starting
// at <depth> (i.e. the <depth>-th index column, <depth+1>-th index column, etc.).
//
// It works as follows: we look at expressions which constrain column <depth>
// and generate spans from those expressions. Then, we try to extend those span
// keys with more columns by recursively calculating the constraints for a
// higher <depth>. The results are memoized so each depth is calculated at most
// once.
//
// For example:
//   @1 >= 5 AND @1 <= 10 AND @2 = 2 AND @3 > 1
//
// - calcDepth(depth = 0):          // calculate constraints for @1, @2, @3
//   - process expressions, generating span [/5 - /10]
//   - call calcDepth(depth = 1):   // calculate constraints for @2, @3
//     - process expressions, generating span [/2 - /2]
//     - call calcDepth(depth = 2): // calculate constraints for @3
//       - process expressions, return span (/1 - ]
//     - extend the keys in the span [/2 - /2] with (/1 - ], return (/2/1 - /2].
//   - extend the keys in the span [/5 - /10] with (/2/1 - /2], return
//     (/5/2/1 - /10/2].
//
// TODO(radu): add an example with tuple constraints once they are supported.
func (c *indexConstraintCalc) calcDepth(depth int) LogicalSpans {
	if c.constraints[depth] != nil {
		// The results of this function are memoized.
		return c.constraints[depth]
	}

	// Start with a span with no boundaries.
	spans := LogicalSpans{MakeFullSpan()}

	// TODO(radu): sorting the expressions by the variable index, or pre-building
	// a map could help here.
	for _, e := range c.andExprs {
		exprSpans, ok := c.makeSpansForExpr(depth, e)
		if !ok {
			continue
		}

		if len(exprSpans) == 1 {
			// More efficient path for the common case of a single expression span.
			for i := 0; i < len(spans); i++ {
				if spans[i], ok = c.intersectSpan(&spans[i], &exprSpans[0]); !ok {
					// Remove this span.
					copy(spans[i:], spans[i+1:])
					spans = spans[:len(spans)-1]
				}
			}
		} else {
			var newSpans LogicalSpans
			for i := range spans {
				newSpans = append(newSpans, c.intersectSpanSet(&spans[i], exprSpans)...)
			}
			spans = newSpans
		}
	}

	// Try to extend the spans with constraints on more columns.

	// newSpans accumulates the extended spans, but is initialized only if we need
	// to break up a span into multiple spans (otherwise spans is modified in
	// place).
	var newSpans LogicalSpans
	for i := 0; i < len(spans); i++ {
		start, end := spans[i].Start, spans[i].End
		startLen, endLen := len(start.Vals), len(end.Vals)

		// Currently startLen, endLen can be at most 1; but this will change
		// when we will support tuple expressions.

		if startLen == endLen && startLen > 0 && depth+startLen < len(c.colTypes) &&
			c.compareKeyVals(start.Vals, end.Vals) == 0 {
			// Special case when the start and end keys are equal (i.e. an exact value
			// on this column). This is the only case where we can break up a single
			// span into multiple spans.
			//
			// For example:
			//  @1 = 1 AND @2 IN (3, 4, 5)
			//  At depth 0 so far we have:
			//    [/1 - /1]
			//  At depth 1 we have:
			//    [/3 - /3]
			//    [/4 - /4]
			//    [/5 - /5]
			//  We break up the span to get:
			//    [/1/3 - /1/3]
			//    [/1/4 - /1/4]
			//    [/1/5 - /1/5]
			s := c.calcDepth(depth + startLen)
			switch len(s) {
			case 0:
			case 1:
				spans[i].Start.extend(s[0].Start)
				spans[i].End.extend(s[0].End)
				if newSpans != nil {
					newSpans = append(newSpans, spans[i])
				}
			default:
				if newSpans == nil {
					newSpans = make(LogicalSpans, 0, len(spans)-1+len(s))
					newSpans = append(newSpans, spans[:i]...)
				}
				for j := range s {
					newSpan := spans[i]
					newSpan.Start.extend(s[j].Start)
					newSpan.End.extend(s[j].End)
					newSpans = append(newSpans, newSpan)
				}
			}
			continue
		}

		if startLen > 0 && depth+startLen < len(c.colTypes) && spans[i].Start.Inclusive {
			// We can advance the starting boundary. Calculate constraints for the
			// column that follows.
			s := c.calcDepth(depth + startLen)
			if len(s) > 0 {
				// If we have multiple constraints, we can only use the start of the
				// first one to tighten the span.
				// For example:
				//   @1 >= 2 AND @2 IN (1, 2, 3).
				//   At depth 0 so far we have:
				//     [/2 - ]
				//   At depth 1 we have:
				//     [/1 - /1]
				//     [/2 - /2]
				//     [/3 - /3]
				//   The best we can do is tighten the span to:
				//     [/2/1 - ]
				spans[i].Start.extend(s[0].Start)
			}
		}

		if endLen > 0 && depth+endLen < len(c.colTypes) && spans[i].End.Inclusive {
			// We can restrict the ending boundary. Calculate constraints for the
			// column that follows.
			s := c.calcDepth(depth + endLen)
			if len(s) > 0 {
				// If we have multiple constraints, we can only use the end of the
				// last one to tighten the span.
				spans[i].End.extend(s[len(s)-1].End)
			}
		}
		if newSpans != nil {
			newSpans = append(newSpans, spans[i])
		}
	}
	if newSpans != nil {
		spans = newSpans
	}
	c.checkSpans(spans)
	c.constraints[depth] = spans
	return spans
}

// MakeIndexConstraints generates constraints from a scalar boolean filter
// expression. See LogicalSpans for more information on how constraints are
// represented.
//
// TODO(radu): for now we assume the index columns are always columns
// @1, @2, @3, etc. Eventually we will need to pass in a list of column
// indices.
func MakeIndexConstraints(
	filter *expr, colTypes []types.T, evalCtx *tree.EvalContext,
) LogicalSpans {
	var andExprs []*expr
	if filter.op == andOp {
		andExprs = filter.children
	} else {
		andExprs = []*expr{filter}
	}
	c := makeIndexConstraintCalc(colTypes, andExprs, evalCtx)
	return c.calcDepth(0)
}
