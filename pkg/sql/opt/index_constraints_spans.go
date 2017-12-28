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
//
// This file implements data structures used by index constraints generation.

package opt

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
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

// LogicalSpan is a high-level representation of a span. The Vals in Start and
// End are values for contiguous prefixes of index columns.
//
// Note: internally, we also use LogicalKeys with values for a non-prefix
// sequence of index columns (index columns starting at a given offset). Such
// a LogicalSpan is only meaningful in the context of a certain offset.
type LogicalSpan struct {
	// boundaries for the span.
	// If a column i is ascending, start.Vals[i] <= end.Vals[i].
	// If a column i is descending, start.Vals[i] >= end.Vals[i].
	Start, End LogicalKey
}

// MakeFullSpan creates an unconstrained span.
func MakeFullSpan() LogicalSpan {
	return LogicalSpan{
		Start: LogicalKey{Inclusive: true},
		End:   LogicalKey{Inclusive: true},
	}
}

// IsFullSpan returns true if the given span is unconstrained.
func (sp LogicalSpan) IsFullSpan() bool {
	return len(sp.Start.Vals) == 0 && len(sp.End.Vals) == 0
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
// contiguous sequence of index columns, starting at a given offset.
type LogicalSpans []LogicalSpan

type indexConstraintCtx struct {
	// types of the columns of the index we are generating constraints for.
	colInfos []IndexColumnInfo

	evalCtx *tree.EvalContext
}

func makeIndexConstraintCtx(
	colInfos []IndexColumnInfo, evalCtx *tree.EvalContext,
) indexConstraintCtx {
	return indexConstraintCtx{
		colInfos: colInfos,
		evalCtx:  evalCtx,
	}
}

// isIndexColumn returns true if e is an indexed var that corresponds
// to index column <offset>.
func (c *indexConstraintCtx) isIndexColumn(e *expr, index int) bool {
	return isIndexedVar(e, c.colInfos[index].VarIdx)
}

// compareKeyVals compares two lists of values for a sequence of index columns
// (namely <offset>, <offset+1>, ...). The directions of the index columns are
// taken into account.
//
// Returns 0 if the lists are equal, or one is a prefix of the other.
func (c *indexConstraintCtx) compareKeyVals(offset int, a, b tree.Datums) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if cmp := a[i].Compare(c.evalCtx, b[i]); cmp != 0 {
			if c.colInfos[offset+i].Direction == encoding.Descending {
				return -cmp
			}
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
func (c *indexConstraintCtx) compare(
	offset int, a, b LogicalKey, direction comparisonDirection,
) int {
	if cmp := c.compareKeyVals(offset, a.Vals, b.Vals); cmp != 0 {
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

// startsAfter returns true if a span that ends in prevSpanEnd cannot
// overlap with a span that starts at spanStart.
func (c *indexConstraintCtx) startsAfter(offset int, prevSpanEnd, spanStart LogicalKey) bool {
	cmp := c.compareKeyVals(offset, prevSpanEnd.Vals, spanStart.Vals)
	if cmp != 0 {
		return cmp < 0
	}
	switch {
	case len(prevSpanEnd.Vals) < len(spanStart.Vals):
		// The previous end key is a prefix of the current start key.
		// Overlap only if the end key is inclusive.
		return !prevSpanEnd.Inclusive

	case len(prevSpanEnd.Vals) > len(spanStart.Vals):
		// The current start key is a prefix of the previous end key.
		// Overlap only if the start key is inclusive.
		return !spanStart.Inclusive

	default:
		// The previous end key and the current start key have the same values.
		// If either is inclusive, the spans "touch".
		return !prevSpanEnd.Inclusive && !spanStart.Inclusive
	}
}

func (c *indexConstraintCtx) isSpanValid(offset int, sp *LogicalSpan) bool {
	a, b := sp.Start, sp.End
	if cmp := c.compareKeyVals(offset, a.Vals, b.Vals); cmp != 0 {
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
func (c *indexConstraintCtx) checkSpans(offset int, ls LogicalSpans) {
	for i := range ls {
		if !c.isSpanValid(offset, &ls[i]) {
			panic(fmt.Sprintf("invalid span %s", ls[i]))
		}
		if i > 0 && !c.startsAfter(offset, ls[i-1].End, ls[i].Start) {
			panic(fmt.Sprintf("spans not ordered and disjoint: %s, %s\n", ls[i-1], ls[i]))
		}
	}
}

// nextDatum returns the datum that follows d, in the given direction.
func (c *indexConstraintCtx) nextDatum(
	d tree.Datum, direction encoding.Direction,
) (tree.Datum, bool) {
	if direction == encoding.Descending {
		return d.Prev(c.evalCtx)
	}
	return d.Next(c.evalCtx)
}

// preferInclusive tries to convert exclusive keys to inclusive keys. This is
// only possible if the type supports Next/Prev.
//
// We prefer inclusive constraints because we can extend inclusive constraints
// with more constraints on columns that follow.
//
// Examples:
//  - for an integer column (/1 - /5)  =>  [/2 - /4].
//  - for a descending integer column (/5 - /1) => (/4 - /2).
//  - for a string column, we don't have Prev so
//      (/foo - /qux)  =>  [/foo\x00 - /qux).
//  - for a decimal column, we don't have either Next or Prev so we can't
//    change anything.
func (c *indexConstraintCtx) preferInclusive(offset int, sp *LogicalSpan) {
	if !sp.Start.Inclusive {
		col := len(sp.Start.Vals) - 1
		if nextVal, hasNext := c.nextDatum(
			sp.Start.Vals[col], c.colInfos[offset+col].Direction,
		); hasNext {
			sp.Start.Vals[col] = nextVal
			sp.Start.Inclusive = true
		}
	}

	if !sp.End.Inclusive {
		col := len(sp.End.Vals) - 1
		if prevVal, hasPrev := c.nextDatum(
			sp.End.Vals[col], c.colInfos[offset+col].Direction.Reverse(),
		); hasPrev {
			sp.End.Vals[col] = prevVal
			sp.End.Inclusive = true
		}
	}
}

// intersectSpan intersects two LogicalSpans and returns
// a new LogicalSpan. If there is no intersection, returns ok=false.
func (c *indexConstraintCtx) intersectSpan(
	offset int, a *LogicalSpan, b *LogicalSpan,
) (_ LogicalSpan, ok bool) {
	res := *a
	changed := 0
	if c.compare(offset, a.Start, b.Start, compareStartKeys) < 0 {
		res.Start = b.Start
		changed++
	}
	if c.compare(offset, a.End, b.End, compareEndKeys) > 0 {
		res.End = b.End
		changed++
	}
	// If changed is 0 or 2, the result is identical to one of the input spans.
	if changed == 1 && !c.isSpanValid(offset, &res) {
		return LogicalSpan{}, false
	}
	return LogicalSpan{Start: res.Start.clone(), End: res.End.clone()}, true
}

// intersectSpanSet intersects a span with (the union of) a set of spans. The
// spans in spanSet must be ordered and non-overlapping. The resulting spans are
// guaranteed to be ordered and non-overlapping.
func (c *indexConstraintCtx) intersectSpanSet(
	offset int, a *LogicalSpan, spanSet LogicalSpans,
) LogicalSpans {
	res := make(LogicalSpans, 0, len(spanSet))
	for i := range spanSet {
		if sp, ok := c.intersectSpan(offset, a, &spanSet[i]); ok {
			res = append(res, sp)
		}
	}
	c.checkSpans(offset, res)
	return res
}

// mergeSpanSets takes two sets of (ordered, non-overlapping) spans and
// generates their union (as ordered, non-overlapping spans).
func (c *indexConstraintCtx) mergeSpanSets(
	offset int, a LogicalSpans, b LogicalSpans,
) LogicalSpans {
	if len(a) == 0 && len(b) == 0 {
		return LogicalSpans{}
	}
	result := make(LogicalSpans, 0, len(a)+len(b))
	for len(a) > 0 || len(b) > 0 {
		if len(b) > 0 &&
			(len(a) == 0 || c.compare(offset, a[0].Start, b[0].Start, compareStartKeys) > 0) {
			// Swap the two sets, so that going forward a[0] starts before b[0].
			a, b = b, a
		}
		span := a[0]
		a = a[1:]

		// Merge this span with any overlapping spans in a or b. Initially, it can
		// only overlap with spans in b, but after the merge we can have new
		// overlaps; hence why this is a loop and we check against both a and b. For
		// example:
		//   a: [/1 /10] [/20 - /30] [/40 - /50]
		//   b: [/5 /25] [/30 - /40]
		//                           span
		//   initial:                [/1 - /10]
		//   merge with [/5 - /25]:  [/1 - /25]
		//   merge with [/20 - /30]: [/1 - /30]
		//   merge with [/30 - /40]: [/1 - /40]
		//   merge with [/40 - /50]: [/1 - /50]
		//
		for {
			var mergeSpan LogicalSpan
			if len(a) > 0 && !c.startsAfter(offset, span.End, a[0].Start) {
				mergeSpan = a[0]
				a = a[1:]
			} else if len(b) > 0 && !c.startsAfter(offset, span.End, b[0].Start) {
				mergeSpan = b[0]
				b = b[1:]
			} else {
				break
			}

			// Take the "max" of the end keys.
			if c.compare(offset, span.End, mergeSpan.End, compareEndKeys) < 0 {
				span.End = mergeSpan.End
			}
		}
		result = append(result, span)
	}
	return result
}

// isSpanSubset returns true if the spans in a are completely
// contained in the spans in b.
func (c *indexConstraintCtx) isSpanSubset(offset int, a LogicalSpans, b LogicalSpans) bool {
	for _, sp := range a {
		for len(b) > 0 && c.startsAfter(offset, b[0].End, sp.Start) {
			// b[0] ends before the first span in a.
			b = b[1:]
		}
		// b[0] is the first span that ends at or after sp.Start.
		// If a is a subset of b, b[0] must completely contain sp.
		if len(b) == 0 ||
			c.compare(offset, sp.Start, b[0].Start, compareStartKeys) < 0 ||
			c.compare(offset, sp.End, b[0].End, compareEndKeys) > 0 {
			return false
		}
	}
	return true
}

type logicalSpanSorter struct {
	c      *indexConstraintCtx
	offset int
	spans  []LogicalSpan
}

var _ sort.Interface = &logicalSpanSorter{}

// Len is part of sort.Interface.
func (ss *logicalSpanSorter) Len() int {
	return len(ss.spans)
}

// Less is part of sort.Interface.
func (ss *logicalSpanSorter) Less(i, j int) bool {
	// Compare start keys.
	return ss.c.compare(ss.offset, ss.spans[i].Start, ss.spans[j].Start, compareStartKeys) < 0
}

// Swap is part of sort.Interface.
func (ss *logicalSpanSorter) Swap(i, j int) {
	ss.spans[i], ss.spans[j] = ss.spans[j], ss.spans[i]
}

func (c *indexConstraintCtx) sortSpans(offset int, spans LogicalSpans) {
	ss := logicalSpanSorter{
		c:      c,
		offset: offset,
		spans:  spans,
	}
	sort.Sort(&ss)
}
