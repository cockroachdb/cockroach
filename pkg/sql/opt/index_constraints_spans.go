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

type indexCtx struct {
	// types of the columns of the index we are generating constraints for.
	colInfos []IndexColumnInfo

	evalCtx *tree.EvalContext
}

// compareKeyVals compares two lists of values for a sequence of index columns
// (namely <offset>, <offset+1>, ...). The directions of the index columns are
// taken into account.
//
// Returns 0 if the lists are equal, or one is a prefix of the other.
func (c *indexCtx) compareKeyVals(offset int, a, b tree.Datums) int {
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
func (c *indexCtx) compare(offset int, a, b LogicalKey, direction comparisonDirection) int {
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

func (c *indexCtx) isSpanValid(offset int, sp *LogicalSpan) bool {
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
func (c *indexCtx) checkSpans(offset int, ls LogicalSpans) {
	for i := range ls {
		if !c.isSpanValid(offset, &ls[i]) {
			panic(fmt.Sprintf("invalid span %s", ls[i]))
		}
		if i > 0 {
			cmp := c.compareKeyVals(offset, ls[i-1].End.Vals, ls[i].Start.Vals)
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

// nextDatum returns the datum that follows d, in the given direction.
func (c *indexCtx) nextDatum(d tree.Datum, direction encoding.Direction) (tree.Datum, bool) {
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
func (c *indexCtx) preferInclusive(offset int, sp *LogicalSpan) {
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
func (c *indexCtx) intersectSpan(
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
func (c *indexCtx) intersectSpanSet(offset int, a *LogicalSpan, spanSet LogicalSpans) LogicalSpans {
	res := make(LogicalSpans, 0, len(spanSet))
	for i := range spanSet {
		if sp, ok := c.intersectSpan(offset, a, &spanSet[i]); ok {
			res = append(res, sp)
		}
	}
	c.checkSpans(offset, res)
	return res
}
