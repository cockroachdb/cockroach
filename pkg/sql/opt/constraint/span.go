// Copyright 2018 The Cockroach Authors.
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

package constraint

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// SpanBoundary specifies whether a span endpoint is inclusive or exclusive of
// its start or end key. An inclusive boundary is represented as '[' and an
// exclusive boundary is represented as ')'. Examples:
//   [/0 - /1]  (inclusive, inclusive)
//   [/1 - /10) (inclusive, exclusive)
type SpanBoundary bool

const (
	// ExcludeBoundary indicates that the boundary does not include the start
	// or end key.
	ExcludeBoundary SpanBoundary = false

	// IncludeBoundary indicates that the boundary does include the start or
	// end key.
	IncludeBoundary SpanBoundary = true
)

// EmptySpan is a zero length span of which every other span is a superset. It
// is represented as [Ø - Ø)
var EmptySpan = Span{}

// FullSpan is an unconstrained span that is a superset of every other span. It
// is represented as [Ø - ∞]
var FullSpan = Span{endExt: ExtendInf}

// Span represents the range between two composite keys. The end keys of the
// range can be inclusive or exclusive. Each key value within the range is
// an N-tuple of datum values, one for each constrained column. Here are some
// examples:
//   @1 < 100                                          : [Ø - /100)
//   @1 >= 100                                         : [/100 - ∞]
//   @1 >= 1 AND @1 <= 10                              : [/1 - /10]
//   (@1 = 100 AND @2 > 10) OR (@1 > 100 AND @1 <= 101): (/100/10 - /101]
type Span struct {
	// Start is the starting boundary for the span.
	start Key

	// End is the ending boundary for the span.
	end Key

	// startExt is the key extension value (empty or infinity) that is used
	// when comparing the start key to other keys. See the comment for the
	// Key.Compare method for more details.
	startExt KeyExtension

	// endExt is the key extension value (empty or infinity) that is used when
	// comparing the end key to other keys. See the comment for the Key.Compare
	// method for more details.
	endExt KeyExtension
}

// TrySet sets the boundaries of this span to the given values. If the start
// boundary is >= the end boundary, TrySet returns false and does not make any
// update.
func (sp *Span) TrySet(
	evalCtx *tree.EvalContext,
	start Key,
	startBoundary SpanBoundary,
	end Key,
	endBoundary SpanBoundary,
) bool {
	var startExt, endExt KeyExtension

	// Inclusive start effectively extends the key with an empty value,
	// exclusive with an infinite value:
	//   [/1  =  /1/Ø
	//   (/1  =  /1/∞
	if startBoundary == IncludeBoundary {
		startExt = ExtendEmpty
	} else {
		startExt = ExtendInf
	}

	// Inclusive end effectively extends the key with an infinite value,
	// exclusive with an empty value:
	//   /1]  =  /1/∞
	//   /1)  =  /1/Ø
	if endBoundary == IncludeBoundary {
		endExt = ExtendInf
	} else {
		endExt = ExtendEmpty
	}

	// Check boundaries.
	cmp := start.Compare(evalCtx, end, startExt, endExt)
	if cmp > 0 {
		// Start boundary is greater than end boundary, so return false.
		return false
	}
	if cmp == 0 && startExt == endExt {
		// Empty span, so return false.
		return false
	}

	// Make updates now that boundaries are checked.
	sp.start = start
	sp.end = end
	sp.startExt = startExt
	sp.endExt = endExt
	return true
}

// IsEmpty is true if this is the empty span.
func (sp *Span) IsEmpty() bool {
	return sp.start.IsEmpty() && sp.end.IsEmpty() && sp.endExt == ExtendEmpty
}

// IsFull is true if this is the full span.
func (sp *Span) IsFull() bool {
	return sp.start.IsEmpty() && sp.end.IsEmpty() && sp.endExt == ExtendInf
}

// Compare returns an integer indicating the ordering of the two spans. The
// result will be 0 if the spans are equal, -1 if this span is less than the
// given span, or 1 if this span is greater. Spans are first compared based on
// their start boundaries. If those are equal, then their end boundaries are
// compared. An inclusive start boundary is less than an exclusive start
// boundary, and an exclusive end boundary is less than an inclusive end
// boundary. Here are examples of how various spans are ordered, with
// equivalent extended keys shown as well:
//   [/1   - /1)    =  /1/Ø   - /1/Ø
//   [/1   - /1/2)  =  /1/Ø   - /1/2/Ø
//   [/1   - /1/2]  =  /1/Ø   - /1/2/∞
//   [/1   - /1]    =  /1/Ø   - /1/∞
//   [/1   - /2)    =  /1/Ø   - /2/Ø
//   [/1/2 - /2)    =  /1/2/Ø - /2/Ø
//   (/1/2 - /2)    =  /1/2/∞ - /2/Ø
//   (/1   - /2)    =  /1/∞   - /2/Ø
//   (/1   - /9)    =  /1/∞   - /9/Ø
//   [/2   - /9]    =  /2/Ø   - /9/∞
func (sp *Span) Compare(evalCtx *tree.EvalContext, other *Span) int {
	// Span with lowest start boundary is less than the other.
	if cmp := sp.CompareStarts(evalCtx, other); cmp != 0 {
		return cmp
	}

	// Start boundary is same, so span with lowest end boundary is less than
	// the other.
	if cmp := sp.CompareEnds(evalCtx, other); cmp != 0 {
		return cmp
	}

	// End boundary is same as well, so spans are the same.
	return 0
}

// CompareStarts returns an integer indicating the ordering of the start
// boundaries of the two spans. The result will be 0 if the spans have the same
// start boundary, -1 if this span has a smaller start boundary than the given
// span, or 1 if this span has a bigger start boundary than the given span.
func (sp *Span) CompareStarts(evalCtx *tree.EvalContext, other *Span) int {
	return sp.start.Compare(evalCtx, other.start, sp.startExt, other.startExt)
}

// CompareEnds returns an integer indicating the ordering of the end boundaries
// of the two spans. The result will be 0 if the spans have the same end
// boundary, -1 if this span has a smaller end boundary than the given span, or
// 1 if this span has a bigger end boundary than the given span.
func (sp *Span) CompareEnds(evalCtx *tree.EvalContext, other *Span) int {
	return sp.end.Compare(evalCtx, other.end, sp.endExt, other.endExt)
}

// StartsAfter returns true if this span is greater than the given span and
// does not overlap it. In other words, this span's start boundary is greater
// or equal to the given span's end boundary.
func (sp *Span) StartsAfter(evalCtx *tree.EvalContext, other *Span) bool {
	return sp.start.Compare(evalCtx, other.end, sp.startExt, other.endExt) >= 0
}

// TryIntersectWith finds the overlap between this span and the given span.
// This span is updated to only cover the range that is common to both spans.
// If there is no overlap, then this span will not be updated, and
// TryIntersectWith will return false.
func (sp *Span) TryIntersectWith(evalCtx *tree.EvalContext, other *Span) bool {
	cmpStarts := sp.CompareStarts(evalCtx, other)
	if cmpStarts > 0 {
		// If this span's start boundary is >= the other span's end boundary,
		// then intersection is empty.
		if sp.start.Compare(evalCtx, other.end, sp.startExt, other.endExt) >= 0 {
			return false
		}
	}

	cmpEnds := sp.CompareEnds(evalCtx, other)
	if cmpEnds < 0 {
		// If this span's end boundary is <= the other span's start boundary,
		// then intersection is empty.
		if sp.end.Compare(evalCtx, other.start, sp.endExt, other.startExt) <= 0 {
			return false
		}
	}

	// Only update now that it's known that intersection is not empty.
	if cmpStarts < 0 {
		sp.start = other.start
		sp.startExt = other.startExt
	}
	if cmpEnds > 0 {
		sp.end = other.end
		sp.endExt = other.endExt
	}
	return true
}

// TryUnionWith attempts to merge this span with the given span. If the merged
// spans cannot be expressed as a single span, then TryUnionWith will not
// update the span and TryUnionWith returns false. This could occur if the
// spans are disjoint, for example:
//   [1-5] UNION [10-15]
//
// Otherwise, this span is updated to the merged span range and TryUnionWith
// returns true.
func (sp *Span) TryUnionWith(evalCtx *tree.EvalContext, other *Span) bool {
	// Determine the minimum start boundary.
	cmpStartKeys := sp.CompareStarts(evalCtx, other)

	var cmp int
	if cmpStartKeys <= 0 {
		// This span is less, so see if there's any "space" after it and before
		// the start of the other span.
		cmp = sp.end.Compare(evalCtx, other.start, sp.endExt, other.startExt)
	} else {
		// This span is less, so see if there's any "space" after the end of
		// the other span and the start of this span.
		cmp = other.end.Compare(evalCtx, sp.start, other.endExt, sp.startExt)
	}
	if cmp < 0 {
		// There's "space" between spans, so union of these spans can't be
		// expressed as a single span.
		return false
	}

	// Determine the maximum end boundary.
	cmpEndKeys := sp.CompareEnds(evalCtx, other)

	// Create the merged span.
	if cmpStartKeys > 0 {
		sp.start = other.start
		sp.startExt = other.startExt
	}
	if cmpEndKeys < 0 {
		sp.end = other.end
		sp.endExt = other.endExt
	}
	return true
}

// String formats a Span. Inclusivity/exclusivity is shown using
// brackets/parens. Some examples:
//   [1 - 2]
//   (1/1 - 2)
//   [Ø - 5/6)
//   [1 - ∞]
//   [Ø - ∞]
func (sp Span) String() string {
	var buf bytes.Buffer
	if sp.startExt == ExtendEmpty {
		buf.WriteRune('[')
	} else {
		buf.WriteRune('(')
	}

	if sp.start.IsEmpty() {
		if sp.startExt == ExtendEmpty {
			buf.WriteRune('Ø')
		} else {
			panic("span cannot have exclusive empty start key")
		}
	} else {
		buf.WriteString(sp.start.String())
	}

	buf.WriteString(" - ")

	if sp.end.IsEmpty() {
		if sp.endExt == ExtendEmpty {
			buf.WriteRune('Ø')
		} else {
			buf.WriteRune('∞')
		}
	} else {
		buf.WriteString(sp.end.String())
	}

	if sp.endExt == ExtendEmpty {
		buf.WriteRune(')')
	} else {
		buf.WriteRune(']')
	}

	return buf.String()
}
