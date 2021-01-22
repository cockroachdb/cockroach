// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package inverted

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// SpanExpression forms a set expression tree that represents unions and
// intersections over spans read from an inverted index.
type SpanExpression2 struct {
	// Tight is true if the SpanExpression produces no false positives. In other
	// words, if the SpanExpression is Tight, the original JSON, Array, or spatial
	// expression will not need to be reevaluated on each row output by the query
	// evaluation over the inverted index.
	Tight bool

	// Unique is true if the spans are guaranteed not to produce duplicate primary
	// keys. Unique may be true for certain JSON or Array SpanExpressions, and it
	// holds when SpanExpressions are intersected. It does not hold when these
	// SpanExpressions are unioned. If Unique is true and there are no children
	// (i.e., the SpanExpression represents a UNION over UnionSpans), that allows
	// the optimizer to remove the UNION altogether (implemented with the
	// invertedFilterer), and simply return the results of the constrained scan.
	Unique bool

	// UnionSpans are the spans to read from the inverted index. They will
	// be unioned to evaluate this SpanExpression. These are non-overlapping, but
	// not guaranteed to be sorted. If Children is non-nil, UnionSpans must be nil
	// and vice-versa.
	UnionSpans roachpb.Spans

	// Operator is the set operation to apply to the Children (either UNION or
	// INTERSECTION).
	Operator SetOperator
	Children []*SpanExpression2
}

// MakeIntersection makes a new span expression that is an intersection of the
// provided children. The values of Tight and Unique are determined based on the
// values in the children.
func MakeIntersection(children []*SpanExpression2) *SpanExpression2 {
	tight, unique := true, true
	for i := range children {
		tight = tight && children[i].Tight
		unique = unique && children[i].Unique
	}
	return &SpanExpression2{
		Operator: SetIntersection,
		Tight:    tight,
		Unique:   unique,
		Children: children,
	}
}

// ContainsKeys traverses the SpanExpression to determine whether the span
// expression contains the given keys. It is primarily used for testing.
func (s *SpanExpression2) ContainsKeys(keys [][]byte) (bool, error) {
	// First check that the span expression is valid. Only leaf nodes in a
	// SpanExpression tree are allowed to have UnionSpans set.
	if len(s.UnionSpans) > 0 && len(s.Children) > 0 {
		return false, errors.AssertionFailedf(
			"invalid SpanExpression: cannot be both a leaf and internal node",
		)
	}
	if len(s.Children) == 0 && len(s.UnionSpans) == 0 {
		return false, nil
	}

	// UnionSpans represents a union over the spans, so any span in the slice
	// can contain any of the keys.
	if len(s.UnionSpans) > 0 {
		for _, span := range s.UnionSpans {
			for _, key := range keys {
				if span.ContainsKey(key) {
					return true, nil
				}
			}
		}
		return false, nil
	}

	// This is either a UNION or INTERSECTION over the children.
	res := true
	for _, child := range s.Children {
		childRes, err := child.ContainsKeys(keys)
		if err != nil {
			return false, err
		}
		switch s.Operator {
		case SetIntersection:
			res = res && childRes
		case SetUnion:
			res = res || childRes
		default:
			return false, errors.AssertionFailedf("invalid operator %v", child.Operator)
		}
	}
	return res, nil
}

// SortAndUniquifySpanSets deduplicates the leaf nodes in a SpanExpression tree
// using a sort and unique. It modifies the slices in place. The resulting tree
// will not contain any duplicate leaf nodes that have the same parent, and the
// nodes will be sorted according to the following logic:
// - Internal nodes with children will be ordered before those without children,
//   but no specific order is maintained among the nodes with children.
// - Nodes without children are ordered based on the spans in UnionSpans. Node a
//   will be ordered before node b if the first span in a.UnionSpans that is not
//   equal to the corresponding span in b.UnionSpans (i.e., at the same position)
//   is less than it (according to Span.Compare) or all corresponding spans are
//   equal but there are fewer spans in a.UnionSpans.
// - Each node's UnionSpans will itself be sorted using Span.Compare.
func (s *SpanExpression2) SortAndUniquifySpans() {
	// First sort the union spans.
	sort.Slice(s.UnionSpans, func(i int, j int) bool {
		return s.UnionSpans[i].Compare(s.UnionSpans[j]) < 0
	})

	if len(s.Children) == 0 {
		return
	}

	// Recursively sort each child individually.
	for i := range s.Children {
		s.Children[i].SortAndUniquifySpans()
	}

	// Then sort all children.
	sort.Slice(s.Children, func(i int, j int) bool {
		return compare(s.Children[i], s.Children[j]) < 0
	})

	// Then distinct.
	lastUniqueIdx := 0
	for i := 1; i < len(s.Children); i++ {
		if compare(s.Children[i], s.Children[lastUniqueIdx]) != 0 {
			// We found a unique entry, at index i. The last unique entry in the array
			// was at lastUniqueIdx, so set the entry after that one to our new unique
			// entry, and bump lastUniqueIdx for the next loop iteration.
			lastUniqueIdx++
			s.Children[lastUniqueIdx] = s.Children[i]
		}
	}
	s.Children = s.Children[:lastUniqueIdx+1]
}

// compare returns an integer comparing two SpanExpressions.
// - The result will be -1 if a has children.
// - The result will be +1 if b has children but a does not.
// - If neither SpanExpression has children, the result depends on
//   the output of running compareSpans on the UnionSpans of each
//   SpanExpression.
func compare(a, b *SpanExpression2) int {
	if len(a.Children) > 0 {
		return -1
	}
	if len(b.Children) > 0 {
		return 1
	}
	return compareSpans(a.UnionSpans, b.UnionSpans)
}

// compareSpans returns an integer comparing two Spans lexicographically.
// - The result will be 0 if a==b.
// - The result will be -1 if the first span in a that is not equal to the
//   corresponding span in b (i.e., at the same position) is less than it
//   (according to Span.Compare) or all corresponding spans are equal but
//   there are fewer spans in a.
// - The result will be +1 otherwise.
// Assumes that each of the Spans are already sorted using Span.Compare.
func compareSpans(a, b roachpb.Spans) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		cmp := a[i].Compare(b[i])
		if cmp != 0 {
			return cmp
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(b) < len(a) {
		return 1
	}
	return 0
}
