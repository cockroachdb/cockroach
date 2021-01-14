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
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/stretchr/testify/require"
)

// spanExprForTest is used to easily create SpanExpressions
// for testing purposes.
type spanExprForTest struct {
	unionSpans [][]string
	children   []spanExprForTest
	operator   SetOperator
}

// makeSpanExpression converts a spanExprForTest to a SpanExpression.
func (expr spanExprForTest) makeSpanExpression() *SpanExpression {
	spanExpr := &SpanExpression{
		UnionSpans: make(roachpb.Spans, len(expr.unionSpans)),
		Children:   make([]*SpanExpression, len(expr.children)),
		Operator:   expr.operator,
	}

	for i := range expr.unionSpans {
		spanExpr.UnionSpans[i] = roachpb.Span{
			Key:    roachpb.Key(expr.unionSpans[i][0]),
			EndKey: roachpb.Key(expr.unionSpans[i][1]),
		}
	}

	for i := range expr.children {
		spanExpr.Children[i] = expr.children[i].makeSpanExpression()
	}

	return spanExpr
}

// permute randomly changes the order of the nodes in the span expression tree.
func (expr spanExprForTest) permute() {
	// Recursively permute the children.
	for i := range expr.children {
		expr.children[i].permute()
	}

	// Add a random permutation of the union spans.
	rand.Shuffle(len(expr.unionSpans), func(i, j int) {
		expr.unionSpans[i], expr.unionSpans[j] = expr.unionSpans[j], expr.unionSpans[i]
	})

	// Add a random permutation of the children.
	rand.Shuffle(len(expr.children), func(i, j int) {
		expr.children[i], expr.children[j] = expr.children[j], expr.children[i]
	})
}

func TestContainsKeys(t *testing.T) {
	tests := []struct {
		input    spanExprForTest
		keys     []string
		expected bool
	}{
		{
			// The start key of a span is inclusive.
			input:    spanExprForTest{unionSpans: [][]string{{"a", "b"}}},
			keys:     []string{"a"},
			expected: true,
		},
		{
			// The end key of a span is exclusive.
			input:    spanExprForTest{unionSpans: [][]string{{"a", "b"}}},
			keys:     []string{"b"},
			expected: false,
		},
		{
			// At least one of the keys is contained in the span.
			input:    spanExprForTest{unionSpans: [][]string{{"a", "c"}}},
			keys:     []string{"b", "c"},
			expected: true,
		},
		{
			// Key falls between the spans.
			input:    spanExprForTest{unionSpans: [][]string{{"a", "b"}, {"d", "e"}}},
			keys:     []string{"c"},
			expected: false,
		},
		{
			// Key is contained in one of the spans.
			input:    spanExprForTest{unionSpans: [][]string{{"a", "b"}, {"d", "e"}}},
			keys:     []string{"dog"},
			expected: true,
		},
		{
			// Key is contained in one of the spans.
			input: spanExprForTest{operator: SetUnion,
				children: []spanExprForTest{
					{unionSpans: [][]string{{"a", "b"}}},
					{unionSpans: [][]string{{"d", "e"}}},
				},
			},
			keys:     []string{"dog"},
			expected: true,
		},
		{
			// Key is only contained in one of the spans, but intersection requires
			// both.
			input: spanExprForTest{operator: SetIntersection,
				children: []spanExprForTest{
					{unionSpans: [][]string{{"a", "b"}}},
					{unionSpans: [][]string{{"d", "e"}}},
				},
			},
			keys:     []string{"dog"},
			expected: false,
		},
		{
			// At least one key is contained in each span.
			input: spanExprForTest{operator: SetIntersection,
				children: []spanExprForTest{
					{unionSpans: [][]string{{"a", "b"}}},
					{unionSpans: [][]string{{"d", "e"}}},
				},
			},
			keys:     []string{"dog", "apple"},
			expected: true,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {

			// Add random permutations to the input.
			tt.input.permute()

			// Build span expressions from the test case.
			input := tt.input.makeSpanExpression()

			// Build keys from the test case.
			keys := make([][]byte, len(tt.keys))
			for i := range tt.keys {
				keys[i] = encoding.UnsafeConvertStringToBytes(tt.keys[i])
			}

			actual, err := input.ContainsKeys(keys)
			require.NoError(t, err)
			if actual != tt.expected {
				t.Errorf("for ContainsKeys() got %v, expected %v", actual, tt.expected)
			}
		})
	}
}

func TestSortAndUniquifySpans(t *testing.T) {
	tests := []struct {
		input    spanExprForTest
		expected spanExprForTest
	}{
		// Note: even when there are children, we don't bother setting the operator
		// since it's not needed for this test.
		{
			input:    spanExprForTest{},
			expected: spanExprForTest{},
		},
		{
			// We only sort UnionSpans, we don't deduplicate them, as there is
			// currently not a use case that requires it.
			input:    spanExprForTest{unionSpans: [][]string{{"a", "b"}, {"c", "d"}, {"a", "b"}}},
			expected: spanExprForTest{unionSpans: [][]string{{"a", "b"}, {"a", "b"}, {"c", "d"}}},
		},
		{
			input:    spanExprForTest{unionSpans: [][]string{{"a", "b"}}},
			expected: spanExprForTest{unionSpans: [][]string{{"a", "b"}}},
		},
		{
			input: spanExprForTest{children: []spanExprForTest{
				{unionSpans: [][]string{{"a", "b"}}},
				{unionSpans: [][]string{{"a", "b"}}},
			}},
			expected: spanExprForTest{children: []spanExprForTest{
				{unionSpans: [][]string{{"a", "b"}}},
			}},
		},
		{
			input: spanExprForTest{children: []spanExprForTest{
				{unionSpans: [][]string{{"a", "b"}}},
				{unionSpans: [][]string{{"a", "b"}, {"c", "d"}}},
				{unionSpans: [][]string{{"a", "b"}}},
			}},
			expected: spanExprForTest{children: []spanExprForTest{
				{unionSpans: [][]string{{"a", "b"}}},
				{unionSpans: [][]string{{"a", "b"}, {"c", "d"}}},
			}},
		},
		{
			input: spanExprForTest{children: []spanExprForTest{
				{unionSpans: [][]string{{"a", "b"}, {"c", "d"}}},
				{unionSpans: [][]string{{"a", "b"}}},
			}},
			expected: spanExprForTest{children: []spanExprForTest{
				{unionSpans: [][]string{{"a", "b"}}},
				{unionSpans: [][]string{{"a", "b"}, {"c", "d"}}},
			}},
		},
		{
			input: spanExprForTest{children: []spanExprForTest{
				{unionSpans: [][]string{{"bar", "foo"}}},
				{unionSpans: [][]string{{"bar", "foo"}}},
				{unionSpans: [][]string{{"foobar", "foobaz"}}},
			}},
			expected: spanExprForTest{children: []spanExprForTest{
				{unionSpans: [][]string{{"bar", "foo"}}},
				{unionSpans: [][]string{{"foobar", "foobaz"}}},
			}},
		},
		{
			input: spanExprForTest{children: []spanExprForTest{
				{children: []spanExprForTest{{unionSpans: [][]string{{"bar", "foo"}}}}},
				{unionSpans: [][]string{{"bar", "foo"}}},
				{children: []spanExprForTest{
					{unionSpans: [][]string{{"bar", "foo"}}},
					{unionSpans: [][]string{{"bar", "foo"}}},
				}},
				{unionSpans: [][]string{{"foobar", "foobaz"}}},
			}},
			expected: spanExprForTest{children: []spanExprForTest{
				{children: []spanExprForTest{{unionSpans: [][]string{{"bar", "foo"}}}}},
				{children: []spanExprForTest{{unionSpans: [][]string{{"bar", "foo"}}}}},
				{unionSpans: [][]string{{"bar", "foo"}}},
				{unionSpans: [][]string{{"foobar", "foobaz"}}},
			}},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {

			// Add random permutations to the input.
			tt.input.permute()

			// Build span expressions from the test case.
			input := tt.input.makeSpanExpression()
			expected := tt.expected.makeSpanExpression()

			input.SortAndUniquifySpans()
			if got := input; !reflect.DeepEqual(got, expected) {
				t.Errorf("for SortAndUniquifySpans() got %v, expected %v", got, expected)
			}
		})
	}
}
