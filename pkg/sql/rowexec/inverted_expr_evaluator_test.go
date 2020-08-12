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
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func setToString(set []KeyIndex) string {
	var b strings.Builder
	for i, elem := range set {
		sep := ","
		if i == len(set)-1 {
			sep = ""
		}
		fmt.Fprintf(&b, "%d%s", elem, sep)
	}
	return b.String()
}

func writeSpan(b *strings.Builder, span invertedSpan) {
	fmt.Fprintf(b, "[%s, %s) ", span.Start, span.End)
}

func spansToString(spans []invertedSpan) string {
	var b strings.Builder
	for _, elem := range spans {
		writeSpan(&b, elem)
	}
	return b.String()
}

func spansIndexToString(spans []spansAndSetIndex) string {
	var b strings.Builder
	for _, elem := range spans {
		b.WriteString("spans: ")
		for _, s := range elem.spans {
			writeSpan(&b, s)
		}
		fmt.Fprintf(&b, " setIndex: %d\n", elem.setIndex)
	}
	return b.String()
}

func fragmentedSpansToString(spans []invertedSpanRoutingInfo) string {
	var b strings.Builder
	for _, elem := range spans {
		b.WriteString("span: ")
		writeSpan(&b, elem.span)
		b.WriteString(" indexes (expr, set): ")
		for _, indexes := range elem.exprAndSetIndexList {
			fmt.Fprintf(&b, "(%d, %d) ", indexes.exprIndex, indexes.setIndex)
		}
		b.WriteString("\n")
	}
	return b.String()
}

func keyIndexesToString(indexes [][]KeyIndex) string {
	var b strings.Builder
	for i, elem := range indexes {
		fmt.Fprintf(&b, "%d: ", i)
		for _, index := range elem {
			fmt.Fprintf(&b, "%d ", index)
		}
		b.WriteString("\n")
	}
	return b.String()
}

func TestSetContainerUnion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testCase struct {
		a        setContainer
		b        setContainer
		expected setContainer
	}
	cases := []testCase{
		{a: nil, b: nil, expected: nil},
		{a: []KeyIndex{5}, b: nil, expected: []KeyIndex{5}},
		{a: []KeyIndex{5}, b: []KeyIndex{2, 12}, expected: []KeyIndex{2, 5, 12}},
		{a: []KeyIndex{2, 5}, b: []KeyIndex{12}, expected: []KeyIndex{2, 5, 12}},
		{a: []KeyIndex{2}, b: []KeyIndex{5, 12}, expected: []KeyIndex{2, 5, 12}},
		{a: []KeyIndex{2, 12}, b: []KeyIndex{2, 5}, expected: []KeyIndex{2, 5, 12}},
		{a: []KeyIndex{2, 5}, b: []KeyIndex{5, 12}, expected: []KeyIndex{2, 5, 12}},
	}
	for _, c := range cases {
		require.Equal(t, setToString(c.expected), setToString(unionSetContainers(c.a, c.b)))
	}
}

func TestSetContainerIntersection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testCase struct {
		a        setContainer
		b        setContainer
		expected setContainer
	}
	cases := []testCase{
		{a: nil, b: nil, expected: nil},
		{a: []KeyIndex{5}, b: nil, expected: nil},
		{a: []KeyIndex{5}, b: []KeyIndex{2, 12}, expected: nil},
		{a: []KeyIndex{2, 12}, b: []KeyIndex{2, 5}, expected: []KeyIndex{2}},
		{a: []KeyIndex{2, 5}, b: []KeyIndex{5, 12}, expected: []KeyIndex{5}},
		{a: []KeyIndex{2, 5, 17, 25, 30}, b: []KeyIndex{2, 12, 13, 17, 23, 30},
			expected: []KeyIndex{2, 17, 30}},
	}
	for _, c := range cases {
		require.Equal(t, setToString(c.expected), setToString(intersectSetContainers(c.a, c.b)))
	}
}

type keyAndIndex struct {
	key   string
	index int
}

// Tests both invertedExprEvaluator and batchedInvertedExprEvaluator.
func TestInvertedExpressionEvaluator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	leaf1 := &spanExpression{
		FactoredUnionSpans: []invertedSpan{{Start: []byte("a"), End: []byte("d")}},
		Operator:           invertedexpr.None,
	}
	leaf2 := &spanExpression{
		FactoredUnionSpans: []invertedSpan{{Start: []byte("e"), End: []byte("h")}},
		Operator:           invertedexpr.None,
	}
	l1Andl2 := &spanExpression{
		FactoredUnionSpans: []invertedSpan{
			{Start: []byte("i"), End: []byte("j")}, {Start: []byte("k"), End: []byte("n")}},
		Operator: invertedexpr.SetIntersection,
		Left:     leaf1,
		Right:    leaf2,
	}
	leaf3 := &spanExpression{
		FactoredUnionSpans: []invertedSpan{{Start: []byte("d"), End: []byte("f")}},
		Operator:           invertedexpr.None,
	}
	leaf4 := &spanExpression{
		FactoredUnionSpans: []invertedSpan{{Start: []byte("a"), End: []byte("c")}},
		Operator:           invertedexpr.None,
	}
	l3Andl4 := &spanExpression{
		FactoredUnionSpans: []invertedSpan{
			{Start: []byte("g"), End: []byte("m")}},
		Operator: invertedexpr.SetIntersection,
		Left:     leaf3,
		Right:    leaf4,
	}
	// In reality, the FactoredUnionSpans of l1Andl2 and l3Andl4 would be moved
	// up to expr, by the factoring code in the invertedexpr package. But the
	// evaluator does not care, and keeping them separate exercises more code.
	exprUnion := &spanExpression{
		Operator: invertedexpr.SetUnion,
		Left:     l1Andl2,
		Right:    l3Andl4,
	}

	exprIntersection := &spanExpression{
		Operator: invertedexpr.SetIntersection,
		Left:     l1Andl2,
		Right:    l3Andl4,
	}

	expectedSpansAndSetIndex := "spans: [i, j) [k, n)  setIndex: 1\nspans: [a, d)  setIndex: 2\n" +
		"spans: [e, h)  setIndex: 3\nspans: [g, m)  setIndex: 4\nspans: [d, f)  setIndex: 5\n" +
		"spans: [a, c)  setIndex: 6\n"

	// Test the getSpansAndSetIndex() method on the invertedExprEvaluator
	// directly. The rest of the methods we will only exercise through
	// batchedInvertedExprEvaluator.
	evalUnion := newInvertedExprEvaluator(exprUnion)
	// Indexes are being assigned using a pre-order traversal.
	require.Equal(t, expectedSpansAndSetIndex,
		spansIndexToString(evalUnion.getSpansAndSetIndex()))

	evalIntersection := newInvertedExprEvaluator(exprIntersection)
	require.Equal(t, expectedSpansAndSetIndex,
		spansIndexToString(evalIntersection.getSpansAndSetIndex()))

	// The batchedInvertedExprEvaluators will construct their own
	// invertedExprEvaluators.
	protoUnion := invertedexpr.SpanExpressionProto{Node: *exprUnion}
	batchEvalUnion := &batchedInvertedExprEvaluator{
		exprs: []*invertedexpr.SpanExpressionProto{&protoUnion, nil},
	}
	protoIntersection := invertedexpr.SpanExpressionProto{Node: *exprIntersection}
	batchEvalIntersection := &batchedInvertedExprEvaluator{
		exprs: []*invertedexpr.SpanExpressionProto{&protoIntersection, nil},
	}
	expectedSpans := "[a, n) "
	expectedFragmentedSpans :=
		"span: [a, c)  indexes (expr, set): (0, 6) (0, 2) \n" +
			"span: [c, d)  indexes (expr, set): (0, 2) \n" +
			"span: [d, e)  indexes (expr, set): (0, 5) \n" +
			"span: [e, f)  indexes (expr, set): (0, 5) (0, 3) \n" +
			"span: [f, g)  indexes (expr, set): (0, 3) \n" +
			"span: [g, h)  indexes (expr, set): (0, 3) (0, 4) \n" +
			"span: [h, i)  indexes (expr, set): (0, 4) \n" +
			"span: [i, j)  indexes (expr, set): (0, 1) (0, 4) \n" +
			"span: [j, k)  indexes (expr, set): (0, 4) \n" +
			"span: [k, m)  indexes (expr, set): (0, 4) (0, 1) \n" +
			"span: [m, n)  indexes (expr, set): (0, 1) \n"

	require.Equal(t, expectedSpans, spansToString(batchEvalUnion.init()))
	require.Equal(t, expectedFragmentedSpans,
		fragmentedSpansToString(batchEvalUnion.fragmentedSpans))
	require.Equal(t, expectedSpans, spansToString(batchEvalIntersection.init()))
	require.Equal(t, expectedFragmentedSpans,
		fragmentedSpansToString(batchEvalIntersection.fragmentedSpans))

	// Construct the test input. This is hand-constructed to exercise cases where
	// intersections sometimes propagate an index across multiple levels.
	// This input is not in order of the inverted key since the evaluator does
	// not care. In fact, we randomize the input order to exercise the code.
	indexRows := []keyAndIndex{{"ii", 0}, {"a", 1}, {"ee", 2},
		{"aa", 3}, {"ab", 4}, {"g", 4}, {"d", 3},
		{"mn", 5}, {"mp", 6}, {"gh", 6}, {"gi", 7},
		{"aaa", 8}, {"e", 8}, {"aaa", 1}}
	// These expected values were hand-constructed by evaluating the expression over
	// these inputs.
	// (([a, d) ∩ [e, h)) U [i, j) U [k, n)) U
	// (([d, f) ∩ [a, c)) U [g, m))
	expectedUnion := "0: 0 3 4 5 6 7 8 \n1: \n"
	// (([a, d) ∩ [e, h)) U [i, j) U [k, n)) ∩
	// (([d, f) ∩ [a, c)) U [g, m))
	expectedIntersection := "0: 0 4 6 8 \n1: \n"
	rand.Shuffle(len(indexRows), func(i, j int) {
		indexRows[i], indexRows[j] = indexRows[j], indexRows[i]
	})
	for _, elem := range indexRows {
		batchEvalUnion.addIndexRow(invertedexpr.EncInvertedVal(elem.key), elem.index)
		batchEvalIntersection.addIndexRow(invertedexpr.EncInvertedVal(elem.key), elem.index)
	}
	require.Equal(t, expectedUnion, keyIndexesToString(batchEvalUnion.evaluate()))
	require.Equal(t, expectedIntersection, keyIndexesToString(batchEvalIntersection.evaluate()))

	// Now do both exprUnion and exprIntersection in a single batch.
	batchBoth := batchEvalUnion
	batchBoth.reset()
	batchBoth.exprs = append(batchBoth.exprs, &protoUnion, &protoIntersection)
	batchBoth.init()
	for _, elem := range indexRows {
		batchBoth.addIndexRow(invertedexpr.EncInvertedVal(elem.key), elem.index)
	}
	require.Equal(t, "0: 0 3 4 5 6 7 8 \n1: 0 4 6 8 \n",
		keyIndexesToString(batchBoth.evaluate()))

	// Reset and evaluate nil expressions.
	batchBoth.reset()
	batchBoth.exprs = append(batchBoth.exprs, nil, nil)
	require.Equal(t, 0, len(batchBoth.init()))
	require.Equal(t, "0: \n1: \n", keyIndexesToString(batchBoth.evaluate()))
}

// Test fragmentation for routing when multiple expressions in the batch have
// overlapping spans.
func TestFragmentedSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	expr1 := invertedexpr.SpanExpressionProto{
		Node: spanExpression{
			FactoredUnionSpans: []invertedSpan{{Start: []byte("a"), End: []byte("g")}},
			Operator:           invertedexpr.None,
		},
	}
	expr2 := invertedexpr.SpanExpressionProto{
		Node: spanExpression{
			FactoredUnionSpans: []invertedSpan{{Start: []byte("d"), End: []byte("j")}},
			Operator:           invertedexpr.None,
		},
	}
	expr3 := invertedexpr.SpanExpressionProto{
		Node: spanExpression{
			FactoredUnionSpans: []invertedSpan{
				{Start: []byte("e"), End: []byte("f")}, {Start: []byte("i"), End: []byte("l")},
				{Start: []byte("o"), End: []byte("p")}},
			Operator: invertedexpr.None,
		},
	}
	batchEval := &batchedInvertedExprEvaluator{
		exprs: []*invertedexpr.SpanExpressionProto{&expr1, &expr2, &expr3},
	}
	require.Equal(t, "[a, l) [o, p) ", spansToString(batchEval.init()))
	require.Equal(t,
		"span: [a, d)  indexes (expr, set): (0, 0) \n"+
			"span: [d, e)  indexes (expr, set): (0, 0) (1, 0) \n"+
			"span: [e, f)  indexes (expr, set): (2, 0) (0, 0) (1, 0) \n"+
			"span: [f, g)  indexes (expr, set): (0, 0) (1, 0) \n"+
			"span: [g, i)  indexes (expr, set): (1, 0) \n"+
			"span: [i, j)  indexes (expr, set): (1, 0) (2, 0) \n"+
			"span: [j, l)  indexes (expr, set): (2, 0) \n"+
			"span: [o, p)  indexes (expr, set): (2, 0) \n",
		fragmentedSpansToString(batchEval.fragmentedSpans))
}

// TODO(sumeer): randomized inputs for union, intersection and expression evaluation.
