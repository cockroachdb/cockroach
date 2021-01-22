// Copyright 2020 The Cockroach Authors.
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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
	"github.com/cockroachdb/datadriven"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

/*
Format for the datadriven test:

new-span-leaf name=<name> tight=<true|false> unique=<true|false> span=<start>[,<end>]
----
<SpanExpression as string>

  Creates a new leaf SpanExpression with the given name

new-unknown-leaf name=<name> tight=<true|false>
----

  Creates a new leaf unknownExpression with the given name

new-non-inverted-leaf name=<name>
----

  Creates a new NonInvertedColExpression with the given name

and result=<name> left=<name> right=<name>
----
<SpanExpression as string>

  Ands the left and right expressions and stores the result

or result=<name> left=<name> right=<name>
----
<SpanExpression as string>

  Ors the left and right expressions and stores the result

to-proto name=<name>
----
<SpanExpressionProto as string>

  Converts the SpanExpression to SpanExpressionProto
*/

func getSpan(t *testing.T, d *datadriven.TestData) Span {
	var str string
	d.ScanArgs(t, "span", &str)
	parts := strings.Split(str, ",")
	if len(parts) > 2 {
		d.Fatalf(t, "incorrect span format: %s", str)
		return Span{}
	} else if len(parts) == 2 {
		return Span{Start: []byte(parts[0]), End: []byte(parts[1])}
	} else {
		return MakeSingleValSpan([]byte(parts[0]))
	}
}

type UnknownExpression struct {
	tight bool
}

func (u UnknownExpression) IsTight() bool { return u.tight }
func (u UnknownExpression) SetNotTight()  { u.tight = false }
func (u UnknownExpression) String() string {
	return fmt.Sprintf("unknown expression: tight=%t", u.tight)
}
func (u UnknownExpression) Copy() Expression {
	return UnknownExpression{tight: u.tight}
}

// Makes a (shallow) copy of the root node of the expression identified
// by name, since calls to And() and Or() can modify that root node, and
// the test wants to preserve the unmodified expression for later use.
func getExprCopy(
	t *testing.T, d *datadriven.TestData, name string, exprsByName map[string]Expression,
) Expression {
	expr := exprsByName[name]
	if expr == nil {
		d.Fatalf(t, "unknown expr: %s", name)
	}
	switch e := expr.(type) {
	case *SpanExpression:
		return &SpanExpression{
			Tight:              e.Tight,
			Unique:             e.Unique,
			SpansToRead:        append([]Span(nil), e.SpansToRead...),
			FactoredUnionSpans: append([]Span(nil), e.FactoredUnionSpans...),
			Operator:           e.Operator,
			Left:               e.Left,
			Right:              e.Right,
		}
	case NonInvertedColExpression:
		return NonInvertedColExpression{}
	case UnknownExpression:
		return UnknownExpression{tight: e.tight}
	default:
		d.Fatalf(t, "unknown expr type")
		return nil
	}
}

func toString(expr Expression) string {
	tp := treeprinter.New()
	formatExpression(tp, expr, true /* includeSpansToRead */)
	return tp.String()
}

func getLeftAndRightExpr(
	t *testing.T, d *datadriven.TestData, exprsByName map[string]Expression,
) (Expression, Expression) {
	var leftName, rightName string
	d.ScanArgs(t, "left", &leftName)
	d.ScanArgs(t, "right", &rightName)
	return getExprCopy(t, d, leftName, exprsByName), getExprCopy(t, d, rightName, exprsByName)
}

func TestExpression(t *testing.T) {
	defer leaktest.AfterTest(t)()
	exprsByName := make(map[string]Expression)

	datadriven.RunTest(t, "testdata/expression", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "new-span-leaf":
			var name string
			d.ScanArgs(t, "name", &name)
			var tight, unique bool
			d.ScanArgs(t, "tight", &tight)
			d.ScanArgs(t, "unique", &unique)
			span := getSpan(t, d)
			expr := ExprForSpan(span, tight)
			expr.Unique = unique
			exprsByName[name] = expr
			return expr.String()
		case "new-unknown-leaf":
			var name string
			d.ScanArgs(t, "name", &name)
			var tight bool
			d.ScanArgs(t, "tight", &tight)
			expr := UnknownExpression{tight: tight}
			exprsByName[name] = expr
			return fmt.Sprintf("%v", expr)
		case "new-non-inverted-leaf":
			var name string
			d.ScanArgs(t, "name", &name)
			exprsByName[name] = NonInvertedColExpression{}
			return ""
		case "and":
			var name string
			d.ScanArgs(t, "result", &name)
			left, right := getLeftAndRightExpr(t, d, exprsByName)
			expr := And(left, right)
			exprsByName[name] = expr
			return toString(expr)
		case "or":
			var name string
			d.ScanArgs(t, "result", &name)
			left, right := getLeftAndRightExpr(t, d, exprsByName)
			expr := Or(left, right)
			exprsByName[name] = expr
			return toString(expr)
		case "to-proto":
			var name string
			d.ScanArgs(t, "name", &name)
			expr := exprsByName[name]
			if expr == nil {
				expr = (*SpanExpression)(nil)
			}
			return proto.MarshalTextString(expr.(*SpanExpression).ToProto())
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func span(start, end string) Span {
	return Span{Start: []byte(start), End: []byte(end)}
}

func single(start string) Span {
	return MakeSingleValSpan([]byte(start))
}

func checkEqual(t *testing.T, expected, actual []Span) {
	require.Equal(t, len(expected), len(actual))
	for i := range expected {
		require.Equal(t, expected[i].Start, actual[i].Start)
		require.Equal(t, expected[i].End, actual[i].End)
	}
}

func TestSetUnion(t *testing.T) {
	checkEqual(t,
		[]Span{span("b", "c")},
		unionSpans(
			[]Span{single("b")},
			[]Span{span("b", "c")},
		),
	)
	checkEqual(t,
		[]Span{span("b", "d")},
		unionSpans(
			[]Span{single("b")},
			[]Span{single("c")},
		),
	)
	checkEqual(t,
		[]Span{span("b", "d")},
		unionSpans(
			[]Span{single("b")},
			[]Span{span("c", "d")},
		),
	)
	checkEqual(t,
		[]Span{span("b", "d")},
		unionSpans(
			[]Span{span("b", "c")},
			[]Span{span("c", "d")},
		),
	)
	checkEqual(t,
		[]Span{span("b", "c"), single("d")},
		unionSpans(
			[]Span{span("b", "c")},
			[]Span{single("d")},
		),
	)
	checkEqual(t,
		[]Span{span("b", "c"), single("d"), single("f")},
		unionSpans(
			[]Span{span("b", "c"), single("f")},
			[]Span{single("d")},
		),
	)
	checkEqual(t,
		[]Span{span("b", "d"), single("e")},
		unionSpans(
			[]Span{span("b", "c"), single("e")},
			[]Span{span("c", "d")},
		),
	)
	checkEqual(t,
		[]Span{span("b", "f")},
		unionSpans(
			[]Span{span("b", "c"), single("e")},
			[]Span{span("c", "f")},
		),
	)
}

func TestSetIntersection(t *testing.T) {
	checkEqual(t,
		[]Span{single("b")},
		intersectSpans(
			[]Span{single("b")},
			[]Span{span("b", "c")},
		),
	)
	checkEqual(t,
		nil,
		intersectSpans(
			[]Span{single("b")},
			[]Span{span("c", "d")},
		),
	)
	checkEqual(t,
		[]Span{single("b"), span("d", "d\x00"), span("dd", "e"), span("f", "ff")},
		intersectSpans(
			[]Span{single("b"), span("d", "e"), span("f", "g")},
			[]Span{span("b", "d\x00"), span("dd", "ff")},
		),
	)
}

func TestSetSubtraction(t *testing.T) {
	checkEqual(t,
		nil,
		subtractSpans(
			[]Span{single("b")},
			[]Span{span("b", "c")},
		),
	)
	checkEqual(t,
		[]Span{span("b\x00", "d")},
		subtractSpans(
			[]Span{span("b", "d")},
			[]Span{span("b", "b\x00")},
		),
	)
	checkEqual(t,
		[]Span{span("b", "d"), span("e", "ea")},
		subtractSpans(
			[]Span{span("b", "d"), span("e", "f")},
			[]Span{span("ea", "f")},
		),
	)
	checkEqual(t,
		[]Span{span("d", "da"), span("db", "dc"),
			span("dd", "df"), span("fa", "g")},
		subtractSpans(
			[]Span{single("b"), span("d", "e"), span("f", "g")},
			[]Span{span("b", "c"), span("da", "db"),
				span("dc", "dd"), span("df", "e"), span("f", "fa")},
		),
	)

}

// spanExprForTest is used to easily create SpanExpressions
// for testing purposes.
type spanExprForTest struct {
	unionSpans [][]string
	children   []spanExprForTest
	operator   SetOperator
}

// makeSpanExpression converts a spanExprForTest to a SpanExpression.
func (expr spanExprForTest) makeSpanExpression() *SpanExpression {
	var invertedExpr Expression

	for i := range expr.unionSpans {
		spanExpr := ExprForSpan(Span{
			Start: EncVal(expr.unionSpans[i][0]),
			End:   EncVal(expr.unionSpans[i][1]),
		}, true /* tight */)
		if invertedExpr == nil {
			invertedExpr = spanExpr
		} else {
			invertedExpr = Or(invertedExpr, spanExpr)
		}
	}

	for i := range expr.children {
		child := expr.children[i].makeSpanExpression()
		if invertedExpr == nil {
			invertedExpr = child
		} else {
			switch expr.operator {
			case SetIntersection:
				invertedExpr = And(invertedExpr, child)
			case SetUnion:
				invertedExpr = Or(invertedExpr, child)
			default:
				panic(fmt.Sprintf("invalid operator %v", expr.operator))
			}
		}
	}

	if invertedExpr == nil {
		return nil
	}
	return invertedExpr.(*SpanExpression)
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
