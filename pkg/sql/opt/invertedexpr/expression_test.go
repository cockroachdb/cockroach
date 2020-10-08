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
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
	"github.com/cockroachdb/datadriven"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

/*
Format for the datadriven test:

new-span-leaf name=<name> tight=<true|false> span=<start>[,<end>]
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

func getSpan(t *testing.T, d *datadriven.TestData) InvertedSpan {
	var str string
	d.ScanArgs(t, "span", &str)
	parts := strings.Split(str, ",")
	if len(parts) > 2 {
		d.Fatalf(t, "incorrect span format: %s", str)
		return InvertedSpan{}
	} else if len(parts) == 2 {
		return InvertedSpan{Start: []byte(parts[0]), End: []byte(parts[1])}
	} else {
		return MakeSingleInvertedValSpan([]byte(parts[0]))
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

// Makes a (shallow) copy of the root node of the expression identified
// by name, since calls to And() and Or() can modify that root node, and
// the test wants to preserve the unmodified expression for later use.
func getExprCopy(
	t *testing.T, d *datadriven.TestData, name string, exprsByName map[string]InvertedExpression,
) InvertedExpression {
	expr := exprsByName[name]
	if expr == nil {
		d.Fatalf(t, "unknown expr: %s", name)
	}
	switch e := expr.(type) {
	case *SpanExpression:
		return &SpanExpression{
			Tight:              e.Tight,
			SpansToRead:        append([]InvertedSpan(nil), e.SpansToRead...),
			FactoredUnionSpans: append([]InvertedSpan(nil), e.FactoredUnionSpans...),
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

func toString(expr InvertedExpression) string {
	tp := treeprinter.New()
	formatExpression(tp, expr, true /* includeSpansToRead */)
	return tp.String()
}

func getLeftAndRightExpr(
	t *testing.T, d *datadriven.TestData, exprsByName map[string]InvertedExpression,
) (InvertedExpression, InvertedExpression) {
	var leftName, rightName string
	d.ScanArgs(t, "left", &leftName)
	d.ScanArgs(t, "right", &rightName)
	return getExprCopy(t, d, leftName, exprsByName), getExprCopy(t, d, rightName, exprsByName)
}

func TestExpression(t *testing.T) {
	defer leaktest.AfterTest(t)()
	exprsByName := make(map[string]InvertedExpression)

	datadriven.RunTest(t, "testdata/expression", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "new-span-leaf":
			var name string
			d.ScanArgs(t, "name", &name)
			var tight bool
			d.ScanArgs(t, "tight", &tight)
			span := getSpan(t, d)
			expr := ExprForInvertedSpan(span, tight)
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

func span(start, end string) InvertedSpan {
	return InvertedSpan{Start: []byte(start), End: []byte(end)}
}

func single(start string) InvertedSpan {
	return MakeSingleInvertedValSpan([]byte(start))
}

func checkEqual(t *testing.T, expected, actual []InvertedSpan) {
	require.Equal(t, len(expected), len(actual))
	for i := range expected {
		require.Equal(t, expected[i].Start, actual[i].Start)
		require.Equal(t, expected[i].End, actual[i].End)
	}
}

func TestSetUnion(t *testing.T) {
	checkEqual(t,
		[]InvertedSpan{span("b", "c")},
		unionSpans(
			[]InvertedSpan{single("b")},
			[]InvertedSpan{span("b", "c")},
		),
	)
	checkEqual(t,
		[]InvertedSpan{span("b", "d")},
		unionSpans(
			[]InvertedSpan{single("b")},
			[]InvertedSpan{single("c")},
		),
	)
	checkEqual(t,
		[]InvertedSpan{span("b", "d")},
		unionSpans(
			[]InvertedSpan{single("b")},
			[]InvertedSpan{span("c", "d")},
		),
	)
	checkEqual(t,
		[]InvertedSpan{span("b", "d")},
		unionSpans(
			[]InvertedSpan{span("b", "c")},
			[]InvertedSpan{span("c", "d")},
		),
	)
	checkEqual(t,
		[]InvertedSpan{span("b", "c"), single("d")},
		unionSpans(
			[]InvertedSpan{span("b", "c")},
			[]InvertedSpan{single("d")},
		),
	)
	checkEqual(t,
		[]InvertedSpan{span("b", "c"), single("d"), single("f")},
		unionSpans(
			[]InvertedSpan{span("b", "c"), single("f")},
			[]InvertedSpan{single("d")},
		),
	)
	checkEqual(t,
		[]InvertedSpan{span("b", "d"), single("e")},
		unionSpans(
			[]InvertedSpan{span("b", "c"), single("e")},
			[]InvertedSpan{span("c", "d")},
		),
	)
	checkEqual(t,
		[]InvertedSpan{span("b", "f")},
		unionSpans(
			[]InvertedSpan{span("b", "c"), single("e")},
			[]InvertedSpan{span("c", "f")},
		),
	)
}

func TestSetIntersection(t *testing.T) {
	checkEqual(t,
		[]InvertedSpan{single("b")},
		intersectSpans(
			[]InvertedSpan{single("b")},
			[]InvertedSpan{span("b", "c")},
		),
	)
	checkEqual(t,
		nil,
		intersectSpans(
			[]InvertedSpan{single("b")},
			[]InvertedSpan{span("c", "d")},
		),
	)
	checkEqual(t,
		[]InvertedSpan{single("b"), span("d", "d\x00"), span("dd", "e"), span("f", "ff")},
		intersectSpans(
			[]InvertedSpan{single("b"), span("d", "e"), span("f", "g")},
			[]InvertedSpan{span("b", "d\x00"), span("dd", "ff")},
		),
	)
}

func TestSetSubtraction(t *testing.T) {
	checkEqual(t,
		nil,
		subtractSpans(
			[]InvertedSpan{single("b")},
			[]InvertedSpan{span("b", "c")},
		),
	)
	checkEqual(t,
		[]InvertedSpan{span("b\x00", "d")},
		subtractSpans(
			[]InvertedSpan{span("b", "d")},
			[]InvertedSpan{span("b", "b\x00")},
		),
	)
	checkEqual(t,
		[]InvertedSpan{span("b", "d"), span("e", "ea")},
		subtractSpans(
			[]InvertedSpan{span("b", "d"), span("e", "f")},
			[]InvertedSpan{span("ea", "f")},
		),
	)
	checkEqual(t,
		[]InvertedSpan{span("d", "da"), span("db", "dc"),
			span("dd", "df"), span("fa", "g")},
		subtractSpans(
			[]InvertedSpan{single("b"), span("d", "e"), span("f", "g")},
			[]InvertedSpan{span("b", "c"), span("da", "db"),
				span("dc", "dd"), span("df", "e"), span("f", "fa")},
		),
	)

}
