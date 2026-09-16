// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execexpr

import (
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDeserializeExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	e := execinfrapb.Expression{Expr: "@1 * (@2 + @3) + @1"}

	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	semaCtx := tree.MakeSemaContext(nil /* resolver */)
	expr, err := DeserializeExpr(
		context.Background(),
		e,
		[]*types.T{types.Int, types.Int, types.Int},
		&semaCtx,
		&evalCtx,
	)
	if err != nil {
		t.Fatal(err)
	}

	str := expr.String()
	expectedStr := "(@1 * (@2 + @3)) + @1"
	if str != expectedStr {
		t.Errorf("invalid expression string '%s', expected '%s'", str, expectedStr)
	}

	// Verify the expression is fully typed.
	typ := expr.ResolvedType()
	if !typ.Equivalent(types.Int) {
		t.Errorf("invalid expression type %s", typ)
	}

	// We can process a new expression with the same tree.IndexedVarHelper.
	e = execinfrapb.Expression{Expr: "@4 - @1"}
	expr, err = DeserializeExpr(
		context.Background(),
		e,
		[]*types.T{types.Int, types.Int, types.Int, types.Int},
		&semaCtx,
		&evalCtx,
	)
	if err != nil {
		t.Fatal(err)
	}

	// Verify that the new expression can be formatted correctly.
	str = expr.String()
	expectedStr = "@4 - @1"
	if str != expectedStr {
		t.Errorf("invalid expression string '%s', expected '%s'", str, expectedStr)
	}

	// Verify the expression is fully typed.
	typ = expr.ResolvedType()
	if !typ.Equivalent(types.Int) {
		t.Errorf("invalid expression type %s", typ)
	}
}

// Test that processExpression evaluates constant exprs into datums.
func TestDeserializeExpressionConstantEval(t *testing.T) {
	defer leaktest.AfterTest(t)()

	e := execinfrapb.Expression{Expr: "ARRAY[1:::INT,2:::INT]"}

	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	semaCtx := tree.MakeSemaContext(nil /* resolver */)
	expr, err := DeserializeExpr(
		context.Background(),
		e,
		[]*types.T{types.Int, types.Int},
		&semaCtx,
		&evalCtx,
	)
	if err != nil {
		t.Fatal(err)
	}

	expected := tree.NewDArrayFromDatums(
		types.Int, tree.Datums{tree.NewDInt(1), tree.NewDInt(2)},
	)
	if !reflect.DeepEqual(expr, expected) {
		t.Errorf("invalid expr '%v', expected '%v'", expr, expected)
	}
}

func TestDeserializeDecimalSignedZero(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(ctx)
	semaCtx := tree.MakeSemaContext(nil /* resolver */)

	for _, input := range []string{
		"-0", "-0.00", "-0E+3", "-0E-2000", "-0E+2000",
		"0", "0.00", "-1.25", "1.25", "Infinity", "-Infinity", "NaN",
	} {
		t.Run(input, func(t *testing.T) {
			// Arithmetic can produce negative zero even though SQL literal parsing
			// canonicalizes it. Construct that intermediate representation directly.
			d := &tree.DDecimal{}
			if _, _, err := d.Decimal.SetString(input); err != nil {
				t.Fatal(err)
			}
			before := d.Decimal.String()
			for name, flags := range map[string]tree.FmtFlags{
				"parsable": tree.FmtParsable, "serializable": tree.FmtSerializable,
				"equivalence": tree.FmtCheckEquivalence,
			} {
				t.Run(name, func(t *testing.T) {
					serialized := tree.AsStringWithFlags(d, flags)
					actual, err := DeserializeExpr(ctx, execinfrapb.Expression{Expr: serialized}, nil, &semaCtx, &evalCtx)
					if err != nil {
						t.Fatal(err)
					}
					got, ok := actual.(*tree.DDecimal)
					if !ok || d.Decimal.CmpTotal(&got.Decimal) != 0 {
						t.Fatalf("%s serialized as %s, deserialized as %s", before, serialized, actual)
					}
					if d.Decimal.String() != before {
						t.Fatal("formatting mutated the original datum")
					}
				})
			}
			for _, nested := range []tree.Datum{
				tree.NewDArrayFromDatums(types.Decimal, tree.Datums{d, tree.DNull}),
				tree.NewDTuple(types.MakeTuple([]*types.T{types.Decimal, types.Int}), d, tree.NewDInt(1)),
			} {
				serialized := tree.Serialize(nested)
				actual, err := DeserializeExpr(ctx, execinfrapb.Expression{Expr: serialized}, nil, &semaCtx, &evalCtx)
				if err != nil {
					t.Fatal(err)
				}
				if actual.String() != nested.String() {
					t.Fatalf("nested %s serialized as %s, deserialized as %s", nested, serialized, actual)
				}
			}
			for _, flags := range []tree.FmtFlags{tree.FmtSimple, tree.FmtPgwireText, tree.FmtExport} {
				if actual := tree.AsStringWithFlags(d, flags); actual != before {
					t.Fatalf("ordinary formatting changed %s to %s", before, actual)
				}
			}
		})
	}
}
