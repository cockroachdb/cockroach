// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemaexpr_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func TestValidateExpr(t *testing.T) {
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext(nil /* resolver */)

	// Trick to get the init() for the builtins package to run.
	_ = builtins.AllBuiltinNames()

	database := tree.Name("foo")
	table := tree.Name("bar")
	tn := tree.MakeTableNameWithSchema(database, catconstants.PublicSchemaName, table)

	desc := testTableDesc(
		string(table),
		[]testCol{{"a", types.Bool}, {"b", types.Int}},
		[]testCol{{"c", types.String}},
	)

	testData := []struct {
		expr          string
		expectedValid bool
		expectedExpr  string
		typ           *types.T
		maxVolatility volatility.V
	}{
		// De-qualify column names.
		{"bar.a", true, "a", types.Bool, volatility.Immutable},
		{"foo.bar.a", true, "a", types.Bool, volatility.Immutable},
		{"bar.b = 0", true, "b = 0:::INT8", types.Bool, volatility.Immutable},
		{"foo.bar.b = 0", true, "b = 0:::INT8", types.Bool, volatility.Immutable},
		{"bar.a AND foo.bar.b = 0", true, "a AND (b = 0:::INT8)", types.Bool, volatility.Immutable},

		// Validates the type of the expression.
		{"concat(c, c)", true, "concat(c, c)", types.String, volatility.Immutable},
		{"concat(c, c)", false, "", types.Int, volatility.Immutable},
		{"b + 1", true, "b + 1:::INT8", types.Int, volatility.Immutable},
		{"b + 1", false, "", types.Bool, volatility.Immutable},

		// Validates that the expression has no variable expressions.
		{"$1", false, "", types.AnyElement, volatility.Immutable},

		// Validates the volatility check.
		{"now()", true, "now():::TIMESTAMPTZ", types.TimestampTZ, volatility.Volatile},
		{"now()", true, "now():::TIMESTAMPTZ", types.TimestampTZ, volatility.Stable},
		{"now()", false, "", types.AnyElement, volatility.Immutable},
		{"uuid_v4()::STRING", true, "uuid_v4()::STRING", types.String, volatility.Volatile},
		{"uuid_v4()::STRING", false, "", types.String, volatility.Stable},
		{"uuid_v4()::STRING", false, "", types.String, volatility.Immutable},
	}

	for _, d := range testData {
		t.Run(d.expr, func(t *testing.T) {
			expr, err := parser.ParseExpr(d.expr)
			if err != nil {
				t.Fatalf("%s: unexpected error: %s", d.expr, err)
			}

			deqExpr, _, _, err := schemaexpr.DequalifyAndValidateExpr(
				ctx,
				desc,
				expr,
				d.typ,
				"test-validate-expr",
				&semaCtx,
				d.maxVolatility,
				&tn,
				clusterversion.TestingClusterVersion,
			)

			if !d.expectedValid {
				if err == nil {
					t.Fatalf("%s: expected invalid expression, but was valid", d.expr)
				}
				// The input expression is invalid so there is no need to check
				// the output expression r.
				return
			}

			if err != nil {
				t.Fatalf("%s: expected valid expression, but found error: %s", d.expr, err)
			}

			if deqExpr != d.expectedExpr {
				t.Errorf("%s: expected %q, got %q", d.expr, d.expectedExpr, deqExpr)
			}
		})
	}
}

func TestExtractColumnIDs(t *testing.T) {
	// Trick to get the init() for the builtins package to run.
	_ = builtins.AllBuiltinNames()

	table := tree.Name("foo")
	desc := testTableDesc(
		string(table),
		[]testCol{{"a", types.Bool}, {"b", types.Int}},
		[]testCol{{"c", types.String}},
	)

	testData := []struct {
		expr     string
		expected string
	}{
		{"true", "()"},
		{"now()", "()"},
		{"a", "(1)"},
		{"a AND b > 1", "(1,2)"},
		{"a AND c = 'foo'", "(1,3)"},
		{"a OR (b > 1 AND c = 'foo')", "(1-3)"},
		{"a AND abs(b) > 5 AND lower(c) = 'foo'", "(1-3)"},
	}

	for _, d := range testData {
		t.Run(d.expr, func(t *testing.T) {
			expr, err := parser.ParseExpr(d.expr)
			if err != nil {
				t.Fatalf("%s: unexpected error: %s", d.expr, err)
			}

			colIDs, err := schemaexpr.ExtractColumnIDs(desc, expr)
			if err != nil {
				t.Fatalf("%s: unexpected error: %s", d.expr, err)
			}

			if colIDs.String() != d.expected {
				t.Errorf("%s: expected %q, got %q", d.expr, d.expected, colIDs)
			}
		})
	}
}

func TestValidColumnReferences(t *testing.T) {
	// Trick to get the init() for the builtins package to run.
	_ = builtins.AllBuiltinNames()

	table := tree.Name("foo")
	desc := testTableDesc(
		string(table),
		[]testCol{{"a", types.Bool}, {"b", types.Int}},
		[]testCol{{"c", types.String}},
	)

	testData := []struct {
		expr     string
		expected bool
	}{
		{"true", true},
		{"now()", true},
		{"a", true},
		{"a AND b > 1", true},
		{"a AND c = 'foo'", true},
		{"a OR (b > 1 AND c = 'foo')", true},
		{"a AND abs(b) > 5 AND lower(c) = 'foo'", true},
		{"x", false},
		{"a AND x > 1", false},
		{"a AND x = 'foo'", false},
		{"a OR (b > 1 AND x = 'foo')", false},
		{"a AND abs(x) > 5 AND lower(y) = 'foo'", false},
	}

	for _, d := range testData {
		t.Run(d.expr, func(t *testing.T) {
			expr, err := parser.ParseExpr(d.expr)
			if err != nil {
				t.Fatalf("%s: unexpected error: %s", d.expr, err)
			}

			res, err := schemaexpr.HasValidColumnReferences(desc, expr)
			if err != nil {
				t.Fatalf("%s: unexpected error: %s", d.expr, err)
			}

			if res != d.expected {
				t.Errorf("%s: expected %t, got %t", d.expr, d.expected, res)
			}
		})
	}
}
