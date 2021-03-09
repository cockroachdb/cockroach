// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemaexpr_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func TestCheckConstraintBuilder_Build(t *testing.T) {
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()

	// Trick to get the init() for the builtins package to run.
	_ = builtins.AllBuiltinNames

	database := tree.Name("foo")
	table := tree.Name("bar")
	tn := tree.MakeTableNameWithSchema(database, tree.PublicSchemaName, table)

	desc := testTableDesc(
		string(table),
		[]testCol{{"a", types.Bool}, {"b", types.Int}},
		[]testCol{{"c", types.String}},
	)

	builder := schemaexpr.MakeCheckConstraintBuilder(ctx, tn, desc, &semaCtx)
	builder.MarkNameInUse("check_a3")

	testData := []struct {
		name          string
		expr          string
		expectedValid bool
		expectedExpr  string
		expectedName  string
	}{
		// Respect custom names.
		{"chk_1", "a", true, "a", "chk_1"},

		// Use unique default names when there is no custom name.
		{"", "a", true, "a", "check_a"},
		{"", "a", true, "a", "check_a1"},
		{"", "a", true, "a", "check_a2"},
		{"", "a AND b = 0", true, "a AND (b = 0:::INT8)", "check_a_b"},
		{"", "a AND b = 1", true, "a AND (b = 1:::INT8)", "check_a_b1"},
		{"", "a AND b = 1", true, "a AND (b = 1:::INT8)", "check_a_b2"},

		// Respect that "check_a3" has been marked, so the next check constraint
		// with "a" should be "check_a4".
		{"", "a", true, "a", "check_a4"},
		{"", "a", true, "a", "check_a5"},

		// Allow expressions that result in a bool.
		{"ck", "a", true, "a", "ck"},
		{"ck", "b = 0", true, "b = 0:::INT8", "ck"},
		{"ck", "a AND b = 0", true, "a AND (b = 0:::INT8)", "ck"},
		{"ck", "a IS NULL", true, "a IS NULL", "ck"},
		{"ck", "b IN (1, 2)", true, "b IN (1:::INT8, 2:::INT8)", "ck"},

		// Allow immutable functions.
		{"ck", "abs(b) > 0", true, "abs(b) > 0:::INT8", "ck"},
		{"ck", "c || c = 'foofoo'", true, "(c || c) = 'foofoo':::STRING", "ck"},
		{"ck", "lower(c) = 'bar'", true, "lower(c) = 'bar':::STRING", "ck"},

		// Allow mutable functions.
		{"ck", "b > random()", true, "b > random()", "ck"},

		// Disallow references to columns not in the table.
		{"", "d", false, "", ""},
		{"", "t.a", false, "", ""},

		// Disallow expressions that do not result in a bool.
		{"", "b", false, "", ""},
		{"", "abs(b)", false, "", ""},
		{"", "lower(c)", false, "", ""},

		// Disallow subqueries.
		{"", "exists(select 1)", false, "", ""},
		{"", "b IN (select 1)", false, "", ""},

		// Disallow aggregate, window, and set returning functions.
		{"", "sum(b) > 10", false, "", ""},
		{"", "row_number() OVER () > 1", false, "", ""},
		{"", "generate_series(1, 1) > 2", false, "", ""},

		// Dequalify column names.
		{"ck", "bar.a", true, "a", "ck"},
		{"ck", "foo.bar.a", true, "a", "ck"},
		{"ck", "bar.b = 0", true, "b = 0:::INT8", "ck"},
		{"ck", "foo.bar.b = 0", true, "b = 0:::INT8", "ck"},
		{"ck", "bar.a AND foo.bar.b = 0", true, "a AND (b = 0:::INT8)", "ck"},
	}

	for _, d := range testData {
		t.Run(d.expr, func(t *testing.T) {
			expr, err := parser.ParseExpr(d.expr)
			if err != nil {
				t.Fatalf("%s: unexpected error: %s", d.expr, err)
			}

			ckDef := &tree.CheckConstraintTableDef{Name: tree.Name(d.name), Expr: expr}

			ck, err := builder.Build(ckDef)

			if !d.expectedValid {
				if err == nil {
					t.Fatalf("%s: expected invalid expression, but was valid", d.expr)
				}
				// The input expression is invalid so there is no need to check
				// the output ck.
				return
			}

			if err != nil {
				t.Fatalf("%s: expected valid expression, but found error: %s", d.expr, err)
			}

			if ck.Name != d.expectedName || ck.Expr != d.expectedExpr {
				t.Errorf(
					`%s: expected "%s CHECK %s", got "%s CHECK %s"`,
					d.expr,
					d.expectedName,
					d.expectedExpr,
					ck.Name,
					ck.Expr,
				)
			}
		})
	}
}

func TestCheckConstraintBuilder_DefaultName(t *testing.T) {
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()

	database := tree.Name("foo")
	table := tree.Name("bar")
	tn := tree.MakeTableNameWithSchema(database, tree.PublicSchemaName, table)

	desc := testTableDesc(
		string(table),
		[]testCol{{"a", types.Bool}, {"b", types.Int}},
		[]testCol{{"c", types.String}, {"d", types.String}, {"e", types.String}},
	)

	builder := schemaexpr.MakeCheckConstraintBuilder(ctx, tn, desc, &semaCtx)

	testData := []struct {
		expr     string
		expected string
	}{
		{"a > 0", "check_a"},
		{"a > 0 AND b = 'foo'", "check_a_b"},
		{"a > 0 AND (b = 'foo' OR a < 20)", "check_a_b_a"},
		{"a > 0 AND a < 20", "check_a_a"},
		{"((a AND (b OR c)) AND d) OR e", "check_a_b_c_d_e"},
	}

	for _, d := range testData {
		t.Run(d.expr, func(t *testing.T) {
			expr, err := parser.ParseExpr(d.expr)
			if err != nil {
				t.Fatalf("%s: unexpected error: %s", d.expr, err)
			}

			r, err := builder.DefaultName(expr)
			if err != nil {
				t.Fatalf("%s: expected success, but found error: %s", d.expr, err)
			}

			if r != d.expected {
				t.Errorf("%s: expected %q, got %q", d.expr, d.expected, r)
			}
		})
	}
}
