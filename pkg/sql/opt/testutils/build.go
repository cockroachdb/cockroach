// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// BuildQuery initializes an optimizer and builds the given sql statement.
func BuildQuery(
	t *testing.T, o *xform.Optimizer, catalog cat.Catalog, evalCtx *eval.Context, sql string,
) {
	stmt, err := parser.ParseOne(sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	ctx := context.Background()
	semaCtx := tree.MakeSemaContext(catalog)
	semaCtx.SearchPath = &evalCtx.SessionData().SearchPath
	semaCtx.Placeholders.Init(stmt.NumPlaceholders, nil /* typeHints */)
	semaCtx.Annotations = tree.MakeAnnotations(stmt.NumAnnotations)
	o.Init(ctx, evalCtx, catalog)
	err = optbuilder.New(ctx, &semaCtx, evalCtx, catalog, o.Factory(), stmt.AST).Build()
	if err != nil {
		t.Fatalf("%+v", err)
	}
}

// BuildScalar builds the given input string as a ScalarExpr and returns it.
func BuildScalar(
	t *testing.T, f *norm.Factory, semaCtx *tree.SemaContext, evalCtx *eval.Context, input string,
) opt.ScalarExpr {
	expr, err := parser.ParseExpr(input)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	b := optbuilder.NewScalar(context.Background(), semaCtx, evalCtx, f)
	root, err := b.Build(expr)
	if err != nil {
		t.Fatal(err)
	}

	return root
}

// BuildFilters builds the given input string as a FiltersExpr and returns it.
// Calls a subset of the normalization rules that would apply if these filters
// were built as part of a Select or Join.
func BuildFilters(
	t *testing.T, f *norm.Factory, semaCtx *tree.SemaContext, evalCtx *eval.Context, input string,
) memo.FiltersExpr {
	if input == "" {
		return memo.TrueFilter
	}
	root := BuildScalar(t, f, semaCtx, evalCtx, input)

	if _, ok := root.(*memo.TrueExpr); ok {
		return memo.TrueFilter
	}
	filters := memo.FiltersExpr{f.ConstructFiltersItem(root)}
	filters = f.CustomFuncs().SimplifyFilters(filters)
	filters = f.CustomFuncs().ConsolidateFilters(filters)
	return filters
}
