// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// BuildQuery initializes an optimizer and builds the given sql statement.
func BuildQuery(
	t *testing.T, o *xform.Optimizer, catalog cat.Catalog, evalCtx *tree.EvalContext, sql string,
) {
	stmt, err := parser.ParseOne(sql)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()
	if err := semaCtx.Placeholders.Init(stmt.NumPlaceholders, nil /* typeHints */); err != nil {
		t.Fatal(err)
	}
	semaCtx.Annotations = tree.MakeAnnotations(stmt.NumAnnotations)
	o.Init(evalCtx, catalog)
	err = optbuilder.New(ctx, &semaCtx, evalCtx, catalog, o.Factory(), stmt.AST).Build()
	if err != nil {
		t.Fatal(err)
	}
}

// BuildScalar builds the given input string as a ScalarExpr and returns it.
func BuildScalar(
	t *testing.T, f *norm.Factory, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, input string,
) opt.ScalarExpr {
	expr, err := parser.ParseExpr(input)
	if err != nil {
		t.Fatal(err)
	}

	b := optbuilder.NewScalar(context.Background(), semaCtx, evalCtx, f)
	if err := b.Build(expr); err != nil {
		t.Fatal(err)
	}

	return f.Memo().RootExpr().(opt.ScalarExpr)
}

// BuildFilters builds the given input string as a FiltersExpr and returns it.
// Calls a subset of the normalization rules that would apply if these filters
// were built as part of a Select or Join.
func BuildFilters(
	t *testing.T, f *norm.Factory, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, input string,
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
