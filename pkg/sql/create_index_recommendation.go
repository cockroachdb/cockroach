// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
)

// buildIndexRecExpression builds a statement and walks through it to find
// potential index candidates.
func (p *planner) buildIndexRecExpression(ctx context.Context) {
	opc := &p.optPlanningCtx
	f := opc.optimizer.Factory()
	f.FoldingControl().AllowStableFolds()
	bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), &opc.catalog, f, opc.p.stmt.AST)
	if err := bld.Build(); err != nil {
		return
	}

	indexCandidates := make(map[cat.Table][][]string)
	walkOptExprIndexCandidates(
		f.Memo().RootExpr(),
		f.Memo().Metadata(),
		&opc.catalog,
		indexCandidates,
	)
}

// walkOptExprIndexCandidates finds potential index candidates for a given
// query. It adds candidates for the following cases:
//
// 	1. Add a single index on all columns in a Group By expression if the columns
//	   are from the same table. Otherwise, group expressions into indexes by
//	   table.
// 	2. Add a single-column index on any column that appears in a JOIN predicate.
//  3. Add a single-column index on any Range expression, or comparison
//     expression (=, <, >, <=, >=).
//  4. If there exists multiple columns from the same table in a JOIN predicate,
//     create a single index on all such columns.
//
// TODO(neha): *memo.SortExpr is not added in the build phase, we need to wait
//  until optimization for those potential candidates.
// TODO(neha): For now, index candidates are added to a map of tables to string
//   matrices. Each outer slice contains inner slices that represent indexes.
//   Each string in the index slice represents a column. This information
//   will be used to create hypothetical indexes.
// TODO(neha): Add information about potential STORING columns to add for
// 	indexes where adding them could avoid index-joins.
// TODO(neha): Add potential multi-column candidates that combine different
//  categories (JOIN, ORDER, etc.)
// TODO(neha): Formally test these functions and ensure the code is in the
//  desired place in the codebase.
func walkOptExprIndexCandidates(
	expr opt.Expr,
	metadata *opt.Metadata,
	catalog *optCatalog,
	indexCandidates map[cat.Table][][]string,
) {
	switch expr := expr.(type) {
	case *memo.GroupByExpr:
		addMultiColumnIndex(expr.GroupingCols, metadata, catalog, indexCandidates)
	case *memo.RangeExpr:
		exprAnd := expr.And.(*memo.AndExpr)
		addVariableExprIndex(exprAnd.Left, metadata, catalog, indexCandidates)
		addVariableExprIndex(exprAnd.Right, metadata, catalog, indexCandidates)
	case *memo.EqExpr:
		addVariableExprIndex(expr.Left, metadata, catalog, indexCandidates)
		addVariableExprIndex(expr.Right, metadata, catalog, indexCandidates)
	case *memo.LtExpr:
		addVariableExprIndex(expr.Left, metadata, catalog, indexCandidates)
		addVariableExprIndex(expr.Right, metadata, catalog, indexCandidates)
	case *memo.GtExpr:
		addVariableExprIndex(expr.Left, metadata, catalog, indexCandidates)
		addVariableExprIndex(expr.Right, metadata, catalog, indexCandidates)
	case *memo.LeExpr:
		addVariableExprIndex(expr.Left, metadata, catalog, indexCandidates)
		addVariableExprIndex(expr.Right, metadata, catalog, indexCandidates)
	case *memo.GeExpr:
		addVariableExprIndex(expr.Left, metadata, catalog, indexCandidates)
		addVariableExprIndex(expr.Right, metadata, catalog, indexCandidates)
	case *memo.InnerJoinExpr:
		for _, col := range expr.On.OuterCols().ToList() {
			addSingleColumnIndex(col, metadata, catalog, indexCandidates)
		}
		addMultiColumnIndex(expr.On.OuterCols(), metadata, catalog, indexCandidates)
	case *memo.LeftJoinExpr:
		for _, col := range expr.On.OuterCols().ToList() {
			addSingleColumnIndex(col, metadata, catalog, indexCandidates)
		}
		addMultiColumnIndex(expr.On.OuterCols(), metadata, catalog, indexCandidates)
	case *memo.RightJoinExpr:
		for _, col := range expr.On.OuterCols().ToList() {
			addSingleColumnIndex(col, metadata, catalog, indexCandidates)
		}
		addMultiColumnIndex(expr.On.OuterCols(), metadata, catalog, indexCandidates)
	case *memo.FullJoinExpr:
		for _, col := range expr.On.OuterCols().ToList() {
			addSingleColumnIndex(col, metadata, catalog, indexCandidates)
		}
		addMultiColumnIndex(expr.On.OuterCols(), metadata, catalog, indexCandidates)
	case *memo.SemiJoinExpr:
		for _, col := range expr.On.OuterCols().ToList() {
			addSingleColumnIndex(col, metadata, catalog, indexCandidates)
		}
		addMultiColumnIndex(expr.On.OuterCols(), metadata, catalog, indexCandidates)
	case *memo.AntiJoinExpr:
		for _, col := range expr.On.OuterCols().ToList() {
			addSingleColumnIndex(col, metadata, catalog, indexCandidates)
		}
		addMultiColumnIndex(expr.On.OuterCols(), metadata, catalog, indexCandidates)
	}
	for i, n := 0, expr.ChildCount(); i < n; i++ {
		walkOptExprIndexCandidates(expr.Child(i), metadata, catalog, indexCandidates)
	}
}

// addVariableExprIndex adds an index candidate to indexCandidates if the expr
// argument can be cast to a *memo.VariableExpr and the index does not already
// exist.
func addVariableExprIndex(
	expr opt.Expr,
	metadata *opt.Metadata,
	catalog *optCatalog,
	indexCandidates map[cat.Table][][]string,
) {
	switch expr := expr.(type) {
	case *memo.VariableExpr:
		addSingleColumnIndex(expr.Col, metadata, catalog, indexCandidates)
	}
}

// addMultiColumnIndex adds indexes to indexCandidates for groups of columns
// in a column set that are from the same table, without duplicates.
func addMultiColumnIndex(
	cols opt.ColSet,
	metadata *opt.Metadata,
	catalog *optCatalog,
	indexCandidates map[cat.Table][][]string,
) {
	// Group columns by table in a temporary map as single-column indexes,
	// getting rid of duplicates.
	tableToCols := make(map[cat.Table][][]string)
	for _, col := range cols.ToList() {
		addSingleColumnIndex(col, metadata, catalog, tableToCols)
	}

	// Combine all single-column indexes for a given table into one, and add
	// the corresponding multi-column index.
	for currTable := range tableToCols {
		var index []string
		for _, colSlice := range tableToCols[currTable] {
			index = append(index, colSlice[0])
		}
		addIndex(index, currTable, indexCandidates)
	}
}

// addSingleColumnIndex adds an index to indexCandidates on the column with the
// given opt.ColumnID if it does not already exist.
func addSingleColumnIndex(
	col opt.ColumnID,
	metadata *opt.Metadata,
	catalog *optCatalog,
	indexCandidates map[cat.Table][][]string,
) {
	columnMeta := metadata.ColumnMeta(col)
	columnName := metadata.QualifiedAlias(col, true, catalog)
	if columnMeta.Table == 0 {
		return
	}
	currTable := metadata.Table(columnMeta.Table)
	addIndex([]string{columnName}, currTable, indexCandidates)
}

// addIndex adds an index to indexCandidates if it does not already exist.
func addIndex(newIndex []string, currTable cat.Table, indexCandidates map[cat.Table][][]string) {
	// Don't add duplicate indexes.
	for _, existingIndex := range indexCandidates[currTable] {
		if len(existingIndex) != len(newIndex) {
			continue
		}
		potentialDuplicate := true
		for i := range existingIndex {
			if newIndex[i] != existingIndex[i] {
				potentialDuplicate = false
				break
			}
		}
		if potentialDuplicate {
			// Duplicate index found, return.
			return
		}
	}
	// Index does not exist already, add it.
	indexCandidates[currTable] = append(indexCandidates[currTable], newIndex)
}
