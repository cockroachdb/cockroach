// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// upsertExcludedTable is the name of a synthetic table used in an upsert's set
// expressions to refer to the values that would be inserted for a row if it
// didn't conflict.
// Example: `INSERT INTO kv VALUES (1, 2) ON CONFLICT (k) DO UPDATE SET v = excluded.v`
var upsertExcludedTable = tree.TableName{TableName: "excluded"}

type upsertHelper struct {
	p                  *planner
	evalExprs          []tree.TypedExpr
	whereExpr          tree.TypedExpr
	sourceInfo         *dataSourceInfo
	excludedSourceInfo *dataSourceInfo
	curSourceRow       tree.Datums
	curExcludedRow     tree.Datums

	ivarHelper *tree.IndexedVarHelper

	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	//
	//lint:ignore U1000 this marker prevents by-value copies.
	noCopy util.NoCopy
}

var _ tableUpsertEvaler = (*upsertHelper)(nil)

// IndexedVarEval implements the tree.IndexedVarContainer interface.
func (uh *upsertHelper) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	numSourceColumns := len(uh.sourceInfo.sourceColumns)
	if idx >= numSourceColumns {
		return uh.curExcludedRow[idx-numSourceColumns].Eval(ctx)
	}
	return uh.curSourceRow[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (uh *upsertHelper) IndexedVarResolvedType(idx int) types.T {
	numSourceColumns := len(uh.sourceInfo.sourceColumns)
	if idx >= numSourceColumns {
		return uh.excludedSourceInfo.sourceColumns[idx-numSourceColumns].Typ
	}
	return uh.sourceInfo.sourceColumns[idx].Typ
}

// IndexedVarNodeFormatter implements the tree.IndexedVarContainer interface.
func (uh *upsertHelper) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	numSourceColumns := len(uh.sourceInfo.sourceColumns)
	if idx >= numSourceColumns {
		return uh.excludedSourceInfo.NodeFormatter(idx - numSourceColumns)
	}
	return uh.sourceInfo.NodeFormatter(idx)
}

func (p *planner) makeUpsertHelper(
	ctx context.Context,
	tn *tree.TableName,
	tableDesc *sqlbase.TableDescriptor,
	insertCols []sqlbase.ColumnDescriptor,
	updateCols []sqlbase.ColumnDescriptor,
	updateExprs tree.UpdateExprs,
	upsertConflictIndex *sqlbase.IndexDescriptor,
	whereClause *tree.Where,
) (*upsertHelper, error) {
	defaultExprs, err := sqlbase.MakeDefaultExprs(
		updateCols, &p.txCtx, p.EvalContext())
	if err != nil {
		return nil, err
	}

	untupledExprs := make(tree.Exprs, 0, len(updateExprs))
	i := 0
	for _, updateExpr := range updateExprs {
		if updateExpr.Tuple {
			if t, ok := updateExpr.Expr.(*tree.Tuple); ok {
				for _, e := range t.Exprs {
					e = fillDefault(e, i, defaultExprs)
					untupledExprs = append(untupledExprs, e)
					i++
				}
			}
		} else {
			e := fillDefault(updateExpr.Expr, i, defaultExprs)
			untupledExprs = append(untupledExprs, e)
			i++
		}
	}

	sourceInfo := newSourceInfoForSingleTable(
		*tn, sqlbase.ResultColumnsFromColDescs(tableDesc.Columns),
	)
	excludedSourceInfo := newSourceInfoForSingleTable(
		upsertExcludedTable, sqlbase.ResultColumnsFromColDescs(insertCols),
	)

	helper := &upsertHelper{
		p:                  p,
		sourceInfo:         sourceInfo,
		excludedSourceInfo: excludedSourceInfo,
	}

	var evalExprs []tree.TypedExpr
	ivarHelper := tree.MakeIndexedVarHelper(helper, len(sourceInfo.sourceColumns)+len(excludedSourceInfo.sourceColumns))
	sources := multiSourceInfo{sourceInfo, excludedSourceInfo}
	for i, expr := range untupledExprs {
		typ := updateCols[i].Type.ToDatumType()
		normExpr, err := p.analyzeExpr(ctx, expr, sources, ivarHelper, typ, true, "ON CONFLICT")
		if err != nil {
			return nil, err
		}
		evalExprs = append(evalExprs, normExpr)
	}
	helper.ivarHelper = &ivarHelper
	helper.evalExprs = evalExprs

	if whereClause != nil {
		whereExpr, err := p.analyzeExpr(
			ctx, whereClause.Expr, sources, ivarHelper, types.Bool, true /* requireType */, "WHERE",
		)
		if err != nil {
			return nil, err
		}

		// Make sure there are no aggregation/window functions in the filter
		// (after subqueries have been expanded).
		if err := p.txCtx.AssertNoAggregationOrWindowing(
			whereExpr, "WHERE", p.SessionData().SearchPath,
		); err != nil {
			return nil, err
		}

		helper.whereExpr = whereExpr
	}

	return helper, nil
}

func (uh *upsertHelper) walkExprs(walk func(desc string, index int, expr tree.TypedExpr)) {
	for i, evalExpr := range uh.evalExprs {
		walk("eval", i, evalExpr)
	}
}

// eval returns the values for the update case of an upsert, given the row
// that would have been inserted and the existing (conflicting) values.
func (uh *upsertHelper) eval(insertRow tree.Datums, existingRow tree.Datums) (tree.Datums, error) {
	uh.curSourceRow = existingRow
	uh.curExcludedRow = insertRow

	var err error
	ret := make([]tree.Datum, len(uh.evalExprs))
	uh.p.extendedEvalCtx.IVarHelper = uh.ivarHelper
	defer func() { uh.p.extendedEvalCtx.IVarHelper = nil }()
	for i, evalExpr := range uh.evalExprs {
		ret[i], err = evalExpr.Eval(uh.p.EvalContext())
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

// shouldUpdate returns the result of evaluating the WHERE clause of the
// ON CONFLICT ... DO UPDATE clause.
func (uh *upsertHelper) shouldUpdate(insertRow tree.Datums, existingRow tree.Datums) (bool, error) {
	uh.curSourceRow = existingRow
	uh.curExcludedRow = insertRow

	uh.p.extendedEvalCtx.IVarHelper = uh.ivarHelper
	defer func() { uh.p.extendedEvalCtx.IVarHelper = nil }()
	return sqlbase.RunFilter(uh.whereExpr, uh.p.EvalContext())
}

// upsertExprsAndIndex returns the upsert conflict index and the (possibly
// synthetic) SET expressions used when a row conflicts.
func upsertExprsAndIndex(
	tableDesc *sqlbase.TableDescriptor,
	onConflict tree.OnConflict,
	insertCols []sqlbase.ColumnDescriptor,
) (tree.UpdateExprs, *sqlbase.IndexDescriptor, error) {
	if onConflict.IsUpsertAlias() {
		// The UPSERT syntactic sugar is the same as the longhand specifying the
		// primary index as the conflict index and SET expressions for the columns
		// in insertCols minus any columns in the conflict index. Example:
		// `UPSERT INTO abc VALUES (1, 2, 3)` is syntactic sugar for
		// `INSERT INTO abc VALUES (1, 2, 3) ON CONFLICT a DO UPDATE SET b = 2, c = 3`.
		conflictIndex := &tableDesc.PrimaryIndex
		indexColSet := make(map[sqlbase.ColumnID]struct{}, len(conflictIndex.ColumnIDs))
		for _, colID := range conflictIndex.ColumnIDs {
			indexColSet[colID] = struct{}{}
		}
		updateExprs := make(tree.UpdateExprs, 0, len(insertCols))
		for _, c := range insertCols {
			if _, ok := indexColSet[c.ID]; !ok {
				n := tree.Name(c.Name)
				names := tree.UnresolvedNames{
					tree.UnresolvedName{&n},
				}
				expr := &tree.ColumnItem{
					TableName:  upsertExcludedTable,
					ColumnName: n,
				}
				updateExprs = append(updateExprs, &tree.UpdateExpr{Names: names, Expr: expr})
			}
		}
		return updateExprs, conflictIndex, nil
	}

	indexMatch := func(index sqlbase.IndexDescriptor) bool {
		if !index.Unique {
			return false
		}
		if len(index.ColumnNames) != len(onConflict.Columns) {
			return false
		}
		for i, colName := range index.ColumnNames {
			if colName != string(onConflict.Columns[i]) {
				return false
			}
		}
		return true
	}

	if indexMatch(tableDesc.PrimaryIndex) {
		return onConflict.Exprs, &tableDesc.PrimaryIndex, nil
	}
	for _, index := range tableDesc.Indexes {
		if indexMatch(index) {
			return onConflict.Exprs, &index, nil
		}
	}
	return nil, nil, fmt.Errorf("there is no unique or exclusion constraint matching the ON CONFLICT specification")
}
