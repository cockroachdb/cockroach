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
//
// Author: Daniel Harrison (daniel.harrison@gmail.com)

package sql

import (
	"fmt"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
)

// upsertExcludedTable is the name of a synthetic table used in an upsert's set
// expressions to refer to the values that would be inserted for a row if it
// didn't conflict.
// Example: `INSERT INTO kv VALUES (1, 2) ON CONFLICT (k) DO UPDATE SET v = excluded.v`
const upsertExcludedTable = "excluded"

type upsertHelper struct {
	p                  *planner
	qvals              qvalMap
	evalExprs          []parser.TypedExpr
	sourceInfo         *dataSourceInfo
	excludedSourceInfo *dataSourceInfo
	allExprsIdentity   bool
}

var _ tableUpsertEvaler = (*upsertHelper)(nil)

func (p *planner) makeUpsertHelper(
	tableDesc *sqlbase.TableDescriptor,
	insertCols []sqlbase.ColumnDescriptor,
	updateCols []sqlbase.ColumnDescriptor,
	updateExprs parser.UpdateExprs,
	upsertConflictIndex *sqlbase.IndexDescriptor,
) (*upsertHelper, error) {
	defaultExprs, err := makeDefaultExprs(updateCols, &p.parser, &p.evalCtx)
	if err != nil {
		return nil, err
	}

	untupledExprs := make(parser.Exprs, 0, len(updateExprs))
	i := 0
	for _, updateExpr := range updateExprs {
		if updateExpr.Tuple {
			if t, ok := updateExpr.Expr.(*parser.Tuple); ok {
				for _, e := range t.Exprs {
					typ := updateCols[i].Type.ToDatumType()
					e := fillDefault(e, typ, i, defaultExprs)
					untupledExprs = append(untupledExprs, e)
					i++
				}
			}
		} else {
			typ := updateCols[i].Type.ToDatumType()
			e := fillDefault(updateExpr.Expr, typ, i, defaultExprs)
			untupledExprs = append(untupledExprs, e)
			i++
		}
	}

	allExprsIdentity := true
	for i, expr := range untupledExprs {
		qn, ok := expr.(*parser.QualifiedName)
		if !ok {
			allExprsIdentity = false
			break
		}
		if err := qn.NormalizeColumnName(); err != nil {
			return nil, err
		}
		if qn.Base != upsertExcludedTable || qn.Column() != updateCols[i].Name {
			allExprsIdentity = false
			break
		}
	}

	sourceInfo := newSourceInfoForSingleTable(tableDesc.Name, makeResultColumns(tableDesc.Columns))
	excludedSourceInfo := newSourceInfoForSingleTable(upsertExcludedTable, makeResultColumns(insertCols))

	var evalExprs []parser.TypedExpr
	qvals := make(qvalMap)
	sources := multiSourceInfo{sourceInfo, excludedSourceInfo}
	for _, expr := range untupledExprs {
		normExpr, err := p.analyzeExpr(expr, sources, qvals, parser.NoTypePreference, false, "")
		if err != nil {
			return nil, err
		}
		evalExprs = append(evalExprs, normExpr)
	}

	helper := &upsertHelper{
		p:                  p,
		qvals:              qvals,
		evalExprs:          evalExprs,
		sourceInfo:         sourceInfo,
		excludedSourceInfo: excludedSourceInfo,
		allExprsIdentity:   allExprsIdentity,
	}

	return helper, nil
}

func (uh *upsertHelper) expand() error {
	for _, evalExpr := range uh.evalExprs {
		if err := uh.p.expandSubqueryPlans(evalExpr); err != nil {
			return err
		}
	}
	return nil
}

func (uh *upsertHelper) start() error {
	for _, evalExpr := range uh.evalExprs {
		if err := uh.p.startSubqueryPlans(evalExpr); err != nil {
			return err
		}
	}
	return nil
}

// eval returns the values for the update case of an upsert, given the row
// that would have been inserted and the existing (conflicting) values.
func (uh *upsertHelper) eval(
	insertRow parser.DTuple, existingRow parser.DTuple,
) (parser.DTuple, error) {
	uh.qvals.populateQVals(uh.sourceInfo, existingRow)
	uh.qvals.populateQVals(uh.excludedSourceInfo, insertRow)

	var err error
	ret := make([]parser.Datum, len(uh.evalExprs))
	for i, evalExpr := range uh.evalExprs {
		ret[i], err = evalExpr.Eval(&uh.p.evalCtx)
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func (uh *upsertHelper) isIdentityEvaler() bool {
	return uh.allExprsIdentity
}

// upsertExprsAndIndex returns the upsert conflict index and the (possibly
// synthetic) SET expressions used when a row conflicts.
func upsertExprsAndIndex(
	tableDesc *sqlbase.TableDescriptor,
	onConflict parser.OnConflict,
	insertCols []sqlbase.ColumnDescriptor,
) (parser.UpdateExprs, *sqlbase.IndexDescriptor, error) {
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
		updateExprs := make(parser.UpdateExprs, 0, len(insertCols))
		for _, c := range insertCols {
			if _, ok := indexColSet[c.ID]; !ok {
				names := parser.QualifiedNames{&parser.QualifiedName{Base: parser.Name(c.Name)}}
				expr := &parser.QualifiedName{
					Base:     upsertExcludedTable,
					Indirect: parser.Indirection{parser.NameIndirection(c.Name)},
				}
				if err := expr.NormalizeColumnName(); err != nil {
					return nil, nil, err
				}
				updateExprs = append(updateExprs, &parser.UpdateExpr{Names: names, Expr: expr})
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
			if colName != onConflict.Columns[i] {
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
