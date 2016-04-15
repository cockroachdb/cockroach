// Copyright 2015 The Cockroach Authors.
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
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"fmt"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// Insert inserts rows into the database.
// Privileges: INSERT on table
//   Notes: postgres requires INSERT. No "on duplicate key update" option.
//          mysql requires INSERT. Also requires UPDATE on "ON DUPLICATE KEY UPDATE".
func (p *planner) Insert(n *parser.Insert, autoCommit bool) (planNode, *roachpb.Error) {
	// TODO(marcb): We can't use the cached descriptor here because a recent
	// update of the schema (e.g. the addition of an index) might not be
	// reflected in the cached version (yet). Perhaps schema modification
	// routines such as CREATE INDEX should not return until the schema change
	// has been pushed everywhere.
	tableDesc, pErr := p.getTableLease(n.Table)
	if pErr != nil {
		return nil, pErr
	}

	if err := p.checkPrivilege(&tableDesc, privilege.INSERT); err != nil {
		return nil, roachpb.NewError(err)
	}

	var cols []ColumnDescriptor
	// Determine which columns we're inserting into.
	if n.DefaultValues() {
		cols = tableDesc.Columns
	} else {
		var err error
		if cols, err = p.processColumns(&tableDesc, n.Columns); err != nil {
			return nil, roachpb.NewError(err)
		}
	}
	// Number of columns expecting an input. This doesn't include the
	// columns receiving a default value.
	numInputColumns := len(cols)

	// Construct a map from column ID to the index the value appears at within a
	// row.
	colIDtoRowIndex := map[ColumnID]int{}
	for i, c := range cols {
		colIDtoRowIndex[c.ID] = i
	}

	// Add the column if it has a DEFAULT expression.
	addIfDefault := func(col ColumnDescriptor) {
		if col.DefaultExpr != nil {
			if _, ok := colIDtoRowIndex[col.ID]; !ok {
				colIDtoRowIndex[col.ID] = len(cols)
				cols = append(cols, col)
			}
		}
	}

	// Add any column that has a DEFAULT expression.
	for _, col := range tableDesc.Columns {
		addIfDefault(col)
	}
	// Also add any column in a mutation that is WRITE_ONLY and has
	// a DEFAULT expression.
	for _, m := range tableDesc.Mutations {
		if m.State != DescriptorMutation_WRITE_ONLY {
			continue
		}
		if col := m.GetColumn(); col != nil {
			addIfDefault(*col)
		}
	}

	// Verify we have at least the columns that are part of the primary key.
	primaryKeyCols := map[ColumnID]struct{}{}
	for i, id := range tableDesc.PrimaryIndex.ColumnIDs {
		if _, ok := colIDtoRowIndex[id]; !ok {
			return nil, roachpb.NewUErrorf("missing %q primary key column", tableDesc.PrimaryIndex.ColumnNames[i])
		}
		primaryKeyCols[id] = struct{}{}
	}

	// Construct the default expressions. The returned slice will be nil if no
	// column in the table has a default expression.
	defaultExprs, err := makeDefaultExprs(cols, &p.parser, p.evalCtx)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	// Replace any DEFAULT markers with the corresponding default expressions.
	insertRows := p.fillDefaults(defaultExprs, cols, n)

	// Construct the check expressions. The returned slice will be nil if no
	// column in the table has a check expression.
	checkExprs, err := p.makeCheckExprs(cols)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	// Prepare the check expressions.
	var qvals qvalMap
	if len(checkExprs) > 0 {
		qvals = make(qvalMap)
		table := tableInfo{
			columns: makeResultColumns(tableDesc.Columns),
		}
		for i := range checkExprs {
			expr, err := resolveQNames(checkExprs[i], &table, qvals, &p.qnameVisitor)
			if err != nil {
				return nil, roachpb.NewError(err)
			}
			checkExprs[i] = expr
		}
	}
	// Transform the values into a rows object. This expands SELECT statements or
	// generates rows from the values contained within the query.
	rows, pErr := p.makePlan(insertRows, false)
	if pErr != nil {
		return nil, pErr
	}

	if expressions := len(rows.Columns()); expressions > numInputColumns {
		return nil, roachpb.NewUErrorf("INSERT has more expressions than target columns: %d/%d", expressions, numInputColumns)
	}

	primaryIndex := tableDesc.PrimaryIndex
	primaryIndexKeyPrefix := MakeIndexKeyPrefix(tableDesc.ID, primaryIndex.ID)

	marshalled := make([]interface{}, len(cols))

	b := p.txn.NewBatch()
	rh, err := makeReturningHelper(p, n.Returning, tableDesc.Name, tableDesc.Columns)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	// Prepare structures for building values to pass to rh.
	var retVals parser.DTuple
	var rowIdxToRetIdx []int
	if rh.exprs != nil {
		// In some cases (e.g. `INSERT INTO t (a) ...`) rowVals does not contain all the table
		// columns. We need to pass values for all table columns to rh, in the correct order; we
		// will use retVals for this. We also need a table that maps row indices to retVals indices
		// to fill in the row values; any absent values will be NULLs.

		retVals = make(parser.DTuple, len(tableDesc.Columns))
		for i := range retVals {
			retVals[i] = parser.DNull
		}

		colIDToRetIndex := map[ColumnID]int{}
		for i, col := range tableDesc.Columns {
			colIDToRetIndex[col.ID] = i
		}

		rowIdxToRetIdx = make([]int, len(cols))
		for i, col := range cols {
			rowIdxToRetIdx[i] = colIDToRetIndex[col.ID]
		}
	}

	for rows.Next() {
		rowVals := rows.Values()

		// The values for the row may be shorter than the number of columns being
		// inserted into. Generate default values for those columns using the
		// default expressions.
		for i := len(rowVals); i < len(cols); i++ {
			if defaultExprs == nil {
				rowVals = append(rowVals, parser.DNull)
				continue
			}
			d, err := defaultExprs[i].Eval(p.evalCtx)
			if err != nil {
				return nil, roachpb.NewError(err)
			}
			rowVals = append(rowVals, d)
		}

		// Check to see if NULL is being inserted into any non-nullable column.
		for _, col := range tableDesc.Columns {
			if !col.Nullable {
				if i, ok := colIDtoRowIndex[col.ID]; !ok || rowVals[i] == parser.DNull {
					return nil, roachpb.NewUErrorf("null value in column %q violates not-null constraint", col.Name)
				}
			}
		}

		// Ensure that the values honor the specified column widths.
		for i := range rowVals {
			if err := checkValueWidth(cols[i], rowVals[i]); err != nil {
				return nil, roachpb.NewError(err)
			}
		}

		if len(checkExprs) > 0 {
			// Populate qvals.
			for ref, qval := range qvals {
				// The colIdx is 0-based, we need to change it to 1-based.
				ri, has := colIDtoRowIndex[ColumnID(ref.colIdx+1)]
				if !has {
					return nil, roachpb.NewUErrorf("failed to to find column %d in row", ColumnID(ref.colIdx+1))
				}
				qval.datum = rowVals[ri]
			}
			for _, expr := range checkExprs {
				if d, err := expr.Eval(p.evalCtx); err != nil {
					return nil, roachpb.NewError(err)
				} else if res, err := parser.GetBool(d); err != nil {
					return nil, roachpb.NewError(err)
				} else if !res {
					// Failed to satisfy CHECK constraint.
					return nil, roachpb.NewUErrorf("failed to satisfy CHECK constraint (%s)", expr.String())
				}
			}
		}

		// Check that the row value types match the column types. This needs to
		// happen before index encoding because certain datum types (i.e. tuple)
		// cannot be used as index values.
		for i, val := range rowVals {
			// Make sure the value can be written to the column before proceeding.
			var mErr error
			if marshalled[i], mErr = marshalColumnValue(cols[i], val, p.evalCtx.Args); mErr != nil {
				return nil, roachpb.NewError(mErr)
			}
		}

		if p.evalCtx.PrepareOnly {
			continue
		}

		primaryIndexKey, _, eErr := encodeIndexKey(
			&primaryIndex, colIDtoRowIndex, rowVals, primaryIndexKeyPrefix)
		if eErr != nil {
			return nil, roachpb.NewError(eErr)
		}

		// Write the row sentinel. We want to write the sentinel first in case
		// we are trying to insert a duplicate primary key: if we write the
		// secondary indexes first, we may get an error that looks like a
		// uniqueness violation on a non-unique index.
		sentinelKey := keys.MakeNonColumnKey(primaryIndexKey)
		if log.V(2) {
			log.Infof("CPut %s -> NULL", roachpb.Key(sentinelKey))
		}
		// This is subtle: An interface{}(nil) deletes the value, so we pass in
		// []byte{} as a non-nil value.
		b.CPut(sentinelKey, []byte{}, nil)

		// Write the secondary indexes.
		indexes := tableDesc.Indexes
		// Also include the secondary indexes in mutation state WRITE_ONLY.
		for _, m := range tableDesc.Mutations {
			if m.State == DescriptorMutation_WRITE_ONLY {
				if index := m.GetIndex(); index != nil {
					indexes = append(indexes, *index)
				}
			}
		}
		secondaryIndexEntries, eErr := encodeSecondaryIndexes(
			tableDesc.ID, indexes, colIDtoRowIndex, rowVals)
		if eErr != nil {
			return nil, roachpb.NewError(eErr)
		}

		for _, secondaryIndexEntry := range secondaryIndexEntries {
			if log.V(2) {
				log.Infof("CPut %s -> %v", secondaryIndexEntry.key,
					secondaryIndexEntry.value)
			}
			b.CPut(secondaryIndexEntry.key, secondaryIndexEntry.value, nil)
		}

		// Write the row columns.
		for i, val := range rowVals {
			col := cols[i]
			if retVals != nil {
				retVals[rowIdxToRetIdx[i]] = val
			}

			if _, ok := primaryKeyCols[col.ID]; ok {
				// Skip primary key columns as their values are encoded in the row
				// sentinel key which is guaranteed to exist for as long as the row
				// exists.
				continue
			}

			if marshalled[i] != nil {
				// We only output non-NULL values. Non-existent column keys are
				// considered NULL during scanning and the row sentinel ensures we know
				// the row exists.

				key := keys.MakeColumnKey(primaryIndexKey, uint32(col.ID))
				if log.V(2) {
					log.Infof("CPut %s -> %v", roachpb.Key(key), val)
				}

				b.CPut(key, marshalled[i], nil)
			}
		}

		if err := rh.append(retVals); err != nil {
			return nil, roachpb.NewError(err)
		}
	}
	if pErr := rows.PErr(); pErr != nil {
		return nil, pErr
	}

	if p.evalCtx.PrepareOnly {
		// Return the result column types.
		return rh.getResults()
	}

	if isSystemConfigID(tableDesc.GetID()) {
		// Mark transaction as operating on the system DB.
		p.txn.SetSystemConfigTrigger()
	}

	if autoCommit {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		pErr = p.txn.CommitInBatch(b)
	} else {
		pErr = p.txn.Run(b)
	}
	if pErr != nil {
		return nil, convertBatchError(&tableDesc, *b, pErr)
	}
	return rh.getResults()
}

func (p *planner) processColumns(tableDesc *TableDescriptor,
	node parser.QualifiedNames) ([]ColumnDescriptor, error) {
	if node == nil {
		// VisibleColumns is used here to prevent INSERT INTO <table> VALUES (...)
		// (as opposed to INSERT INTO <table> (...) VALUES (...)) from writing
		// hidden columns. At present, the only hidden column is the implicit rowid
		// primary key column.
		return tableDesc.VisibleColumns(), nil
	}

	cols := make([]ColumnDescriptor, len(node))
	colIDSet := make(map[ColumnID]struct{}, len(node))
	for i, n := range node {
		// TODO(pmattis): If the name is qualified, verify the table name matches
		// tableDesc.Name.
		if err := n.NormalizeColumnName(); err != nil {
			return nil, err
		}
		col, err := tableDesc.FindActiveColumnByName(n.Column())
		if err != nil {
			return nil, err
		}
		if _, ok := colIDSet[col.ID]; ok {
			return nil, fmt.Errorf("multiple assignments to same column \"%s\"", n.Column())
		}
		colIDSet[col.ID] = struct{}{}
		cols[i] = col
	}

	return cols, nil
}

func (p *planner) fillDefaults(defaultExprs []parser.Expr,
	cols []ColumnDescriptor, n *parser.Insert) parser.SelectStatement {
	if n.DefaultValues() {
		row := make(parser.Exprs, 0, len(cols))
		for i := range cols {
			if defaultExprs == nil {
				row = append(row, parser.DNull)
				continue
			}
			row = append(row, defaultExprs[i])
		}
		return &parser.ValuesClause{Tuples: []*parser.Tuple{{Exprs: row}}}
	}

	values, ok := n.Rows.Select.(*parser.ValuesClause)
	if !ok {
		return n.Rows.Select
	}

	ret := values
	for tIdx, tuple := range values.Tuples {
		tupleCopied := false
		for eIdx, val := range tuple.Exprs {
			switch val.(type) {
			case parser.DefaultVal:
				if !tupleCopied {
					if ret == values {
						ret = &parser.ValuesClause{Tuples: append([]*parser.Tuple(nil), values.Tuples...)}
					}
					ret.Tuples[tIdx] =
						&parser.Tuple{Exprs: append([]parser.Expr(nil), tuple.Exprs...)}
					tupleCopied = true
				}
				if defaultExprs == nil {
					ret.Tuples[tIdx].Exprs[eIdx] = parser.DNull
				} else {
					ret.Tuples[tIdx].Exprs[eIdx] = defaultExprs[eIdx]
				}
			}
		}
	}
	return ret
}

func makeDefaultExprs(
	cols []ColumnDescriptor, parse *parser.Parser, evalCtx parser.EvalContext,
) ([]parser.Expr, error) {
	// Check to see if any of the columns have DEFAULT expressions. If there
	// are no DEFAULT expressions, we don't bother with constructing the
	// defaults map as the defaults are all NULL.
	haveDefaults := false
	for _, col := range cols {
		if col.DefaultExpr != nil {
			haveDefaults = true
			break
		}
	}
	if !haveDefaults {
		return nil, nil
	}

	// Build the default expressions map from the parsed SELECT statement.
	defaultExprs := make([]parser.Expr, 0, len(cols))
	for _, col := range cols {
		if col.DefaultExpr == nil {
			defaultExprs = append(defaultExprs, parser.DNull)
			continue
		}
		expr, err := parser.ParseExprTraditional(*col.DefaultExpr)
		if err != nil {
			return nil, err
		}
		expr, err = parse.NormalizeExpr(evalCtx, expr)
		if err != nil {
			return nil, err
		}
		if parser.ContainsVars(expr) {
			return nil, util.Errorf("default expression contains variables")
		}
		defaultExprs = append(defaultExprs, expr)
	}
	return defaultExprs, nil
}

func (p *planner) makeCheckExprs(cols []ColumnDescriptor) ([]parser.Expr, error) {
	// Check to see if any of the columns have CHECK expressions. If there are
	// no CHECK expressions, we don't bother with constructing it.
	numCheck := 0
	for _, col := range cols {
		if col.CheckExpr != nil {
			numCheck++
			break
		}
	}
	if numCheck == 0 {
		return nil, nil
	}

	checkExprs := make([]parser.Expr, 0, numCheck)
	for _, col := range cols {
		if col.CheckExpr == nil {
			continue
		}
		expr, err := parser.ParseExprTraditional(*col.CheckExpr)
		if err != nil {
			return nil, err
		}
		expr, err = p.parser.NormalizeExpr(p.evalCtx, expr)
		if err != nil {
			return nil, err
		}
		checkExprs = append(checkExprs, expr)
	}
	return checkExprs, nil
}
