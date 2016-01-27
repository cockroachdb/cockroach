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
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
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

	if pErr := p.checkPrivilege(tableDesc, privilege.INSERT); pErr != nil {
		return nil, pErr
	}

	var cols []ColumnDescriptor
	// Determine which columns we're inserting into.
	if n.DefaultValues() {
		cols = tableDesc.Columns
	} else {
		if cols, pErr = p.processColumns(tableDesc, n.Columns); pErr != nil {
			return nil, pErr
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
	defaultExprs, pErr := p.makeDefaultExprs(cols)
	if pErr != nil {
		return nil, pErr
	}

	// Replace any DEFAULT markers with the corresponding default expressions.
	if n.Rows, pErr = p.fillDefaults(defaultExprs, cols, n); pErr != nil {
		return nil, pErr
	}

	// Transform the values into a rows object. This expands SELECT statements or
	// generates rows from the values contained within the query.
	rows, pErr := p.makePlan(n.Rows, false)
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
	result := &valuesNode{}
	for rows.Next() {
		rowVals := rows.Values()
		result.rows = append(result.rows, parser.DTuple(nil))

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

		// Check that the row value types match the column types. This needs to
		// happen before index encoding because certain datum types (i.e. tuple)
		// cannot be used as index values.
		for i, val := range rowVals {
			// Make sure the value can be written to the column before proceeding.
			var mpErr *roachpb.Error
			if marshalled[i], mpErr = marshalColumnValue(cols[i], val); mpErr != nil {
				return nil, mpErr
			}
		}

		primaryIndexKey, _, epErr := encodeIndexKey(
			&primaryIndex, colIDtoRowIndex, rowVals, primaryIndexKeyPrefix)
		if epErr != nil {
			return nil, epErr
		}

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
		secondaryIndexEntries, epErr := encodeSecondaryIndexes(
			tableDesc.ID, indexes, colIDtoRowIndex, rowVals)
		if epErr != nil {
			return nil, epErr
		}

		for _, secondaryIndexEntry := range secondaryIndexEntries {
			if log.V(2) {
				log.Infof("CPut %s -> %v", secondaryIndexEntry.key,
					secondaryIndexEntry.value)
			}
			b.CPut(secondaryIndexEntry.key, secondaryIndexEntry.value, nil)
		}

		// Write the row sentinel.
		sentinelKey := keys.MakeNonColumnKey(primaryIndexKey)
		if log.V(2) {
			log.Infof("CPut %s -> NULL", roachpb.Key(sentinelKey))
		}
		// This is subtle: An interface{}(nil) deletes the value, so we pass in
		// []byte{} as a non-nil value.
		b.CPut(sentinelKey, []byte{}, nil)

		// Write the row columns.
		for i, val := range rowVals {
			col := cols[i]
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
	}
	if pErr := rows.PErr(); pErr != nil {
		return nil, pErr
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
		return nil, convertBatchError(tableDesc, *b, pErr)
	}
	return result, nil
}

func (p *planner) processColumns(tableDesc *TableDescriptor,
	node parser.QualifiedNames) ([]ColumnDescriptor, *roachpb.Error) {
	if node == nil {
		// VisibleColumns is used here to prevent INSERT ... VALUES (...) from
		// writing "hidden" columns. At present, the only hidden column is the
		// implicit rowid primary key.
		return tableDesc.VisibleColumns(), nil
	}

	cols := make([]ColumnDescriptor, len(node))
	colIDSet := make(map[ColumnID]struct{}, len(node))
	for i, n := range node {
		// TODO(pmattis): If the name is qualified, verify the table name matches
		// tableDesc.Name.
		if err := n.NormalizeColumnName(); err != nil {
			return nil, roachpb.NewError(err)
		}
		col, pErr := tableDesc.FindActiveColumnByName(n.Column())
		if pErr != nil {
			return nil, pErr
		}
		if _, ok := colIDSet[col.ID]; ok {
			return nil, roachpb.NewUErrorf("multiple assignments to same column \"%s\"", n.Column())
		}
		colIDSet[col.ID] = struct{}{}
		cols[i] = col
	}

	return cols, nil
}

func (p *planner) fillDefaults(defaultExprs []parser.Expr,
	cols []ColumnDescriptor, n *parser.Insert) (parser.SelectStatement, *roachpb.Error) {
	if n.DefaultValues() {
		row := make(parser.Tuple, 0, len(cols))
		for i := range cols {
			if defaultExprs == nil {
				row = append(row, parser.DNull)
				continue
			}
			row = append(row, defaultExprs[i])
		}
		return parser.Values{row}, nil
	}

	switch values := n.Rows.(type) {
	case parser.Values:
		for _, tuple := range values {
			for i, val := range tuple {
				switch val.(type) {
				case parser.DefaultVal:
					if defaultExprs == nil {
						tuple[i] = parser.DNull
						continue
					}
					tuple[i] = defaultExprs[i]
				}
			}
		}
	}
	return n.Rows, nil
}

func (p *planner) makeDefaultExprs(cols []ColumnDescriptor) ([]parser.Expr, *roachpb.Error) {
	// Check to see if any of the columns have DEFAULT expressions. If there are
	// no DEFAULT expressions, we don't bother with constructing the defaults map
	// as the defaults are all NULL.
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
			return nil, roachpb.NewError(err)
		}
		expr, err = p.parser.NormalizeExpr(p.evalCtx, expr)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		if parser.ContainsVars(expr) {
			return nil, roachpb.NewErrorf("default expression contains variables")
		}
		defaultExprs = append(defaultExprs, expr)
	}
	return defaultExprs, nil
}
