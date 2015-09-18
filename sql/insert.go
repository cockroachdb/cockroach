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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// Insert inserts rows into the database.
// Privileges: INSERT on table
//   Notes: postgres requires INSERT. No "on duplicate key update" option.
//          mysql requires INSERT. Also requires UPDATE on "ON DUPLICATE KEY UPDATE".
func (p *planner) Insert(n *parser.Insert) (planNode, error) {
	tableDesc, err := p.getTableDesc(n.Table)
	if err != nil {
		return nil, err
	}

	if err := p.checkPrivilege(tableDesc, privilege.INSERT); err != nil {
		return nil, err
	}

	// Determine which columns we're inserting into.
	cols, err := p.processColumns(tableDesc, n.Columns)
	if err != nil {
		return nil, err
	}

	// Construct a map from column ID to the index the value appears at within a
	// row.
	colIDtoRowIndex := map[ColumnID]int{}
	for i, c := range cols {
		colIDtoRowIndex[c.ID] = i
	}

	// Add any column not already present that has a DEFAULT expression.
	for _, col := range tableDesc.Columns {
		if _, ok := colIDtoRowIndex[col.ID]; ok {
			continue
		}
		if col.DefaultExpr != nil {
			colIDtoRowIndex[col.ID] = len(cols)
			cols = append(cols, col)
		}
	}

	// Verify we have at least the columns that are part of the primary key.
	primaryKeyCols := map[ColumnID]struct{}{}
	for i, id := range tableDesc.PrimaryIndex.ColumnIDs {
		if _, ok := colIDtoRowIndex[id]; !ok {
			return nil, fmt.Errorf("missing %q primary key column", tableDesc.PrimaryIndex.ColumnNames[i])
		}
		primaryKeyCols[id] = struct{}{}
	}

	// Construct the default expressions. The returned slice will be nil if no
	// column in the table has a default expression.
	defaultExprs, err := makeDefaultExprs(cols)
	if err != nil {
		return nil, err
	}

	// Replace any DEFAULT markers with the corresponding default expressions.
	if n.Rows, err = p.fillDefaults(defaultExprs, cols, n.Rows); err != nil {
		return nil, err
	}

	// Transform the values into a rows object. This expands SELECT statements or
	// generates rows from the values contained within the query.
	rows, err := p.makePlan(n.Rows)
	if err != nil {
		return nil, err
	}

	primaryIndex := tableDesc.PrimaryIndex
	primaryIndexKeyPrefix := MakeIndexKeyPrefix(tableDesc.ID, primaryIndex.ID)

	var b client.Batch
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
			d, err := parser.EvalExpr(defaultExprs[i])
			if err != nil {
				return nil, err
			}
			rowVals = append(rowVals, d)
		}

		// Check to see if NULL is being inserted into any non-nullable column.
		for _, col := range tableDesc.Columns {
			if !col.Nullable {
				if i, ok := colIDtoRowIndex[col.ID]; !ok || rowVals[i] == parser.DNull {
					return nil, fmt.Errorf("null value in column %q violates not-null constraint", col.Name)
				}
			}
		}

		primaryIndexKey, _, err := encodeIndexKey(
			primaryIndex.ColumnIDs, colIDtoRowIndex, rowVals, primaryIndexKeyPrefix)
		if err != nil {
			return nil, err
		}

		// Write the secondary indexes.
		secondaryIndexEntries, err := encodeSecondaryIndexes(
			tableDesc.ID, tableDesc.Indexes, colIDtoRowIndex, rowVals)
		if err != nil {
			return nil, err
		}

		for _, secondaryIndexEntry := range secondaryIndexEntries {
			if log.V(2) {
				log.Infof("CPut %q -> %v", secondaryIndexEntry.key, secondaryIndexEntry.value)
			}
			b.CPut(secondaryIndexEntry.key, secondaryIndexEntry.value, nil)
		}

		// Write the row sentinel.
		if log.V(2) {
			log.Infof("CPut %q -> NULL", primaryIndexKey)
		}
		b.CPut(primaryIndexKey, nil, nil)

		// Write the row columns.
		for i, val := range rowVals {
			col := cols[i]

			// Make sure the value can be written to the column before proceeding.
			primitive, err := convertDatum(col, val)
			if err != nil {
				return nil, err
			}

			if _, ok := primaryKeyCols[col.ID]; ok {
				// Skip primary key columns as their values are encoded in the row
				// sentinel key which is guaranteed to exist for as long as the row
				// exists.
				continue
			}

			if primitive != nil {
				// We only output non-NULL values. Non-existent column keys are
				// considered NULL during scanning and the row sentinel ensures we know
				// the row exists.

				key := MakeColumnKey(col.ID, primaryIndexKey)
				if log.V(2) {
					log.Infof("CPut %q -> %v", key, primitive)
				}

				b.CPut(key, primitive, nil)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if IsSystemID(tableDesc.GetID()) {
		// Mark transaction as operating on the system DB.
		p.txn.SetSystemDBTrigger()
	}
	if err := p.txn.Run(&b); err != nil {
		return nil, convertBatchError(tableDesc, b, err)
	}
	// TODO(tamird/pmattis): return the number of affected rows
	return &valuesNode{}, nil
}

func (p *planner) processColumns(tableDesc *TableDescriptor,
	node parser.QualifiedNames) ([]ColumnDescriptor, error) {
	if node == nil {
		return tableDesc.Columns, nil
	}

	cols := make([]ColumnDescriptor, len(node))
	colIDSet := make(map[ColumnID]struct{}, len(node))
	for i, n := range node {
		// TODO(pmattis): If the name is qualified, verify the table name matches
		// tableDesc.Name.
		if err := n.NormalizeColumnName(); err != nil {
			return nil, err
		}
		col, err := tableDesc.FindColumnByName(n.Column())
		if err != nil {
			return nil, err
		}
		if _, ok := colIDSet[col.ID]; ok {
			return nil, fmt.Errorf("multiple assignments to same column \"%s\"", n.Column())
		}
		colIDSet[col.ID] = struct{}{}
		cols[i] = *col
	}

	return cols, nil
}

func (p *planner) fillDefaults(defaultExprs []parser.Expr,
	cols []ColumnDescriptor, rows parser.SelectStatement) (parser.SelectStatement, error) {
	switch values := rows.(type) {
	case nil:
		// This indicates a DEFAULT VALUES expression.
		row := make(parser.Tuple, 0, len(cols))
		for i := range cols {
			if defaultExprs == nil {
				row = append(row, parser.DNull)
				continue
			}
			row = append(row, defaultExprs[i])
		}
		return parser.Values{row}, nil

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
	return rows, nil
}

func makeDefaultExprs(cols []ColumnDescriptor) ([]parser.Expr, error) {
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

	// Construct a SELECT statement with an output for each column. The output is
	// either the DEFAULT expression or NULL if no default expression was
	// configured.
	var buf bytes.Buffer
	buf.WriteString("SELECT ")
	for i, col := range cols {
		if i > 0 {
			buf.WriteString(", ")
		}
		if col.DefaultExpr != nil {
			buf.WriteString(*col.DefaultExpr)
		} else {
			buf.WriteString("NULL")
		}
	}

	sql := buf.String()
	stmts, err := parser.ParseTraditional(sql)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, util.Errorf("expected 1 statement, but found %d", len(stmts))
	}
	sel, ok := stmts[0].(*parser.Select)
	if !ok {
		return nil, util.Errorf("expected a SELECT statement, but found %T", stmts[0])
	}
	if n, m := len(cols), len(sel.Exprs); n != m {
		return nil, util.Errorf("expected %d outputs, but found %d", n, m)
	}

	// Build the default expressions map from the parsed SELECT statement.
	defaultExprs := make([]parser.Expr, 0, len(cols))
	for i := range cols {
		expr, err := parser.NormalizeExpr(sel.Exprs[i].Expr)
		if err != nil {
			return nil, err
		}
		defaultExprs = append(defaultExprs, expr)
	}
	return defaultExprs, nil
}
