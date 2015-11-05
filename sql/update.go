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

// Update updates columns for a selection of rows from a table.
// Privileges: UPDATE and SELECT on table. We currently always use a select statement.
//   Notes: postgres requires UPDATE. Requires SELECT with WHERE clause with table.
//          mysql requires UPDATE. Also requires SELECT with WHERE clause with table.
func (p *planner) Update(n *parser.Update) (planNode, error) {
	tableDesc, err := p.getAliasedTableLease(n.Table)
	if err != nil {
		return nil, err
	}

	if err := p.checkPrivilege(tableDesc, privilege.UPDATE); err != nil {
		return nil, err
	}

	// Determine which columns we're inserting into.
	var names parser.QualifiedNames
	for _, expr := range n.Exprs {
		var err error
		expr.Expr, err = p.expandSubqueries(expr.Expr, len(expr.Names))
		if err != nil {
			return nil, err
		}

		if expr.Tuple {
			// TODO(pmattis): The distinction between Tuple and DTuple here is
			// irritating. We'll see a DTuple if the expression was a subquery that
			// has been evaluated. We'll see a Tuple in other cases.
			n := 0
			switch t := expr.Expr.(type) {
			case parser.Tuple:
				n = len(t)
			case parser.DTuple:
				n = len(t)
			default:
				return nil, util.Errorf("unsupported tuple assignment: %T", expr.Expr)
			}
			if len(expr.Names) != n {
				return nil, fmt.Errorf("number of columns (%d) does not match number of values (%d)",
					len(expr.Names), n)
			}
		}
		names = append(names, expr.Names...)
	}
	cols, err := p.processColumns(tableDesc, names)
	if err != nil {
		return nil, err
	}

	// Set of columns being updated
	colIDSet := map[ColumnID]struct{}{}
	for _, c := range cols {
		colIDSet[c.ID] = struct{}{}
	}
	// Don't allow updating any column that is part of the primary key.
	for i, id := range tableDesc.PrimaryIndex.ColumnIDs {
		if _, ok := colIDSet[id]; ok {
			return nil, fmt.Errorf("primary key column %q cannot be updated", tableDesc.PrimaryIndex.ColumnNames[i])
		}
	}

	defaultExprs, err := p.makeDefaultExprs(cols)
	if err != nil {
		return nil, err
	}

	// Generate the list of select targets. We need to select all of the columns
	// plus we select all of the update expressions in case those expressions
	// reference columns (e.g. "UPDATE t SET v = v + 1"). Note that we flatten
	// expressions for tuple assignments just as we flattened the column names
	// above. So "UPDATE t SET (a, b) = (1, 2)" translates into select targets of
	// "*, 1, 2", not "*, (1, 2)".
	targets := make(parser.SelectExprs, 0, len(n.Exprs)+1)
	targets = append(targets, parser.StarSelectExpr())
	for _, expr := range n.Exprs {
		if expr.Tuple {
			switch t := expr.Expr.(type) {
			case parser.Tuple:
				for i, e := range t {
					e, err := fillDefault(e, i, defaultExprs)
					if err != nil {
						return nil, err
					}
					targets = append(targets, parser.SelectExpr{Expr: e})
				}
			case parser.DTuple:
				for _, e := range t {
					targets = append(targets, parser.SelectExpr{Expr: e})
				}
			}
		} else {
			e, err := fillDefault(expr.Expr, 0, defaultExprs)
			if err != nil {
				return nil, err
			}
			targets = append(targets, parser.SelectExpr{Expr: e})
		}
	}

	// Query the rows that need updating.
	rows, err := p.Select(&parser.Select{
		Exprs: targets,
		From:  parser.TableExprs{n.Table},
		Where: n.Where,
		MutationColumnsReadable: true,
	})
	if err != nil {
		return nil, err
	}

	// Construct a map from column ID to the index the value appears at within a
	// row.
	colIDtoRowIndex := map[ColumnID]int{}
	for i, col := range tableDesc.Columns {
		colIDtoRowIndex[col.ID] = i
	}

	primaryIndex := tableDesc.PrimaryIndex
	primaryIndexKeyPrefix := MakeIndexKeyPrefix(tableDesc.ID, primaryIndex.ID)

	// Secondary indexes needing updating.
	var indexes []IndexDescriptor
	for _, index := range tableDesc.Indexes {
		for _, id := range index.ColumnIDs {
			if _, ok := colIDSet[id]; ok {
				indexes = append(indexes, index)
				break
			}
		}
	}

	marshalled := make([]interface{}, len(cols))

	b := client.Batch{}
	result := &valuesNode{}
	for rows.Next() {
		rowVals := rows.Values()
		result.rows = append(result.rows, parser.DTuple(nil))

		primaryIndexKey, _, err := encodeIndexKey(
			primaryIndex.ColumnIDs, colIDtoRowIndex, rowVals, primaryIndexKeyPrefix)
		if err != nil {
			return nil, err
		}
		// Compute the current secondary index key:value pairs for this row.
		secondaryIndexEntries, err := encodeSecondaryIndexes(
			tableDesc.ID, indexes, colIDtoRowIndex, rowVals)
		if err != nil {
			return nil, err
		}

		// Our updated value expressions occur immediately after the plain
		// columns in the output.
		newVals := rowVals[len(tableDesc.Columns):]
		// Update the row values.
		for i, col := range cols {
			val := newVals[i]
			if !col.Nullable && val == parser.DNull {
				return nil, fmt.Errorf("null value in column %q violates not-null constraint", col.Name)
			}
			rowVals[colIDtoRowIndex[col.ID]] = val
		}

		// Check that the new value types match the column types. This needs to
		// happen before index encoding because certain datum types (i.e. tuple)
		// cannot be used as index values.
		for i, val := range newVals {
			// Only marshal writable columns.
			if !cols[i].isWritable() {
				continue
			}
			var err error
			if marshalled[i], err = marshalColumnValue(cols[i], val); err != nil {
				return nil, err
			}
		}

		// Compute the new secondary index key:value pairs for this row.
		newSecondaryIndexEntries, err := encodeSecondaryIndexes(
			tableDesc.ID, indexes, colIDtoRowIndex, rowVals)
		if err != nil {
			return nil, err
		}

		// Update secondary indexes.
		for i, newSecondaryIndexEntry := range newSecondaryIndexEntries {
			secondaryIndexEntry := secondaryIndexEntries[i]
			if !bytes.Equal(newSecondaryIndexEntry.key, secondaryIndexEntry.key) {
				if log.V(2) {
					log.Infof("CPut %s -> %v", prettyKey(newSecondaryIndexEntry.key, 0),
						newSecondaryIndexEntry.value)
				}
				b.CPut(newSecondaryIndexEntry.key, newSecondaryIndexEntry.value, nil)
				if log.V(2) {
					log.Infof("Del %s", prettyKey(secondaryIndexEntry.key, 0))
				}
				b.Del(secondaryIndexEntry.key)
			}
		}

		// Add the new values.
		for i, val := range newVals {
			col := cols[i]

			key := MakeColumnKey(col.ID, primaryIndexKey)
			if marshalled[i] != nil {
				// We only output non-NULL values. Non-existent column keys are
				// considered NULL during scanning and the row sentinel ensures we know
				// the row exists.
				if log.V(2) {
					log.Infof("Put %s -> %v", prettyKey(key, 0), val)
				}

				b.Put(key, marshalled[i])
			} else {
				// The column might have already existed but is being set to NULL, so
				// delete it.
				if log.V(2) {
					log.Infof("Del %s", prettyKey(key, 0))
				}

				b.Del(key)
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := p.txn.Run(&b); err != nil {
		return nil, convertBatchError(tableDesc, b, err)
	}

	return result, nil
}

func fillDefault(expr parser.Expr, index int, defaultExprs []parser.Expr) (parser.Expr, error) {
	switch expr.(type) {
	case parser.DefaultVal:
		return defaultExprs[index], nil
	}
	return expr, nil
}
