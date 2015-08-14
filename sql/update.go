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
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util/log"
)

// Update updates columns for a selection of rows from a table.
// Privileges: WRITE and READ on table. We currently always use a select statement.
//   Notes: postgres requires UPDATE. Requires SELECT with WHERE clause with table.
//          mysql requires UPDATE. Also requires SELECT with WHERE clause with table.
func (p *planner) Update(n *parser.Update) (planNode, error) {
	tableDesc, err := p.getAliasedTableDesc(n.Table)
	if err != nil {
		return nil, err
	}

	if err := p.checkPrivilege(tableDesc, privilege.WRITE); err != nil {
		return nil, err
	}

	// Determine which columns we're inserting into.
	var names parser.QualifiedNames
	for _, expr := range n.Exprs {
		names = append(names, expr.Name)
	}
	cols, err := p.processColumns(tableDesc, names)
	if err != nil {
		return nil, err
	}

	// Set of columns being updated
	colIDSet := map[structured.ColumnID]struct{}{}
	for _, c := range cols {
		colIDSet[c.ID] = struct{}{}
	}
	// Don't allow updating any column that is part of the primary key.
	for i, id := range tableDesc.PrimaryIndex.ColumnIDs {
		if _, ok := colIDSet[id]; ok {
			return nil, fmt.Errorf("primary key column %q cannot be updated", tableDesc.PrimaryIndex.ColumnNames[i])
		}
	}

	// Query the rows that need updating.
	// TODO(vivek): Avoid going through Select.
	row, err := p.Select(&parser.Select{
		Exprs: parser.SelectExprs{parser.StarSelectExpr},
		From:  parser.TableExprs{n.Table},
		Where: n.Where,
	})
	if err != nil {
		return nil, err
	}

	// Construct a map from column ID to the index the value appears at within a
	// row.
	colIDtoRowIndex := map[structured.ColumnID]int{}
	for i, name := range row.Columns() {
		c, err := tableDesc.FindColumnByName(name)
		if err != nil {
			return nil, err
		}
		colIDtoRowIndex[c.ID] = i
	}

	primaryIndex := tableDesc.PrimaryIndex
	primaryIndexKeyPrefix := structured.MakeIndexKeyPrefix(tableDesc.ID, primaryIndex.ID)

	// Evaluate all the column value expressions.
	vals := make([]parser.Datum, 0, 10)
	for _, expr := range n.Exprs {
		val, err := parser.EvalExpr(expr.Expr)
		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
	}

	// Secondary indexes needing updating.
	var indexes []structured.IndexDescriptor
	for _, index := range tableDesc.Indexes {
		for _, id := range index.ColumnIDs {
			if _, ok := colIDSet[id]; ok {
				indexes = append(indexes, index)
				break
			}
		}
	}

	// Update all the rows.
	b := client.Batch{}
	for row.Next() {
		rowVals := row.Values()
		primaryIndexKeySuffix, _, err := encodeIndexKey(primaryIndex.ColumnIDs, colIDtoRowIndex, rowVals, nil)
		if err != nil {
			return nil, err
		}
		primaryIndexKey := bytes.Join([][]byte{primaryIndexKeyPrefix, primaryIndexKeySuffix}, nil)
		// Compute the current secondary index key:value pairs for this row.
		secondaryIndexEntries, err := encodeSecondaryIndexes(tableDesc.ID, indexes, colIDtoRowIndex, rowVals, primaryIndexKeySuffix)
		if err != nil {
			return nil, err
		}
		// Compute the new secondary index key:value pairs for this row.
		//
		// Update the row values.
		for i, col := range cols {
			val := vals[i]

			if !col.Nullable && val == parser.DNull {
				return nil, fmt.Errorf("null value in column %q violates not-null constraint", col.Name)
			}
			rowVals[colIDtoRowIndex[col.ID]] = val
		}
		newSecondaryIndexEntries, err := encodeSecondaryIndexes(tableDesc.ID, indexes, colIDtoRowIndex, rowVals, primaryIndexKeySuffix)
		if err != nil {
			return nil, err
		}
		// Update secondary indexes.
		for i, newSecondaryIndexEntry := range newSecondaryIndexEntries {
			secondaryIndexEntry := secondaryIndexEntries[i]
			if !bytes.Equal(newSecondaryIndexEntry.key, secondaryIndexEntry.key) {
				if log.V(2) {
					log.Infof("CPut %q -> %v", newSecondaryIndexEntry.key, newSecondaryIndexEntry.value)
				}
				b.CPut(newSecondaryIndexEntry.key, newSecondaryIndexEntry.value, nil)
				if log.V(2) {
					log.Infof("Del %q", secondaryIndexEntry.key)
				}
				b.Del(secondaryIndexEntry.key)
			}
		}

		// Add the new values.
		for i, val := range vals {
			col := cols[i]

			primitive, err := convertDatum(col, val)
			if err != nil {
				return nil, err
			}

			key := structured.MakeColumnKey(col.ID, primaryIndexKey)
			if primitive != nil {
				// We only output non-NULL values. Non-existent column keys are
				// considered NULL during scanning and the row sentinel ensures we know
				// the row exists.
				if log.V(2) {
					log.Infof("Put %q -> %v", key, val)
				}

				b.Put(key, primitive)
			} else {
				// The column might have already existed but is being set to NULL, so
				// delete it.
				if log.V(2) {
					log.Infof("Del %q", key)
				}

				b.Del(key)
			}
		}
	}

	if err := row.Err(); err != nil {
		return nil, err
	}

	if err := p.txn.Run(&b); err != nil {
		if tErr, ok := err.(*proto.ConditionFailedError); ok {
			return nil, fmt.Errorf("duplicate key value %q violates unique constraint %s", tErr.ActualValue.Bytes, "TODO(tamird)")
		}
		return nil, err
	}

	// TODO(tamird/pmattis): return the number of affected rows.
	return &valuesNode{}, nil
}
