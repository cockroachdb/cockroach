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

// Insert inserts rows into the database.
// Privileges: WRITE on table
//   Notes: postgres requires INSERT. No "on duplicate key update" option.
//          mysql requires INSERT. Also requires UPDATE on "ON DUPLICATE KEY UPDATE".
func (p *planner) Insert(n *parser.Insert) (planNode, error) {
	tableDesc, err := p.getTableDesc(n.Table)
	if err != nil {
		return nil, err
	}

	if err := p.checkPrivilege(tableDesc, privilege.WRITE); err != nil {
		return nil, err
	}

	// Determine which columns we're inserting into.
	cols, err := p.processColumns(tableDesc, n.Columns)
	if err != nil {
		return nil, err
	}

	// Construct a map from column ID to the index the value appears at within a
	// row.
	colIDtoRowIndex := map[structured.ColumnID]int{}
	for i, c := range cols {
		colIDtoRowIndex[c.ID] = i
	}

	// Verify we have at least the columns that are part of the primary key.
	primaryKeyCols := map[structured.ColumnID]struct{}{}
	for i, id := range tableDesc.PrimaryIndex.ColumnIDs {
		if _, ok := colIDtoRowIndex[id]; !ok {
			return nil, fmt.Errorf("missing %q primary key column", tableDesc.PrimaryIndex.ColumnNames[i])
		}
		primaryKeyCols[id] = struct{}{}
	}

	// Transform the values into a rows object. This expands SELECT statements or
	// generates rows from the values contained within the query.
	rows, err := p.makePlan(n.Rows)
	if err != nil {
		return nil, err
	}

	primaryIndex := tableDesc.PrimaryIndex
	primaryIndexKeyPrefix := structured.MakeIndexKeyPrefix(tableDesc.ID, primaryIndex.ID)

	b := client.Batch{}

	for rows.Next() {
		values := rows.Values()
		for range cols[len(values):] {
			values = append(values, parser.DNull)
		}

		for _, col := range tableDesc.Columns {
			if !col.Nullable {
				if i, ok := colIDtoRowIndex[col.ID]; !ok || values[i] == parser.DNull {
					return nil, fmt.Errorf("null value in column %q violates not-null constraint", col.Name)
				}
			}
		}

		primaryIndexKeySuffix, _, err := encodeIndexKey(primaryIndex.ColumnIDs, colIDtoRowIndex, values, nil)
		if err != nil {
			return nil, err
		}
		primaryIndexKey := bytes.Join([][]byte{primaryIndexKeyPrefix, primaryIndexKeySuffix}, nil)

		// Write the secondary indexes.
		secondaryIndexEntries, err := encodeSecondaryIndexes(tableDesc.ID, tableDesc.Indexes, colIDtoRowIndex, values, primaryIndexKeySuffix)
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
		for i, val := range values {
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

				key := structured.MakeColumnKey(col.ID, primaryIndexKey)
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
	if err := p.txn.Run(&b); err != nil {
		if tErr, ok := err.(*proto.ConditionFailedError); ok {
			return nil, fmt.Errorf("duplicate key value %q violates unique constraint %s", tErr.ActualValue.Bytes, "TODO(tamird)")
		}
		return nil, err
	}
	// TODO(tamird/pmattis): return the number of affected rows
	return &valuesNode{}, nil
}

func (p *planner) processColumns(tableDesc *structured.TableDescriptor,
	node parser.QualifiedNames) ([]structured.ColumnDescriptor, error) {
	if node == nil {
		return tableDesc.Columns, nil
	}

	cols := make([]structured.ColumnDescriptor, len(node))
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
		cols[i] = *col
	}

	return cols, nil
}
