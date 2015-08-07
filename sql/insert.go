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

	if !tableDesc.HasPrivilege(p.user, parser.PrivilegeWrite) {
		return nil, fmt.Errorf("user %s does not have %s privilege on table %s",
			p.user, parser.PrivilegeWrite, tableDesc.Name)
	}

	// Determine which columns we're inserting into.
	cols, err := p.processColumns(tableDesc, n.Columns)
	if err != nil {
		return nil, err
	}

	// Construct a map from column ID to the index the value appears at within a
	// row.
	colIDtoRowIndex := map[structured.ID]int{}
	for i, c := range cols {
		colIDtoRowIndex[c.ID] = i
	}

	// Verify we have at least the columns that are part of the primary key.
	for i, id := range tableDesc.PrimaryIndex.ColumnIDs {
		if _, ok := colIDtoRowIndex[id]; !ok {
			return nil, fmt.Errorf("missing %q primary key column", tableDesc.PrimaryIndex.ColumnNames[i])
		}
	}

	// Transform the values into a rows object. This expands SELECT statements or
	// generates rows from the values contained within the query.
	rows, err := p.makePlan(n.Rows)
	if err != nil {
		return nil, err
	}

	primaryIndex := tableDesc.PrimaryIndex
	primaryIndexKeyPrefix := encodeIndexKeyPrefix(tableDesc.ID, primaryIndex.ID)

	b := client.Batch{}

	for rows.Next() {
		values := rows.Values()
		if len(values) != len(cols) {
			return nil, fmt.Errorf("invalid values for columns: %d != %d", len(values), len(cols))
		}

		primaryIndexKeySuffix, err := encodeIndexKey(primaryIndex.ColumnIDs, colIDtoRowIndex, values, nil)
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

		// Write the row.
		for i, val := range values {
			key := encodeColumnKey(cols[i], primaryIndexKey)
			if log.V(2) {
				log.Infof("CPut %q -> %v", key, val)
			}
			v, err := prepareVal(cols[i], val)
			if err != nil {
				return nil, err
			}
			b.CPut(key, v, nil)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := p.db.Run(&b); err != nil {
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
		var err error
		col, err := tableDesc.FindColumnByName(n.Column())
		if err != nil {
			return nil, err
		}
		cols[i] = *col
	}

	return cols, nil
}
