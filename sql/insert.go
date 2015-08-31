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
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util/log"
)

// TOOD(tamird): instead of tracking every write, use the failed CPut's
// key to decode the index and values.
type writePair struct {
	tableDesc *TableDescriptor
	columnID  ColumnID
	val       parser.Datum
}

type writePairs []writePair

func (wp writePairs) String() string {
	cols := make([]string, 0, len(wp))
	vals := make([]string, 0, len(wp))

	for _, pair := range wp {
		colDesc, err := pair.tableDesc.FindColumnByID(pair.columnID)
		if err != nil {
			panic(err)
		}

		cols = append(cols, colDesc.Name)
		vals = append(vals, pair.val.String())
	}

	return fmt.Sprintf("(%s)=(%s)", strings.Join(cols, ","), strings.Join(vals, ","))
}

type write struct {
	constraint *IndexDescriptor
	values     writePairs
}

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

	// Verify we have at least the columns that are part of the primary key.
	primaryKeyCols := map[ColumnID]struct{}{}
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
	primaryIndexKeyPrefix := MakeIndexKeyPrefix(tableDesc.ID, primaryIndex.ID)

	var b client.Batch
	var writes []write
	for rows.Next() {
		rowVals := rows.Values()
		for range cols[len(rowVals):] {
			rowVals = append(rowVals, parser.DNull)
		}

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

		for i, secondaryIndexEntry := range secondaryIndexEntries {
			if log.V(2) {
				log.Infof("CPut %q -> %v", secondaryIndexEntry.key, secondaryIndexEntry.value)
			}
			b.CPut(secondaryIndexEntry.key, secondaryIndexEntry.value, nil)

			var w write

			indexDesc := &tableDesc.Indexes[i]
			for _, columnID := range indexDesc.ColumnIDs {
				w.values = append(w.values, writePair{
					tableDesc: tableDesc,
					columnID:  columnID,
					val:       rowVals[colIDtoRowIndex[columnID]],
				})
			}

			w.constraint = indexDesc

			writes = append(writes, w)
		}

		// Write the row sentinel.
		if log.V(2) {
			log.Infof("CPut %q -> NULL", primaryIndexKey)
		}
		b.CPut(primaryIndexKey, nil, nil)

		var w write

		indexDesc := &tableDesc.PrimaryIndex
		for _, columnID := range indexDesc.ColumnIDs {
			w.values = append(w.values, writePair{
				tableDesc: tableDesc,
				columnID:  columnID,
				val:       rowVals[colIDtoRowIndex[columnID]],
			})
		}

		w.constraint = indexDesc

		writes = append(writes, w)

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

				writes = append(writes, write{})
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := p.txn.Run(&b); err != nil {
		for i, result := range b.Results {
			if _, ok := result.Err.(*proto.ConditionFailedError); ok {
				w := writes[i]

				return nil, fmt.Errorf("duplicate key value %s violates unique constraint %q", w.values, w.constraint.Name)
			}
		}
		return nil, err
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
