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
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util/log"
)

// Delete deletes rows from a table.
// Privileges: DELETE and SELECT on table. We currently always use a SELECT statement.
//   Notes: postgres requires DELETE. Also requires SELECT for "USING" and "WHERE" with tables.
//          mysql requires DELETE. Also requires SELECT if a table is used in the "WHERE" clause.
func (p *planner) Delete(n *parser.Delete) (planNode, *roachpb.Error) {
	tableDesc, pErr := p.getAliasedTableLease(n.Table)
	if pErr != nil {
		return nil, pErr
	}

	if pErr := p.checkPrivilege(tableDesc, privilege.DELETE); pErr != nil {
		return nil, pErr
	}

	// TODO(tamird,pmattis): avoid going through Select to avoid encoding
	// and decoding keys.
	rows, pErr := p.Select(&parser.Select{
		Exprs: tableDesc.allColumnsSelector(),
		From:  parser.TableExprs{n.Table},
		Where: n.Where,
	})
	if pErr != nil {
		return nil, pErr
	}

	// Construct a map from column ID to the index the value appears at within a
	// row.
	colIDtoRowIndex, pErr := makeColIDtoRowIndex(rows, tableDesc)
	if pErr != nil {
		return nil, pErr
	}

	primaryIndex := tableDesc.PrimaryIndex
	primaryIndexKeyPrefix := MakeIndexKeyPrefix(tableDesc.ID, primaryIndex.ID)

	b := client.Batch{}
	result := &valuesNode{}
	for rows.Next() {
		rowVals := rows.Values()
		result.rows = append(result.rows, parser.DTuple(nil))

		primaryIndexKey, _, pErr := encodeIndexKey(
			primaryIndex.ColumnIDs, colIDtoRowIndex, rowVals, primaryIndexKeyPrefix)
		if pErr != nil {
			return nil, pErr
		}

		// Delete the secondary indexes.
		indexes := tableDesc.Indexes
		// Also include all the indexes under mutation; mutation state is
		// irrelevant for deletions.
		for _, m := range tableDesc.Mutations {
			if index := m.GetIndex(); index != nil {
				indexes = append(indexes, *index)
			}
		}
		secondaryIndexEntries, pErr := encodeSecondaryIndexes(
			tableDesc.ID, indexes, colIDtoRowIndex, rowVals)
		if pErr != nil {
			return nil, pErr
		}

		for _, secondaryIndexEntry := range secondaryIndexEntries {
			if log.V(2) {
				log.Infof("Del %s", secondaryIndexEntry.key)
			}
			b.Del(secondaryIndexEntry.key)
		}

		// Delete the row.
		rowStartKey := roachpb.Key(primaryIndexKey)
		rowEndKey := rowStartKey.PrefixEnd()
		if log.V(2) {
			log.Infof("DelRange %s - %s", rowStartKey, rowEndKey)
		}
		b.DelRange(rowStartKey, rowEndKey)
	}

	if pErr := rows.PErr(); pErr != nil {
		return nil, pErr
	}

	if isSystemConfigID(tableDesc.GetID()) {
		// Mark transaction as operating on the system DB.
		p.txn.SetSystemConfigTrigger()
	}
	if pErr := p.txn.Run(&b); pErr != nil {
		return nil, pErr
	}

	return result, nil
}
