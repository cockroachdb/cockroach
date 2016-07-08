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
// Author: XisiHuang (cockhuangxh@163.com)

package sql

import (
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/log"
)

// Truncate deletes all rows from a table.
// Privileges: DROP on table.
//   Notes: postgres requires TRUNCATE.
//          mysql requires DROP (for mysql >= 5.1.16, DELETE before that).
func (p *planner) Truncate(n *parser.Truncate) (planNode, error) {
	for _, tableQualifiedName := range n.Tables {
		tableDesc, err := p.getTableLease(tableQualifiedName)
		if err != nil {
			return nil, err
		}

		if err := p.checkPrivilege(&tableDesc, privilege.DROP); err != nil {
			return nil, err
		}

		if err := truncateTableInTxn(&tableDesc, p.txn); err != nil {
			return nil, err
		}

		fkTables := TablesNeededForFKs(tableDesc, CheckDeletes)
		if err := p.fillFKTableMap(fkTables); err != nil {
			return nil, err
		}
		colMap := colIDtoRowIndexFromCols(tableDesc.Columns)
		if helper, err := makeFKDeleteHelper(p.txn, tableDesc, fkTables, colMap); err != nil {
			return nil, err
		} else if err = helper.checkAll(nil); err != nil {
			return nil, err
		}
	}

	return &emptyNode{}, nil
}

// TableTruncateChunkSize is the size of each chunk during table truncation.
const TableTruncateChunkSize = 2000

// truncateTable truncates the data of a table in many chunks.
// It deletes a range of data for the table, which includes the PK and all
// indexes.
func truncateTable(tableDesc *sqlbase.TableDescriptor, db *client.DB) error {
	tablePrefix := keys.MakeTablePrefix(uint32(tableDesc.ID))

	// Delete rows and indexes starting with the table's prefix.
	tableStartKey := roachpb.Key(tablePrefix)
	tableEndKey := tableStartKey.PrefixEnd()
	if log.V(2) {
		log.Infof("DelRange %s - %s", tableStartKey, tableEndKey)
	}

	for done := false; !done; {
		if err := db.Txn(func(txn *client.Txn) error {
			b := client.Batch{}
			b.DelRange(tableStartKey, tableEndKey, TableTruncateChunkSize, false)
			if err := txn.Run(&b); err != nil {
				return err
			}
			// The table has been completed truncated once no keys are
			// returned.
			if len(b.Results) != 1 {
				panic("incorrect number of results returned")
			}
			if len(b.Results[0].Keys) == 0 {
				done = true
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// truncateTableInTxn truncates the data of a table in a single transaction.
// It deletes a range of data for the table, which includes the PK and all
// indexes.
//
// TODO(vivek): fix #2003
func truncateTableInTxn(tableDesc *sqlbase.TableDescriptor, txn *client.Txn) error {
	tablePrefix := keys.MakeTablePrefix(uint32(tableDesc.ID))

	// Delete rows and indexes starting with the table's prefix.
	tableStartKey := roachpb.Key(tablePrefix)
	tableEndKey := tableStartKey.PrefixEnd()
	if log.V(2) {
		log.Infof("DelRange %s - %s", tableStartKey, tableEndKey)
	}
	b := client.Batch{}
	b.DelRange(tableStartKey, tableEndKey, 0, false)
	return txn.Run(&b)
}
