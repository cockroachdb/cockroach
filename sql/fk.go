// Copyright 2016 The Cockroach Authors.
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

package sql

import (
	"fmt"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
)

type fkInsertHelper []baseFKHelper

func makeFKInsertHelper(
	txn *client.Txn, table *sqlbase.TableDescriptor, colMap map[sqlbase.ColumnID]int,
) (fkInsertHelper, error) {
	fks := make(fkInsertHelper, 0, len(table.Indexes))
	for _, idx := range table.AllNonDropIndexes() {
		if idx.ForeignKey != nil {
			fk, err := makeBaseFKHelepr(txn, idx, idx.ForeignKey, colMap)
			if err != nil {
				return fks, err
			}
			fks = append(fks, fk)
		}
	}
	return fks, nil
}

func (fks fkInsertHelper) check(row parser.DTuple) error {
	for _, fk := range fks {
		found, err := fk.check(row)
		if err != nil {
			return err
		}
		if found == nil {
			fkValues := make(parser.DTuple, len(fk.searchIdx.ColumnIDs))
			for i := range fk.searchIdx.ColumnIDs {
				fkValues[i] = row[fk.ids[fk.searchIdx.ColumnIDs[i]]]
			}
			return fmt.Errorf("foreign key violation: value %s not found in %s@%s %s", fkValues, fk.searchTable.Name, fk.searchIdx.Name, fk.searchIdx.ColumnNames)
		}
	}
	return nil
}

type fkDeleteHelper []baseFKHelper

func makeFKDeleteHelper(
	txn *client.Txn, table *sqlbase.TableDescriptor, colMap map[sqlbase.ColumnID]int,
) (fkDeleteHelper, error) {
	var fks fkDeleteHelper
	for _, idx := range table.AllNonDropIndexes() {
		for _, ref := range idx.ReferencedBy {
			fk, err := makeBaseFKHelepr(txn, idx, ref, colMap)
			if err != nil {
				return fks, err
			}
			fks = append(fks, fk)
		}
	}
	return fks, nil
}

func (fks fkDeleteHelper) check(row parser.DTuple) error {
	for _, fk := range fks {
		found, err := fk.check(row)
		if err != nil {
			return err
		}
		if found != nil {
			fkValues := make(parser.DTuple, len(fk.searchIdx.ColumnIDs))
			for i := range fk.searchIdx.ColumnIDs {
				fkValues[i] = row[fk.ids[fk.searchIdx.ColumnIDs[i]]]
			}
			return fmt.Errorf("foreign key violation: value(s) %v in columns %s referenced in table %q",
				fkValues, fk.writeIdx.ColumnNames, fk.searchTable.Name)
		}
	}
	return nil
}

type baseFKHelper struct {
	txn          *client.Txn
	rf           sqlbase.RowFetcher
	searchTable  *sqlbase.TableDescriptor // the table being searched (for err msg)
	searchIdx    *sqlbase.IndexDescriptor // the index that must (not) contain a value
	writeIdx     sqlbase.IndexDescriptor  // the index we want to modify
	searchPrefix []byte                   // prefix of keys in searchIdx
	ids          map[sqlbase.ColumnID]int // col IDs
}

func makeBaseFKHelepr(
	txn *client.Txn,
	writeIdx sqlbase.IndexDescriptor,
	ref *sqlbase.TableAndIndexID,
	colMap map[sqlbase.ColumnID]int, // col ids (for idx being written) to row offset.
) (baseFKHelper, error) {
	b := baseFKHelper{txn: txn, writeIdx: writeIdx, searchPrefix: ref.IndexKeyPrefix()}
	// TODO(dt): Get a lease manager here and use that.
	searchTable, err := getTableDescFromID(txn, ref.Table)
	if err != nil {
		return b, err
	}
	b.searchTable = searchTable
	searchIdx, err := searchTable.FindIndexByID(ref.Index)
	if err != nil {
		return b, err
	}
	b.searchIdx = searchIdx
	ids := colIDtoRowIndexFromCols(searchTable.Columns)
	needed := make([]bool, len(ids))
	for _, i := range searchIdx.ColumnIDs {
		needed[ids[i]] = true
	}
	isSecondary := searchTable.PrimaryIndex.ID != searchIdx.ID
	err = b.rf.Init(searchTable, ids, searchIdx, false, isSecondary, searchTable.Columns, needed)
	if err != nil {
		return b, err
	}

	b.ids = make(map[sqlbase.ColumnID]int, len(writeIdx.ColumnIDs))
	for i := range writeIdx.ColumnIDs {
		b.ids[searchIdx.ColumnIDs[i]] = colMap[writeIdx.ColumnIDs[i]]
	}
	return b, nil
}

// TODO(dt): Batch checks of many rows.
func (f baseFKHelper) check(values parser.DTuple) (parser.DTuple, error) {
	keyBytes, _, err := sqlbase.EncodeIndexKey(f.searchIdx, f.ids, values, f.searchPrefix)
	if err != nil {
		return nil, err
	}
	key := roachpb.Key(keyBytes)

	spans := sqlbase.Spans{sqlbase.Span{Start: key, End: key.PrefixEnd()}}
	if err := f.rf.StartScan(f.txn, spans, 1); err != nil {
		return nil, err
	}
	return f.rf.NextRow()
}
