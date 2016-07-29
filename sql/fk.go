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

	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/pkg/errors"
)

// TablesByID maps table IDs to looked up descriptors.
type TablesByID map[sqlbase.ID]*sqlbase.TableDescriptor

// FKCheck indicates a kind of FK check (delete, insert, or both).
type FKCheck int

const (
	// CheckDeletes checks if rows reference a changed value.
	CheckDeletes FKCheck = iota
	// CheckInserts checks if a new/changed value references an existing row.
	CheckInserts
	// CheckUpdates checks all references (CheckDeletes+CheckInserts).
	CheckUpdates
)

// TablesNeededForFKs calculates the IDs of the additional TableDescriptors that
// will be needed for FK checking delete and/or insert operations on `table`.
//
// NB: the returned map's values are *not* set -- higher level calling code, eg
// in planner, should fill the map's values by acquiring leases. This function
// is essentially just returning a slice of IDs, but the empty map can be filled
// in place and reused, avoiding a second allocation.
func TablesNeededForFKs(table sqlbase.TableDescriptor, usage FKCheck) TablesByID {
	var ret TablesByID
	for _, idx := range table.AllNonDropIndexes() {
		if usage != CheckDeletes && idx.ForeignKey.IsSet() {
			if ret == nil {
				ret = make(TablesByID)
			}
			ret[idx.ForeignKey.Table] = nil
		}
		if usage != CheckInserts {
			for _, idx := range table.AllNonDropIndexes() {
				for _, ref := range idx.ReferencedBy {
					if ret == nil {
						ret = make(TablesByID)
					}
					ret[ref.Table] = nil
				}
			}
		}
	}
	return ret
}

type fkInsertHelper map[sqlbase.IndexID][]baseFKHelper

var errSkipUnsedFK = errors.New("no columns involved in FK included in writer")

func makeFKInsertHelper(
	txn *client.Txn, table sqlbase.TableDescriptor, otherTables TablesByID, colMap map[sqlbase.ColumnID]int,
) (fkInsertHelper, error) {
	var fks fkInsertHelper
	for _, idx := range table.AllNonDropIndexes() {
		if idx.ForeignKey.IsSet() {
			fk, err := makeBaseFKHelper(txn, otherTables, idx, idx.ForeignKey, colMap)
			if err == errSkipUnsedFK {
				continue
			}
			if err != nil {
				return fks, err
			}
			if fks == nil {
				fks = make(fkInsertHelper)
			}
			fks[idx.ID] = append(fks[idx.ID], fk)
		}
	}
	return fks, nil
}

func (fks fkInsertHelper) checkAll(row parser.DTuple) error {
	for idx := range fks {
		if err := fks.checkIdx(idx, row); err != nil {
			return err
		}
	}
	return nil
}

func (fks fkInsertHelper) checkIdx(idx sqlbase.IndexID, row parser.DTuple) error {
	for _, fk := range fks[idx] {
		nulls := true
		for _, colID := range fk.searchIdx.ColumnIDs[:fk.prefixLen] {
			found, ok := fk.ids[colID]
			if !ok {
				panic("fk ids missing column id")
			}
			nulls = nulls && row[found] == parser.DNull
		}
		if nulls {
			continue
		}

		found, err := fk.check(row)
		if err != nil {
			return err
		}
		if found == nil {
			fkValues := make(parser.DTuple, fk.prefixLen)
			for i, colID := range fk.searchIdx.ColumnIDs[:fk.prefixLen] {
				fkValues[i] = row[fk.ids[colID]]
			}
			return fmt.Errorf("foreign key violation: value %s not found in %s@%s %s", fkValues, fk.searchTable.Name, fk.searchIdx.Name, fk.searchIdx.ColumnNames[:fk.prefixLen])
		}
	}
	return nil
}

type fkDeleteHelper map[sqlbase.IndexID][]baseFKHelper

func makeFKDeleteHelper(
	txn *client.Txn, table sqlbase.TableDescriptor, otherTables TablesByID, colMap map[sqlbase.ColumnID]int,
) (fkDeleteHelper, error) {
	var fks fkDeleteHelper
	for _, idx := range table.AllNonDropIndexes() {
		for _, ref := range idx.ReferencedBy {
			fk, err := makeBaseFKHelper(txn, otherTables, idx, ref, colMap)
			if err == errSkipUnsedFK {
				continue
			}
			if err != nil {
				return fks, err
			}
			if fks == nil {
				fks = make(fkDeleteHelper)
			}
			fks[idx.ID] = append(fks[idx.ID], fk)
		}
	}
	return fks, nil
}

func (fks fkDeleteHelper) checkAll(row parser.DTuple) error {
	for idx := range fks {
		if err := fks.checkIdx(idx, row); err != nil {
			return err
		}
	}
	return nil
}

func (fks fkDeleteHelper) checkIdx(idx sqlbase.IndexID, row parser.DTuple) error {
	for _, fk := range fks[idx] {
		found, err := fk.check(row)
		if err != nil {
			return err
		}
		if found != nil {
			if row == nil {
				return fmt.Errorf("foreign key violation: non-empty columns %s referenced in table %q",
					fk.writeIdx.ColumnNames[:fk.prefixLen], fk.searchTable.Name)
			}
			fkValues := make(parser.DTuple, fk.prefixLen)
			for i, colID := range fk.searchIdx.ColumnIDs[:fk.prefixLen] {
				fkValues[i] = row[fk.ids[colID]]
			}
			return fmt.Errorf("foreign key violation: value(s) %v in columns %s referenced in table %q",
				fkValues, fk.writeIdx.ColumnNames[:fk.prefixLen], fk.searchTable.Name)
		}

	}
	return nil
}

type fkUpdateHelper struct {
	inbound  fkDeleteHelper // Check old values are not referenced.
	outbound fkInsertHelper // Check rows referenced by new values still exist.
}

func makeFKUpdateHelper(
	txn *client.Txn, table sqlbase.TableDescriptor, otherTables TablesByID, colMap map[sqlbase.ColumnID]int,
) (fkUpdateHelper, error) {
	ret := fkUpdateHelper{}
	var err error
	if ret.inbound, err = makeFKDeleteHelper(txn, table, otherTables, colMap); err != nil {
		return ret, err
	}
	ret.outbound, err = makeFKInsertHelper(txn, table, otherTables, colMap)
	return ret, err
}

func (fks fkUpdateHelper) checkIdx(idx sqlbase.IndexID, oldValues, newValues parser.DTuple) error {
	if err := fks.inbound.checkIdx(idx, oldValues); err != nil {
		return err
	}
	return fks.outbound.checkIdx(idx, newValues)
}

type baseFKHelper struct {
	txn          *client.Txn
	rf           sqlbase.RowFetcher
	searchTable  *sqlbase.TableDescriptor // the table being searched (for err msg)
	searchIdx    *sqlbase.IndexDescriptor // the index that must (not) contain a value
	prefixLen    int
	writeIdx     sqlbase.IndexDescriptor  // the index we want to modify
	searchPrefix []byte                   // prefix of keys in searchIdx
	ids          map[sqlbase.ColumnID]int // col IDs
}

func makeBaseFKHelper(
	txn *client.Txn,
	otherTables TablesByID,
	writeIdx sqlbase.IndexDescriptor,
	ref sqlbase.ForeignKeyReference,
	colMap map[sqlbase.ColumnID]int, // col ids (for idx being written) to row offset.
) (baseFKHelper, error) {
	b := baseFKHelper{txn: txn, writeIdx: writeIdx}
	searchTable, ok := otherTables[ref.Table]
	if !ok {
		return b, errors.Errorf("referenced table %d not in provided table map %+v", ref.Table, otherTables)
	}
	b.searchTable = searchTable
	b.searchPrefix = sqlbase.MakeIndexKeyPrefix(b.searchTable, ref.Index)
	searchIdx, err := searchTable.FindIndexByID(ref.Index)
	if err != nil {
		return b, err
	}
	b.prefixLen = len(searchIdx.ColumnIDs)
	if len(writeIdx.ColumnIDs) < b.prefixLen {
		b.prefixLen = len(writeIdx.ColumnIDs)
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
	nulls := true
	for i, writeColID := range writeIdx.ColumnIDs[:b.prefixLen] {
		if found, ok := colMap[writeColID]; ok {
			b.ids[searchIdx.ColumnIDs[i]] = found
			nulls = false
		}
	}
	if nulls {
		return b, errSkipUnsedFK
	}
	return b, nil
}

// TODO(dt): Batch checks of many rows.
func (f baseFKHelper) check(values parser.DTuple) (parser.DTuple, error) {
	var key roachpb.Key
	if values != nil {
		keyBytes, _, err := sqlbase.EncodeIndexKey(
			f.searchTable, f.searchIdx, f.ids, values, f.searchPrefix)
		if err != nil {
			return nil, err
		}
		key = roachpb.Key(keyBytes)
	} else {
		key = roachpb.Key(f.searchPrefix)
	}
	spans := sqlbase.Spans{sqlbase.Span{Start: key, End: key.PrefixEnd()}}
	if err := f.rf.StartScan(f.txn, spans, 1); err != nil {
		return nil, err
	}
	return f.rf.NextRow()
}
