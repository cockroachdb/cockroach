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
// Author: Daniel Harrison (daniel.harrison@gmail.com)

package sql

import (
	"bytes"
	"fmt"
	"sort"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/util/encoding"

	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/pkg/errors"
)

const (
	checkFKs = true
	skipFKs  = false
)

// rowHelper has the common methods for table row manipulations.
type rowHelper struct {
	tableDesc    *sqlbase.TableDescriptor
	indexes      []sqlbase.IndexDescriptor
	indexEntries []sqlbase.IndexEntry

	// Computed and cached.
	primaryIndexKeyPrefix []byte
	primaryIndexCols      map[sqlbase.ColumnID]struct{}
	sortedColumnFamilies  map[sqlbase.FamilyID][]sqlbase.ColumnID
}

// encodeIndexes encodes the primary and secondary index keys. The
// secondaryIndexEntries are only valid until the next call to encodeIndexes or
// encodeSecondaryIndexes.
func (rh *rowHelper) encodeIndexes(
	colIDtoRowIndex map[sqlbase.ColumnID]int, values []parser.Datum,
) (
	primaryIndexKey []byte,
	secondaryIndexEntries []sqlbase.IndexEntry,
	err error,
) {
	if rh.primaryIndexKeyPrefix == nil {
		rh.primaryIndexKeyPrefix = sqlbase.MakeIndexKeyPrefix(rh.tableDesc,
			rh.tableDesc.PrimaryIndex.ID)
	}
	primaryIndexKey, _, err = sqlbase.EncodeIndexKey(
		rh.tableDesc, &rh.tableDesc.PrimaryIndex, colIDtoRowIndex, values, rh.primaryIndexKeyPrefix)
	if err != nil {
		return nil, nil, err
	}
	secondaryIndexEntries, err = rh.encodeSecondaryIndexes(colIDtoRowIndex, values)
	if err != nil {
		return nil, nil, err
	}
	return primaryIndexKey, secondaryIndexEntries, nil
}

// encodeSecondaryIndexes encodes the secondary index keys. The
// secondaryIndexEntries are only valid until the next call to encodeIndexes or
// encodeSecondaryIndexes.
func (rh *rowHelper) encodeSecondaryIndexes(
	colIDtoRowIndex map[sqlbase.ColumnID]int, values []parser.Datum,
) (
	secondaryIndexEntries []sqlbase.IndexEntry,
	err error,
) {
	if len(rh.indexEntries) != len(rh.indexes) {
		rh.indexEntries = make([]sqlbase.IndexEntry, len(rh.indexes))
	}
	err = sqlbase.EncodeSecondaryIndexes(
		rh.tableDesc, rh.indexes, colIDtoRowIndex, values, rh.indexEntries)
	if err != nil {
		return nil, err
	}
	return rh.indexEntries, nil
}

// TODO(dan): This logic is common and being moved into sqlbase.TableDescriptor (see
// #6233). Once it is, use the shared one.
func (rh *rowHelper) columnInPK(colID sqlbase.ColumnID) bool {
	if rh.primaryIndexCols == nil {
		rh.primaryIndexCols = make(map[sqlbase.ColumnID]struct{})
		for _, colID := range rh.tableDesc.PrimaryIndex.ColumnIDs {
			rh.primaryIndexCols[colID] = struct{}{}
		}
	}
	_, ok := rh.primaryIndexCols[colID]
	return ok
}

type columnIDs []sqlbase.ColumnID

func (c columnIDs) Len() int           { return len(c) }
func (c columnIDs) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c columnIDs) Less(i, j int) bool { return c[i] < c[j] }

func (rh *rowHelper) sortedColumnFamily(famID sqlbase.FamilyID) ([]sqlbase.ColumnID, bool) {
	if rh.sortedColumnFamilies == nil {
		rh.sortedColumnFamilies = make(map[sqlbase.FamilyID][]sqlbase.ColumnID, len(rh.tableDesc.Families))
		for _, family := range rh.tableDesc.Families {
			colIDs := append([]sqlbase.ColumnID(nil), family.ColumnIDs...)
			sort.Sort(columnIDs(colIDs))
			rh.sortedColumnFamilies[family.ID] = colIDs
		}
	}
	colIDs, ok := rh.sortedColumnFamilies[famID]
	return colIDs, ok
}

// rowInserter abstracts the key/value operations for inserting table rows.
type rowInserter struct {
	helper                rowHelper
	insertCols            []sqlbase.ColumnDescriptor
	insertColIDtoRowIndex map[sqlbase.ColumnID]int
	fks                   fkInsertHelper

	// For allocation avoidance.
	marshalled []roachpb.Value
	key        roachpb.Key
	valueBuf   []byte
	value      roachpb.Value
}

// makeRowInserter creates a rowInserter for the given table.
//
// insertCols must contain every column in the primary key.
func makeRowInserter(
	txn *client.Txn,
	tableDesc *sqlbase.TableDescriptor,
	fkTables TablesByID,
	insertCols []sqlbase.ColumnDescriptor,
	checkFKs bool,
) (rowInserter, error) {
	indexes := tableDesc.Indexes
	// Also include the secondary indexes in mutation state WRITE_ONLY.
	for _, m := range tableDesc.Mutations {
		if m.State == sqlbase.DescriptorMutation_WRITE_ONLY {
			if index := m.GetIndex(); index != nil {
				indexes = append(indexes, *index)
			}
		}
	}

	ri := rowInserter{
		helper:                rowHelper{tableDesc: tableDesc, indexes: indexes},
		insertCols:            insertCols,
		insertColIDtoRowIndex: colIDtoRowIndexFromCols(insertCols),
		marshalled:            make([]roachpb.Value, len(insertCols)),
	}

	for i, col := range tableDesc.PrimaryIndex.ColumnIDs {
		if _, ok := ri.insertColIDtoRowIndex[col]; !ok {
			return rowInserter{}, fmt.Errorf("missing %q primary key column", tableDesc.PrimaryIndex.ColumnNames[i])
		}
	}

	if checkFKs {
		var err error
		if ri.fks, err = makeFKInsertHelper(txn, *tableDesc, fkTables, ri.insertColIDtoRowIndex); err != nil {
			return ri, err
		}
	}
	return ri, nil
}

// insertCPutFn is used by insertRow when conflicts should be respected.
// logValue is used for pretty printing.
func insertCPutFn(ctx context.Context, b *client.Batch, key *roachpb.Key, value *roachpb.Value) {
	// TODO(dan): We want do this V(2) log everywhere in sql. Consider making a
	// client.Batch wrapper instead of inlining it everywhere.
	if log.V(2) {
		log.InfofDepth(ctx, 1, "CPut %s -> %s", *key, value.PrettyPrint())
	}
	b.CPut(key, value, nil)
}

// insertPutFn is used by insertRow when conflicts should be ignored.
// logValue is used for pretty printing.
func insertPutFn(ctx context.Context, b *client.Batch, key *roachpb.Key, value *roachpb.Value) {
	if log.V(2) {
		log.InfofDepth(ctx, 1, "Put %s -> %s", *key, value.PrettyPrint())
	}
	b.Put(key, value)
}

// insertRow adds to the batch the kv operations necessary to insert a table row
// with the given values.
func (ri *rowInserter) insertRow(
	ctx context.Context, b *client.Batch, values []parser.Datum, ignoreConflicts bool,
) error {
	if len(values) != len(ri.insertCols) {
		return errors.Errorf("got %d values but expected %d", len(values), len(ri.insertCols))
	}

	putFn := insertCPutFn
	if ignoreConflicts {
		putFn = insertPutFn
	}

	// Encode the values to the expected column type. This needs to
	// happen before index encoding because certain datum types (i.e. tuple)
	// cannot be used as index values.
	for i, val := range values {
		// Make sure the value can be written to the column before proceeding.
		var err error
		if ri.marshalled[i], err = sqlbase.MarshalColumnValue(ri.insertCols[i], val); err != nil {
			return err
		}
	}

	if err := ri.fks.checkAll(values); err != nil {
		return err
	}

	primaryIndexKey, secondaryIndexEntries, err := ri.helper.encodeIndexes(ri.insertColIDtoRowIndex, values)
	if err != nil {
		return err
	}

	// Add the new values.
	// TODO(dan): This has gotten very similar to the loop in updateRow, see if
	// they can be DRY'd. Ideally, this would also work for
	// truncateAndBackfillColumnsChunk, which is currently abusing rowUdpdater.
	for i, family := range ri.helper.tableDesc.Families {
		if i > 0 {
			// HACK: MakeFamilyKey appends to its argument, so on every loop iteration
			// after the first, trim primaryIndexKey so nothing gets overwritten.
			// TODO(dan): Instead of this, use something like engine.ChunkAllocator.
			primaryIndexKey = primaryIndexKey[:len(primaryIndexKey):len(primaryIndexKey)]
		}

		if len(family.ColumnIDs) == 1 && family.ColumnIDs[0] == family.DefaultColumnID {
			// Storage optimization to store DefaultColumnID directly as a value. Also
			// backwards compatible with the original BaseFormatVersion.

			idx, ok := ri.insertColIDtoRowIndex[family.DefaultColumnID]
			if !ok {
				continue
			}

			if ri.marshalled[idx].RawBytes != nil {
				// We only output non-NULL values. Non-existent column keys are
				// considered NULL during scanning and the row sentinel ensures we know
				// the row exists.

				ri.key = keys.MakeFamilyKey(primaryIndexKey, uint32(family.ID))
				putFn(ctx, b, &ri.key, &ri.marshalled[idx])
				ri.key = nil
			}

			continue
		}

		ri.key = keys.MakeFamilyKey(primaryIndexKey, uint32(family.ID))
		ri.valueBuf = ri.valueBuf[:0]

		var lastColID sqlbase.ColumnID
		familySortedColumnIDs, ok := ri.helper.sortedColumnFamily(family.ID)
		if !ok {
			panic("invalid family sorted column id map")
		}
		for _, colID := range familySortedColumnIDs {
			if ri.helper.columnInPK(colID) {
				if family.ID != 0 {
					return errors.Errorf("primary index column %d must be in family 0, was %d", colID, family.ID)
				}
				// Skip primary key columns as their values are encoded in the key of
				// each family. Family 0 is guaranteed to exist and acts as a sentinel.
				continue
			}

			idx, ok := ri.insertColIDtoRowIndex[colID]
			if !ok {
				// Column not being inserted.
				continue
			}
			col := ri.insertCols[idx]

			if values[idx].Compare(parser.DNull) == 0 {
				continue
			}

			if lastColID > col.ID {
				panic(fmt.Errorf("cannot write column id %d after %d", col.ID, lastColID))
			}
			colIDDiff := col.ID - lastColID
			lastColID = col.ID
			ri.valueBuf, err = sqlbase.EncodeTableValue(ri.valueBuf, colIDDiff, values[idx])
			if err != nil {
				return err
			}
		}

		if family.ID == 0 || len(ri.valueBuf) > 0 {
			ri.value.SetTuple(ri.valueBuf)
			putFn(ctx, b, &ri.key, &ri.value)
		}

		ri.key = nil
	}

	for i := range secondaryIndexEntries {
		e := &secondaryIndexEntries[i]
		putFn(ctx, b, &e.Key, &e.Value)
	}

	return nil
}

// rowUpdater abstracts the key/value operations for updating table rows.
type rowUpdater struct {
	helper                rowHelper
	fetchCols             []sqlbase.ColumnDescriptor
	fetchColIDtoRowIndex  map[sqlbase.ColumnID]int
	updateCols            []sqlbase.ColumnDescriptor
	updateColIDtoRowIndex map[sqlbase.ColumnID]int
	deleteOnlyIndex       map[int]struct{}
	primaryKeyColChange   bool

	rd rowDeleter
	ri rowInserter

	fks fkUpdateHelper

	// For allocation avoidance.
	marshalled      []roachpb.Value
	newValues       []parser.Datum
	key             roachpb.Key
	indexEntriesBuf []sqlbase.IndexEntry
	valueBuf        []byte
	value           roachpb.Value
}

type rowUpdaterType int

const (
	rowUpdaterDefault     rowUpdaterType = 0
	rowUpdaterOnlyColumns rowUpdaterType = 1
)

// makeRowUpdater creates a rowUpdater for the given table.
//
// updateCols are the columns being updated and correspond to the updateValues
// that will be passed to updateRow.
//
// The returned rowUpdater contains a fetchCols field that defines the
// expectation of which values are passed as oldValues to updateRow. Any column
// passed in requestedCols will be included in fetchCols.
func makeRowUpdater(
	txn *client.Txn,
	tableDesc *sqlbase.TableDescriptor,
	fkTables TablesByID,
	updateCols []sqlbase.ColumnDescriptor,
	requestedCols []sqlbase.ColumnDescriptor,
	updateType rowUpdaterType,
) (rowUpdater, error) {
	updateColIDtoRowIndex := colIDtoRowIndexFromCols(updateCols)

	primaryIndexCols := make(map[sqlbase.ColumnID]struct{}, len(tableDesc.PrimaryIndex.ColumnIDs))
	for _, colID := range tableDesc.PrimaryIndex.ColumnIDs {
		primaryIndexCols[colID] = struct{}{}
	}

	var primaryKeyColChange bool
	for _, c := range updateCols {
		if _, ok := primaryIndexCols[c.ID]; ok {
			primaryKeyColChange = true
			break
		}
	}

	// Secondary indexes needing updating.
	needsUpdate := func(index sqlbase.IndexDescriptor) bool {
		if updateType == rowUpdaterOnlyColumns {
			// Only update columns.
			return false
		}
		// If the primary key changed, we need to update all of them.
		if primaryKeyColChange {
			return true
		}
		for _, id := range index.ColumnIDs {
			if _, ok := updateColIDtoRowIndex[id]; ok {
				return true
			}
		}
		return false
	}

	indexes := make([]sqlbase.IndexDescriptor, 0, len(tableDesc.Indexes)+len(tableDesc.Mutations))
	for _, index := range tableDesc.Indexes {
		if needsUpdate(index) {
			indexes = append(indexes, index)
		}
	}

	var deleteOnlyIndex map[int]struct{}
	for _, m := range tableDesc.Mutations {
		if index := m.GetIndex(); index != nil {
			if needsUpdate(*index) {
				indexes = append(indexes, *index)

				switch m.State {
				case sqlbase.DescriptorMutation_DELETE_ONLY:
					if deleteOnlyIndex == nil {
						// Allocate at most once.
						deleteOnlyIndex = make(map[int]struct{}, len(tableDesc.Mutations))
					}
					deleteOnlyIndex[len(indexes)-1] = struct{}{}

				case sqlbase.DescriptorMutation_WRITE_ONLY:
				}
			}
		}
	}

	ru := rowUpdater{
		helper:                rowHelper{tableDesc: tableDesc, indexes: indexes},
		updateCols:            updateCols,
		updateColIDtoRowIndex: updateColIDtoRowIndex,
		deleteOnlyIndex:       deleteOnlyIndex,
		primaryKeyColChange:   primaryKeyColChange,
		marshalled:            make([]roachpb.Value, len(updateCols)),
		newValues:             make([]parser.Datum, len(tableDesc.Columns)+len(tableDesc.Mutations)),
	}

	if primaryKeyColChange {
		// These fields are only used when the primary key is changing.
		var err error
		// When changing the primary key, we delete the old values and reinsert
		// them, so request them all.
		if ru.rd, err = makeRowDeleter(txn, tableDesc, fkTables, tableDesc.Columns, skipFKs); err != nil {
			return rowUpdater{}, err
		}
		ru.fetchCols = ru.rd.fetchCols
		ru.fetchColIDtoRowIndex = colIDtoRowIndexFromCols(ru.fetchCols)
		if ru.ri, err = makeRowInserter(txn, tableDesc, fkTables, tableDesc.Columns, skipFKs); err != nil {
			return rowUpdater{}, err
		}
	} else {
		ru.fetchCols = requestedCols[:len(requestedCols):len(requestedCols)]
		ru.fetchColIDtoRowIndex = colIDtoRowIndexFromCols(ru.fetchCols)

		maybeAddCol := func(colID sqlbase.ColumnID) error {
			if _, ok := ru.fetchColIDtoRowIndex[colID]; !ok {
				col, err := tableDesc.FindColumnByID(colID)
				if err != nil {
					return err
				}
				ru.fetchColIDtoRowIndex[col.ID] = len(ru.fetchCols)
				ru.fetchCols = append(ru.fetchCols, *col)
			}
			return nil
		}
		for _, colID := range tableDesc.PrimaryIndex.ColumnIDs {
			if err := maybeAddCol(colID); err != nil {
				return rowUpdater{}, err
			}
		}
		for _, fam := range tableDesc.Families {
			familyBeingUpdated := false
			for _, colID := range fam.ColumnIDs {
				if _, ok := ru.updateColIDtoRowIndex[colID]; ok {
					familyBeingUpdated = true
					break
				}
			}
			if familyBeingUpdated {
				for _, colID := range fam.ColumnIDs {
					if err := maybeAddCol(colID); err != nil {
						return rowUpdater{}, err
					}
				}
			}
		}
		for _, index := range indexes {
			for _, colID := range index.ColumnIDs {
				if err := maybeAddCol(colID); err != nil {
					return rowUpdater{}, err
				}
			}
		}
	}

	var err error
	if ru.fks, err = makeFKUpdateHelper(txn, *tableDesc, fkTables, ru.fetchColIDtoRowIndex); err != nil {
		return rowUpdater{}, err
	}
	return ru, nil
}

// updateRow adds to the batch the kv operations necessary to update a table row
// with the given values.
//
// The row corresponding to oldValues is updated with the ones in updateValues.
// Note that updateValues only contains the ones that are changing.
//
// The return value is only good until the next call to UpdateRow.
func (ru *rowUpdater) updateRow(
	ctx context.Context,
	b *client.Batch,
	oldValues []parser.Datum,
	updateValues []parser.Datum,
) ([]parser.Datum, error) {
	if len(oldValues) != len(ru.fetchCols) {
		return nil, errors.Errorf("got %d values but expected %d", len(oldValues), len(ru.fetchCols))
	}
	if len(updateValues) != len(ru.updateCols) {
		return nil, errors.Errorf("got %d values but expected %d", len(updateValues), len(ru.updateCols))
	}

	primaryIndexKey, secondaryIndexEntries, err := ru.helper.encodeIndexes(ru.fetchColIDtoRowIndex, oldValues)
	if err != nil {
		return nil, err
	}

	// The secondary index entries returned by rowHelper.encodeIndexes are only
	// valid until the next call to encodeIndexes. We need to copy them so that
	// we can compare against the new secondary index entries.
	secondaryIndexEntries = append(ru.indexEntriesBuf[:0], secondaryIndexEntries...)
	ru.indexEntriesBuf = secondaryIndexEntries

	// Check that the new value types match the column types. This needs to
	// happen before index encoding because certain datum types (i.e. tuple)
	// cannot be used as index values.
	for i, val := range updateValues {
		if ru.marshalled[i], err = sqlbase.MarshalColumnValue(ru.updateCols[i], val); err != nil {
			return nil, err
		}
	}

	// Update the row values.
	copy(ru.newValues, oldValues)
	for i, updateCol := range ru.updateCols {
		ru.newValues[ru.fetchColIDtoRowIndex[updateCol.ID]] = updateValues[i]
	}

	rowPrimaryKeyChanged := false
	var newSecondaryIndexEntries []sqlbase.IndexEntry
	if ru.primaryKeyColChange {
		var newPrimaryIndexKey []byte
		newPrimaryIndexKey, newSecondaryIndexEntries, err =
			ru.helper.encodeIndexes(ru.fetchColIDtoRowIndex, ru.newValues)
		if err != nil {
			return nil, err
		}
		rowPrimaryKeyChanged = !bytes.Equal(primaryIndexKey, newPrimaryIndexKey)
	} else {
		newSecondaryIndexEntries, err =
			ru.helper.encodeSecondaryIndexes(ru.fetchColIDtoRowIndex, ru.newValues)
		if err != nil {
			return nil, err
		}
	}

	if rowPrimaryKeyChanged {
		if err := ru.fks.checkIdx(ru.helper.tableDesc.PrimaryIndex.ID, oldValues, ru.newValues); err != nil {
			return nil, err
		}
		for i := range newSecondaryIndexEntries {
			if !bytes.Equal(newSecondaryIndexEntries[i].Key, secondaryIndexEntries[i].Key) {
				if err := ru.fks.checkIdx(ru.helper.indexes[i].ID, oldValues, ru.newValues); err != nil {
					return nil, err
				}
			}
		}

		if err := ru.rd.deleteRow(ctx, b, oldValues); err != nil {
			return nil, err
		}
		if err := ru.ri.insertRow(ctx, b, ru.newValues, false); err != nil {
			return nil, err
		}
		return ru.newValues, nil
	}

	// Add the new values.
	// TODO(dan): This has gotten very similar to the loop in insertRow, see if
	// they can be DRY'd. Ideally, this would also work for
	// truncateAndBackfillColumnsChunk, which is currently abusing rowUdpdater.
	for i, family := range ru.helper.tableDesc.Families {
		update := false
		for _, colID := range family.ColumnIDs {
			if _, ok := ru.updateColIDtoRowIndex[colID]; ok {
				update = true
				break
			}
		}
		if !update {
			continue
		}

		if i > 0 {
			// HACK: MakeFamilyKey appends to its argument, so on every loop iteration
			// after the first, trim primaryIndexKey so nothing gets overwritten.
			// TODO(dan): Instead of this, use something like engine.ChunkAllocator.
			primaryIndexKey = primaryIndexKey[:len(primaryIndexKey):len(primaryIndexKey)]
		}

		if len(family.ColumnIDs) == 1 && family.ColumnIDs[0] == family.DefaultColumnID {
			// Storage optimization to store DefaultColumnID directly as a value. Also
			// backwards compatible with the original BaseFormatVersion.

			idx, ok := ru.updateColIDtoRowIndex[family.DefaultColumnID]
			if !ok {
				continue
			}

			ru.key = keys.MakeFamilyKey(primaryIndexKey, uint32(family.ID))
			if log.V(2) {
				log.Infof(ctx, "Put %s -> %v", ru.key, ru.marshalled[idx].PrettyPrint())
			}
			b.Put(&ru.key, &ru.marshalled[idx])
			ru.key = nil

			continue
		}

		ru.key = keys.MakeFamilyKey(primaryIndexKey, uint32(family.ID))
		ru.valueBuf = ru.valueBuf[:0]

		var lastColID sqlbase.ColumnID
		familySortedColumnIDs, ok := ru.helper.sortedColumnFamily(family.ID)
		if !ok {
			panic("invalid family sorted column id map")
		}
		for _, colID := range familySortedColumnIDs {
			if ru.helper.columnInPK(colID) {
				if family.ID != 0 {
					return nil, errors.Errorf("primary index column %d must be in family 0, was %d", colID, family.ID)
				}
				// Skip primary key columns as their values are encoded in the key of
				// each family. Family 0 is guaranteed to exist and acts as a sentinel.
				continue
			}

			idx, ok := ru.fetchColIDtoRowIndex[colID]
			if !ok {
				return nil, errors.Errorf("column %d was expected to be fetched, but wasn't", colID)
			}
			col := ru.fetchCols[idx]

			if ru.newValues[idx].Compare(parser.DNull) == 0 {
				continue
			}

			if lastColID > col.ID {
				panic(fmt.Errorf("cannot write column id %d after %d", col.ID, lastColID))
			}
			colIDDiff := col.ID - lastColID
			lastColID = col.ID
			ru.valueBuf, err = sqlbase.EncodeTableValue(ru.valueBuf, colIDDiff, ru.newValues[idx])
			if err != nil {
				return nil, err
			}
		}

		if family.ID != 0 && len(ru.valueBuf) == 0 {
			// The family might have already existed but every column in it is being
			// set to NULL, so delete it.
			if log.V(2) {
				log.Infof(ctx, "Del %s", ru.key)
			}

			b.Del(&ru.key)
		} else {
			ru.value.SetTuple(ru.valueBuf)
			if log.V(2) {
				log.Infof(ctx, "Put %s -> %v", ru.key, ru.value.PrettyPrint())
			}
			b.Put(&ru.key, &ru.value)
		}

		ru.key = nil
	}

	// Update secondary indexes.
	for i, newSecondaryIndexEntry := range newSecondaryIndexEntries {
		secondaryIndexEntry := secondaryIndexEntries[i]
		secondaryKeyChanged := !bytes.Equal(newSecondaryIndexEntry.Key, secondaryIndexEntry.Key)
		if secondaryKeyChanged {
			if err := ru.fks.checkIdx(ru.helper.indexes[i].ID, oldValues, ru.newValues); err != nil {
				return nil, err
			}

			if log.V(2) {
				log.Infof(ctx, "Del %s", secondaryIndexEntry.Key)
			}
			b.Del(secondaryIndexEntry.Key)
			// Do not update Indexes in the DELETE_ONLY state.
			if _, ok := ru.deleteOnlyIndex[i]; !ok {
				if log.V(2) {
					log.Infof(ctx, "CPut %s -> %v", newSecondaryIndexEntry.Key, newSecondaryIndexEntry.Value.PrettyPrint())
				}
				b.CPut(newSecondaryIndexEntry.Key, &newSecondaryIndexEntry.Value, nil)
			}
		}
	}

	return ru.newValues, nil
}

// isColumnOnlyUpdate returns true if this rowUpdater is only updating column
// data (in contrast to updating the primary key or other indexes).
func (ru *rowUpdater) isColumnOnlyUpdate() bool {
	// TODO(dan): This is used in the schema change backfill to assert that it was
	// configured correctly and will not be doing things it shouldn't. This is an
	// unfortunate bleeding of responsibility and indicates the abstraction could
	// be improved. Specifically, rowUpdater currently has two responsibilities
	// (computing which indexes need to be updated and mapping sql rows to k/v
	// operations) and these should be split.
	return !ru.primaryKeyColChange && len(ru.deleteOnlyIndex) == 0 && len(ru.helper.indexes) == 0
}

// rowDeleter abstracts the key/value operations for deleting table rows.
type rowDeleter struct {
	helper               rowHelper
	fetchCols            []sqlbase.ColumnDescriptor
	fetchColIDtoRowIndex map[sqlbase.ColumnID]int
	fks                  fkDeleteHelper
	// For allocation avoidance.
	startKey roachpb.Key
	endKey   roachpb.Key
}

// makeRowDeleter creates a rowDeleter for the given table.
//
// The returned rowDeleter contains a fetchCols field that defines the
// expectation of which values are passed as values to deleteRow. Any column
// passed in requestedCols will be included in fetchCols.
func makeRowDeleter(
	txn *client.Txn,
	tableDesc *sqlbase.TableDescriptor,
	fkTables TablesByID,
	requestedCols []sqlbase.ColumnDescriptor,
	checkFKs bool,
) (rowDeleter, error) {
	indexes := tableDesc.Indexes
	for _, m := range tableDesc.Mutations {
		if index := m.GetIndex(); index != nil {
			indexes = append(indexes, *index)
		}
	}

	fetchCols := requestedCols[:len(requestedCols):len(requestedCols)]
	fetchColIDtoRowIndex := colIDtoRowIndexFromCols(fetchCols)

	maybeAddCol := func(colID sqlbase.ColumnID) error {
		if _, ok := fetchColIDtoRowIndex[colID]; !ok {
			col, err := tableDesc.FindColumnByID(colID)
			if err != nil {
				return err
			}
			fetchColIDtoRowIndex[col.ID] = len(fetchCols)
			fetchCols = append(fetchCols, *col)
		}
		return nil
	}
	for _, colID := range tableDesc.PrimaryIndex.ColumnIDs {
		if err := maybeAddCol(colID); err != nil {
			return rowDeleter{}, err
		}
	}
	for _, index := range indexes {
		for _, colID := range index.ColumnIDs {
			if err := maybeAddCol(colID); err != nil {
				return rowDeleter{}, err
			}
		}
	}

	rd := rowDeleter{
		helper:               rowHelper{tableDesc: tableDesc, indexes: indexes},
		fetchCols:            fetchCols,
		fetchColIDtoRowIndex: fetchColIDtoRowIndex,
	}
	if checkFKs {
		var err error
		if rd.fks, err = makeFKDeleteHelper(txn, *tableDesc, fkTables, fetchColIDtoRowIndex); err != nil {
			return rowDeleter{}, nil
		}
	}

	return rd, nil
}

// deleteRow adds to the batch the kv operations necessary to delete a table row
// with the given values.
func (rd *rowDeleter) deleteRow(ctx context.Context, b *client.Batch, values []parser.Datum) error {
	if err := rd.fks.checkAll(values); err != nil {
		return err
	}

	primaryIndexKey, secondaryIndexEntries, err := rd.helper.encodeIndexes(rd.fetchColIDtoRowIndex, values)
	if err != nil {
		return err
	}

	for _, secondaryIndexEntry := range secondaryIndexEntries {
		if log.V(2) {
			log.Infof(ctx, "Del %s", secondaryIndexEntry.Key)
		}
		b.Del(secondaryIndexEntry.Key)
	}

	// Delete the row.
	rd.startKey = roachpb.Key(primaryIndexKey)
	rd.endKey = roachpb.Key(encoding.EncodeNotNullDescending(primaryIndexKey))
	if log.V(2) {
		log.Infof(ctx, "DelRange %s - %s", rd.startKey, rd.endKey)
	}
	b.DelRange(&rd.startKey, &rd.endKey, false)
	rd.startKey, rd.endKey = nil, nil

	return nil
}

// deleteIndexRow adds to the batch the kv operations necessary to delete a
// table row from the given index.
func (rd *rowDeleter) deleteIndexRow(
	ctx context.Context, b *client.Batch, idx *sqlbase.IndexDescriptor, values []parser.Datum,
) error {
	if err := rd.fks.checkAll(values); err != nil {
		return err
	}
	secondaryIndexEntry, err := sqlbase.EncodeSecondaryIndex(
		rd.helper.tableDesc, idx, rd.fetchColIDtoRowIndex, values)
	if err != nil {
		return err
	}
	if log.V(2) {
		log.Infof(ctx, "Del %s", secondaryIndexEntry.Key)
	}
	b.Del(secondaryIndexEntry.Key)
	return nil
}

func colIDtoRowIndexFromCols(cols []sqlbase.ColumnDescriptor) map[sqlbase.ColumnID]int {
	colIDtoRowIndex := make(map[sqlbase.ColumnID]int, len(cols))
	for i, col := range cols {
		colIDtoRowIndex[col.ID] = i
	}
	return colIDtoRowIndex
}
