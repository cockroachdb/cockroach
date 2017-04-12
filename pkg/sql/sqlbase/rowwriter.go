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

package sqlbase

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

const (
	// CheckFKs can be passed to row writers to check fk validity.
	CheckFKs = true
	// SkipFKs can be passed to row writer to skip fk validity checks.
	SkipFKs = false
)

// rowHelper has the common methods for table row manipulations.
type rowHelper struct {
	TableDesc    *TableDescriptor
	Indexes      []IndexDescriptor
	indexEntries []IndexEntry

	// Computed and cached.
	primaryIndexKeyPrefix []byte
	primaryIndexCols      map[ColumnID]struct{}
	sortedColumnFamilies  map[FamilyID][]ColumnID
}

// encodeIndexes encodes the primary and secondary index keys. The
// secondaryIndexEntries are only valid until the next call to encodeIndexes or
// encodeSecondaryIndexes.
func (rh *rowHelper) encodeIndexes(
	colIDtoRowIndex map[ColumnID]int, values []parser.Datum,
) (primaryIndexKey []byte, secondaryIndexEntries []IndexEntry, err error) {
	if rh.primaryIndexKeyPrefix == nil {
		rh.primaryIndexKeyPrefix = MakeIndexKeyPrefix(rh.TableDesc,
			rh.TableDesc.PrimaryIndex.ID)
	}
	primaryIndexKey, _, err = EncodeIndexKey(
		rh.TableDesc, &rh.TableDesc.PrimaryIndex, colIDtoRowIndex, values, rh.primaryIndexKeyPrefix)
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
	colIDtoRowIndex map[ColumnID]int, values []parser.Datum,
) (secondaryIndexEntries []IndexEntry, err error) {
	if len(rh.indexEntries) != len(rh.Indexes) {
		rh.indexEntries = make([]IndexEntry, len(rh.Indexes))
	}
	err = EncodeSecondaryIndexes(
		rh.TableDesc, rh.Indexes, colIDtoRowIndex, values, rh.indexEntries)
	if err != nil {
		return nil, err
	}
	return rh.indexEntries, nil
}

// skipColumnInPK returns true if the value at column colID does not need
// to be encoded because it is already part of the primary key. Composite
// datums are considered too, so a composite datum in a PK will return false.
// TODO(dan): This logic is common and being moved into TableDescriptor (see
// #6233). Once it is, use the shared one.
func (rh *rowHelper) skipColumnInPK(
	colID ColumnID, family FamilyID, value parser.Datum,
) (bool, error) {
	if rh.primaryIndexCols == nil {
		rh.primaryIndexCols = make(map[ColumnID]struct{})
		for _, colID := range rh.TableDesc.PrimaryIndex.ColumnIDs {
			rh.primaryIndexCols[colID] = struct{}{}
		}
	}
	if _, ok := rh.primaryIndexCols[colID]; !ok {
		return false, nil
	}
	if family != 0 {
		return false, errors.Errorf("primary index column %d must be in family 0, was %d", colID, family)
	}
	if cdatum, ok := value.(parser.CompositeDatum); ok {
		// Composite columns are encoded in both the key and the value.
		return !cdatum.IsComposite(), nil
	}
	// Skip primary key columns as their values are encoded in the key of
	// each family. Family 0 is guaranteed to exist and acts as a
	// sentinel.
	return true, nil
}

type columnIDs []ColumnID

func (c columnIDs) Len() int           { return len(c) }
func (c columnIDs) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c columnIDs) Less(i, j int) bool { return c[i] < c[j] }

func (rh *rowHelper) sortedColumnFamily(famID FamilyID) ([]ColumnID, bool) {
	if rh.sortedColumnFamilies == nil {
		rh.sortedColumnFamilies = make(map[FamilyID][]ColumnID, len(rh.TableDesc.Families))
		for _, family := range rh.TableDesc.Families {
			colIDs := append([]ColumnID(nil), family.ColumnIDs...)
			sort.Sort(columnIDs(colIDs))
			rh.sortedColumnFamilies[family.ID] = colIDs
		}
	}
	colIDs, ok := rh.sortedColumnFamilies[famID]
	return colIDs, ok
}

// RowInserter abstracts the key/value operations for inserting table rows.
type RowInserter struct {
	Helper                rowHelper
	InsertCols            []ColumnDescriptor
	InsertColIDtoRowIndex map[ColumnID]int
	Fks                   fkInsertHelper

	// For allocation avoidance.
	marshalled []roachpb.Value
	key        roachpb.Key
	valueBuf   []byte
	value      roachpb.Value
}

// MakeRowInserter creates a RowInserter for the given table.
//
// InsertCols must contain every column in the primary key.
func MakeRowInserter(
	txn *client.Txn,
	tableDesc *TableDescriptor,
	fkTables TableLookupsByID,
	insertCols []ColumnDescriptor,
	checkFKs bool,
) (RowInserter, error) {
	indexes := tableDesc.Indexes
	// Also include the secondary indexes in mutation state WRITE_ONLY.
	for _, m := range tableDesc.Mutations {
		if m.State == DescriptorMutation_WRITE_ONLY {
			if index := m.GetIndex(); index != nil {
				indexes = append(indexes, *index)
			}
		}
	}

	ri := RowInserter{
		Helper:                rowHelper{TableDesc: tableDesc, Indexes: indexes},
		InsertCols:            insertCols,
		InsertColIDtoRowIndex: ColIDtoRowIndexFromCols(insertCols),
		marshalled:            make([]roachpb.Value, len(insertCols)),
	}

	for i, col := range tableDesc.PrimaryIndex.ColumnIDs {
		if _, ok := ri.InsertColIDtoRowIndex[col]; !ok {
			return RowInserter{}, fmt.Errorf("missing %q primary key column", tableDesc.PrimaryIndex.ColumnNames[i])
		}
	}

	if checkFKs {
		var err error
		if ri.Fks, err = makeFKInsertHelper(txn, *tableDesc, fkTables, ri.InsertColIDtoRowIndex); err != nil {
			return ri, err
		}
	}
	return ri, nil
}

// insertCPutFn is used by insertRow when conflicts should be respected.
// logValue is used for pretty printing.
func insertCPutFn(ctx context.Context, b puter, key *roachpb.Key, value *roachpb.Value) {
	// TODO(dan): We want do this V(2) log everywhere in sql. Consider making a
	// client.Batch wrapper instead of inlining it everywhere.
	if log.V(2) {
		log.InfofDepth(ctx, 1, "CPut %s -> %s", *key, value.PrettyPrint())
	}
	b.CPut(key, value, nil)
}

// insertPutFn is used by insertRow when conflicts should be ignored.
// logValue is used for pretty printing.
func insertPutFn(ctx context.Context, b puter, key *roachpb.Key, value *roachpb.Value) {
	if log.V(2) {
		log.InfofDepth(ctx, 1, "Put %s -> %s", *key, value.PrettyPrint())
	}
	b.Put(key, value)
}

type puter interface {
	CPut(key, value, expValue interface{})
	Put(key, value interface{})
}

// InsertRow adds to the batch the kv operations necessary to insert a table row
// with the given values.
func (ri *RowInserter) InsertRow(
	ctx context.Context, b puter, values []parser.Datum, ignoreConflicts bool,
) error {
	if len(values) != len(ri.InsertCols) {
		return errors.Errorf("got %d values but expected %d", len(values), len(ri.InsertCols))
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
		if ri.marshalled[i], err = MarshalColumnValue(ri.InsertCols[i], val); err != nil {
			return err
		}
	}

	if err := ri.Fks.checkAll(ctx, values); err != nil {
		return err
	}

	primaryIndexKey, secondaryIndexEntries, err := ri.Helper.encodeIndexes(ri.InsertColIDtoRowIndex, values)
	if err != nil {
		return err
	}

	// Add the new values.
	// TODO(dan): This has gotten very similar to the loop in UpdateRow, see if
	// they can be DRY'd. Ideally, this would also work for
	// truncateAndBackfillColumnsChunk, which is currently abusing rowUdpdater.
	for i, family := range ri.Helper.TableDesc.Families {
		if i > 0 {
			// HACK: MakeFamilyKey appends to its argument, so on every loop iteration
			// after the first, trim primaryIndexKey so nothing gets overwritten.
			// TODO(dan): Instead of this, use something like engine.ChunkAllocator.
			primaryIndexKey = primaryIndexKey[:len(primaryIndexKey):len(primaryIndexKey)]
		}

		if len(family.ColumnIDs) == 1 && family.ColumnIDs[0] == family.DefaultColumnID {
			// Storage optimization to store DefaultColumnID directly as a value. Also
			// backwards compatible with the original BaseFormatVersion.

			idx, ok := ri.InsertColIDtoRowIndex[family.DefaultColumnID]
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

		var lastColID ColumnID
		familySortedColumnIDs, ok := ri.Helper.sortedColumnFamily(family.ID)
		if !ok {
			panic("invalid family sorted column id map")
		}
		for _, colID := range familySortedColumnIDs {
			idx, ok := ri.InsertColIDtoRowIndex[colID]
			if !ok || values[idx] == parser.DNull {
				// Column not being inserted.
				continue
			}

			if skip, err := ri.Helper.skipColumnInPK(colID, family.ID, values[idx]); err != nil {
				return err
			} else if skip {
				continue
			}

			col := ri.InsertCols[idx]

			if lastColID > col.ID {
				panic(fmt.Errorf("cannot write column id %d after %d", col.ID, lastColID))
			}
			colIDDiff := col.ID - lastColID
			lastColID = col.ID
			ri.valueBuf, err = EncodeTableValue(ri.valueBuf, colIDDiff, values[idx])
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

// RowUpdater abstracts the key/value operations for updating table rows.
type RowUpdater struct {
	Helper                rowHelper
	FetchCols             []ColumnDescriptor
	FetchColIDtoRowIndex  map[ColumnID]int
	UpdateCols            []ColumnDescriptor
	updateColIDtoRowIndex map[ColumnID]int
	deleteOnlyIndex       map[int]struct{}
	primaryKeyColChange   bool

	rd RowDeleter
	ri RowInserter

	Fks fkUpdateHelper

	// For allocation avoidance.
	marshalled      []roachpb.Value
	newValues       []parser.Datum
	key             roachpb.Key
	indexEntriesBuf []IndexEntry
	valueBuf        []byte
	value           roachpb.Value
}

type rowUpdaterType int

const (
	// RowUpdaterDefault indicates that a RowUpdater should update everything
	// about a row, including secondary indexes.
	RowUpdaterDefault rowUpdaterType = 0
	// RowUpdaterOnlyColumns indicates that a RowUpdater should only update the
	// columns of a row.
	RowUpdaterOnlyColumns rowUpdaterType = 1
)

// MakeRowUpdater creates a RowUpdater for the given table.
//
// UpdateCols are the columns being updated and correspond to the updateValues
// that will be passed to UpdateRow.
//
// The returned RowUpdater contains a FetchCols field that defines the
// expectation of which values are passed as oldValues to UpdateRow. Any column
// passed in requestedCols will be included in FetchCols.
func MakeRowUpdater(
	txn *client.Txn,
	tableDesc *TableDescriptor,
	fkTables TableLookupsByID,
	updateCols []ColumnDescriptor,
	requestedCols []ColumnDescriptor,
	updateType rowUpdaterType,
) (RowUpdater, error) {
	updateColIDtoRowIndex := ColIDtoRowIndexFromCols(updateCols)

	primaryIndexCols := make(map[ColumnID]struct{}, len(tableDesc.PrimaryIndex.ColumnIDs))
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
	needsUpdate := func(index IndexDescriptor) bool {
		if updateType == RowUpdaterOnlyColumns {
			// Only update columns.
			return false
		}
		// If the primary key changed, we need to update all of them.
		if primaryKeyColChange {
			return true
		}
		return index.RunOverAllColumns(func(id ColumnID) error {
			if _, ok := updateColIDtoRowIndex[id]; ok {
				return returnTruePseudoError
			}
			return nil
		}) != nil
	}

	indexes := make([]IndexDescriptor, 0, len(tableDesc.Indexes)+len(tableDesc.Mutations))
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
				case DescriptorMutation_DELETE_ONLY:
					if deleteOnlyIndex == nil {
						// Allocate at most once.
						deleteOnlyIndex = make(map[int]struct{}, len(tableDesc.Mutations))
					}
					deleteOnlyIndex[len(indexes)-1] = struct{}{}

				case DescriptorMutation_WRITE_ONLY:
				}
			}
		}
	}

	ru := RowUpdater{
		Helper:                rowHelper{TableDesc: tableDesc, Indexes: indexes},
		UpdateCols:            updateCols,
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
		if ru.rd, err = MakeRowDeleter(txn, tableDesc, fkTables, tableDesc.Columns, SkipFKs); err != nil {
			return RowUpdater{}, err
		}
		ru.FetchCols = ru.rd.FetchCols
		ru.FetchColIDtoRowIndex = ColIDtoRowIndexFromCols(ru.FetchCols)
		if ru.ri, err = MakeRowInserter(txn, tableDesc, fkTables, tableDesc.Columns, SkipFKs); err != nil {
			return RowUpdater{}, err
		}
	} else {
		ru.FetchCols = requestedCols[:len(requestedCols):len(requestedCols)]
		ru.FetchColIDtoRowIndex = ColIDtoRowIndexFromCols(ru.FetchCols)

		maybeAddCol := func(colID ColumnID) error {
			if _, ok := ru.FetchColIDtoRowIndex[colID]; !ok {
				col, err := tableDesc.FindColumnByID(colID)
				if err != nil {
					return err
				}
				ru.FetchColIDtoRowIndex[col.ID] = len(ru.FetchCols)
				ru.FetchCols = append(ru.FetchCols, *col)
			}
			return nil
		}
		for _, colID := range tableDesc.PrimaryIndex.ColumnIDs {
			if err := maybeAddCol(colID); err != nil {
				return RowUpdater{}, err
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
						return RowUpdater{}, err
					}
				}
			}
		}
		for _, index := range indexes {
			if err := index.RunOverAllColumns(maybeAddCol); err != nil {
				return RowUpdater{}, err
			}
		}
	}

	var err error
	if ru.Fks, err = makeFKUpdateHelper(txn, *tableDesc, fkTables, ru.FetchColIDtoRowIndex); err != nil {
		return RowUpdater{}, err
	}
	return ru, nil
}

// UpdateRow adds to the batch the kv operations necessary to update a table row
// with the given values.
//
// The row corresponding to oldValues is updated with the ones in updateValues.
// Note that updateValues only contains the ones that are changing.
//
// The return value is only good until the next call to UpdateRow.
func (ru *RowUpdater) UpdateRow(
	ctx context.Context, b *client.Batch, oldValues []parser.Datum, updateValues []parser.Datum,
) ([]parser.Datum, error) {
	if len(oldValues) != len(ru.FetchCols) {
		return nil, errors.Errorf("got %d values but expected %d", len(oldValues), len(ru.FetchCols))
	}
	if len(updateValues) != len(ru.UpdateCols) {
		return nil, errors.Errorf("got %d values but expected %d", len(updateValues), len(ru.UpdateCols))
	}

	primaryIndexKey, secondaryIndexEntries, err := ru.Helper.encodeIndexes(ru.FetchColIDtoRowIndex, oldValues)
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
		if ru.marshalled[i], err = MarshalColumnValue(ru.UpdateCols[i], val); err != nil {
			return nil, err
		}
	}

	// Update the row values.
	copy(ru.newValues, oldValues)
	for i, updateCol := range ru.UpdateCols {
		ru.newValues[ru.FetchColIDtoRowIndex[updateCol.ID]] = updateValues[i]
	}

	rowPrimaryKeyChanged := false
	var newSecondaryIndexEntries []IndexEntry
	if ru.primaryKeyColChange {
		var newPrimaryIndexKey []byte
		newPrimaryIndexKey, newSecondaryIndexEntries, err =
			ru.Helper.encodeIndexes(ru.FetchColIDtoRowIndex, ru.newValues)
		if err != nil {
			return nil, err
		}
		rowPrimaryKeyChanged = !bytes.Equal(primaryIndexKey, newPrimaryIndexKey)
	} else {
		newSecondaryIndexEntries, err =
			ru.Helper.encodeSecondaryIndexes(ru.FetchColIDtoRowIndex, ru.newValues)
		if err != nil {
			return nil, err
		}
	}

	if rowPrimaryKeyChanged {
		if err := ru.Fks.checkIdx(ctx, ru.Helper.TableDesc.PrimaryIndex.ID, oldValues, ru.newValues); err != nil {
			return nil, err
		}
		for i := range newSecondaryIndexEntries {
			if !bytes.Equal(newSecondaryIndexEntries[i].Key, secondaryIndexEntries[i].Key) {
				if err := ru.Fks.checkIdx(ctx, ru.Helper.Indexes[i].ID, oldValues, ru.newValues); err != nil {
					return nil, err
				}
			}
		}

		if err := ru.rd.DeleteRow(ctx, b, oldValues); err != nil {
			return nil, err
		}
		if err := ru.ri.InsertRow(ctx, b, ru.newValues, false); err != nil {
			return nil, err
		}
		return ru.newValues, nil
	}

	// Add the new values.
	// TODO(dan): This has gotten very similar to the loop in insertRow, see if
	// they can be DRY'd. Ideally, this would also work for
	// truncateAndBackfillColumnsChunk, which is currently abusing rowUdpdater.
	for i, family := range ru.Helper.TableDesc.Families {
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

		var lastColID ColumnID
		familySortedColumnIDs, ok := ru.Helper.sortedColumnFamily(family.ID)
		if !ok {
			panic("invalid family sorted column id map")
		}
		for _, colID := range familySortedColumnIDs {
			idx, ok := ru.FetchColIDtoRowIndex[colID]
			if !ok {
				return nil, errors.Errorf("column %d was expected to be fetched, but wasn't", colID)
			}
			if ru.newValues[idx] == parser.DNull {
				continue
			}

			if skip, err := ru.Helper.skipColumnInPK(colID, family.ID, ru.newValues[idx]); err != nil {
				return nil, err
			} else if skip {
				continue
			}

			col := ru.FetchCols[idx]

			if lastColID > col.ID {
				panic(fmt.Errorf("cannot write column id %d after %d", col.ID, lastColID))
			}
			colIDDiff := col.ID - lastColID
			lastColID = col.ID
			ru.valueBuf, err = EncodeTableValue(ru.valueBuf, colIDDiff, ru.newValues[idx])
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
		var expValue interface{}
		if !bytes.Equal(newSecondaryIndexEntry.Key, secondaryIndexEntry.Key) {
			if err := ru.Fks.checkIdx(ctx, ru.Helper.Indexes[i].ID, oldValues, ru.newValues); err != nil {
				return nil, err
			}

			if log.V(2) {
				log.Infof(ctx, "Del %s", secondaryIndexEntry.Key)
			}
			b.Del(secondaryIndexEntry.Key)
		} else if !bytes.Equal(newSecondaryIndexEntry.Value.RawBytes, secondaryIndexEntry.Value.RawBytes) {
			expValue = &secondaryIndexEntry.Value
		} else {
			continue
		}
		// Do not update Indexes in the DELETE_ONLY state.
		if _, ok := ru.deleteOnlyIndex[i]; !ok {
			if log.V(2) {
				log.Infof(ctx, "CPut %s -> %v", newSecondaryIndexEntry.Key, newSecondaryIndexEntry.Value.PrettyPrint())
			}
			b.CPut(newSecondaryIndexEntry.Key, &newSecondaryIndexEntry.Value, expValue)
		}
	}

	return ru.newValues, nil
}

// IsColumnOnlyUpdate returns true if this RowUpdater is only updating column
// data (in contrast to updating the primary key or other indexes).
func (ru *RowUpdater) IsColumnOnlyUpdate() bool {
	// TODO(dan): This is used in the schema change backfill to assert that it was
	// configured correctly and will not be doing things it shouldn't. This is an
	// unfortunate bleeding of responsibility and indicates the abstraction could
	// be improved. Specifically, RowUpdater currently has two responsibilities
	// (computing which indexes need to be updated and mapping sql rows to k/v
	// operations) and these should be split.
	return !ru.primaryKeyColChange && len(ru.deleteOnlyIndex) == 0 && len(ru.Helper.Indexes) == 0
}

// RowDeleter abstracts the key/value operations for deleting table rows.
type RowDeleter struct {
	Helper               rowHelper
	FetchCols            []ColumnDescriptor
	FetchColIDtoRowIndex map[ColumnID]int
	Fks                  fkDeleteHelper
	// For allocation avoidance.
	startKey roachpb.Key
	endKey   roachpb.Key
}

// MakeRowDeleter creates a RowDeleter for the given table.
//
// The returned RowDeleter contains a FetchCols field that defines the
// expectation of which values are passed as values to DeleteRow. Any column
// passed in requestedCols will be included in FetchCols.
func MakeRowDeleter(
	txn *client.Txn,
	tableDesc *TableDescriptor,
	fkTables TableLookupsByID,
	requestedCols []ColumnDescriptor,
	checkFKs bool,
) (RowDeleter, error) {
	indexes := tableDesc.Indexes
	for _, m := range tableDesc.Mutations {
		if index := m.GetIndex(); index != nil {
			indexes = append(indexes, *index)
		}
	}

	fetchCols := requestedCols[:len(requestedCols):len(requestedCols)]
	fetchColIDtoRowIndex := ColIDtoRowIndexFromCols(fetchCols)

	maybeAddCol := func(colID ColumnID) error {
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
			return RowDeleter{}, err
		}
	}
	for _, index := range indexes {
		for _, colID := range index.ColumnIDs {
			if err := maybeAddCol(colID); err != nil {
				return RowDeleter{}, err
			}
		}
		// The extra columns are needed to fix #14601.
		for _, colID := range index.ExtraColumnIDs {
			if err := maybeAddCol(colID); err != nil {
				return RowDeleter{}, err
			}
		}
	}

	rd := RowDeleter{
		Helper:               rowHelper{TableDesc: tableDesc, Indexes: indexes},
		FetchCols:            fetchCols,
		FetchColIDtoRowIndex: fetchColIDtoRowIndex,
	}
	if checkFKs {
		var err error
		if rd.Fks, err = makeFKDeleteHelper(txn, *tableDesc, fkTables, fetchColIDtoRowIndex); err != nil {
			return RowDeleter{}, err
		}
	}

	return rd, nil
}

// DeleteRow adds to the batch the kv operations necessary to delete a table row
// with the given values.
func (rd *RowDeleter) DeleteRow(ctx context.Context, b *client.Batch, values []parser.Datum) error {
	if err := rd.Fks.checkAll(ctx, values); err != nil {
		return err
	}

	primaryIndexKey, secondaryIndexEntries, err := rd.Helper.encodeIndexes(rd.FetchColIDtoRowIndex, values)
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

// DeleteIndexRow adds to the batch the kv operations necessary to delete a
// table row from the given index.
func (rd *RowDeleter) DeleteIndexRow(
	ctx context.Context, b *client.Batch, idx *IndexDescriptor, values []parser.Datum,
) error {
	if err := rd.Fks.checkAll(ctx, values); err != nil {
		return err
	}
	secondaryIndexEntry, err := EncodeSecondaryIndex(
		rd.Helper.TableDesc, idx, rd.FetchColIDtoRowIndex, values)
	if err != nil {
		return err
	}
	if log.V(2) {
		log.Infof(ctx, "Del %s", secondaryIndexEntry.Key)
	}
	b.Del(secondaryIndexEntry.Key)
	return nil
}

// ColIDtoRowIndexFromCols groups a slice of ColumnDescriptors by their ID
// field, returning a map from ID to ColumnDescriptor. It assumes there are no
// duplicate descriptors in the input.
func ColIDtoRowIndexFromCols(cols []ColumnDescriptor) map[ColumnID]int {
	colIDtoRowIndex := make(map[ColumnID]int, len(cols))
	for i, col := range cols {
		colIDtoRowIndex[col.ID] = i
	}
	return colIDtoRowIndex
}
