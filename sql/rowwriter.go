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

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// rowHelper has the common methods for table row manipulations.
type rowHelper struct {
	tableDesc *sqlbase.TableDescriptor
	indexes   []sqlbase.IndexDescriptor

	// Computed and cached.
	primaryIndexKeyPrefix []byte
	primaryIndexCols      map[sqlbase.ColumnID]struct{}
}

func (rh *rowHelper) encodeIndexes(
	colIDtoRowIndex map[sqlbase.ColumnID]int, values []parser.Datum,
) (
	primaryIndexKey []byte,
	secondaryIndexEntries []sqlbase.IndexEntry,
	err error,
) {
	if rh.primaryIndexKeyPrefix == nil {
		rh.primaryIndexKeyPrefix = sqlbase.MakeIndexKeyPrefix(rh.tableDesc.ID,
			rh.tableDesc.PrimaryIndex.ID)
	}
	primaryIndexKey, _, err = sqlbase.EncodeIndexKey(
		&rh.tableDesc.PrimaryIndex, colIDtoRowIndex, values, rh.primaryIndexKeyPrefix)
	if err != nil {
		return nil, nil, err
	}
	secondaryIndexEntries, err = sqlbase.EncodeSecondaryIndexes(
		rh.tableDesc.ID, rh.indexes, colIDtoRowIndex, values)
	if err != nil {
		return nil, nil, err
	}

	return primaryIndexKey, secondaryIndexEntries, nil
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

// rowInserter abstracts the key/value operations for inserting table rows.
type rowInserter struct {
	helper                rowHelper
	insertCols            []sqlbase.ColumnDescriptor
	insertColIDtoRowIndex map[sqlbase.ColumnID]int

	// For allocation avoidance.
	marshalled    []roachpb.Value
	key           roachpb.Key
	sentinelValue roachpb.Value
}

// makeRowInserter creates a rowInserter for the given table.
//
// insertCols must contain every column in the primary key.
func makeRowInserter(
	tableDesc *sqlbase.TableDescriptor,
	insertCols []sqlbase.ColumnDescriptor,
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

	return ri, nil
}

// insertCPutFn is used by insertRow when conflicts should be respected.
// logValue is used for pretty printing.
func insertCPutFn(b *client.Batch, key *roachpb.Key, value interface{}, logValue interface{}) {
	if log.V(2) {
		log.InfofDepth(1, "CPut %s -> %v", *key, logValue)
	}
	b.CPut(key, value, nil)
}

// insertPutFn is used by insertRow when conflicts should be ignored.
// logValue is used for pretty printing.
func insertPutFn(b *client.Batch, key *roachpb.Key, value interface{}, logValue interface{}) {
	if log.V(2) {
		log.InfofDepth(1, "Put %s -> %v", *key, logValue)
	}
	b.Put(key, value)
}

// insertRow adds to the batch the kv operations necessary to insert a table row
// with the given values.
func (ri *rowInserter) insertRow(b *client.Batch, values []parser.Datum, ignoreConflicts bool) error {
	if len(values) != len(ri.insertCols) {
		return util.Errorf("got %d values but expected %d", len(values), len(ri.insertCols))
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

	primaryIndexKey, secondaryIndexEntries, err := ri.helper.encodeIndexes(ri.insertColIDtoRowIndex, values)
	if err != nil {
		return err
	}

	// Write the row sentinel. We want to write the sentinel first in case
	// we are trying to insert a duplicate primary key: if we write the
	// secondary indexes first, we may get an error that looks like a
	// uniqueness violation on a non-unique index.
	ri.key = keys.MakeNonColumnKey(primaryIndexKey)
	// Each sentinel value needs a distinct RawBytes field as the computed
	// checksum includes the key the value is associated with.
	ri.sentinelValue.SetBytes([]byte{})
	putFn(b, &ri.key, &ri.sentinelValue, &ri.sentinelValue)
	ri.key = nil

	for _, secondaryIndexEntry := range secondaryIndexEntries {
		ri.key = secondaryIndexEntry.Key
		putFn(b, &ri.key, secondaryIndexEntry.Value, secondaryIndexEntry.Value)
	}
	ri.key = nil

	// Write the row columns.
	for i, val := range values {
		col := ri.insertCols[i]

		if ri.helper.columnInPK(col.ID) {
			// Skip primary key columns as their values are encoded in the row
			// sentinel key which is guaranteed to exist for as long as the row
			// exists.
			continue
		}

		if ri.marshalled[i].RawBytes != nil {
			// We only output non-NULL values. Non-existent column keys are
			// considered NULL during scanning and the row sentinel ensures we know
			// the row exists.

			ri.key = keys.MakeColumnKey(primaryIndexKey, uint32(col.ID))
			putFn(b, &ri.key, &ri.marshalled[i], val)
			ri.key = nil
		}
	}

	return nil
}

// rowUpdater abstracts the key/value operations for updating table rows.
type rowUpdater struct {
	helper               rowHelper
	fetchCols            []sqlbase.ColumnDescriptor
	fetchColIDtoRowIndex map[sqlbase.ColumnID]int
	updateCols           []sqlbase.ColumnDescriptor
	deleteOnlyIndex      map[int]struct{}
	primaryKeyColChange  bool

	rd rowDeleter
	ri rowInserter

	// For allocation avoidance.
	marshalled []roachpb.Value
	newValues  []parser.Datum
	key        roachpb.Key
}

// makeRowUpdater creates a rowUpdater for the given table.
//
// updateCols are the columns being updated and correspond to the updateValues
// that will be passed to updateRow.
//
// The returned rowUpdater contains a fetchCols field that defines the
// expectation of which values are passed as oldValues to updateRow. Any column
// passed in requestedCols will be included in fetchCols.
func makeRowUpdater(
	tableDesc *sqlbase.TableDescriptor,
	updateCols []sqlbase.ColumnDescriptor,
	requestedCols []sqlbase.ColumnDescriptor,
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
		helper:              rowHelper{tableDesc: tableDesc, indexes: indexes},
		updateCols:          updateCols,
		deleteOnlyIndex:     deleteOnlyIndex,
		primaryKeyColChange: primaryKeyColChange,
		marshalled:          make([]roachpb.Value, len(updateCols)),
		newValues:           make([]parser.Datum, len(tableDesc.Columns)),
	}

	if primaryKeyColChange {
		// These fields are only used when the primary key is changing.
		var err error
		// When changing the primary key, we delete the old values and reinsert
		// them, so request them all.
		if ru.rd, err = makeRowDeleter(tableDesc, tableDesc.Columns); err != nil {
			return rowUpdater{}, err
		}
		ru.fetchCols = ru.rd.fetchCols
		ru.fetchColIDtoRowIndex = colIDtoRowIndexFromCols(ru.fetchCols)
		if ru.ri, err = makeRowInserter(tableDesc, tableDesc.Columns); err != nil {
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
		for _, index := range indexes {
			for _, colID := range index.ColumnIDs {
				if err := maybeAddCol(colID); err != nil {
					return rowUpdater{}, err
				}
			}
		}
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
	b *client.Batch,
	oldValues []parser.Datum,
	updateValues []parser.Datum,
) ([]parser.Datum, error) {
	if len(oldValues) != len(ru.fetchCols) {
		return nil, util.Errorf("got %d values but expected %d", len(oldValues), len(ru.fetchCols))
	}
	if len(updateValues) != len(ru.updateCols) {
		return nil, util.Errorf("got %d values but expected %d", len(updateValues), len(ru.updateCols))
	}

	primaryIndexKey, secondaryIndexEntries, err := ru.helper.encodeIndexes(ru.fetchColIDtoRowIndex, oldValues)
	if err != nil {
		return nil, err
	}

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

	newPrimaryIndexKey := primaryIndexKey
	rowPrimaryKeyChanged := false
	var newSecondaryIndexEntries []sqlbase.IndexEntry
	if ru.primaryKeyColChange {
		newPrimaryIndexKey, newSecondaryIndexEntries, err = ru.helper.encodeIndexes(ru.fetchColIDtoRowIndex, ru.newValues)
		if err != nil {
			return nil, err
		}
		rowPrimaryKeyChanged = !bytes.Equal(primaryIndexKey, newPrimaryIndexKey)
	} else {
		newSecondaryIndexEntries, err = sqlbase.EncodeSecondaryIndexes(
			ru.helper.tableDesc.ID, ru.helper.indexes, ru.fetchColIDtoRowIndex, ru.newValues)
		if err != nil {
			return nil, err
		}
	}

	if rowPrimaryKeyChanged {
		err := ru.rd.deleteRow(b, oldValues)
		if err != nil {
			return nil, err
		}
		err = ru.ri.insertRow(b, ru.newValues, false)
		return ru.newValues, err
	}

	// Update secondary indexes.
	for i, newSecondaryIndexEntry := range newSecondaryIndexEntries {
		secondaryIndexEntry := secondaryIndexEntries[i]
		secondaryKeyChanged := !bytes.Equal(newSecondaryIndexEntry.Key, secondaryIndexEntry.Key)
		if secondaryKeyChanged {
			if log.V(2) {
				log.Infof("Del %s", secondaryIndexEntry.Key)
			}
			b.Del(secondaryIndexEntry.Key)
			// Do not update Indexes in the DELETE_ONLY state.
			if _, ok := ru.deleteOnlyIndex[i]; !ok {
				if log.V(2) {
					log.Infof("CPut %s -> %v", newSecondaryIndexEntry.Key, newSecondaryIndexEntry.Value)
				}
				b.CPut(newSecondaryIndexEntry.Key, newSecondaryIndexEntry.Value, nil)
			}
		}
	}

	// Add the new values.
	for i, val := range updateValues {
		col := ru.updateCols[i]

		if ru.helper.columnInPK(col.ID) {
			// Skip primary key columns as their values are encoded in the row
			// sentinel key which is guaranteed to exist for as long as the row
			// exists.
			continue
		}

		ru.key = keys.MakeColumnKey(newPrimaryIndexKey, uint32(col.ID))
		if ru.marshalled[i].RawBytes != nil {
			// We only output non-NULL values. Non-existent column keys are
			// considered NULL during scanning and the row sentinel ensures we know
			// the row exists.
			if log.V(2) {
				log.Infof("Put %s -> %v", ru.key, val)
			}

			b.Put(&ru.key, &ru.marshalled[i])
		} else {
			// The column might have already existed but is being set to NULL, so
			// delete it.
			if log.V(2) {
				log.Infof("Del %s", ru.key)
			}

			b.Del(&ru.key)
		}
		ru.key = nil
	}

	return ru.newValues, nil
}

// rowDeleter abstracts the key/value operations for deleting table rows.
type rowDeleter struct {
	helper               rowHelper
	fetchCols            []sqlbase.ColumnDescriptor
	fetchColIDtoRowIndex map[sqlbase.ColumnID]int

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
	tableDesc *sqlbase.TableDescriptor,
	requestedCols []sqlbase.ColumnDescriptor,
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
	return rd, nil
}

// deleteRow adds to the batch the kv operations necessary to delete a table row
// with the given values.
func (rd *rowDeleter) deleteRow(b *client.Batch, values []parser.Datum) error {
	primaryIndexKey, secondaryIndexEntries, err := rd.helper.encodeIndexes(rd.fetchColIDtoRowIndex, values)
	if err != nil {
		return err
	}

	for _, secondaryIndexEntry := range secondaryIndexEntries {
		if log.V(2) {
			log.Infof("Del %s", secondaryIndexEntry.Key)
		}
		b.Del(secondaryIndexEntry.Key)
	}

	// Delete the row.
	rd.startKey = roachpb.Key(primaryIndexKey)
	rd.endKey = rd.startKey.PrefixEnd()
	if log.V(2) {
		log.Infof("DelRange %s - %s", rd.startKey, rd.endKey)
	}
	b.DelRange(&rd.startKey, &rd.endKey, false)
	rd.startKey, rd.endKey = nil, nil

	return nil
}

func colIDtoRowIndexFromCols(cols []sqlbase.ColumnDescriptor) map[sqlbase.ColumnID]int {
	colIDtoRowIndex := make(map[sqlbase.ColumnID]int, len(cols))
	for i, col := range cols {
		colIDtoRowIndex[col.ID] = i
	}
	return colIDtoRowIndex
}
