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
	"github.com/cockroachdb/cockroach/util/log"
)

// rowWriter is a base of common functionality for row{Inserter/Updater/Deleter}.
type rowWriter struct {
	tableDesc             *TableDescriptor
	indexes               []IndexDescriptor
	colIDtoRowIndex       map[ColumnID]int
	primaryIndexKeyPrefix []byte

	// Computed and cached by InPrimaryIndex.
	primaryIndexCols map[ColumnID]struct{}
}

func makeRowWriter(
	tableDesc *TableDescriptor,
	colIDtoRowIndex map[ColumnID]int,
	indexes []IndexDescriptor,
) rowWriter {
	return rowWriter{
		tableDesc:             tableDesc,
		colIDtoRowIndex:       colIDtoRowIndex,
		indexes:               indexes,
		primaryIndexKeyPrefix: MakeIndexKeyPrefix(tableDesc.ID, tableDesc.PrimaryIndex.ID),
	}
}

func (rw rowWriter) requirePrimaryIndexCols() error {
	for i, col := range rw.tableDesc.PrimaryIndex.ColumnIDs {
		if _, ok := rw.colIDtoRowIndex[col]; !ok {
			return fmt.Errorf("missing %q primary key column", rw.tableDesc.PrimaryIndex.ColumnNames[i])
		}
	}
	return nil
}

func (rw rowWriter) requireAllIndexCols() error {
	for _, index := range rw.indexes {
		for _, col := range index.ColumnIDs {
			if _, ok := rw.colIDtoRowIndex[col]; !ok {
				return fmt.Errorf("missing %q index column", col)
			}
		}
	}
	return nil
}

func (rw rowWriter) encodeIndexes(values []parser.Datum) (
	primaryIndexKey []byte,
	secondaryIndexEntries []indexEntry,
	err error,
) {
	primaryIndexKey, _, err = encodeIndexKey(
		&rw.tableDesc.PrimaryIndex, rw.colIDtoRowIndex, values, rw.primaryIndexKeyPrefix)
	if err != nil {
		return nil, nil, err
	}
	secondaryIndexEntries, err = encodeSecondaryIndexes(
		rw.tableDesc.ID, rw.indexes, rw.colIDtoRowIndex, values)
	if err != nil {
		return nil, nil, err
	}

	return primaryIndexKey, secondaryIndexEntries, nil
}

func (rw rowWriter) InPrimaryIndex(colID ColumnID) bool {
	if rw.primaryIndexCols == nil {
		rw.primaryIndexCols = make(map[ColumnID]struct{})
		for _, colID := range rw.tableDesc.PrimaryIndex.ColumnIDs {
			rw.primaryIndexCols[colID] = struct{}{}
		}
	}
	_, ok := rw.primaryIndexCols[colID]
	return ok
}

// rowInserter abstracts the key/value operations for inserting table rows.
type rowInserter struct {
	rowWriter
	cols []ColumnDescriptor

	// For allocation avoidance.
	marshalled []interface{}
}

func makeRowInserter(
	tableDesc *TableDescriptor,
	colIDtoRowIndex map[ColumnID]int,
	cols []ColumnDescriptor,
) (rowInserter, error) {
	indexes := tableDesc.Indexes
	// Also include the secondary indexes in mutation state WRITE_ONLY.
	for _, m := range tableDesc.Mutations {
		if m.State == DescriptorMutation_WRITE_ONLY {
			if index := m.GetIndex(); index != nil {
				indexes = append(indexes, *index)
			}
		}
	}

	rw := makeRowWriter(tableDesc, colIDtoRowIndex, indexes)
	if err := rw.requirePrimaryIndexCols(); err != nil {
		return rowInserter{}, err
	}

	return rowInserter{
		rowWriter:  rw,
		cols:       cols,
		marshalled: make([]interface{}, len(cols)),
	}, nil
}

// InsertRow creates a new sql table row with the given values.
func (ri rowInserter) InsertRow(b *client.Batch, values []parser.Datum) *roachpb.Error {
	if len(values) != len(ri.cols) {
		return roachpb.NewErrorf("got %d values but expected %d", len(values), len(ri.cols))
	}

	// Encode the values to the expected column type. This needs to
	// happen before index encoding because certain datum types (i.e. tuple)
	// cannot be used as index values.
	for i, val := range values {
		// Make sure the value can be written to the column before proceeding.
		var err error
		if ri.marshalled[i], err = marshalColumnValue(ri.cols[i], val); err != nil {
			return roachpb.NewError(err)
		}
	}

	primaryIndexKey, secondaryIndexEntries, err := ri.encodeIndexes(values)
	if err != nil {
		return roachpb.NewError(err)
	}

	// Write the row sentinel. We want to write the sentinel first in case
	// we are trying to insert a duplicate primary key: if we write the
	// secondary indexes first, we may get an error that looks like a
	// uniqueness violation on a non-unique index.
	sentinelKey := keys.MakeNonColumnKey(primaryIndexKey)
	if log.V(2) {
		log.Infof("CPut %s -> NULL", roachpb.Key(sentinelKey))
	}
	// This is subtle: An interface{}(nil) deletes the value, so we pass in
	// []byte{} as a non-nil value.
	b.CPut(sentinelKey, []byte{}, nil)

	for _, secondaryIndexEntry := range secondaryIndexEntries {
		if log.V(2) {
			log.Infof("CPut %s -> %v", secondaryIndexEntry.key, secondaryIndexEntry.value)
		}
		b.CPut(secondaryIndexEntry.key, secondaryIndexEntry.value, nil)
	}

	// Write the row columns.
	for i, val := range values {
		col := ri.cols[i]

		if ri.InPrimaryIndex(col.ID) {
			// Skip primary key columns as their values are encoded in the row
			// sentinel key which is guaranteed to exist for as long as the row
			// exists.
			continue
		}

		if ri.marshalled[i] != nil {
			// We only output non-NULL values. Non-existent column keys are
			// considered NULL during scanning and the row sentinel ensures we know
			// the row exists.

			key := keys.MakeColumnKey(primaryIndexKey, uint32(col.ID))
			if log.V(2) {
				log.Infof("CPut %s -> %v", roachpb.Key(key), val)
			}

			b.CPut(key, ri.marshalled[i], nil)
		}
	}

	return nil
}

// rowUpdater abstracts the key/value operations for updating table rows.
type rowUpdater struct {
	rowWriter
	rd                  rowDeleter
	ri                  rowInserter
	updateCols          []ColumnDescriptor
	deleteOnlyIndex     map[int]struct{}
	primaryKeyColChange bool

	// For allocation avoidance.
	marshalled []interface{}
	newValues  []parser.Datum
}

func makeRowUpdater(
	tableDesc *TableDescriptor,
	colIDtoRowIndex map[ColumnID]int,
	updateCols []ColumnDescriptor,
) (rowUpdater, error) {
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

	updateColsMap := make(map[ColumnID]struct{})
	for _, updateCol := range updateCols {
		updateColsMap[updateCol.ID] = struct{}{}
	}

	// Secondary indexes needing updating.
	needsUpdate := func(index IndexDescriptor) bool {
		// If the primary key changed, we need to update all of them.
		if primaryKeyColChange {
			return true
		}
		for _, id := range index.ColumnIDs {
			if _, ok := updateColsMap[id]; ok {
				return true
			}
		}
		return false
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

	rw := makeRowWriter(tableDesc, colIDtoRowIndex, indexes)
	// We already had to compute this, so may as well save it.
	rw.primaryIndexCols = primaryIndexCols

	rd, err := makeRowDeleter(tableDesc, colIDtoRowIndex)
	if err != nil {
		return rowUpdater{}, err
	}
	ri, err := makeRowInserter(tableDesc, colIDtoRowIndex, tableDesc.Columns)
	if err != nil {
		return rowUpdater{}, err
	}

	return rowUpdater{
		rowWriter:           rw,
		rd:                  rd,
		ri:                  ri,
		updateCols:          updateCols,
		deleteOnlyIndex:     deleteOnlyIndex,
		primaryKeyColChange: primaryKeyColChange,
		marshalled:          make([]interface{}, len(updateCols)),
		newValues:           make([]parser.Datum, len(tableDesc.Columns)),
	}, nil
}

// UpdateRow updates a sql table row.
//
// The row corresponding to oldValues is updated with the ones in updateValues.
// Note that updateValues only contains the ones that are changing.
//
// The return value is only good until the next call to UpdateRow.
func (ru rowUpdater) UpdateRow(
	b *client.Batch,
	oldValues []parser.Datum,
	updateValues []parser.Datum,
) ([]parser.Datum, *roachpb.Error) {
	if len(oldValues) != len(ru.tableDesc.Columns) {
		return nil, roachpb.NewErrorf("got %d values but expected %d", len(oldValues), len(ru.tableDesc.Columns))
	}
	if len(updateValues) != len(ru.updateCols) {
		return nil, roachpb.NewErrorf("got %d values but expected %d", len(updateValues), len(ru.updateCols))
	}

	primaryIndexKey, secondaryIndexEntries, err := ru.encodeIndexes(oldValues)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	// Check that the new value types match the column types. This needs to
	// happen before index encoding because certain datum types (i.e. tuple)
	// cannot be used as index values.
	for i, val := range updateValues {
		if ru.marshalled[i], err = marshalColumnValue(ru.updateCols[i], val); err != nil {
			return nil, roachpb.NewError(err)
		}
	}

	// Update the row values.
	copy(ru.newValues, oldValues)
	for i, updateCol := range ru.updateCols {
		ru.newValues[ru.colIDtoRowIndex[updateCol.ID]] = updateValues[i]
	}

	newPrimaryIndexKey := primaryIndexKey
	rowPrimaryKeyChanged := false
	var newSecondaryIndexEntries []indexEntry
	if ru.primaryKeyColChange {
		newPrimaryIndexKey, newSecondaryIndexEntries, err = ru.encodeIndexes(ru.newValues)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		rowPrimaryKeyChanged = !bytes.Equal(primaryIndexKey, newPrimaryIndexKey)
	} else {
		newSecondaryIndexEntries, err = encodeSecondaryIndexes(
			ru.tableDesc.ID, ru.indexes, ru.colIDtoRowIndex, ru.newValues)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
	}

	if rowPrimaryKeyChanged {
		pErr := ru.rd.DeleteRow(b, oldValues)
		if pErr != nil {
			return nil, pErr
		}
		pErr = ru.ri.InsertRow(b, ru.newValues)
		return ru.newValues, pErr
	}

	// Update secondary indexes.
	for i, newSecondaryIndexEntry := range newSecondaryIndexEntries {
		secondaryIndexEntry := secondaryIndexEntries[i]
		secondaryKeyChanged := !bytes.Equal(newSecondaryIndexEntry.key, secondaryIndexEntry.key)
		if secondaryKeyChanged {
			if log.V(2) {
				log.Infof("Del %s", secondaryIndexEntry.key)
			}
			b.Del(secondaryIndexEntry.key)
			// Do not update Indexes in the DELETE_ONLY state.
			if _, ok := ru.deleteOnlyIndex[i]; !ok {
				if log.V(2) {
					log.Infof("CPut %s -> %v", newSecondaryIndexEntry.key, newSecondaryIndexEntry.value)
				}
				b.CPut(newSecondaryIndexEntry.key, newSecondaryIndexEntry.value, nil)
			}
		}
	}

	// Add the new values.
	for i, val := range updateValues {
		col := ru.updateCols[i]

		if ru.InPrimaryIndex(col.ID) {
			// Skip primary key columns as their values are encoded in the row
			// sentinel key which is guaranteed to exist for as long as the row
			// exists.
			continue
		}

		key := keys.MakeColumnKey(newPrimaryIndexKey, uint32(col.ID))
		if ru.marshalled[i] != nil {
			// We only output non-NULL values. Non-existent column keys are
			// considered NULL during scanning and the row sentinel ensures we know
			// the row exists.
			if log.V(2) {
				log.Infof("Put %s -> %v", roachpb.Key(key), val)
			}

			b.Put(key, ru.marshalled[i])
		} else {
			// The column might have already existed but is being set to NULL, so
			// delete it.
			if log.V(2) {
				log.Infof("Del %s", roachpb.Key(key))
			}

			b.Del(key)
		}

	}

	return ru.newValues, nil
}

// rowDeleter abstracts the key/value operations for deleting table rows.
type rowDeleter struct {
	rowWriter
}

func makeRowDeleter(tableDesc *TableDescriptor, colIDtoRowIndex map[ColumnID]int) (rowDeleter, error) {
	indexes := tableDesc.Indexes
	for _, m := range tableDesc.Mutations {
		if index := m.GetIndex(); index != nil {
			indexes = append(indexes, *index)
		}
	}
	rw := makeRowWriter(tableDesc, colIDtoRowIndex, indexes)
	if err := rw.requireAllIndexCols(); err != nil {
		return rowDeleter{}, err
	}
	return rowDeleter{rw}, nil
}

// DeleteRow deletes a sql table row.
func (rd rowDeleter) DeleteRow(b *client.Batch, values []parser.Datum) *roachpb.Error {
	primaryIndexKey, secondaryIndexEntries, err := rd.encodeIndexes(values)
	if err != nil {
		return roachpb.NewError(err)
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
	b.DelRange(rowStartKey, rowEndKey, false)

	return nil
}

// HasFast returns true if the FastDelete optimization can be used.
func (rd rowDeleter) HasFast() bool {
	if len(rd.indexes) != 0 {
		if log.V(2) {
			log.Infof("delete forced to scan: values required to update %d secondary indexes", len(rd.indexes))
		}
		return false
	}
	return true
}

// FastDelete deletes a sql table row without knowing the values that are
// currently present.
func (rd rowDeleter) FastDelete(
	b *client.Batch,
	scan *scanNode,
	commitFunc func(b *client.Batch) *roachpb.Error,
) (rowCount int, pErr *roachpb.Error) {
	for _, span := range scan.spans {
		if log.V(2) {
			log.Infof("Skipping scan and just deleting %s - %s", span.start, span.end)
		}
		b.DelRange(span.start, span.end, true)
	}

	pErr = commitFunc(b)
	if pErr != nil {
		return 0, pErr
	}

	for _, r := range b.Results {
		var prev []byte
		for _, i := range r.Keys {
			// If prefix is same, don't bother decoding key.
			if len(prev) > 0 && bytes.HasPrefix(i, prev) {
				continue
			}

			after, err := scan.readIndexKey(i)
			if err != nil {
				return 0, roachpb.NewError(err)
			}
			k := i[:len(i)-len(after)]
			if !bytes.Equal(k, prev) {
				prev = k
				rowCount++
			}
		}
	}

	return rowCount, nil
}
