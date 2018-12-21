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

package row

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

type checkFKConstraints bool

const (
	// CheckFKs can be passed to row writers to check fk validity.
	CheckFKs checkFKConstraints = true
	// SkipFKs can be passed to row writer to skip fk validity checks.
	SkipFKs checkFKConstraints = false
)

// Inserter abstracts the key/value operations for inserting table rows.
type Inserter struct {
	Helper                rowHelper
	InsertCols            []sqlbase.ColumnDescriptor
	InsertColIDtoRowIndex map[sqlbase.ColumnID]int
	Fks                   fkInsertHelper

	// For allocation avoidance.
	marshaled []roachpb.Value
	key       roachpb.Key
	valueBuf  []byte
	value     roachpb.Value
}

// MakeInserter creates a Inserter for the given table.
//
// insertCols must contain every column in the primary key.
func MakeInserter(
	txn *client.Txn,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	fkTables TableLookupsByID,
	insertCols []sqlbase.ColumnDescriptor,
	checkFKs checkFKConstraints,
	alloc *sqlbase.DatumAlloc,
) (Inserter, error) {
	ri := Inserter{
		Helper:                newRowHelper(tableDesc, tableDesc.WritableIndexes()),
		InsertCols:            insertCols,
		InsertColIDtoRowIndex: ColIDtoRowIndexFromCols(insertCols),
		marshaled:             make([]roachpb.Value, len(insertCols)),
	}

	for i, col := range tableDesc.PrimaryIndex.ColumnIDs {
		if _, ok := ri.InsertColIDtoRowIndex[col]; !ok {
			return Inserter{}, fmt.Errorf("missing %q primary key column", tableDesc.PrimaryIndex.ColumnNames[i])
		}
	}

	if checkFKs == CheckFKs {
		var err error
		if ri.Fks, err = makeFKInsertHelper(txn, tableDesc, fkTables,
			ri.InsertColIDtoRowIndex, alloc); err != nil {
			return ri, err
		}
	}
	return ri, nil
}

// insertCPutFn is used by insertRow when conflicts (i.e. the key already exists)
// should generate errors.
func insertCPutFn(
	ctx context.Context, b putter, key *roachpb.Key, value *roachpb.Value, traceKV bool,
) {
	// TODO(dan): We want do this V(2) log everywhere in sql. Consider making a
	// client.Batch wrapper instead of inlining it everywhere.
	if traceKV {
		log.VEventfDepth(ctx, 1, 2, "CPut %s -> %s", *key, value.PrettyPrint())
	}
	b.CPut(key, value, nil /* expValue */)
}

// insertPutFn is used by insertRow when conflicts should be ignored.
func insertPutFn(
	ctx context.Context, b putter, key *roachpb.Key, value *roachpb.Value, traceKV bool,
) {
	if traceKV {
		log.VEventfDepth(ctx, 1, 2, "Put %s -> %s", *key, value.PrettyPrint())
	}
	b.Put(key, value)
}

// insertDelFn is used by insertRow to delete existing rows.
func insertDelFn(ctx context.Context, b putter, key *roachpb.Key, traceKV bool) {
	if traceKV {
		log.VEventfDepth(ctx, 1, 2, "Del %s", *key)
	}
	b.Del(key)
}

// insertPutFn is used by insertRow when conflicts should be ignored.
func insertInvertedPutFn(
	ctx context.Context, b putter, key *roachpb.Key, value *roachpb.Value, traceKV bool,
) {
	if traceKV {
		log.VEventfDepth(ctx, 1, 2, "InitPut %s -> %s", *key, value.PrettyPrint())
	}
	b.InitPut(key, value, false)
}

type putter interface {
	CPut(key, value, expValue interface{})
	Put(key, value interface{})
	InitPut(key, value interface{}, failOnTombstones bool)
	Del(key ...interface{})
}

// InsertRow adds to the batch the kv operations necessary to insert a table row
// with the given values.
func (ri *Inserter) InsertRow(
	ctx context.Context,
	b putter,
	values []tree.Datum,
	overwrite bool,
	checkFKs checkFKConstraints,
	traceKV bool,
) error {
	if len(values) != len(ri.InsertCols) {
		return errors.Errorf("got %d values but expected %d", len(values), len(ri.InsertCols))
	}

	putFn := insertCPutFn
	if overwrite {
		putFn = insertPutFn
	}

	// Encode the values to the expected column type. This needs to
	// happen before index encoding because certain datum types (i.e. tuple)
	// cannot be used as index values.
	for i, val := range values {
		// Make sure the value can be written to the column before proceeding.
		var err error
		if ri.marshaled[i], err = sqlbase.MarshalColumnValue(ri.InsertCols[i], val); err != nil {
			return err
		}
	}

	if ri.Fks.checker != nil && checkFKs == CheckFKs {
		if err := ri.Fks.addAllIdxChecks(ctx, values, traceKV); err != nil {
			return err
		}
		if err := ri.Fks.checker.runCheck(ctx, nil, values); err != nil {
			return err
		}
	}

	primaryIndexKey, secondaryIndexEntries, err := ri.Helper.encodeIndexes(ri.InsertColIDtoRowIndex, values)
	if err != nil {
		return err
	}

	// Add the new values.
	ri.valueBuf, err = prepareInsertOrUpdateBatch(ctx, b,
		&ri.Helper, primaryIndexKey, ri.InsertCols,
		values, ri.InsertColIDtoRowIndex,
		ri.marshaled, ri.InsertColIDtoRowIndex,
		&ri.key, &ri.value, ri.valueBuf, putFn, overwrite, traceKV)
	if err != nil {
		return err
	}

	putFn = insertInvertedPutFn
	for i := range secondaryIndexEntries {
		e := &secondaryIndexEntries[i]
		putFn(ctx, b, &e.Key, &e.Value, traceKV)
	}

	return nil
}

// prepareInsertOrUpdateBatch constructs a KV batch that inserts or
// updates a row in KV.
// - batch is the KV batch where commands should be appended.
// - putFn is the functions that can append Put/CPut commands to the batch.
//   (must be adapted depending on whether 'overwrite' is set)
// - helper is the rowHelper that knows about the table being modified.
// - primaryIndexKey is the PK prefix for the current row.
// - fetchedCols is the list of schema columns that have been fetched
//   in preparation for this update.
// - values is the SQL-level row values that are being written.
// - marshaledValues contains the pre-encoded KV-level row values.
//   marshaledValues is only used when writing single column families.
//   Regardless of whether there are single column families,
//   pre-encoding must occur prior to calling this function to check whether
//   the encoding is _possible_ (i.e. values fit in the column types, etc).
// - valColIDMapping/marshaledColIDMapping is the mapping from column
//   IDs into positions of the slices values or marshaledValues.
// - kvKey and kvValues must be heap-allocated scratch buffers to write
//   roachpb.Key and roachpb.Value values.
// - rawValueBuf must be a scratch byte array. This must be reinitialized
//   to an empty slice on each call but can be preserved at its current
//   capacity to avoid allocations. The function returns the slice.
// - overwrite must be set to true for UPDATE and UPSERT.
// - traceKV is to be set to log the KV operations added to the batch.
func prepareInsertOrUpdateBatch(
	ctx context.Context,
	batch putter,
	helper *rowHelper,
	primaryIndexKey []byte,
	fetchedCols []sqlbase.ColumnDescriptor,
	values []tree.Datum,
	valColIDMapping map[sqlbase.ColumnID]int,
	marshaledValues []roachpb.Value,
	marshaledColIDMapping map[sqlbase.ColumnID]int,
	kvKey *roachpb.Key,
	kvValue *roachpb.Value,
	rawValueBuf []byte,
	putFn func(ctx context.Context, b putter, key *roachpb.Key, value *roachpb.Value, traceKV bool),
	overwrite, traceKV bool,
) ([]byte, error) {
	for i, family := range helper.TableDesc.Families {
		update := false
		for _, colID := range family.ColumnIDs {
			if _, ok := marshaledColIDMapping[colID]; ok {
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

		*kvKey = keys.MakeFamilyKey(primaryIndexKey, uint32(family.ID))
		if len(family.ColumnIDs) == 1 && family.ColumnIDs[0] == family.DefaultColumnID {
			// Storage optimization to store DefaultColumnID directly as a value. Also
			// backwards compatible with the original BaseFormatVersion.

			idx, ok := marshaledColIDMapping[family.DefaultColumnID]
			if !ok {
				continue
			}

			if marshaledValues[idx].RawBytes == nil {
				if overwrite {
					// If the new family contains a NULL value, then we must
					// delete any pre-existing row.
					insertDelFn(ctx, batch, kvKey, traceKV)
				}
			} else {
				// We only output non-NULL values. Non-existent column keys are
				// considered NULL during scanning and the row sentinel ensures we know
				// the row exists.
				putFn(ctx, batch, kvKey, &marshaledValues[idx], traceKV)
			}

			continue
		}

		rawValueBuf = rawValueBuf[:0]

		var lastColID sqlbase.ColumnID
		familySortedColumnIDs, ok := helper.sortedColumnFamily(family.ID)
		if !ok {
			return nil, pgerror.NewAssertionErrorf("invalid family sorted column id map")
		}
		for _, colID := range familySortedColumnIDs {
			idx, ok := valColIDMapping[colID]
			if !ok || values[idx] == tree.DNull {
				// Column not being updated or inserted.
				continue
			}

			if skip, err := helper.skipColumnInPK(colID, family.ID, values[idx]); err != nil {
				return nil, err
			} else if skip {
				continue
			}

			col := fetchedCols[idx]

			if lastColID > col.ID {
				return nil, pgerror.NewAssertionErrorf("cannot write column id %d after %d", col.ID, lastColID)
			}
			colIDDiff := col.ID - lastColID
			lastColID = col.ID
			var err error
			rawValueBuf, err = sqlbase.EncodeTableValue(rawValueBuf, colIDDiff, values[idx], nil)
			if err != nil {
				return nil, err
			}
		}

		if family.ID != 0 && len(rawValueBuf) == 0 {
			if overwrite {
				// The family might have already existed but every column in it is being
				// set to NULL, so delete it.
				insertDelFn(ctx, batch, kvKey, traceKV)
			}
		} else {
			kvValue.SetTuple(rawValueBuf)
			putFn(ctx, batch, kvKey, kvValue, traceKV)
		}

		*kvKey = nil
	}

	return rawValueBuf, nil
}

// EncodeIndexesForRow encodes the provided values into their primary and
// secondary index keys. The secondaryIndexEntries are only valid until the next
// call to EncodeIndexesForRow.
func (ri *Inserter) EncodeIndexesForRow(
	values []tree.Datum,
) (primaryIndexKey []byte, secondaryIndexEntries []sqlbase.IndexEntry, err error) {
	return ri.Helper.encodeIndexes(ri.InsertColIDtoRowIndex, values)
}

// Updater abstracts the key/value operations for updating table rows.
type Updater struct {
	Helper                rowHelper
	DeleteHelper          *rowHelper
	FetchCols             []sqlbase.ColumnDescriptor
	FetchColIDtoRowIndex  map[sqlbase.ColumnID]int
	UpdateCols            []sqlbase.ColumnDescriptor
	updateColIDtoRowIndex map[sqlbase.ColumnID]int
	primaryKeyColChange   bool

	// rd and ri are used when the update this Updater is created for modifies
	// the primary key of the table. In that case, rows must be deleted and
	// re-added instead of merely updated, since the keys are changing.
	rd Deleter
	ri Inserter

	Fks      fkUpdateHelper
	cascader *cascader

	// For allocation avoidance.
	marshaled       []roachpb.Value
	newValues       []tree.Datum
	key             roachpb.Key
	indexEntriesBuf []sqlbase.IndexEntry
	valueBuf        []byte
	value           roachpb.Value
}

type rowUpdaterType int

const (
	// UpdaterDefault indicates that an Updater should update everything
	// about a row, including secondary indexes.
	UpdaterDefault rowUpdaterType = 0
	// UpdaterOnlyColumns indicates that an Updater should only update the
	// columns of a row.
	UpdaterOnlyColumns rowUpdaterType = 1
)

// MakeUpdater creates a Updater for the given table.
//
// UpdateCols are the columns being updated and correspond to the updateValues
// that will be passed to UpdateRow.
//
// The returned Updater contains a FetchCols field that defines the
// expectation of which values are passed as oldValues to UpdateRow. All the columns
// passed in requestedCols will be included in FetchCols at the beginning.
func MakeUpdater(
	txn *client.Txn,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	fkTables TableLookupsByID,
	updateCols []sqlbase.ColumnDescriptor,
	requestedCols []sqlbase.ColumnDescriptor,
	updateType rowUpdaterType,
	evalCtx *tree.EvalContext,
	alloc *sqlbase.DatumAlloc,
) (Updater, error) {
	rowUpdater, err := makeUpdaterWithoutCascader(
		txn, tableDesc, fkTables, updateCols, requestedCols, updateType, alloc,
	)
	if err != nil {
		return Updater{}, err
	}
	rowUpdater.cascader, err = makeUpdateCascader(
		txn, tableDesc, fkTables, updateCols, evalCtx, alloc,
	)
	if err != nil {
		return Updater{}, err
	}
	return rowUpdater, nil
}

type returnTrue struct{}

func (returnTrue) Error() string { panic("unimplemented") }

var returnTruePseudoError error = returnTrue{}

// makeUpdaterWithoutCascader is the same function as MakeUpdater but does not
// create a cascader.
func makeUpdaterWithoutCascader(
	txn *client.Txn,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	fkTables TableLookupsByID,
	updateCols []sqlbase.ColumnDescriptor,
	requestedCols []sqlbase.ColumnDescriptor,
	updateType rowUpdaterType,
	alloc *sqlbase.DatumAlloc,
) (Updater, error) {
	updateColIDtoRowIndex := ColIDtoRowIndexFromCols(updateCols)

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
		if updateType == UpdaterOnlyColumns {
			// Only update columns.
			return false
		}
		// If the primary key changed, we need to update all of them.
		if primaryKeyColChange {
			return true
		}
		return index.RunOverAllColumns(func(id sqlbase.ColumnID) error {
			if _, ok := updateColIDtoRowIndex[id]; ok {
				return returnTruePseudoError
			}
			return nil
		}) != nil
	}

	writableIndexes := tableDesc.WritableIndexes()
	includeIndexes := make([]sqlbase.IndexDescriptor, 0, len(writableIndexes))
	for _, index := range writableIndexes {
		if needsUpdate(index) {
			includeIndexes = append(includeIndexes, index)
		}
	}

	// Columns of the table to update, including those in delete/write-only state
	tableCols := tableDesc.DeletableColumns()

	var deleteOnlyIndexes []sqlbase.IndexDescriptor
	for _, idx := range tableDesc.DeleteOnlyIndexes() {
		if needsUpdate(idx) {
			if deleteOnlyIndexes == nil {
				// Allocate at most once.
				deleteOnlyIndexes = make([]sqlbase.IndexDescriptor, 0, len(tableDesc.DeleteOnlyIndexes()))
			}
			deleteOnlyIndexes = append(deleteOnlyIndexes, idx)
		}
	}

	var deleteOnlyHelper *rowHelper
	if len(deleteOnlyIndexes) > 0 {
		rh := newRowHelper(tableDesc, deleteOnlyIndexes)
		deleteOnlyHelper = &rh
	}

	ru := Updater{
		Helper:                newRowHelper(tableDesc, includeIndexes),
		DeleteHelper:          deleteOnlyHelper,
		UpdateCols:            updateCols,
		updateColIDtoRowIndex: updateColIDtoRowIndex,
		primaryKeyColChange:   primaryKeyColChange,
		marshaled:             make([]roachpb.Value, len(updateCols)),
		newValues:             make([]tree.Datum, len(tableCols)),
	}

	if primaryKeyColChange {
		// These fields are only used when the primary key is changing.
		// When changing the primary key, we delete the old values and reinsert
		// them, so request them all.
		var err error
		if ru.rd, err = makeRowDeleterWithoutCascader(
			txn, tableDesc, fkTables, tableCols, SkipFKs, alloc,
		); err != nil {
			return Updater{}, err
		}
		ru.FetchCols = ru.rd.FetchCols
		ru.FetchColIDtoRowIndex = ColIDtoRowIndexFromCols(ru.FetchCols)
		if ru.ri, err = MakeInserter(txn, tableDesc, fkTables,
			tableCols, SkipFKs, alloc); err != nil {
			return Updater{}, err
		}
	} else {
		ru.FetchCols = requestedCols[:len(requestedCols):len(requestedCols)]
		ru.FetchColIDtoRowIndex = ColIDtoRowIndexFromCols(ru.FetchCols)

		// maybeAddCol adds the provided column to ru.FetchCols and
		// ru.FetchColIDtoRowIndex if it isn't already present.
		maybeAddCol := func(colID sqlbase.ColumnID) error {
			if _, ok := ru.FetchColIDtoRowIndex[colID]; !ok {
				col, _, err := tableDesc.FindReadableColumnByID(colID)
				if err != nil {
					return err
				}
				ru.FetchColIDtoRowIndex[col.ID] = len(ru.FetchCols)
				ru.FetchCols = append(ru.FetchCols, *col)
			}
			return nil
		}

		// Fetch all columns in the primary key so that we can construct the
		// keys when writing out the new kvs to the primary index.
		for _, colID := range tableDesc.PrimaryIndex.ColumnIDs {
			if err := maybeAddCol(colID); err != nil {
				return Updater{}, err
			}
		}

		// If any part of a column family is being updated, fetch all columns in
		// that column family so that we can reconstruct the column family with
		// the updated columns before writing it.
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
						return Updater{}, err
					}
				}
			}
		}

		// Fetch all columns from indices that are being update so that they can
		// be used to create the new kv pairs for those indices.
		for _, index := range includeIndexes {
			if err := index.RunOverAllColumns(maybeAddCol); err != nil {
				return Updater{}, err
			}
		}
		for _, index := range deleteOnlyIndexes {
			if err := index.RunOverAllColumns(maybeAddCol); err != nil {
				return Updater{}, err
			}
		}
	}

	var err error
	if ru.Fks, err = makeFKUpdateHelper(txn, tableDesc, fkTables,
		ru.FetchColIDtoRowIndex, alloc); err != nil {
		return Updater{}, err
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
func (ru *Updater) UpdateRow(
	ctx context.Context,
	b *client.Batch,
	oldValues []tree.Datum,
	updateValues []tree.Datum,
	checkFKs checkFKConstraints,
	traceKV bool,
) ([]tree.Datum, error) {
	batch := b
	if ru.cascader != nil {
		batch = ru.cascader.txn.NewBatch()
	}

	if len(oldValues) != len(ru.FetchCols) {
		return nil, errors.Errorf("got %d values but expected %d", len(oldValues), len(ru.FetchCols))
	}
	if len(updateValues) != len(ru.UpdateCols) {
		return nil, errors.Errorf("got %d values but expected %d", len(updateValues), len(ru.UpdateCols))
	}

	primaryIndexKey, oldSecondaryIndexEntries, err := ru.Helper.encodeIndexes(ru.FetchColIDtoRowIndex, oldValues)
	if err != nil {
		return nil, err
	}
	var deleteOldSecondaryIndexEntries []sqlbase.IndexEntry
	if ru.DeleteHelper != nil {
		_, deleteOldSecondaryIndexEntries, err = ru.DeleteHelper.encodeIndexes(ru.FetchColIDtoRowIndex, oldValues)
		if err != nil {
			return nil, err
		}
	}
	// The secondary index entries returned by rowHelper.encodeIndexes are only
	// valid until the next call to encodeIndexes. We need to copy them so that
	// we can compare against the new secondary index entries.
	oldSecondaryIndexEntries = append(ru.indexEntriesBuf[:0], oldSecondaryIndexEntries...)
	ru.indexEntriesBuf = oldSecondaryIndexEntries

	// Check that the new value types match the column types. This needs to
	// happen before index encoding because certain datum types (i.e. tuple)
	// cannot be used as index values.
	for i, val := range updateValues {
		if ru.marshaled[i], err = sqlbase.MarshalColumnValue(ru.UpdateCols[i], val); err != nil {
			return nil, err
		}
	}

	// Update the row values.
	copy(ru.newValues, oldValues)
	for i, updateCol := range ru.UpdateCols {
		ru.newValues[ru.FetchColIDtoRowIndex[updateCol.ID]] = updateValues[i]
	}

	rowPrimaryKeyChanged := false
	var newSecondaryIndexEntries []sqlbase.IndexEntry
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
		if err := ru.rd.DeleteRow(ctx, batch, oldValues, SkipFKs, traceKV); err != nil {
			return nil, err
		}
		if err := ru.ri.InsertRow(
			ctx, batch, ru.newValues, false /* ignoreConflicts */, SkipFKs, traceKV,
		); err != nil {
			return nil, err
		}

		ru.Fks.addCheckForIndex(ru.Helper.TableDesc.PrimaryIndex.ID, ru.Helper.TableDesc.PrimaryIndex.Type)
		for i := range ru.Helper.Indexes {
			if !bytes.Equal(newSecondaryIndexEntries[i].Key, oldSecondaryIndexEntries[i].Key) {
				ru.Fks.addCheckForIndex(ru.Helper.Indexes[i].ID, ru.Helper.Indexes[i].Type)
			}
		}

		if ru.cascader != nil {
			if err := ru.cascader.txn.Run(ctx, batch); err != nil {
				return nil, ConvertBatchError(ctx, ru.Helper.TableDesc, batch)
			}
			if err := ru.cascader.cascadeAll(
				ctx,
				ru.Helper.TableDesc,
				tree.Datums(oldValues),
				tree.Datums(ru.newValues),
				ru.FetchColIDtoRowIndex,
				traceKV,
			); err != nil {
				return nil, err
			}
		}

		if checkFKs == CheckFKs {
			if err := ru.Fks.addIndexChecks(ctx, oldValues, ru.newValues, traceKV); err != nil {
				return nil, err
			}
			if !ru.Fks.hasFKs() {
				return ru.newValues, nil
			}
			if err := ru.Fks.checker.runCheck(ctx, oldValues, ru.newValues); err != nil {
				return nil, err
			}
		}

		return ru.newValues, nil
	}

	// Add the new values.
	ru.valueBuf, err = prepareInsertOrUpdateBatch(ctx, b,
		&ru.Helper, primaryIndexKey, ru.FetchCols,
		ru.newValues, ru.FetchColIDtoRowIndex,
		ru.marshaled, ru.updateColIDtoRowIndex,
		&ru.key, &ru.value, ru.valueBuf, insertPutFn, true /* overwrite */, traceKV)
	if err != nil {
		return ru.newValues, nil
	}

	// Update secondary indexes.
	// We're iterating through all of the indexes, which should have corresponding entries in both oldSecondaryIndexEntries
	// and newSecondaryIndexEntries. Inverted indexes could potentially have more entries at the end of both and we will
	// update those separately.
	for i, index := range ru.Helper.Indexes {
		oldSecondaryIndexEntry := oldSecondaryIndexEntries[i]
		newSecondaryIndexEntry := newSecondaryIndexEntries[i]

		// We're skipping inverted indexes in this loop, but appending the inverted index entry to the back of
		// newSecondaryIndexEntries to process later. For inverted indexes we need to remove all old entries before adding
		// new ones.
		if index.Type == sqlbase.IndexDescriptor_INVERTED {
			newSecondaryIndexEntries = append(newSecondaryIndexEntries, newSecondaryIndexEntry)
			oldSecondaryIndexEntries = append(oldSecondaryIndexEntries, oldSecondaryIndexEntry)

			continue
		}

		var expValue interface{}
		if !bytes.Equal(newSecondaryIndexEntry.Key, oldSecondaryIndexEntry.Key) {
			ru.Fks.addCheckForIndex(ru.Helper.Indexes[i].ID, ru.Helper.Indexes[i].Type)
			if traceKV {
				log.VEventf(ctx, 2, "Del %s", keys.PrettyPrint(ru.Helper.secIndexValDirs[i], oldSecondaryIndexEntry.Key))
			}
			batch.Del(oldSecondaryIndexEntry.Key)
		} else if !newSecondaryIndexEntry.Value.EqualData(oldSecondaryIndexEntry.Value) {
			expValue = &oldSecondaryIndexEntry.Value
		} else {
			continue
		}

		if traceKV {
			log.VEventf(ctx, 2, "CPut %s -> %v", keys.PrettyPrint(ru.Helper.secIndexValDirs[i], newSecondaryIndexEntry.Key), newSecondaryIndexEntry.Value.PrettyPrint())
		}
		batch.CPut(newSecondaryIndexEntry.Key, &newSecondaryIndexEntry.Value, expValue)
	}

	// We're deleting indexes in a delete only state. We're bounding this by the number of indexes because inverted
	// indexed will be handled separately.
	if ru.DeleteHelper != nil {
		for _, deletedSecondaryIndexEntry := range deleteOldSecondaryIndexEntries {
			if traceKV {
				log.VEventf(ctx, 2, "Del %s", deletedSecondaryIndexEntry.Key)
			}
			batch.Del(deletedSecondaryIndexEntry.Key)
		}
	}

	// We're removing all of the inverted index entries from the row being updated.
	for i := len(ru.Helper.Indexes); i < len(oldSecondaryIndexEntries); i++ {
		if traceKV {
			log.VEventf(ctx, 2, "Del %s", oldSecondaryIndexEntries[i].Key)
		}
		batch.Del(oldSecondaryIndexEntries[i].Key)
	}

	putFn := insertInvertedPutFn
	// We're adding all of the inverted index entries from the row being updated.
	for i := len(ru.Helper.Indexes); i < len(newSecondaryIndexEntries); i++ {
		putFn(ctx, b, &newSecondaryIndexEntries[i].Key, &newSecondaryIndexEntries[i].Value, traceKV)
	}

	if ru.cascader != nil {
		if err := ru.cascader.txn.Run(ctx, batch); err != nil {
			return nil, ConvertBatchError(ctx, ru.Helper.TableDesc, batch)
		}
		if err := ru.cascader.cascadeAll(
			ctx,
			ru.Helper.TableDesc,
			tree.Datums(oldValues),
			tree.Datums(ru.newValues),
			ru.FetchColIDtoRowIndex,
			traceKV,
		); err != nil {
			return nil, err
		}
	}

	if checkFKs == CheckFKs {
		if err := ru.Fks.addIndexChecks(ctx, oldValues, ru.newValues, traceKV); err != nil {
			return nil, err
		}
		if !ru.Fks.hasFKs() {
			return ru.newValues, nil
		}
		if err := ru.Fks.checker.runCheck(ctx, oldValues, ru.newValues); err != nil {
			return nil, err
		}
	}

	return ru.newValues, nil
}

// IsColumnOnlyUpdate returns true if this Updater is only updating column
// data (in contrast to updating the primary key or other indexes).
func (ru *Updater) IsColumnOnlyUpdate() bool {
	// TODO(dan): This is used in the schema change backfill to assert that it was
	// configured correctly and will not be doing things it shouldn't. This is an
	// unfortunate bleeding of responsibility and indicates the abstraction could
	// be improved. Specifically, Updater currently has two responsibilities
	// (computing which indexes need to be updated and mapping sql rows to k/v
	// operations) and these should be split.
	return !ru.primaryKeyColChange && ru.DeleteHelper == nil && len(ru.Helper.Indexes) == 0
}

// Deleter abstracts the key/value operations for deleting table rows.
type Deleter struct {
	Helper               rowHelper
	FetchCols            []sqlbase.ColumnDescriptor
	FetchColIDtoRowIndex map[sqlbase.ColumnID]int
	Fks                  fkDeleteHelper
	cascader             *cascader
	// For allocation avoidance.
	key roachpb.Key
}

// MakeDeleter creates a Deleter for the given table.
//
// The returned Deleter contains a FetchCols field that defines the
// expectation of which values are passed as values to DeleteRow. Any column
// passed in requestedCols will be included in FetchCols.
func MakeDeleter(
	txn *client.Txn,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	fkTables TableLookupsByID,
	requestedCols []sqlbase.ColumnDescriptor,
	checkFKs checkFKConstraints,
	evalCtx *tree.EvalContext,
	alloc *sqlbase.DatumAlloc,
) (Deleter, error) {
	rowDeleter, err := makeRowDeleterWithoutCascader(
		txn, tableDesc, fkTables, requestedCols, checkFKs, alloc,
	)
	if err != nil {
		return Deleter{}, err
	}
	if checkFKs == CheckFKs {
		var err error
		rowDeleter.cascader, err = makeDeleteCascader(txn, tableDesc, fkTables, evalCtx, alloc)
		if err != nil {
			return Deleter{}, err
		}
	}
	return rowDeleter, nil
}

// makeRowDeleterWithoutCascader creates a rowDeleter but does not create an
// additional cascader.
func makeRowDeleterWithoutCascader(
	txn *client.Txn,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	fkTables TableLookupsByID,
	requestedCols []sqlbase.ColumnDescriptor,
	checkFKs checkFKConstraints,
	alloc *sqlbase.DatumAlloc,
) (Deleter, error) {
	indexes := tableDesc.DeletableIndexes()

	fetchCols := requestedCols[:len(requestedCols):len(requestedCols)]
	fetchColIDtoRowIndex := ColIDtoRowIndexFromCols(fetchCols)

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
			return Deleter{}, err
		}
	}
	for _, index := range indexes {
		for _, colID := range index.ColumnIDs {
			if err := maybeAddCol(colID); err != nil {
				return Deleter{}, err
			}
		}
		// The extra columns are needed to fix #14601.
		for _, colID := range index.ExtraColumnIDs {
			if err := maybeAddCol(colID); err != nil {
				return Deleter{}, err
			}
		}
	}

	rd := Deleter{
		Helper:               newRowHelper(tableDesc, indexes),
		FetchCols:            fetchCols,
		FetchColIDtoRowIndex: fetchColIDtoRowIndex,
	}
	if checkFKs == CheckFKs {
		var err error
		if rd.Fks, err = makeFKDeleteHelper(txn, tableDesc, fkTables,
			fetchColIDtoRowIndex, alloc); err != nil {
			return Deleter{}, err
		}
	}

	return rd, nil
}

// DeleteRow adds to the batch the kv operations necessary to delete a table row
// with the given values. It also will cascade as required and check for
// orphaned rows. The bytesMonitor is only used if cascading/fk checking and can
// be nil if not.
func (rd *Deleter) DeleteRow(
	ctx context.Context,
	b *client.Batch,
	values []tree.Datum,
	checkFKs checkFKConstraints,
	traceKV bool,
) error {
	primaryIndexKey, secondaryIndexEntries, err := rd.Helper.encodeIndexes(rd.FetchColIDtoRowIndex, values)
	if err != nil {
		return err
	}

	// Delete the row from any secondary indices.
	for i, secondaryIndexEntry := range secondaryIndexEntries {
		if traceKV {
			log.VEventf(ctx, 2, "Del %s", keys.PrettyPrint(rd.Helper.secIndexValDirs[i], secondaryIndexEntry.Key))
		}
		b.Del(&secondaryIndexEntry.Key)
	}

	// Delete the row.
	for i, family := range rd.Helper.TableDesc.Families {
		if i > 0 {
			// HACK: MakeFamilyKey appends to its argument, so on every loop iteration
			// after the first, trim primaryIndexKey so nothing gets overwritten.
			// TODO(dan): Instead of this, use something like engine.ChunkAllocator.
			primaryIndexKey = primaryIndexKey[:len(primaryIndexKey):len(primaryIndexKey)]
		}
		rd.key = keys.MakeFamilyKey(primaryIndexKey, uint32(family.ID))
		if traceKV {
			log.VEventf(ctx, 2, "Del %s", keys.PrettyPrint(rd.Helper.primIndexValDirs, rd.key))
		}
		b.Del(&rd.key)
		rd.key = nil
	}

	if rd.cascader != nil {
		if err := rd.cascader.cascadeAll(
			ctx,
			rd.Helper.TableDesc,
			tree.Datums(values),
			nil, /* updatedValues */
			rd.FetchColIDtoRowIndex,
			traceKV,
		); err != nil {
			return err
		}
	}
	if rd.Fks.checker != nil && checkFKs == CheckFKs {
		if err := rd.Fks.addAllIdxChecks(ctx, values, traceKV); err != nil {
			return err
		}
		return rd.Fks.checker.runCheck(ctx, values, nil)
	}
	return nil
}

// DeleteIndexRow adds to the batch the kv operations necessary to delete a
// table row from the given index.
func (rd *Deleter) DeleteIndexRow(
	ctx context.Context,
	b *client.Batch,
	idx *sqlbase.IndexDescriptor,
	values []tree.Datum,
	traceKV bool,
) error {
	if rd.Fks.checker != nil {
		if err := rd.Fks.addAllIdxChecks(ctx, values, traceKV); err != nil {
			return err
		}
		if err := rd.Fks.checker.runCheck(ctx, values, nil); err != nil {
			return err
		}
	}
	secondaryIndexEntry, err := sqlbase.EncodeSecondaryIndex(
		rd.Helper.TableDesc.TableDesc(), idx, rd.FetchColIDtoRowIndex, values)
	if err != nil {
		return err
	}

	for _, entry := range secondaryIndexEntry {
		if traceKV {
			log.VEventf(ctx, 2, "Del %s", entry.Key)
		}
		b.Del(entry.Key)
	}
	return nil
}

// ColIDtoRowIndexFromCols groups a slice of ColumnDescriptors by their ID
// field, returning a map from ID to ColumnDescriptor. It assumes there are no
// duplicate descriptors in the input.
func ColIDtoRowIndexFromCols(cols []sqlbase.ColumnDescriptor) map[sqlbase.ColumnID]int {
	colIDtoRowIndex := make(map[sqlbase.ColumnID]int, len(cols))
	for i, col := range cols {
		colIDtoRowIndex[col.ID] = i
	}
	return colIDtoRowIndex
}
