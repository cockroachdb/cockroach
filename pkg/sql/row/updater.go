// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package row

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Updater abstracts the key/value operations for updating table rows.
type Updater struct {
	Helper                rowHelper
	DeleteHelper          *rowHelper
	FetchCols             []sqlbase.ColumnDescriptor
	FetchColIDtoRowIndex  map[sqlbase.ColumnID]int
	UpdateCols            []sqlbase.ColumnDescriptor
	UpdateColIDtoRowIndex map[sqlbase.ColumnID]int
	primaryKeyColChange   bool

	// rd and ri are used when the update this Updater is created for modifies
	// the primary key of the table. In that case, rows must be deleted and
	// re-added instead of merely updated, since the keys are changing.
	rd Deleter
	ri Inserter

	Fks      fkExistenceCheckForUpdate
	cascader *cascader

	// For allocation avoidance.
	marshaled       []roachpb.Value
	newValues       []tree.Datum
	key             roachpb.Key
	valueBuf        []byte
	value           roachpb.Value
	oldIndexEntries [][]sqlbase.IndexEntry
	newIndexEntries [][]sqlbase.IndexEntry
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
	ctx context.Context,
	txn *kv.Txn,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	fkTables FkTableMetadata,
	updateCols []sqlbase.ColumnDescriptor,
	requestedCols []sqlbase.ColumnDescriptor,
	updateType rowUpdaterType,
	checkFKs checkFKConstraints,
	evalCtx *tree.EvalContext,
	alloc *sqlbase.DatumAlloc,
) (Updater, error) {
	rowUpdater, err := makeUpdaterWithoutCascader(
		ctx, txn, tableDesc, fkTables, updateCols, requestedCols, updateType, checkFKs, alloc,
	)
	if err != nil {
		return Updater{}, err
	}
	if checkFKs == CheckFKs {
		rowUpdater.cascader, err = makeUpdateCascader(
			ctx, txn, tableDesc, fkTables, updateCols, evalCtx, alloc,
		)
		if err != nil {
			return Updater{}, err
		}
	}
	return rowUpdater, nil
}

type returnTrue struct{}

func (returnTrue) Error() string { panic(errors.AssertionFailedf("unimplemented")) }

var returnTruePseudoError error = returnTrue{}

// makeUpdaterWithoutCascader is the same function as MakeUpdater but does not
// create a cascader.
func makeUpdaterWithoutCascader(
	ctx context.Context,
	txn *kv.Txn,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	fkTables FkTableMetadata,
	updateCols []sqlbase.ColumnDescriptor,
	requestedCols []sqlbase.ColumnDescriptor,
	updateType rowUpdaterType,
	checkFKs checkFKConstraints,
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
		UpdateColIDtoRowIndex: updateColIDtoRowIndex,
		primaryKeyColChange:   primaryKeyColChange,
		marshaled:             make([]roachpb.Value, len(updateCols)),
		oldIndexEntries:       make([][]sqlbase.IndexEntry, len(includeIndexes)),
		newIndexEntries:       make([][]sqlbase.IndexEntry, len(includeIndexes)),
	}

	if primaryKeyColChange {
		// These fields are only used when the primary key is changing.
		// When changing the primary key, we delete the old values and reinsert
		// them, so request them all.
		var err error
		if ru.rd, err = makeRowDeleterWithoutCascader(
			ctx, txn, tableDesc, fkTables, tableCols, SkipFKs, alloc,
		); err != nil {
			return Updater{}, err
		}
		ru.FetchCols = ru.rd.FetchCols
		ru.FetchColIDtoRowIndex = ColIDtoRowIndexFromCols(ru.FetchCols)
		if ru.ri, err = MakeInserter(
			ctx, txn, tableDesc, tableCols, SkipFKs, nil /* fkTables */, alloc,
		); err != nil {
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
		for i := range tableDesc.Families {
			family := &tableDesc.Families[i]
			familyBeingUpdated := false
			for _, colID := range family.ColumnIDs {
				if _, ok := ru.UpdateColIDtoRowIndex[colID]; ok {
					familyBeingUpdated = true
					break
				}
			}
			if familyBeingUpdated {
				for _, colID := range family.ColumnIDs {
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

	// If we are fetching from specific families, we might get
	// less columns than in the table. So we cannot assign this to
	// have length len(tableCols).
	ru.newValues = make(tree.Datums, len(ru.FetchCols))

	if checkFKs == CheckFKs {
		var err error
		if primaryKeyColChange {
			updateCols = nil
		}
		if ru.Fks, err = makeFkExistenceCheckHelperForUpdate(ctx, txn, tableDesc, fkTables,
			updateCols, ru.FetchColIDtoRowIndex, alloc); err != nil {
			return Updater{}, err
		}
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
	batch *kv.Batch,
	oldValues []tree.Datum,
	updateValues []tree.Datum,
	checkFKs checkFKConstraints,
	traceKV bool,
) ([]tree.Datum, error) {
	if ru.cascader != nil {
		batch = ru.cascader.txn.NewBatch()
	}

	if len(oldValues) != len(ru.FetchCols) {
		return nil, errors.Errorf("got %d values but expected %d", len(oldValues), len(ru.FetchCols))
	}
	if len(updateValues) != len(ru.UpdateCols) {
		return nil, errors.Errorf("got %d values but expected %d", len(updateValues), len(ru.UpdateCols))
	}

	primaryIndexKey, err := ru.Helper.encodePrimaryIndex(ru.FetchColIDtoRowIndex, oldValues)
	if err != nil {
		return nil, err
	}
	var deleteOldSecondaryIndexEntries []sqlbase.IndexEntry
	if ru.DeleteHelper != nil {
		// We want to include empty k/v pairs because we want
		// to delete all k/v's for this row. By setting includeEmpty
		// to true, we will get a k/v pair for each family in the row,
		// which will guarantee that we delete all the k/v's in this row.
		// N.B. that setting includeEmpty to true will sometimes cause
		// deletes of keys that aren't present. We choose to make this
		// compromise in order to avoid having to read all values of
		// the row that is being updated.
		_, deleteOldSecondaryIndexEntries, err = ru.DeleteHelper.encodeIndexes(
			ru.FetchColIDtoRowIndex, oldValues, true /* includeEmpty */)
		if err != nil {
			return nil, err
		}
	}

	// Check that the new value types match the column types. This needs to
	// happen before index encoding because certain datum types (i.e. tuple)
	// cannot be used as index values.
	for i, val := range updateValues {
		if ru.marshaled[i], err = sqlbase.MarshalColumnValue(&ru.UpdateCols[i], val); err != nil {
			return nil, err
		}
	}

	// Update the row values.
	copy(ru.newValues, oldValues)
	for i, updateCol := range ru.UpdateCols {
		ru.newValues[ru.FetchColIDtoRowIndex[updateCol.ID]] = updateValues[i]
	}

	rowPrimaryKeyChanged := false
	if ru.primaryKeyColChange {
		var newPrimaryIndexKey []byte
		newPrimaryIndexKey, err =
			ru.Helper.encodePrimaryIndex(ru.FetchColIDtoRowIndex, ru.newValues)
		if err != nil {
			return nil, err
		}
		rowPrimaryKeyChanged = !bytes.Equal(primaryIndexKey, newPrimaryIndexKey)
	}

	for i := range ru.Helper.Indexes {
		// We don't want to insert any empty k/v's, so set includeEmpty to false.
		// Consider the following case:
		// TABLE t (
		//   x INT PRIMARY KEY, y INT, z INT, w INT,
		//   INDEX (y) STORING (z, w),
		//   FAMILY (x), FAMILY (y), FAMILY (z), FAMILY (w)
		//)
		// If we are to perform an update on row (1, 2, 3, NULL),
		// the k/v pair for index i that encodes column w would have
		// an empty value because w is null and the sole resident
		// of that family. We want to ensure that we don't insert
		// empty k/v pairs during the process of the update, so
		// set includeEmpty to false while generating the old
		// and new index entries.
		ru.oldIndexEntries[i], err = sqlbase.EncodeSecondaryIndex(
			ru.Helper.TableDesc.TableDesc(),
			&ru.Helper.Indexes[i],
			ru.FetchColIDtoRowIndex,
			oldValues,
			false, /* includeEmpty */
		)
		if err != nil {
			return nil, err
		}
		ru.newIndexEntries[i], err = sqlbase.EncodeSecondaryIndex(
			ru.Helper.TableDesc.TableDesc(),
			&ru.Helper.Indexes[i],
			ru.FetchColIDtoRowIndex,
			ru.newValues,
			false, /* includeEmpty */
		)
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

		if ru.Fks.checker != nil {
			ru.Fks.addCheckForIndex(ru.Helper.TableDesc.PrimaryIndex.ID, ru.Helper.TableDesc.PrimaryIndex.Type)
			for i := range ru.Helper.Indexes {
				if ru.Helper.Indexes[i].Type == sqlbase.IndexDescriptor_INVERTED {
					// We ignore FK existence checks for inverted indexes.
					//
					// TODO(knz): verify that this is indeed correct.
					continue
				}
				// * We always will have at least 1 entry in the index, so indexing 0 is safe.
				// * The only difference between column family 0 vs other families encodings is
				//   just the family key ending of the key, so if index[0] is different, the other
				//   index entries will be different as well.
				if !bytes.Equal(ru.newIndexEntries[i][0].Key, ru.oldIndexEntries[i][0].Key) {
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
		}

		if checkFKs {
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
	ru.valueBuf, err = prepareInsertOrUpdateBatch(ctx, batch,
		&ru.Helper, primaryIndexKey, ru.FetchCols,
		ru.newValues, ru.FetchColIDtoRowIndex,
		ru.marshaled, ru.UpdateColIDtoRowIndex,
		&ru.key, &ru.value, ru.valueBuf, insertPutFn, true /* overwrite */, traceKV)
	if err != nil {
		return nil, err
	}

	// Update secondary indexes.
	// We're iterating through all of the indexes, which should have corresponding entries
	// in the new and old values.
	for i := range ru.Helper.Indexes {
		index := &ru.Helper.Indexes[i]
		if index.Type == sqlbase.IndexDescriptor_FORWARD {
			oldIdx, newIdx := 0, 0
			oldEntries, newEntries := ru.oldIndexEntries[i], ru.newIndexEntries[i]
			// The index entries for a particular index are stored in
			// family sorted order. We use this fact to update rows.
			// The algorithm to update a row using the old k/v pairs
			// for the row and the new k/v pairs for the row is very
			// similar to the algorithm to merge two sorted lists.
			// We move in lock step through the entries, and potentially
			// update k/v's that belong to the same family.
			// If we are in the case where there exists a family's k/v
			// in the old entries but not the new entries, we need to
			// delete that k/v. If we are in the case where a family's
			// k/v exists in the new index entries, then we need to just
			// insert that new k/v.
			for oldIdx < len(oldEntries) && newIdx < len(newEntries) {
				oldEntry, newEntry := &oldEntries[oldIdx], &newEntries[newIdx]
				if oldEntry.Family == newEntry.Family {
					// If the families are equal, then check if the keys have changed. If so, delete the old key.
					// Then, issue a CPut for the new value of the key if the value has changed.
					// Because the indexes will always have a k/v for family 0, it suffices to only
					// add foreign key checks in this case, because we are guaranteed to enter here.
					oldIdx++
					newIdx++
					var expValue *roachpb.Value
					if !bytes.Equal(oldEntry.Key, newEntry.Key) {
						ru.Fks.addCheckForIndex(index.ID, index.Type)
						if traceKV {
							log.VEventf(ctx, 2, "Del %s", keys.PrettyPrint(ru.Helper.secIndexValDirs[i], oldEntry.Key))
						}
						batch.Del(oldEntry.Key)
					} else if !newEntry.Value.EqualData(oldEntry.Value) {
						expValue = &oldEntry.Value
					} else {
						continue
					}
					if traceKV {
						k := keys.PrettyPrint(ru.Helper.secIndexValDirs[i], newEntry.Key)
						v := newEntry.Value.PrettyPrint()
						if expValue != nil {
							log.VEventf(ctx, 2, "CPut %s -> %v (replacing %v, if exists)", k, v, expValue)
						} else {
							log.VEventf(ctx, 2, "CPut %s -> %v (expecting does not exist)", k, v)
						}
					}
					batch.CPutAllowingIfNotExists(newEntry.Key, &newEntry.Value, expValue)
				} else if oldEntry.Family < newEntry.Family {
					if oldEntry.Family == sqlbase.FamilyID(0) {
						return nil, errors.AssertionFailedf(
							"index entry for family 0 for table %s, index %s was not generated",
							ru.Helper.TableDesc.Name, index.Name,
						)
					}
					// In this case, the index has a k/v for a family that does not exist in
					// the new set of k/v's for the row. So, we need to delete the old k/v.
					if traceKV {
						log.VEventf(ctx, 2, "Del %s", keys.PrettyPrint(ru.Helper.secIndexValDirs[i], oldEntry.Key))
					}
					batch.Del(oldEntry.Key)
					oldIdx++
				} else {
					if newEntry.Family == sqlbase.FamilyID(0) {
						return nil, errors.AssertionFailedf(
							"index entry for family 0 for table %s, index %s was not generated",
							ru.Helper.TableDesc.Name, index.Name,
						)
					}
					// In this case, the index now has a k/v that did not exist in the
					// old row, so we should expect to not see a value for the new
					// key, and put the new key in place.
					if traceKV {
						k := keys.PrettyPrint(ru.Helper.secIndexValDirs[i], newEntry.Key)
						v := newEntry.Value.PrettyPrint()
						log.VEventf(ctx, 2, "CPut %s -> %v (expecting does not exist)", k, v)
					}
					batch.CPut(newEntry.Key, &newEntry.Value, nil)
					newIdx++
				}
			}
			for oldIdx < len(oldEntries) {
				// Delete any remaining old entries that are not matched by new entries in this row.
				oldEntry := &oldEntries[oldIdx]
				if oldEntry.Family == sqlbase.FamilyID(0) {
					return nil, errors.AssertionFailedf(
						"index entry for family 0 for table %s, index %s was not generated",
						ru.Helper.TableDesc.Name, index.Name,
					)
				}
				if traceKV {
					log.VEventf(ctx, 2, "Del %s", keys.PrettyPrint(ru.Helper.secIndexValDirs[i], oldEntry.Key))
				}
				batch.Del(oldEntry.Key)
				oldIdx++
			}
			for newIdx < len(newEntries) {
				// Insert any remaining new entries that are not present in the old row.
				newEntry := &newEntries[newIdx]
				if newEntry.Family == sqlbase.FamilyID(0) {
					return nil, errors.AssertionFailedf(
						"index entry for family 0 for table %s, index %s was not generated",
						ru.Helper.TableDesc.Name, index.Name,
					)
				}
				if traceKV {
					k := keys.PrettyPrint(ru.Helper.secIndexValDirs[i], newEntry.Key)
					v := newEntry.Value.PrettyPrint()
					log.VEventf(ctx, 2, "CPut %s -> %v (expecting does not exist)", k, v)
				}
				batch.CPut(newEntry.Key, &newEntry.Value, nil)
				newIdx++
			}
		} else {
			// Remove all inverted index entries, and re-add them.
			for j := range ru.oldIndexEntries[i] {
				if traceKV {
					log.VEventf(ctx, 2, "Del %s", ru.oldIndexEntries[i][j].Key)
				}
				batch.Del(ru.oldIndexEntries[i][j].Key)
			}
			putFn := insertInvertedPutFn
			// We're adding all of the inverted index entries from the row being updated.
			for j := range ru.newIndexEntries[i] {
				putFn(ctx, batch, &ru.newIndexEntries[i][j].Key, &ru.newIndexEntries[i][j].Value, traceKV)
			}
		}
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
		if ru.Fks.hasFKs() {
			if err := ru.Fks.checker.runCheck(ctx, oldValues, ru.newValues); err != nil {
				return nil, err
			}
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
