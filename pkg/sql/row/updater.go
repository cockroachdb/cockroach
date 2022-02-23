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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/unique"
	"github.com/cockroachdb/errors"
)

// Updater abstracts the key/value operations for updating table rows.
type Updater struct {
	Helper       rowHelper
	DeleteHelper *rowHelper
	FetchCols    []catalog.Column
	// FetchColIDtoRowIndex must be kept in sync with FetchCols.
	FetchColIDtoRowIndex  catalog.TableColMap
	UpdateCols            []catalog.Column
	UpdateColIDtoRowIndex catalog.TableColMap
	primaryKeyColChange   bool

	// rd and ri are used when the update this Updater is created for modifies
	// the primary key of the table. In that case, rows must be deleted and
	// re-added instead of merely updated, since the keys are changing.
	rd Deleter
	ri Inserter

	// For allocation avoidance.
	marshaled       []roachpb.Value
	newValues       []tree.Datum
	key             roachpb.Key
	valueBuf        []byte
	value           roachpb.Value
	oldIndexEntries [][]rowenc.IndexEntry
	newIndexEntries [][]rowenc.IndexEntry
}

type rowUpdaterType int

const (
	// UpdaterDefault indicates that an Updater should update everything
	// about a row, including secondary indexes.
	UpdaterDefault rowUpdaterType = 0
	// UpdaterOnlyColumns indicates that an Updater should only update the
	// columns of a row and not the secondary indexes.
	UpdaterOnlyColumns rowUpdaterType = 1
)

// MakeUpdater creates a Updater for the given table.
//
// UpdateCols are the columns being updated and correspond to the updateValues
// that will be passed to UpdateRow.
//
// The returned Updater contains a FetchCols field that defines the
// expectation of which values are passed as oldValues to UpdateRow.
// requestedCols must be non-nil and define the schema that determines
// FetchCols.
func MakeUpdater(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	tableDesc catalog.TableDescriptor,
	updateCols []catalog.Column,
	requestedCols []catalog.Column,
	updateType rowUpdaterType,
	alloc *tree.DatumAlloc,
	sv *settings.Values,
	internal bool,
	metrics *Metrics,
) (Updater, error) {
	if requestedCols == nil {
		return Updater{}, errors.AssertionFailedf("requestedCols is nil in MakeUpdater")
	}

	updateColIDtoRowIndex := ColIDtoRowIndexFromCols(updateCols)

	var primaryIndexCols catalog.TableColSet
	for i := 0; i < tableDesc.GetPrimaryIndex().NumKeyColumns(); i++ {
		colID := tableDesc.GetPrimaryIndex().GetKeyColumnID(i)
		primaryIndexCols.Add(colID)
	}

	var primaryKeyColChange bool
	for _, c := range updateCols {
		if primaryIndexCols.Contains(c.GetID()) {
			primaryKeyColChange = true
			break
		}
	}

	// needsUpdate returns true if the given index may need to be updated for
	// the current UPDATE mutation.
	needsUpdate := func(index catalog.Index) bool {
		// If the UPDATE is set to only update columns and not secondary
		// indexes, return false.
		if updateType == UpdaterOnlyColumns {
			return false
		}
		// If the primary key changed, we need to update all secondary indexes.
		if primaryKeyColChange {
			return true
		}
		// If the index is a partial index, an update may be required even if
		// the indexed columns aren't changing. For example, an index entry must
		// be added when an update to a non-indexed column causes a row to
		// satisfy the partial index predicate when it did not before.
		// TODO(mgartner): needsUpdate does not need to return true for every
		// partial index. A partial index will never require updating if neither
		// its indexed columns nor the columns referenced in its predicate
		// expression are changing.
		if index.IsPartial() {
			return true
		}
		colIDs := index.CollectKeyColumnIDs()
		colIDs.UnionWith(index.CollectSecondaryStoredColumnIDs())
		colIDs.UnionWith(index.CollectKeySuffixColumnIDs())
		for _, colID := range colIDs.Ordered() {
			if _, ok := updateColIDtoRowIndex.Get(colID); ok {
				return true
			}
		}
		return false
	}

	includeIndexes := make([]catalog.Index, 0, len(tableDesc.WritableNonPrimaryIndexes()))
	var deleteOnlyIndexes []catalog.Index
	for _, index := range tableDesc.DeletableNonPrimaryIndexes() {
		if !needsUpdate(index) {
			continue
		}
		if !index.DeleteOnly() {
			includeIndexes = append(includeIndexes, index)
		} else {
			if deleteOnlyIndexes == nil {
				// Allocate at most once.
				deleteOnlyIndexes = make([]catalog.Index, 0, len(tableDesc.DeleteOnlyNonPrimaryIndexes()))
			}
			deleteOnlyIndexes = append(deleteOnlyIndexes, index)
		}
	}

	var deleteOnlyHelper *rowHelper
	if len(deleteOnlyIndexes) > 0 {
		rh := newRowHelper(codec, tableDesc, deleteOnlyIndexes, sv, internal, metrics)
		deleteOnlyHelper = &rh
	}

	ru := Updater{
		Helper:                newRowHelper(codec, tableDesc, includeIndexes, sv, internal, metrics),
		DeleteHelper:          deleteOnlyHelper,
		FetchCols:             requestedCols,
		FetchColIDtoRowIndex:  ColIDtoRowIndexFromCols(requestedCols),
		UpdateCols:            updateCols,
		UpdateColIDtoRowIndex: updateColIDtoRowIndex,
		primaryKeyColChange:   primaryKeyColChange,
		marshaled:             make([]roachpb.Value, len(updateCols)),
		oldIndexEntries:       make([][]rowenc.IndexEntry, len(includeIndexes)),
		newIndexEntries:       make([][]rowenc.IndexEntry, len(includeIndexes)),
	}

	if primaryKeyColChange {
		// These fields are only used when the primary key is changing.
		var err error
		ru.rd = MakeDeleter(codec, tableDesc, requestedCols, sv, internal, metrics)
		if ru.ri, err = MakeInserter(
			ctx, txn, codec, tableDesc, requestedCols, alloc, sv, internal, metrics,
		); err != nil {
			return Updater{}, err
		}
	}

	// If we are fetching from specific families, we might get
	// less columns than in the table. So we cannot assign this to
	// have length len(tableCols).
	ru.newValues = make(tree.Datums, len(ru.FetchCols))

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
	pm PartialIndexUpdateHelper,
	traceKV bool,
) ([]tree.Datum, error) {
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
	var deleteOldSecondaryIndexEntries map[catalog.Index][]rowenc.IndexEntry
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
			ru.FetchColIDtoRowIndex, oldValues, pm.IgnoreForDel, true /* includeEmpty */)
		if err != nil {
			return nil, err
		}
	}

	// Check that the new value types match the column types. This needs to
	// happen before index encoding because certain datum types (i.e. tuple)
	// cannot be used as index values.
	//
	// TODO(radu): the legacy marshaling is used only in rare cases; this is
	// wasteful.
	for i, val := range updateValues {
		if ru.marshaled[i], err = valueside.MarshalLegacy(ru.UpdateCols[i].GetType(), val); err != nil {
			return nil, err
		}
	}

	// Update the row values.
	copy(ru.newValues, oldValues)
	for i, updateCol := range ru.UpdateCols {
		idx, ok := ru.FetchColIDtoRowIndex.Get(updateCol.GetID())
		if !ok {
			return nil, errors.AssertionFailedf("update column without a corresponding fetch column")
		}
		ru.newValues[idx] = updateValues[i]
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

	for i, index := range ru.Helper.Indexes {
		// We don't want to insert any empty k/v's, so set includeEmpty to false.
		// Consider the following case:
		// TABLE t (
		//   x INT PRIMARY KEY, y INT, z INT, w INT,
		//   INDEX (y) STORING (z, w),
		//   FAMILY (x), FAMILY (y), FAMILY (z), FAMILY (w)
		//)
		// If we are to perform an update on row (1, 2, 3, NULL), the k/v pair
		// for index i that encodes column w would have an empty value because w
		// is null and the sole resident of that family. We want to ensure that
		// we don't insert empty k/v pairs during the process of the update, so
		// set includeEmpty to false while generating the old and new index
		// entries.
		//
		// Also, we don't build entries for old and new values if the index
		// exists in ignoreIndexesForDel and ignoreIndexesForPut, respectively.
		// Index IDs in these sets indicate that old and new values for the row
		// do not satisfy a partial index's predicate expression.
		if pm.IgnoreForDel.Contains(int(index.GetID())) {
			ru.oldIndexEntries[i] = nil
		} else {
			ru.oldIndexEntries[i], err = rowenc.EncodeSecondaryIndex(
				ru.Helper.Codec,
				ru.Helper.TableDesc,
				index,
				ru.FetchColIDtoRowIndex,
				oldValues,
				false, /* includeEmpty */
			)
			if err != nil {
				return nil, err
			}
		}
		if pm.IgnoreForPut.Contains(int(index.GetID())) {
			ru.newIndexEntries[i] = nil
		} else {
			ru.newIndexEntries[i], err = rowenc.EncodeSecondaryIndex(
				ru.Helper.Codec,
				ru.Helper.TableDesc,
				index,
				ru.FetchColIDtoRowIndex,
				ru.newValues,
				false, /* includeEmpty */
			)
			if err != nil {
				return nil, err
			}
		}
		if ru.Helper.Indexes[i].GetType() == descpb.IndexDescriptor_INVERTED && !ru.Helper.Indexes[i].IsTemporaryIndexForBackfill() {
			// Deduplicate the keys we're adding and removing if we're updating an
			// inverted index. For example, imagine a table with an inverted index on j:
			//
			// a | j
			// --+----------------
			// 1 | {"foo": "bar"}
			//
			// If we update the json value to be {"foo": "bar", "baz": "qux"}, we don't
			// want to delete the /foo/bar key and re-add it, that would be wasted work.
			// So, we are going to remove keys from both the new and old index entry
			// array if they're identical.
			//
			// We don't do this deduplication on temporary indexes used during the
			// backfill because any deletes that are elided here are not elided on the
			// newly added index when it is in DELETE_ONLY.
			newIndexEntries := ru.newIndexEntries[i]
			oldIndexEntries := ru.oldIndexEntries[i]
			sort.Slice(oldIndexEntries, func(i, j int) bool {
				return compareIndexEntries(oldIndexEntries[i], oldIndexEntries[j]) < 0
			})
			sort.Slice(newIndexEntries, func(i, j int) bool {
				return compareIndexEntries(newIndexEntries[i], newIndexEntries[j]) < 0
			})
			oldLen, newLen := unique.UniquifyAcrossSlices(
				oldIndexEntries, newIndexEntries,
				func(l, r int) int {
					return compareIndexEntries(oldIndexEntries[l], newIndexEntries[r])
				},
				func(i, j int) {
					oldIndexEntries[i] = oldIndexEntries[j]
				},
				func(i, j int) {
					newIndexEntries[i] = newIndexEntries[j]
				})
			ru.oldIndexEntries[i] = oldIndexEntries[:oldLen]
			ru.newIndexEntries[i] = newIndexEntries[:newLen]
		}
	}

	if rowPrimaryKeyChanged {
		if err := ru.rd.DeleteRow(ctx, batch, oldValues, pm, traceKV); err != nil {
			return nil, err
		}
		if err := ru.ri.InsertRow(
			ctx, batch, ru.newValues, pm, false /* ignoreConflicts */, traceKV,
		); err != nil {
			return nil, err
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
	for i, index := range ru.Helper.Indexes {
		if index.GetType() == descpb.IndexDescriptor_FORWARD {
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
					var expValue []byte
					if !bytes.Equal(oldEntry.Key, newEntry.Key) {
						if err := ru.Helper.deleteIndexEntry(ctx, batch, index, ru.Helper.secIndexValDirs[i], oldEntry, traceKV); err != nil {
							return nil, err
						}
					} else if !newEntry.Value.EqualTagAndData(oldEntry.Value) {
						expValue = oldEntry.Value.TagAndDataBytes()
					} else {
						continue
					}

					if index.ForcePut() {
						// See the comemnt on (catalog.Index).ForcePut() for more details.
						insertPutFn(ctx, batch, &newEntry.Key, &newEntry.Value, traceKV)
					} else {
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
					}
				} else if oldEntry.Family < newEntry.Family {
					if oldEntry.Family == descpb.FamilyID(0) {
						return nil, errors.AssertionFailedf(
							"index entry for family 0 for table %s, index %s was not generated",
							ru.Helper.TableDesc.GetName(), index.GetName(),
						)
					}
					// In this case, the index has a k/v for a family that does not exist in
					// the new set of k/v's for the row. So, we need to delete the old k/v.
					if err := ru.Helper.deleteIndexEntry(ctx, batch, index, ru.Helper.secIndexValDirs[i], oldEntry, traceKV); err != nil {
						return nil, err
					}
					oldIdx++
				} else {
					if newEntry.Family == descpb.FamilyID(0) {
						return nil, errors.AssertionFailedf(
							"index entry for family 0 for table %s, index %s was not generated",
							ru.Helper.TableDesc.GetName(), index.GetName(),
						)
					}

					if index.ForcePut() {
						// See the comemnt on (catalog.Index).ForcePut() for more details.
						insertPutFn(ctx, batch, &newEntry.Key, &newEntry.Value, traceKV)
					} else {
						// In this case, the index now has a k/v that did not exist in the
						// old row, so we should expect to not see a value for the new key,
						// and put the new key in place.
						if traceKV {
							k := keys.PrettyPrint(ru.Helper.secIndexValDirs[i], newEntry.Key)
							v := newEntry.Value.PrettyPrint()
							log.VEventf(ctx, 2, "CPut %s -> %v (expecting does not exist)", k, v)
						}
						batch.CPut(newEntry.Key, &newEntry.Value, nil)
					}
					newIdx++
				}
			}
			for oldIdx < len(oldEntries) {
				// Delete any remaining old entries that are not matched by new
				// entries in this row because 1) the family does not exist in
				// the new set of k/v's or 2) the index is a partial index and
				// the new row values do not match the partial index predicate.
				oldEntry := &oldEntries[oldIdx]
				if err := ru.Helper.deleteIndexEntry(ctx, batch, index, ru.Helper.secIndexValDirs[i], oldEntry, traceKV); err != nil {
					return nil, err
				}
				oldIdx++
			}
			for newIdx < len(newEntries) {
				// Insert any remaining new entries that are not present in the
				// old row. Insert any remaining new entries that are not
				// present in the old row because 1) the family does not exist
				// in the old set of k/v's or 2) the index is a partial index
				// and the old row values do not match the partial index
				// predicate.
				newEntry := &newEntries[newIdx]
				if index.ForcePut() {
					// See the comemnt on (catalog.Index).ForcePut() for more details.
					insertPutFn(ctx, batch, &newEntry.Key, &newEntry.Value, traceKV)
				} else {
					if traceKV {
						k := keys.PrettyPrint(ru.Helper.secIndexValDirs[i], newEntry.Key)
						v := newEntry.Value.PrettyPrint()
						log.VEventf(ctx, 2, "CPut %s -> %v (expecting does not exist)", k, v)
					}
					batch.CPut(newEntry.Key, &newEntry.Value, nil)
				}
				newIdx++
			}
		} else {
			// Remove all inverted index entries, and re-add them.
			for j := range ru.oldIndexEntries[i] {
				if err := ru.Helper.deleteIndexEntry(ctx, batch, index, nil /*valDir*/, &ru.oldIndexEntries[i][j], traceKV); err != nil {
					return nil, err
				}
			}
			// We're adding all of the inverted index entries from the row being updated.
			for j := range ru.newIndexEntries[i] {
				if index.ForcePut() {
					// See the comemnt on (catalog.Index).ForcePut() for more details.
					insertPutFn(ctx, batch, &ru.newIndexEntries[i][j].Key, &ru.newIndexEntries[i][j].Value, traceKV)
				} else {
					insertInvertedPutFn(ctx, batch, &ru.newIndexEntries[i][j].Key, &ru.newIndexEntries[i][j].Value, traceKV)
				}
			}
		}
	}

	// We're deleting indexes in a delete only state. We're bounding this by the number of indexes because inverted
	// indexed will be handled separately.
	if ru.DeleteHelper != nil {

		// For determinism, add the entries for the secondary indexes in the same
		// order as they appear in the helper.
		for idx := range ru.DeleteHelper.Indexes {
			index := ru.DeleteHelper.Indexes[idx]
			deletedSecondaryIndexEntries, ok := deleteOldSecondaryIndexEntries[index]

			if ok {
				for _, deletedSecondaryIndexEntry := range deletedSecondaryIndexEntries {
					if err := ru.DeleteHelper.deleteIndexEntry(ctx, batch, index, nil /*valDir*/, &deletedSecondaryIndexEntry, traceKV); err != nil {
						return nil, err
					}
				}
			}
		}
	}

	return ru.newValues, nil
}

func compareIndexEntries(left, right rowenc.IndexEntry) int {
	cmp := bytes.Compare(left.Key, right.Key)
	if cmp != 0 {
		return cmp
	}

	return bytes.Compare(left.Value.RawBytes, right.Value.RawBytes)
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
