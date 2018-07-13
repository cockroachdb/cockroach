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

package sqlbase

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type checkFKConstraints bool

const (
	// CheckFKs can be passed to row writers to check fk validity.
	CheckFKs checkFKConstraints = true
	// SkipFKs can be passed to row writer to skip fk validity checks.
	SkipFKs checkFKConstraints = false
)

// rowHelper has the common methods for table row manipulations.
type rowHelper struct {
	TableDesc *TableDescriptor
	// Secondary indexes.
	Indexes      []IndexDescriptor
	indexEntries []IndexEntry

	// Computed during initialization for pretty-printing.
	primIndexValDirs []encoding.Direction
	secIndexValDirs  [][]encoding.Direction

	// Computed and cached.
	primaryIndexKeyPrefix []byte
	primaryIndexCols      map[ColumnID]struct{}
	sortedColumnFamilies  map[FamilyID][]ColumnID
}

func newRowHelper(desc *TableDescriptor, indexes []IndexDescriptor) rowHelper {
	rh := rowHelper{TableDesc: desc, Indexes: indexes}

	// Pre-compute the encoding directions of the index key values for
	// pretty-printing in traces.
	rh.primIndexValDirs = IndexKeyValDirs(&rh.TableDesc.PrimaryIndex)

	rh.secIndexValDirs = make([][]encoding.Direction, len(rh.Indexes))
	for i, index := range rh.Indexes {
		rh.secIndexValDirs[i] = IndexKeyValDirs(&index)
	}

	return rh
}

// encodeIndexes encodes the primary and secondary index keys. The
// secondaryIndexEntries are only valid until the next call to encodeIndexes or
// encodeSecondaryIndexes.
func (rh *rowHelper) encodeIndexes(
	colIDtoRowIndex map[ColumnID]int, values []tree.Datum,
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
	colIDtoRowIndex map[ColumnID]int, values []tree.Datum,
) (secondaryIndexEntries []IndexEntry, err error) {
	if len(rh.indexEntries) != len(rh.Indexes) {
		rh.indexEntries = make([]IndexEntry, len(rh.Indexes))
	}
	rh.indexEntries, err = EncodeSecondaryIndexes(
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
	colID ColumnID, family FamilyID, value tree.Datum,
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
	if cdatum, ok := value.(tree.CompositeDatum); ok {
		// Composite columns are encoded in both the key and the value.
		return !cdatum.IsComposite(), nil
	}
	// Skip primary key columns as their values are encoded in the key of
	// each family. Family 0 is guaranteed to exist and acts as a
	// sentinel.
	return true, nil
}

func (rh *rowHelper) sortedColumnFamily(famID FamilyID) ([]ColumnID, bool) {
	if rh.sortedColumnFamilies == nil {
		rh.sortedColumnFamilies = make(map[FamilyID][]ColumnID, len(rh.TableDesc.Families))
		for _, family := range rh.TableDesc.Families {
			colIDs := append([]ColumnID(nil), family.ColumnIDs...)
			sort.Sort(ColumnIDs(colIDs))
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
	CheckFKs              checkFKConstraints
	Fks                   fkInsertHelper

	// For allocation avoidance.
	marshaled []roachpb.Value
	key       roachpb.Key
	valueBuf  []byte
	value     roachpb.Value
}

// MakeRowInserter creates a RowInserter for the given table.
//
// insertCols must contain every column in the primary key.
func MakeRowInserter(
	txn *client.Txn,
	tableDesc *TableDescriptor,
	fkTables TableLookupsByID,
	insertCols []ColumnDescriptor,
	checkFKs checkFKConstraints,
	alloc *DatumAlloc,
) (RowInserter, error) {
	indexes := tableDesc.Indexes
	// Also include the secondary indexes in mutation state
	// DELETE_AND_WRITE_ONLY.
	for _, m := range tableDesc.Mutations {
		if m.State == DescriptorMutation_DELETE_AND_WRITE_ONLY {
			if index := m.GetIndex(); index != nil {
				indexes = append(indexes, *index)
			}
		}
	}

	ri := RowInserter{
		Helper:                newRowHelper(tableDesc, indexes),
		InsertCols:            insertCols,
		InsertColIDtoRowIndex: ColIDtoRowIndexFromCols(insertCols),
		marshaled:             make([]roachpb.Value, len(insertCols)),
		CheckFKs:              checkFKs,
	}

	for i, col := range tableDesc.PrimaryIndex.ColumnIDs {
		if _, ok := ri.InsertColIDtoRowIndex[col]; !ok {
			return RowInserter{}, fmt.Errorf("missing %q primary key column", tableDesc.PrimaryIndex.ColumnNames[i])
		}
	}

	if checkFKs == CheckFKs {
		var err error
		if ri.Fks, err = makeFKInsertHelper(txn, *tableDesc, fkTables,
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
func (ri *RowInserter) InsertRow(
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
		if ri.marshaled[i], err = MarshalColumnValue(ri.InsertCols[i], val); err != nil {
			return err
		}
	}

	if ri.CheckFKs == CheckFKs && checkFKs == CheckFKs {
		if err := ri.Fks.addAllIdxChecks(ctx, values); err != nil {
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
// - updatedCols is the list of schema columns being updated.
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
	updatedCols []ColumnDescriptor,
	values []tree.Datum,
	valColIDMapping map[ColumnID]int,
	marshaledValues []roachpb.Value,
	marshaledColIDMapping map[ColumnID]int,
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

		var lastColID ColumnID
		familySortedColumnIDs, ok := helper.sortedColumnFamily(family.ID)
		if !ok {
			return nil, pgerror.NewErrorf(pgerror.CodeInternalError,
				"programming error: invalid family sorted column id map")
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

			col := updatedCols[idx]

			if lastColID > col.ID {
				return nil, pgerror.NewErrorf(pgerror.CodeInternalError,
					"programming error: cannot write column id %d after %d", col.ID, lastColID)
			}
			colIDDiff := col.ID - lastColID
			lastColID = col.ID
			var err error
			rawValueBuf, err = EncodeTableValue(rawValueBuf, colIDDiff, values[idx], nil)
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
func (ri *RowInserter) EncodeIndexesForRow(
	values []tree.Datum,
) (primaryIndexKey []byte, secondaryIndexEntries []IndexEntry, err error) {
	return ri.Helper.encodeIndexes(ri.InsertColIDtoRowIndex, values)
}

// RowUpdater abstracts the key/value operations for updating table rows.
type RowUpdater struct {
	Helper                rowHelper
	DeleteHelper          *rowHelper
	FetchCols             []ColumnDescriptor
	FetchColIDtoRowIndex  map[ColumnID]int
	UpdateCols            []ColumnDescriptor
	updateColIDtoRowIndex map[ColumnID]int
	primaryKeyColChange   bool

	// rd and ri are used when the update this RowUpdater is created for modifies
	// the primary key of the table. In that case, rows must be deleted and
	// re-added instead of merely updated, since the keys are changing.
	rd RowDeleter
	ri RowInserter

	Fks      fkUpdateHelper
	cascader *cascader

	// For allocation avoidance.
	marshaled       []roachpb.Value
	newValues       []tree.Datum
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
	evalCtx *tree.EvalContext,
	alloc *DatumAlloc,
) (RowUpdater, error) {
	rowUpdater, err := makeRowUpdaterWithoutCascader(
		txn, tableDesc, fkTables, updateCols, requestedCols, updateType, alloc,
	)
	if err != nil {
		return RowUpdater{}, err
	}
	rowUpdater.cascader, err = makeUpdateCascader(
		txn, tableDesc, fkTables, updateCols, evalCtx, alloc,
	)
	if err != nil {
		return RowUpdater{}, err
	}
	return rowUpdater, nil
}

// makeRowUpdaterWithoutCascader is the same function as MakeRowUpdated but does not
// create a cascader.
func makeRowUpdaterWithoutCascader(
	txn *client.Txn,
	tableDesc *TableDescriptor,
	fkTables TableLookupsByID,
	updateCols []ColumnDescriptor,
	requestedCols []ColumnDescriptor,
	updateType rowUpdaterType,
	alloc *DatumAlloc,
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

	// Columns of the table to update, including those in delete/write-only state
	tableCols := tableDesc.Columns
	if len(tableDesc.Mutations) > 0 {
		tableCols = make([]ColumnDescriptor, 0, len(tableDesc.Columns)+len(tableDesc.Mutations))
		tableCols = append(tableCols, tableDesc.Columns...)
	}

	var deleteOnlyIndexes []IndexDescriptor
	for _, m := range tableDesc.Mutations {
		if index := m.GetIndex(); index != nil {
			if needsUpdate(*index) {
				switch m.State {
				case DescriptorMutation_DELETE_ONLY:
					if deleteOnlyIndexes == nil {
						// Allocate at most once.
						deleteOnlyIndexes = make([]IndexDescriptor, 0, len(tableDesc.Mutations))
					}
					deleteOnlyIndexes = append(deleteOnlyIndexes, *index)
				default:
					indexes = append(indexes, *index)
				}
			}
		} else if col := m.GetColumn(); col != nil {
			tableCols = append(tableCols, *col)
		}
	}

	var deleteOnlyHelper *rowHelper
	if len(deleteOnlyIndexes) > 0 {
		rh := newRowHelper(tableDesc, deleteOnlyIndexes)
		deleteOnlyHelper = &rh
	}

	ru := RowUpdater{
		Helper:                newRowHelper(tableDesc, indexes),
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
			return RowUpdater{}, err
		}
		ru.FetchCols = ru.rd.FetchCols
		ru.FetchColIDtoRowIndex = ColIDtoRowIndexFromCols(ru.FetchCols)
		if ru.ri, err = MakeRowInserter(txn, tableDesc, fkTables,
			tableCols, SkipFKs, alloc); err != nil {
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
		for _, index := range deleteOnlyIndexes {
			if err := index.RunOverAllColumns(maybeAddCol); err != nil {
				return RowUpdater{}, err
			}
		}
	}

	var err error
	if ru.Fks, err = makeFKUpdateHelper(txn, *tableDesc, fkTables,
		ru.FetchColIDtoRowIndex, alloc); err != nil {
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
	var deleteOldSecondaryIndexEntries []IndexEntry
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
		if ru.marshaled[i], err = MarshalColumnValue(ru.UpdateCols[i], val); err != nil {
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
			if err := ru.Fks.addIndexChecks(ctx, oldValues, ru.newValues); err != nil {
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
		if index.Type == IndexDescriptor_INVERTED {
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
		if err := ru.Fks.addIndexChecks(ctx, oldValues, ru.newValues); err != nil {
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

// IsColumnOnlyUpdate returns true if this RowUpdater is only updating column
// data (in contrast to updating the primary key or other indexes).
func (ru *RowUpdater) IsColumnOnlyUpdate() bool {
	// TODO(dan): This is used in the schema change backfill to assert that it was
	// configured correctly and will not be doing things it shouldn't. This is an
	// unfortunate bleeding of responsibility and indicates the abstraction could
	// be improved. Specifically, RowUpdater currently has two responsibilities
	// (computing which indexes need to be updated and mapping sql rows to k/v
	// operations) and these should be split.
	return !ru.primaryKeyColChange && ru.DeleteHelper == nil && len(ru.Helper.Indexes) == 0
}

// RowDeleter abstracts the key/value operations for deleting table rows.
type RowDeleter struct {
	Helper               rowHelper
	FetchCols            []ColumnDescriptor
	FetchColIDtoRowIndex map[ColumnID]int
	Fks                  fkDeleteHelper
	CheckFKs             checkFKConstraints
	cascader             *cascader
	// For allocation avoidance.
	key roachpb.Key
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
	checkFKs checkFKConstraints,
	evalCtx *tree.EvalContext,
	alloc *DatumAlloc,
) (RowDeleter, error) {
	rowDeleter, err := makeRowDeleterWithoutCascader(
		txn, tableDesc, fkTables, requestedCols, checkFKs, alloc,
	)
	if err != nil {
		return RowDeleter{}, err
	}
	if checkFKs == CheckFKs {
		var err error
		rowDeleter.cascader, err = makeDeleteCascader(txn, tableDesc, fkTables, evalCtx, alloc)
		if err != nil {
			return RowDeleter{}, err
		}
	}
	return rowDeleter, nil
}

// makeRowDeleterWithoutCascader creates a rowDeleter but does not create an
// additional cascader.
func makeRowDeleterWithoutCascader(
	txn *client.Txn,
	tableDesc *TableDescriptor,
	fkTables TableLookupsByID,
	requestedCols []ColumnDescriptor,
	checkFKs checkFKConstraints,
	alloc *DatumAlloc,
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
		Helper:               newRowHelper(tableDesc, indexes),
		FetchCols:            fetchCols,
		FetchColIDtoRowIndex: fetchColIDtoRowIndex,
		CheckFKs:             checkFKs,
	}
	if checkFKs == CheckFKs {
		var err error
		if rd.Fks, err = makeFKDeleteHelper(txn, *tableDesc, fkTables,
			fetchColIDtoRowIndex, alloc); err != nil {
			return RowDeleter{}, err
		}
	}

	return rd, nil
}

// DeleteRow adds to the batch the kv operations necessary to delete a table row
// with the given values. It also will cascade as required and check for
// orphaned rows. The bytesMonitor is only used if cascading/fk checking and can
// be nil if not.
func (rd *RowDeleter) DeleteRow(
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
	if rd.CheckFKs == CheckFKs && checkFKs == CheckFKs {
		if err := rd.Fks.addAllIdxChecks(ctx, values); err != nil {
			return err
		}
		return rd.Fks.checker.runCheck(ctx, values, nil)
	}
	return nil
}

// DeleteIndexRow adds to the batch the kv operations necessary to delete a
// table row from the given index.
func (rd *RowDeleter) DeleteIndexRow(
	ctx context.Context, b *client.Batch, idx *IndexDescriptor, values []tree.Datum, traceKV bool,
) error {
	if rd.CheckFKs == CheckFKs {
		if err := rd.Fks.addAllIdxChecks(ctx, values); err != nil {
			return err
		}
		if err := rd.Fks.checker.runCheck(ctx, values, nil); err != nil {
			return err
		}
	}
	secondaryIndexEntry, err := EncodeSecondaryIndex(
		rd.Helper.TableDesc, idx, rd.FetchColIDtoRowIndex, values)
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
func ColIDtoRowIndexFromCols(cols []ColumnDescriptor) map[ColumnID]int {
	colIDtoRowIndex := make(map[ColumnID]int, len(cols))
	for i, col := range cols {
		colIDtoRowIndex[col.ID] = i
	}
	return colIDtoRowIndex
}
