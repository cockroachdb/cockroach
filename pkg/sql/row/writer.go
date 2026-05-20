// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package row

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// This file contains common functions for the three writers, Inserter, Deleter
// and Updater.

// ColIDtoRowIndexFromCols groups a slice of ColumnDescriptors by their ID
// field, returning a map from ID to the index of the column in the input slice.
// It assumes there are no duplicate descriptors in the input.
func ColIDtoRowIndexFromCols(cols []catalog.Column) catalog.TableColMap {
	var colIDtoRowIndex catalog.TableColMap
	for i := range cols {
		colIDtoRowIndex.Set(cols[i].GetID(), i)
	}
	return colIDtoRowIndex
}

// ColMapping returns a map from ordinals in the fromCols list to ordinals in
// the toCols list. More precisely, for 0 <= i < fromCols:
//
//	result[i] = j such that fromCols[i].ID == toCols[j].ID, or
//	             -1 if the column is not part of toCols.
func ColMapping(fromCols, toCols []catalog.Column) []int {
	// colMap is a map from ColumnID to ordinal into fromCols.
	var colMap util.FastIntMap
	for i := range fromCols {
		colMap.Set(int(fromCols[i].GetID()), i)
	}

	result := make([]int, len(fromCols))
	for i := range result {
		// -1 value indicates that this column is not being returned.
		result[i] = -1
	}

	// Set the appropriate index values for the returning columns.
	for toOrd := range toCols {
		if fromOrd, ok := colMap.Get(int(toCols[toOrd].GetID())); ok {
			result[fromOrd] = toOrd
		}
	}

	return result
}

// prepareInsertOrUpdateBatch constructs a KV batch that inserts or updates
// a row in KV in the primary index. Per-family value encoding is delegated
// to rowenc.PrimaryIndexEncoder via helper.PrimaryIndexEncoder(); this
// function adds the family-key construction, KV-op dispatch (CPut / Put /
// PutMustAcquireExclusiveLock / Delete), origin-timestamp / old-PK
// validation handling, and row-size guard rails.
//
//   - batch is the KV batch where commands should be appended.
//   - helper is the RowHelper that knows about the table being modified.
//   - primaryIndexKey is the PK prefix for the current row.
//   - values is the SQL-level row values being written.
//   - valColIDMapping maps column IDs to positions of the slice values.
//   - updatedColIDMapping maps column IDs to positions of the updated
//     values; only families that touch one of these columns are processed.
//   - kvKey and kvValue are heap-allocated scratch buffers used to stage
//     each family's roachpb.Key and roachpb.Value.
//   - rawValueBuf is a scratch byte buffer for tuple-encoded family values;
//     it can be reused across calls to amortize allocations and is returned
//     for that purpose.
//   - kvOp selects the KV write op for non-deletes (CPutOp / PutOp /
//     PutMustAcquireExclusiveLockOp). PutOp also indicates that the old
//     keys have been locked.
//   - oldValues, oth, and mustValidateOldPKValues drive the construction of
//     expected previous bytes for CPut-based writes.
//   - traceKV enables KV-op logging.
func prepareInsertOrUpdateBatch(
	ctx context.Context,
	batch Putter,
	helper *RowHelper,
	primaryIndexKey []byte,
	values []tree.Datum,
	valColIDMapping catalog.TableColMap,
	updatedColIDMapping catalog.TableColMap,
	kvKey *roachpb.Key,
	kvValue *roachpb.Value,
	rawValueBuf []byte,
	oth OriginTimestampCPutHelper,
	oldValues []tree.Datum,
	kvOp KVInsertOp,
	mustValidateOldPKValues bool,
	traceKV bool,
) ([]byte, error) {
	families := helper.TableDesc.GetFamilies()
	// TODO(ssd): We don't currently support multiple column families on
	// the LDR write path. As a result, we don't have good end-to-end
	// testing of multi-column family writes with the origin timestamp
	// helper set. Until we write such tests, we error if we ever see such
	// writes.
	if oth.IsSet() && len(families) > 1 {
		return nil, errors.AssertionFailedf("OriginTimestampCPutHelper is not yet testing with multi-column family writes")
	}
	var putFn func(context.Context, Putter, *roachpb.Key, *roachpb.Value, bool, *RowHelper, lazyIndexDirs)
	var oldKeysLocked, overwrite bool
	switch kvOp {
	case CPutOp:
		putFn = insertCPutFn
		oldKeysLocked = false
		overwrite = false
	case PutOp:
		putFn = insertPutFn
		oldKeysLocked = true
		overwrite = true
	case PutMustAcquireExclusiveLockOp:
		putFn = insertPutMustAcquireExclusiveLockFn
		oldKeysLocked = false
		overwrite = true
	}

	encoder := helper.PrimaryIndexEncoder()
	wantOldValue := (oth.IsSet() || mustValidateOldPKValues) && len(oldValues) > 0
	var oldBuf []byte
	for i := range families {
		family := &families[i]
		// Empty family.ColumnIDs occurs when the PK lives in a non-zero
		// family and family 0's only column has been dropped; family 0
		// must still be visited so it emits the row sentinel.
		if !familyTouchedByUpdate(family, updatedColIDMapping) && len(family.ColumnIDs) != 0 {
			continue
		}

		if i > 0 {
			// HACK: MakeFamilyKey appends to its argument, so on every
			// loop iteration after the first, trim primaryIndexKey so
			// nothing gets overwritten.
			// TODO(dan): Instead of this, use something like engine.ChunkAllocator.
			primaryIndexKey = primaryIndexKey[:len(primaryIndexKey):len(primaryIndexKey)]
		}
		*kvKey = keys.MakeFamilyKey(primaryIndexKey, uint32(family.ID))

		res, retBuf, err := encoder.EncodeFamily(family, valColIDMapping, values, rawValueBuf)
		if err != nil {
			return nil, err
		}
		rawValueBuf = retBuf
		if res.Skipped {
			*kvKey = nil
			continue
		}

		var expBytes []byte
		if wantOldValue {
			// TODO(ssd): investigate reducing the number of allocations
			// required to marshal the old value.
			oldRes, retOldBuf, err := encoder.EncodeFamily(family, valColIDMapping, oldValues, oldBuf)
			if err != nil {
				return nil, err
			}
			oldBuf = retOldBuf
			if oldRes.Value.IsPresent() {
				expBytes = oldRes.Value.TagAndDataBytes()
			}
		}

		if !res.Value.IsPresent() {
			// No current value for this family. EncodeFamily guarantees
			// res.Value is present for family 0, so reaching here means
			// family.ID != 0. Issue a Delete in OT and overwrite paths;
			// inserts skip silently.
			switch {
			case oth.IsSet():
				// OT'd CPuts always emit a Delete to confirm expected bytes.
				oth.DelWithCPut(ctx, batch, kvKey, expBytes, traceKV)
			case overwrite && mustValidateOldPKValues:
				delWithCPutFn(ctx, batch, kvKey, expBytes, traceKV, helper, primaryIndexDirs)
			case overwrite:
				delFn(ctx, batch, kvKey, !oldKeysLocked, traceKV, helper, primaryIndexDirs)
			}
			*kvKey = nil
			continue
		}

		if err := helper.CheckRowSize(ctx, kvKey, res.Value.RawBytes, family.ID); err != nil {
			return nil, err
		}
		*kvValue = res.Value
		switch {
		case oth.IsSet():
			oth.CPutFn(ctx, batch, kvKey, kvValue, expBytes, traceKV)
		case mustValidateOldPKValues:
			updateCPutFn(ctx, batch, kvKey, kvValue, expBytes, traceKV, helper, primaryIndexDirs)
		default:
			// TODO(yuzefovich): in case of multiple column families,
			// whenever we locked the primary index during the initial
			// scan, we might not have locked the key for a column family
			// where all columns had NULL values (because the KV didn't
			// exist) and now at least one becomes non-NULL. In this
			// scenario we're inserting a new KV with non-locking Put, yet
			// we don't have the lock.
			//
			// However, at the moment we disable the lock eliding
			// optimization with multiple column families, so we'll use
			// the locking Put because of that.
			putFn(ctx, batch, kvKey, kvValue, traceKV, helper, primaryIndexDirs)
		}
		// Release the kvKey/kvValue references; they're shared across
		// calls and must not alias values already handed off to batch.
		*kvKey = nil
		*kvValue = roachpb.Value{}
	}

	return rawValueBuf, nil
}

// familyTouchedByUpdate reports whether any column in family appears in
// the updated-column map (i.e., is being inserted or updated this row).
func familyTouchedByUpdate(
	family *descpb.ColumnFamilyDescriptor, updatedColIDMapping catalog.TableColMap,
) bool {
	for _, colID := range family.ColumnIDs {
		if _, ok := updatedColIDMapping.Get(colID); ok {
			return true
		}
	}
	return false
}
