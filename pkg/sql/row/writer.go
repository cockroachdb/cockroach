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
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
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

// prepareInsertOrUpdateBatch constructs a KV batch that inserts or
// updates a row in KV in the primary index.
//   - batch is the KV batch where commands should be appended.
//   - helper is the rowHelper that knows about the table being modified.
//   - primaryIndexKey is the PK prefix for the current row.
//   - fetchedCols is the list of schema columns that have been fetched
//     in preparation for this update.
//   - values is the SQL-level row values that are being written.
//   - valColIDMapping is the mapping from column IDs into positions of the slice
//     values.
//   - updatedColIDMapping is the mapping from column IDs into the positions of
//     the updated values.
//   - kvKey and kvValues must be heap-allocated scratch buffers to write
//     roachpb.Key and roachpb.Value values.
//   - rawValueBuf must be a scratch byte array. This must be reinitialized
//     to an empty slice on each call but can be preserved at its current
//     capacity to avoid allocations. The function returns the slice.
//   - kvOp indicates which KV write operation should be used.
//   - traceKV is to be set to log the KV operations added to the batch.
func prepareInsertOrUpdateBatch(
	ctx context.Context,
	batch Putter,
	helper *RowHelper,
	primaryIndexKey []byte,
	fetchedCols []catalog.Column,
	values []tree.Datum,
	valColIDMapping catalog.TableColMap,
	updatedColIDMapping catalog.TableColMap,
	kvKey *roachpb.Key,
	kvValue *roachpb.Value,
	rawValueBuf []byte,
	oth *OriginTimestampCPutHelper,
	oldValues []tree.Datum,
	kvOp KVInsertOp,
	traceKV bool,
) ([]byte, error) {
	families := helper.TableDesc.GetFamilies()
	// TODO(ssd): We don't currently support multiple column
	// families on the LDR write path. As a result, we don't have
	// good end-to-end testing of multi-column family writes with
	// the origin timestamp helper set. Until we write such tests,
	// we error if we ever see such writes.
	if oth.IsSet() && len(families) > 1 {
		return nil, errors.AssertionFailedf("OriginTimestampCPutHelper is not yet testing with multi-column family writes")
	}
	var putFn func(context.Context, Putter, *roachpb.Key, *roachpb.Value, bool, []encoding.Direction)
	var overwrite bool
	switch kvOp {
	case CPutOp:
		putFn = insertCPutFn
		overwrite = false
	case PutOp:
		putFn = insertPutFn
		overwrite = true
	case PutMustAcquireExclusiveLockOp:
		putFn = insertPutMustAcquireExclusiveLockFn
		overwrite = true
	}

	for i := range families {
		family := &families[i]
		update := false
		for _, colID := range family.ColumnIDs {
			if _, ok := updatedColIDMapping.Get(colID); ok {
				update = true
				break
			}
		}
		// We can have an empty family.ColumnIDs in the following case:
		// * A table is created with the primary key not in family 0, and another column in family 0.
		// * The column in family 0 is dropped, leaving the 0'th family empty.
		// In this case, we must keep the empty 0'th column family in order to ensure that column family 0
		// is always encoded as the sentinel k/v for a row.
		if !update && len(family.ColumnIDs) != 0 {
			continue
		}

		if i > 0 {
			// HACK: MakeFamilyKey appends to its argument, so on every loop iteration
			// after the first, trim primaryIndexKey so nothing gets overwritten.
			// TODO(dan): Instead of this, use something like engine.ChunkAllocator.
			primaryIndexKey = primaryIndexKey[:len(primaryIndexKey):len(primaryIndexKey)]
		}

		*kvKey = keys.MakeFamilyKey(primaryIndexKey, uint32(family.ID))
		// We need to ensure that column family 0 contains extra metadata, like composite primary key values.
		// Additionally, the decoders expect that column family 0 is encoded with a TUPLE value tag, so we
		// don't want to use the untagged value encoding.
		if len(family.ColumnIDs) == 1 && family.ColumnIDs[0] == family.DefaultColumnID && family.ID != 0 {
			// Storage optimization to store DefaultColumnID directly as a value. Also
			// backwards compatible with the original BaseFormatVersion.

			idx, ok := valColIDMapping.Get(family.DefaultColumnID)
			if !ok {
				continue
			}

			var marshaled roachpb.Value
			var err error
			typ := fetchedCols[idx].GetType()

			// Skip any values with a default ID not stored in the primary index,
			// which can happen if we are adding new columns.
			skip, couldBeComposite := helper.SkipColumnNotInPrimaryIndexValue(family.DefaultColumnID, values[idx])
			if skip {
				// If the column could be composite, there could be a previous KV, so we
				// still need to issue a Delete.
				if !couldBeComposite {
					continue
				}
			} else {
				marshaled, err = valueside.MarshalLegacy(typ, values[idx])
				if err != nil {
					return nil, err
				}
			}

			var oldVal []byte
			if oth.IsSet() && len(oldValues) > 0 {
				// If the column could be composite, we only encode the old value if it
				// was a composite value.
				if !couldBeComposite || oldValues[idx].(tree.CompositeDatum).IsComposite() {
					old, err := valueside.MarshalLegacy(typ, oldValues[idx])
					if err != nil {
						return nil, err
					}
					if old.IsPresent() {
						oldVal = old.TagAndDataBytes()
					}
				}
			}

			if !marshaled.IsPresent() {
				if oth.IsSet() {
					// If using OriginTimestamp'd CPuts, we _always_ want to issue a Delete
					// so that we can confirm our expected bytes were correct.
					oth.DelWithCPut(ctx, batch, kvKey, oldVal, traceKV)
				} else if overwrite {
					// If the new family contains a NULL value, then we must
					// delete any pre-existing row.
					insertDelFn(ctx, batch, kvKey, traceKV, helper.primIndexValDirs)
				}
			} else {
				// We only output non-NULL values. Non-existent column keys are
				// considered NULL during scanning and the row sentinel ensures we know
				// the row exists.
				if err := helper.CheckRowSize(ctx, kvKey, marshaled.RawBytes, family.ID); err != nil {
					return nil, err
				}

				if oth.IsSet() {
					oth.CPutFn(ctx, batch, kvKey, &marshaled, oldVal, traceKV)
				} else {
					putFn(ctx, batch, kvKey, &marshaled, traceKV, helper.primIndexValDirs)
				}
			}

			continue
		}

		familySortedColumnIDs, ok := helper.SortedColumnFamily(family.ID)
		if !ok {
			return nil, errors.AssertionFailedf("invalid family sorted column id map")
		}

		rawValueBuf = rawValueBuf[:0]
		var err error
		rawValueBuf, err = helper.encodePrimaryIndexValuesToBuf(values, valColIDMapping, familySortedColumnIDs, fetchedCols, rawValueBuf)
		if err != nil {
			return nil, err
		}

		// TODO(ssd): Here and below investigate reducing the number of
		// allocations required to marshal the old value.
		//
		// If we are using OriginTimestamp ConditionalPuts, calculate the expected
		// value.
		var expBytes []byte
		if oth.IsSet() && len(oldValues) > 0 {
			var oldBytes []byte
			oldBytes, err = helper.encodePrimaryIndexValuesToBuf(oldValues, valColIDMapping, familySortedColumnIDs, fetchedCols, oldBytes)
			if err != nil {
				return nil, err
			}
			// For family 0, we expect a value even when
			// no columns have been encoded to oldBytes.
			if family.ID == 0 || len(oldBytes) > 0 {
				old := &roachpb.Value{}
				old.SetTuple(oldBytes)
				expBytes = old.TagAndDataBytes()
			}
		}

		if family.ID != 0 && len(rawValueBuf) == 0 {
			if oth.IsSet() {
				// If using OriginTimestamp'd CPuts, we _always_ want to issue a Delete
				// so that we can confirm our expected bytes were correct.
				oth.DelWithCPut(ctx, batch, kvKey, expBytes, traceKV)
			} else if overwrite {
				// The family might have already existed but every column in it is being
				// set to NULL, so delete it.
				insertDelFn(ctx, batch, kvKey, traceKV, helper.primIndexValDirs)
			}
		} else {
			// Copy the contents of rawValueBuf into the roachpb.Value. This is
			// a deep copy so rawValueBuf can be re-used by other calls to the
			// function.
			kvValue.SetTuple(rawValueBuf)
			if err := helper.CheckRowSize(ctx, kvKey, kvValue.RawBytes, family.ID); err != nil {
				return nil, err
			}
			if oth.IsSet() {
				oth.CPutFn(ctx, batch, kvKey, kvValue, expBytes, traceKV)
			} else {
				putFn(ctx, batch, kvKey, kvValue, traceKV, helper.primIndexValDirs)
			}
		}

		// Release reference to roachpb.Key.
		*kvKey = nil
		// Prevent future calls to prepareInsertOrUpdateBatch from mutating
		// the RawBytes in the kvValue we just added to the batch. Remember
		// that we share the kvValue reference across calls to this function.
		*kvValue = roachpb.Value{}
	}

	return rawValueBuf, nil
}
