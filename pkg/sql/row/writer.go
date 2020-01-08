// Copyright 2015 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// This file contains common functions for the three writers, Inserter, Deleter
// and Updater.

type checkFKConstraints bool

const (
	// CheckFKs can be passed to row writers to check fk validity.
	CheckFKs checkFKConstraints = true
	// SkipFKs can be passed to row writer to skip fk validity checks.
	SkipFKs checkFKConstraints = false
)

// ColIDtoRowIndexFromCols groups a slice of ColumnDescriptors by their ID
// field, returning a map from ID to ColumnDescriptor. It assumes there are no
// duplicate descriptors in the input.
func ColIDtoRowIndexFromCols(cols []sqlbase.ColumnDescriptor) map[sqlbase.ColumnID]int {
	colIDtoRowIndex := make(map[sqlbase.ColumnID]int, len(cols))
	for i := range cols {
		colIDtoRowIndex[cols[i].ID] = i
	}
	return colIDtoRowIndex
}

// ColMapping returns a map from ordinals in the fromCols list to ordinals in
// the toCols list. More precisely, for 0 <= i < fromCols:
//
//   result[i] = j such that fromCols[i].ID == toCols[j].ID, or
//                -1 if the column is not part of toCols.
func ColMapping(fromCols, toCols []sqlbase.ColumnDescriptor) []int {
	// colMap is a map from ColumnID to ordinal into fromCols.
	var colMap util.FastIntMap
	for i := range fromCols {
		colMap.Set(int(fromCols[i].ID), i)
	}

	result := make([]int, len(fromCols))
	for i := range result {
		// -1 value indicates that this column is not being returned.
		result[i] = -1
	}

	// Set the appropriate index values for the returning columns.
	for toOrd := range toCols {
		if fromOrd, ok := colMap.Get(int(toCols[toOrd].ID)); ok {
			result[fromOrd] = toOrd
		}
	}

	return result
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
	for i := range helper.TableDesc.Families {
		family := &helper.TableDesc.Families[i]
		update := false
		for _, colID := range family.ColumnIDs {
			if _, ok := marshaledColIDMapping[colID]; ok {
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
			return nil, errors.AssertionFailedf("invalid family sorted column id map")
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

			col := &fetchedCols[idx]
			if lastColID > col.ID {
				return nil, errors.AssertionFailedf("cannot write column id %d after %d", col.ID, lastColID)
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
			// Copy the contents of rawValueBuf into the roachpb.Value. This is
			// a deep copy so rawValueBuf can be re-used by other calls to the
			// function.
			kvValue.SetTuple(rawValueBuf)
			putFn(ctx, batch, kvKey, kvValue, traceKV)
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
