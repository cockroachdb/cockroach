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
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Deleter abstracts the key/value operations for deleting table rows.
type Deleter struct {
	Helper    rowHelper
	FetchCols []catalog.Column
	// FetchColIDtoRowIndex must be kept in sync with FetchCols.
	FetchColIDtoRowIndex catalog.TableColMap
	// For allocation avoidance.
	key roachpb.Key
}

// MakeDeleter creates a Deleter for the given table.
//
// The returned Deleter contains a FetchCols field that defines the
// expectation of which values are passed as values to DeleteRow. If
// requestedCols is non-nil, then only the requested columns are included in
// FetchCols; otherwise, all columns that are part of the key of any index
// (either primary or secondary) are included in FetchCols.
func MakeDeleter(
	codec keys.SQLCodec, tableDesc catalog.TableDescriptor, requestedCols []catalog.Column,
) Deleter {
	indexes := tableDesc.DeletableNonPrimaryIndexes()

	var fetchCols []catalog.Column
	var fetchColIDtoRowIndex catalog.TableColMap
	if requestedCols != nil {
		fetchCols = requestedCols[:len(requestedCols):len(requestedCols)]
		fetchColIDtoRowIndex = ColIDtoRowIndexFromCols(fetchCols)
	} else {
		maybeAddCol := func(colID descpb.ColumnID) error {
			if _, ok := fetchColIDtoRowIndex.Get(colID); !ok {
				col, err := tableDesc.FindColumnWithID(colID)
				if err != nil {
					return err
				}
				fetchColIDtoRowIndex.Set(col.GetID(), len(fetchCols))
				fetchCols = append(fetchCols, col)
			}
			return nil
		}
		for j := 0; j < tableDesc.GetPrimaryIndex().NumKeyColumns(); j++ {
			colID := tableDesc.GetPrimaryIndex().GetKeyColumnID(j)
			if err := maybeAddCol(colID); err != nil {
				return Deleter{}
			}
		}
		for _, index := range indexes {
			for j := 0; j < index.NumKeyColumns(); j++ {
				colID := index.GetKeyColumnID(j)
				if err := maybeAddCol(colID); err != nil {
					return Deleter{}
				}
			}
			// The extra columns are needed to fix #14601.
			for j := 0; j < index.NumKeySuffixColumns(); j++ {
				colID := index.GetKeySuffixColumnID(j)
				if err := maybeAddCol(colID); err != nil {
					return Deleter{}
				}
			}
		}
	}

	rd := Deleter{
		Helper:               newRowHelper(codec, tableDesc, indexes),
		FetchCols:            fetchCols,
		FetchColIDtoRowIndex: fetchColIDtoRowIndex,
	}

	return rd
}

// DeleteRow adds to the batch the kv operations necessary to delete a table row
// with the given values. It also will cascade as required and check for
// orphaned rows. The bytesMonitor is only used if cascading/fk checking and can
// be nil if not.
func (rd *Deleter) DeleteRow(
	ctx context.Context, b *kv.Batch, values []tree.Datum, pm PartialIndexUpdateHelper, traceKV bool,
) error {

	// Delete the row from any secondary indices.
	for i := range rd.Helper.Indexes {
		// If the index ID exists in the set of indexes to ignore, do not
		// attempt to delete from the index.
		if pm.IgnoreForDel.Contains(int(rd.Helper.Indexes[i].GetID())) {
			continue
		}

		// We want to include empty k/v pairs because we want to delete all k/v's for this row.
		entries, err := rowenc.EncodeSecondaryIndex(
			rd.Helper.Codec,
			rd.Helper.TableDesc,
			rd.Helper.Indexes[i],
			rd.FetchColIDtoRowIndex,
			values,
			true, /* includeEmpty */
		)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if traceKV {
				log.VEventf(ctx, 2, "Del %s", keys.PrettyPrint(rd.Helper.secIndexValDirs[i], e.Key))
			}
			b.Del(&e.Key)
		}
	}

	primaryIndexKey, err := rd.Helper.encodePrimaryIndex(rd.FetchColIDtoRowIndex, values)
	if err != nil {
		return err
	}

	// Delete the row.
	var called bool
	return rd.Helper.TableDesc.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
		if called {
			// HACK: MakeFamilyKey appends to its argument, so on every loop iteration
			// after the first, trim primaryIndexKey so nothing gets overwritten.
			// TODO(dan): Instead of this, use something like engine.ChunkAllocator.
			primaryIndexKey = primaryIndexKey[:len(primaryIndexKey):len(primaryIndexKey)]
		} else {
			called = true
		}
		familyID := family.ID
		rd.key = keys.MakeFamilyKey(primaryIndexKey, uint32(familyID))
		if traceKV {
			log.VEventf(ctx, 2, "Del %s", keys.PrettyPrint(rd.Helper.primIndexValDirs, rd.key))
		}
		b.Del(&rd.key)
		rd.key = nil
		return nil
	})
}

// DeleteIndexRow adds to the batch the kv operations necessary to delete a
// table row from the given index.
func (rd *Deleter) DeleteIndexRow(
	ctx context.Context, b *kv.Batch, idx catalog.Index, values []tree.Datum, traceKV bool,
) error {
	// We want to include empty k/v pairs because we want
	// to delete all k/v's for this row. By setting includeEmpty
	// to true, we will get a k/v pair for each family in the row,
	// which will guarantee that we delete all the k/v's in this row.
	secondaryIndexEntry, err := rowenc.EncodeSecondaryIndex(
		rd.Helper.Codec,
		rd.Helper.TableDesc,
		idx,
		rd.FetchColIDtoRowIndex,
		values,
		true, /* includeEmpty */
	)
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
