// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package row

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
)

// Deleter abstracts the key/value operations for deleting table rows.
type Deleter struct {
	Helper    RowHelper
	FetchCols []catalog.Column
	// FetchColIDtoRowIndex must be kept in sync with FetchCols.
	FetchColIDtoRowIndex catalog.TableColMap
	// primaryLocked, if true, indicates that no lock is needed when deleting
	// from the primary index because the caller already acquired it.
	primaryLocked bool
	// secondaryLocked, if set, indicates that no lock is needed when deleting
	// from this secondary index because the caller already acquired it.
	secondaryLocked catalog.Index

	// For allocation avoidance.
	key         roachpb.Key
	rawValueBuf []byte
}

// MakeDeleter creates a Deleter for the given table.
//
// The returned Deleter contains a FetchCols field that defines the
// expectation of which values are passed as values to DeleteRow. If
// requestedCols is non-nil, then only the requested columns are included in
// FetchCols; otherwise, all columns that are part of the key of any index
// (either primary or secondary) are included in FetchCols.
//
// lockedIndexes describes the set of indexes such that any keys that might be
// deleted from the index had already locks acquired on them. In other words,
// the caller guarantees that the deleter has exclusive access to the keys in
// these indexes. It is assumed that at most one secondary index is already
// locked.
func MakeDeleter(
	codec keys.SQLCodec,
	tableDesc catalog.TableDescriptor,
	lockedIndexes []catalog.Index,
	requestedCols []catalog.Column,
	sd *sessiondata.SessionData,
	sv *settings.Values,
	metrics *rowinfra.Metrics,
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
				col, err := catalog.MustFindColumnByID(tableDesc, colID)
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

	var primaryLocked bool
	var secondaryLocked catalog.Index
	for _, index := range lockedIndexes {
		if index.Primary() {
			primaryLocked = true
		} else {
			secondaryLocked = index
		}
	}
	if buildutil.CrdbTestBuild && len(lockedIndexes) > 1 && !primaryLocked {
		// We don't expect multiple secondary indexes to be locked, yet if that
		// happens in prod, we'll just not use the already acquired locks on all
		// but the last secondary index, which means a possible performance hit
		// but no correctness issues.
		panic(errors.AssertionFailedf("locked at least two secondary indexes in the initial scan: %v", lockedIndexes))
	}
	rd := Deleter{
		Helper:               NewRowHelper(codec, tableDesc, indexes, nil /* uniqueWithTombstoneIndexes */, sd, sv, metrics),
		FetchCols:            fetchCols,
		FetchColIDtoRowIndex: fetchColIDtoRowIndex,
		primaryLocked:        primaryLocked,
		secondaryLocked:      secondaryLocked,
	}

	return rd
}

// DeleteRow adds to the batch the kv operations necessary to delete a table row
// with the given values.
func (rd *Deleter) DeleteRow(
	ctx context.Context,
	batch *kv.Batch,
	values []tree.Datum,
	pm PartialIndexUpdateHelper,
	vh VectorIndexUpdateHelper,
	oth OriginTimestampCPutHelper,
	mustValidateOldPKValues bool,
	traceKV bool,
) error {
	b := &KVBatchAdapter{Batch: batch}

	primaryIndexKey, err := rd.Helper.encodePrimaryIndexKey(rd.FetchColIDtoRowIndex, values)
	if err != nil {
		return err
	}

	// Delete the row from the primary index.
	var called bool
	err = rd.Helper.TableDesc.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
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

		if oth.IsSet() || mustValidateOldPKValues {
			var expValue []byte
			if !oth.IsSet() || !oth.PreviousWasDeleted {
				prevValue, err := rd.encodeValueForPrimaryIndexFamily(family, values)
				if err != nil {
					return err
				}
				if prevValue.IsPresent() {
					expValue = prevValue.TagAndDataBytes()
				}
			}
			if oth.IsSet() {
				oth.DelWithCPut(ctx, b, &rd.key, expValue, traceKV)
			} else {
				delWithCPutFn(ctx, b, &rd.key, expValue, traceKV, rd.Helper.primIndexValDirs)
			}
		} else {
			delFn(ctx, b, &rd.key, !rd.primaryLocked /* needsLock */, traceKV, rd.Helper.primIndexValDirs)
		}

		rd.key = nil
		return nil
	})
	if err != nil {
		return err
	}

	// Delete the row from any secondary indices.
	for i, index := range rd.Helper.Indexes {
		// If the index ID exists in the set of indexes to ignore, do not
		// attempt to delete from the index.
		if pm.IgnoreForDel.Contains(int(index.GetID())) {
			continue
		}

		// We want to include empty k/v pairs because we want to delete all k/v's for this row.
		entries, err := rowenc.EncodeSecondaryIndex(
			ctx,
			rd.Helper.Codec,
			rd.Helper.TableDesc,
			index,
			rd.FetchColIDtoRowIndex,
			values,
			vh.GetDel(),
			true, /* includeEmpty */
		)
		if err != nil {
			return err
		}
		alreadyLocked := rd.secondaryLocked != nil && rd.secondaryLocked.GetID() == index.GetID()
		for _, e := range entries {
			if err = rd.Helper.deleteIndexEntry(
				ctx, b, index, &e.Key, alreadyLocked, rd.Helper.sd.BufferedWritesUseLockingOnNonUniqueIndexes,
				traceKV, rd.Helper.secIndexValDirs[i],
			); err != nil {
				return err
			}
		}
	}

	return nil
}

// encodeValueForPrimaryIndexFamily encodes the expected roachpb.Value
// for the given family and valuses.
//
// TODO(ssd): Lots of duplication between this and
// prepareInsertOrUpdateBatch. This is rather unfortunate.
func (rd *Deleter) encodeValueForPrimaryIndexFamily(
	family *descpb.ColumnFamilyDescriptor, values []tree.Datum,
) (roachpb.Value, error) {
	if len(family.ColumnIDs) == 1 && family.ColumnIDs[0] == family.DefaultColumnID && family.ID != 0 {
		idx, ok := rd.FetchColIDtoRowIndex.Get(family.DefaultColumnID)
		if !ok {
			return roachpb.Value{}, nil
		}
		if skip, _ := rd.Helper.SkipColumnNotInPrimaryIndexValue(family.DefaultColumnID, values[idx]); skip {
			return roachpb.Value{}, nil
		}
		typ := rd.FetchCols[idx].GetType()
		marshaled, err := valueside.MarshalLegacy(typ, values[idx])
		if err != nil {
			return roachpb.Value{}, err
		}

		return marshaled, err
	}

	rd.rawValueBuf = rd.rawValueBuf[:0]
	familySortedColumnIDs, ok := rd.Helper.SortedColumnFamily(family.ID)
	if !ok {
		return roachpb.Value{}, errors.AssertionFailedf("invalid family sorted column id map")
	}

	var err error
	rd.rawValueBuf, err = rd.Helper.encodePrimaryIndexValuesToBuf(values, rd.FetchColIDtoRowIndex, familySortedColumnIDs, rd.FetchCols, rd.rawValueBuf)
	if err != nil {
		return roachpb.Value{}, err
	}
	ret := roachpb.Value{}
	// For family 0, we expect a value even when no columns have
	// been encoded to oldBytes.
	if family.ID == 0 || len(rd.rawValueBuf) > 0 {
		ret.SetTuple(rd.rawValueBuf)
	}
	return ret, nil
}
