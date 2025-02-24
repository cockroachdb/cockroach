// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package row

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Inserter abstracts the key/value operations for inserting table rows.
type Inserter struct {
	Helper                RowHelper
	InsertCols            []catalog.Column
	InsertColIDtoRowIndex catalog.TableColMap

	// For allocation avoidance.
	key      roachpb.Key
	valueBuf []byte
	value    roachpb.Value
}

// MakeInserter creates a Inserter for the given table.
//
// insertCols must contain every column in the primary key. Virtual columns must
// be present if they are part of any index.
func MakeInserter(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	tableDesc catalog.TableDescriptor,
	uniqueWithTombstoneIndexes []catalog.Index,
	insertCols []catalog.Column,
	alloc *tree.DatumAlloc,
	sv *settings.Values,
	internal bool,
	metrics *rowinfra.Metrics,
) (Inserter, error) {
	ri := Inserter{
		Helper: NewRowHelper(
			codec, tableDesc, tableDesc.WritableNonPrimaryIndexes(), uniqueWithTombstoneIndexes, sv, internal, metrics,
		),

		InsertCols:            insertCols,
		InsertColIDtoRowIndex: ColIDtoRowIndexFromCols(insertCols),
	}

	if err := CheckPrimaryKeyColumns(tableDesc, ri.InsertColIDtoRowIndex); err != nil {
		return Inserter{}, err
	}

	return ri, nil
}

func CheckPrimaryKeyColumns(tableDesc catalog.TableDescriptor, colMap catalog.TableColMap) error {
	for i := 0; i < tableDesc.GetPrimaryIndex().NumKeyColumns(); i++ {
		colID := tableDesc.GetPrimaryIndex().GetKeyColumnID(i)
		if _, ok := colMap.Get(colID); !ok {
			return fmt.Errorf("missing %q primary key column", tableDesc.GetPrimaryIndex().GetKeyColumnName(i))
		}
	}
	return nil
}

// insertCPutFn is used by insertRow when conflicts (i.e. the key already exists)
// should generate errors.
func insertCPutFn(
	ctx context.Context,
	b Putter,
	key *roachpb.Key,
	value *roachpb.Value,
	traceKV bool,
	keyEncodingDirs []encoding.Direction,
) {
	if traceKV {
		log.VEventfDepth(ctx, 1, 2, "CPut %s -> %s", keys.PrettyPrint(keyEncodingDirs, *key), value.PrettyPrint())
	}
	b.CPut(key, value, nil /* expValue */)
}

// insertPutFn is used by insertRow when conflicts should be ignored.
func insertPutFn(
	ctx context.Context,
	b Putter,
	key *roachpb.Key,
	value *roachpb.Value,
	traceKV bool,
	keyEncodingDirs []encoding.Direction,
) {
	if traceKV {
		log.VEventfDepth(ctx, 1, 2, "Put %s -> %s", keys.PrettyPrint(keyEncodingDirs, *key), value.PrettyPrint())
	}
	b.Put(key, value)
}

// insertPutMustAcquireExclusiveLockFn is used by insertRow when conflicts
// should be ignored while ensuring that an exclusive lock is acquired on the
// key.
func insertPutMustAcquireExclusiveLockFn(
	ctx context.Context,
	b Putter,
	key *roachpb.Key,
	value *roachpb.Value,
	traceKV bool,
	keyEncodingDirs []encoding.Direction,
) {
	if traceKV {
		log.VEventfDepth(ctx, 1, 2, "Put (locking) %s -> %s", keys.PrettyPrint(keyEncodingDirs, *key), value.PrettyPrint())
	}
	b.PutMustAcquireExclusiveLock(key, value)
}

// insertDelFn is used by insertRow to delete existing rows.
func insertDelFn(
	ctx context.Context,
	b Putter,
	key *roachpb.Key,
	traceKV bool,
	keyEncodingDirs []encoding.Direction,
) {
	if traceKV {
		log.VEventfDepth(ctx, 1, 2, "Del %s", keys.PrettyPrint(keyEncodingDirs, *key))
	}
	b.Del(key)
}

func writeTombstones(
	ctx context.Context,
	helper *RowHelper,
	index catalog.Index,
	b Putter,
	insertColIDtoRowIndex catalog.TableColMap,
	values []tree.Datum,
	traceKV bool,
) error {
	tombstones, err := helper.encodeTombstonesForIndex(ctx, index, insertColIDtoRowIndex, values)
	if err != nil {
		return err
	}
	for _, tombstone := range tombstones {
		k := roachpb.Key(keys.MakeFamilyKey(tombstone, 0 /* famID */))
		if traceKV {
			log.VEventfDepth(ctx, 1, 2, "CPut %s -> nil (tombstone)", k)
		}
		b.CPut(k, nil, nil /* expValue */)
	}
	return nil
}

// KVInsertOp prescribes which KV operation should be used when inserting a SQL
// row.
type KVInsertOp byte

const (
	// CPutOp prescribes usage of the CPut operation and also indicates that the
	// row **should not** be overwritten.
	CPutOp KVInsertOp = iota
	// PutOp prescribes usage of the Put operation and also indicates that the
	// row **should** be overwritten.
	PutOp
	// PutMustAcquireExclusiveLockOp prescribes usage of the Put operation while
	// ensuring that an exclusive lock is acquired and also indicates that the
	// row **should** be overwritten.
	PutMustAcquireExclusiveLockOp
)

// InsertRow adds to the batch the kv operations necessary to insert a table row
// with the given values.
func (ri *Inserter) InsertRow(
	ctx context.Context,
	b Putter,
	values []tree.Datum,
	pm PartialIndexUpdateHelper,
	oth *OriginTimestampCPutHelper,
	kvOp KVInsertOp,
	traceKV bool,
) error {
	if len(values) != len(ri.InsertCols) {
		return errors.Errorf("got %d values but expected %d", len(values), len(ri.InsertCols))
	}

	// We don't want to insert any empty k/v's, so set includeEmpty to false.
	// Consider the following case:
	// TABLE t (
	//   x INT PRIMARY KEY, y INT, z INT, w INT,
	//   INDEX (y) STORING (z, w),
	//   FAMILY (x), FAMILY (y), FAMILY (z), FAMILY (w)
	//)
	// If we are to insert row (1, 2, 3, NULL), the k/v pair for
	// index i that encodes column w would have an empty value,
	// because w is null, and the sole resident of that family.
	// We don't want to insert empty k/v's like this, so we
	// set includeEmpty to false.
	primaryIndexKey, secondaryIndexEntries, err := ri.Helper.encodeIndexes(
		ctx, ri.InsertColIDtoRowIndex, values, pm.IgnoreForPut, false, /* includeEmpty */
	)
	if err != nil {
		return err
	}

	// Add the new values.
	ri.valueBuf, err = prepareInsertOrUpdateBatch(ctx, b,
		&ri.Helper, primaryIndexKey, ri.InsertCols,
		values, ri.InsertColIDtoRowIndex,
		ri.InsertColIDtoRowIndex,
		&ri.key, &ri.value, ri.valueBuf, oth, nil /* oldValues */, kvOp, traceKV)
	if err != nil {
		return err
	}

	if err := writeTombstones(ctx, &ri.Helper, ri.Helper.TableDesc.GetPrimaryIndex(), b, ri.InsertColIDtoRowIndex, values, traceKV); err != nil {
		return err
	}

	// For determinism, add the entries for the secondary indexes in the same
	// order as they appear in the helper.
	for idx, index := range ri.Helper.Indexes {
		entries, ok := secondaryIndexEntries[ri.Helper.Indexes[idx]]
		if ok {
			for i := range entries {
				e := &entries[i]

				if ri.Helper.Indexes[idx].ForcePut() {
					// See the comment on (catalog.Index).ForcePut() for more details.
					insertPutFn(ctx, b, &e.Key, &e.Value, traceKV, ri.Helper.secIndexValDirs[idx])
				} else {
					insertCPutFn(ctx, b, &e.Key, &e.Value, traceKV, ri.Helper.secIndexValDirs[idx])
				}
			}

			// If a row does not satisfy a partial index predicate, it will have no
			// entries, implying that we should also not write tombstones.
			if len(entries) > 0 {
				if err := writeTombstones(ctx, &ri.Helper, index, b, ri.InsertColIDtoRowIndex, values, traceKV); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
