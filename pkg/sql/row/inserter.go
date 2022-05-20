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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Inserter abstracts the key/value operations for inserting table rows.
type Inserter struct {
	Helper                rowHelper
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
	insertCols []catalog.Column,
	alloc *tree.DatumAlloc,
	sv *settings.Values,
	internal bool,
	metrics *rowinfra.Metrics,
) (Inserter, error) {
	ri := Inserter{
		Helper: newRowHelper(
			codec, tableDesc, tableDesc.WritableNonPrimaryIndexes(), sv, internal, metrics,
		),

		InsertCols:            insertCols,
		InsertColIDtoRowIndex: ColIDtoRowIndexFromCols(insertCols),
	}

	for i := 0; i < tableDesc.GetPrimaryIndex().NumKeyColumns(); i++ {
		colID := tableDesc.GetPrimaryIndex().GetKeyColumnID(i)
		if _, ok := ri.InsertColIDtoRowIndex.Get(colID); !ok {
			return Inserter{}, fmt.Errorf("missing %q primary key column", tableDesc.GetPrimaryIndex().GetKeyColumnName(i))
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
	CPut(key, value interface{}, expValue []byte)
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
	pm PartialIndexUpdateHelper,
	overwrite bool,
	traceKV bool,
) error {
	if len(values) != len(ri.InsertCols) {
		return errors.Errorf("got %d values but expected %d", len(values), len(ri.InsertCols))
	}

	putFn := insertCPutFn
	if overwrite {
		putFn = insertPutFn
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
		ri.InsertColIDtoRowIndex, values, pm.IgnoreForPut, false /* includeEmpty */)
	if err != nil {
		return err
	}

	// Add the new values.
	ri.valueBuf, err = prepareInsertOrUpdateBatch(ctx, b,
		&ri.Helper, primaryIndexKey, ri.InsertCols,
		values, ri.InsertColIDtoRowIndex,
		ri.InsertColIDtoRowIndex,
		&ri.key, &ri.value, ri.valueBuf, putFn, overwrite, traceKV)
	if err != nil {
		return err
	}

	putFn = insertInvertedPutFn

	// For determinism, add the entries for the secondary indexes in the same
	// order as they appear in the helper.
	for idx := range ri.Helper.Indexes {
		entries, ok := secondaryIndexEntries[ri.Helper.Indexes[idx]]
		if ok {
			for i := range entries {
				e := &entries[i]

				if ri.Helper.Indexes[idx].ForcePut() {
					// See the comemnt on (catalog.Index).ForcePut() for more details.
					insertPutFn(ctx, b, &e.Key, &e.Value, traceKV)
				} else {
					putFn(ctx, b, &e.Key, &e.Value, traceKV)
				}
			}
		}
	}

	return nil
}
