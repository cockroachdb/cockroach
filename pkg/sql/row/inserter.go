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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// Inserter abstracts the key/value operations for inserting table rows.
type Inserter struct {
	Helper                rowHelper
	InsertCols            []sqlbase.ColumnDescriptor
	InsertColIDtoRowIndex map[sqlbase.ColumnID]int
	Fks                   fkExistenceCheckForInsert

	// For allocation avoidance.
	marshaled []roachpb.Value
	key       roachpb.Key
	valueBuf  []byte
	value     roachpb.Value
}

// MakeInserter creates a Inserter for the given table.
//
// insertCols must contain every column in the primary key.
func MakeInserter(
	ctx context.Context,
	txn *client.Txn,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	insertCols []sqlbase.ColumnDescriptor,
	checkFKs checkFKConstraints,
	fkTables FkTableMetadata,
	alloc *sqlbase.DatumAlloc,
) (Inserter, error) {
	ri := Inserter{
		Helper:                newRowHelper(tableDesc, tableDesc.WritableIndexes()),
		InsertCols:            insertCols,
		InsertColIDtoRowIndex: ColIDtoRowIndexFromCols(insertCols),
		marshaled:             make([]roachpb.Value, len(insertCols)),
	}

	for i, col := range tableDesc.PrimaryIndex.ColumnIDs {
		if _, ok := ri.InsertColIDtoRowIndex[col]; !ok {
			return Inserter{}, fmt.Errorf("missing %q primary key column", tableDesc.PrimaryIndex.ColumnNames[i])
		}
	}

	if checkFKs == CheckFKs {
		var err error
		if ri.Fks, err = makeFkExistenceCheckHelperForInsert(ctx, txn, tableDesc, fkTables,
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
	CPut(key, value interface{}, expValue *roachpb.Value)
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
		if ri.marshaled[i], err = sqlbase.MarshalColumnValue(&ri.InsertCols[i], val); err != nil {
			return err
		}
	}

	if ri.Fks.checker != nil && checkFKs == CheckFKs {
		if err := ri.Fks.addAllIdxChecks(ctx, values, traceKV); err != nil {
			return err
		}
		if err := ri.Fks.checker.runCheck(ctx, nil, values); err != nil {
			return err
		}
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
		ri.InsertColIDtoRowIndex, values, false /* includeEmpty */)
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
