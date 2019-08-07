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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// fkExistenceCheckForUpdate is an auxiliary object with two purposes:
//
// - its main purpose is to facilitate the existence checks on both
//   referencing and referenced tables when modifying rows in a table.
//
//   Note that users of this purpose are responsible for calling
//   addCheckForIndex() on all mutated indexes, to register a mutated
//   index for FK checking.
//
//   TODO(knz): why cannot the fkExistenceCheckForUpdate make this determination
//   itself, like the other helpers? The asymmetry is concerning.
//
// - its secondary purpose is to serve the boolean "hasFk()" for "does
//   the mutated table have any FK constraints, either forward or
//   backward?"  This boolean is used by the row writer and the
//   CASCADEing close.
//
//   TODO(knz): this responsibility should be carried by another
//   object, so that the helper can specialize to only do existence
//   checks!
//
type fkExistenceCheckForUpdate struct {
	// inbound is responsible for existence checks in referencing tables.
	inbound fkExistenceCheckForDelete
	// output is responsible for existence checks in referenced tables.
	outbound fkExistenceCheckForInsert

	// indexIDsToCheck determines the list of indexes in the mutated
	// table for which to perform FK checks.
	//
	// This may be a subset of all constraints on the mutated table.
	// The inbound and outbound checkers look at all constraints by default;
	// the update helper needs to maintain its own list of index IDs
	// to operate on only a subset, and also define its own addIndexChecks()
	// logic instead of deferring to addAllIdxChecks().
	indexIDsToCheck map[sqlbase.IndexID]struct{}

	// checker is the object that actually carries out the lookups in
	// KV.
	checker *fkExistenceBatchChecker
}

// makeFkExistenceCheckHelperForUpdate instantiates an update helper.
func makeFkExistenceCheckHelperForUpdate(
	txn *client.Txn,
	table *sqlbase.ImmutableTableDescriptor,
	otherTables FkTableMetadata,
	updateCols []sqlbase.ColumnDescriptor,
	colMap map[sqlbase.ColumnID]int,
	alloc *sqlbase.DatumAlloc,
) (fkExistenceCheckForUpdate, error) {
	ret := fkExistenceCheckForUpdate{
		indexIDsToCheck: map[sqlbase.IndexID]struct{}{0: {}},
	}

	// Instantiate a helper for the referencing tables.
	var err error
	if ret.inbound, err = makeFkExistenceCheckHelperForDelete(txn, table, otherTables, colMap,
		alloc); err != nil {
		return ret, err
	}

	// Instantiate a helper for the referenced table(s).
	ret.outbound, err = makeFkExistenceCheckHelperForInsert(txn, table, otherTables, colMap, alloc)
	ret.outbound.checker = ret.inbound.checker

	// We need *some* KV batch checker to perform the checks. It doesn't
	// matter which; so we use the one instantiated by the inbound
	// checker and simply disregard/ignore the one instantiated by the
	// outbound checker.
	ret.checker = ret.inbound.checker

	return ret, err
}

// addCheckForIndex registers a mutated index to perform FK existence checks for.
func (fks fkExistenceCheckForUpdate) addCheckForIndex(
	indexID sqlbase.IndexID, descriptorType sqlbase.IndexDescriptor_Type,
) {
	if descriptorType == sqlbase.IndexDescriptor_FORWARD {
		// We ignore FK existence checks for inverted indexes.
		//
		// TODO(knz): verify that this is indeed correct.
		fks.indexIDsToCheck[indexID] = struct{}{}
	}
}

// hasFKs determines whether the table being mutated has any forward
// or backward FK constraints. This is the secondary purpose of the helper
// and is unrelated to the task of FK existence checks.
func (fks fkExistenceCheckForUpdate) hasFKs() bool {
	return len(fks.inbound.fks) > 0 || len(fks.outbound.fks) > 0
}

// addAllIdxChecks queues a FK existence check for the backward and forward
// constraints for the indexes
func (fks fkExistenceCheckForUpdate) addIndexChecks(
	ctx context.Context, oldValues, newValues tree.Datums, traceKV bool,
) error {
	for indexID := range fks.indexIDsToCheck {
		if err := queueFkExistenceChecksForRow(ctx, fks.checker, fks.inbound.fks[indexID], oldValues, traceKV); err != nil {
			return err
		}
		if err := queueFkExistenceChecksForRow(ctx, fks.checker, fks.outbound.fks[indexID], newValues, traceKV); err != nil {
			return err
		}
	}
	return nil
}
