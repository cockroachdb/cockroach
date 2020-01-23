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
	"github.com/cockroachdb/errors"
)

// fkExistenceCheckForDelete is an auxiliary object that facilitates the
// existence checks on the referencing table when deleting rows in a
// referenced table.
type fkExistenceCheckForDelete struct {
	// fks maps mutated index id to slice of fkExistenceCheckBaseHelper, which
	// performs FK existence checks in referencing tables.
	fks map[sqlbase.IndexID][]fkExistenceCheckBaseHelper

	// checker is the object that actually carries out the lookups in
	// KV.
	checker *fkExistenceBatchChecker
}

// makeFkExistenceCheckHelperForDelete instantiates a delete helper.
func makeFkExistenceCheckHelperForDelete(
	ctx context.Context,
	txn *client.Txn,
	table *sqlbase.ImmutableTableDescriptor,
	otherTables FkTableMetadata,
	colMap map[sqlbase.ColumnID]int,
	alloc *sqlbase.DatumAlloc,
) (fkExistenceCheckForDelete, error) {
	h := fkExistenceCheckForDelete{
		checker: &fkExistenceBatchChecker{
			txn: txn,
		},
	}

	// We need an existence check helper for every referencing
	// table.
	for i := range table.InboundFKs {
		ref := &table.InboundFKs[i]
		originTable := otherTables[ref.OriginTableID]
		if originTable.IsAdding {
			// We can assume that a table being added but not yet public is empty,
			// and thus does not need to be checked for FK violations.
			continue
		}
		// TODO(jordan,radu): this is busted, rip out when HP is removed.
		// Fake a forward foreign key constraint. The HP requires an index on the
		// reverse table, which won't be required by the CBO. So in HP, fail if we
		// don't have this precondition.
		// This will never be on an actual table descriptor, so we don't need to
		// populate all the legacy index fields.
		fakeRef := &sqlbase.ForeignKeyConstraint{
			ReferencedTableID:   ref.OriginTableID,
			ReferencedColumnIDs: ref.OriginColumnIDs,
			OriginTableID:       ref.ReferencedTableID,
			OriginColumnIDs:     ref.ReferencedColumnIDs,
			// N.B.: Back-references always must have SIMPLE match method, because ... TODO(jordan): !!!
			Match:    sqlbase.ForeignKeyReference_SIMPLE,
			OnDelete: ref.OnDelete,
			OnUpdate: ref.OnUpdate,
		}
		searchIdx, err := sqlbase.FindFKOriginIndex(originTable.Desc.TableDesc(), ref.OriginColumnIDs)
		if err != nil {
			return fkExistenceCheckForDelete{}, errors.NewAssertionErrorWithWrappedErrf(
				err, "failed to find a suitable index on table %d for deletion", ref.OriginTableID)
		}
		mutatedIdx, err := sqlbase.FindFKReferencedIndex(table.TableDesc(), ref.ReferencedColumnIDs)
		if err != nil {
			return fkExistenceCheckForDelete{}, errors.NewAssertionErrorWithWrappedErrf(
				err, "failed to find a suitable index on table %d for deletion", ref.ReferencedTableID)
		}
		fk, err := makeFkExistenceCheckBaseHelper(txn, otherTables, fakeRef, searchIdx, mutatedIdx, colMap, alloc,
			CheckDeletes)
		if err == errSkipUnusedFK {
			continue
		}
		if err != nil {
			return fkExistenceCheckForDelete{}, err
		}
		if h.fks == nil {
			h.fks = make(map[sqlbase.IndexID][]fkExistenceCheckBaseHelper)
		}
		h.fks[mutatedIdx.ID] = append(h.fks[mutatedIdx.ID], fk)
	}

	if len(h.fks) > 0 {
		// TODO(knz,radu): FK existence checks need to see the writes
		// performed by the mutation.
		//
		// In order to make this true, we need to split the existence
		// checks into a separate sequencing step, and have the first
		// check happen no early than the end of all the "main" part of
		// the statement. Unfortunately, the organization of the code does
		// not allow this today.
		//
		// See: https://github.com/cockroachdb/cockroach/issues/33475
		//
		// In order to "make do" and preserve a modicum of FK semantics we
		// thus need to disable step-wise execution here. The result is that
		// it will also enable any interleaved read part to observe the
		// mutation, and thus introduce the risk of a Halloween problem for
		// any mutation that uses FK relationships.
		_ = txn.ConfigureStepping(ctx, client.SteppingDisabled)
	}

	return h, nil
}

// addAllIdxChecks queues a FK existence check for every referencing table.
func (h fkExistenceCheckForDelete) addAllIdxChecks(
	ctx context.Context, row tree.Datums, traceKV bool,
) error {
	for idx := range h.fks {
		if err := queueFkExistenceChecksForRow(ctx, h.checker, h.fks[idx], row, traceKV); err != nil {
			return err
		}
	}
	return nil
}
