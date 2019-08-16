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
		searchIdx, err := originTable.Desc.TableDesc().FindIndexByID(ref.LegacyOriginIndex)
		if err != nil {
			return fkExistenceCheckForDelete{}, errors.NewAssertionErrorWithWrappedErrf(
				err, "failed to find index %d (table %d) for deletion", ref.LegacyOriginIndex, ref.OriginTableID)
		}
		mutatedIdx, err := table.TableDesc().FindIndexByID(ref.LegacyReferencedIndex)
		if err != nil {
			return fkExistenceCheckForDelete{}, errors.NewAssertionErrorWithWrappedErrf(
				err, "failed to find available index %d (table %d) for deletion", ref.LegacyReferencedIndex, ref.ReferencedTableID)
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
