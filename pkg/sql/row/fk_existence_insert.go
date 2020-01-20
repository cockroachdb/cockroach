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

// fkExistenceCheckForInsert is an auxiliary object that facilitates the existence
// checks on the referenced table when inserting new rows in
// a referencing table.
type fkExistenceCheckForInsert struct {
	// fks maps mutated index id to slice of fkExistenceCheckBaseHelper, the outgoing
	// foreign key existence checkers for each mutated index.
	//
	// In an fkInsertHelper, these slices will have at most one entry,
	// since there can be (currently) at most one outgoing foreign key
	// per mutated index. We use this data structure instead of a
	// one-to-one map for consistency with the other helpers.
	//
	// TODO(knz): this limitation in CockroachDB is arbitrary and
	// incompatible with PostgreSQL. pg supports potentially multiple
	// referencing FK constraints for a single column tuple.
	fks map[sqlbase.IndexID][]fkExistenceCheckBaseHelper

	// checker is the object that actually carries out the lookups in
	// KV.
	checker *fkExistenceBatchChecker
}

// makeFkExistenceCheckHelperForInsert instantiates an insert helper.
func makeFkExistenceCheckHelperForInsert(
	ctx context.Context,
	txn *client.Txn,
	table *sqlbase.ImmutableTableDescriptor,
	otherTables FkTableMetadata,
	colMap map[sqlbase.ColumnID]int,
	alloc *sqlbase.DatumAlloc,
) (fkExistenceCheckForInsert, error) {
	h := fkExistenceCheckForInsert{
		checker: &fkExistenceBatchChecker{
			txn: txn,
		},
	}

	// We need an existence check helper for every referenced table.
	for i := range table.OutboundFKs {
		ref := &table.OutboundFKs[i]
		// Look up the searched table.
		searchTable := otherTables[ref.ReferencedTableID].Desc
		if searchTable == nil {
			return h, errors.AssertionFailedf("referenced table %d not in provided table map %+v", ref.ReferencedTableID,
				otherTables)
		}
		searchIdx, err := searchTable.TableDesc().FindIndexByID(ref.LegacyReferencedIndex)
		if err != nil {
			return h, errors.NewAssertionErrorWithWrappedErrf(err,
				"failed to find search index %d for fk %q", ref.LegacyReferencedIndex, ref.Name)
		}
		mutatedIdx, err := table.TableDesc().FindIndexByID(ref.LegacyOriginIndex)
		if err != nil {
			return h, errors.NewAssertionErrorWithWrappedErrf(err,
				"failed to find search index %d for fk %q", ref.LegacyOriginIndex, ref.Name)
		}
		fk, err := makeFkExistenceCheckBaseHelper(txn, otherTables, ref, searchIdx, mutatedIdx, colMap, alloc, CheckInserts)
		if err == errSkipUnusedFK {
			continue
		}
		if err != nil {
			return h, err
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

// addAllIdxChecks queues a FK existence check for every referenced table.
func (h fkExistenceCheckForInsert) addAllIdxChecks(
	ctx context.Context, row tree.Datums, traceKV bool,
) error {
	for idx := range h.fks {
		if err := queueFkExistenceChecksForRow(ctx, h.checker, h.fks[idx], row, traceKV); err != nil {
			return err
		}
	}
	return nil
}
