// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package row

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	// table. Today, referencing tables are determined by
	// the index descriptors.
	// TODO(knz): make foreign key constraints independent
	// of index definitions.
	for _, idx := range table.AllNonDropIndexes() {
		for _, ref := range idx.ReferencedBy {
			if otherTables[ref.Table].IsAdding {
				// We can assume that a table being added but not yet public is empty,
				// and thus does not need to be checked for FK violations.
				continue
			}
			fk, err := makeFkExistenceCheckBaseHelper(txn, otherTables, idx, ref, colMap, alloc, CheckDeletes)
			if err == errSkipUnusedFK {
				continue
			}
			if err != nil {
				return fkExistenceCheckForDelete{}, err
			}
			if h.fks == nil {
				h.fks = make(map[sqlbase.IndexID][]fkExistenceCheckBaseHelper)
			}
			h.fks[idx.ID] = append(h.fks[idx.ID], fk)
		}
	}

	return h, nil
}

// addAllIdxChecks queues a FK existence check for every referencing table.
func (h fkExistenceCheckForDelete) addAllIdxChecks(
	ctx context.Context, row tree.Datums, traceKV bool,
) error {
	for idx := range h.fks {
		if err := queueFkExistenceChecksForRow(ctx, h.checker, h.fks, idx, row, traceKV); err != nil {
			return err
		}
	}
	return nil
}
