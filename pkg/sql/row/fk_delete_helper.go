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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// fkDeleteHelper is an auxiliary object that facilitates the
// existence checks on the referencing table when deleting rows in a
// referenced table.
type fkDeleteHelper struct {
	// fks maps mutated index id to slice of baseFKHelper, which
	// performs FK existence checks in referencing tables.
	fks map[sqlbase.IndexID][]baseFKHelper

	// checker is the object that actually carries out the lookups in
	// KV.
	checker *fkBatchChecker
}

// makeFKDeleteHelper instantiates a delete helper.
func makeFKDeleteHelper(
	txn *client.Txn,
	table *sqlbase.ImmutableTableDescriptor,
	otherTables TableLookupsByID,
	colMap map[sqlbase.ColumnID]int,
	alloc *sqlbase.DatumAlloc,
) (fkDeleteHelper, error) {
	h := fkDeleteHelper{
		checker: &fkBatchChecker{
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
			fk, err := makeBaseFKHelper(txn, otherTables, idx, ref, colMap, alloc, CheckDeletes)
			if err == errSkipUnusedFK {
				continue
			}
			if err != nil {
				return fkDeleteHelper{}, err
			}
			if h.fks == nil {
				h.fks = make(map[sqlbase.IndexID][]baseFKHelper)
			}
			h.fks[idx.ID] = append(h.fks[idx.ID], fk)
		}
	}

	return h, nil
}

// addAllIdxChecks queues a FK existence check for every referencing table.
func (h fkDeleteHelper) addAllIdxChecks(ctx context.Context, row tree.Datums, traceKV bool) error {
	for idx := range h.fks {
		if err := checkIdx(ctx, h.checker, h.fks, idx, row, traceKV); err != nil {
			return err
		}
	}
	return nil
}

// CollectSpans implements the FkSpanCollector interface.
func (h fkDeleteHelper) CollectSpans() roachpb.Spans {
	return collectSpansWithFKMap(h.fks)
}

// CollectSpansForValues implements the FkSpanCollector interface.
func (h fkDeleteHelper) CollectSpansForValues(values tree.Datums) (roachpb.Spans, error) {
	return collectSpansForValuesWithFKMap(h.fks, values)
}
