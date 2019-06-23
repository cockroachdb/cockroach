// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// fastTableUpserter implements the fast path for an upsert. See
// tableUpserter for the general case.
//
// If certain conditions are met (no secondary indexes, all table
// values being inserted, update expressions of the form `SET a =
// excluded.a`) then the upsert can be done in one `client.Batch` and
// using only `Put`s. In this case, the single batch is created during
// `init`, operated on during `row`, and run during `finalize`. This
// is the same model as the other `tableFoo`s, which are more simple
// than upsert.
type fastTableUpserter struct {
	tableUpserterBase
}

// desc is part of the tableWriter interface.
func (*fastTableUpserter) desc() string { return "fast upserter" }

// init is part of the tableWriter interface.
func (tu *fastTableUpserter) init(txn *client.Txn, _ *tree.EvalContext) error {
	tu.tableWriterBase.init(txn)
	return nil
}

// row is part of the tableWriter interface.
func (tu *fastTableUpserter) row(ctx context.Context, d tree.Datums, traceKV bool) error {
	tu.batchSize++
	// Use the fast path, ignore conflicts.
	return tu.ri.InsertRow(ctx, tu.b, d, true /* ignoreConflicts */, row.CheckFKs, traceKV)
}

// batchedCount is part of the batchedTableWriter interface.
func (tu *fastTableUpserter) batchedCount() int { return tu.batchSize }

// batchedValues is part of the batchedTableWriter interface.
// This is not implemented for the fast path on upsert. If a plan
// needs result values, it should use tableUpserter instead.
func (tu *fastTableUpserter) batchedValues(rowIdx int) tree.Datums {
	panic("programmer error: tableUpserter should be used if values are needed")
}

// atBatchEnd is part of the extendedTableWriter interface.
func (tu *fastTableUpserter) atBatchEnd(_ context.Context, _ bool) error { return nil }

// flushAndStartNewBatch is part of the extendedTableWriter interface.
func (tu *fastTableUpserter) flushAndStartNewBatch(ctx context.Context) error {
	return tu.tableWriterBase.flushAndStartNewBatch(ctx, tu.tableDesc())
}

// close is part of the tableWriter interface.
func (tu *fastTableUpserter) close(ctx context.Context) {}

// walkExprs is part of the tableWriter interface.
func (tu *fastTableUpserter) walkExprs(_ func(_ string, _ int, _ tree.TypedExpr)) {}
