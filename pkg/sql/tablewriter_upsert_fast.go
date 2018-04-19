// Copyright 2018 The Cockroach Authors.
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

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	tableWriterBase
	ri sqlbase.RowInserter
}

// init is part of the tableWriter interface.
func (tu *fastTableUpserter) init(txn *client.Txn, _ *tree.EvalContext) error {
	tu.tableWriterBase.init(txn)
	return nil
}

// row is part of the tableWriter interface.
func (tu *fastTableUpserter) row(
	ctx context.Context, row tree.Datums, traceKV bool,
) (tree.Datums, error) {
	tu.batchSize++
	// Use the fast path, ignore conflicts.
	return nil, tu.ri.InsertRow(
		ctx, tu.b, row, true /* ignoreConflicts */, sqlbase.CheckFKs, traceKV)
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

// finalize is part of the tableWriter interface.
func (tu *fastTableUpserter) finalize(
	ctx context.Context, autoCommit autoCommitOpt, traceKV bool,
) (*sqlbase.RowContainer, error) {
	return nil, tu.tableWriterBase.finalize(ctx, autoCommit, tu.tableDesc())
}

// fkSpanCollector is part of the tableWriter interface.
func (tu *fastTableUpserter) fkSpanCollector() sqlbase.FkSpanCollector {
	return tu.ri.Fks
}

// tableDesc is part of the tableWriter interface.
func (tu *fastTableUpserter) tableDesc() *sqlbase.TableDescriptor {
	return tu.ri.Helper.TableDesc
}

// close is part of the tableWriter interface.
func (tu *fastTableUpserter) close(ctx context.Context) {}

// walkExprs is part of the tableWriter interface.
func (tu *fastTableUpserter) walkExprs(_ func(_ string, _ int, _ tree.TypedExpr)) {}
