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
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// tableInserter handles writing kvs and forming table rows for inserts.
type tableInserter struct {
	tableWriterBase
	ri row.Inserter
}

// desc is part of the tableWriter interface.
func (*tableInserter) desc() string { return "inserter" }

// init is part of the tableWriter interface.
func (ti *tableInserter) init(txn *client.Txn, _ *tree.EvalContext) error {
	ti.tableWriterBase.init(txn)
	return nil
}

// row is part of the tableWriter interface.
func (ti *tableInserter) row(
	ctx context.Context, values tree.Datums, traceKV bool,
) (tree.Datums, error) {
	ti.batchSize++
	return nil, ti.ri.InsertRow(ctx, ti.b, values, false, row.CheckFKs, traceKV)
}

// atBatchEnd is part of the extendedTableWriter interface.
func (ti *tableInserter) atBatchEnd(_ context.Context, _ bool) error { return nil }

// flushAndStartNewBatch is part of the extendedTableWriter interface.
func (ti *tableInserter) flushAndStartNewBatch(ctx context.Context) error {
	return ti.tableWriterBase.flushAndStartNewBatch(ctx, ti.tableDesc())
}

// finalize is part of the tableWriter interface.
func (ti *tableInserter) finalize(
	ctx context.Context, autoCommit autoCommitOpt, _ bool,
) (*sqlbase.RowContainer, error) {
	return nil, ti.tableWriterBase.finalize(ctx, autoCommit, ti.tableDesc())
}

// tableDesc is part of the tableWriter interface.
func (ti *tableInserter) tableDesc() *sqlbase.ImmutableTableDescriptor {
	return ti.ri.Helper.TableDesc
}

// fkSpanCollector is part of the tableWriter interface.
func (ti *tableInserter) fkSpanCollector() row.FkSpanCollector {
	return ti.ri.Fks
}

// close is part of the tableWriter interface.
func (ti *tableInserter) close(_ context.Context) {}

// walkExprs is part of the tableWriter interface.
func (ti *tableInserter) walkExprs(_ func(desc string, index int, expr tree.TypedExpr)) {}
