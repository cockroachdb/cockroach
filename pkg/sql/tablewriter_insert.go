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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// tableInserter handles writing kvs and forming table rows for inserts.
type tableInserter struct {
	tableWriterBase
	ri row.Inserter
}

var _ tableWriter = &tableInserter{}

// desc is part of the tableWriter interface.
func (*tableInserter) desc() string { return "inserter" }

// init is part of the tableWriter interface.
func (ti *tableInserter) init(_ context.Context, txn *kv.Txn, _ *tree.EvalContext) error {
	ti.tableWriterBase.init(txn)
	return nil
}

// row is part of the tableWriter interface.
func (ti *tableInserter) row(
	ctx context.Context, values tree.Datums, ignoreIndexes util.FastIntSet, traceKV bool,
) error {
	ti.batchSize++
	return ti.ri.InsertRow(ctx, ti.b, values, ignoreIndexes, false /* overwrite */, traceKV)
}

// atBatchEnd is part of the tableWriter interface.
func (ti *tableInserter) atBatchEnd(_ context.Context, _ bool) error { return nil }

// flushAndStartNewBatch is part of the tableWriter interface.
func (ti *tableInserter) flushAndStartNewBatch(ctx context.Context) error {
	return ti.tableWriterBase.flushAndStartNewBatch(ctx, ti.tableDesc())
}

// finalize is part of the tableWriter interface.
func (ti *tableInserter) finalize(ctx context.Context, _ bool) (*rowcontainer.RowContainer, error) {
	return nil, ti.tableWriterBase.finalize(ctx, ti.tableDesc())
}

// tableDesc is part of the tableWriter interface.
func (ti *tableInserter) tableDesc() *sqlbase.ImmutableTableDescriptor {
	return ti.ri.Helper.TableDesc
}

// close is part of the tableWriter interface.
func (ti *tableInserter) close(_ context.Context) {}

// walkExprs is part of the tableWriter interface.
func (ti *tableInserter) walkExprs(_ func(desc string, index int, expr tree.TypedExpr)) {}
