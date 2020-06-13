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

// tableUpdater handles writing kvs and forming table rows for updates.
type tableUpdater struct {
	tableWriterBase
	ru row.Updater
}

var _ tableWriter = &tableUpdater{}

// desc is part of the tableWriter interface.
func (*tableUpdater) desc() string { return "updater" }

// init is part of the tableWriter interface.
func (tu *tableUpdater) init(_ context.Context, txn *kv.Txn, _ *tree.EvalContext) error {
	tu.tableWriterBase.init(txn)
	return nil
}

// row is part of the tableWriter interface.
// We don't implement this because tu.ru.UpdateRow wants two slices
// and it would be a shame to split the incoming slice on every call.
// Instead provide a separate rowForUpdate() below.
func (tu *tableUpdater) row(context.Context, tree.Datums, util.FastIntSet, bool) error {
	panic("unimplemented")
}

// rowForUpdate extends row() from the tableWriter interface.
func (tu *tableUpdater) rowForUpdate(
	ctx context.Context, oldValues, updateValues tree.Datums, traceKV bool,
) (tree.Datums, error) {
	tu.batchSize++
	return tu.ru.UpdateRow(ctx, tu.b, oldValues, updateValues, traceKV)
}

// atBatchEnd is part of the tableWriter interface.
func (tu *tableUpdater) atBatchEnd(_ context.Context, _ bool) error { return nil }

// flushAndStartNewBatch is part of the tableWriter interface.
func (tu *tableUpdater) flushAndStartNewBatch(ctx context.Context) error {
	return tu.tableWriterBase.flushAndStartNewBatch(ctx, tu.tableDesc())
}

// finalize is part of the tableWriter interface.
func (tu *tableUpdater) finalize(ctx context.Context, _ bool) (*rowcontainer.RowContainer, error) {
	return nil, tu.tableWriterBase.finalize(ctx, tu.tableDesc())
}

// tableDesc is part of the tableWriter interface.
func (tu *tableUpdater) tableDesc() *sqlbase.ImmutableTableDescriptor {
	return tu.ru.Helper.TableDesc
}

// close is part of the tableWriter interface.
func (tu *tableUpdater) close(_ context.Context) {}

// walkExprs is part of the tableWriter interface.
func (tu *tableUpdater) walkExprs(_ func(desc string, index int, expr tree.TypedExpr)) {}
