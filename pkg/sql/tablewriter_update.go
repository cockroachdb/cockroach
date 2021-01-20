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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
func (tu *tableUpdater) init(_ context.Context, txn *kv.Txn, evalCtx *tree.EvalContext) error {
	tu.tableWriterBase.init(txn, tu.tableDesc(), evalCtx)
	return nil
}

// row is part of the tableWriter interface.
// We don't implement this because tu.ru.UpdateRow wants two slices
// and it would be a shame to split the incoming slice on every call.
// Instead provide a separate rowForUpdate() below.
func (tu *tableUpdater) row(
	context.Context, tree.Datums, row.PartialIndexUpdateHelper, bool,
) error {
	panic("unimplemented")
}

// rowForUpdate extends row() from the tableWriter interface.
func (tu *tableUpdater) rowForUpdate(
	ctx context.Context,
	oldValues, updateValues tree.Datums,
	pm row.PartialIndexUpdateHelper,
	traceKV bool,
) (tree.Datums, error) {
	tu.currentBatchSize++
	return tu.ru.UpdateRow(ctx, tu.b, oldValues, updateValues, pm, traceKV)
}

// tableDesc is part of the tableWriter interface.
func (tu *tableUpdater) tableDesc() catalog.TableDescriptor {
	return tu.ru.Helper.TableDesc
}

// walkExprs is part of the tableWriter interface.
func (tu *tableUpdater) walkExprs(_ func(desc string, index int, expr tree.TypedExpr)) {}
