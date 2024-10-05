// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
func (ti *tableInserter) init(
	_ context.Context, txn *kv.Txn, evalCtx *eval.Context, sv *settings.Values,
) error {
	return ti.tableWriterBase.init(txn, ti.tableDesc(), evalCtx, sv)
}

// row is part of the tableWriter interface.
func (ti *tableInserter) row(
	ctx context.Context, values tree.Datums, pm row.PartialIndexUpdateHelper, traceKV bool,
) error {
	ti.currentBatchSize++
	return ti.ri.InsertRow(ctx, &ti.putter, values, pm, false /* overwrite */, traceKV)
}

// tableDesc is part of the tableWriter interface.
func (ti *tableInserter) tableDesc() catalog.TableDescriptor {
	return ti.ri.Helper.TableDesc
}

// walkExprs is part of the tableWriter interface.
func (ti *tableInserter) walkExprs(_ func(desc string, index int, expr tree.TypedExpr)) {}
