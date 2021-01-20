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

// tableInserter handles writing kvs and forming table rows for inserts.
type tableInserter struct {
	tableWriterBase
	ri row.Inserter
}

var _ tableWriter = &tableInserter{}

// desc is part of the tableWriter interface.
func (*tableInserter) desc() string { return "inserter" }

// init is part of the tableWriter interface.
func (ti *tableInserter) init(_ context.Context, txn *kv.Txn, evalCtx *tree.EvalContext) error {
	ti.tableWriterBase.init(txn, ti.tableDesc(), evalCtx)
	return nil
}

// row is part of the tableWriter interface.
func (ti *tableInserter) row(
	ctx context.Context, values tree.Datums, pm row.PartialIndexUpdateHelper, traceKV bool,
) error {
	ti.currentBatchSize++
	return ti.ri.InsertRow(ctx, ti.b, values, pm, false /* overwrite */, traceKV)
}

// tableDesc is part of the tableWriter interface.
func (ti *tableInserter) tableDesc() catalog.TableDescriptor {
	return ti.ri.Helper.TableDesc
}

// walkExprs is part of the tableWriter interface.
func (ti *tableInserter) walkExprs(_ func(desc string, index int, expr tree.TypedExpr)) {}
