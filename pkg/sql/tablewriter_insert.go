// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
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

// init initializes the tableInserter with a Txn.
func (ti *tableInserter) init(_ context.Context, txn *kv.Txn, evalCtx *eval.Context) error {
	return ti.tableWriterBase.init(txn, ti.tableDesc(), evalCtx)
}

// row performs an insert.
//
// The passed Datums is not used after `row` returns.
//
// The PartialIndexUpdateHelper is used to determine which partial indexes
// to avoid updating when performing row modification. This is necessary
// because not all rows are indexed by partial indexes.
//
// The traceKV parameter determines whether the individual K/V operations
// should be logged to the context. We use a separate argument here instead
// of a Value field on the context because Value access in context.Context
// is rather expensive.
func (ti *tableInserter) row(
	ctx context.Context, values tree.Datums, pm row.PartialIndexUpdateHelper, traceKV bool,
) error {
	ti.currentBatchSize++
	return ti.ri.InsertRow(ctx, &ti.putter, values, pm, nil, row.CPutOp, traceKV)
}

// tableDesc returns the TableDescriptor for the table that the tableInserter
// will modify.
func (ti *tableInserter) tableDesc() catalog.TableDescriptor {
	return ti.ri.Helper.TableDesc
}
