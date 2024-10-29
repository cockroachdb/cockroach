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

// tableUpdater handles writing kvs and forming table rows for updates.
type tableUpdater struct {
	tableWriterBase
	ru row.Updater
}

// init initializes the tableUpdater with a Txn.
func (tu *tableUpdater) init(_ context.Context, txn *kv.Txn, evalCtx *eval.Context) error {
	return tu.tableWriterBase.init(txn, tu.tableDesc(), evalCtx)
}

// rowForUpdate performs an update.
//
// The passed Datums is not used after `rowForUpdate` returns.
//
// The PartialIndexUpdateHelper is used to determine which partial indexes
// to avoid updating when performing row modification. This is necessary
// because not all rows are indexed by partial indexes.
//
// The traceKV parameter determines whether the individual K/V operations
// should be logged to the context. We use a separate argument here instead
// of a Value field on the context because Value access in context.Context
// is rather expensive.
func (tu *tableUpdater) rowForUpdate(
	ctx context.Context,
	oldValues, updateValues tree.Datums,
	pm row.PartialIndexUpdateHelper,
	traceKV bool,
) (tree.Datums, error) {
	tu.currentBatchSize++
	return tu.ru.UpdateRow(ctx, tu.b, oldValues, updateValues, pm, nil, traceKV)
}

// tableDesc returns the TableDescriptor for the table that the tableUpdater
// will modify.
func (tu *tableUpdater) tableDesc() catalog.TableDescriptor {
	return tu.ru.Helper.TableDesc
}
