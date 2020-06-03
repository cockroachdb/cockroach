// Copyright 2016 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// expressionCarrier handles visiting sub-expressions.
type expressionCarrier interface {
	// walkExprs explores all sub-expressions held by this object, if
	// any.
	walkExprs(func(desc string, index int, expr tree.TypedExpr))
}

// tableWriter handles writing kvs and forming table rows.
//
// Usage:
//   err := tw.init(txn, evalCtx)
//   // Handle err.
//   for {
//      values := ...
//      row, err := tw.row(values)
//      // Handle err.
//   }
//   err := tw.finalize()
//   // Handle err.
type tableWriter interface {
	expressionCarrier

	// init provides the tableWriter with a Txn and optional monitor to write to
	// and returns an error if it was misconfigured.
	init(context.Context, *kv.Txn, *tree.EvalContext) error

	// row performs a sql row modification (tableInserter performs an insert,
	// etc). It batches up writes to the init'd txn and periodically sends them.
	//
	// The passed Datums is not used after `row` returns.
	//
	// The ignoreIndexes parameter is a set of IndexIDs to avoid updating when
	// performing the row modification. It is necessary to avoid writing to
	// partial indexes when rows do not satisfy the partial index predicate.
	//
	// The traceKV parameter determines whether the individual K/V operations
	// should be logged to the context. We use a separate argument here instead
	// of a Value field on the context because Value access in context.Context
	// is rather expensive and the tableWriter interface is used on the
	// inner loop of table accesses.
	row(context.Context, tree.Datums, util.FastIntSet /* ignoreIndexes */, bool /* traceKV */) error

	// finalize flushes out any remaining writes. It is called after all calls to
	// row.  It returns a slice of all Datums not yet returned by calls to `row`.
	// The traceKV parameter determines whether the individual K/V operations
	// should be logged to the context. See the comment above for why
	// this a separate parameter as opposed to a Value field on the context.
	finalize(ctx context.Context, traceKV bool) (*rowcontainer.RowContainer, error)

	// tableDesc returns the TableDescriptor for the table that the tableWriter
	// will modify.
	tableDesc() *sqlbase.ImmutableTableDescriptor

	// close frees all resources held by the tableWriter.
	close(context.Context)

	// desc returns a name suitable for describing the table writer in
	// the output of EXPLAIN.
	desc() string

	// enable auto commit in call to finalize().
	enableAutoCommit()

	// atBatchEnd is called at the end of each batch, just before
	// finalize/flush. It can utilize the current KV batch which is
	// still open at that point. It must not run the batch itself; that
	// task is left to tableWriter.finalize() or flushAndStartNewBatch()
	// below.
	atBatchEnd(context.Context, bool /* traceKV */) error

	// flushAndStartNewBatch is called at the end of each batch but the last.
	// This should flush the current batch.
	flushAndStartNewBatch(context.Context) error

	// curBatchSize returns an upper bound for the amount of KV work
	// needed for the current batch. This cannot reflect the actual KV
	// batch size because the actual KV batch will be constructed only
	// during the call to atBatchEnd().
	curBatchSize() int
}

type autoCommitOpt int

const (
	autoCommitDisabled autoCommitOpt = 0
	autoCommitEnabled  autoCommitOpt = 1
)

// tableWriterBase is meant to be used to factor common code between
// the other tableWriters.
type tableWriterBase struct {
	// txn is the current KV transaction.
	txn *kv.Txn
	// is autoCommit turned on.
	autoCommit autoCommitOpt
	// b is the current batch.
	b *kv.Batch
	// batchSize is the current batch size (when known).
	batchSize int
}

func (tb *tableWriterBase) init(txn *kv.Txn) {
	tb.txn = txn
	tb.b = txn.NewBatch()
}

// flushAndStartNewBatch shares the common flushAndStartNewBatch() code between
// tableWriters.
func (tb *tableWriterBase) flushAndStartNewBatch(
	ctx context.Context, tableDesc *sqlbase.ImmutableTableDescriptor,
) error {
	if err := tb.txn.Run(ctx, tb.b); err != nil {
		return row.ConvertBatchError(ctx, tableDesc, tb.b)
	}
	tb.b = tb.txn.NewBatch()
	tb.batchSize = 0
	return nil
}

// curBatchSize shares the common curBatchSize() code between tableWriters.
func (tb *tableWriterBase) curBatchSize() int { return tb.batchSize }

// finalize shares the common finalize() code between tableWriters.
func (tb *tableWriterBase) finalize(
	ctx context.Context, tableDesc *sqlbase.ImmutableTableDescriptor,
) (err error) {
	if tb.autoCommit == autoCommitEnabled {
		log.Event(ctx, "autocommit enabled")
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		err = tb.txn.CommitInBatch(ctx, tb.b)
	} else {
		err = tb.txn.Run(ctx, tb.b)
	}

	if err != nil {
		return row.ConvertBatchError(ctx, tableDesc, tb.b)
	}
	return nil
}

func (tb *tableWriterBase) enableAutoCommit() {
	tb.autoCommit = autoCommitEnabled
}
