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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/mutations"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
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
	init(context.Context, *kv.Txn, *tree.EvalContext, *settings.Values) error

	// row performs a sql row modification (tableInserter performs an insert,
	// etc). It batches up writes to the init'd txn and periodically sends them.
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
	// is rather expensive and the tableWriter interface is used on the
	// inner loop of table accesses.
	row(context.Context, tree.Datums, row.PartialIndexUpdateHelper, bool /* traceKV */) error

	// flushAndStartNewBatch is called at the end of each batch but the last.
	// This should flush the current batch.
	flushAndStartNewBatch(context.Context) error

	// finalize flushes out any remaining writes. It is called after all calls
	// to row.
	finalize(context.Context) error

	// tableDesc returns the TableDescriptor for the table that the tableWriter
	// will modify.
	tableDesc() catalog.TableDescriptor

	// close frees all resources held by the tableWriter.
	close(context.Context)

	// desc returns a name suitable for describing the table writer in
	// the output of EXPLAIN.
	desc() string

	// enable auto commit in call to finalize().
	enableAutoCommit()
}

type autoCommitOpt int

const (
	autoCommitDisabled autoCommitOpt = 0
	autoCommitEnabled  autoCommitOpt = 1
)

// tableWriterBase is meant to be used to factor common code between
// the all tableWriters.
type tableWriterBase struct {
	// txn is the current KV transaction.
	txn *kv.Txn
	// desc is the descriptor of the table that we're writing.
	desc catalog.TableDescriptor
	// is autoCommit turned on.
	autoCommit autoCommitOpt
	// b is the current batch.
	b *kv.Batch
	// lockTimeout specifies the maximum amount of time that the writer will
	// wait while attempting to acquire a lock on a key.
	lockTimeout time.Duration
	// maxBatchSize determines the maximum number of entries in the KV batch
	// for a mutation operation. By default, it will be set to 10k but can be
	// a different value in tests.
	maxBatchSize int
	// maxBatchByteSize determines the maximum number of key and value bytes in
	// the KV batch for a mutation operation.
	maxBatchByteSize int
	// currentBatchSize is the size of the current batch. It is updated on
	// every row() call and is reset once a new batch is started.
	currentBatchSize int
	// lastBatchSize is the size of the last batch. It is set to the value of
	// currentBatchSize once the batch is flushed or finalized.
	lastBatchSize int
	// rowsWritten tracks the number of rows written by this tableWriterBase so
	// far.
	rowsWritten int64
	// rowsWrittenLimit if positive indicates that
	// `transaction_rows_written_err` is enabled. The limit will be checked in
	// finalize() before deciding whether it is safe to auto commit (if auto
	// commit is enabled).
	rowsWrittenLimit int64
	// rows contains the accumulated result rows if rowsNeeded is set on the
	// corresponding tableWriter.
	rows *rowcontainer.RowContainer
	// If set, mutations.MaxBatchSize and row.getKVBatchSize will be overridden
	// to use the non-test value.
	forceProductionBatchSizes bool
	// sv settings values for cluster settings
	sv *settings.Values
}

var maxBatchBytes = settings.RegisterByteSizeSetting(
	settings.TenantWritable,
	"sql.mutations.mutation_batch_byte_size",
	"byte size - in key and value lengths -- for mutation batches",
	4<<20,
)

func (tb *tableWriterBase) init(
	txn *kv.Txn,
	tableDesc catalog.TableDescriptor,
	evalCtx *tree.EvalContext,
	settings *settings.Values,
) {
	tb.txn = txn
	tb.desc = tableDesc
	tb.lockTimeout = 0
	if evalCtx != nil {
		tb.lockTimeout = evalCtx.SessionData().LockTimeout
	}
	tb.forceProductionBatchSizes = evalCtx != nil && evalCtx.TestingKnobs.ForceProductionBatchSizes
	tb.maxBatchSize = mutations.MaxBatchSize(tb.forceProductionBatchSizes)
	batchMaxBytes := int(maxBatchBytes.Default())
	if evalCtx != nil {
		batchMaxBytes = int(maxBatchBytes.Get(&evalCtx.Settings.SV))
	}
	tb.maxBatchByteSize = mutations.MaxBatchByteSize(batchMaxBytes, tb.forceProductionBatchSizes)
	tb.sv = settings
	tb.initNewBatch()
}

// setRowsWrittenLimit should be called before finalize whenever the
// `transaction_rows_written_err` guardrail should be enforced in case the auto
// commit might be enabled.
func (tb *tableWriterBase) setRowsWrittenLimit(sd *sessiondata.SessionData) {
	if sd != nil && !sd.Internal {
		// Only set the limit for non-internal queries (for internal ones we
		// never error out based on the txn row count guardrails).
		tb.rowsWrittenLimit = sd.TxnRowsWrittenErr
	}
}

// flushAndStartNewBatch shares the common flushAndStartNewBatch() code between
// tableWriters.
func (tb *tableWriterBase) flushAndStartNewBatch(ctx context.Context) error {
	if err := tb.txn.Run(ctx, tb.b); err != nil {
		return row.ConvertBatchError(ctx, tb.desc, tb.b)
	}
	if err := tb.tryDoResponseAdmission(ctx); err != nil {
		return err
	}
	tb.initNewBatch()
	tb.rowsWritten += int64(tb.currentBatchSize)
	tb.lastBatchSize = tb.currentBatchSize
	tb.currentBatchSize = 0
	return nil
}

// finalize shares the common finalize() code between tableWriters.
func (tb *tableWriterBase) finalize(ctx context.Context) (err error) {
	// NB: unlike flushAndStartNewBatch, we don't bother with admission control
	// for response processing when finalizing.
	tb.rowsWritten += int64(tb.currentBatchSize)
	if tb.autoCommit == autoCommitEnabled &&
		// We can only auto commit if the rows written guardrail is disabled or
		// we haven't exceeded the specified limit (the optimizer is responsible
		// for making sure that there is exactly one mutation before enabling
		// the auto commit).
		(tb.rowsWrittenLimit == 0 || tb.rowsWritten <= tb.rowsWrittenLimit) &&
		// Also, we don't want to try to commit here if the deadline is expired.
		// If we bubble back up to SQL then maybe we can get a fresh deadline
		// before committing.
		!tb.txn.DeadlineLikelySufficient(tb.sv) {
		log.Event(ctx, "autocommit enabled")
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		err = tb.txn.CommitInBatch(ctx, tb.b)
	} else {
		err = tb.txn.Run(ctx, tb.b)
	}
	tb.lastBatchSize = tb.currentBatchSize
	if err != nil {
		return row.ConvertBatchError(ctx, tb.desc, tb.b)
	}
	return tb.tryDoResponseAdmission(ctx)
}

func (tb *tableWriterBase) tryDoResponseAdmission(ctx context.Context) error {
	// Do admission control for response processing. This is the shared write
	// path for most SQL mutations.
	responseAdmissionQ := tb.txn.DB().SQLKVResponseAdmissionQ
	if responseAdmissionQ != nil {
		requestAdmissionHeader := tb.txn.AdmissionHeader()
		responseAdmission := admission.WorkInfo{
			TenantID:   roachpb.SystemTenantID,
			Priority:   admission.WorkPriority(requestAdmissionHeader.Priority),
			CreateTime: requestAdmissionHeader.CreateTime,
		}
		if _, err := responseAdmissionQ.Admit(ctx, responseAdmission); err != nil {
			return err
		}
	}
	return nil
}

func (tb *tableWriterBase) enableAutoCommit() {
	tb.autoCommit = autoCommitEnabled
}

func (tb *tableWriterBase) initNewBatch() {
	tb.b = tb.txn.NewBatch()
	tb.b.Header.LockTimeout = tb.lockTimeout
}

func (tb *tableWriterBase) clearLastBatch(ctx context.Context) {
	tb.lastBatchSize = 0
	if tb.rows != nil {
		tb.rows.Clear(ctx)
	}
}

func (tb *tableWriterBase) close(ctx context.Context) {
	if tb.rows != nil {
		tb.rows.Close(ctx)
		tb.rows = nil
	}
}
