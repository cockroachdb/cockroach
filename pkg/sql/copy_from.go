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
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"strings"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/copy"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
)

// CopyBatchRowSizeDefault is the number of rows we insert in one insert
// statement.
const CopyBatchRowSizeDefault = 100

// Vector wise inserts scale much better and this is suitable default.
// Empirically determined limit where we start to see diminishing speedups.
const CopyBatchRowSizeVectorDefault = 32 << 10

// When this many rows are in the copy buffer, they are inserted.
var CopyBatchRowSize = util.ConstantWithMetamorphicTestRange("copy-batch-size", CopyBatchRowSizeDefault, 1, 50_000)

// copyRetryBufferSize can be used to enable the transparent retry of atomic
// copies that fail due to retriable errors. If an atomic copy under implicit
// transaction control encounters a retriable error and the amount of the copy
// data is less than copyRetryBufferSize, we will start a new txn and replay
// the COPY from the beginning. If a retriable error occurs before reading all
// the COPY data we will read and buffer it all before retrying to ensure it
// is less than this limit.
var copyRetryBufferSize = settings.RegisterByteSizeSetting(
	settings.TenantWritable,
	"sql.copy.retry.max_size",
	"set to non-zero to enable automatic atomic copy retry when the COPY data size is less than max_size"+
		", non-atomic copies are not affected",
	128<<20, //128MiB
).WithPublic()

// SetCopyFromBatchSize exports overriding copy batch size for test code.
func SetCopyFromBatchSize(i int) int {
	old := CopyBatchRowSize
	if buildutil.CrdbTestBuild {
		CopyBatchRowSize = i
	} else {
		// We don't want non-test code mutating globals.
		panic("SetCopyFromBatchSize is a test utility that requires crdb_test tag")
	}
	return old
}

type copyMachineInterface interface {
	run(ctx context.Context) error
	numInsertedRows() int

	// Close closes memory accounts associated with copy.
	Close(ctx context.Context)
}

type copyOptions struct {
	csvEscape       rune
	csvExpectHeader bool

	delimiter byte
	format    tree.CopyFormat
	null      string
}

// TODO(#sql-sessions): copy all pre-condition checks from the PG code
// https://github.com/postgres/postgres/blob/1de58df4fec7325d91f5a8345757314be7ac05da/src/backend/commands/copy.c#L405
func processCopyOptions(
	ctx context.Context, p *planner, opts tree.CopyOptions,
) (copyOptions, error) {
	c := copyOptions{
		format:          opts.CopyFormat,
		csvExpectHeader: opts.Header,
	}

	switch c.format {
	case tree.CopyFormatText:
		c.null = `\N`
		c.delimiter = '\t'
	case tree.CopyFormatCSV:
		c.null = ""
		c.delimiter = ','
	}

	if opts.Header && c.format != tree.CopyFormatCSV {
		return c, pgerror.Newf(pgcode.FeatureNotSupported, "HEADER only supported with CSV format")
	}

	if opts.Quote != nil {
		if c.format != tree.CopyFormatCSV {
			return c, pgerror.Newf(pgcode.FeatureNotSupported, "QUOTE only supported with CSV format")
		}
		if opts.Quote.RawString() != `"` {
			return c, unimplemented.NewWithIssuef(85574, `QUOTE value %s unsupported`, opts.Quote.RawString())
		}
	}

	exprEval := p.ExprEvaluator("COPY")
	if opts.Delimiter != nil {
		if c.format == tree.CopyFormatBinary {
			return c, pgerror.Newf(
				pgcode.Syntax,
				"DELIMITER unsupported in BINARY format",
			)
		}
		delim, err := exprEval.String(ctx, opts.Delimiter)
		if err != nil {
			return c, err
		}
		if len(delim) != 1 || !utf8.ValidString(delim) {
			return c, pgerror.Newf(
				pgcode.FeatureNotSupported,
				"delimiter must be a single-byte character",
			)
		}
		c.delimiter = delim[0]
	}
	if opts.Null != nil {
		if c.format == tree.CopyFormatBinary {
			return c, pgerror.Newf(
				pgcode.Syntax,
				"NULL unsupported in BINARY format",
			)
		}
		null, err := exprEval.String(ctx, opts.Null)
		if err != nil {
			return c, err
		}
		c.null = null
	}
	if opts.Escape != nil {
		s := opts.Escape.RawString()
		if len(s) != 1 {
			return c, pgerror.Newf(
				pgcode.FeatureNotSupported,
				"ESCAPE must be a single one-byte character",
			)
		}

		if c.format != tree.CopyFormatCSV {
			return c, pgerror.Newf(
				pgcode.FeatureNotSupported,
				"ESCAPE can only be specified for CSV",
			)
		}

		c.csvEscape, _ = utf8.DecodeRuneInString(s)
	}

	if opts.Destination != nil {
		return c, pgerror.Newf(
			pgcode.FeatureNotSupported,
			"DESTINATION can only be specified when table is external storage table",
		)
	}

	return c, nil
}

// copyMachine supports the Copy-in pgwire subprotocol (COPY...FROM STDIN). The
// machine is created by the Executor when that statement is executed; from that
// moment on, the machine takes control of the pgwire connection until
// copyMachine.run() returns. During this time, the machine is responsible for
// sending all the protocol messages (including the messages that are usually
// associated with statement results). Errors however are not sent on the
// connection by the machine; the higher layer is responsible for sending them.
//
// Incoming data is buffered and batched; batches are turned into insertNodes
// that are executed. INSERT privileges are required on the destination table.
//
// See: https://www.postgresql.org/docs/current/static/sql-copy.html
// and: https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-COPY
type copyMachine struct {
	copyFromAST              *tree.CopyFrom
	table                    tree.TableExpr
	columns                  tree.NameList
	resultColumns            colinfo.ResultColumns
	expectedHiddenColumnIdxs []int
	copyOptions
	// textDelim is delimiter converted to a []byte so that we don't have to do that per row.
	textDelim   []byte
	binaryState binaryState
	// forceNotNull disables converting values matching the null string to
	// NULL. The spec says this is only supported for CSV, and also must specify
	// which columns it applies to.
	forceNotNull bool
	// buf is a buffer on top of copy.Reader for text mode and is a pointer to
	// csvReader's underlying buffer for CSV (so we can account for that memory).
	buf *bufio.Reader
	// reader is an io.Reader for binary data, its a copy.Reader unless we're
	// doing a retry in which case its reading from an in memory buffer.
	reader io.Reader
	//csvReader reads directly from the reader since it has a buffer internally.
	csvReader *csv.Reader
	// rows accumulates a batch of rows to be eventually inserted.
	rows rowcontainer.RowContainer
	// insertedRows keeps track of the total number of rows inserted by the
	// machine.
	insertedRows int
	// copyMon tracks copy's memory usage.
	copyMon *mon.BytesMonitor
	// rowsMemAcc accounts for memory used by `rows`.
	rowsMemAcc mon.BoundAccount
	// bufMemAcc accounts for memory used by `buf`; it is kept in sync with
	// buf.Cap().
	bufMemAcc mon.BoundAccount

	// conn is the pgwire connection from which data is to be read.
	conn pgwirebase.Conn

	// execInsertPlan is a function to be used to execute the plan (stored in the
	// planner) which performs an INSERT.
	execInsertPlan func(ctx context.Context, p *planner, res RestrictedCommandResult) error

	txnOpt copyTxnOpt

	// p is the planner used to plan inserts. preparePlanner() needs to be called
	// before preparing each new statement.
	p *planner

	// parsingEvalCtx is an EvalContext used for the very limited needs to strings
	// parsing. Is it not correctly initialized with timestamps, transactions and
	// other things that statements more generally need.
	parsingEvalCtx *eval.Context

	processRows func(ctx context.Context, finalBatch bool) error

	scratchRow    []tree.Datum
	batch         coldata.Batch
	accHelper     colmem.SetAccountingHelper
	typs          []*types.T
	valueHandlers []tree.ValueHandler
	ph            pgdate.ParseHelper

	scratch [128]byte

	// For testing we want to be able to override this on the instance level.
	copyBatchRowSize int
	maxRowMem        int64
	implicitTxn      bool
	copyFastPath     bool
	vectorized       bool
}

// newCopyMachine creates a new copyMachine.
func newCopyMachine(
	ctx context.Context,
	conn pgwirebase.Conn,
	n *tree.CopyFrom,
	p *planner,
	txnOpt copyTxnOpt,
	parentMon *mon.BytesMonitor,
	implicitTxn bool,
	execInsertPlan func(ctx context.Context, p *planner, res RestrictedCommandResult) error,
) (_ *copyMachine, retErr error) {
	cOpts, err := processCopyOptions(ctx, p, n.Options)
	if err != nil {
		return nil, err
	}
	c := &copyMachine{
		conn:        conn,
		copyFromAST: n,
		// TODO(georgiah): Currently, insertRows depends on Table and Columns,
		//  but that dependency can be removed by refactoring it.
		table:          &n.Table,
		columns:        n.Columns,
		copyOptions:    cOpts,
		txnOpt:         txnOpt,
		p:              p,
		execInsertPlan: execInsertPlan,
		implicitTxn:    implicitTxn,
	}
	// We need a planner to do the initial planning, in addition
	// to those used for the main execution of the COPY afterwards.
	txnOpt.initPlanner(ctx, c.p)
	if c.p.SessionData().CopyFromRetriesEnabled && c.implicitTxn {
		// If we are doing retries we may leave the outer txn in the
		// conn_executor in an aborted state which will cause errors
		// when it tries to commit, so have newCopyMachine commit that
		// one and start the copy with a new one.
		cleanup := p.preparePlannerForCopy(ctx, &c.txnOpt, false /*final*/, c.implicitTxn)
		defer func() {
			retErr = cleanup(ctx, retErr)
		}()
	} else {
		txnOpt.resetPlanner(ctx, p, txnOpt.txn, txnOpt.txnTimestamp, txnOpt.stmtTimestamp)
	}
	c.parsingEvalCtx = c.p.EvalContext()
	flags := tree.ObjectLookupFlags{
		Required:             true,
		DesiredObjectKind:    tree.TableObject,
		DesiredTableDescKind: tree.ResolveRequireTableDesc,
	}
	_, tableDesc, err := resolver.ResolveExistingTableObject(ctx, c.p, &n.Table, flags)
	if err != nil {
		return nil, err
	}
	if err := c.p.CheckPrivilege(ctx, tableDesc, privilege.INSERT); err != nil {
		return nil, err
	}
	cols, err := colinfo.ProcessTargetColumns(tableDesc, n.Columns,
		true /* ensureColumns */, false /* allowMutations */)
	if err != nil {
		return nil, err
	}
	c.resultColumns = make(colinfo.ResultColumns, len(cols))
	typs := make([]*types.T, len(cols))
	for i, col := range cols {
		c.resultColumns[i] = colinfo.ResultColumn{
			Name:           col.GetName(),
			Typ:            col.GetType(),
			TableID:        tableDesc.GetID(),
			PGAttributeNum: uint32(col.GetPGAttributeNum()),
		}
		typs[i] = col.GetType()
	}
	c.typs = typs
	// If there are no column specifiers and we expect non-visible columns
	// to have field data then we have to populate the expectedHiddenColumnIdxs
	// field with the columns indexes we expect to be hidden.
	if c.p.SessionData().ExpectAndIgnoreNotVisibleColumnsInCopy && len(n.Columns) == 0 {
		for i, col := range tableDesc.PublicColumns() {
			if col.IsHidden() {
				c.expectedHiddenColumnIdxs = append(c.expectedHiddenColumnIdxs, i)
			}
		}
	}
	c.initMonitoring(ctx, parentMon)
	c.processRows = c.insertRows
	c.copyFastPath = c.p.SessionData().CopyFastPathEnabled

	// We want to do as many rows as we can keeping things under command mem
	// limit. Conservatively target a fraction of kv command size. If we
	// exceed this due to large dynamic values we will bail early and
	// insert the rows we have so far. Note once the coldata.Batch is full
	// we still have all the encoder allocations to make.
	c.maxRowMem = kvserverbase.MaxCommandSize.Get(c.p.execCfg.SV()) / 3

	if c.canSupportVectorized(tableDesc) {
		if err := c.initVectorizedCopy(ctx, typs); err != nil {
			return nil, err
		}
	} else {
		c.copyBatchRowSize = CopyBatchRowSize
		c.vectorized = false
		c.rows.Init(c.rowsMemAcc, colinfo.ColTypeInfoFromResCols(c.resultColumns), c.copyBatchRowSize)
		c.scratchRow = make(tree.Datums, len(c.resultColumns))
	}
	return c, nil
}

func (c *copyMachine) canSupportVectorized(table catalog.TableDescriptor) bool {
	// TODO(cucaroach): support vectorized binary.
	if c.format == tree.CopyFormatBinary {
		return false
	}
	// Vectorized requires avoiding materializing the rows for the optimizer.
	if !c.copyFastPath {
		return false
	}
	if c.p.SessionData().VectorizeMode == sessiondatapb.VectorizeOff {
		return false
	}
	// Vectorized COPY doesn't support foreign key checks, no reason it couldn't
	// but it doesn't work right now because we don't have the ability to
	// hold the results in a bufferNode. We wouldn't want to enable it
	// until we were sure that all the checks could be vectorized so the
	// "bufferNode" used doesn't just get materialized into a datum based
	// row container. I think that requires a vectorized version of lookup
	// join. TODO(cucaroach): extend the vectorized insert code to support
	// insertFastPath style FK checks.
	return len(table.EnforcedOutboundForeignKeys()) == 0
}

func (c *copyMachine) initVectorizedCopy(ctx context.Context, typs []*types.T) error {
	if buildutil.CrdbTestBuild {
		// We have to honor metamorphic default in testing, the transaction
		// commit tests rely on it, specifically they override it to
		// 1.
		c.copyBatchRowSize = CopyBatchRowSize
	} else {
		batchSize := CopyBatchRowSizeVectorDefault
		minBatchSize := 100
		// When the coldata.Batch memory usage exceeds maxRowMem we flush the
		// rows we have so we want the batch's initial memory usage to
		// be smaller so we don't flush every row. We also want to
		// leave a comfortable buffer so some dynamic values (ie
		// strings, json) don't unnecessarily push us past the limit
		// but if we encounter lots of huge dynamic values we do want
		// to flush the batch.
		targetBatchMemUsage := c.maxRowMem / 2

		// Now adjust batch size down based on EstimateBatchSizeBytes. Rather than
		// try to unpack EstimateBatchSizeBytes just use a simple
		// iterative algorithm to arrive at a reasonable batch size.
		// Basically we want something from 100 to maxBatchSize but we
		// don't want to have a bunch of unused memory in the
		// coldata.Batch so dial it in using EstimateBatchSizeBytes.
		for colmem.EstimateBatchSizeBytes(typs, batchSize) > targetBatchMemUsage &&
			batchSize > minBatchSize {
			batchSize /= 2
		}
		// Go back up by tenths to make up for 1/2 reduction overshoot.
		for colmem.EstimateBatchSizeBytes(typs, batchSize) < targetBatchMemUsage &&
			batchSize < CopyBatchRowSizeVectorDefault {
			batchSize += batchSize / 10
		}
		if batchSize > CopyBatchRowSizeVectorDefault {
			batchSize = CopyBatchRowSizeVectorDefault
		}
		// Note its possible we overshot minBatchSize and schema was so wide we
		// didn't go back over it. Worst case we end up with a batch size of 50
		// but if the schema has that many columns smaller is probably better.
		c.copyBatchRowSize = batchSize
	}
	log.VEventf(ctx, 2, "vectorized copy chose %d for batch size", c.copyBatchRowSize)
	c.vectorized = true
	factory := coldataext.NewExtendedColumnFactory(c.p.EvalContext())
	alloc := colmem.NewLimitedAllocator(ctx, &c.rowsMemAcc, nil /*optional unlimited memory account*/, factory)
	alloc.SetMaxBatchSize(c.copyBatchRowSize)
	// TODO(cucaroach): Avoid allocating selection vector.
	c.accHelper.Init(alloc, c.maxRowMem, typs, false /*alwaysReallocate*/)
	// Start with small number of rows, compromise between going too big and
	// overallocating memory and avoiding some doubling growth batches.
	if err := colexecerror.CatchVectorizedRuntimeError(func() {
		c.batch, _ = c.accHelper.ResetMaybeReallocate(c.typs, c.batch, 64)
	}); err != nil {
		return err
	}
	initialMemUsage := c.rowsMemAcc.Used()
	if initialMemUsage > c.maxRowMem {
		// Some tests set the max raft command size lower and if the metamorphic
		// batch size is big enough this can happen. The affect is
		// that every row will be flushed which is fine for testing so
		// ignore it.
		if !buildutil.CrdbTestBuild {
			// The logic above failed us, this shouldn't happen, basically this
			// means EstimateBatchSizeBytes off by a factor of 2.
			panic(errors.AssertionFailedf("EstimateBatchSizeBytes estimated %s for %d row but actual was %s and maxRowMem was %s",
				humanize.IBytes(uint64(colmem.EstimateBatchSizeBytes(typs, c.copyBatchRowSize))),
				c.copyBatchRowSize,
				humanize.IBytes(uint64(initialMemUsage)),
				humanize.IBytes(uint64(c.maxRowMem))))
		}
	}
	c.valueHandlers = make([]tree.ValueHandler, len(typs))
	for i := range typs {
		c.valueHandlers[i] = coldataext.MakeVecHandler(c.batch.ColVec(i))
	}
	return nil
}

func (c *copyMachine) numInsertedRows() int {
	if c == nil {
		return 0
	}
	return c.insertedRows
}

func (c *copyMachine) initMonitoring(ctx context.Context, parentMon *mon.BytesMonitor) {
	// Create a monitor for the COPY command so it can be tracked separate from transaction or session.
	memMetrics := &MemoryMetrics{}
	const noteworthyCopyMemoryUsageBytes = 10 << 20
	c.copyMon = mon.NewMonitor("copy",
		mon.MemoryResource,
		memMetrics.CurBytesCount, memMetrics.MaxBytesHist,
		0, /* increment */
		noteworthyCopyMemoryUsageBytes, c.p.ExecCfg().Settings)
	c.copyMon.StartNoReserved(ctx, parentMon)
	c.bufMemAcc = c.copyMon.MakeBoundAccount()
	c.rowsMemAcc = c.copyMon.MakeBoundAccount()
}

// copyTxnOpt contains information about the transaction in which the copying
// should take place. Can be empty, in which case the copyMachine is responsible
// for managing its own transactions.
type copyTxnOpt struct {
	// If set, txn is the transaction within which all writes have to be
	// performed. Committing the txn is left to the higher layer.  If not set, the
	// machine will split writes between multiple transactions that it will
	// initiate.
	txn           *kv.Txn
	txnTimestamp  time.Time
	stmtTimestamp time.Time
	initPlanner   func(ctx context.Context, p *planner)
	resetPlanner  func(ctx context.Context, p *planner, txn *kv.Txn, txnTS time.Time, stmtTS time.Time)
}

func (c *copyMachine) Close(ctx context.Context) {
	if c.vectorized {
		c.rowsMemAcc.Close(ctx)
	} else {
		c.rows.Close(ctx)
	}
	c.bufMemAcc.Close(ctx)
	c.copyMon.Stop(ctx)
}

type readFunc func(ctx context.Context) error

// run consumes all the copy-in data from the network connection and inserts it
// in the database.
func (c *copyMachine) run(ctx context.Context) (err error) {
	// reader is used to parse input data into rows. It coalesces CopyData
	// protocol messages into a stream of just the data bytes.
	cr := copy.NewReader(c.conn.Rd(), c.p.execCfg.SV())
	defer func() {
		// Discard any bytes we haven't read yet.
		err = errors.CombineErrors(err, cr.Drain())
		// There's a fair number of error paths that can occur outside the
		// control of preparePlannerForCopy and now that we can instill a new
		// txn here due to retries make sure to clean up if we fail.
		if err != nil && c.txnOpt.txn != nil && c.implicitTxn {
			if rollbackErr := c.txnOpt.txn.Rollback(ctx); rollbackErr != nil {
				log.Eventf(ctx, "rollback failed: %s", rollbackErr)
			}
		}
	}()
	format := pgwirebase.FormatText
	if c.format == tree.CopyFormatBinary {
		format = pgwirebase.FormatBinary
	}
	// Send the message describing the columns to the client.
	if err := c.conn.BeginCopyIn(ctx, c.resultColumns, format); err != nil {
		return err
	}

	var r io.Reader = cr
	var bw *copy.BufferingWriter
	atomicRetryEnabled := false
	if c.implicitTxn &&
		c.p.SessionData().CopyFromAtomicEnabled &&
		c.p.SessionData().CopyFromRetriesEnabled &&
		copyRetryBufferSize.Get(c.p.execCfg.SV()) > 0 {
		atomicRetryEnabled = true
		// Memory accounting is simple, it just grows and grows and when run returns account is closed.
		replayAcc := c.copyMon.MakeBoundAccount()
		defer func() {
			replayAcc.Close(ctx)
		}()
		bw = &copy.BufferingWriter{Limit: copyRetryBufferSize.Get(c.p.execCfg.SV()), Grow: func(i int) error {
			return replayAcc.Grow(ctx, int64(i))
		}}
		// Tee everything read from copy.Reader to the buffer.
		r = io.TeeReader(r, bw)
	}

	readFn := c.initReader(r)
	final := false
	for !final {
		if err := readFn(ctx); err != nil {
			if err != io.EOF {
				return err
			} else {
				final = true
			}
		}
		// Buffer will grow when data move in from pgwire and shrink as rows are
		// consumed so just hard adjust on each row.
		if c.buf != nil {
			if err := c.bufMemAcc.ResizeTo(ctx, int64(c.buf.Buffered())); err != nil {
				return err
			}
		}
		batchDone := false
		if c.vectorized && !final {
			if err := colexecerror.CatchVectorizedRuntimeError(func() {
				batchDone = c.accHelper.AccountForSet(c.batch.Length() - 1)
			}); err != nil {
				return err
			}
		}
		if len := c.currentBatchSize(); c.rowsMemAcc.Used() > c.maxRowMem || // flush on wide row
			len == c.copyBatchRowSize || // flush when we reach batch size
			batchDone || // flush when coldata.Batch is full
			final {
			if len != c.copyBatchRowSize {
				log.VEventf(ctx, 2, "copy batch of %d rows flushing due to memory usage %d > %d", len, c.rowsMemAcc.Used(), c.maxRowMem)
			}
			if err := c.processRows(ctx, final); err != nil {
				// Handle atomic retry where we replay the entire COPY.
				if errIsRetriable(err) {
					if atomicRetryEnabled {
						if err2 := c.resetForReplay(ctx, r, bw); err2 != nil {
							return errors.CombineErrors(err, err2)
						} else {
							// We can only replay once.
							atomicRetryEnabled = false
							// If we hit an error on final batch we start over so reset this.
							final = false
							txnOpt := &c.txnOpt
							if rollbackErr := txnOpt.txn.Rollback(ctx); rollbackErr != nil {
								log.Eventf(ctx, "rollback failed: %s", rollbackErr)
							}
							// Start a new transaction.
							// TODO(cucaroach): I feel like we should be delegating to conn_executor for this.
							nodeID, _ := c.p.execCfg.NodeInfo.NodeID.OptionalNodeID()
							txnOpt.txn = kv.NewTxnWithSteppingEnabled(ctx, c.p.execCfg.DB, nodeID, c.p.SessionData().DefaultTxnQualityOfService)
							txnOpt.txn.SetDebugName("copy replay txn")
							txnOpt.txnTimestamp = c.p.execCfg.Clock.PhysicalTime()
							txnOpt.stmtTimestamp = txnOpt.txnTimestamp
							txnOpt.resetPlanner(ctx, c.p, txnOpt.txn, txnOpt.txnTimestamp, txnOpt.stmtTimestamp)
							continue
						}
					} else if bw != nil {
						// If bw was initialized we know we retried and failed
						// again, report a nice error.
						return errors.CombineErrors(errors.New("copy was retried and failed again, trying disabling copy_from_atomic_enabled"), err)
					}
				}
				return err
			}
		}
	}

	return nil
}

func (c *copyMachine) resetForReplay(
	ctx context.Context, r io.Reader, bw *copy.BufferingWriter,
) error {
	// Drain r so all data gets buffered.
	scratch := c.scratch[:]
	for {
		_, err := io.ReadFull(r, scratch)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		} else if err != nil {
			return err
		}
	}
	// If we exceeded the limit the buffering writer turns itself off and is
	// zero'd, no big deal that we still read all that data, we have to do
	// it anyways to drain the protocol buffer and keep on trucking.
	if bw.Cap() == 0 {
		return errors.New("copy retry aborted, sql.copy.retry.max_size exceeded")
	}
	c.initReader(bw.GetReader())
	return c.resetRows(ctx)
}

func (c *copyMachine) initReader(r io.Reader) readFunc {
	c.insertedRows = 0
	var rf readFunc
	switch c.format {
	case tree.CopyFormatText:
		c.textDelim = []byte{c.delimiter}
		rf = c.readTextData
		c.buf = bufio.NewReader(r)
	case tree.CopyFormatBinary:
		rf = c.readBinaryData
		c.buf = nil
		c.reader = r
		c.binaryState = binaryStateNeedSignature
	case tree.CopyFormatCSV:
		rf = c.readCSVData
		c.csvReader = csv.NewReader(r)
		c.buf = c.csvReader.GetBuffer()
		c.csvReader.Comma = rune(c.delimiter)
		c.csvReader.ReuseRecord = true
		c.csvReader.FieldsPerRecord = len(c.resultColumns) + len(c.expectedHiddenColumnIdxs)
		c.csvReader.DontSkipEmptyLines = true
		if c.csvEscape != 0 {
			c.csvReader.Escape = c.csvEscape
		}
	default:
		panic("unknown copy format")
	}

	return rf
}

const (
	lineDelim = '\n'
	endOfData = `\.`
)

func (c *copyMachine) currentBatchSize() int {
	if c.vectorized {
		return c.batch.Length()
	}
	return c.rows.Len()
}

func (c *copyMachine) readTextData(ctx context.Context) (err error) {
	line, err := c.buf.ReadBytes(lineDelim)
	if err != nil && err != io.EOF {
		return err
	}
	if len(line) > 0 && line[len(line)-1] == lineDelim {
		// Remove lineDelim from end.
		line = line[:len(line)-1]
		// Remove a single '\r' at EOL, if present.
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
	}
	if bytes.Equal(line, []byte(`\.`)) {
		return nil
	}
	if err == io.EOF && len(line) == 0 {
		return err
	}
	// If readTextTuple returns nil we want to return EOF if we saw it.
	if e := c.readTextTuple(ctx, line); e != nil {
		return e
	}
	return err
}

func (c *copyMachine) readCSVData(ctx context.Context) (err error) {
	record, err := c.csvReader.Read()

	// If we are using COPY FROM and expecting a header, PostgreSQL ignores
	// the header row in all circumstances. Do the same.
	if c.csvExpectHeader {
		c.csvExpectHeader = false
		return nil
	}

	// Look for end of data before checking for errors, since a field count
	// error will still return record data.
	if len(record) == 1 && !record[0].Quoted && record[0].Val == endOfData {
		return io.EOF
	}
	if err != nil {
		if err == io.EOF {
			return err
		}
		return pgerror.Wrap(err, pgcode.BadCopyFileFormat, "read CSV record")
	}
	return c.readCSVTuple(ctx, record)
}

func (c *copyMachine) maybeIgnoreHiddenColumnsStr(in []csv.Record) []csv.Record {
	if len(c.expectedHiddenColumnIdxs) == 0 {
		return in
	}
	ret := in[:0]
	nextStartIdx := 0
	for _, toIdx := range c.expectedHiddenColumnIdxs {
		ret = append(ret, in[nextStartIdx:toIdx]...)
		nextStartIdx = toIdx + 1
	}
	ret = append(ret, in[nextStartIdx:]...)
	return ret
}

func (c *copyMachine) readCSVTuple(ctx context.Context, record []csv.Record) error {
	if expected := len(c.resultColumns) + len(c.expectedHiddenColumnIdxs); expected != len(record) {
		return pgerror.Newf(pgcode.BadCopyFileFormat,
			"expected %d values, got %d", expected, len(record))
	}
	record = c.maybeIgnoreHiddenColumnsStr(record)
	if c.vectorized {
		vh := c.valueHandlers
		for i, s := range record {
			// NB: When we implement FORCE_NULL, then quoted values also are allowed
			// to be treated as NULL.
			if !s.Quoted && s.Val == c.null {
				vh[i].Null()
				continue
			}
			if err := tree.ParseAndRequireStringHandler(c.resultColumns[i].Typ, s.Val, c.parsingEvalCtx, vh[i], &c.ph); err != nil {
				return err
			}
		}
		c.batch.SetLength(c.batch.Length() + 1)
	} else {
		datums := c.scratchRow
		for i, s := range record {
			// NB: When we implement FORCE_NULL, then quoted values also are allowed
			// to be treated as NULL.
			if !s.Quoted && s.Val == c.null {
				datums[i] = tree.DNull
				continue
			}
			d, _, err := tree.ParseAndRequireString(c.resultColumns[i].Typ, s.Val, c.parsingEvalCtx)
			if err != nil {
				return err
			}
			datums[i] = d
		}
		if _, err := c.rows.AddRow(ctx, datums); err != nil {
			return err
		}
	}
	return nil
}

func (c *copyMachine) readBinaryData(ctx context.Context) (err error) {
	if len(c.expectedHiddenColumnIdxs) > 0 {
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"expect_and_ignore_not_visible_columns_in_copy not supported in binary mode",
		)
	}
	switch c.binaryState {
	case binaryStateNeedSignature:
		if _, err := c.readBinarySignature(); err != nil {
			return err
		}
	case binaryStateRead:
		if err := c.readBinaryTuple(ctx); err != nil {
			if err != io.EOF {
				return errors.Wrapf(err, "read binary tuple")
			}
			return err
		}
	case binaryStateFoundTrailer:
		return pgerror.New(pgcode.BadCopyFileFormat,
			"copy data present after trailer")
	default:
		panic("unknown binary state")
	}
	return nil
}

func (c *copyMachine) readBinaryTuple(ctx context.Context) error {
	var fieldCount int16
	var fieldCountBytes [2]byte
	if _, err := io.ReadFull(c.reader, fieldCountBytes[:]); err != nil {
		return err
	}
	fieldCount = int16(binary.BigEndian.Uint16(fieldCountBytes[:]))
	if fieldCount == -1 {
		c.binaryState = binaryStateFoundTrailer
		return nil
	}
	if fieldCount < 1 {
		return pgerror.Newf(pgcode.BadCopyFileFormat,
			"unexpected field count: %d", fieldCount)
	}
	datums := make(tree.Datums, fieldCount)
	var byteCount int32
	var byteCountBytes [4]byte
	for i := range datums {
		if _, err := io.ReadFull(c.reader, byteCountBytes[:]); err != nil {
			return err
		}
		byteCount = int32(binary.BigEndian.Uint32(byteCountBytes[:]))
		if byteCount == -1 {
			datums[i] = tree.DNull
			continue
		}
		data := make([]byte, byteCount)
		if _, err := io.ReadFull(c.reader, data); err != nil {
			return err
		}
		d, err := pgwirebase.DecodeDatum(
			ctx,
			c.parsingEvalCtx,
			c.resultColumns[i].Typ,
			pgwirebase.FormatBinary,
			data,
		)
		if err != nil {
			return pgerror.Wrapf(err, pgcode.BadCopyFileFormat,
				"decode datum as %s: %s", c.resultColumns[i].Typ.SQLString(), data)
		}
		datums[i] = d
	}
	if _, err := c.rows.AddRow(ctx, datums); err != nil {
		return err
	}
	return nil
}

func (c *copyMachine) readBinarySignature() ([]byte, error) {
	// This is the standard 11-byte binary signature with the flags and
	// header 32-bit integers appended since we only support the zero value
	// of them.
	const binarySignature = "PGCOPY\n\377\r\n\000" + "\x00\x00\x00\x00" + "\x00\x00\x00\x00"
	var sig [11 + 8]byte
	if n, err := io.ReadFull(c.reader, sig[:]); err != nil {
		return sig[:n], err
	}
	if !bytes.Equal(sig[:], []byte(binarySignature)) {
		return sig[:], pgerror.New(pgcode.BadCopyFileFormat,
			"unrecognized binary copy signature")
	}
	c.binaryState = binaryStateRead
	return sig[:], nil
}

// preparePlannerForCopy resets the planner so that it can be used during
// a COPY operation (either COPY to table, or COPY to file).
//
// Depending on how the requesting COPY machine was configured, a new
// transaction might be created.
//
// It returns a cleanup function that needs to be called when we're done with the
// planner (before preparePlannerForCopy is called again). The cleanup
// function commits the txn (if it hasn't already been committed) or rolls it
// back depending on whether it is passed an error. If an error is passed in
// to the cleanup function, the same error is returned.
func (p *planner) preparePlannerForCopy(
	ctx context.Context, txnOpt *copyTxnOpt, finalBatch bool, newTxn bool,
) func(context.Context, error) error {
	txnOpt.resetPlanner(ctx, p, txnOpt.txn, txnOpt.txnTimestamp, txnOpt.stmtTimestamp)
	if newTxn {
		autoCommit := true
		if p.SessionData().CopyFromAtomicEnabled {
			// If the COPY should be atomic, only the final batch can commit.
			autoCommit = finalBatch
		}
		p.autoCommit = autoCommit && !p.execCfg.TestingKnobs.DisableAutoCommitDuringExec
		return func(ctx context.Context, prevErr error) (err error) {
			// Ensure that we commit the transaction if atomic copy is off. If it's on,
			// the conn executor will commit the transaction.
			if prevErr == nil {
				// Ensure that the txn is committed if the copyMachine is in charge of
				// committing its transactions and the execution didn't already commit it
				// (through the planner.autoCommit optimization).
				if !txnOpt.txn.IsCommitted() {
					err = txnOpt.txn.Commit(ctx)
					if err != nil {
						if rollbackErr := txnOpt.txn.Rollback(ctx); rollbackErr != nil {
							log.Eventf(ctx, "rollback failed: %s", rollbackErr)
						}
						return err
					}
				}
			} else if rollbackErr := txnOpt.txn.Rollback(ctx); rollbackErr != nil {
				log.Eventf(ctx, "rollback failed: %s", rollbackErr)
			}

			// Start the implicit txn for the next batch unless its the last one
			// or there was error (need a new txn in case we retry).
			if !finalBatch || prevErr != nil {
				nodeID, _ := p.execCfg.NodeInfo.NodeID.OptionalNodeID()
				txnOpt.txn = kv.NewTxnWithSteppingEnabled(ctx, p.execCfg.DB, nodeID, p.SessionData().DefaultTxnQualityOfService)
				txnOpt.txn.SetDebugName("copy txn")
				txnOpt.txnTimestamp = p.execCfg.Clock.PhysicalTime()
				txnOpt.stmtTimestamp = txnOpt.txnTimestamp
			}
			return prevErr
		}
	} else {
		p.autoCommit = false
		return func(ctx context.Context, prevErr error) (err error) { return prevErr }
	}
}

// insertRows inserts rows, retrying if necessary.
func (c *copyMachine) insertRows(ctx context.Context, finalBatch bool) (err error) {
	rOpts := base.DefaultRetryOptions()
	rOpts.MaxRetries = 5
	r := retry.StartWithCtx(ctx, rOpts)
	for r.Next() {
		if err = c.insertRowsInternal(ctx, finalBatch); err == nil {
			return nil
		} else {
			// It is currently only safe to retry if we are in an implicit transaction.
			// NOTE: we cannot re-use the connExecutor retry scheme here as COPY
			// consumes directly from the read buffer, and the data would no longer
			// be available during the retry.
			// NOTE: in theory we can also retry if c.insertRows == 0.
			if c.implicitTxn && !c.p.SessionData().CopyFromAtomicEnabled && c.p.SessionData().CopyFromRetriesEnabled && errIsRetriable(err) {
				log.SqlExec.Infof(ctx, "%s failed on attempt %d and is retrying, error %+v", c.copyFromAST.String(), r.CurrentAttempt(), err)
				if c.p.ExecCfg().TestingKnobs.CopyFromInsertRetry != nil {
					if err := c.p.ExecCfg().TestingKnobs.CopyFromInsertRetry(); err != nil {
						return err
					}
				}
				continue
			}
			return err
		}
	}
	return err
}

// insertRowsInternal transforms the buffered rows into an insertNode and executes it.
func (c *copyMachine) insertRowsInternal(ctx context.Context, finalBatch bool) (retErr error) {
	newTxn := c.implicitTxn && (!c.p.SessionData().CopyFromAtomicEnabled || finalBatch)
	cleanup := c.p.preparePlannerForCopy(ctx, &c.txnOpt, finalBatch, newTxn)
	defer func() {
		retErr = cleanup(ctx, retErr)
	}()
	if c.p.ExecCfg().TestingKnobs.BeforeCopyFromInsert != nil {
		if err := c.p.ExecCfg().TestingKnobs.BeforeCopyFromInsert(); err != nil {
			return err
		}
	}
	numRows := c.currentBatchSize()
	if numRows == 0 {
		return nil
	}
	// TODO(cucaroach): Investigate caching memo/plan/etc so that we don't
	// rebuild everything for every batch.
	var vc tree.SelectStatement
	if c.copyFastPath {
		if c.vectorized {
			b := tree.VectorRows{Batch: c.batch}
			vc = &tree.LiteralValuesClause{Rows: &b}
		} else {
			vc = &tree.LiteralValuesClause{Rows: &c.rows}
		}
	} else {
		// This is best effort way of mimic'ing pre-copyFastPath behavior, its
		// not exactly the same but should suffice to workaround any bugs due to
		// circumventing the optimizer if they pop up. The difference is this
		// way of emulating the old behavior has the datums go through the row
		// container before becoming []tree.Exprs.
		exprs := make([]tree.Exprs, c.rows.Len())
		for i := 0; i < c.rows.Len(); i++ {
			r := c.rows.At(i)
			newrow := make(tree.Exprs, len(r))
			for j, val := range r {
				newrow[j] = val
			}
			exprs[i] = newrow
		}
		vc = &tree.ValuesClause{Rows: exprs}
	}

	c.p.stmt = Statement{}
	c.p.stmt.AST = &tree.Insert{
		Table:   c.table,
		Columns: c.columns,
		Rows: &tree.Select{
			Select: vc,
		},
		Returning: tree.AbsentReturningClause,
	}

	// TODO(cucaroach): We shouldn't need to do this for every batch.
	if err := c.p.makeOptimizerPlan(ctx); err != nil {
		return err
	}

	var res streamingCommandResult
	err := c.execInsertPlan(ctx, c.p, &res)
	if err != nil {
		return err
	}
	if err := res.Err(); err != nil {
		return err
	}

	if rows := res.RowsAffected(); rows != numRows {
		return errors.AssertionFailedf("COPY didn't insert all buffered rows and yet no error was reported. "+
			"Inserted %d out of %d rows.", rows, numRows)
	}
	c.insertedRows += numRows
	// We're done reset for next batch.
	return c.resetRows(ctx)
}

func (c *copyMachine) resetRows(ctx context.Context) error {
	if c.vectorized {
		var realloc bool
		if err := colexecerror.CatchVectorizedRuntimeError(func() {
			c.batch, realloc = c.accHelper.ResetMaybeReallocate(c.typs, c.batch, 0 /* tuplesToBeSet*/)
		}); err != nil {
			return err
		}
		if realloc {
			for i := range c.typs {
				c.valueHandlers[i] = coldataext.MakeVecHandler(c.batch.ColVec(i))
			}
		} else {
			for _, vh := range c.valueHandlers {
				vh.Reset()
			}
		}
	} else {
		if err := c.rows.UnsafeReset(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (c *copyMachine) maybeIgnoreHiddenColumnsBytes(in [][]byte) [][]byte {
	if len(c.expectedHiddenColumnIdxs) == 0 {
		return in
	}
	ret := in[:0]
	nextStartIdx := 0
	for _, toIdx := range c.expectedHiddenColumnIdxs {
		ret = append(ret, in[nextStartIdx:toIdx]...)
		nextStartIdx = toIdx + 1
	}
	ret = append(ret, in[nextStartIdx:]...)
	return ret
}

func (c *copyMachine) readTextTuple(ctx context.Context, line []byte) error {
	parts := bytes.Split(line, c.textDelim)
	if expected := len(c.resultColumns) + len(c.expectedHiddenColumnIdxs); expected != len(parts) {
		return pgerror.Newf(pgcode.BadCopyFileFormat,
			"expected %d values, got %d", expected, len(parts))
	}
	parts = c.maybeIgnoreHiddenColumnsBytes(parts)
	if c.vectorized {
		return c.readTextTupleVec(ctx, parts)
	} else {
		return c.readTextTupleDatum(ctx, parts)
	}
}

func (c *copyMachine) readTextTupleDatum(ctx context.Context, parts [][]byte) error {
	datums := c.scratchRow
	for i, part := range parts {
		s := unsafeUint8ToString(part)
		// Disable NULL conversion during file uploads.
		if !c.forceNotNull && s == c.null {
			datums[i] = tree.DNull
			continue
		}
		decodeTyp := c.resultColumns[i].Typ
		for decodeTyp.Family() == types.ArrayFamily {
			decodeTyp = decodeTyp.ArrayContents()
		}
		switch decodeTyp.Family() {
		case types.BytesFamily,
			types.DateFamily,
			types.IntervalFamily,
			types.INetFamily,
			types.StringFamily,
			types.TimestampFamily,
			types.TimestampTZFamily,
			types.UuidFamily:
			s = DecodeCopy(s)
		}

		var d tree.Datum
		var err error
		switch c.resultColumns[i].Typ.Family() {
		case types.BytesFamily:
			d = tree.NewDBytes(tree.DBytes(s))
		default:
			d, _, err = tree.ParseAndRequireString(c.resultColumns[i].Typ, s, c.parsingEvalCtx)
		}

		if err != nil {
			return err
		}

		datums[i] = d
	}
	_, err := c.rows.AddRow(ctx, datums)
	return err
}

func (c *copyMachine) readTextTupleVec(ctx context.Context, parts [][]byte) error {
	for i, part := range parts {
		s := unsafeUint8ToString(part)
		// Disable NULL conversion during file uploads.
		if !c.forceNotNull && s == c.null {
			c.valueHandlers[i].Null()
			continue
		}
		decodeTyp := c.resultColumns[i].Typ
		for decodeTyp.Family() == types.ArrayFamily {
			decodeTyp = decodeTyp.ArrayContents()
		}
		switch decodeTyp.Family() {
		case types.BytesFamily,
			types.DateFamily,
			types.IntervalFamily,
			types.INetFamily,
			types.StringFamily,
			types.TimestampFamily,
			types.TimestampTZFamily,
			types.UuidFamily:
			s = DecodeCopy(s)
		}
		switch c.resultColumns[i].Typ.Family() {
		case types.BytesFamily:
			// This just bypasses DecodeRawBytesToByteArrayAuto, not sure why...
			c.valueHandlers[i].Bytes(encoding.UnsafeConvertStringToBytes(s))
		default:
			if err := tree.ParseAndRequireStringHandler(c.resultColumns[i].Typ, s, c.parsingEvalCtx, c.valueHandlers[i], &c.ph); err != nil {
				return err
			}
		}
	}
	c.batch.SetLength(c.batch.Length() + 1)
	return nil
}

// DecodeCopy unescapes a single COPY field.
//
// See: https://www.postgresql.org/docs/9.5/static/sql-copy.html#AEN74432
func DecodeCopy(in string) string {
	var buf strings.Builder
	start := 0
	for i, n := 0, len(in); i < n; i++ {
		if in[i] != '\\' {
			continue
		}
		buf.WriteString(in[start:i])
		i++

		if i >= n {
			// If the last character is \, then write it as-is.
			buf.WriteByte('\\')
		} else {
			ch := in[i]
			if decodedChar := decodeMap[ch]; decodedChar != 0 {
				buf.WriteByte(decodedChar)
			} else if ch == 'x' {
				// \x can be followed by 1 or 2 hex digits.
				if i+1 >= n {
					// Interpret as 'x' if nothing follows.
					buf.WriteByte('x')
				} else {
					ch = in[i+1]
					digit, ok := decodeHexDigit(ch)
					if !ok {
						// If the following character after 'x' is not a digit,
						// write the current character as 'x'.
						buf.WriteByte('x')
					} else {
						i++
						if i+1 < n {
							if v, ok := decodeHexDigit(in[i+1]); ok {
								i++
								digit <<= 4
								digit += v
							}
						}
						buf.WriteByte(digit)
					}
				}
			} else if ch >= '0' && ch <= '7' {
				digit, _ := decodeOctDigit(ch)
				// 1 to 2 more octal digits follow.
				if i+1 < n {
					if v, ok := decodeOctDigit(in[i+1]); ok {
						i++
						digit <<= 3
						digit += v
					}
				}
				if i+1 < n {
					if v, ok := decodeOctDigit(in[i+1]); ok {
						i++
						digit <<= 3
						digit += v
					}
				}
				buf.WriteByte(digit)
			} else {
				// Any other backslashed character will be taken to represent itself.
				buf.WriteByte(ch)
			}
		}
		start = i + 1
	}
	// If there were no backslashes in the input string, we can simply
	// return it.
	if start == 0 {
		return in
	}
	if start < len(in) {
		buf.WriteString(in[start:])
	}
	return buf.String()
}

func decodeDigit(c byte, onlyOctal bool) (byte, bool) {
	switch {
	case c >= '0' && c <= '7':
		return c - '0', true
	case !onlyOctal && c >= '8' && c <= '9':
		return c - '0', true
	case !onlyOctal && c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case !onlyOctal && c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	default:
		return 0, false
	}
}

func decodeOctDigit(c byte) (byte, bool) { return decodeDigit(c, true) }
func decodeHexDigit(c byte) (byte, bool) { return decodeDigit(c, false) }

var decodeMap = map[byte]byte{
	'b':  '\b',
	'f':  '\f',
	'n':  '\n',
	'r':  '\r',
	't':  '\t',
	'v':  '\v',
	'\\': '\\',
}

type binaryState int

const (
	binaryStateNeedSignature binaryState = iota
	binaryStateRead
	binaryStateFoundTrailer
)

func unsafeUint8ToString(data []uint8) string {
	return *(*string)(unsafe.Pointer(&data))
}
