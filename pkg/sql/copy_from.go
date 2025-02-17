// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"strings"
	"time"
	"unicode/utf8"

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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
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
var CopyBatchRowSize = metamorphic.ConstantWithTestRange("copy-batch-size", CopyBatchRowSizeDefault, 1, 50_000)

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
	encoding  string
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

	if opts.Encoding != nil {
		e, err := exprEval.String(ctx, opts.Encoding)
		if err != nil {
			return c, err
		}
		if strings.ToUpper(e) != "UTF8" {
			return c, pgerror.New(pgcode.FeatureNotSupported, "only 'utf8' ENCODING is supported")
		}
		c.encoding = "utf8"
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
	csvInput     bytes.Buffer
	csvReader    *csv.Reader
	// buf is used to parse input data into rows. It also accumulates a partial
	// row between protocol messages.
	buf []byte
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
	cleanup := c.p.preparePlannerForCopy(ctx, &c.txnOpt, false /* finalBatch */, c.implicitTxn)
	defer func() {
		retErr = cleanup(ctx, retErr)
	}()
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
	//
	// We also make the fraction depend on the number of indexes in the table
	// since each secondary index will require a separate CPut command for
	// each input row. We want to pick the fraction to be in [0.1, 0.33] range
	// so that 0.33 is used with no secondary indexes and 0.1 is used with 16 or
	// more secondary indexes.
	// TODO(yuzefovich): the choice of 1/3 as the upper bound with no secondary
	// indexes was made in #98605 via empirical testing. However, it's possible
	// that the testing was missing the realization that each secondary index
	// results in a separate KV and was done on TPCH lineitem table (which has 8
	// secondary indexes), so 1/3 might be actually too conservative.
	maxCommandFraction := copyMaxCommandSizeFraction.Get(c.p.execCfg.SV())
	if maxCommandFraction == 0 {
		maxCommandFraction = 1.0 / 3.0
		if numIndexes := len(tableDesc.AllIndexes()); numIndexes > 1 {
			// Each additional secondary index is "penalized" by reducing the
			// fraction by 1.5%, until 0.1 which is the lower bound.
			maxCommandFraction -= 0.015 * float64(numIndexes-1)
			if maxCommandFraction < 0.1 {
				maxCommandFraction = 0.1
			}
		}
	}
	c.maxRowMem = int64(float64(kvserverbase.MaxCommandSize.Get(c.p.execCfg.SV())) * maxCommandFraction)

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

var copyMaxCommandSizeFraction = settings.RegisterFloatSetting(
	settings.ApplicationLevel,
	"sql.copy.fraction_of_max_command_size",
	"determines the fraction of kv.raft.command.max_size that is used when "+
		"sizing batches of rows when processing COPY commands. Use 0 for default "+
		"adaptive strategy that considers number of secondary indexes",
	0.0,
	settings.Fraction,
)

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
	// TODO(cucaroach): Avoid allocating selection vector.
	c.accHelper.Init(alloc, c.maxRowMem, typs, false /* alwaysReallocate */)
	c.accHelper.SetMaxBatchSize(c.copyBatchRowSize)
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
	c.copyMon = mon.NewMonitor(mon.Options{
		Name:     mon.MakeMonitorName("copy"),
		CurCount: memMetrics.CurBytesCount,
		MaxHist:  memMetrics.MaxBytesHist,
		Settings: c.p.ExecCfg().Settings,
	})
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

// run consumes all the copy-in data from the network connection and inserts it
// in the database.
func (c *copyMachine) run(ctx context.Context) error {
	format := pgwirebase.FormatText
	if c.format == tree.CopyFormatBinary {
		format = pgwirebase.FormatBinary
	}
	// Send the message describing the columns to the client.
	if err := c.conn.BeginCopyIn(ctx, c.resultColumns, format); err != nil {
		return err
	}

	// Read from the connection until we see an ClientMsgCopyDone.
	readBuf := pgwirebase.MakeReadBuffer(
		pgwirebase.ReadBufferOptionWithClusterSettings(&c.p.execCfg.Settings.SV),
	)

	switch c.format {
	case tree.CopyFormatText:
		c.textDelim = []byte{c.delimiter}
	case tree.CopyFormatCSV:
		c.csvInput.Reset()
		c.csvReader = csv.NewReader(&c.csvInput)
		c.csvReader.Comma = rune(c.delimiter)
		c.csvReader.ReuseRecord = true
		c.csvReader.FieldsPerRecord = len(c.resultColumns) + len(c.expectedHiddenColumnIdxs)
		if c.csvEscape != 0 {
			c.csvReader.Escape = c.csvEscape
		}
	}

Loop:
	for {
		typ, _, err := readBuf.ReadTypedMsg(c.conn.Rd())
		if err != nil {
			if pgwirebase.IsMessageTooBigError(err) && typ == pgwirebase.ClientMsgCopyData {
				// Slurp the remaining bytes.
				_, slurpErr := readBuf.SlurpBytes(c.conn.Rd(), pgwirebase.GetMessageTooBigSize(err))
				if slurpErr != nil {
					return errors.CombineErrors(err, errors.Wrapf(slurpErr, "error slurping remaining bytes in COPY"))
				}

				// As per the pgwire spec, we must continue reading until we encounter
				// CopyDone or CopyFail. We don't support COPY in the extended
				// protocol, so we don't need to look for Sync messages. See
				// https://www.postgresql.org/docs/13/protocol-flow.html#PROTOCOL-COPY
				for {
					typ, _, slurpErr = readBuf.ReadTypedMsg(c.conn.Rd())
					if typ == pgwirebase.ClientMsgCopyDone || typ == pgwirebase.ClientMsgCopyFail {
						break
					}
					if slurpErr != nil && !pgwirebase.IsMessageTooBigError(slurpErr) {
						return errors.CombineErrors(err, errors.Wrapf(slurpErr, "error slurping remaining bytes in COPY"))
					}

					_, slurpErr = readBuf.SlurpBytes(c.conn.Rd(), pgwirebase.GetMessageTooBigSize(slurpErr))
					if slurpErr != nil {
						return errors.CombineErrors(err, errors.Wrapf(slurpErr, "error slurping remaining bytes in COPY"))
					}
				}
			}
			return err
		}

		switch typ {
		case pgwirebase.ClientMsgCopyData:
			if err := c.processCopyData(
				ctx, encoding.UnsafeConvertBytesToString(readBuf.Msg), false, /* final */
			); err != nil {
				return err
			}
		case pgwirebase.ClientMsgCopyDone:
			if err := c.processCopyData(
				ctx, "" /* data */, true, /* final */
			); err != nil {
				return err
			}
			break Loop
		case pgwirebase.ClientMsgCopyFail:
			return pgerror.Newf(pgcode.QueryCanceled, "COPY from stdin failed: %s", string(readBuf.Msg))
		case pgwirebase.ClientMsgFlush, pgwirebase.ClientMsgSync:
			// Spec says to "ignore Flush and Sync messages received during copy-in mode".
		default:
			return pgwirebase.NewUnrecognizedMsgTypeErr(typ)
		}
	}

	return nil
}

const (
	lineDelim = '\n'
	endOfData = `\.`
)

// processCopyData buffers incoming data and, once the buffer fills up, inserts
// the accumulated rows.
//
// Args:
// final: If set, buffered data is written even if the buffer is not full.
func (c *copyMachine) processCopyData(ctx context.Context, data string, final bool) (retErr error) {
	// At the end, adjust the mem accounting to reflect what's left in the buffer.
	defer func() {
		if err := c.bufMemAcc.ResizeTo(ctx, int64(cap(c.buf))); err != nil && retErr == nil {
			retErr = err
		}
	}()

	if len(data) > (cap(c.buf) - len(c.buf)) {
		// If it looks like the buffer will need to allocate to accommodate data,
		// account for the memory here. This is not particularly accurate - we don't
		// know how much the buffer will actually grow by.
		if err := c.bufMemAcc.Grow(ctx, int64(len(data))); err != nil {
			return err
		}
	}
	c.buf = append(c.buf, data...)
	var readFn func(ctx context.Context, final bool) (brk bool, err error)
	switch c.format {
	case tree.CopyFormatText:
		readFn = c.readTextData
	case tree.CopyFormatBinary:
		readFn = c.readBinaryData
	case tree.CopyFormatCSV:
		readFn = c.readCSVData
	default:
		panic("unknown copy format")
	}
	for len(c.buf) > 0 {
		brk, err := readFn(ctx, final)
		if err != nil {
			return err
		}
		var batchDone bool
		if !brk && c.vectorized {
			if err := colexecerror.CatchVectorizedRuntimeError(func() {
				batchDone = c.accHelper.AccountForSet(c.batch.Length() - 1)
			}); err != nil {
				if sqlerrors.IsOutOfMemoryError(err) {
					// Getting the COPY to complete is a hail mary but the
					// vectorized inserter will fall back to inserting a row at
					// a time so give it a shot.
					batchDone = true
				} else {
					return err
				}
			}
		}
		// If we have a full batch of rows or we have exceeded maxRowMem process
		// them. Only set finalBatch to true if this is the last
		// CopyData segment AND we have no more data in the buffer.
		if length := c.currentBatchSize(); length > 0 && (c.rowsMemAcc.Used() > c.maxRowMem || length >= c.copyBatchRowSize || batchDone) {
			log.VEventf(
				ctx, 2, "flushing copy batch of %d rows (rowsMemAcc=%s, maxRowMem=%s, batchDone=%t)",
				length, humanize.IBytes(uint64(c.rowsMemAcc.Used())), humanize.IBytes(uint64(c.maxRowMem)), batchDone,
			)
			if err = c.processRows(ctx, final && len(c.buf) == 0); err != nil {
				return err
			}
		}
		if brk {
			break
		}
	}
	// If we're done, process any remainder, if we're not done let more rows
	// accumulate.
	if final {
		log.VEventf(
			ctx, 2, "flushing final copy batch of %d rows (rowsMemAcc=%s, maxRowMem=%s)",
			c.currentBatchSize(), humanize.IBytes(uint64(c.rowsMemAcc.Used())), humanize.IBytes(uint64(c.maxRowMem)),
		)
		return c.processRows(ctx, final)
	}
	return nil
}

func (c *copyMachine) currentBatchSize() int {
	if c.vectorized {
		return c.batch.Length()
	}
	return c.rows.Len()
}

func (c *copyMachine) readTextData(ctx context.Context, final bool) (brk bool, err error) {
	idx := bytes.IndexByte(c.buf, lineDelim)
	var line []byte
	if idx == -1 {
		if !final {
			// Leave the incomplete row in the buffer, to be processed next time.
			return true, nil
		}
		// If this is the final batch, use the whole buffer.
		line = c.buf[:len(c.buf)]
		c.buf = c.buf[len(c.buf):]
	} else {
		// Remove lineDelim from end.
		line = c.buf[:idx]
		c.buf = c.buf[idx+1:]
		// Remove a single '\r' at EOL, if present.
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
	}
	if len(c.buf) == 0 && bytes.Equal(line, []byte(`\.`)) {
		return true, nil
	}
	err = c.readTextTuple(ctx, line)
	return false, err
}

func (c *copyMachine) readCSVData(ctx context.Context, final bool) (brk bool, err error) {
	var fullLine []byte
	quoteCharsSeen := 0
	offset := 0
	// Keep reading lines until we encounter a newline that is not inside a
	// quoted field, and therefore signifies the end of a CSV record.
	for {
		idx := bytes.IndexByte(c.buf[offset:], lineDelim)
		if idx == -1 {
			if final {
				// If we reached EOF and this is the final chunk of input data, then
				// try to process it.
				fullLine = append(fullLine, c.buf[offset:]...)
				c.buf = c.buf[len(c.buf):]
				break
			} else {
				// If there's more CopyData, keep the incomplete row in the
				// buffer, to be processed next time.
				return true, nil
			}
		}
		// Include the delimiter in the line.
		line := c.buf[offset : offset+idx+1]
		offset += idx + 1
		fullLine = append(fullLine, line...)
		// Now we need to calculate if we have reached the end of the quote.
		// If so, break out.
		if c.csvEscape == 0 {
			// CSV escape is not specified and hence defaults to '"'.¥
			// At this point, we know fullLine ends in '\n'. Keep track of the total
			// number of QUOTE chars in fullLine -- if it is even, then it means that
			// the quotes are balanced and '\n' is not in a quoted field.
			// Currently, the QUOTE char and ESCAPE char are both always equal to '"'
			// and are not configurable. As per the COPY spec, any appearance of the
			// QUOTE or ESCAPE characters in an actual value must be preceded by an
			// ESCAPE character. This means that an escaped '"' also results in an even
			// number of '"' characters.
			// This branch is kept in the interests of "backporting safely" - this
			// was the old code. Users who use COPY ... ESCAPE will be the only
			// ones hitting the new code below.
			quoteCharsSeen += bytes.Count(line, []byte{'"'})
		} else {
			// Otherwise, we have to do a manual count of double quotes and
			// ignore any escape characters preceding quotes for counting.
			// For example, if the escape character is '\', we should ignore
			// the intermediate quotes in a string such as `"start"\"\"end"`.
			skipNextChar := false
			for _, ch := range line {
				if skipNextChar {
					skipNextChar = false
					continue
				}
				if ch == '"' {
					quoteCharsSeen++
				}
				if rune(ch) == c.csvEscape {
					skipNextChar = true
				}
			}
		}
		if quoteCharsSeen%2 == 0 {
			c.buf = c.buf[offset:]
			break
		}
	}

	// If we are using COPY FROM and expecting a header, PostgreSQL ignores
	// the header row in all circumstances. Do the same.
	if c.csvExpectHeader {
		c.csvExpectHeader = false
		return c.readCSVData(ctx, final)
	}

	c.csvInput.Write(fullLine)
	record, err := c.csvReader.Read()
	// Look for end of data before checking for errors, since a field count
	// error will still return record data.
	if len(record) == 1 && !record[0].Quoted && record[0].Val == endOfData && len(c.buf) == 0 {
		return true, nil
	}
	if err != nil {
		return false, pgerror.Wrap(err, pgcode.BadCopyFileFormat,
			"read CSV record")
	}
	err = c.readCSVTuple(ctx, record)
	return false, err
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

func (c *copyMachine) readBinaryData(ctx context.Context, final bool) (brk bool, err error) {
	if len(c.expectedHiddenColumnIdxs) > 0 {
		return false, pgerror.Newf(
			pgcode.FeatureNotSupported,
			"expect_and_ignore_not_visible_columns_in_copy not supported in binary mode",
		)
	}
	switch c.binaryState {
	case binaryStateNeedSignature:
		n, err := c.readBinarySignature()
		if err != nil {
			// If this isn't the last message and we saw incomplete data, then
			// leave it in the buffer to process more next time.
			if !final && err == io.ErrUnexpectedEOF {
				return true, nil
			}
			c.buf = c.buf[n:]
			return false, err
		}
		c.buf = c.buf[n:]
		return false, nil
	case binaryStateRead:
		n, err := c.readBinaryTuple(ctx)
		if err != nil {
			// If this isn't the last message and we saw incomplete data, then
			// leave it in the buffer to process more next time.
			if !final && err == io.ErrUnexpectedEOF {
				return true, nil
			}
			c.buf = c.buf[n:]
			return false, errors.Wrapf(err, "read binary tuple")
		}
		c.buf = c.buf[n:]
		return false, nil
	case binaryStateFoundTrailer:
		if !final {
			return false, pgerror.New(pgcode.BadCopyFileFormat,
				"copy data present after trailer")
		}
		return true, nil
	default:
		panic("unknown binary state")
	}
}

func (c *copyMachine) readBinaryTuple(ctx context.Context) (bytesRead int, err error) {
	var fieldCount int16
	var fieldCountBytes [2]byte
	n := copy(fieldCountBytes[:], c.buf[bytesRead:])
	bytesRead += n
	if n < len(fieldCountBytes) {
		return bytesRead, io.ErrUnexpectedEOF
	}
	fieldCount = int16(binary.BigEndian.Uint16(fieldCountBytes[:]))
	if fieldCount == -1 {
		c.binaryState = binaryStateFoundTrailer
		return bytesRead, nil
	}
	if fieldCount < 1 {
		return bytesRead, pgerror.Newf(pgcode.BadCopyFileFormat,
			"unexpected field count: %d", fieldCount)
	}
	datums := make(tree.Datums, fieldCount)
	var byteCount int32
	var byteCountBytes [4]byte
	for i := range datums {
		n := copy(byteCountBytes[:], c.buf[bytesRead:])
		bytesRead += n
		if n < len(byteCountBytes) {
			return bytesRead, io.ErrUnexpectedEOF
		}
		byteCount = int32(binary.BigEndian.Uint32(byteCountBytes[:]))
		if byteCount == -1 {
			datums[i] = tree.DNull
			continue
		}
		data := make([]byte, byteCount)
		n = copy(data, c.buf[bytesRead:])
		bytesRead += n
		if n < len(data) {
			return bytesRead, io.ErrUnexpectedEOF
		}
		d, err := pgwirebase.DecodeDatum(
			ctx,
			c.parsingEvalCtx,
			c.resultColumns[i].Typ,
			pgwirebase.FormatBinary,
			data,
			c.p.datumAlloc,
		)
		if err != nil {
			return bytesRead, pgerror.Wrapf(err, pgcode.BadCopyFileFormat,
				"decode datum as %s: %s", c.resultColumns[i].Typ.SQLString(), data)
		}
		datums[i] = d
	}
	_, err = c.rows.AddRow(ctx, datums)
	if err != nil {
		return bytesRead, err
	}
	return bytesRead, nil
}

// This is the standard 11-byte binary signature with the flags and
// header 32-bit integers appended since we only support the zero value
// of them.
var copyBinarySignature = [19]byte{'P', 'G', 'C', 'O', 'P', 'Y', '\n', '\377', '\r', '\n', '\000', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00'}

func (c *copyMachine) readBinarySignature() (int, error) {
	var sig [19]byte
	n := copy(sig[:], c.buf)
	if n < len(sig) {
		return n, io.ErrUnexpectedEOF
	}
	if sig != copyBinarySignature {
		return n, pgerror.New(pgcode.BadCopyFileFormat,
			"unrecognized binary copy signature")
	}
	c.binaryState = binaryStateRead
	return n, nil
}

// preparePlannerForCopy resets the planner so that it can be used during
// a COPY operation (either COPY to table, or COPY to file).
//
// Depending on how the requesting COPY machine was configured, a new
// transaction might be created.
//
// It returns a cleanup function that needs to be called when we're done with
// the planner (before preparePlannerForCopy is called again). If
// CopyFromAtomicEnabled is false, the cleanup function commits the txn (if it
// hasn't already been committed) or rolls it back depending on whether it is
// passed an error. If an error is passed in to the cleanup function, the same
// error is returned.
func (p *planner) preparePlannerForCopy(
	ctx context.Context, txnOpt *copyTxnOpt, finalBatch bool, implicitTxn bool,
) func(context.Context, error) error {
	autoCommit := implicitTxn
	txnOpt.resetPlanner(ctx, p, txnOpt.txn, txnOpt.txnTimestamp, txnOpt.stmtTimestamp)
	if implicitTxn {
		if p.SessionData().CopyFromAtomicEnabled {
			// If the COPY should be atomic, only the final batch can commit.
			autoCommit = finalBatch
		}
	}
	p.autoCommit = autoCommit && !p.execCfg.TestingKnobs.DisableAutoCommitDuringExec

	return func(ctx context.Context, prevErr error) error {
		// Ensure that we commit the transaction if atomic copy is off. If it's
		// on, the conn executor will commit the transaction.
		if implicitTxn && !p.SessionData().CopyFromAtomicEnabled {
			if prevErr == nil {
				// Ensure that the txn is committed if the copyMachine is in
				// charge of committing its transactions and the execution
				// didn't already commit it (through the planner.autoCommit
				// optimization).
				if !txnOpt.txn.IsCommitted() {
					if err := txnOpt.txn.Commit(ctx); err != nil {
						if rollbackErr := txnOpt.txn.Rollback(ctx); rollbackErr != nil {
							// Since we failed to roll back the txn, we don't
							// know whether retrying this batch wouldn't corrupt
							// the data, so we return this non-retriable error.
							return errors.Wrap(rollbackErr, "non-atomic COPY couldn't roll back its txn")
						}
						// The rollback succeeded, so we can simply attempt to
						// retry this batch, after having prepared a fresh txn
						// below.
						prevErr = err
					}
				}
			} else if rollbackErr := txnOpt.txn.Rollback(ctx); rollbackErr != nil {
				// Since we failed to roll back the txn, we don't know whether
				// retrying this batch wouldn't corrupt the data, so we return
				// this non-retriable error.
				return errors.Wrap(rollbackErr, "non-atomic COPY couldn't roll back its txn")
			}

			// Start the implicit txn for the next batch.
			nodeID, _ := p.execCfg.NodeInfo.NodeID.OptionalNodeID()
			txnOpt.txn = kv.NewTxnWithSteppingEnabled(ctx, p.execCfg.DB, nodeID, p.SessionData().CopyTxnQualityOfService)
			if !p.SessionData().CopyWritePipeliningEnabled {
				if err := txnOpt.txn.DisablePipelining(); err != nil {
					return err
				}
			}
			txnOpt.txnTimestamp = p.execCfg.Clock.PhysicalTime()
			txnOpt.stmtTimestamp = txnOpt.txnTimestamp
		}
		return prevErr
	}
}

// doneWithRows resets the buffered data (either the columnar batch or the row
// container) for reuse. It also updates insertedRows accordingly.
func (c *copyMachine) doneWithRows(ctx context.Context) error {
	c.insertedRows += c.currentBatchSize()
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
		return nil
	}
	return c.rows.UnsafeReset(ctx)
}

// insertRows inserts rows, retrying if necessary.
func (c *copyMachine) insertRows(ctx context.Context, finalBatch bool) error {
	var err error

	rOpts := base.DefaultRetryOptions()
	rOpts.MaxRetries = int(c.p.SessionData().CopyNumRetriesPerBatch)
	if rOpts.MaxRetries < 1 {
		// MaxRetries == 0 means infinite number of attempts, and although
		// CopyNumRetriesPerBatch should always be a positive number, let's be
		// careful here.
		rOpts.MaxRetries = 1
	}
	r := retry.StartWithCtx(ctx, rOpts)
	for r.Next() {
		if err = c.insertRowsInternal(ctx, finalBatch); err == nil {
			// We're done with this batch of rows, so reset the buffered data
			// for the next batch.
			return c.doneWithRows(ctx)
		} else {
			if errIsRetriable(err) {
				log.SqlExec.Infof(ctx, "%s failed on attempt %d and with retriable error %+v", c.copyFromAST.String(), r.CurrentAttempt(), err)
				// It is currently only safe to retry if we are not in atomic copy
				// mode & we are in an implicit transaction.
				//
				// NOTE: we cannot re-use the connExecutor retry scheme here as COPY
				// consumes directly from the read buffer, and the data would no
				// longer be available during the retry.
				if c.implicitTxn && !c.p.SessionData().CopyFromAtomicEnabled && c.p.SessionData().CopyFromRetriesEnabled {
					log.SqlExec.Infof(ctx, "%s is retrying", c.copyFromAST.String())
					if c.p.ExecCfg().TestingKnobs.CopyFromInsertRetry != nil {
						if err := c.p.ExecCfg().TestingKnobs.CopyFromInsertRetry(); err != nil {
							return err
						}
					}
					continue
				} else {
					log.SqlExec.Infof(
						ctx,
						"%s is not retrying; "+
							"implicit: %v; copy_from_atomic_enabled: %v; copy_from_retriable_enabled %v",
						c.copyFromAST.String(), c.implicitTxn,
						c.p.SessionData().CopyFromAtomicEnabled, c.p.SessionData().CopyFromRetriesEnabled,
					)
				}
			} else {
				log.SqlExec.Infof(ctx, "%s failed on attempt %d and with non-retriable error %+v", c.copyFromAST.String(), r.CurrentAttempt(), err)
			}
			return err
		}
	}
	return err
}

// insertRowsInternal transforms the buffered rows into an insertNode and executes it.
func (c *copyMachine) insertRowsInternal(ctx context.Context, finalBatch bool) (retErr error) {
	numRows := c.currentBatchSize()
	if numRows == 0 {
		return nil
	}
	cleanup := c.p.preparePlannerForCopy(ctx, &c.txnOpt, finalBatch, c.implicitTxn)
	defer func() {
		retErr = cleanup(ctx, retErr)
	}()
	if c.p.ExecCfg().TestingKnobs.CopyFromInsertBeforeBatch != nil {
		if err := c.p.ExecCfg().TestingKnobs.CopyFromInsertBeforeBatch(c.txnOpt.txn); err != nil {
			return err
		}
	}
	if c.p.ExecCfg().TestingKnobs.CopyFromInsertAfterBatch != nil {
		defer func() {
			if retErr == nil {
				if err := c.p.ExecCfg().TestingKnobs.CopyFromInsertAfterBatch(); err != nil {
					retErr = err
				}
			}
		}()
	}
	// TODO(cucaroach): Investigate caching memo/plan/etc so that we don't
	// rebuild everything for every batch.
	var vc tree.SelectStatement
	if c.copyFastPath {
		if c.vectorized {
			if buildutil.CrdbTestBuild {
				if c.txnOpt.txn.BufferedWritesEnabled() {
					return errors.AssertionFailedf("buffered writes should have been disabled for COPY")
				}
			}
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
		s := encoding.UnsafeConvertBytesToString(part)
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
		s := encoding.UnsafeConvertBytesToString(part)
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
