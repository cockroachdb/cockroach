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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/csv"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

type copyMachineInterface interface {
	run(ctx context.Context) error
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
	table         tree.TableExpr
	columns       tree.NameList
	resultColumns colinfo.ResultColumns
	format        tree.CopyFormat
	delimiter     byte
	// textDelim is delimiter converted to a []byte so that we don't have to do that per row.
	textDelim   []byte
	null        string
	binaryState binaryState
	// forceNotNull disables converting values matching the null string to
	// NULL. The spec says this is only supported for CSV, and also must specify
	// which columns it applies to.
	forceNotNull bool
	csvInput     bytes.Buffer
	csvReader    *csv.Reader
	// buf is used to parse input data into rows. It also accumulates a partial
	// row between protocol messages.
	buf bytes.Buffer
	// rows accumulates a batch of rows to be eventually inserted.
	rows []tree.Exprs
	// insertedRows keeps track of the total number of rows inserted by the
	// machine.
	insertedRows int
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
	p planner

	// parsingEvalCtx is an EvalContext used for the very limited needs to strings
	// parsing. Is it not correctly initialized with timestamps, transactions and
	// other things that statements more generally need.
	parsingEvalCtx *tree.EvalContext

	processRows func(ctx context.Context) error
}

// newCopyMachine creates a new copyMachine.
func newCopyMachine(
	ctx context.Context,
	conn pgwirebase.Conn,
	n *tree.CopyFrom,
	txnOpt copyTxnOpt,
	execCfg *ExecutorConfig,
	execInsertPlan func(ctx context.Context, p *planner, res RestrictedCommandResult) error,
) (_ *copyMachine, retErr error) {
	c := &copyMachine{
		conn: conn,
		// TODO(georgiah): Currently, insertRows depends on Table and Columns,
		//  but that dependency can be removed by refactoring it.
		table:   &n.Table,
		columns: n.Columns,
		format:  n.Options.CopyFormat,
		txnOpt:  txnOpt,
		// The planner will be prepared before use.
		p:              planner{execCfg: execCfg, alloc: &tree.DatumAlloc{}},
		execInsertPlan: execInsertPlan,
	}

	// We need a planner to do the initial planning, in addition
	// to those used for the main execution of the COPY afterwards.
	cleanup := c.p.preparePlannerForCopy(ctx, txnOpt)
	defer func() {
		retErr = cleanup(ctx, retErr)
	}()
	c.parsingEvalCtx = c.p.EvalContext()

	switch c.format {
	case tree.CopyFormatText:
		c.null = `\N`
		c.delimiter = '\t'
	case tree.CopyFormatCSV:
		c.null = ""
		c.delimiter = ','
	}

	if n.Options.Delimiter != nil {
		if c.format == tree.CopyFormatBinary {
			return nil, errors.Newf("DELIMITER unsupported in BINARY format")
		}
		fn, err := c.p.TypeAsString(ctx, n.Options.Delimiter, "COPY")
		if err != nil {
			return nil, err
		}
		delim, err := fn()
		if err != nil {
			return nil, err
		}
		if len(delim) != 1 || !utf8.ValidString(delim) {
			return nil, errors.Newf("delimiter must be a single-byte character")
		}
		c.delimiter = delim[0]
	}
	if n.Options.Null != nil {
		if c.format == tree.CopyFormatBinary {
			return nil, errors.Newf("NULL unsupported in BINARY format")
		}
		fn, err := c.p.TypeAsString(ctx, n.Options.Null, "COPY")
		if err != nil {
			return nil, err
		}
		c.null, err = fn()
		if err != nil {
			return nil, err
		}
	}

	flags := tree.ObjectLookupFlagsWithRequiredTableKind(tree.ResolveRequireTableDesc)
	_, tableDesc, err := resolver.ResolveExistingTableObject(ctx, &c.p, &n.Table, flags)
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
	for i, col := range cols {
		c.resultColumns[i] = colinfo.ResultColumn{
			Name:           col.GetName(),
			Typ:            col.GetType(),
			TableID:        tableDesc.GetID(),
			PGAttributeNum: col.GetPGAttributeNum(),
		}
	}
	c.rowsMemAcc = c.p.extendedEvalCtx.Mon.MakeBoundAccount()
	c.bufMemAcc = c.p.extendedEvalCtx.Mon.MakeBoundAccount()
	c.processRows = c.insertRows
	return c, nil
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
	resetPlanner  func(ctx context.Context, p *planner, txn *kv.Txn, txnTS time.Time, stmtTS time.Time)

	// resetExecutor should be called upon completing a batch from the copy
	// machine when the copy machine handles its own transaction.
	resetExtraTxnState func(ctx context.Context) error
}

// run consumes all the copy-in data from the network connection and inserts it
// in the database.
func (c *copyMachine) run(ctx context.Context) error {
	defer c.rowsMemAcc.Close(ctx)
	defer c.bufMemAcc.Close(ctx)

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
		c.csvReader.FieldsPerRecord = len(c.resultColumns)
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
				ctx, string(readBuf.Msg), false, /* final */
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
			return errors.Newf("client canceled COPY")
		case pgwirebase.ClientMsgFlush, pgwirebase.ClientMsgSync:
			// Spec says to "ignore Flush and Sync messages received during copy-in mode".
		default:
			return pgwirebase.NewUnrecognizedMsgTypeErr(typ)
		}
	}

	// Finalize execution by sending the statement tag and number of rows
	// inserted.
	dummy := tree.CopyFrom{}
	tag := []byte(dummy.StatementTag())
	tag = append(tag, ' ')
	tag = strconv.AppendInt(tag, int64(c.insertedRows), 10 /* base */)
	return c.conn.SendCommandComplete(tag)
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
		if err := c.bufMemAcc.ResizeTo(ctx, int64(c.buf.Cap())); err != nil && retErr == nil {
			retErr = err
		}
	}()

	// When this many rows are in the copy buffer, they are inserted.
	const copyBatchRowSize = 100

	if len(data) > (c.buf.Cap() - c.buf.Len()) {
		// If it looks like the buffer will need to allocate to accommodate data,
		// account for the memory here. This is not particularly accurate - we don't
		// know how much the buffer will actually grow by.
		if err := c.bufMemAcc.ResizeTo(ctx, int64(len(data))); err != nil {
			return err
		}
	}
	c.buf.WriteString(data)
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
	for c.buf.Len() > 0 {
		brk, err := readFn(ctx, final)
		if err != nil {
			return err
		}
		if brk {
			break
		}
	}
	// Only do work if we have a full batch of rows or this is the end.
	if ln := len(c.rows); !final && (ln == 0 || ln < copyBatchRowSize) {
		return nil
	}
	return c.processRows(ctx)
}

func (c *copyMachine) readTextData(ctx context.Context, final bool) (brk bool, err error) {
	line, err := c.buf.ReadBytes(lineDelim)
	if err != nil {
		if err != io.EOF {
			return false, err
		} else if !final {
			// Put the incomplete row back in the buffer, to be processed next time.
			c.buf.Write(line)
			return true, nil
		}
	} else {
		// Remove lineDelim from end.
		line = line[:len(line)-1]
		// Remove a single '\r' at EOL, if present.
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
	}
	if c.buf.Len() == 0 && bytes.Equal(line, []byte(`\.`)) {
		return true, nil
	}
	err = c.readTextTuple(ctx, line)
	return false, err
}

func (c *copyMachine) readCSVData(ctx context.Context, final bool) (brk bool, err error) {
	var fullLine []byte
	quoteCharsSeen := 0
	// Keep reading lines until we encounter a newline that is not inside a
	// quoted field, and therefore signifies the end of a CSV record.
	for {
		line, err := c.buf.ReadBytes(lineDelim)
		fullLine = append(fullLine, line...)
		if err != nil {
			if err == io.EOF {
				if final {
					// If we reached EOF and this is the final chunk of input data, then
					// try to process it.
					break
				} else {
					// If there's more CopyData, put the incomplete row back in the
					// buffer, to be processed next time.
					c.buf.Write(fullLine)
					return true, nil
				}
			} else {
				return false, err
			}
		}
		// At this point, we know fullLine ends in '\n'. Keep track of the total
		// number of QUOTE chars in fullLine -- if it is even, then it means that
		// the quotes are balanced and '\n' is not in a quoted field.
		// Currently, the QUOTE char and ESCAPE char are both always equal to '"'
		// and are not configurable. As per the COPY spec, any appearance of the
		// QUOTE or ESCAPE characters in an actual value must be preceded by an
		// ESCAPE character. This means that an escaped '"' also results in an even
		// number of '"' characters.
		quoteCharsSeen += bytes.Count(line, []byte{'"'})
		if quoteCharsSeen%2 == 0 {
			break
		}
	}

	c.csvInput.Write(fullLine)
	record, err := c.csvReader.Read()
	// Look for end of data before checking for errors, since a field count
	// error will still return record data.
	if len(record) == 1 && record[0] == endOfData && c.buf.Len() == 0 {
		return true, nil
	}
	if err != nil {
		return false, pgerror.Wrap(err, pgcode.BadCopyFileFormat,
			"read CSV record")
	}
	err = c.readCSVTuple(ctx, record)
	return false, err
}

func (c *copyMachine) readCSVTuple(ctx context.Context, record []string) error {
	if len(record) != len(c.resultColumns) {
		return pgerror.Newf(pgcode.BadCopyFileFormat,
			"expected %d values, got %d", len(c.resultColumns), len(record))
	}
	exprs := make(tree.Exprs, len(record))
	for i, s := range record {
		if s == c.null {
			exprs[i] = tree.DNull
			continue
		}
		d, _, err := tree.ParseAndRequireString(c.resultColumns[i].Typ, s, c.parsingEvalCtx)
		if err != nil {
			return err
		}

		sz := d.Size()
		if err := c.rowsMemAcc.Grow(ctx, int64(sz)); err != nil {
			return err
		}

		exprs[i] = d
	}
	if err := c.rowsMemAcc.Grow(ctx, int64(unsafe.Sizeof(exprs))); err != nil {
		return err
	}

	c.rows = append(c.rows, exprs)
	return nil
}

func (c *copyMachine) readBinaryData(ctx context.Context, final bool) (brk bool, err error) {
	switch c.binaryState {
	case binaryStateNeedSignature:
		if readSoFar, err := c.readBinarySignature(); err != nil {
			// If this isn't the last message and we saw incomplete data, then
			// put it back in the buffer to process more next time.
			if !final && (err == io.EOF || err == io.ErrUnexpectedEOF) {
				c.buf.Write(readSoFar)
				return true, nil
			}
			return false, err
		}
	case binaryStateRead:
		if readSoFar, err := c.readBinaryTuple(ctx); err != nil {
			// If this isn't the last message and we saw incomplete data, then
			// put it back in the buffer to process more next time.
			if !final && (err == io.EOF || err == io.ErrUnexpectedEOF) {
				c.buf.Write(readSoFar)
				return true, nil
			}
			return false, errors.Wrapf(err, "read binary tuple")
		}
	case binaryStateFoundTrailer:
		if !final {
			return false, pgerror.New(pgcode.BadCopyFileFormat,
				"copy data present after trailer")
		}
		return true, nil
	default:
		panic("unknown binary state")
	}
	return false, nil
}

func (c *copyMachine) readBinaryTuple(ctx context.Context) (readSoFar []byte, err error) {
	var fieldCount int16
	var fieldCountBytes [2]byte
	n, err := io.ReadFull(&c.buf, fieldCountBytes[:])
	readSoFar = append(readSoFar, fieldCountBytes[:n]...)
	if err != nil {
		return readSoFar, err
	}
	fieldCount = int16(binary.BigEndian.Uint16(fieldCountBytes[:]))
	if fieldCount == -1 {
		c.binaryState = binaryStateFoundTrailer
		return nil, nil
	}
	if fieldCount < 1 {
		return nil, pgerror.Newf(pgcode.BadCopyFileFormat,
			"unexpected field count: %d", fieldCount)
	}
	exprs := make(tree.Exprs, fieldCount)
	var byteCount int32
	var byteCountBytes [4]byte
	for i := range exprs {
		n, err := io.ReadFull(&c.buf, byteCountBytes[:])
		readSoFar = append(readSoFar, byteCountBytes[:n]...)
		if err != nil {
			return readSoFar, err
		}
		byteCount = int32(binary.BigEndian.Uint32(byteCountBytes[:]))
		if byteCount == -1 {
			exprs[i] = tree.DNull
			continue
		}
		data := make([]byte, byteCount)
		n, err = io.ReadFull(&c.buf, data)
		readSoFar = append(readSoFar, data[:n]...)
		if err != nil {
			return readSoFar, err
		}
		d, err := pgwirebase.DecodeDatum(
			c.parsingEvalCtx,
			c.resultColumns[i].Typ,
			pgwirebase.FormatBinary,
			data,
		)
		if err != nil {
			return nil, pgerror.Wrapf(err, pgcode.BadCopyFileFormat,
				"decode datum as %s: %s", c.resultColumns[i].Typ.SQLString(), data)
		}
		sz := d.Size()
		if err := c.rowsMemAcc.Grow(ctx, int64(sz)); err != nil {
			return nil, err
		}
		exprs[i] = d
	}
	if err = c.rowsMemAcc.Grow(ctx, int64(unsafe.Sizeof(exprs))); err != nil {
		return nil, err
	}
	c.rows = append(c.rows, exprs)
	return nil, nil
}

func (c *copyMachine) readBinarySignature() ([]byte, error) {
	// This is the standard 11-byte binary signature with the flags and
	// header 32-bit integers appended since we only support the zero value
	// of them.
	const binarySignature = "PGCOPY\n\377\r\n\000" + "\x00\x00\x00\x00" + "\x00\x00\x00\x00"
	var sig [11 + 8]byte
	if n, err := io.ReadFull(&c.buf, sig[:]); err != nil {
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
// It returns a cleanup function that needs to be called when we're
// done with the planner (before preparePlannerForCopy is called
// again). The cleanup function commits the txn (if it hasn't already
// been committed) or rolls it back depending on whether it is passed
// an error. If an error is passed in to the cleanup function, the
// same error is returned.
func (p *planner) preparePlannerForCopy(
	ctx context.Context, txnOpt copyTxnOpt,
) func(context.Context, error) error {
	txn := txnOpt.txn
	txnTs := txnOpt.txnTimestamp
	stmtTs := txnOpt.stmtTimestamp
	autoCommit := false
	if txn == nil {
		nodeID, _ := p.execCfg.NodeID.OptionalNodeID()
		// The session data stack in the planner is not set up at this point, so use
		// the default Normal QoSLevel.
		txn = kv.NewTxnWithSteppingEnabled(ctx, p.execCfg.DB, nodeID, sessiondatapb.Normal)
		txnTs = p.execCfg.Clock.PhysicalTime()
		stmtTs = txnTs
		autoCommit = true
	}
	txnOpt.resetPlanner(ctx, p, txn, txnTs, stmtTs)
	p.autoCommit = autoCommit

	return func(ctx context.Context, prevErr error) (err error) {
		// Ensure that we clean up any accumulated extraTxnState state if we've
		// been handed a mechanism to do so.
		if txnOpt.resetExtraTxnState != nil {
			defer func() {
				// Note: combine errors will return nil if both are nil and the
				// non-nil error in the case that there's just one.
				err = errors.CombineErrors(err, txnOpt.resetExtraTxnState(ctx))
			}()
		}
		if prevErr == nil {
			// Ensure that the txn is committed if the copyMachine is in charge of
			// committing its transactions and the execution didn't already commit it
			// (through the planner.autoCommit optimization).
			if autoCommit && !txn.IsCommitted() {
				return txn.CommitOrCleanup(ctx)
			}
			return nil
		}
		txn.CleanupOnError(ctx, prevErr)
		return prevErr
	}
}

// insertRows transforms the buffered rows into an insertNode and executes it.
func (c *copyMachine) insertRows(ctx context.Context) (retErr error) {
	if len(c.rows) == 0 {
		return nil
	}
	cleanup := c.p.preparePlannerForCopy(ctx, c.txnOpt)
	defer func() {
		retErr = cleanup(ctx, retErr)
	}()

	vc := &tree.ValuesClause{Rows: c.rows}
	numRows := len(c.rows)
	// Reuse the same backing array once the Insert is complete.
	c.rows = c.rows[:0]
	c.rowsMemAcc.Clear(ctx)

	c.p.stmt = Statement{}
	c.p.stmt.AST = &tree.Insert{
		Table:   c.table,
		Columns: c.columns,
		Rows: &tree.Select{
			Select: vc,
		},
		Returning: tree.AbsentReturningClause,
	}
	if err := c.p.makeOptimizerPlan(ctx); err != nil {
		return err
	}

	var res streamingCommandResult
	err := c.execInsertPlan(ctx, &c.p, &res)
	if err != nil {
		return err
	}
	if err := res.Err(); err != nil {
		return err
	}

	if rows := res.RowsAffected(); rows != numRows {
		log.Fatalf(ctx, "didn't insert all buffered rows and yet no error was reported. "+
			"Inserted %d out of %d rows.", rows, numRows)
	}
	c.insertedRows += numRows

	return nil
}

func (c *copyMachine) readTextTuple(ctx context.Context, line []byte) error {
	parts := bytes.Split(line, c.textDelim)
	if len(parts) != len(c.resultColumns) {
		return pgerror.Newf(pgcode.BadCopyFileFormat,
			"expected %d values, got %d", len(c.resultColumns), len(parts))
	}
	exprs := make(tree.Exprs, len(parts))
	for i, part := range parts {
		s := string(part)
		// Disable NULL conversion during file uploads.
		if !c.forceNotNull && s == c.null {
			exprs[i] = tree.DNull
			continue
		}
		switch t := c.resultColumns[i].Typ; t.Family() {
		case types.BytesFamily,
			types.DateFamily,
			types.IntervalFamily,
			types.INetFamily,
			types.StringFamily,
			types.TimestampFamily,
			types.TimestampTZFamily,
			types.UuidFamily:
			s = decodeCopy(s)
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

		sz := d.Size()
		if err := c.rowsMemAcc.Grow(ctx, int64(sz)); err != nil {
			return err
		}

		exprs[i] = d
	}
	if err := c.rowsMemAcc.Grow(ctx, int64(unsafe.Sizeof(exprs))); err != nil {
		return err
	}

	c.rows = append(c.rows, exprs)
	return nil
}

// decodeCopy unescapes a single COPY field.
//
// See: https://www.postgresql.org/docs/9.5/static/sql-copy.html#AEN74432
func decodeCopy(in string) string {
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
