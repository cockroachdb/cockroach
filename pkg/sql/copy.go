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
	"io"
	"strconv"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	resultColumns sqlbase.ResultColumns
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
		txnOpt:  txnOpt,
		// The planner will be prepared before use.
		p:              planner{execCfg: execCfg, alloc: &sqlbase.DatumAlloc{}},
		execInsertPlan: execInsertPlan,
	}

	// We need a planner to do the initial planning, in addition
	// to those used for the main execution of the COPY afterwards.
	cleanup := c.p.preparePlannerForCopy(ctx, txnOpt)
	defer func() {
		retErr = cleanup(ctx, retErr)
	}()
	c.parsingEvalCtx = c.p.EvalContext()

	tableDesc, err := resolver.ResolveExistingTableObject(ctx, &c.p, &n.Table, tree.ObjectLookupFlagsWithRequired(), resolver.ResolveRequireTableDesc)
	if err != nil {
		return nil, err
	}
	if err := c.p.CheckPrivilege(ctx, tableDesc, privilege.INSERT); err != nil {
		return nil, err
	}
	cols, err := sqlbase.ProcessTargetColumns(tableDesc, n.Columns,
		true /* ensureColumns */, false /* allowMutations */)
	if err != nil {
		return nil, err
	}
	c.resultColumns = make(sqlbase.ResultColumns, len(cols))
	for i := range cols {
		c.resultColumns[i] = sqlbase.ResultColumn{
			Name:           cols[i].Name,
			Typ:            cols[i].Type,
			TableID:        tableDesc.GetID(),
			PGAttributeNum: cols[i].GetLogicalColumnID(),
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
}

// run consumes all the copy-in data from the network connection and inserts it
// in the database.
func (c *copyMachine) run(ctx context.Context) error {
	defer c.rowsMemAcc.Close(ctx)
	defer c.bufMemAcc.Close(ctx)

	// Send the message describing the columns to the client.
	if err := c.conn.BeginCopyIn(ctx, c.resultColumns); err != nil {
		return err
	}

	// Read from the connection until we see an ClientMsgCopyDone.
	readBuf := pgwirebase.ReadBuffer{}

Loop:
	for {
		typ, _, err := readBuf.ReadTypedMsg(c.conn.Rd())
		if err != nil {
			return err
		}

		switch typ {
		case pgwirebase.ClientMsgCopyData:
			if err := c.processCopyData(
				ctx, string(readBuf.Msg), c.p.EvalContext(), false, /* final */
			); err != nil {
				return err
			}
		case pgwirebase.ClientMsgCopyDone:
			if err := c.processCopyData(
				ctx, "" /* data */, c.p.EvalContext(), true, /* final */
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
	nullString = `\N`
	lineDelim  = '\n'
)

var (
	fieldDelim = []byte{'\t'}
)

// processCopyData buffers incoming data and, once the buffer fills up, inserts
// the accumulated rows.
//
// Args:
// final: If set, buffered data is written even if the buffer is not full.
func (c *copyMachine) processCopyData(
	ctx context.Context, data string, evalCtx *tree.EvalContext, final bool,
) (retErr error) {
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
	for c.buf.Len() > 0 {
		line, err := c.buf.ReadBytes(lineDelim)
		if err != nil {
			if err != io.EOF {
				return err
			} else if !final {
				// Put the incomplete row back in the buffer, to be processed next time.
				c.buf.Write(line)
				break
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
			break
		}
		if err := c.addRow(ctx, line); err != nil {
			return err
		}
	}
	// Only do work if we have a full batch of rows or this is the end.
	if ln := len(c.rows); !final && (ln == 0 || ln < copyBatchRowSize) {
		return nil
	}
	return c.processRows(ctx)
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
		txn = kv.NewTxnWithSteppingEnabled(ctx, p.execCfg.DB, p.execCfg.NodeID.Get())
		txnTs = p.execCfg.Clock.PhysicalTime()
		stmtTs = txnTs
		autoCommit = true
	}
	txnOpt.resetPlanner(ctx, p, txn, txnTs, stmtTs)
	p.autoCommit = autoCommit

	return func(ctx context.Context, err error) error {
		if err == nil {
			// Ensure that the txn is committed if the copyMachine is in charge of
			// committing its transactions and the execution didn't already commit it
			// (through the planner.autoCommit optimization).
			if autoCommit && !txn.IsCommitted() {
				return txn.CommitOrCleanup(ctx)
			}
			return nil
		}
		txn.CleanupOnError(ctx, err)
		return err
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

	c.p.stmt = &Statement{}
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

	var res bufferedCommandResult
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

func (c *copyMachine) addRow(ctx context.Context, line []byte) error {
	var err error
	parts := bytes.Split(line, fieldDelim)
	if len(parts) != len(c.resultColumns) {
		return pgerror.Newf(pgcode.ProtocolViolation,
			"expected %d values, got %d", len(c.resultColumns), len(parts))
	}
	exprs := make(tree.Exprs, len(parts))
	for i, part := range parts {
		s := string(part)
		if s == nullString {
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
			s, err = decodeCopy(s)
			if err != nil {
				return err
			}
		}
		d, err := sqlbase.ParseDatumStringAsWithRawBytes(c.resultColumns[i].Typ, s, c.parsingEvalCtx)
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
func decodeCopy(in string) (string, error) {
	var buf bytes.Buffer
	start := 0
	for i, n := 0, len(in); i < n; i++ {
		if in[i] != '\\' {
			continue
		}
		buf.WriteString(in[start:i])
		i++
		if i >= n {
			return "", pgerror.Newf(pgcode.Syntax,
				"unknown escape sequence: %q", in[i-1:])
		}

		ch := in[i]
		if decodedChar := decodeMap[ch]; decodedChar != 0 {
			buf.WriteByte(decodedChar)
		} else if ch == 'x' {
			// \x can be followed by 1 or 2 hex digits.
			i++
			if i >= n {
				return "", pgerror.Newf(pgcode.Syntax,
					"unknown escape sequence: %q", in[i-2:])
			}
			ch = in[i]
			digit, ok := decodeHexDigit(ch)
			if !ok {
				return "", pgerror.Newf(pgcode.Syntax,
					"unknown escape sequence: %q", in[i-2:i])
			}
			if i+1 < n {
				if v, ok := decodeHexDigit(in[i+1]); ok {
					i++
					digit <<= 4
					digit += v
				}
			}
			buf.WriteByte(digit)
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
			return "", pgerror.Newf(pgcode.Syntax,
				"unknown escape sequence: %q", in[i-1:i+1])
		}
		start = i + 1
	}
	buf.WriteString(in[start:])
	return buf.String(), nil
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
