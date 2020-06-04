// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgwire

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

type completionMsgType int

const (
	_ completionMsgType = iota
	commandComplete
	bindComplete
	closeComplete
	parseComplete
	emptyQueryResponse
	readyForQuery
	flush
	// Some commands, like Describe, don't need a completion message.
	noCompletionMsg
)

// commandResult is an implementation of sql.CommandResult that streams a
// commands results over a pgwire network connection.
type commandResult struct {
	// conn is the parent connection of this commandResult.
	conn *conn
	// conv indicates the conversion settings for SQL values.
	conv sessiondata.DataConversionConfig
	// pos identifies the position of the command within the connection.
	pos sql.CmdPos
	// flushBeforeClose contains a list of functions to flush
	// before a command is closed.
	flushBeforeCloseFuncs []func(ctx context.Context) error

	err error
	// errExpected, if set, enforces that an error had been set when Close is
	// called.
	errExpected bool

	typ completionMsgType
	// If typ == commandComplete, this is the tag to be written in the
	// CommandComplete message.
	cmdCompleteTag string

	stmtType     tree.StatementType
	descOpt      sql.RowDescOpt
	rowsAffected int

	// formatCodes describes the encoding of each column of result rows. It is nil
	// for statements not returning rows (or for results for commands other than
	// executing statements). It can also be nil for queries returning rows,
	// meaning that all columns will be encoded in the text format (this is the
	// case for queries executed through the simple protocol). Otherwise, it needs
	// to have an entry for every column.
	formatCodes []pgwirebase.FormatCode

	// types is a map from result column index to its type T, similar to formatCodes
	// (except types must always be set).
	types []*types.T

	// bufferingDisabled is conditionally set during planning of certain
	// statements.
	bufferingDisabled bool

	// released is set when the command result has been released so that its
	// memory can be reused. It is also used to assert against use-after-free
	// errors.
	released bool
}

var _ sql.CommandResult = &commandResult{}

// Close is part of the CommandResult interface.
func (r *commandResult) Close(ctx context.Context, t sql.TransactionStatusIndicator) {
	r.assertNotReleased()
	defer r.release()
	if r.errExpected && r.err == nil {
		panic("expected err to be set on result by Close, but wasn't")
	}

	r.conn.writerState.fi.registerCmd(r.pos)
	if r.err != nil {
		r.conn.bufferErr(ctx, r.err)
		return
	}

	for _, f := range r.flushBeforeCloseFuncs {
		if err := f(ctx); err != nil {
			panic(fmt.Sprintf("unexpected err when closing: %s", err))
		}
	}
	r.flushBeforeCloseFuncs = nil

	// Send a completion message, specific to the type of result.
	switch r.typ {
	case commandComplete:
		tag := cookTag(
			r.cmdCompleteTag, r.conn.writerState.tagBuf[:0], r.stmtType, r.rowsAffected,
		)
		r.conn.bufferCommandComplete(tag)
	case parseComplete:
		r.conn.bufferParseComplete()
	case bindComplete:
		r.conn.bufferBindComplete()
	case closeComplete:
		r.conn.bufferCloseComplete()
	case readyForQuery:
		r.conn.bufferReadyForQuery(byte(t))
		// The error is saved on conn.err.
		_ /* err */ = r.conn.Flush(r.pos)
	case emptyQueryResponse:
		r.conn.bufferEmptyQueryResponse()
	case flush:
		// The error is saved on conn.err.
		_ /* err */ = r.conn.Flush(r.pos)
	case noCompletionMsg:
		// nothing to do
	default:
		panic(fmt.Sprintf("unknown type: %v", r.typ))
	}
}

// Discard is part of the CommandResult interface.
func (r *commandResult) Discard() {
	r.assertNotReleased()
	defer r.release()
}

// Err is part of the CommandResult interface.
func (r *commandResult) Err() error {
	r.assertNotReleased()
	return r.err
}

// SetError is part of the CommandResult interface.
//
// We're not going to write any bytes to the buffer in order to support future
// SetError() calls. The error will only be serialized at Close() time.
func (r *commandResult) SetError(err error) {
	r.assertNotReleased()
	r.err = err
}

// AddRow is part of the CommandResult interface.
func (r *commandResult) AddRow(ctx context.Context, row tree.Datums) error {
	r.assertNotReleased()
	if r.err != nil {
		panic(fmt.Sprintf("can't call AddRow after having set error: %s",
			r.err))
	}
	r.conn.writerState.fi.registerCmd(r.pos)
	if err := r.conn.GetErr(); err != nil {
		return err
	}
	if r.err != nil {
		panic("can't send row after error")
	}
	r.rowsAffected++

	r.conn.bufferRow(ctx, row, r.formatCodes, r.conv, r.types)
	var err error
	if r.bufferingDisabled {
		err = r.conn.Flush(r.pos)
	} else {
		_ /* flushed */, err = r.conn.maybeFlush(r.pos)
	}
	return err
}

// DisableBuffering is part of the CommandResult interface.
func (r *commandResult) DisableBuffering() {
	r.assertNotReleased()
	r.bufferingDisabled = true
}

// AppendParamStatusUpdate is part of the CommandResult interface.
func (r *commandResult) AppendParamStatusUpdate(param string, val string) {
	r.flushBeforeCloseFuncs = append(
		r.flushBeforeCloseFuncs,
		func(ctx context.Context) error { return r.conn.bufferParamStatus(param, val) },
	)
}

// AppendNotice is part of the CommandResult interface.
func (r *commandResult) AppendNotice(noticeErr error) {
	r.flushBeforeCloseFuncs = append(
		r.flushBeforeCloseFuncs,
		func(ctx context.Context) error {
			return r.conn.bufferNotice(ctx, noticeErr)
		},
	)
}

// SetColumns is part of the CommandResult interface.
func (r *commandResult) SetColumns(ctx context.Context, cols sqlbase.ResultColumns) {
	r.assertNotReleased()
	r.conn.writerState.fi.registerCmd(r.pos)
	if r.descOpt == sql.NeedRowDesc {
		_ /* err */ = r.conn.writeRowDescription(ctx, cols, r.formatCodes, &r.conn.writerState.buf)
	}
	r.types = make([]*types.T, len(cols))
	for i, col := range cols {
		r.types[i] = col.Typ
	}
}

// SetInferredTypes is part of the DescribeResult interface.
func (r *commandResult) SetInferredTypes(types []oid.Oid) {
	r.assertNotReleased()
	r.conn.writerState.fi.registerCmd(r.pos)
	r.conn.bufferParamDesc(types)
}

// SetNoDataRowDescription is part of the DescribeResult interface.
func (r *commandResult) SetNoDataRowDescription() {
	r.assertNotReleased()
	r.conn.writerState.fi.registerCmd(r.pos)
	r.conn.bufferNoDataMsg()
}

// SetPrepStmtOutput is part of the DescribeResult interface.
func (r *commandResult) SetPrepStmtOutput(ctx context.Context, cols sqlbase.ResultColumns) {
	r.assertNotReleased()
	r.conn.writerState.fi.registerCmd(r.pos)
	_ /* err */ = r.conn.writeRowDescription(ctx, cols, nil /* formatCodes */, &r.conn.writerState.buf)
}

// SetPortalOutput is part of the DescribeResult interface.
func (r *commandResult) SetPortalOutput(
	ctx context.Context, cols sqlbase.ResultColumns, formatCodes []pgwirebase.FormatCode,
) {
	r.assertNotReleased()
	r.conn.writerState.fi.registerCmd(r.pos)
	_ /* err */ = r.conn.writeRowDescription(ctx, cols, formatCodes, &r.conn.writerState.buf)
}

// IncrementRowsAffected is part of the CommandResult interface.
func (r *commandResult) IncrementRowsAffected(n int) {
	r.assertNotReleased()
	r.rowsAffected += n
}

// RowsAffected is part of the CommandResult interface.
func (r *commandResult) RowsAffected() int {
	r.assertNotReleased()
	return r.rowsAffected
}

// ResetStmtType is part of the CommandResult interface.
func (r *commandResult) ResetStmtType(stmt tree.Statement) {
	r.assertNotReleased()
	r.stmtType = stmt.StatementType()
	r.cmdCompleteTag = stmt.StatementTag()
}

// release frees the commandResult and allows its memory to be reused.
func (r *commandResult) release() {
	*r = commandResult{released: true}
}

// assertNotReleased asserts that the commandResult is not being used after
// being freed by one of the methods in the CommandResultClose interface. The
// assertion can have false negatives, where it fails to detect a use-after-free
// condition, but will never result in a false positive.
func (r *commandResult) assertNotReleased() {
	if r.released {
		panic("commandResult used after being released")
	}
}

func (c *conn) allocCommandResult() *commandResult {
	r := &c.res
	if r.released {
		r.released = false
	} else {
		// In practice, each conn only ever uses a single commandResult at a
		// time, so we could make this panic. However, doing so would entail
		// complicating the ClientComm interface, which doesn't seem worth it.
		r = new(commandResult)
	}
	return r
}

func (c *conn) newCommandResult(
	descOpt sql.RowDescOpt,
	pos sql.CmdPos,
	stmt tree.Statement,
	formatCodes []pgwirebase.FormatCode,
	conv sessiondata.DataConversionConfig,
	limit int,
	portalName string,
	implicitTxn bool,
) sql.CommandResult {
	r := c.allocCommandResult()
	*r = commandResult{
		conn:           c,
		conv:           conv,
		pos:            pos,
		typ:            commandComplete,
		cmdCompleteTag: stmt.StatementTag(),
		stmtType:       stmt.StatementType(),
		descOpt:        descOpt,
		formatCodes:    formatCodes,
	}
	if limit == 0 {
		return r
	}
	telemetry.Inc(sqltelemetry.PortalWithLimitRequestCounter)
	return &limitedCommandResult{
		limit:         limit,
		portalName:    portalName,
		implicitTxn:   implicitTxn,
		commandResult: r,
	}
}

func (c *conn) newMiscResult(pos sql.CmdPos, typ completionMsgType) *commandResult {
	r := c.allocCommandResult()
	*r = commandResult{
		conn: c,
		pos:  pos,
		typ:  typ,
	}
	return r
}

// limitedCommandResult is a commandResult that has a limit, after which calls
// to AddRow will block until the associated client connection asks for more
// rows. It essentially implements the "execute portal with limit" part of the
// Postgres protocol.
//
// This design is known to be flawed. It only supports a specific subset of the
// protocol. We only allow a portal suspension in an explicit transaction where
// the suspended portal is completely exhausted before any other pgwire command
// is executed, otherwise an error is produced. You cannot, for example,
// interleave portal executions (a portal must be executed to completion before
// another can be executed). It also breaks the software layering by adding an
// additional state machine here, instead of teaching the state machine in the
// sql package about portals.
//
// This has been done because refactoring the executor to be able to correctly
// suspend a portal will require a lot of work, and we wanted to move
// forward. The work included is things like auditing all of the defers and
// post-execution stuff (like stats collection) to have it only execute once
// per statement instead of once per portal.
type limitedCommandResult struct {
	*commandResult
	portalName  string
	implicitTxn bool

	seenTuples int
	// If set, an error will be sent to the client if more rows are produced than
	// this limit.
	limit int
}

// AddRow is part of the CommandResult interface.
func (r *limitedCommandResult) AddRow(ctx context.Context, row tree.Datums) error {
	if err := r.commandResult.AddRow(ctx, row); err != nil {
		return err
	}
	r.seenTuples++

	if r.seenTuples == r.limit {
		// If we've seen up to the limit of rows, send a "portal suspended" message
		// and wait for another exec portal message.
		r.conn.bufferPortalSuspended()
		if err := r.conn.Flush(r.pos); err != nil {
			return err
		}
		r.seenTuples = 0

		return r.moreResultsNeeded(ctx)
	}
	if _ /* flushed */, err := r.conn.maybeFlush(r.pos); err != nil {
		return err
	}
	return nil
}

// moreResultsNeeded is a restricted connection handler that waits for more
// requests for rows from the active portal, during the "execute portal" flow
// when a limit has been specified.
func (r *limitedCommandResult) moreResultsNeeded(ctx context.Context) error {
	// In an implicit transaction, a portal suspension is immediately
	// followed by closing the portal.
	if r.implicitTxn {
		r.typ = noCompletionMsg
		return sql.ErrLimitedResultClosed
	}

	// Keep track of the previous CmdPos so we can rewind if needed.
	prevPos := r.conn.stmtBuf.AdvanceOne()
	for {
		cmd, curPos, err := r.conn.stmtBuf.CurCmd()
		if err != nil {
			return err
		}
		switch c := cmd.(type) {
		case sql.DeletePreparedStmt:
			// The client wants to close a portal or statement. We
			// support the case where it is exactly this
			// portal. This is done by closing the portal in
			// the same way implicit transactions do, but also
			// rewinding the stmtBuf to still point to the portal
			// close so that the state machine can do its part of
			// the cleanup. We are in effect peeking to see if the
			// next message is a delete portal.
			if c.Type != pgwirebase.PreparePortal || c.Name != r.portalName {
				telemetry.Inc(sqltelemetry.InterleavedPortalRequestCounter)
				return errors.WithDetail(sql.ErrLimitedResultNotSupported,
					"cannot close a portal while a different one is open")
			}
			r.typ = noCompletionMsg
			// Rewind to before the delete so the AdvanceOne in
			// connExecutor.execCmd ends up back on it.
			r.conn.stmtBuf.Rewind(ctx, prevPos)
			return sql.ErrLimitedResultClosed
		case sql.ExecPortal:
			// The happy case: the client wants more rows from the portal.
			if c.Name != r.portalName {
				telemetry.Inc(sqltelemetry.InterleavedPortalRequestCounter)
				return errors.WithDetail(sql.ErrLimitedResultNotSupported,
					"cannot execute a portal while a different one is open")
			}
			r.limit = c.Limit
			// In order to get the correct command tag, we need to reset the seen rows.
			r.rowsAffected = 0
			return nil
		case sql.Sync:
			// The client wants to see a ready for query message
			// back. Send it then run the for loop again.
			r.conn.stmtBuf.AdvanceOne()
			// We can hard code InTxnBlock here because we don't
			// support implicit transactions, so we know we're in
			// a transaction.
			r.conn.bufferReadyForQuery(byte(sql.InTxnBlock))
			if err := r.conn.Flush(r.pos); err != nil {
				return err
			}
		default:
			// We got some other message, but we only support executing to completion.
			telemetry.Inc(sqltelemetry.InterleavedPortalRequestCounter)
			return errors.WithSafeDetails(sql.ErrLimitedResultNotSupported,
				"cannot perform operation %T while a different portal is open",
				errors.Safe(c))
		}
		prevPos = curPos
	}
}
