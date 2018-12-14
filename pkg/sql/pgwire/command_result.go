// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package pgwire

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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

// limitedCommandResult is a commandResult that has a limit, after which calls
// to AddRow will block until the associated client connection asks for more
// rows. It essentially implements the "execute portal with limit" part of the
// Postgres protocol.
type limitedCommandResult struct {
	commandResult

	seenTuples int
	// If set, an error will be sent to the client if more rows are produced than
	// this limit.
	limit int
}

// AddRow is part of the CommandResult interface.
func (r *limitedCommandResult) AddRow(ctx context.Context, row tree.Datums) (cont bool, _ error) {
	if _, err := r.commandResult.AddRow(ctx, row); err != nil {
		return false, err
	}
	r.seenTuples++

	if r.seenTuples == r.limit {
		// If we've seen up to the limit of rows, send a "portal suspended" message
		// and wait for further instructions.
		r.conn.bufferPortalSuspended()
		if err := r.conn.Flush(r.pos); err != nil {
			return false, err
		}
		r.seenTuples = 0

		return r.moreResultsNeeded()

	}
	if _ /* flushed */, err := r.conn.maybeFlush(r.pos); err != nil {
		return false, err
	}
	return true, nil
}

// moreResultsNeeded is a restricted connection handler that waits for more
// requests for rows from the active portal, during the "execute portal" flow
// when a limit has been specified.
func (r *limitedCommandResult) moreResultsNeeded() (cont bool, err error) {
	r.conn.stmtBuf.AdvanceOne()
	for {
		cmd, _, err := r.conn.stmtBuf.CurCmd()
		if err != nil {
			return false, err
		}
		switch c := cmd.(type) {
		case sql.ExecPortal:
			// The client wants more rows from the portal.
			// TODO(jordan,andrei): Check the portal name - what to do if it doesn't match?
			// Do we need to handle this? c.TimeReceived
			r.limit = c.Limit
			return true, nil
		case sql.Sync:
			// The client wants to see a ready for query message back.
			// TODO(jordan,andrei): we should be doing something different
			// (returning to the outer loop) in the case that we are only in an
			// implicit transaction.
			r.conn.stmtBuf.AdvanceOne()
			r.conn.bufferReadyForQuery(byte(sql.InTxnBlock))
			if err := r.conn.Flush(r.pos); err != nil {
				return false, err
			}
		default:
			return false, nil
		}
	}
}

// commandResult is an implementation of sql.CommandResult that streams a
// commands results over a pgwire network connection.
type commandResult struct {
	// conn is the parent connection of this commandResult.
	conn *conn
	// conv indicates the conversion settings for SQL values.
	conv sessiondata.DataConversionConfig
	// pos identifies the position of the command within the connection.
	pos sql.CmdPos

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
}

func (c *conn) makeCommandResult(
	descOpt sql.RowDescOpt,
	pos sql.CmdPos,
	stmt tree.Statement,
	formatCodes []pgwirebase.FormatCode,
	conv sessiondata.DataConversionConfig,
	limit int,
) sql.CommandResult {
	res := commandResult{
		conn:           c,
		pos:            pos,
		descOpt:        descOpt,
		stmtType:       stmt.StatementType(),
		formatCodes:    formatCodes,
		typ:            commandComplete,
		cmdCompleteTag: stmt.StatementTag(),
		conv:           conv,
	}
	if limit == 0 {
		return &res
	}
	return &limitedCommandResult{
		limit:         limit,
		commandResult: res,
	}
}

func (c *conn) makeMiscResult(pos sql.CmdPos, typ completionMsgType) commandResult {
	return commandResult{
		conn: c,
		pos:  pos,
		typ:  typ,
	}
}

// Close is part of the CommandResult interface.
func (r *commandResult) Close(t sql.TransactionStatusIndicator) {
	if r.errExpected && r.err == nil {
		panic("expected err to be set on result by Close, but wasn't")
	}

	r.conn.writerState.fi.registerCmd(r.pos)
	if r.err != nil {
		// TODO(andrei): I'm not sure this is the best place to do error conversion.
		r.conn.bufferErr(convertToErrWithPGCode(r.err))
		return
	}

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

// CloseWithErr is part of the CommandResult interface.
func (r *commandResult) CloseWithErr(err error) {
	if r.err != nil {
		panic(fmt.Sprintf("can't overwrite err: %s with err: %s", r.err, err))
	}
	r.conn.writerState.fi.registerCmd(r.pos)

	r.err = err
	// TODO(andrei): I'm not sure this is the best place to do error conversion.
	r.conn.bufferErr(convertToErrWithPGCode(err))
}

// Discard is part of the CommandResult interface.
func (r *commandResult) Discard() {
}

// Err is part of the CommandResult interface.
func (r *commandResult) Err() error {
	return r.err
}

// SetError is part of the CommandResult interface.
//
// We're not going to write any bytes to the buffer in order to support future
// OverwriteError() calls. The error will only be serialized at Close() time.
func (r *commandResult) SetError(err error) {
	if r.err != nil {
		panic(fmt.Sprintf("can't overwrite err: %s with err: %s", r.err, err))
	}
	r.err = err
}

// OverwriteError is part of the CommandResult interface.
func (r *commandResult) OverwriteError(err error) {
	r.err = err
}

// AddRow is part of the CommandResult interface.
func (r *commandResult) AddRow(ctx context.Context, row tree.Datums) (cont bool, _ error) {
	if r.err != nil {
		panic(fmt.Sprintf("can't call AddRow after having set error: %s",
			r.err))
	}
	r.conn.writerState.fi.registerCmd(r.pos)
	if err := r.conn.GetErr(); err != nil {
		return false, err
	}
	if r.err != nil {
		panic("can't send row after error")
	}
	r.rowsAffected++

	r.conn.bufferRow(ctx, row, r.formatCodes, r.conv)
	_ /* flushed */, err := r.conn.maybeFlush(r.pos)
	return true, err
}

// SetColumns is part of the CommandResult interface.
func (r *commandResult) SetColumns(ctx context.Context, cols sqlbase.ResultColumns) {
	r.conn.writerState.fi.registerCmd(r.pos)
	if r.descOpt == sql.NeedRowDesc {
		_ /* err */ = r.conn.writeRowDescription(ctx, cols, r.formatCodes, &r.conn.writerState.buf)
	}
}

// SetInTypes is part of the DescribeResult interface.
func (r *commandResult) SetInTypes(types []oid.Oid) {
	r.conn.writerState.fi.registerCmd(r.pos)
	r.conn.bufferParamDesc(types)
}

// SetNoDataRowDescription is part of the DescribeResult interface.
func (r *commandResult) SetNoDataRowDescription() {
	r.conn.writerState.fi.registerCmd(r.pos)
	r.conn.bufferNoDataMsg()
}

// SetPrepStmtOutput is part of the DescribeResult interface.
func (r *commandResult) SetPrepStmtOutput(ctx context.Context, cols sqlbase.ResultColumns) {
	r.conn.writerState.fi.registerCmd(r.pos)
	_ /* err */ = r.conn.writeRowDescription(ctx, cols, nil /* formatCodes */, &r.conn.writerState.buf)
}

// SetPortalOutput is part of the DescribeResult interface.
func (r *commandResult) SetPortalOutput(
	ctx context.Context, cols sqlbase.ResultColumns, formatCodes []pgwirebase.FormatCode,
) {
	r.conn.writerState.fi.registerCmd(r.pos)
	_ /* err */ = r.conn.writeRowDescription(ctx, cols, formatCodes, &r.conn.writerState.buf)
}

// IncrementRowsAffected is part of the CommandResult interface.
func (r *commandResult) IncrementRowsAffected(n int) {
	r.rowsAffected += n
}

// RowsAffected is part of the CommandResult interface.
func (r *commandResult) RowsAffected() int {
	return r.rowsAffected
}

// ResetStmtType is part of the CommandResult interface.
func (r *commandResult) ResetStmtType(stmt tree.Statement) {
	r.stmtType = stmt.StatementType()
	r.cmdCompleteTag = stmt.StatementTag()
}
