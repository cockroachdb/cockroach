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
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
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

	// pos identifies the position of the command within the connection.
	pos sql.CmdPos

	typ completionMsgType

	descOpt sql.RowDescOpt

	// If typ == commandComplete, this is the tag to be written in the
	// CommandComplete message.
	cmdCompleteTag string

	err error
	// errExpected, if set, enforces that an error had been set when Close is
	// called.
	errExpected bool

	stmtType     tree.StatementType
	rowsAffected int

	loc *time.Location

	// formatCodes describes the encoding of each column of result rows. It is nil
	// for statements not returning rows (or for results for commands other than
	// executing statements). It can also be nil for queries returning rows,
	// meaning that all columns will be encoded in the text format (this is the
	// case for queries executed through the simple protocol). Otherwise, it needs
	// to have an entry for every column.
	formatCodes []pgwirebase.FormatCode

	// If set, an error will be sent to the client if more rows are produced than
	// this limit.
	limit int

	// finishedCallback, if set, will be called on Close/CloseWithErr/Discard.
	finishedCallback func()
}

func (c *conn) makeCommandResult(
	descOpt sql.RowDescOpt,
	pos sql.CmdPos,
	stmt tree.Statement,
	formatCodes []pgwirebase.FormatCode,
	loc *time.Location,
) commandResult {
	return commandResult{
		conn:           c,
		pos:            pos,
		descOpt:        descOpt,
		stmtType:       stmt.StatementType(),
		formatCodes:    formatCodes,
		loc:            loc,
		typ:            commandComplete,
		cmdCompleteTag: stmt.StatementTag(),
	}
}

func (c *conn) makeMiscResult(
	pos sql.CmdPos, typ completionMsgType,
) commandResult {
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

	if r.limit != 0 &&
		r.rowsAffected > r.limit &&
		r.typ == commandComplete &&
		r.stmtType == tree.Rows {

		err := errors.Errorf("execute row count limits not supported: %d of %d",
			r.limit, r.rowsAffected)
		r.conn.bufferErr(convertToErrWithPGCode(err))
		goto End
	}

	if r.err != nil {
		// If there was an error, we have already sent it. We're not sending another
		// code.
		goto End
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
		panic(fmt.Sprintf("unknown type: %s", r.typ))
	}

End:
	if f := r.finishedCallback; f != nil {
		f()
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
	if f := r.finishedCallback; f != nil {
		f()
	}
}

// Discard is part of the CommandResult interface.
func (r *commandResult) Discard() {
	if f := r.finishedCallback; f != nil {
		f()
	}
}

// Err is part of the CommandResult interface.
func (r *commandResult) Err() error {
	return r.err
}

// SetError is part of the CommandResult interface.
func (r *commandResult) SetError(err error) {
	r.conn.writerState.fi.registerCmd(r.pos)
	r.err = err
	// TODO(andrei): I'm not sure this is the best place to do error conversion.
	r.conn.bufferErr(convertToErrWithPGCode(err))
}

// AddRow is part of the CommandResult interface.
func (r *commandResult) AddRow(ctx context.Context, row tree.Datums) error {
	r.conn.writerState.fi.registerCmd(r.pos)
	if err := r.conn.GetErr(); err != nil {
		return err
	}
	if r.err != nil {
		panic("can't send row after error")
	}
	r.rowsAffected++

	r.conn.bufferRow(ctx, row, r.formatCodes, r.loc)
	_ /* flushed */, err := r.conn.maybeFlush(r.pos)
	return err
}

// SetColumns is part of the CommandResult interface.
func (r *commandResult) SetColumns(ctx context.Context, cols sqlbase.ResultColumns) {
	r.conn.writerState.fi.registerCmd(r.pos)
	if r.descOpt == sql.NeedRowDesc {
		r.conn.writeRowDescription(ctx, cols, r.formatCodes, &r.conn.writerState.buf)
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
	r.conn.writeRowDescription(ctx, cols, nil /* formatCodes */, &r.conn.writerState.buf)
}

// SetPortalOutput is part of the DescribeResult interface.
func (r *commandResult) SetPortalOutput(
	ctx context.Context, cols sqlbase.ResultColumns, formatCodes []pgwirebase.FormatCode,
) {
	r.conn.writerState.fi.registerCmd(r.pos)
	r.conn.writeRowDescription(ctx, cols, formatCodes, &r.conn.writerState.buf)
}

// IncrementRowsAffected is part of the CommandResult interface.
func (r *commandResult) IncrementRowsAffected(n int) {
	r.rowsAffected += n
}

// RowsAffected is part of the CommandResult interface.
func (r *commandResult) RowsAffected() int {
	return r.rowsAffected
}

// SetLimit is part of the CommandResult interface.
func (r *commandResult) SetLimit(n int) {
	r.limit = n
}

// ResetStmtType is part of the CommandResult interface.
func (r *commandResult) ResetStmtType(stmt tree.Statement) {
	r.stmtType = stmt.StatementType()
	r.cmdCompleteTag = stmt.StatementTag()
}

// SetFinishedCallback is part of the CommandResult interface.
func (r *commandResult) SetFinishedCallback(callback func()) {
	r.finishedCallback = callback
}
