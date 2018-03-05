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
	loc  *time.Location
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
	// If set, an error will be sent to the client if more rows are produced than
	// this limit.
	limit int

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

	if r.err == nil &&
		r.limit != 0 &&
		r.rowsAffected > r.limit &&
		r.typ == commandComplete &&
		r.stmtType == tree.Rows {

		r.err = errors.Errorf("execute row count limits not supported: %d of %d",
			r.limit, r.rowsAffected)
		r.conn.bufferErr(convertToErrWithPGCode(r.err))
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
func (r *commandResult) AddRow(ctx context.Context, row tree.Datums) error {
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

	r.conn.bufferRow(ctx, row, r.formatCodes, r.loc)
	_ /* flushed */, err := r.conn.maybeFlush(r.pos)
	return err
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

// SetLimit is part of the CommandResult interface.
func (r *commandResult) SetLimit(n int) {
	r.limit = n
}

// ResetStmtType is part of the CommandResult interface.
func (r *commandResult) ResetStmtType(stmt tree.Statement) {
	r.stmtType = stmt.StatementType()
	r.cmdCompleteTag = stmt.StatementTag()
}
