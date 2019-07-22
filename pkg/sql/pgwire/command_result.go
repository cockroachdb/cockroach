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

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
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

	// oids is a map from result column index to its Oid, similar to formatCodes
	// (except oids must always be set).
	oids []oid.Oid

	// bufferingDisabled is conditionally set during planning of certain
	// statements.
	bufferingDisabled bool

	// released is set when the command result has been released so that its
	// memory can be reused. It is also used to assert against use-after-free
	// errors.
	released bool
}

// Close is part of the CommandResult interface.
func (r *commandResult) Close(t sql.TransactionStatusIndicator) {
	r.assertNotReleased()
	defer r.release()
	if r.errExpected && r.err == nil {
		panic("expected err to be set on result by Close, but wasn't")
	}

	r.conn.writerState.fi.registerCmd(r.pos)
	if r.err != nil {
		r.conn.bufferErr(r.err)
		return
	}

	if r.err == nil &&
		r.limit != 0 &&
		r.rowsAffected > r.limit &&
		r.typ == commandComplete &&
		r.stmtType == tree.Rows {

		r.err = unimplemented.NewWithIssuef(4035,
			"execute row count limits not supported: %d of %d",
			r.limit, r.rowsAffected)
		r.conn.bufferErr(r.err)
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
	r.assertNotReleased()
	defer r.release()
	if r.err != nil {
		panic(fmt.Sprintf("can't overwrite err: %s with err: %s", r.err, err))
	}

	r.conn.writerState.fi.registerCmd(r.pos)
	r.conn.bufferErr(err)
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

	r.conn.bufferRow(ctx, row, r.formatCodes, r.conv, r.oids)
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

// SetColumns is part of the CommandResult interface.
func (r *commandResult) SetColumns(ctx context.Context, cols sqlbase.ResultColumns) {
	r.assertNotReleased()
	r.conn.writerState.fi.registerCmd(r.pos)
	if r.descOpt == sql.NeedRowDesc {
		_ /* err */ = r.conn.writeRowDescription(ctx, cols, r.formatCodes, &r.conn.writerState.buf)
	}
	r.oids = make([]oid.Oid, len(cols))
	for i, col := range cols {
		r.oids[i] = mapResultOid(col.Typ.Oid())
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

// SetLimit is part of the CommandResult interface.
func (r *commandResult) SetLimit(n int) {
	r.assertNotReleased()
	r.limit = n
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
