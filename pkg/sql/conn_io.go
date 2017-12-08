// Copyright 2017 The Cockroach Authors.
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

package sql

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/lib/pq/oid"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// This file contains utils and interfaces used by a connExecutor to communicate
// with a SQL client. There's StmtBuf used for input and ClientComm used for
// output.

// CmdPos represents the index of a command relative to the start of a
// connection. The first command received on a connection has position 0.
type CmdPos int64

type TransactionStatusIndicator byte

const (
	IdleTxnBlock     TransactionStatusIndicator = 'I'
	InTxnBlock       TransactionStatusIndicator = 'T'
	InFailedTxnBlock TransactionStatusIndicator = 'E'
)

// StmtBuf maintains a list of commands that a SQL client has sent for execution
// over a network connection. The commands are SQL queries to be executed,
// statements to be prepared, etc. At any point in time the buffer contains
// outstanding commands that have yet to be executed, and it can also contain
// some history of commands that we might want to retry - in the case of a
// retriable error, we'd like to retry all the commands pertaining to the
// current SQL transaction.
//
// The buffer is supposed to be used by one reader and one writer. The writer
// adds commands to the buffer using Push(). The reader reads one command at a
// time using curCmd(). The consumer is then supposed to create command results
// (the buffer is not involved in this).
// The buffer internally maintains a cursor representing the reader's position.
// The reader has to manually move the cursor using advanceOne(),
// seekToNextBatch() and rewind(). To support querying the buffer to ask if the
// consumer is idle (through the ConsumerIdle() method), the contract is that
// the consumer only advances the cursor after it is done producing results for
// the previous command.
// In practice, the writer is a module responsible for communicating with a SQL
// client (i.e. pgwire.conn) and the reader is a connExecutor.
//
// The StmtBuf supports grouping commands into "batches" delimited by sync
// commands. A reader can then at any time chose to skip over commands from the
// current batch. This is used to implement Postgres error semantics: when an
// error happens during processing of a command, some future commands might need
// to be skipped. Batches correspond either to multiple queries received in a
// single query string (when the SQL client sends a semicolon-separated list of
// queries as part of the "simple" protocol), or to different commands pipelined
// by the cliend, separated from "sync" messages.
//
// push() can be called concurrently with curCmd().
//
// The connExecutor will use the buffer to maintain a window around the
// command it is currently executing. It will maintain enough history for
// executing commands again in case of an automatic retry. The connExecutor is
// in charge of trimming completed commands from the buffer when it's done with
// them.
type StmtBuf struct {
	mu struct {
		syncutil.Mutex

		// closed, if set, means that the writer has closed the buffer. See Close().
		closed bool

		// cond is signaled when new commands are pushed.
		cond *sync.Cond

		// readerBlocked is set while the reader is blocked on waiting for a command
		// to be pushed into the buffer.
		readerBlocked bool

		// data contains the elements of the buffer.
		data []Command

		// startPos indicates the index of the first command currently in data
		// relative to the start of the connection.
		startPos CmdPos
		// curPos is the current position of the cursor going through the commands.
		// At any time, curPos indicates the position of the command to be returned
		// by curCmd().
		curPos CmdPos
		// lastPos indicates the position of the last command that was pushed into
		// the buffer.
		lastPos CmdPos
	}
}

// Command is an interface implemented by all commands pushed by pgwire into the
// buffer.
type Command interface {
	command()
}

// ExecStmt is the command for running a query sent through the "simple" pgwire
// protocol.
type ExecStmt struct {
	// Stmt can be nil, in which case a "empty query response" message should be
	// produced.
	Stmt tree.Statement
	// ParseStart/ParseEnd are the timing info for parsing of the query. Used for
	// stats reporting.
	ParseStart time.Time
	ParseEnd   time.Time
}

// command implements the Command interface.
func (ExecStmt) command() {}

var _ Command = ExecStmt{}

// SendError is a command that, upon execution, send a specific error to the
// client. This is used by pgwire to schedule errors to be sent at an
// appropriate time.
type SendError struct {
	// Err is a *pgerror.Error.
	Err error
}

// command implements the Command interface.
func (SendError) command() {}

var _ Command = SendError{}

// Sync is a command that serves two purposes:
// 1) It marks the end of one batch of commands and the beginning of the next.
// stmtBuf.seekToNextBatch will seek to this marker.
// 2) It generates a ReadyForQuery protocol message.
//
// A Sync command is generated for both the simple and the extended pgwire
// protocol variants. So, it doesn't strictly correspond to a pgwire sync
// message - those are not sent in the simple protocol. We synthesize Sync
// commands though because their handling matches the simple protocol too.
type Sync struct{}

// command implements the Command interface.
func (Sync) command() {}

func (Sync) String() string {
	return "Sync"
}

var _ Command = Sync{}

// PrepareStmt is the command for creating a prepared statement.
type PrepareStmt struct {
	Name string
	// Stmt can be nil, in which case executing it should produce an "empty query
	// response" message.
	Stmt      tree.Statement
	TypeHints tree.PlaceholderTypes
	// RawTypeHints is the representation of type hints exactly as specified by
	// the client.
	RawTypeHints []oid.Oid
	ParseStart   time.Time
	ParseEnd     time.Time
}

// command implements the Command interface.
func (PrepareStmt) command() {}

var _ Command = PrepareStmt{}

// DescribeStmt is the Command for producing info about a prepared statement or
// portal.
type DescribeStmt struct {
	Name string
	Type pgwirebase.PrepareType
}

// command implements the Command interface.
func (DescribeStmt) command() {}

var _ Command = DescribeStmt{}

// DeletePreparedStmt is the Command for freeing a prepared statement.
type DeletePreparedStmt struct {
	Name string
	Type pgwirebase.PrepareType
}

// command implements the Command interface.
func (DeletePreparedStmt) command() {}

var _ Command = DeletePreparedStmt{}

// BindStmt is the Command for creating a portal from a prepared statement.
type BindStmt struct {
	PreparedStatementName string
	PortalName            string
	// OutFormats contains the requested formats for the output columns.
	OutFormats []pgwirebase.FormatCode
	// Args are the arguments for the prepared statement.
	// They are passed in without decoding because decoding requires type
	// inference to have been performed.
	//
	// A nil element means a tree.DNull argument.
	Args [][]byte
	// ArgFormatCodes are the codes to be used to deserialize the Args.
	ArgFormatCodes []pgwirebase.FormatCode
}

// command implements the Command interface.
func (BindStmt) command() {}

var _ Command = BindStmt{}

// ExecPortal is the Command for executing a portal.
type ExecPortal struct {
	Name string
	// limit is a feature of pgwire that we don't really support. We accept it and
	// don't complain as long as the statement produces fewer results than this.
	Limit int
}

// command implements the Command interface.
func (ExecPortal) command() {}

var _ Command = ExecPortal{}

// CopyIn is the command for execution of the Copy-in pgwire subprotocol.
type CopyIn struct {
	Stmt *tree.CopyFrom
	// Conn is the network connection. Execution of the CopyFrom statement takes
	// control of the connection.
	Conn pgwirebase.Conn
	// CopyDone is decremented once execution finishes, signaling that control of
	// the connection is being handed back to the network routine.
	CopyDone *sync.WaitGroup
}

// command implements the Command interface.
func (CopyIn) command() {}

var _ Command = CopyIn{}

// !!!
// DrainRequest commands don't produce results.
type DrainRequest struct{}

// command implements the Command interface.
func (DrainRequest) command() {}

var _ Command = DrainRequest{}

// NewStmtBuf creates a StmtBuf.
func NewStmtBuf() *StmtBuf {
	var buf StmtBuf
	buf.mu.lastPos = -1
	buf.mu.cond = sync.NewCond(&buf.mu.Mutex)
	return &buf
}

// Close marks the buffer as closed. Once Close() is called, no further push()es
// are allowed. If a reader is blocked on a curCmd() call, it is unblocked with
// io.EOF. Any further curCmd() call also returns io.EOF (even if some
// commands were already available in the buffer before the Close()).
//
// Close() is idempotent.
func (buf *StmtBuf) Close() {
	buf.mu.Lock()
	buf.mu.closed = true
	buf.mu.cond.Signal()
	buf.mu.Unlock()
}

// Push adds a Command to the end of the buffer. If a curCmd() call was blocked
// waiting for this command to arrive, it will be woken up.
//
// An error is returned if the buffer has been closed.
func (buf *StmtBuf) Push(ctx context.Context, cmd Command) error {
	buf.mu.Lock()
	defer buf.mu.Unlock()
	if buf.mu.closed {
		return fmt.Errorf("buffer is closed")
	}
	buf.mu.data = append(buf.mu.data, cmd)
	buf.mu.lastPos++

	buf.mu.cond.Signal()
	return nil
}

// curCmd returns the Command currently indicated by the cursor. Besides the
// Command itself, the command's position is also returned; the position can be
// used to later rewind() to this Command.
//
// If the cursor is positioned over an empty slot, the call blocks until the
// next Command is pushed into the buffer.
//
// If the buffer has previously been Close()d, or is closed while this is
// blocked, io.EOF is returned.
func (buf *StmtBuf) curCmd(ctx context.Context) (Command, CmdPos, error) {
	buf.mu.Lock()
	defer buf.mu.Unlock()
	for {
		if buf.mu.closed {
			return nil, 0, io.EOF
		}
		curPos := buf.mu.curPos
		cmdIdx, err := buf.translatePosLocked(curPos)
		if err != nil {
			return nil, 0, err
		}
		if cmdIdx < len(buf.mu.data) {
			return buf.mu.data[cmdIdx], curPos, nil
		}
		if cmdIdx != len(buf.mu.data) {
			return nil, 0, errors.Errorf(
				"can only wait for next command; corrupt cursor: %d", curPos)
		}
		// Wait for the next Command to arrive to the buffer.
		buf.mu.readerBlocked = true
		buf.mu.cond.Wait()
		buf.mu.readerBlocked = false
	}
}

// ConsumerIdle returns true if the consumer is currently blocked waiting for a
// command to be pushed into the buffer. See top comments about contract.
func (buf *StmtBuf) ConsumerIdle() bool {
	buf.mu.Lock()
	defer buf.mu.Unlock()
	return buf.mu.readerBlocked
}

// translatePosLocked translates an absolute position of a command (counting
// from the connection start) to the index of the respective command in the
// buffer (so, it returns an index relative to the start of the buffer).
//
// Attempting to translate a position that's below buf.startPos returns an
// error.
func (buf *StmtBuf) translatePosLocked(pos CmdPos) (int, error) {
	if pos < buf.mu.startPos {
		return 0, errors.Errorf(
			"position %d no longer in buffer (buffer starting at %d)",
			pos, buf.mu.startPos)
	}
	return int(pos - buf.mu.startPos), nil
}

// ltrim iterates over the buffer forward and removes all commands up to
// (not including) the command at pos.
//
// It's illegal to ltrim to a position higher than the current cursor.
func (buf *StmtBuf) ltrim(ctx context.Context, pos CmdPos) {
	buf.mu.Lock()
	defer buf.mu.Unlock()
	if pos < buf.mu.startPos {
		log.Fatalf(ctx, "invalid ltrim position: %d. buf starting at: %d",
			pos, buf.mu.startPos)
	}
	if buf.mu.curPos < pos {
		log.Fatalf(ctx, "invalid ltrim position: %d when cursor is: %d",
			pos, buf.mu.curPos)
	}
	// Remove commands one by one.
	for {
		if buf.mu.startPos == pos {
			break
		}
		buf.mu.data = buf.mu.data[1:]
		buf.mu.startPos++
	}
}

// advanceOne advances the cursor one Command over. The command over which the
// cursor will be positioned when this returns may not be in the buffer yet.
func (buf *StmtBuf) advanceOne(ctx context.Context) {
	buf.mu.Lock()
	buf.mu.curPos++
	buf.mu.Unlock()
}

// seekToNextBatch moves the cursor position to the start of the next batch of
// commands, skipping past remaining commands from the current batch (if any).
// Batches are delimited by Sync commands. Sync is considered to be the first
// command in a batch, so once this returns, the cursor will be positioned over
// a Sync command. If the cursor is positioned on a Sync when this is called,
// that Sync will be skipped.
//
// This method blocks until a Sync command is pushed to the buffer.
//
// It is an error to start seeking when the cursor is positioned on an empty
// slot.
func (buf *StmtBuf) seekToNextBatch(ctx context.Context) error {
	buf.mu.Lock()
	curPos := buf.mu.curPos
	cmdIdx, err := buf.translatePosLocked(curPos)
	if err != nil {
		buf.mu.Unlock()
		return err
	}
	if cmdIdx == len(buf.mu.data) {
		buf.mu.Unlock()
		return errors.Errorf("invalid seek start point")
	}
	buf.mu.Unlock()

	var foundSync bool
	for !foundSync {
		buf.advanceOne(ctx)
		_, pos, err := buf.curCmd(ctx)
		if err != nil {
			return err
		}
		buf.mu.Lock()
		cmdIdx, err := buf.translatePosLocked(pos)
		if err != nil {
			buf.mu.Unlock()
			return err
		}

		if _, ok := buf.mu.data[cmdIdx].(Sync); ok {
			foundSync = true
		}

		buf.mu.Unlock()
	}
	return nil
}

// rewind resets the buffer's position to pos.
func (buf *StmtBuf) rewind(ctx context.Context, pos CmdPos) {
	buf.mu.Lock()
	defer buf.mu.Unlock()
	if pos < buf.mu.startPos {
		log.Fatalf(ctx, "attempting to rewind below buffer start")
	}
	buf.mu.curPos = pos
}

// RowDescOpt specifies whether a result needs a row description message.
type RowDescOpt bool

const (
	// NeedRowDesc specifies that a row description message is needed.
	NeedRowDesc RowDescOpt = false
	// DontNeedRowDesc specifies that a row description message is not needed.
	DontNeedRowDesc RowDescOpt = true
)

// ClientComm is the interface used by the connExecutor for creating results to
// be communicated to client and for exerting some control over this
// communication.
//
// ClientComm is implemented by the pgwire connection.
type ClientComm interface {
	// createStatementResult creates a StatementResult for stmt.
	//
	// descOpt specifies if result needs to inform the client about row schema. If
	// it doesn't, a SetColumns call becomes a no-op.
	//
	// pos is the stmt's position within the connection and is used to enforce
	// that results are created in order and also to discard results through
	// ClientLock.rtrim(pos).
	//
	// formatCodes describe how each column in the result rows is to be encoded.
	// It should be nil if statement type != Rows. Otherwise, it can be nil, in
	// which case every column will be encoded using the text encoding, otherwise
	// it needs to contain a value for every column.
	CreateStatementResult(
		stmt tree.Statement,
		descOpt RowDescOpt,
		pos CmdPos,
		formatCodes []pgwirebase.FormatCode,
		loc *time.Location,
	) CommandResult
	CreateParseResult(pos CmdPos) ParseResult
	CreateDescribeResult(pos CmdPos) DescribeResult
	CreateBindResult(pos CmdPos) BindResult
	CreateDeleteResult(pos CmdPos) DeleteResult
	CreateSyncResult(pos CmdPos) SyncResult
	// CreateErrorResult creates a result on which only errors can be communicated
	// to the client.
	CreateErrorResult(pos CmdPos) ErrorResult
	// CreateEmptyQueryResult creates a result for an emptry-string query.
	CreateEmptyQueryResult(pos CmdPos) EmptyQueryResult
	// CreateCopyInResult creates a result for a Copy-in command.
	CreateCopyInResult(pos CmdPos) CopyInResult

	// lockCommunication ensures that no further results are delivered to the
	// client. The returned ClientLock can be queried to see what results have
	// been already delivered to the client and to discard results that haven't
	// been delivered.
	//
	// ClientLock.Close() needs to be called on the returned lock once
	// communication can be unlocked (i.e. results can be delivered to the client
	// again).
	LockCommunication() ClientLock

	// !!!
	// // FlushResults tells the implementation that all the results produced so far
	// // can be delivered to the SQL client. In other words, this is promising that
	// // the corresponding StmtBuf will not be rewind()ed to positions <= the last
	// // position passed to createStatementResult().
	// // FlushResults() is generally called once a transaction ends, as the
	// // connExecutor no longer needs to be able to rewind past the txn's last
	// // statement.
	// //
	// // An error is returned if the client connection is broken. In this case, no
	// // further calls can be made on this ClientComm. The caller should interrupt
	// // any statements running on the respective connection.
	// //
	// // The connExecutor's expectation is that, after FlushResults() is called,
	// // results that are part of future StatementResult's will be buffered (in the
	// // now empty buffer*) by the implementer until one of the following happens:
	// // - the buffer overflows
	// // - FlushResults() is called again
	// // - the last StatementResult is Close()d and there are no more statements in
	// // 	 StmtBuf at the moment.
	// // If the implementer respects this, then it will be guaranteed that, as long
	// // as a transaction prefix's results fit in said buffer, the connExecutor will
	// // be able to automatically retry the prefix in case of retriable errors so
	// // that the client doesn't need to worry about them. As an important special
	// // case, this means that implicit transactions (i.e. statements executed
	// // outside of a transaction) are always automatically retried as long as their
	// // results fit in the implementer's buffer.
	// //
	// // TODO(andrei): In the future we might want to add a time component to this
	// // policy (or make it user configurable), and restrict the guarantees about
	// // automatic retries to queries that are fast enough.
	// //
	// // (*) The implication is that, if the implementer wishes to deliver the
	// // contents of the buffer to the client concurrently with accepting new
	// // results after the FlushResults() call, it needs to behave as if the
	// // buffer's capacity was just expanded: the size of the buffer's contents at
	// // the moment of the FlushResults() call cannot impact the capacity available
	// // for future results.
	// FlushResults() error

	// !!!
	// // TerminateConn is used by the connExecutor to abruptly terminate the client
	// // connection in case of unexpected processing errors. The implementer should
	// // attempt to deliver this error to the client and close the network
	// // connection.
	// TerminateConn(err error)
}

// CommandResult represents the result of a statement. It which needs to be
// ultimately delivered to the client. The pgwire module implements this.
type CommandResult interface {
	RestrictedCommandResult

	// SetLimit is used when executing a portal to set a limit on the number of
	// rows to be returned. We don't currently properly support this feature of
	// the Postgres protocol; instead, we'll return an error if the number of rows
	// produced is larger than this limit.
	SetLimit(n int)

	// Close - see ResultBase.
	Close(TransactionStatusIndicator)

	// CloseWithErr - see ResultBase.
	CloseWithErr(error)

	// Discard - see ResultBase.
	Discard()
}

// RestrictedCommandResult is a subset of CommandResult meant to make it clear
// that its clients don't close the CommandResult.
type RestrictedCommandResult interface {
	// SetColumns informs the client about the schema of the result. The columns
	// can be nil.
	//
	// This needs to be called (once) before AddRow.
	SetColumns(context.Context, sqlbase.ResultColumns)

	// ResetStmtType allows a client to change the statement type of the current
	// result, from the original one set when the result was created trough
	// ClientComm.createStatementResult.
	ResetStmtType(stmt tree.Statement)

	// AddRow accumulates a result row.
	//
	// The implementation cannot hold on to the row slice; it needs to make a
	// shallow copy if it needs to.
	AddRow(ctx context.Context, row tree.Datums) error

	// SetFinishedCallback takes a callback that will be called once
	// Close/CloseWithErr/Discard is called. This is used by SQL to unregister
	// queries from a registry of running queries.
	//
	// This can be called multiple times, each time overwriting the previous
	// value. The callback can be nil, in which case nothing will be called.
	SetFinishedCallback(callback func())

	// SetError accumulates an execution error that needs to be reported to the
	// client. No further calls other that Err() and Close()/Discard() are allowed
	// - in particular, CloseWithErr() is not allowed.
	SetError(error)

	// Err returns the error previously set with SetError(), if any.
	Err() error

	// IncrementRowsAffected increments a counter by n. This is used for all
	// result types other than tree.Rows.
	IncrementRowsAffected(n int)

	// RowsAffected returns either the number of times AddRow was called, or the
	// sum of all n passed into IncrementRowsAffected.
	RowsAffected() int
}

// DescribeResult represent the result of a Describe command (for either
// describing a prepared statement or a portal).
type DescribeResult interface {
	ResultBase

	// SetInTypes tells the client about the inferred placeholder types.
	SetInTypes([]oid.Oid)
	// SetNoDataDescription is used to tell the client that the prepared statement
	// or portal produces no rows.
	SetNoDataRowDescription()
	// SetPrepStmtOutput tells the client about the results schema of a prepared
	// statement.
	SetPrepStmtOutput(context.Context, sqlbase.ResultColumns)
	// SetPortalOutput tells the client about the results schema and formatting of
	// a portal.
	SetPortalOutput(context.Context, sqlbase.ResultColumns, []pgwirebase.FormatCode)
}

// ParseResult represents the result of a Parse command.
type ParseResult interface {
	ResultBase
}

// BindResult represent the result of a Bind command.
type BindResult interface {
	ResultBase
}

// DeleteResult represent the result of a DeletePreparedStatement command.
type ErrorResult interface {
	ResultBase
}

// ErrorResult represent the result of a SendError command.
type DeleteResult interface {
	ResultBase
}

// SyncResult represent the result of a Sync command. When closed, a
// readyForQuery message will be generated and all buffered data will be
// flushed.
type SyncResult interface {
	ResultBase
}

type EmptyQueryResult interface {
	ResultBase
}

type CopyInResult interface {
	ResultBase
}

// ResultBase is the common interface implemented by all the different command
// results.
type ResultBase interface {
	// SetError accumulates an execution error that needs to be reported to the
	// client. No further calls other than Close() and Discard() are allowed. In
	// particular, CloseWithErr() is not allowed.
	SetError(error)

	// Err returns the error previously set with SetError(), if any.
	Err() error

	// Close marks a result as complete. All results must be eventually closed
	// through Close()/CloseWithErr()/Discard. No further uses of the CommandResult are
	// allowed.
	// The implementation is free to deliver it to the client at will (except if
	// there's a ClientLock in effect).
	Close(TransactionStatusIndicator)

	// CloseWithErr is like Close, except it tells the client that an execution
	// error has happened. All rows previously accumulated on the result might be
	// discarded; only this error might be delivered to the client as a result of
	// the command.
	//
	// After calling CloseWithErr it is illegal to create CommandResults for any
	// command in the same batch as the one being closed. The contract is that the
	// next result created corresponds to the first command in the next batch.
	CloseWithErr(err error)

	// Discard is called to mark the fact that the result is being disposed off.
	// No completion message will be sent to the client. The expectation is that
	// either the no other methods on the result had previously been used (and so
	// no data has been buffered for the client), or there is a communication lock
	// in effect and the buffer will be rewound - in either case, the client will
	// never see any bytes pertaining to this result.
	Discard()
}

// ClientLock is an interface returned by ClientComm.lockCommunication(). It
// represents a lock on the delivery of results to a SQL client. While such a
// lock is used, no more results are delivered. The lock itself can be used to
// query what results have already been delivered and to discard results that
// haven't been delivered.
type ClientLock interface {
	// Close unlocks the ClientComm from whence this ClientLock came from. After
	// Close is called, buffered results may again be sent to the client,
	// according to the result streaming policy.
	//
	// Once Close() is called, the ClientLock cannot be used anymore.
	Close()

	// ClientPos returns the position of the latest command for which results
	// have been sent to the client. The position is relative to the start of the
	// connection.
	ClientPos() CmdPos

	// RTrim iterates backwards through the results and drops all results with
	// position >= pos.
	// It is illegal to call rtrim with a position <= clientPos(). In other words,
	// results can
	RTrim(ctx context.Context, pos CmdPos)
}

// rewindCapability is used pass rewinding instructions in between different
// layers of the connExecutor state machine. It ties together a position to
// which we want to rewind within the stream of commands with:
// a) a ClientLock that guarantees that the rewind to the respective position is
// (and remains) possible.
// b) the StmtBuf that needs to be rewound at the same time as the results.
//
// rewindAndUnlock() needs to be called eventually in order to actually perform
// the rewinding and unlock the respective ClientComm.
type rewindCapability struct {
	cl  ClientLock
	buf *StmtBuf

	rewindPos CmdPos
}

// WIP(andrei)
var _ = func() {
	var rc rewindCapability
	// use the rewindAndUnlock member function
	var _ = rc.rewindAndUnlock
}
var _ = rewindCapability{}.cl
var _ = rewindCapability{}.buf
var _ = rewindCapability{}.rewindPos

// rewindAndUnlock performs the rewinding described by the rewindCapability and
// unlocks the respective ClientComm.
func (rc *rewindCapability) rewindAndUnlock(ctx context.Context) {
	rc.cl.RTrim(ctx, rc.rewindPos)
	rc.buf.rewind(ctx, rc.rewindPos)
	rc.cl.Close()
}

// close relinquishes these rewind capability without performing the rewind.
// The ClientComm is unlocked and so results can be delivered again to clients.
func (rc *rewindCapability) close() {
	rc.cl.Close()
}

type errOnlyRestrictedCommandResult struct {
	err          error
	rowsAffected int
}

var _ RestrictedCommandResult = &errOnlyRestrictedCommandResult{}

// SetColumns is part of the RestrictedCommandResult interface.
func (r *errOnlyRestrictedCommandResult) SetColumns(context.Context, sqlbase.ResultColumns) {
	panic("unimplemented")
}

// ResetStmtType is part of the RestrictedCommandResult interface.
func (r *errOnlyRestrictedCommandResult) ResetStmtType(stmt tree.Statement) {
	panic("unimplemented")
}

// AddRow is part of the RestrictedCommandResult interface.
func (r *errOnlyRestrictedCommandResult) AddRow(ctx context.Context, row tree.Datums) error {
	panic("unimplemented")
}

// SetFinishedCallback is part of the RestrictedCommandResult interface.
func (r *errOnlyRestrictedCommandResult) SetFinishedCallback(callback func()) {
	panic("unimplemented")
}

// SetError is part of the RestrictedCommandResult interface.
func (r *errOnlyRestrictedCommandResult) SetError(err error) {
	r.err = err
}

// Err is part of the RestrictedCommandResult interface.
func (r *errOnlyRestrictedCommandResult) Err() error {
	return r.err
}

// IncrementRowsAffected is part of the RestrictedCommandResult interface.
func (r *errOnlyRestrictedCommandResult) IncrementRowsAffected(n int) {
	r.rowsAffected += n
}

// RowsAffected is part of the RestrictedCommandResult interface.
func (r *errOnlyRestrictedCommandResult) RowsAffected() int {
	return r.rowsAffected
}
