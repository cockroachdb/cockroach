// Copyright 2017 The Cockroach Authors.
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
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/ring"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// This file contains utils and interfaces used by a connExecutor to communicate
// with a SQL client. There's StmtBuf used for input and ClientComm used for
// output.

// CmdPos represents the index of a command relative to the start of a
// connection. The first command received on a connection has position 0.
type CmdPos int64

// TransactionStatusIndicator represents a pg identifier for the transaction state.
type TransactionStatusIndicator byte

const (
	// IdleTxnBlock means the session is outside of a transaction.
	IdleTxnBlock TransactionStatusIndicator = 'I'
	// InTxnBlock means the session is inside a transaction.
	InTxnBlock TransactionStatusIndicator = 'T'
	// InFailedTxnBlock means the session is inside a transaction, but the
	// transaction is in the Aborted state.
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
// time using CurCmd(). The consumer is then supposed to create command results
// (the buffer is not involved in this).
// The buffer internally maintains a cursor representing the reader's position.
// The reader has to manually move the cursor using AdvanceOne(),
// seekToNextBatch() and rewind().
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
// push() can be called concurrently with CurCmd().
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

		// data contains the elements of the buffer.
		data ring.Buffer // []Command

		// startPos indicates the index of the first command currently in data
		// relative to the start of the connection.
		startPos CmdPos
		// curPos is the current position of the cursor going through the commands.
		// At any time, curPos indicates the position of the command to be returned
		// by CurCmd().
		curPos CmdPos
		// lastPos indicates the position of the last command that was pushed into
		// the buffer.
		lastPos CmdPos
	}
}

// Command is an interface implemented by all commands pushed by pgwire into the
// buffer.
type Command interface {
	fmt.Stringer
	// command returns a string representation of the command type (e.g.
	// "prepare stmt", "exec stmt").
	command() string
}

// ExecStmt is the command for running a query sent through the "simple" pgwire
// protocol.
type ExecStmt struct {
	// Information returned from parsing: AST, SQL, NumPlaceholders.
	// Note that AST can be nil, in which case executing it should produce an
	// "empty query response" message.
	parser.Statement

	// TimeReceived is the time at which the exec message was received
	// from the client. Used to compute the service latency.
	TimeReceived time.Time
	// ParseStart/ParseEnd are the timing info for parsing of the query. Used for
	// stats reporting.
	ParseStart time.Time
	ParseEnd   time.Time
}

// command implements the Command interface.
func (ExecStmt) command() string { return "exec stmt" }

func (e ExecStmt) String() string {
	// We have the original SQL, but we still use String() because it obfuscates
	// passwords.
	s := "(empty)"
	// e.AST could be nil in the case of a completely empty query.
	if e.AST != nil {
		s = e.AST.String()
	}
	return fmt.Sprintf("ExecStmt: %s", s)
}

var _ Command = ExecStmt{}

// ExecPortal is the Command for executing a portal.
type ExecPortal struct {
	Name string
	// limit is a feature of pgwire that we don't really support. We accept it and
	// don't complain as long as the statement produces fewer results than this.
	Limit int
	// TimeReceived is the time at which the exec message was received
	// from the client. Used to compute the service latency.
	TimeReceived time.Time
}

// command implements the Command interface.
func (ExecPortal) command() string { return "exec portal" }

func (e ExecPortal) String() string {
	return fmt.Sprintf("ExecPortal name: %q", e.Name)
}

var _ Command = ExecPortal{}

// PrepareStmt is the command for creating a prepared statement.
type PrepareStmt struct {
	// Name of the prepared statement (optional).
	Name string

	// Information returned from parsing: AST, SQL, NumPlaceholders.
	// Note that AST can be nil, in which case executing it should produce an
	// "empty query response" message.
	parser.Statement

	TypeHints tree.PlaceholderTypes
	// RawTypeHints is the representation of type hints exactly as specified by
	// the client.
	RawTypeHints []oid.Oid
	ParseStart   time.Time
	ParseEnd     time.Time
}

// command implements the Command interface.
func (PrepareStmt) command() string { return "prepare stmt" }

func (p PrepareStmt) String() string {
	// We have the original SQL, but we still use String() because it obfuscates
	// passwords.
	s := "(empty)"
	// p.AST could be nil in the case of a completely empty query.
	if p.AST != nil {
		s = p.AST.String()
	}
	return fmt.Sprintf("PrepareStmt: %s", s)
}

var _ Command = PrepareStmt{}

// DescribeStmt is the Command for producing info about a prepared statement or
// portal.
type DescribeStmt struct {
	Name string
	Type pgwirebase.PrepareType
}

// command implements the Command interface.
func (DescribeStmt) command() string { return "describe stmt" }

func (d DescribeStmt) String() string {
	return fmt.Sprintf("Describe: %q", d.Name)
}

var _ Command = DescribeStmt{}

// BindStmt is the Command for creating a portal from a prepared statement.
type BindStmt struct {
	PreparedStatementName string
	PortalName            string
	// OutFormats contains the requested formats for the output columns.
	// It either contains a bunch of format codes, in which case the number will
	// need to match the number of output columns of the portal, or contains a single
	// code, in which case that code will be applied to all columns.
	OutFormats []pgwirebase.FormatCode
	// Args are the arguments for the prepared statement.
	// They are passed in without decoding because decoding requires type
	// inference to have been performed.
	//
	// A nil element means a tree.DNull argument.
	Args [][]byte
	// ArgFormatCodes are the codes to be used to deserialize the Args.
	// It either contains a bunch of format codes, in which case the number will
	// need to match the number of arguments for the portal, or contains a single
	// code, in which case that code will be applied to all arguments.
	ArgFormatCodes []pgwirebase.FormatCode

	// internalArgs, if not nil, represents the arguments for the prepared
	// statements as produced by the internal clients. These don't need to go
	// through encoding/decoding of the args. However, the types of the datums
	// must correspond exactly to the inferred types (but note that the types of
	// the datums are passes as type hints to the PrepareStmt command, so the
	// inferred types should reflect that).
	// If internalArgs is specified, Args and ArgFormatCodes are ignored.
	internalArgs []tree.Datum
}

// command implements the Command interface.
func (BindStmt) command() string { return "bind stmt" }

func (b BindStmt) String() string {
	return fmt.Sprintf("BindStmt: %q->%q", b.PreparedStatementName, b.PortalName)
}

var _ Command = BindStmt{}

// DeletePreparedStmt is the Command for freeing a prepared statement.
type DeletePreparedStmt struct {
	Name string
	Type pgwirebase.PrepareType
}

// command implements the Command interface.
func (DeletePreparedStmt) command() string { return "delete stmt" }

func (d DeletePreparedStmt) String() string {
	return fmt.Sprintf("DeletePreparedStmt: %q", d.Name)
}

var _ Command = DeletePreparedStmt{}

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
func (Sync) command() string { return "sync" }

func (Sync) String() string {
	return "Sync"
}

var _ Command = Sync{}

// Flush is a Command asking for the results of all previous commands to be
// delivered to the client.
type Flush struct{}

// command implements the Command interface.
func (Flush) command() string { return "flush" }

func (Flush) String() string {
	return "Flush"
}

var _ Command = Flush{}

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
func (CopyIn) command() string { return "copy" }

func (CopyIn) String() string {
	return "CopyIn"
}

var _ Command = CopyIn{}

// DrainRequest represents a notice that the server is draining and command
// processing should stop soon.
//
// DrainRequest commands don't produce results.
type DrainRequest struct{}

// command implements the Command interface.
func (DrainRequest) command() string { return "drain" }

func (DrainRequest) String() string {
	return "Drain"
}

var _ Command = DrainRequest{}

// SendError is a command that, upon execution, send a specific error to the
// client. This is used by pgwire to schedule errors to be sent at an
// appropriate time.
type SendError struct {
	// Err is a *pgerror.Error.
	Err error
}

// command implements the Command interface.
func (SendError) command() string { return "send error" }

func (s SendError) String() string {
	return fmt.Sprintf("SendError: %s", s.Err)
}

var _ Command = SendError{}

// NewStmtBuf creates a StmtBuf.
func NewStmtBuf() *StmtBuf {
	var buf StmtBuf
	buf.Init()
	return &buf
}

// Init initializes a StmtBuf. It exists to avoid the allocation imposed by
// NewStmtBuf.
func (buf *StmtBuf) Init() {
	buf.mu.lastPos = -1
	buf.mu.cond = sync.NewCond(&buf.mu.Mutex)
}

// Close marks the buffer as closed. Once Close() is called, no further push()es
// are allowed. If a reader is blocked on a CurCmd() call, it is unblocked with
// io.EOF. Any further CurCmd() call also returns io.EOF (even if some
// commands were already available in the buffer before the Close()).
//
// Close() is idempotent.
func (buf *StmtBuf) Close() {
	buf.mu.Lock()
	buf.mu.closed = true
	buf.mu.cond.Signal()
	buf.mu.Unlock()
}

// Push adds a Command to the end of the buffer. If a CurCmd() call was blocked
// waiting for this command to arrive, it will be woken up.
//
// An error is returned if the buffer has been closed.
func (buf *StmtBuf) Push(ctx context.Context, cmd Command) error {
	buf.mu.Lock()
	defer buf.mu.Unlock()
	if buf.mu.closed {
		return errors.AssertionFailedf("buffer is closed")
	}
	buf.mu.data.AddLast(cmd)
	buf.mu.lastPos++

	buf.mu.cond.Signal()
	return nil
}

// CurCmd returns the Command currently indicated by the cursor. Besides the
// Command itself, the command's position is also returned; the position can be
// used to later rewind() to this Command.
//
// If the cursor is positioned over an empty slot, the call blocks until the
// next Command is pushed into the buffer.
//
// If the buffer has previously been Close()d, or is closed while this is
// blocked, io.EOF is returned.
func (buf *StmtBuf) CurCmd() (Command, CmdPos, error) {
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
		len := buf.mu.data.Len()
		if cmdIdx < len {
			return buf.mu.data.Get(cmdIdx).(Command), curPos, nil
		}
		if cmdIdx != len {
			return nil, 0, errors.AssertionFailedf(
				"can only wait for next command; corrupt cursor: %d", errors.Safe(curPos))
		}
		// Wait for the next Command to arrive to the buffer.
		buf.mu.cond.Wait()
	}
}

// translatePosLocked translates an absolute position of a command (counting
// from the connection start) to the index of the respective command in the
// buffer (so, it returns an index relative to the start of the buffer).
//
// Attempting to translate a position that's below buf.startPos returns an
// error.
func (buf *StmtBuf) translatePosLocked(pos CmdPos) (int, error) {
	if pos < buf.mu.startPos {
		return 0, errors.AssertionFailedf(
			"position %d no longer in buffer (buffer starting at %d)",
			errors.Safe(pos), errors.Safe(buf.mu.startPos))
	}
	return int(pos - buf.mu.startPos), nil
}

// Ltrim iterates over the buffer forward and removes all commands up to
// (not including) the command at pos.
//
// It's illegal to Ltrim to a position higher than the current cursor.
func (buf *StmtBuf) Ltrim(ctx context.Context, pos CmdPos) {
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
		buf.mu.data.RemoveFirst()
		buf.mu.startPos++
	}
}

// AdvanceOne advances the cursor one Command over. The command over which
// the cursor will be positioned when this returns may not be in the buffer
// yet. The previous CmdPos is returned.
func (buf *StmtBuf) AdvanceOne() CmdPos {
	buf.mu.Lock()
	prev := buf.mu.curPos
	buf.mu.curPos++
	buf.mu.Unlock()
	return prev
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
func (buf *StmtBuf) seekToNextBatch() error {
	buf.mu.Lock()
	curPos := buf.mu.curPos
	cmdIdx, err := buf.translatePosLocked(curPos)
	if err != nil {
		buf.mu.Unlock()
		return err
	}
	if cmdIdx == buf.mu.data.Len() {
		buf.mu.Unlock()
		return errors.AssertionFailedf("invalid seek start point")
	}
	buf.mu.Unlock()

	var foundSync bool
	for !foundSync {
		buf.AdvanceOne()
		_, pos, err := buf.CurCmd()
		if err != nil {
			return err
		}
		buf.mu.Lock()
		cmdIdx, err := buf.translatePosLocked(pos)
		if err != nil {
			buf.mu.Unlock()
			return err
		}

		if _, ok := buf.mu.data.Get(cmdIdx).(Sync); ok {
			foundSync = true
		}

		buf.mu.Unlock()
	}
	return nil
}

// Rewind resets the buffer's position to pos.
func (buf *StmtBuf) Rewind(ctx context.Context, pos CmdPos) {
	buf.mu.Lock()
	defer buf.mu.Unlock()
	if pos < buf.mu.startPos {
		log.Fatalf(ctx, "attempting to rewind below buffer start")
	}
	buf.mu.curPos = pos
}

// Len returns the buffer's length.
func (buf *StmtBuf) Len() int {
	buf.mu.Lock()
	defer buf.mu.Unlock()
	return buf.mu.data.Len()
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
		conv sessiondatapb.DataConversionConfig,
		location *time.Location,
		limit int,
		portalName string,
		implicitTxn bool,
	) CommandResult
	// CreatePrepareResult creates a result for a PrepareStmt command.
	CreatePrepareResult(pos CmdPos) ParseResult
	// CreateDescribeResult creates a result for a DescribeStmt command.
	CreateDescribeResult(pos CmdPos) DescribeResult
	// CreateBindResult creates a result for a BindStmt command.
	CreateBindResult(pos CmdPos) BindResult
	// CreateDeleteResult creates a result for a DeletePreparedStmt command.
	CreateDeleteResult(pos CmdPos) DeleteResult
	// CreateSyncResult creates a result for a Sync command.
	CreateSyncResult(pos CmdPos) SyncResult
	// CreateFlushResult creates a result for a Flush command.
	CreateFlushResult(pos CmdPos) FlushResult
	// CreateErrorResult creates a result on which only errors can be communicated
	// to the client.
	CreateErrorResult(pos CmdPos) ErrorResult
	// CreateEmptyQueryResult creates a result for an empty-string query.
	CreateEmptyQueryResult(pos CmdPos) EmptyQueryResult
	// CreateCopyInResult creates a result for a Copy-in command.
	CreateCopyInResult(pos CmdPos) CopyInResult
	// CreateDrainResult creates a result for a Drain command.
	CreateDrainResult(pos CmdPos) DrainResult

	// lockCommunication ensures that no further results are delivered to the
	// client. The returned ClientLock can be queried to see what results have
	// been already delivered to the client and to discard results that haven't
	// been delivered.
	//
	// ClientLock.Close() needs to be called on the returned lock once
	// communication can be unlocked (i.e. results can be delivered to the client
	// again).
	LockCommunication() ClientLock

	// Flush delivers all the previous results to the client. The results might
	// have been buffered, in which case this flushes the buffer.
	Flush(pos CmdPos) error
}

// CommandResult represents the result of a statement. It which needs to be
// ultimately delivered to the client. pgwire.conn implements this.
type CommandResult interface {
	RestrictedCommandResult
	CommandResultClose
}

// CommandResultErrBase is the subset of CommandResult dealing with setting a
// query execution error.
type CommandResultErrBase interface {
	// SetError accumulates an execution error that needs to be reported to the
	// client. No further calls other than SetError(), Close() and Discard() are
	// allowed.
	//
	// Calling SetError() a second time overwrites the previously set error.
	SetError(error)

	// Err returns the error previously set with SetError(), if any.
	Err() error
}

// ResultBase is the common interface implemented by all the different command
// results.
type ResultBase interface {
	CommandResultErrBase
	CommandResultClose
}

// CommandResultClose is a subset of CommandResult dealing with the closing of
// the result.
type CommandResultClose interface {
	// Close marks a result as complete. No further uses of the CommandResult are
	// allowed after this call. All results must be eventually closed through
	// Close()/Discard(), except in case query processing has encountered an
	// irrecoverable error and the client connection will be closed; in such
	// cases it is not mandated that these functions are called on the result
	// that may have been open at the time the error occurred.
	// NOTE(andrei): We might want to tighten the contract if the results get any
	// state that needs to be closed even when the whole connection is about to be
	// terminated.
	Close(context.Context, TransactionStatusIndicator)

	// Discard is called to mark the fact that the result is being disposed off.
	// No completion message will be sent to the client. The expectation is that
	// either the no other methods on the result had previously been used (and so
	// no data has been buffered for the client), or there is a communication lock
	// in effect and the buffer will be rewound - in either case, the client will
	// never see any bytes pertaining to this result.
	Discard()
}

// RestrictedCommandResult is a subset of CommandResult meant to make it clear
// that its clients don't close the CommandResult.
type RestrictedCommandResult interface {
	CommandResultErrBase

	// BufferParamStatusUpdate buffers a parameter status update to the result.
	// This gets flushed only when the CommandResult is closed.
	BufferParamStatusUpdate(string, string)

	// BufferNotice appends a notice to the result.
	// This gets flushed only when the CommandResult is closed.
	BufferNotice(notice pgnotice.Notice)

	// SetColumns informs the client about the schema of the result. The columns
	// can be nil.
	//
	// This needs to be called (once) before AddRow.
	SetColumns(context.Context, colinfo.ResultColumns)

	// ResetStmtType allows a client to change the statement type of the current
	// result, from the original one set when the result was created trough
	// ClientComm.createStatementResult.
	ResetStmtType(stmt tree.Statement)

	// AddRow accumulates a result row.
	//
	// The implementation cannot hold on to the row slice; it needs to make a
	// shallow copy if it needs to.
	AddRow(ctx context.Context, row tree.Datums) error

	// AddBatch accumulates a result batch.
	//
	// The implementation cannot hold on to the contents of the batch without
	// deeply copying them. The memory in the input batch is safe to modify as
	// soon as AddBatch returns.
	AddBatch(ctx context.Context, batch coldata.Batch) error

	// SupportsAddBatch returns whether this command result supports AddBatch
	// method of adding the data. If false is returned, then the behavior of
	// AddBatch is undefined.
	SupportsAddBatch() bool

	// IncrementRowsAffected increments a counter by n. This is used for all
	// result types other than tree.Rows.
	IncrementRowsAffected(ctx context.Context, n int)

	// RowsAffected returns either the number of times AddRow was called, or the
	// sum of all n passed into IncrementRowsAffected.
	RowsAffected() int

	// DisableBuffering can be called during execution to ensure that
	// the results accumulated so far, and all subsequent rows added
	// to this CommandResult, will be flushed immediately to the client.
	// This is currently used for sinkless changefeeds.
	DisableBuffering()
}

// DescribeResult represents the result of a Describe command (for either
// describing a prepared statement or a portal).
type DescribeResult interface {
	ResultBase

	// SetInferredTypes tells the client about the inferred placeholder types.
	SetInferredTypes([]oid.Oid)
	// SetNoDataRowDescription is used to tell the client that the prepared
	// statement or portal produces no rows.
	SetNoDataRowDescription()
	// SetPrepStmtOutput tells the client about the results schema of a prepared
	// statement.
	SetPrepStmtOutput(context.Context, colinfo.ResultColumns)
	// SetPortalOutput tells the client about the results schema and formatting of
	// a portal.
	SetPortalOutput(context.Context, colinfo.ResultColumns, []pgwirebase.FormatCode)
}

// ParseResult represents the result of a Parse command.
type ParseResult interface {
	ResultBase
}

// BindResult represents the result of a Bind command.
type BindResult interface {
	ResultBase
}

// ErrorResult represents the result of a SendError command.
type ErrorResult interface {
	ResultBase
}

// DeleteResult represents the result of a DeletePreparedStatement command.
type DeleteResult interface {
	ResultBase
}

// SyncResult represents the result of a Sync command. When closed, a
// readyForQuery message will be generated and all buffered data will be
// flushed.
type SyncResult interface {
	ResultBase
}

// FlushResult represents the result of a Flush command. When this result is
// closed, all previously accumulated results are flushed to the client.
type FlushResult interface {
	ResultBase
}

// DrainResult represents the result of a Drain command. Closing this result
// produces no output for the client.
type DrainResult interface {
	ResultBase
}

// EmptyQueryResult represents the result of an empty query (a query
// representing a blank string).
type EmptyQueryResult interface {
	ResultBase
}

// CopyInResult represents the result of a CopyIn command. Closing this result
// produces no output for the client.
type CopyInResult interface {
	ResultBase
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

// rewindAndUnlock performs the rewinding described by the rewindCapability and
// unlocks the respective ClientComm.
func (rc *rewindCapability) rewindAndUnlock(ctx context.Context) {
	rc.cl.RTrim(ctx, rc.rewindPos)
	rc.buf.Rewind(ctx, rc.rewindPos)
	rc.cl.Close()
}

// close closes the underlying ClientLock.
func (rc *rewindCapability) close() {
	rc.cl.Close()
}

type resCloseType bool

const closed resCloseType = true
const discarded resCloseType = false

// streamingCommandResult is a CommandResult that streams rows on the channel
// and can call a provided callback when closed.
type streamingCommandResult struct {
	// All the data (the rows and the metadata) are written into w. The
	// goroutine writing into this streamingCommandResult might block depending
	// on the synchronization strategy.
	w ieResultWriter

	err          error
	rowsAffected int

	// closeCallback, if set, is called when Close()/Discard() is called.
	closeCallback func(*streamingCommandResult, resCloseType)
}

var _ RestrictedCommandResult = &streamingCommandResult{}
var _ CommandResultClose = &streamingCommandResult{}

// SetColumns is part of the RestrictedCommandResult interface.
func (r *streamingCommandResult) SetColumns(ctx context.Context, cols colinfo.ResultColumns) {
	// The interface allows for cols to be nil, yet the iterator result expects
	// non-nil value to indicate that it was the column metadata.
	if cols == nil {
		cols = colinfo.ResultColumns{}
	}
	_ = r.w.addResult(ctx, ieIteratorResult{cols: cols})
}

// BufferParamStatusUpdate is part of the RestrictedCommandResult interface.
func (r *streamingCommandResult) BufferParamStatusUpdate(key string, val string) {
	panic("unimplemented")
}

// BufferNotice is part of the RestrictedCommandResult interface.
func (r *streamingCommandResult) BufferNotice(notice pgnotice.Notice) {
	panic("unimplemented")
}

// ResetStmtType is part of the RestrictedCommandResult interface.
func (r *streamingCommandResult) ResetStmtType(stmt tree.Statement) {
	panic("unimplemented")
}

// AddRow is part of the RestrictedCommandResult interface.
func (r *streamingCommandResult) AddRow(ctx context.Context, row tree.Datums) error {
	// AddRow() and IncrementRowsAffected() are never called on the same command
	// result, so we will not double count the affected rows by an increment
	// here.
	r.rowsAffected++
	rowCopy := make(tree.Datums, len(row))
	copy(rowCopy, row)
	return r.w.addResult(ctx, ieIteratorResult{row: rowCopy})
}

// AddBatch is part of the RestrictedCommandResult interface.
func (r *streamingCommandResult) AddBatch(context.Context, coldata.Batch) error {
	// TODO(yuzefovich): implement this.
	panic("unimplemented")
}

// SupportsAddBatch is part of the RestrictedCommandResult interface.
func (r *streamingCommandResult) SupportsAddBatch() bool {
	return false
}

func (r *streamingCommandResult) DisableBuffering() {
	panic("cannot disable buffering here")
}

// SetError is part of the RestrictedCommandResult interface.
func (r *streamingCommandResult) SetError(err error) {
	r.err = err
	// Note that we intentionally do not send the error on the channel (when it
	// is present) since we might replace the error with another one later which
	// is allowed by the interface. An example of this is queryDone() closure
	// in execStmtInOpenState().
}

// Err is part of the RestrictedCommandResult interface.
func (r *streamingCommandResult) Err() error {
	return r.err
}

// IncrementRowsAffected is part of the RestrictedCommandResult interface.
func (r *streamingCommandResult) IncrementRowsAffected(ctx context.Context, n int) {
	r.rowsAffected += n
	// streamingCommandResult might be used outside of the internal executor
	// (i.e. not by rowsIterator) in which case the channel is not set.
	if r.w != nil {
		_ = r.w.addResult(ctx, ieIteratorResult{rowsAffectedIncrement: &n})
	}
}

// RowsAffected is part of the RestrictedCommandResult interface.
func (r *streamingCommandResult) RowsAffected() int {
	return r.rowsAffected
}

// Close is part of the CommandResultClose interface.
func (r *streamingCommandResult) Close(context.Context, TransactionStatusIndicator) {
	if r.closeCallback != nil {
		r.closeCallback(r, closed)
	}
}

// Discard is part of the CommandResult interface.
func (r *streamingCommandResult) Discard() {
	if r.closeCallback != nil {
		r.closeCallback(r, discarded)
	}
}

// SetInferredTypes is part of the DescribeResult interface.
func (r *streamingCommandResult) SetInferredTypes([]oid.Oid) {}

// SetNoDataRowDescription is part of the DescribeResult interface.
func (r *streamingCommandResult) SetNoDataRowDescription() {}

// SetPrepStmtOutput is part of the DescribeResult interface.
func (r *streamingCommandResult) SetPrepStmtOutput(context.Context, colinfo.ResultColumns) {}

// SetPortalOutput is part of the DescribeResult interface.
func (r *streamingCommandResult) SetPortalOutput(
	context.Context, colinfo.ResultColumns, []pgwirebase.FormatCode,
) {
}
