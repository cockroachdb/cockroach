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

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// This file contains utils and interfaces used by a ConnExecutor to communicate
// with a SQL client. There's StmtBuf used for input and clientComm used for
// output.

// cmdPos represents the index of a command relative to the start of a
// connection. The first command received on a connection has position 0.
type cmdPos int64

type batchNumber int64

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
// time using curCmd(). The buffer internally maintains a cursor representing
// the reader's position. The reader has to manually move the cursor using
// advanceOne(), seekToNextBatch() and rewind(). In practice, the writer is a
// module responsible for communicating with a SQL client (i.e.  pgwire) and the
// reader is a ConnExecutor.
//
// The StmtBuf supports grouping commands into "batches". The writer specifies
// when a batch starts and ends via StartBatch. A reader can then at any time
// chose to skip over commands from the current batch. This is used to implement
// Postgres error semantics: when an error happens during processing of a
// command, some future commands might need to be skipped. Batches correspond
// either to multiple queries received in a single query string (when the SQL
// client sends a semicolon-separated list of queries as part of the "simple"
// protocol), or to different commands pipelined by the cliend, separated from
// "sync" messages.
//
// push() can be called concurrently with curCmd().
//
// The ConnExecutor will use the buffer to maintain a window around the
// command it is currently executing. It will maintain enough history for
// executing commands again in case of an automatic retry. The ConnExecutor is
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
		data []bufferEntry

		// startPos indicates the index of the first command currently in data
		// relative to the start of the connection.
		startPos cmdPos
		// cmdPos is the current position of the cursor going through the commands.
		// At any time, curPos indicates the position of the command to be returned
		// by curCmd().
		curPos cmdPos

		// curWriteBatch maintains the batch number that pushed commands belong to.
		curWriteBatch batchNumber
	}
}

type bufferEntry struct {
	Command

	// batchNum is the id of the batch that this Command belongs to.
	batchNum batchNumber
}

// Command is an interface implemented by all commands pushed by pgwire into the
// buffer.
type Command interface {
	command()
}

// ExecStmt is the command for running a query sent through the "simple" pgwire
// protocol.
type ExecStmt struct {
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

// PrepareStmt is the command for creating a prepared statement.
type PrepareStmt struct {
	Name       string
	Stmt       tree.Statement
	TypeHints  tree.PlaceholderTypes
	ParseStart time.Time
	ParseEnd   time.Time
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
	ProtocolMeta          interface{}
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

// NewStmtBuf creates a StmtBuf.
func NewStmtBuf() *StmtBuf {
	var buf StmtBuf
	buf.mu.cond = sync.NewCond(&buf.mu.Mutex)
	return &buf
}

// Close marks the buffer as closed. Once Close() is called, no further push()es
// are allowed. If a reader is blocked on a curCmd() call, it is unblocked with
// io.EOF. Any further curCmd() call also returns io.EOF (even if some
// commands were already available in the buffer before the Close()).
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
	buf.mu.data = append(buf.mu.data, bufferEntry{Command: cmd, batchNum: buf.mu.curWriteBatch})
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
func (buf *StmtBuf) curCmd(ctx context.Context) (Command, cmdPos, error) {
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
			return buf.mu.data[cmdIdx].Command, curPos, nil
		}
		if cmdIdx != len(buf.mu.data) {
			return nil, 0, errors.Errorf(
				"can only wait for next command; corrupt cursor: %d", curPos)
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
func (buf *StmtBuf) translatePosLocked(pos cmdPos) (int, error) {
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
func (buf *StmtBuf) ltrim(ctx context.Context, pos cmdPos) {
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
//
// seekToNextBatch blocks until a command from the next batch is pushed into the
// buffer.
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
	curBatch := buf.mu.data[cmdIdx].batchNum
	buf.mu.Unlock()

	for {
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
		batch := buf.mu.data[cmdIdx].batchNum
		buf.mu.Unlock()

		if curBatch != batch {
			break
		}
	}
	return nil
}

// rewind resets the buffer's position to pos.
func (buf *StmtBuf) rewind(ctx context.Context, pos cmdPos) {
	buf.mu.Lock()
	defer buf.mu.Unlock()
	if pos < buf.mu.startPos {
		log.Fatalf(ctx, "attempting to rewind below buffer start")
	}
	buf.mu.curPos = pos
}

// StartBatch makes the next commands to be pushed be part of a new batch.
func (buf *StmtBuf) StartBatch() {
	buf.mu.Lock()
	buf.mu.curWriteBatch++
	buf.mu.Unlock()
}

// clientComm is the interface used by the ConnExecutor for creating results to
// be communicated to client and for exerting some control over this
// communication.
//
// clientComm is implemented by the pgwire connection.
type clientComm interface {
	// createStatementResult creates a StatementResult for stmt. pos is the stmt's
	// position within the connection and is used to enforce that results are
	// created in order and also to discard results through clientLock.rtrim(pos).
	createStatementResult(stmt tree.Statement, pos cmdPos) StatementResult

	// lockCommunication ensures that no further results are delivered to the
	// client. The returned clientLock can be queried to see what results have
	// been already delivered to the client and to discard results that haven't
	// been delivered.
	//
	// clientLock.Close() needs to be called on the returned lock once
	// communication can be unlocked (i.e. results can be delivered to the client
	// again).
	lockCommunication() clientLock

	// flush tells the implementation that all the results produced so far can be
	// delivered to the SQL client. In other words, this is promising that
	// the corresponding StmtBuf will not be rewind()ed to positions <= the
	// last position passed to createStatementResult().
	// flush() is generally called once a transaction ends, as the connExecutor no
	// longer needs to be able to rewind past the txn's last statement.
	//
	// An error is returned if the client connection is broken. In this case, no
	// further calls can be made on this clientComm. The caller should interrupt
	// any statements running on the respective connection.
	//
	// The connExecutor's expectation is that, after flush() is called, results
	// that are part of future StatementResult's will be buffered (in the now
	// empty buffer*) by the implementer until one of the following happens:
	// - the buffer overflows
	// - flush() is called again
	// - the last StatementResult is Close()d and there are no more statements in
	// 	 StmtBuf at the moment.
	// If the implementer respects this, then it will be guaranteed that, as long
	// as a transaction prefix's results fit in said buffer, the connExecutor will
	// be able to automatically retry the prefix in case of retriable errors so
	// that the client doesn't need to worry about them. As an important special
	// case, this means that implicit transactions (i.e. statements executed
	// outside of a transaction) are always automatically retried as long as their
	// results fit in the implementer's buffer.
	//
	// TODO(andrei): In the future we might want to add a time component to this
	// policy (or make it user configurable), and restrict the guarantees about
	// automatic retries to queries that are fast enough.
	//
	// (*) The implication is that, if the implementer wishes to deliver the
	// contents of the buffer to the client concurrently with accepting new
	// results after the flush() call, it needs to behave as if the buffer's
	// capacity was just expanded: the size of the buffer's contents at the moment
	// of the flush() call cannot impact the capacity available for future
	// results.
	flush() error
}

// WIP(andrei)
var _ clientComm

// clientLock is an interface returned by clientComm.lockCommunication(). It
// represents a lock on the delivery of results to a SQL client. While such a
// lock is used, no more results are delivered. The lock itself can be used to
// query what results have already been delivered and to discard results that
// haven't been delivered.
type clientLock interface {
	// Close unlocks the clientComm from whence this clientLock came from. After
	// Close is called, buffered results may again be sent to the client,
	// according to the result streaming policy.
	//
	// Once Close() is called, the clientLock cannot be used anymore.
	Close()

	// clientPos returns the position of the latest command for which results
	// have been sent to the client. The position is relative to the start of the
	// connection.
	clientPos() cmdPos

	// rtrim iterates backwards through the results and drops all results with
	// position >= pos.
	// It is illegal to call rtrim with a position <= clientPos(). In other words,
	// results can
	rtrim(pos cmdPos)
}

// rewindCapability is used pass rewinding instructions in between different
// layers of the ConnExecutor state machine. It ties together a position to
// which we want to rewind within the stream of commands with:
// a) a clientLock that guarantees that the rewind to the respective position is
// (and remains) possible.
// b) the StmtBuf that needs to be rewound at the same time as the results.
//
// rewindAndUnlock() needs to be called eventually in order to actually perform
// the rewinding and unlock the respective clientComm.
type rewindCapability struct {
	cl  clientLock
	buf *StmtBuf

	rewindPos cmdPos
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
// unlocks the respective clientComm.
func (rc *rewindCapability) rewindAndUnlock(ctx context.Context) {
	rc.cl.rtrim(rc.rewindPos)
	rc.buf.rewind(ctx, rc.rewindPos)
	rc.cl.Close()
}

// WIP(andrei):
//
// // getRewindTxnCapability locks the clientComm if it's possible to rewind to the
// // point where the transaction started. If that is possible, a rewindCapability
// // is returned (and thus the clientComm is locked) and the returned bool is
// // true. In this case, rewindAndUnlock() needs to be called on the returned
// // capability.
// //
// // If it's not possible to rewind to the transaction's start point (because
// // further results have been already sent to the client), then the returned bool
// // is false and the returned rewingCapability can be ignored.
// func (s *ConnExecutor) getRewindTxnCapability() (rewindCapability, bool) {
//   cl := s.clientComm.lockCommunication()
//   if s.txnStartPos.compare(cl.clientPos()) <= 0 {
//     cl.Close()
//     return rewindCapability{}, false
//   }
//   return rewindCapability{
//     // Pass cl along. The caller will have to Close() it through
//     // rewindCapability.rewindAndUnlock().
//     cl:             cl,
//     buf:            s.stmtsBuf,
//     rewindPosition: s.TxnState.txnStartPos,
//   }, true
// }
