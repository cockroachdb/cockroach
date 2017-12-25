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

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// This file contains utils and interfaces used by a ConnExecutor to communicate
// with a SQL client. There's stmtBuf used for input and clientComm used for
// output.

// queryStrPos represents the index of a query string relative to the start of a
// ConnExecutor. The first query string received on a connection has
// queryStrPos = 0.
type queryStrPos int64

// stmtIdx represents the index of a statement within the query string that it's
// part of.
type stmtIdx int

// cursorPosition represents a position of a cursor iterating over statements in
// a stmtBuf.
type cursorPosition struct {
	// queryStrPos indicates the current query string (a.k.a. batch of statements
	// received from the client as a unit). The position is relative to the start
	// of the connection, even though the stmtBuf may not contain query strings
	// from the beginning of the connection (old query strings are cleared).
	queryStrPos queryStrPos
	// stmtIdx indicates the index of a query within a query string.
	stmtIdx stmtIdx
}

func (c cursorPosition) String() string {
	return fmt.Sprintf("%d:%d", c.queryStrPos, c.stmtIdx)
}

// compare return -1 if c < other, 0 if c == other, 1 if c > other.
func (c cursorPosition) compare(other cursorPosition) int {
	if c.queryStrPos < other.queryStrPos ||
		(c.queryStrPos == other.queryStrPos && c.stmtIdx < other.stmtIdx) {
		return -1
	}
	if c.queryStrPos == other.queryStrPos && c.stmtIdx == other.stmtIdx {
		return 0
	}
	return 1
}

// stmtBuf represents a list of query strings (batches of SQL statements sent as
// a unit by a client) that a SQL client has sent for execution. The buffer
// is supposed to be used by one reader and one writer. The writer adds
// statements to the buffer using push(). The reader reads one statement at a
// time using curStmt(). The buffer internally maintains a cursor representing
// the reader's position. The reader has to manually move the cursor using
// advanceOne(), seekToNextQueryStr() and rewind().
//
// In practice, the writer is a module responsible for communicating with a SQL
// client (i.e.  pgwire) and the reader is a ConnExecutor.
//
// push() can be called concurrently with curStmt().
//
// The ConnExecutor will use the buffer to maintain a window around the
// statement it is currently executing. It will maintain enough history for
// executing statements again in case of an automatic retry. The ConnExecutor is
// in charge of trimming completed statements from the buffer when it's done
// with them.
type stmtBuf struct {
	mu struct {
		syncutil.Mutex

		// closed, if set, means that the writer has closed the buffer. See Close().
		closed bool

		// cond is signaled when new statements are pushed.
		cond *sync.Cond

		queryStrings []tree.StatementList
		// startPos indicates the index of queryStrings[0] relative to the start of
		// the connection.
		startPos queryStrPos
		// curPos is the current position of the cursor going through the statements.
		// At any time, curPos indicates the position of the statement to be returned
		// by curStmt().
		curPos cursorPosition
	}
}

// newStmtBuf creates a stmtBuf.
func newStmtBuf() *stmtBuf {
	var buf stmtBuf
	buf.mu.cond = sync.NewCond(&buf.mu.Mutex)
	return &buf
}

// Close marks the buffer as closed. Once Close() is called, no further push()es
// are allowed. If a reader is blocked on a curStmt() call, it is unblocked with
// io.EOF. Any further curStmt() call also returns io.EOF (even if some
// statements were already available in the buffer before the Close()).
func (buf *stmtBuf) Close() {
	buf.mu.Lock()
	buf.mu.closed = true
	buf.mu.cond.Signal()
	buf.mu.Unlock()
}

// push adds a query string to the end of the buffer. If a curStmt() call was
// blocked waiting for this query string to arrive, it will be woken up.
func (buf *stmtBuf) push(ctx context.Context, stmts tree.StatementList) {
	buf.mu.Lock()
	defer buf.mu.Unlock()
	if buf.mu.closed {
		log.Fatal(ctx, "cannot push after Close()")
	}
	buf.mu.queryStrings = append(buf.mu.queryStrings, stmts)
	buf.mu.cond.Signal()
}

// curStmt returns the statement currently indicated by the cursor. Besides
// the statement itself, the statement's position is also returned; the position
// can be used to later rewind() to this statement.
// If the cursor is positioned at the beginning of the (next) query string and
// that query string hasn't arrived yet, this blocks until the query string is
// pushed to the buffer.
//
// If the buffer has previously been Close()d, or is closed while this is
// blocked, io.EOF is returned.
func (buf *stmtBuf) curStmt(ctx context.Context) (tree.Statement, cursorPosition, error) {
	buf.mu.Lock()
	defer buf.mu.Unlock()
	for {
		if buf.mu.closed {
			return nil, cursorPosition{}, io.EOF
		}
		curPos := buf.mu.curPos
		qsIdx, err := buf.translateQueryStrPosLocked(curPos.queryStrPos)
		if err != nil {
			return nil, cursorPosition{}, err
		}
		if qsIdx < len(buf.mu.queryStrings) {
			qs := buf.mu.queryStrings[qsIdx]
			if int(curPos.stmtIdx) >= len(qs) {
				log.Fatalf(ctx, "corrupt cursor: %s", curPos)
			}
			return buf.mu.queryStrings[qsIdx][curPos.stmtIdx], curPos, nil
		}
		if (qsIdx != len(buf.mu.queryStrings)) || (curPos.stmtIdx != 0) {
			return nil, cursorPosition{}, errors.Errorf(
				"can only wait for next query string; corrupt cursor: %s", curPos)
		}
		// Wait for the next statement to arrive to the buffer.
		buf.mu.cond.Wait()
	}
}

// translateQueryStrPosLocked translates an absolute position of a query string
// (counting from the connection start) to the index of the respective query
// string among the query strings currently buffered in buf (so, it returns an
// index relative to the start of the buffer).
//
// Attempting to translate a position that's below buf.startPos returns an
// error.
func (buf *stmtBuf) translateQueryStrPosLocked(pos queryStrPos) (int, error) {
	if pos < buf.mu.startPos {
		return 0, errors.Errorf(
			"position %d no longer in buffer (buffer starting at %d)",
			pos, buf.mu.startPos)
	}
	return int(pos - buf.mu.startPos), nil
}

// ltrim iterates over the buffer forward and removes all query strings up to
// (not including) the query string indicated by pos.
// Nothing is done for statements in pos' query string (even if pos is higher
// than some statements).
//
// It's illegal to ltrim to a position higher than the current cursor.
func (buf *stmtBuf) ltrim(ctx context.Context, pos cursorPosition) {
	buf.mu.Lock()
	defer buf.mu.Unlock()
	if pos.queryStrPos < buf.mu.startPos {
		log.Fatalf(ctx, "invalid ltrim query string position: %s. buf starting at: %d",
			pos, buf.mu.startPos)
	}
	if buf.mu.curPos.compare(pos) < 0 {
		log.Fatalf(ctx, "invalid ltrim position: %s when cursor is: %s",
			pos, buf.mu.curPos)
	}
	// Remove query strings one by one.
	for {
		if (len(buf.mu.queryStrings) == 0) || (buf.mu.startPos == pos.queryStrPos) {
			break
		}
		buf.mu.queryStrings = buf.mu.queryStrings[1:]
		buf.mu.startPos++
	}
}

// advanceOne advances the cursor one statement over. The statement over which
// the cursor will be positioned when this returns may not be in the
// buffer yet.
func (buf *stmtBuf) advanceOne(ctx context.Context) {
	buf.mu.Lock()
	defer buf.mu.Unlock()
	curBatchIdx, err := buf.translateQueryStrPosLocked(buf.mu.curPos.queryStrPos)
	if err != nil {
		log.Fatal(ctx, err)
	}
	if int(buf.mu.curPos.stmtIdx) == len(buf.mu.queryStrings[curBatchIdx])-1 {
		// We were positioned on the last statement in a query string. Time to
		// advance to the next query string.
		buf.mu.curPos.queryStrPos++
		buf.mu.curPos.stmtIdx = 0
	} else {
		buf.mu.curPos.stmtIdx++
	}
}

// seekToNextQueryStr moves the cursor position to the start of the next query
// string, skipping past any statements left in the current query string (if
// any).
// The statement over which the cursor will be positioned when this returns may
// not be in the buffer yet.
func (buf *stmtBuf) seekToNextQueryStr() {
	buf.mu.Lock()
	buf.mu.curPos.stmtIdx = 0
	buf.mu.curPos.queryStrPos++
	buf.mu.Unlock()
}

// rewind resets the buffer's position to pos.
func (buf *stmtBuf) rewind(ctx context.Context, pos cursorPosition) {
	buf.mu.Lock()
	defer buf.mu.Unlock()
	if pos.queryStrPos < buf.mu.startPos {
		log.Fatalf(ctx, "attempting to rewind below buffer start")
	}
	buf.mu.curPos = pos
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
	createStatementResult(stmt tree.Statement, pos cursorPosition) StatementResult

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
	// the corresponding stmtBuf will not be rewind()ed to positions <= the
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
	// 	 stmtBuf at the moment.
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

	// clientPos returns the position of the latest statement for which results
	// have been sent to the client. The position is relative to the start of the
	// connection.
	clientPos() cursorPosition

	// rtrim iterates backwards through the results and drops all results with
	// position >= pos.
	// It is illegal to call rtrim with a position <= clientPos(). In other words,
	// results can
	rtrim(pos cursorPosition)
}

// rewindCapability is used pass rewinding instructions in between different
// layers of the ConnExecutor state machine. It ties together a position to
// which we want to rewind within the stream of statements with:
// a) a clientLock that guarantees that the rewind to the respective position is
// (and remains) possible.
// b) the stmtBuf that needs to be rewound at the same time as the results.
//
// rewindAndUnlock() needs to be called eventually in order to actually perform
// the rewinding and unlock the respective clientComm.
type rewindCapability struct {
	cl  clientLock
	buf *stmtBuf

	rewindPos cursorPosition
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
