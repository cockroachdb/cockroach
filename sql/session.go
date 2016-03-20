// Copyright 2015 The Cockroach Authors.
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
//
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package sql

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/trace"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
)

type isSessionTimezone interface {
	isSessionTimezone()
}

// SessionLocation ...
type SessionLocation struct {
	Location string
}

// SessionOffset ...
type SessionOffset struct {
	Offset int64
}

func (*SessionLocation) isSessionTimezone() {}
func (*SessionOffset) isSessionTimezone()   {}

// Release through releasePlanner().
var plannerPool = sync.Pool{
	New: func() interface{} {
		return makePlanner()
	},
}

func releasePlanner(p *planner) {
	// Ensure future users don't clobber the session just used.
	p.session = nil
	plannerPool.Put(p)
}

// Session contains the state of a SQL client connection.
type Session struct {
	Database string
	Syntax   int32

	// Info about the open transaction (if any).
	TxnState txnState

	Timezone              isSessionTimezone
	DefaultIsolationLevel roachpb.IsolationType
	Trace                 trace.Trace
}

func (s *Session) getLocation() (*time.Location, error) {
	switch t := s.Timezone.(type) {
	case nil:
		return time.UTC, nil
	case *SessionLocation:
		// TODO(vivek): Cache the location.
		return time.LoadLocation(t.Location)
	case *SessionOffset:
		return time.FixedZone("", int(t.Offset)), nil
	default:
		return nil, util.Errorf("unhandled timezone variant type %T", t)
	}
}

// TxnStateEnum represents the state of a SQL txn.
type TxnStateEnum int

//go:generate stringer -type=TxnStateEnum
const (
	// No txn is in scope. Either there never was one, or it got committed/rolled back.
	NoTxn TxnStateEnum = iota
	// A txn is in scope.
	Open
	// The txn has encoutered a (non-retriable) error.
	// Statements will be rejected until a COMMIT/ROLLBACK is seen.
	Aborted
	// The txn has encoutered a retriable error.
	// Statements will be rejected until a RESTART_TRANSACTION is seen.
	RestartWait
	// The KV txn has been committed successfully through a RELEASE.
	// Statements are rejected until a COMMIT is seen.
	CommitWait
)

// txnState contains state associated with an ongoing SQL txn.
// There may or may not be an open KV txn associated with the SQL txn.
// For interactive transactions (open across batches of SQL commands sent by a
// user), txnState is intended to be stored as part of a user Session.
type txnState struct {
	txn   *client.Txn
	State TxnStateEnum

	// If set, the user declared the intention to retry the txn in case of retriable
	// errors. The txn will enter a RestartWait state in case of such errors.
	retryIntent bool

	// The transaction will be retried in case of retriable error. The retry will be
	// automatic (done by Txn.Exec()). This field behaves the same as retryIntent,
	// except it's reset in between client round trips.
	autoRetry bool

	// A COMMIT statement has been processed. Useful for allowing the txn to
	// survive retriable errors if it will be auto-retried (BEGIN; ... COMMIT; in
	// the same batch), but not if the error needs to be reported to the user.
	commitSeen bool

	// The schema change closures to run when this txn is done.
	schemaChangers schemaChangerCollection
	// TODO(andrei): this is the same as Session.Trace. Consider removing this and
	// passing the Session along everywhere the trace is needed.
	tr trace.Trace
}

// reset creates a new Txn and initializes it using the session defaults.
func (ts *txnState) reset(e *Executor, s *Session) {
	*ts = txnState{}
	ts.txn = client.NewTxn(*e.ctx.DB)
	ts.txn.Proto.Isolation = s.DefaultIsolationLevel
	ts.tr = s.Trace
}

func (ts *txnState) willBeRetried() bool {
	return ts.autoRetry || ts.retryIntent
}

func (ts *txnState) resetStateAndTxn(state TxnStateEnum) {
	ts.State = state
	ts.txn = nil
}

// updateStateAndCleanupOnErr updates txnState based on the type of error that we
// received. If it's a retriable error and we're going to retry the txn,
// then the state moves to RestartWait. Otherwise, the state moves to Aborted
// and the KV txn is cleaned up.
func (ts *txnState) updateStateAndCleanupOnErr(pErr *roachpb.Error, e *Executor) {
	if pErr == nil {
		panic("updateStateAndCleanupOnErr called with no error")
	}
	if pErr.TransactionRestart == roachpb.TransactionRestart_NONE || !ts.willBeRetried() {
		// We can't or don't want to retry this txn, so the txn is over.
		e.txnAbortCount.Inc(1)
		ts.txn.CleanupOnError(pErr)
		ts.resetStateAndTxn(Aborted)
	} else {
		// If we got a retriable error, move the SQL txn to the RestartWait state.
		// Note that TransactionAborted is also a retriable error, handled here;
		// in this case cleanup for the txn has been done for us under the hood.
		switch pErr.TransactionRestart {
		case roachpb.TransactionRestart_BACKOFF:
			// TODO(spencer): Get rid of BACKOFF retries. Note that we don't propagate
			// the backoff hint to the client anyway. See #5249
			fallthrough
		case roachpb.TransactionRestart_IMMEDIATE:
			ts.State = RestartWait
		default:
			panic(fmt.Sprintf("unexpected restart value: %s", pErr.TransactionRestart))
		}
	}
}
