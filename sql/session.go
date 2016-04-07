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
// Author: Andrei Matei (andreimatei1@gmail.com)

package sql

import (
	"fmt"
	"net"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/net/trace"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
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

// Session contains the state of a SQL client connection.
// Create instances using NewSession().
type Session struct {
	Database string
	User     string
	Syntax   int32

	// Info about the open transaction (if any).
	TxnState txnState

	planner planner

	Timezone              isSessionTimezone
	DefaultIsolationLevel roachpb.IsolationType
	Trace                 trace.Trace
}

// SessionArgs contains arguments for creating a new Session with NewSession().
type SessionArgs struct {
	Database string
	User     string
}

// NewSession creates and initializes new Session object.
// remote can be nil.
func NewSession(args SessionArgs, e *Executor, remote net.Addr) *Session {
	s := Session{}
	s.Database = args.Database
	s.User = args.User
	cfg, cache := e.getSystemConfig()
	s.planner = planner{
		// evalCtx is set in the Executor, for each Prepare or Execute.
		evalCtx:       parser.EvalContext{},
		leaseMgr:      e.ctx.LeaseManager,
		systemConfig:  cfg,
		databaseCache: cache,
		session:       &s,
		execCtx:       &e.ctx,
	}
	remoteStr := ""
	if remote != nil {
		remoteStr = remote.String()
	}
	s.Trace = trace.New("sql."+args.User, remoteStr)
	s.Trace.SetMaxEvents(100)
	return &s
}

// Finish releases resources held by the Session.
func (s *Session) Finish() {
	if s.Trace != nil {
		s.Trace.Finish()
		s.Trace = nil
	}
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

	// false at first, true since the moment when a transaction is retried.
	// TODO(andrei): this duplicates the retrying field in client.Txn, but the
	// state of that one is not reliable because of #5531. Clean this field up
	// once that bug is settled.
	retrying bool

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

	// The timestamp to report for current_timestamp(), now() etc.
	// This must be constant for the lifetime of a SQL transaction.
	sqlTimestamp time.Time
}

// reset creates a new Txn and initializes it using the session defaults.
func (ts *txnState) reset(ctx context.Context, e *Executor, s *Session) {
	*ts = txnState{}
	ts.txn = client.NewTxn(ctx, *e.ctx.DB)
	ts.txn.Proto.Isolation = s.DefaultIsolationLevel
	ts.tr = s.Trace
	// Discard the old schemaChangers, if any.
	ts.schemaChangers = schemaChangerCollection{}
}

func (ts *txnState) willBeRetried() bool {
	return ts.autoRetry || ts.retryIntent
}

func (ts *txnState) resetStateAndTxn(state TxnStateEnum) {
	if state != NoTxn && state != Aborted {
		panic(fmt.Sprintf("resetStateAndTxn called with unsupported state: %s", state))
	}
	if ts.txn != nil && !ts.txn.IsFinalized() {
		panic(fmt.Sprintf(
			"attempting to move SQL txn to state %s inconsistent with KV txn state: %s "+
				"(finalized: %t)", state, ts.txn.Proto.Status, ts.txn.IsFinalized()))
	}

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

type schemaChangerCollection struct {
	// A schemaChangerCollection accumulates schemaChangers from potentially
	// multiple user requests, part of the same SQL transaction. We need to
	// remember what group and index within the group each schemaChanger came
	// from, so we can map failures back to the statement that produced them.
	curGroupNum int

	// The index of the current statement, relative to its group. For statements
	// that have been received from the client in the same batch, the
	// group consists of all statements in the same transaction.
	curStatementIdx int
	// schema change callbacks together with the index of the statement
	// that enqueued it (within its group of statements).
	schemaChangers []struct {
		epoch int
		idx   int
		sc    SchemaChanger
	}
}

func (scc *schemaChangerCollection) queueSchemaChanger(
	schemaChanger SchemaChanger) {
	scc.schemaChangers = append(
		scc.schemaChangers,
		struct {
			epoch int
			idx   int
			sc    SchemaChanger
		}{scc.curGroupNum, scc.curStatementIdx, schemaChanger})
}

// execSchemaChanges releases schema leases and runs the queued
// schema changers. This needs to be run after the transaction
// scheduling the schema change has finished.
//
// The list of closures is cleared after (attempting) execution.
//
// Args:
//  results: The results from all statements in the group that scheduled the
//    schema changes we're about to execute. Results corresponding to the
//    schema change statements will be changed in case an error occurs.
func (scc *schemaChangerCollection) execSchemaChanges(
	e *Executor, planMaker *planner, results ResultList) {
	if planMaker.txn != nil {
		panic("trying to execute schema changes while still in a transaction")
	}
	// Release the leases once a transaction is complete.
	planMaker.releaseLeases()
	if len(scc.schemaChangers) == 0 ||
		// Disable execution in some tests.
		disableSyncSchemaChangeExec {
		return
	}
	// Execute any schema changes that were scheduled, in the order of the
	// statements that scheduled them.
	retryOpt := defaultRetryOpt
	for _, scEntry := range scc.schemaChangers {
		sc := &scEntry.sc
		sc.db = *e.ctx.DB
		for r := retry.Start(retryOpt); r.Next(); {
			if done, err := sc.IsDone(); err != nil {
				log.Warning(err)
				break
			} else if done {
				break
			}
			if pErr := sc.exec(); pErr != nil {
				if _, ok := pErr.GetDetail().(*roachpb.ExistingSchemaChangeLeaseError); ok {
					// Try again.
					continue
				}
				// All other errors can be reported; we report it as the result
				// corresponding to the statement that enqueued this changer.
				// There's some sketchiness here: we assume there's a single result
				// per statement and we clobber the result/error of the corresponding
				// statement.
				// There's also another subtlety: we can only report results for
				// statements in the current batch; we can't modify the results of older
				// statements.
				if scEntry.epoch == scc.curGroupNum {
					results[scEntry.idx] = Result{PErr: pErr}
				}
			}
			break
		}
	}
	scc.schemaChangers = scc.schemaChangers[:0]
}
