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

package sql

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

// RestartSavepointName is the only savepoint ident that we accept.
const RestartSavepointName string = "cockroach_restart"

var errSavepointNotUsed = pgerror.NewErrorf(
	pgerror.CodeSavepointExceptionError,
	"savepoint %s has not been used", RestartSavepointName)

// execStmt executes one statement by dispatching according to the current
// state. Returns an Event to be passed to the state machine, or nil if no
// transition is needed. If nil is returned, then the cursor is supposed to
// advance to the next statement.
//
// If an error is returned, the session is supposed to be considered done. Query
// execution errors are not returned explicitly and they're also not
// communicated to the client. Instead they're incorporated in the returned
// event (the returned payload will implement payloadWithError). It is the
// caller's responsibility to deliver execution errors to the client.
//
// Args:
// stmt: The statement to execute.
// res: Used to produce query results.
// pinfo: The values to use for the statement's placeholders. If nil is passed,
// 	 then the statement cannot have any placeholder.
// pos: The position of stmt.
func (ex *connExecutor) execStmt(
	ctx context.Context,
	stmt Statement,
	res RestrictedCommandResult,
	pinfo *tree.PlaceholderInfo,
	pos CmdPos,
) (fsm.Event, fsm.EventPayload, error) {
	if log.V(2) || logStatementsExecuteEnabled.Get(&ex.server.cfg.Settings.SV) ||
		log.HasSpanOrEvent(ctx) {
		log.VEventf(ctx, 2, "executing: %s in state: %s", stmt, ex.machine.CurState())
	}

	// Run observer statements in a separate code path; their execution does not
	// depend on the current transaction state.
	if _, ok := stmt.AST.(tree.ObserverStatement); ok {
		err := ex.runObserverStatement(ctx, stmt, res)
		return nil, nil, err
	}

	queryID := ex.generateID()
	stmt.queryID = queryID

	// Dispatch the statement for execution based on the current state.
	var ev fsm.Event
	var payload fsm.EventPayload
	var err error

	switch ex.machine.CurState().(type) {
	case stateNoTxn:
		ev, payload = ex.execStmtInNoTxnState(ctx, stmt)
	case stateOpen:
		ev, payload, err = ex.execStmtInOpenState(ctx, stmt, pinfo, res)
		switch ev.(type) {
		case eventNonRetriableErr:
			ex.recordFailure()
		}
	case stateAborted, stateRestartWait:
		ev, payload = ex.execStmtInAbortedState(ctx, stmt, res)
	case stateCommitWait:
		ev, payload = ex.execStmtInCommitWaitState(stmt, res)
	default:
		panic(fmt.Sprintf("unexpected txn state: %#v", ex.machine.CurState()))
	}

	return ev, payload, err
}

func (ex *connExecutor) recordFailure() {
	ex.metrics.StatementCounters.FailureCount.Inc(1)
}

// execStmtInOpenState executes one statement in the context of the session's
// current transaction.
// It handles statements that affect the transaction state (BEGIN, COMMIT)
// directly and delegates everything else to the execution engines.
// Results and query execution errors are written to res.
//
// This method also handles "auto commit" - committing of implicit transactions.
//
// If an error is returned, the connection is supposed to be consider done.
// Query execution errors are not returned explicitly; they're incorporated in
// the returned Event.
//
// The returned event can be nil if no state transition is required.
func (ex *connExecutor) execStmtInOpenState(
	ctx context.Context, stmt Statement, pinfo *tree.PlaceholderInfo, res RestrictedCommandResult,
) (retEv fsm.Event, retPayload fsm.EventPayload, retErr error) {
	ex.incrementStmtCounter(stmt)
	os := ex.machine.CurState().(stateOpen)

	var timeoutTicker *time.Timer
	queryTimedOut := false
	doneAfterFunc := make(chan struct{}, 1)

	// Canceling a query cancels its transaction's context so we take a reference
	// to the cancelation function here.
	unregisterFn := ex.addActiveQuery(stmt.queryID, stmt.AST, ex.state.cancel)
	queryDone := func(ctx context.Context, res RestrictedCommandResult) {
		if timeoutTicker != nil {
			if !timeoutTicker.Stop() {
				// Wait for the timer callback to complete to avoid a data race on
				// queryTimedOut.
				<-doneAfterFunc
			}
		}
		unregisterFn()

		// Detect context cancelation and overwrite whatever error might have been
		// set on the result before. The idea is that once the query's context is
		// canceled, all sorts of actors can detect the cancelation and set all
		// sorts of errors on the result. Rather than trying to impose discipline
		// in that jungle, we just overwrite them all here with an error that's
		// nicer to look at for the client.
		if ctx.Err() != nil && res.Err() != nil {
			if queryTimedOut {
				res.OverwriteError(sqlbase.QueryTimeoutError)
			} else {
				res.OverwriteError(sqlbase.QueryCanceledError)
			}
		}
	}
	// Generally we want to unregister after the auto-commit below. However, in
	// case we'll execute the statement through the parallel execution queue,
	// we'll pass the responsibility for unregistering to the queue.
	defer func() {
		if queryDone != nil {
			queryDone(ctx, res)
		}
	}()

	if ex.sessionData.StmtTimeout > 0 {
		timeoutTicker = time.AfterFunc(
			ex.sessionData.StmtTimeout-timeutil.Since(ex.phaseTimes[sessionQueryReceived]),
			func() {
				ex.cancelQuery(stmt.queryID)
				queryTimedOut = true
				doneAfterFunc <- struct{}{}
			})
	}

	defer func() {
		if filter := ex.server.cfg.TestingKnobs.StatementFilter; retErr == nil && filter != nil {
			var execErr error
			if perr, ok := retPayload.(payloadWithError); ok {
				execErr = perr.errorCause()
			}
			filter(ctx, stmt.String(), execErr)
		}

		// Do the auto-commit, if necessary.
		if retEv != nil || retErr != nil {
			return
		}
		if os.ImplicitTxn.Get() {
			retEv, retPayload = ex.handleAutoCommit(ctx, stmt.AST)
			return
		}
	}()

	makeErrEvent := func(err error) (fsm.Event, fsm.EventPayload, error) {
		ev, payload := ex.makeErrEvent(err, stmt.AST)
		return ev, payload, nil
	}

	// Check if the statement should be parallelized. If not, we may need to
	// synchronize.
	parallelize, err := ex.maybeSynchronizeParallelStmts(ctx, stmt)
	if err != nil {
		return makeErrEvent(err)
	}

	switch s := stmt.AST.(type) {
	case *tree.BeginTransaction:
		// BEGIN is always an error when in the Open state. It's legitimate only in
		// the NoTxn state.
		return makeErrEvent(errTransactionInProgress)

	case *tree.CommitTransaction:
		// CommitTransaction is executed fully here; there's no plan for it.
		ev, payload := ex.commitSQLTransaction(ctx, stmt.AST)
		return ev, payload, nil

	case *tree.ReleaseSavepoint:
		if err := ex.validateSavepointName(s.Savepoint); err != nil {
			return makeErrEvent(err)
		}
		if !ex.machine.CurState().(stateOpen).RetryIntent.Get() {
			return makeErrEvent(errSavepointNotUsed)
		}

		// ReleaseSavepoint is executed fully here; there's no plan for it.
		ev, payload := ex.commitSQLTransaction(ctx, stmt.AST)
		res.ResetStmtType((*tree.CommitTransaction)(nil))
		return ev, payload, nil

	case *tree.RollbackTransaction:
		// RollbackTransaction is executed fully here; there's no plan for it.
		ev, payload := ex.rollbackSQLTransaction(ctx)
		return ev, payload, nil

	case *tree.Savepoint:
		// Ensure that the user isn't trying to run BEGIN; SAVEPOINT; SAVEPOINT;
		if ex.state.activeSavepointName != "" {
			err := fmt.Errorf("SAVEPOINT may not be nested")
			return makeErrEvent(err)
		}
		if err := ex.validateSavepointName(s.Name); err != nil {
			return makeErrEvent(err)
		}
		// We want to disallow SAVEPOINTs to be issued after a KV transaction has
		// started running. The client txn's statement count indicates how many
		// statements have been executed as part of this transaction. It is
		// desirable to allow metadata queries against vtables to proceed
		// before starting a SAVEPOINT for better ORM compatibility.
		// See also:
		// https://github.com/cockroachdb/cockroach/issues/15012
		meta := ex.state.mu.txn.GetTxnCoordMeta(ctx)
		if meta.CommandCount > 0 {
			err := fmt.Errorf("SAVEPOINT %s needs to be the first statement in a "+
				"transaction", RestartSavepointName)
			return makeErrEvent(err)
		}
		ex.state.activeSavepointName = s.Name
		// Note that Savepoint doesn't have a corresponding plan node.
		// This here is all the execution there is.
		return eventRetryIntentSet{}, nil /* payload */, nil

	case *tree.RollbackToSavepoint:
		if err := ex.validateSavepointName(s.Savepoint); err != nil {
			return makeErrEvent(err)
		}
		if !os.RetryIntent.Get() {
			return makeErrEvent(errSavepointNotUsed)
		}
		ex.state.activeSavepointName = ""

		res.ResetStmtType((*tree.Savepoint)(nil))
		return eventTxnRestart{}, nil /* payload */, nil

	case *tree.Prepare:
		// This is handling the SQL statement "PREPARE". See execPrepare for
		// handling of the protocol-level command for preparing statements.
		name := s.Name.String()
		if _, ok := ex.prepStmtsNamespace.prepStmts[name]; ok {
			err := pgerror.NewErrorf(
				pgerror.CodeDuplicatePreparedStatementError,
				"prepared statement %q already exists", name,
			)
			return makeErrEvent(err)
		}
		typeHints := make(tree.PlaceholderTypes, len(s.Types))
		for i, t := range s.Types {
			typeHints[strconv.Itoa(i+1)] = coltypes.CastTargetToDatumType(t)
		}
		if _, err := ex.addPreparedStmt(
			ctx, name, Statement{SQL: s.Statement.String(), AST: s.Statement}, typeHints,
		); err != nil {
			return makeErrEvent(err)
		}
		return nil, nil, nil

	case *tree.Execute:
		// Replace the `EXECUTE foo` statement with the prepared statement, and
		// continue execution below.
		name := s.Name.String()
		ps, ok := ex.prepStmtsNamespace.prepStmts[name]
		if !ok {
			err := pgerror.NewErrorf(
				pgerror.CodeInvalidSQLStatementNameError,
				"prepared statement %q does not exist", name,
			)
			return makeErrEvent(err)
		}
		var err error
		pinfo, err = fillInPlaceholders(ps.PreparedStatement, name, s.Params, ex.sessionData.SearchPath)
		if err != nil {
			return makeErrEvent(err)
		}

		stmt.AST = ps.Statement
		stmt.Prepared = ps.PreparedStatement
		stmt.ExpectedTypes = ps.Columns
		stmt.AnonymizedStr = ps.AnonymizedStr
		res.ResetStmtType(ps.Statement)

		// Check again if the statement should be parallelized.
		parallelize, err = ex.maybeSynchronizeParallelStmts(ctx, stmt)
		if err != nil {
			return makeErrEvent(err)
		}
	}

	// For regular statements (the ones that get to this point), we don't return
	// any event unless an an error happens.

	var p *planner
	stmtTS := ex.server.cfg.Clock.PhysicalTime()
	// Only run statements asynchronously through the parallelize queue if the
	// statements are parallelized and we're in a transaction. Parallelized
	// statements outside of a transaction are run synchronously with mocked
	// results, which has the same effect as running asynchronously but
	// immediately blocking.
	runInParallel := parallelize && !os.ImplicitTxn.Get()
	if runInParallel {
		// Create a new planner since we're executing in parallel.
		p = ex.newPlanner(ctx, ex.state.mu.txn, stmtTS)
	} else {
		// We're not executing in parallel; we'll use the cached planner.
		p = &ex.planner
		ex.resetPlanner(ctx, p, ex.state.mu.txn, stmtTS)
	}

	if os.ImplicitTxn.Get() {
		ts, err := p.isAsOf(stmt.AST, ex.server.cfg.Clock.Now())
		if err != nil {
			return makeErrEvent(err)
		}
		if ts != nil {
			p.semaCtx.AsOfTimestamp = ts
			ex.state.mu.txn.SetFixedTimestamp(ctx, *ts)
		}
	} else {
		// If we're in an explicit txn, we allow AOST but only if it matches with
		// the transaction's timestamp. This is useful for running AOST statements
		// using the InternalExecutor inside an external transaction; one might want
		// to do that to force p.avoidCachedDescriptors to be set below.
		ts, err := p.isAsOf(stmt.AST, ex.server.cfg.Clock.Now())
		if err != nil {
			return makeErrEvent(err)
		}
		if ts != nil {
			if *ts != ex.state.mu.txn.OrigTimestamp() {
				return makeErrEvent(errors.Errorf("inconsistent \"as of system time\" timestamp. Expected: %s. "+
					"Generally \"as of system time\" cannot be used inside a transaction.",
					ex.state.mu.txn.OrigTimestamp()))
			}
			p.semaCtx.AsOfTimestamp = ts
		}
	}

	p.semaCtx.Placeholders.Assign(pinfo)
	p.extendedEvalCtx.Placeholders = &p.semaCtx.Placeholders
	ex.phaseTimes[plannerStartExecStmt] = timeutil.Now()
	p.stmt = &stmt

	// TODO(andrei): Ideally we'd like to fork off a context for each individual
	// statement. But the heartbeat loop in TxnCoordSender currently assumes that
	// the context of the first operation in a txn batch lasts at least as long as
	// the transaction itself. Once that sender is able to distinguish between
	// statement and transaction contexts, we should move to per-statement
	// contexts.
	p.cancelChecker = sqlbase.NewCancelChecker(ctx)

	// constantMemAcc accounts for all constant folded values that are computed
	// prior to any rows being computed.
	constantMemAcc := p.EvalContext().Mon.MakeBoundAccount()
	p.EvalContext().ActiveMemAcc = &constantMemAcc
	defer constantMemAcc.Close(ctx)

	if runInParallel {
		cols, err := ex.execStmtInParallel(ctx, stmt, p, queryDone)
		queryDone = nil
		if err != nil {
			return makeErrEvent(err)
		}
		// Produce mocked out results for the query - the "zero value" of the
		// statement's result type:
		// - tree.Rows -> an empty set of rows
		// - tree.RowsAffected -> zero rows affected
		if err := ex.initStatementResult(ctx, res, stmt, cols); err != nil {
			return makeErrEvent(err)
		}
	} else {
		p.autoCommit = os.ImplicitTxn.Get() && !ex.server.cfg.TestingKnobs.DisableAutoCommit
		if err := ex.dispatchToExecutionEngine(ctx, stmt, p, res); err != nil {
			return nil, nil, err
		}
		if err := res.Err(); err != nil {
			return makeErrEvent(err)
		}

		txn := ex.state.mu.txn
		if !os.ImplicitTxn.Get() && txn.IsSerializablePushAndRefreshNotPossible() {
			rc, canAutoRetry := ex.getRewindTxnCapability()
			if canAutoRetry {
				ev := eventRetriableErr{
					IsCommit:     fsm.FromBool(isCommit(stmt.AST)),
					CanAutoRetry: fsm.FromBool(canAutoRetry),
				}
				txn.ManualRestart(ctx, ex.server.cfg.Clock.Now())
				payload := eventRetriableErrPayload{
					err: roachpb.NewHandledRetryableTxnError(
						"serializable transaction timestamp pushed (detected by connExecutor)",
						txn.ID(),
						// No updated transaction required; we've already manually updated our
						// client.Txn.
						roachpb.Transaction{},
					),
					rewCap: rc,
				}
				return ev, payload, nil
			}
		}
	}

	// No event was generated.
	return nil, nil, nil
}

// maybeSynchronizeParallelStmts check if the statement is parallelized or is
// independent from parallel execution. If neither of these cases are true, the
// method synchronizes parallel execution by letting it drain before returning.
func (ex *connExecutor) maybeSynchronizeParallelStmts(
	ctx context.Context, stmt Statement,
) (parallelize bool, _ error) {
	parallelize = tree.IsStmtParallelized(stmt.AST)
	_, independentFromParallelStmts := stmt.AST.(tree.IndependentFromParallelizedPriors)
	if !(parallelize || independentFromParallelStmts) {
		if err := ex.synchronizeParallelStmts(ctx); err != nil {
			return false, err
		}
	}
	return parallelize, nil
}

// checkTableTwoVersionInvariant checks whether any new table schema being
// modified written at a version V has only valid leases at version = V - 1.
// A transaction retry error is returned whenever the invariant is violated.
// Before returning the retry error the current transaction is
// rolled-back and the function waits until there are only outstanding
// leases on the current version. This affords the retry to succeed in the
// event that there are no other schema changes simultaneously contending with
// this txn.
//
// checkTableTwoVersionInvariant blocks until it's legal for the modified
// table descriptors (if any) to be committed.
// Reminder: a descriptor version v can only be written at a timestamp
// that's not covered by a lease on version v-2. So, if the current
// txn wants to write some updated descriptors, it needs
// to wait until all incompatible leases are revoked or expire. If
// incompatible leases exist, we'll block waiting for these leases to
// go away. Then, the transaction is restarted by generating a retriable error.
// Note that we're relying on the fact that the number of conflicting
// leases will only go down over time: no new conflicting leases can be
// created as of the time of this call because v-2 can't be leased once
// v-1 exists.
func (ex *connExecutor) checkTableTwoVersionInvariant(ctx context.Context) error {
	tables := ex.extraTxnState.tables.getTablesWithNewVersion()
	if tables == nil {
		return nil
	}
	txn := ex.state.mu.txn
	if txn.IsCommitted() {
		panic("transaction has already committed")
	}
	if !txn.CommitTimestampFixed() {
		panic("commit timestamp was not fixed")
	}

	// Release leases here for two reasons:
	// 1. If there are existing leases at version V-2 for a descriptor
	// being modified to version V being held the wait loop below that
	// waits on a cluster wide release of old version leases will hang
	// until these leases expire.
	// 2. Once this transaction commits, the schema changers run and
	// increment the version of the modified descriptors. If one of the
	// descriptors being modified has a lease being held the schema
	// changers will stall until the leases expire.
	//
	// The above two cases can be satified by releasing leases for both
	// cases explicitly, but we prefer to call it here and kill two birds
	// with one stone.
	//
	// It is safe to release leases even though the transaction hasn't yet
	// committed only because the transaction timestamp has been fixed using
	// CommitTimestamp().
	//
	// releaseLeases can fail to release a lease if the server is shutting
	// down. This is okay because it will result in the two cases mentioned
	// above simply hanging until the expiration time for the leases.
	ex.extraTxnState.tables.releaseLeases(ex.Ctx())

	count, err := CountLeases(ctx, ex.server.cfg.InternalExecutor, tables, txn.OrigTimestamp())
	if err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	// Restart the transaction so that it is able to replay itself at a newer timestamp
	// with the hope that the next time around there will be leases only at the current
	// version.
	retryErr := roachpb.NewHandledRetryableTxnError(
		fmt.Sprintf(
			`cannot publish new versions for tables: %v, old versions still in use`,
			tables),
		txn.ID(),
		*txn.Serialize(),
	)
	// We cleanup the transaction and create a new transaction after
	// waiting for the invariant to be satisfied because the wait time
	// might be extensive and intents can block out leases being created
	// on a descriptor.
	//
	// TODO(vivek): Change this to restart a txn while fixing #20526 . All the
	// table descriptor intents can be laid down here after the invariant
	// has been checked.
	userPriority := txn.UserPriority()
	// We cleanup the transaction and create a new transaction wait time
	// might be extensive and so we'd better get rid of all the intents.
	txn.CleanupOnError(ctx, retryErr)

	// Wait until all older version leases have been released or expired.
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		// Use the current clock time.
		now := ex.server.cfg.Clock.Now()
		count, err := CountLeases(ctx, ex.server.cfg.InternalExecutor, tables, now)
		if err != nil {
			return err
		}
		if count == 0 {
			break
		}
		if ex.server.cfg.SchemaChangerTestingKnobs.TwoVersionLeaseViolation != nil {
			ex.server.cfg.SchemaChangerTestingKnobs.TwoVersionLeaseViolation()
		}
	}

	// Create a new transaction to retry with a higher timestamp than the
	// timestamps used in the retry loop above.
	ex.state.mu.txn = client.NewTxn(ctx, ex.transitionCtx.db, ex.transitionCtx.nodeID, client.RootTxn)
	if err := ex.state.mu.txn.SetUserPriority(userPriority); err != nil {
		return err
	}
	return retryErr
}

// commitSQLTransaction executes a commit after the execution of a stmt,
// which can be any statement when executing a statement with an implicit
// transaction, or a COMMIT or RELEASE SAVEPOINT statement when using
// an explicit transaction.
func (ex *connExecutor) commitSQLTransaction(
	ctx context.Context, stmt tree.Statement,
) (fsm.Event, fsm.EventPayload) {
	ex.state.activeSavepointName = ""
	isRelease := false
	if _, ok := stmt.(*tree.ReleaseSavepoint); ok {
		isRelease = true
	}

	if err := ex.checkTableTwoVersionInvariant(ctx); err != nil {
		return ex.makeErrEvent(err, stmt)
	}

	if err := ex.state.mu.txn.Commit(ctx); err != nil {
		return ex.makeErrEvent(err, stmt)
	}

	if !isRelease {
		return eventTxnFinish{}, eventTxnFinishPayload{commit: true}
	}
	return eventTxnReleased{}, nil
}

// rollbackSQLTransaction executes a ROLLBACK statement: the KV transaction is
// rolled-back and an event is produced.
func (ex *connExecutor) rollbackSQLTransaction(ctx context.Context) (fsm.Event, fsm.EventPayload) {
	ex.state.activeSavepointName = ""
	if err := ex.state.mu.txn.Rollback(ctx); err != nil {
		log.Warningf(ctx, "txn rollback failed: %s", err)
	}
	// We're done with this txn.
	return eventTxnFinish{}, eventTxnFinishPayload{commit: false}
}

// execStmtInParallel executes a query asynchronously: the query will wait for
// all other currently executing async queries which are not independent, and
// then it will run.
// Note that planning needs to be done synchronously because it's needed by the
// query dependency analysis.
//
// A list of columns is returned for purposes of initializing the statement
// results. This will be nil if the query's result is of type "RowsAffected".
// If this method returns an error, the error is to be treated as a query
// execution error (in other words, it should be sent to the clients as part of
// the query's result, and the connection should be allowed to proceed with
// other queries).
//
// Args:
// queryDone: A cleanup function to be called when the execution is done.
func (ex *connExecutor) execStmtInParallel(
	ctx context.Context,
	stmt Statement,
	planner *planner,
	queryDone func(context.Context, RestrictedCommandResult),
) (sqlbase.ResultColumns, error) {
	params := runParams{
		ctx:             ctx,
		extendedEvalCtx: planner.ExtendedEvalContext(),
		p:               planner,
	}

	ex.sessionTracing.TracePlanStart(ctx, stmt.AST.StatementTag())
	err := planner.makePlan(ctx, stmt)
	ex.sessionTracing.TracePlanEnd(ctx, err)
	if err != nil {
		planner.maybeLogStatement(ctx, "par-prepare" /* lbl */, 0 /* rows */, err)
		return nil, err
	}

	// Prepare the result set, and determine the execution parameters.
	var cols sqlbase.ResultColumns
	if stmt.AST.StatementType() == tree.Rows {
		cols = planColumns(planner.curPlan.plan)
	}

	distributePlan := false
	// If we use the optimizer and we are in "local" mode, don't try to
	// distribute.
	if ex.sessionData.OptimizerMode != sessiondata.OptimizerLocal {
		planner.prepareForDistSQLSupportCheck()
		distributePlan = shouldDistributePlan(
			ctx, ex.sessionData.DistSQLMode, ex.server.cfg.DistSQLPlanner, planner.curPlan.plan)
	}

	ex.mu.Lock()
	queryMeta, ok := ex.mu.ActiveQueries[stmt.queryID]
	if !ok {
		ex.mu.Unlock()
		panic(fmt.Sprintf("query %d not in registry", stmt.queryID))
	}
	queryMeta.phase = executing
	queryMeta.isDistributed = distributePlan
	ex.mu.Unlock()

	if err := ex.parallelizeQueue.Add(params, func() error {
		res := &bufferedCommandResult{errOnly: true}

		defer queryDone(ctx, res)

		defer func() {
			planner.maybeLogStatement(ctx, "par-exec" /* lbl */, res.RowsAffected(), res.Err())
		}()

		if err := ex.initStatementResult(ctx, res, stmt, cols); err != nil {
			return err
		}

		if ex.server.cfg.TestingKnobs.BeforeExecute != nil {
			ex.server.cfg.TestingKnobs.BeforeExecute(ctx, stmt.String(), true /* isParallel */)
		}

		planner.statsCollector.PhaseTimes()[plannerStartExecStmt] = timeutil.Now()

		shouldUseDistSQL := shouldUseDistSQL(distributePlan, ex.sessionData.DistSQLMode)
		samplePlanDescription := ex.sampleLogicalPlanDescription(
			stmt, shouldUseDistSQL, false /* optimizerUsed */, planner)

		var flags planFlags
		if shouldUseDistSQL {
			if distributePlan {
				flags.Set(planFlagDistributed)
			} else {
				flags.Set(planFlagDistSQLLocal)
			}
			ex.sessionTracing.TraceExecStart(ctx, "distributed-parallel")
			err = ex.execWithDistSQLEngine(ctx, planner, stmt.AST.StatementType(), res, distributePlan)
		} else {
			ex.sessionTracing.TraceExecStart(ctx, "local-parallel")
			err = ex.execWithLocalEngine(ctx, planner, stmt.AST.StatementType(), res)
		}
		ex.sessionTracing.TraceExecEnd(ctx, res.Err(), res.RowsAffected())
		planner.statsCollector.PhaseTimes()[plannerEndExecStmt] = timeutil.Now()

		ex.recordStatementSummary(
			planner, stmt, samplePlanDescription, flags, ex.extraTxnState.autoRetryCounter,
			res.RowsAffected(), err,
		)
		if ex.server.cfg.TestingKnobs.AfterExecute != nil {
			ex.server.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), res.Err())
		}

		if err != nil {
			// I think this can't happen; if it does, it's unclear how to react when a
			// this "connection" is toast.
			log.Warningf(ctx, "Connection error from the parallel queue. How can that "+
				"be? err: %s", err)
			res.SetError(err)
			return err
		}
		return res.Err()
	}); err != nil {
		planner.maybeLogStatement(ctx, "par-queue" /* lbl */, 0 /* rows */, err)
		return nil, err
	}

	return cols, nil
}

func enhanceErrWithCorrelation(err error, isCorrelated bool) {
	if err == nil || !isCorrelated {
		return
	}

	// If the query was found to be correlated by the new-gen
	// optimizer, but the optimizer decided to give up (e.g. because
	// of some feature it does not support), in most cases the
	// heuristic planner will choke on the correlation with an
	// unhelpful "table/column not defined" error.
	//
	// ("In most cases" because the heuristic planner does support
	// *some* correlation, specifically that of SRFs in projections.)
	//
	// To help the user understand what is going on, we enhance these
	// error message here when correlation has been found.
	//
	// We cannot be more assertive/definite in the text of the hint
	// (e.g. by replacing the error entirely by "correlated queries are
	// not supported") because perhaps there was an actual mistake in
	// the query in addition to the unsupported correlation, and we also
	// want to give a chance to the user to fix mistakes.
	if pqErr, ok := err.(*pgerror.Error); ok {
		if pqErr.Code == pgerror.CodeUndefinedColumnError ||
			pqErr.Code == pgerror.CodeUndefinedTableError {
			_ = pqErr.SetHintf("some correlated subqueries are not supported yet - see %s",
				"https://github.com/cockroachdb/cockroach/issues/3288")
		}
	}
}

// dispatchToExecutionEngine executes the statement, writes the result to res
// and returns an event for the connection's state machine.
//
// If an error is returned, the connection needs to stop processing queries.
// Query execution errors are written to res; they are not returned; it is
// expected that the caller will inspect res and react to query errors by
// producing an appropriate state machine event.
func (ex *connExecutor) dispatchToExecutionEngine(
	ctx context.Context, stmt Statement, planner *planner, res RestrictedCommandResult,
) error {
	ex.sessionTracing.TracePlanStart(ctx, stmt.AST.StatementTag())
	planner.statsCollector.PhaseTimes()[plannerStartLogicalPlan] = timeutil.Now()

	flags, err := planner.optionallyUseOptimizer(ctx, ex.sessionData, stmt)
	if err == nil && !flags.IsSet(planFlagOptUsed) {
		isCorrelated := planner.curPlan.isCorrelated
		log.VEventf(ctx, 1, "query is correlated: %v", isCorrelated)
		// Fallback if the optimizer was not enabled or used.
		err = planner.makePlan(ctx, stmt)
		enhanceErrWithCorrelation(err, isCorrelated)
	}
	defer planner.curPlan.close(ctx)

	defer func() { planner.maybeLogStatement(ctx, "exec", res.RowsAffected(), res.Err()) }()

	planner.statsCollector.PhaseTimes()[plannerEndLogicalPlan] = timeutil.Now()
	ex.sessionTracing.TracePlanEnd(ctx, err)
	if err != nil {
		res.SetError(err)
		return nil
	}

	var cols sqlbase.ResultColumns
	if stmt.AST.StatementType() == tree.Rows {
		cols = planColumns(planner.curPlan.plan)
	}
	if err := ex.initStatementResult(ctx, res, stmt, cols); err != nil {
		res.SetError(err)
		return nil
	}

	ex.sessionTracing.TracePlanCheckStart(ctx)
	distributePlan := false
	// If we use the optimizer and we are in "local" mode, don't try to
	// distribute.
	if ex.sessionData.OptimizerMode != sessiondata.OptimizerLocal {
		planner.prepareForDistSQLSupportCheck()
		distributePlan = shouldDistributePlan(
			ctx, ex.sessionData.DistSQLMode, ex.server.cfg.DistSQLPlanner, planner.curPlan.plan)
	}
	ex.sessionTracing.TracePlanCheckEnd(ctx, nil, distributePlan)

	if ex.server.cfg.TestingKnobs.BeforeExecute != nil {
		ex.server.cfg.TestingKnobs.BeforeExecute(ctx, stmt.String(), false /* isParallel */)
	}

	planner.statsCollector.PhaseTimes()[plannerStartExecStmt] = timeutil.Now()

	ex.mu.Lock()
	queryMeta, ok := ex.mu.ActiveQueries[stmt.queryID]
	if !ok {
		ex.mu.Unlock()
		panic(fmt.Sprintf("query %d not in registry", stmt.queryID))
	}
	queryMeta.phase = executing
	queryMeta.isDistributed = distributePlan
	ex.mu.Unlock()

	shouldUseDistSQL := shouldUseDistSQL(distributePlan, ex.sessionData.DistSQLMode)
	samplePlanDescription := ex.sampleLogicalPlanDescription(stmt, shouldUseDistSQL, flags.IsSet(planFlagOptUsed), planner)

	if shouldUseDistSQL {
		if distributePlan {
			flags.Set(planFlagDistributed)
		} else {
			flags.Set(planFlagDistSQLLocal)
		}
		ex.sessionTracing.TraceExecStart(ctx, "distributed")
		err = ex.execWithDistSQLEngine(ctx, planner, stmt.AST.StatementType(), res, distributePlan)
	} else {
		ex.sessionTracing.TraceExecStart(ctx, "local")
		err = ex.execWithLocalEngine(ctx, planner, stmt.AST.StatementType(), res)
	}
	ex.sessionTracing.TraceExecEnd(ctx, res.Err(), res.RowsAffected())
	planner.statsCollector.PhaseTimes()[plannerEndExecStmt] = timeutil.Now()
	if err != nil {
		return err
	}
	ex.recordStatementSummary(
		planner, stmt, samplePlanDescription, flags,
		ex.extraTxnState.autoRetryCounter, res.RowsAffected(), res.Err(),
	)
	if ex.server.cfg.TestingKnobs.AfterExecute != nil {
		ex.server.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), res.Err())
	}

	return nil
}

// sampleLogicalPlanDescription returns a serialized representation of a statement's logical plan.
// The returned ExplainTreePlanNode will be nil if plan should not be sampled.
func (ex *connExecutor) sampleLogicalPlanDescription(
	stmt Statement, useDistSQL bool, optimizerUsed bool, planner *planner,
) *roachpb.ExplainTreePlanNode {
	if !sampleLogicalPlans.Get(&ex.appStats.st.SV) {
		return nil
	}

	if ex.saveLogicalPlanDescription(stmt, useDistSQL, optimizerUsed) {
		return planToTree(context.Background(), planner.curPlan)
	}
	return nil
}

// saveLogicalPlanDescription returns if we should save this as a sample logical plan
// for its corresponding fingerprint. We use `saveFingerprintPlanOnceEvery`
// to assess how frequently to sample logical plans.
func (ex *connExecutor) saveLogicalPlanDescription(
	stmt Statement, useDistSQL bool, optimizerUsed bool,
) bool {
	stats := ex.appStats.getStatsForStmt(stmt, useDistSQL, optimizerUsed, nil, false /* createIfNonexistent */)
	if stats == nil {
		// Save logical plan the first time we see new statement fingerprint.
		return true
	}
	stats.Lock()
	defer stats.Unlock()

	return stats.data.Count%saveFingerprintPlanOnceEvery == 0
}

// canFallbackFromOpt returns whether we can fallback on the heuristic planner
// when the optimizer hits an error.
func canFallbackFromOpt(err error, optMode sessiondata.OptimizerMode, stmt Statement) bool {
	pgerr, ok := err.(*pgerror.Error)
	if !ok || pgerr.Code != pgerror.CodeFeatureNotSupportedError {
		// We only fallback on "feature not supported" errors.
		return false
	}

	if optMode == sessiondata.OptimizerAlways {
		// In Always mode we never fallback, with one exception: SET commands (or
		// else we can't switch to another mode).
		_, isSetVar := stmt.AST.(*tree.SetVar)
		return isSetVar
	}

	// If the statement is EXPLAIN (OPT), then don't fallback (we want to return
	// the error, not show a plan from the heuristic planner).
	// TODO(radu): this is hacky and doesn't handle an EXPLAIN (OPT) inside
	// a larger query.
	if e, ok := stmt.AST.(*tree.Explain); ok {
		if opts, err := e.ParseOptions(); err == nil && opts.Mode == tree.ExplainOpt {
			return false
		}
	}
	return true
}

// execWithLocalEngine runs a plan using the local (non-distributed) SQL
// engine.
// If an error is returned, the connection needs to stop processing queries.
// Such errors are also written to res.
// Query execution errors are written to res; they are not returned.
func (ex *connExecutor) execWithLocalEngine(
	ctx context.Context, planner *planner, stmtType tree.StatementType, res RestrictedCommandResult,
) error {
	// Create a BoundAccount to track the memory usage of each row.
	rowAcc := planner.extendedEvalCtx.Mon.MakeBoundAccount()
	planner.extendedEvalCtx.ActiveMemAcc = &rowAcc
	defer rowAcc.Close(ctx)

	params := runParams{
		ctx:             ctx,
		extendedEvalCtx: &planner.extendedEvalCtx,
		p:               planner,
	}

	if err := planner.curPlan.start(params); err != nil {
		res.SetError(err)
		return nil
	}

	switch stmtType {
	case tree.RowsAffected:
		count, err := countRowsAffected(params, planner.curPlan.plan)
		if err != nil {
			res.SetError(err)
			return nil
		}
		res.IncrementRowsAffected(count)
		return nil
	case tree.Rows:
		consumeCtx, cleanup := ex.sessionTracing.TraceExecConsume(ctx)
		defer cleanup()

		var commErr error
		queryErr := ex.forEachRow(params, planner.curPlan.plan, func(values tree.Datums) error {
			for _, val := range values {
				if err := checkResultType(val.ResolvedType()); err != nil {
					return err
				}
			}
			ex.sessionTracing.TraceExecRowsResult(consumeCtx, values)
			commErr = res.AddRow(consumeCtx, values)
			return commErr
		})
		if commErr != nil {
			res.SetError(commErr)
			return commErr
		}
		if queryErr != nil {
			res.SetError(queryErr)
		}
		return nil
	default:
		// Calling StartPlan is sufficient for other statement types.
		return nil
	}
}

// execWithDistSQLEngine converts a plan to a distributed SQL physical plan and
// runs it.
// If an error is returned, the connection needs to stop processing queries.
// Query execution errors are written to res; they are not returned.
func (ex *connExecutor) execWithDistSQLEngine(
	ctx context.Context,
	planner *planner,
	stmtType tree.StatementType,
	res RestrictedCommandResult,
	distribute bool,
) error {
	recv := MakeDistSQLReceiver(
		ctx, res, stmtType,
		ex.server.cfg.RangeDescriptorCache, ex.server.cfg.LeaseHolderCache,
		planner.txn,
		func(ts hlc.Timestamp) {
			_ = ex.server.cfg.Clock.Update(ts)
		},
		&ex.sessionTracing,
	)
	defer recv.Release()

	evalCtx := planner.ExtendedEvalContext()
	var planCtx *PlanningCtx
	if distribute {
		planCtx = ex.server.cfg.DistSQLPlanner.NewPlanningCtx(ctx, evalCtx, planner.txn)
	} else {
		planCtx = ex.server.cfg.DistSQLPlanner.newLocalPlanningCtx(ctx, evalCtx)
	}
	planCtx.isLocal = !distribute
	planCtx.planner = planner
	planCtx.stmtType = recv.stmtType

	if len(planner.curPlan.subqueryPlans) != 0 {
		evalCtxFactory := func() *extendedEvalContext {
			evalCtx := ex.evalCtx(ctx, planner, planner.ExtendedEvalContext().StmtTimestamp)
			evalCtx.Placeholders = &planner.semaCtx.Placeholders
			return &evalCtx
		}
		if !ex.server.cfg.DistSQLPlanner.PlanAndRunSubqueries(ctx, planner, evalCtxFactory, planner.curPlan.subqueryPlans, recv, distribute) {
			return recv.commErr
		}
	}

	// We pass in whether or not we wanted to distribute this plan, which tells
	// the planner whether or not to plan remote table readers.
	ex.server.cfg.DistSQLPlanner.PlanAndRun(
		ctx, evalCtx, planCtx, planner.txn, planner.curPlan.plan, recv)
	return recv.commErr
}

// forEachRow calls the provided closure for each successful call to
// planNode.Next with planNode.Values, making sure to properly track memory
// usage.
//
// f is not allowed to hold on to the row slice. It needs to make a copy if it
// want to use the memory later.
//
// Errors returned by this method are to be considered query errors. If the
// caller wants to handle some errors within the callback differently, it has to
// capture those itself.
func (ex *connExecutor) forEachRow(params runParams, p planNode, f func(tree.Datums) error) error {
	next, err := p.Next(params)
	for ; next; next, err = p.Next(params) {
		// If we're tracking memory, clear the previous row's memory account.
		if params.extendedEvalCtx.ActiveMemAcc != nil {
			params.extendedEvalCtx.ActiveMemAcc.Clear(params.ctx)
		}

		if err := f(p.Values()); err != nil {
			return err
		}
	}
	return err
}

// execStmtInNoTxnState "executes" a statement when no transaction is in scope.
// For anything but BEGIN, this method doesn't actually execute the statement;
// it just returns an Event that will generate a transaction. The statement will
// then be executed again, but this time in the Open state (implicit txn).
//
// Note that eventTxnStart, which is generally returned by this method, causes
// the state to change and previous results to be flushed, but for implicit txns
// the cursor is not advanced. This means that the statement will run again in
// stateOpen, at each point its results will also be flushed.
func (ex *connExecutor) execStmtInNoTxnState(
	ctx context.Context, stmt Statement,
) (fsm.Event, fsm.EventPayload) {
	switch s := stmt.AST.(type) {
	case *tree.BeginTransaction:
		ex.incrementStmtCounter(stmt)
		pri, err := priorityToProto(s.Modes.UserPriority)
		if err != nil {
			return ex.makeErrEvent(err, s)
		}

		return eventTxnStart{ImplicitTxn: fsm.False},
			makeEventTxnStartPayload(
				pri, ex.readWriteModeWithSessionDefault(s.Modes.ReadWriteMode),
				ex.server.cfg.Clock.PhysicalTime(),
				ex.transitionCtx)
	case *tree.CommitTransaction, *tree.ReleaseSavepoint,
		*tree.RollbackTransaction, *tree.SetTransaction, *tree.Savepoint:
		return ex.makeErrEvent(errNoTransactionInProgress, stmt.AST)
	default:
		mode := tree.ReadWrite
		if ex.sessionData.DefaultReadOnly {
			mode = tree.ReadOnly
		}
		return eventTxnStart{ImplicitTxn: fsm.True},
			makeEventTxnStartPayload(
				roachpb.NormalUserPriority,
				mode,
				ex.server.cfg.Clock.PhysicalTime(),
				ex.transitionCtx)
	}
}

// execStmtInAbortedState executes a statement in a txn that's in state
// Aborted or RestartWait. All statements result in error events except:
// - COMMIT / ROLLBACK: aborts the current transaction.
// - ROLLBACK TO SAVEPOINT / SAVEPOINT: reopens the current transaction,
//   allowing it to be retried.
func (ex *connExecutor) execStmtInAbortedState(
	ctx context.Context, stmt Statement, res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload) {
	_, inRestartWait := ex.machine.CurState().(stateRestartWait)

	// TODO(andrei/cuongdo): Figure out what statements to count here.
	switch s := stmt.AST.(type) {
	case *tree.CommitTransaction, *tree.RollbackTransaction:
		if inRestartWait {
			ev, payload := ex.rollbackSQLTransaction(ctx)
			return ev, payload
		}
		ex.state.activeSavepointName = ""

		// Note: Postgres replies to COMMIT of failed txn with "ROLLBACK" too.
		res.ResetStmtType((*tree.RollbackTransaction)(nil))

		return eventTxnFinish{}, eventTxnFinishPayload{commit: false}
	case *tree.RollbackToSavepoint, *tree.Savepoint:
		// We accept both the "ROLLBACK TO SAVEPOINT cockroach_restart" and the
		// "SAVEPOINT cockroach_restart" commands to indicate client intent to
		// retry a transaction in a RestartWait state.
		var spName tree.Name
		var isRollback bool
		switch n := s.(type) {
		case *tree.RollbackToSavepoint:
			spName = n.Savepoint
			isRollback = true
		case *tree.Savepoint:
			spName = n.Name
		default:
			panic("unreachable")
		}
		// If the user issued a SAVEPOINT in the abort state, validate
		// as though there were no active savepoint.
		if !isRollback {
			ex.state.activeSavepointName = ""
		}
		if err := ex.validateSavepointName(spName); err != nil {
			ev := eventNonRetriableErr{IsCommit: fsm.False}
			payload := eventNonRetriableErrPayload{
				err: err,
			}
			return ev, payload
		}
		// Either clear or reset the current savepoint name so that
		// ROLLBACK TO; SAVEPOINT; works.
		if isRollback {
			ex.state.activeSavepointName = ""
		} else {
			ex.state.activeSavepointName = spName
		}

		if !(inRestartWait || ex.machine.CurState().(stateAborted).RetryIntent.Get()) {
			ev := eventNonRetriableErr{IsCommit: fsm.False}
			payload := eventNonRetriableErrPayload{
				err: errSavepointNotUsed,
			}
			return ev, payload
		}

		res.ResetStmtType((*tree.RollbackTransaction)(nil))

		if inRestartWait {
			return eventTxnRestart{}, nil
		}
		// We accept ROLLBACK TO SAVEPOINT even after non-retryable errors to make
		// it easy for client libraries that want to indiscriminately issue
		// ROLLBACK TO SAVEPOINT after every error and possibly follow it with a
		// ROLLBACK and also because we accept ROLLBACK TO SAVEPOINT in the Open
		// state, so this is consistent.
		// We start a new txn with the same sql timestamp and isolation as the
		// current one.

		ev := eventTxnStart{
			ImplicitTxn: fsm.False,
		}
		rwMode := tree.ReadWrite
		if ex.state.readOnly {
			rwMode = tree.ReadOnly
		}
		payload := makeEventTxnStartPayload(
			ex.state.priority, rwMode, ex.state.sqlTimestamp, ex.transitionCtx)
		return ev, payload
	default:
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		if inRestartWait {
			payload := eventNonRetriableErrPayload{
				err: sqlbase.NewTransactionAbortedError(
					"Expected \"ROLLBACK TO SAVEPOINT COCKROACH_RESTART\"" /* customMsg */),
			}
			return ev, payload
		}
		payload := eventNonRetriableErrPayload{
			err: sqlbase.NewTransactionAbortedError("" /* customMsg */),
		}
		return ev, payload
	}
}

// execStmtInCommitWaitState executes a statement in a txn that's in state
// CommitWait.
// Everything but COMMIT/ROLLBACK causes errors. ROLLBACK is treated like COMMIT.
func (ex *connExecutor) execStmtInCommitWaitState(
	stmt Statement, res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload) {
	ex.incrementStmtCounter(stmt)
	switch stmt.AST.(type) {
	case *tree.CommitTransaction, *tree.RollbackTransaction:
		// Reply to a rollback with the COMMIT tag, by analogy to what we do when we
		// get a COMMIT in state Aborted.
		res.ResetStmtType((*tree.CommitTransaction)(nil))
		return eventTxnFinish{}, eventTxnFinishPayload{commit: true}
	default:
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{
			err: sqlbase.NewTransactionCommittedError(),
		}
		return ev, payload
	}
}

// runObserverStatement executes the given observer statement.
//
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) runObserverStatement(
	ctx context.Context, stmt Statement, res RestrictedCommandResult,
) error {
	switch sqlStmt := stmt.AST.(type) {
	case *tree.ShowTransactionStatus:
		return ex.runShowTransactionState(ctx, res)
	case *tree.ShowSyntax:
		return ex.runShowSyntax(ctx, sqlStmt.Statement, res)
	case *tree.SetTracing:
		ex.runSetTracing(ctx, sqlStmt, res)
		return nil
	default:
		res.SetError(pgerror.NewAssertionErrorf("unrecognized observer statement type %T", stmt.AST))
		return nil
	}
}

// runShowSyntax executes a SHOW SYNTAX <stmt> query.
//
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) runShowSyntax(
	ctx context.Context, stmt string, res RestrictedCommandResult,
) error {
	res.SetColumns(ctx, sqlbase.ResultColumns{
		{Name: "field", Typ: types.String},
		{Name: "message", Typ: types.String},
	})
	var commErr error
	if err := runShowSyntax(ctx, stmt,
		func(ctx context.Context, field, msg string) error {
			commErr = res.AddRow(ctx, tree.Datums{tree.NewDString(field), tree.NewDString(msg)})
			return nil
		},
		telemetry.RecordError, /* reportErr */
	); err != nil {
		res.SetError(err)
	}
	return commErr
}

// runShowTransactionState executes a SHOW TRANSACTION STATUS statement.
//
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) runShowTransactionState(
	ctx context.Context, res RestrictedCommandResult,
) error {
	res.SetColumns(ctx, sqlbase.ResultColumns{{Name: "TRANSACTION STATUS", Typ: types.String}})

	state := fmt.Sprintf("%s", ex.machine.CurState())
	return res.AddRow(ctx, tree.Datums{tree.NewDString(state)})
}

func (ex *connExecutor) runSetTracing(
	ctx context.Context, n *tree.SetTracing, res RestrictedCommandResult,
) {
	if len(n.Values) == 0 {
		res.SetError(fmt.Errorf("set tracing missing argument"))
		return
	}

	modes := make([]string, len(n.Values))
	for i, v := range n.Values {
		v = unresolvedNameToStrVal(v)
		strVal, ok := v.(*tree.StrVal)
		if !ok {
			res.SetError(fmt.Errorf("expected string for set tracing argument, not %T", v))
			return
		}
		modes[i] = strVal.RawString()
	}

	if err := ex.enableTracing(modes); err != nil {
		res.SetError(err)
	}
}

func (ex *connExecutor) enableTracing(modes []string) error {
	traceKV := false
	recordingType := tracing.SnowballRecording
	enableMode := true
	showResults := false

	for _, s := range modes {
		switch strings.ToLower(s) {
		case "results":
			showResults = true
		case "on":
			enableMode = true
		case "off":
			enableMode = false
		case "kv":
			traceKV = true
		case "local":
			recordingType = tracing.SingleNodeRecording
		case "cluster":
			recordingType = tracing.SnowballRecording
		default:
			return errors.Errorf("set tracing: unknown mode %q", s)
		}
	}
	if !enableMode {
		return ex.sessionTracing.StopTracing()
	}
	return ex.sessionTracing.StartTracing(recordingType, traceKV, showResults)
}

// addActiveQuery adds a running query to the list of running queries.
//
// It returns a cleanup function that needs to be run when the query is no
// longer executing. NOTE(andrei): As of Feb 2018, "executing" does not imply
// that the results have been delivered to the client.
func (ex *connExecutor) addActiveQuery(
	queryID ClusterWideID, stmt tree.Statement, cancelFun context.CancelFunc,
) func() {

	_, hidden := stmt.(tree.HiddenFromShowQueries)
	qm := &queryMeta{
		start:         ex.phaseTimes[sessionQueryReceived],
		stmt:          stmt,
		phase:         preparing,
		isDistributed: false,
		ctxCancel:     cancelFun,
		hidden:        hidden,
	}
	ex.mu.Lock()
	ex.mu.ActiveQueries[queryID] = qm
	ex.mu.Unlock()
	return func() {
		ex.mu.Lock()
		_, ok := ex.mu.ActiveQueries[queryID]
		if !ok {
			ex.mu.Unlock()
			panic(fmt.Sprintf("query %d missing from ActiveQueries", queryID))
		}
		delete(ex.mu.ActiveQueries, queryID)
		ex.mu.LastActiveQuery = qm.stmt

		ex.mu.Unlock()
	}
}

// handleAutoCommit commits the KV transaction if it hasn't been committed
// already.
//
// It's possible that the statement constituting the implicit txn has already
// committed it (in case it tried to run as a 1PC). This method detects that
// case.
// NOTE(andrei): It bothers me some that we're peeking at txn to figure out
// whether we committed or not, where SQL could already know that - individual
// statements could report this back through the Event.
//
// Args:
// stmt: The statement that we just ran.
func (ex *connExecutor) handleAutoCommit(
	ctx context.Context, stmt tree.Statement,
) (fsm.Event, fsm.EventPayload) {
	txn := ex.state.mu.txn
	if txn.IsCommitted() {
		return eventTxnFinish{}, eventTxnFinishPayload{commit: true}
	}

	if knob := ex.server.cfg.TestingKnobs.BeforeAutoCommit; knob != nil {
		if err := knob(ctx, stmt.String()); err != nil {
			return ex.makeErrEvent(err, stmt)
		}
	}

	ev, payload := ex.commitSQLTransaction(ctx, stmt)
	var err error
	if perr, ok := payload.(payloadWithError); ok {
		err = perr.errorCause()
	}
	log.VEventf(ctx, 2, "AutoCommit. err: %v", err)
	return ev, payload
}

func (ex *connExecutor) incrementStmtCounter(stmt Statement) {
	ex.metrics.StatementCounters.incrementCount(stmt.AST)
}

// validateSavepointName validates that it is that the provided ident
// matches the active savepoint name, begins with RestartSavepointName,
// or that force_savepoint_restart==true. We accept everything with the
// desired prefix because at least the C++ libpqxx appends sequence
// numbers to the savepoint name specified by the user.
func (ex *connExecutor) validateSavepointName(savepoint tree.Name) error {
	if ex.state.activeSavepointName != "" {
		if savepoint == ex.state.activeSavepointName {
			return nil
		}
		return pgerror.NewErrorf(pgerror.CodeInvalidSavepointSpecificationError,
			`SAVEPOINT %q is in use`, tree.ErrString(&ex.state.activeSavepointName))
	}
	if !ex.sessionData.ForceSavepointRestart && !strings.HasPrefix(string(savepoint), RestartSavepointName) {
		return pgerror.UnimplementedWithIssueHintError(10735,
			"SAVEPOINT not supported except for "+RestartSavepointName,
			"Retryable transactions with arbitrary SAVEPOINT names can be enabled "+
				"with SET force_savepoint_restart=true")
	}
	return nil
}
