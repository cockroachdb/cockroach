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
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

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
	// Run observer statements in a separate code path; their execution does not
	// depend on the current transaction state.
	if _, ok := stmt.AST.(tree.ObserverStatement); ok {
		err := ex.runObserverStatement(ctx, stmt, res)
		return nil, nil, err
	}

	if log.V(2) || logStatementsExecuteEnabled.Get(&ex.server.cfg.Settings.SV) ||
		log.HasSpanOrEvent(ctx) {
		log.VEventf(ctx, 2, "executing: %s in state: %s", stmt, ex.machine.CurState())
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
	case stateAborted, stateRestartWait:
		ev, payload = ex.execStmtInAbortedState(ctx, stmt, res)
	case stateCommitWait:
		ev, payload = ex.execStmtInCommitWaitState(stmt, res)
	default:
		panic(fmt.Sprintf("unexpected txn state: %#v", ex.machine.CurState()))
	}

	return ev, payload, err
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
	ex.server.StatementCounters.incrementCount(stmt.AST)
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
			autoCommitErr := ex.handleAutoCommit(ctx, stmt.AST)
			if autoCommitErr != nil {
				retEv, retPayload = ex.makeErrEvent(autoCommitErr, stmt.AST)
				return
			}
			retEv = eventTxnFinish{}
			retPayload = eventTxnFinishPayload{commit: true}
			return
		}
	}()

	makeErrEvent := func(err error) (fsm.Event, fsm.EventPayload, error) {
		ev, payload := ex.makeErrEvent(err, stmt.AST)
		return ev, payload, nil
	}

	// Check if the statement is parallelized or is independent from parallel
	// execution. If neither of these cases are true, we need to synchronize
	// parallel execution by letting it drain before we can begin executing stmt.
	parallelize := IsStmtParallelized(stmt)
	_, independentFromParallelStmts := stmt.AST.(tree.IndependentFromParallelizedPriors)
	if !(parallelize || independentFromParallelStmts) {
		if err := ex.synchronizeParallelStmts(ctx); err != nil {
			return makeErrEvent(err)
		}
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
		if err := tree.ValidateRestartCheckpoint(s.Savepoint); err != nil {
			return makeErrEvent(err)
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
		if err := tree.ValidateRestartCheckpoint(s.Name); err != nil {
			return makeErrEvent(err)
		}
		// We want to disallow SAVEPOINTs to be issued after a transaction has
		// started running. The client txn's statement count indicates how many
		// statements have been executed as part of this transaction.
		if ex.state.mu.txn.GetTxnCoordMeta().CommandCount > 0 {
			err := fmt.Errorf("SAVEPOINT %s needs to be the first statement in a "+
				"transaction", tree.RestartSavepointName)
			return makeErrEvent(err)
		}
		// Note that Savepoint doesn't have a corresponding plan node.
		// This here is all the execution there is.
		return eventRetryIntentSet{}, nil /* payload */, nil

	case *tree.RollbackToSavepoint:
		if err := tree.ValidateRestartCheckpoint(s.Savepoint); err != nil {
			return makeErrEvent(err)
		}
		if !os.RetryIntent.Get() {
			err := fmt.Errorf("SAVEPOINT %s has not been used", tree.RestartSavepointName)
			return makeErrEvent(err)
		}

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
		if _, err := ex.addPreparedStmt(ctx, name, Statement{AST: s.Statement}, typeHints); err != nil {
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
		stmt.ExpectedTypes = ps.Columns
		stmt.AnonymizedStr = ps.AnonymizedStr
		res.ResetStmtType(ps.Statement)
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
		ts, err := isAsOf(stmt.AST, p.EvalContext(), ex.server.cfg.Clock.Now())
		if err != nil {
			return makeErrEvent(err)
		}
		if ts != nil {
			p.asOfSystemTime = true
			p.avoidCachedDescriptors = true
			ex.state.mu.txn.SetFixedTimestamp(ctx, *ts)
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
				txn.Proto().Restart(0 /* userPriority */, 0 /* upgradePriority */, ex.server.cfg.Clock.Now())
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

// commitSQLTransaction executes a COMMIT or RELEASE SAVEPOINT statement. The
// transaction is committed and the statement result is written to res. res is
// closed.
func (ex *connExecutor) commitSQLTransaction(
	ctx context.Context, stmt tree.Statement,
) (fsm.Event, fsm.EventPayload) {
	commitType := commit
	if _, ok := stmt.(*tree.ReleaseSavepoint); ok {
		commitType = release
	}

	if err := ex.state.mu.txn.Commit(ctx); err != nil {
		return ex.makeErrEvent(err, stmt)
	}

	switch commitType {
	case commit:
		return eventTxnFinish{}, eventTxnFinishPayload{commit: true}
	case release:
		return eventTxnReleased{}, nil
	}
	panic("unreached")
}

// rollbackSQLTransaction executes a ROLLBACK statement: the KV transaction is
// rolled-back and an event is produced.
func (ex *connExecutor) rollbackSQLTransaction(ctx context.Context) (fsm.Event, fsm.EventPayload) {
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
//
// TODO(nvanbenschoten): We do not currently support parallelizing distributed SQL
// queries, so this method can only be used with local SQL.
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

	if err := planner.makePlan(ctx, stmt); err != nil {
		planner.maybeLogStatement(ctx, "par-prepare" /* lbl */, 0 /* rows */, err)
		return nil, err
	}

	// Prepare the result set, and determine the execution parameters.
	var cols sqlbase.ResultColumns
	if stmt.AST.StatementType() == tree.Rows {
		cols = planColumns(planner.curPlan.plan)
	}

	ex.mu.Lock()
	queryMeta, ok := ex.mu.ActiveQueries[stmt.queryID]
	if !ok {
		ex.mu.Unlock()
		panic(fmt.Sprintf("query %d not in registry", stmt.queryID))
	}
	queryMeta.phase = executing
	queryMeta.isDistributed = false
	ex.mu.Unlock()

	if err := ex.parallelizeQueue.Add(params, func() error {
		res := &errOnlyRestrictedCommandResult{}

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
		err := ex.execWithLocalEngine(ctx, planner, stmt.AST.StatementType(), res)

		planner.statsCollector.PhaseTimes()[plannerEndExecStmt] = timeutil.Now()
		recordStatementSummary(
			planner, stmt, false /* distSQLUsed*/, ex.extraTxnState.autoRetryCounter,
			res.RowsAffected(), err, &ex.server.EngineMetrics)
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

	planner.statsCollector.PhaseTimes()[plannerStartLogicalPlan] = timeutil.Now()
	useOptimizer := shouldUseOptimizer(ex.sessionData.OptimizerMode, stmt)
	var err error
	if useOptimizer {
		// Experimental path (disabled by default).
		err = planner.makeOptimizerPlan(ctx, stmt)
	} else {
		err = planner.makePlan(ctx, stmt)
	}

	defer func() { planner.maybeLogStatement(ctx, "exec", res.RowsAffected(), res.Err()) }()

	planner.statsCollector.PhaseTimes()[plannerEndLogicalPlan] = timeutil.Now()
	if err != nil {
		res.SetError(err)
		return nil
	}
	defer planner.curPlan.close(ctx)

	var cols sqlbase.ResultColumns
	if stmt.AST.StatementType() == tree.Rows {
		cols = planColumns(planner.curPlan.plan)
	}
	if err := ex.initStatementResult(ctx, res, stmt, cols); err != nil {
		res.SetError(err)
		return nil
	}

	useDistSQL := false
	// TODO(radu): for now, we restrict the optimizer to local execution.
	if !useOptimizer {
		useDistSQL, err = shouldUseDistSQL(
			ctx, ex.sessionData.DistSQLMode, ex.server.cfg.DistSQLPlanner, planner)
		if err != nil {
			res.SetError(err)
			return nil
		}
	}

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
	queryMeta.isDistributed = useDistSQL
	ex.mu.Unlock()

	if useDistSQL {
		err = ex.execWithDistSQLEngine(ctx, planner, stmt.AST.StatementType(), res)
	} else {
		err = ex.execWithLocalEngine(ctx, planner, stmt.AST.StatementType(), res)
	}
	planner.statsCollector.PhaseTimes()[plannerEndExecStmt] = timeutil.Now()
	if err != nil {
		return err
	}
	recordStatementSummary(
		planner, stmt, useDistSQL, ex.extraTxnState.autoRetryCounter,
		res.RowsAffected(), res.Err(), &ex.server.EngineMetrics,
	)
	if ex.server.cfg.TestingKnobs.AfterExecute != nil {
		ex.server.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), res.Err())
	}

	return nil
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
		var commErr error
		queryErr := ex.forEachRow(params, planner.curPlan.plan, func(values tree.Datums) error {
			for _, val := range values {
				if err := checkResultType(val.ResolvedType()); err != nil {
					return err
				}
			}
			commErr = res.AddRow(ctx, values)
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

// exectWithDistSQLEngine converts a plan to a distributed SQL physical plan and
// runs it.
// If an error is returned, the connection needs to stop processing queries.
// Query execution errors are written to res; they are not returned.
func (ex *connExecutor) execWithDistSQLEngine(
	ctx context.Context, planner *planner, stmtType tree.StatementType, res RestrictedCommandResult,
) error {
	recv := makeDistSQLReceiver(
		ctx, res, stmtType,
		ex.server.cfg.RangeDescriptorCache, ex.server.cfg.LeaseHolderCache,
		planner.txn,
		func(ts hlc.Timestamp) {
			_ = ex.server.cfg.Clock.Update(ts)
		},
	)
	ex.server.cfg.DistSQLPlanner.PlanAndRun(
		ctx, planner.txn, planner.curPlan.plan, recv, planner.ExtendedEvalContext(),
	)
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
		ex.server.StatementCounters.incrementCount(stmt.AST)
		iso, err := ex.isolationToProto(s.Modes.Isolation)
		if err != nil {
			return ex.makeErrEvent(err, s)
		}
		pri, err := priorityToProto(s.Modes.UserPriority)
		if err != nil {
			return ex.makeErrEvent(err, s)
		}

		return eventTxnStart{ImplicitTxn: fsm.False},
			makeEventTxnStartPayload(
				iso, pri, ex.readWriteModeWithSessionDefault(s.Modes.ReadWriteMode),
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
				ex.sessionData.DefaultIsolationLevel,
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

		// Note: Postgres replies to COMMIT of failed txn with "ROLLBACK" too.
		res.ResetStmtType((*tree.RollbackTransaction)(nil))

		return eventTxnFinish{}, eventTxnFinishPayload{commit: false}
	case *tree.RollbackToSavepoint, *tree.Savepoint:
		// We accept both the "ROLLBACK TO SAVEPOINT cockroach_restart" and the
		// "SAVEPOINT cockroach_restart" commands to indicate client intent to
		// retry a transaction in a RestartWait state.
		var spName string
		switch n := s.(type) {
		case *tree.RollbackToSavepoint:
			spName = n.Savepoint
		case *tree.Savepoint:
			spName = n.Name
		default:
			panic("unreachable")
		}
		if err := tree.ValidateRestartCheckpoint(spName); err != nil {
			ev := eventNonRetriableErr{IsCommit: fsm.False}
			payload := eventNonRetriableErrPayload{
				err: err,
			}
			return ev, payload
		}

		if !(inRestartWait || ex.machine.CurState().(stateAborted).RetryIntent.Get()) {
			err := fmt.Errorf("SAVEPOINT %s has not been used", tree.RestartSavepointName)
			ev := eventNonRetriableErr{IsCommit: fsm.False}
			payload := eventNonRetriableErrPayload{
				err: err,
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
			ex.state.isolation, ex.state.priority,
			rwMode, ex.state.sqlTimestamp,
			ex.transitionCtx)
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
	ex.server.StatementCounters.incrementCount(stmt.AST)
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
	default:
		res.SetError(pgerror.NewErrorf(pgerror.CodeInternalError,
			"programming error: unrecognized observer statement type %T", stmt.AST))
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
		}); err != nil {
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
func (ex *connExecutor) handleAutoCommit(ctx context.Context, stmt tree.Statement) error {
	txn := ex.state.mu.txn
	if txn.IsCommitted() {
		return nil
	}
	if knob := ex.server.cfg.TestingKnobs.BeforeAutoCommit; knob != nil {
		if err := knob(ctx, stmt.String()); err != nil {
			return err
		}
	}
	err := txn.Commit(ctx)
	log.VEventf(ctx, 2, "AutoCommit. err: %v", err)
	return err
}
