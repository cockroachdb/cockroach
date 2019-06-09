// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sql

import (
	"context"
	"fmt"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// RestartSavepointName is the only savepoint ident that we accept.
const RestartSavepointName string = "cockroach_restart"

var errSavepointNotUsed = pgerror.Newf(
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
func (ex *connExecutor) execStmt(
	ctx context.Context, stmt Statement, res RestrictedCommandResult, pinfo *tree.PlaceholderInfo,
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
		if ex.server.cfg.Settings.IsCPUProfiling() {
			labels := pprof.Labels(
				"stmt.tag", stmt.AST.StatementTag(),
				"stmt.anonymized", stmt.AnonymizedStr,
			)
			pprof.Do(ctx, labels, func(ctx context.Context) {
				ev, payload, err = ex.execStmtInOpenState(ctx, stmt, pinfo, res)
			})
		} else {
			ev, payload, err = ex.execStmtInOpenState(ctx, stmt, pinfo, res)
		}
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
	ex.metrics.EngineMetrics.FailureCount.Inc(1)
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
	ex.incrementStartedStmtCounter(stmt)
	defer func() {
		if retErr == nil && !payloadHasError(retPayload) {
			ex.incrementExecutedStmtCounter(stmt)
		}
	}()
	os := ex.machine.CurState().(stateOpen)

	var timeoutTicker *time.Timer
	queryTimedOut := false
	doneAfterFunc := make(chan struct{}, 1)

	// Canceling a query cancels its transaction's context so we take a reference
	// to the cancelation function here.
	unregisterFn := ex.addActiveQuery(stmt.queryID, stmt.AST, ex.state.cancel)

	// queryDone is a cleanup function dealing with unregistering a query.
	// It also deals with overwriting res.Error to a more user-friendly message in
	// case of query cancelation. res can be nil to opt out of this.
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
		if res != nil && ctx.Err() != nil && res.Err() != nil {
			if queryTimedOut {
				res.SetError(sqlbase.QueryTimeoutError)
			} else {
				res.SetError(sqlbase.QueryCanceledError)
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

	var discardRows bool
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
			err := pgerror.UnimplementedWithIssueDetail(10735, "nested", "SAVEPOINT may not be nested")
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
			err := pgerror.Newf(pgerror.CodeSyntaxError,
				"SAVEPOINT %s needs to be the first statement in a "+
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
		if _, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[name]; ok {
			err := pgerror.Newf(
				pgerror.CodeDuplicatePreparedStatementError,
				"prepared statement %q already exists", name,
			)
			return makeErrEvent(err)
		}
		var typeHints tree.PlaceholderTypes
		if len(s.Types) > 0 {
			if len(s.Types) > stmt.NumPlaceholders {
				err := pgerror.Newf(pgerror.CodeSyntaxError, "too many types provided")
				return makeErrEvent(err)
			}
			typeHints = make(tree.PlaceholderTypes, stmt.NumPlaceholders)
			for i, t := range s.Types {
				typeHints[i] = t
			}
		}
		if _, err := ex.addPreparedStmt(
			ctx, name,
			Statement{
				Statement: parser.Statement{
					// We need the SQL string just for the part that comes after
					// "PREPARE ... AS",
					// TODO(radu): it would be nice if the parser would figure out this
					// string and store it in tree.Prepare.
					SQL:             tree.AsStringWithFlags(s.Statement, tree.FmtParsable),
					AST:             s.Statement,
					NumPlaceholders: stmt.NumPlaceholders,
					NumAnnotations:  stmt.NumAnnotations,
				},
			},
			typeHints,
		); err != nil {
			return makeErrEvent(err)
		}
		return nil, nil, nil

	case *tree.Execute:
		// Replace the `EXECUTE foo` statement with the prepared statement, and
		// continue execution below.
		name := s.Name.String()
		ps, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[name]
		if !ok {
			err := pgerror.Newf(
				pgerror.CodeInvalidSQLStatementNameError,
				"prepared statement %q does not exist", name,
			)
			return makeErrEvent(err)
		}
		var err error
		pinfo, err = fillInPlaceholders(ps, name, s.Params, ex.sessionData.SearchPath)
		if err != nil {
			return makeErrEvent(err)
		}

		stmt.Statement = ps.Statement
		stmt.Prepared = ps
		stmt.ExpectedTypes = ps.Columns
		stmt.AnonymizedStr = ps.AnonymizedStr
		res.ResetStmtType(ps.AST)

		discardRows = s.DiscardRows
	}

	// For regular statements (the ones that get to this point), we don't return
	// any event unless an an error happens.

	p := &ex.planner
	stmtTS := ex.server.cfg.Clock.PhysicalTime()
	ex.resetPlanner(ctx, p, ex.state.mu.txn, stmtTS, stmt.NumAnnotations)

	if os.ImplicitTxn.Get() {
		asOfTs, err := p.isAsOf(stmt.AST)
		if err != nil {
			return makeErrEvent(err)
		}
		if asOfTs != nil {
			p.semaCtx.AsOfTimestamp = asOfTs
			p.extendedEvalCtx.SetTxnTimestamp(asOfTs.GoTime())
			ex.state.setHistoricalTimestamp(ctx, *asOfTs)
		}
	} else {
		// If we're in an explicit txn, we allow AOST but only if it matches with
		// the transaction's timestamp. This is useful for running AOST statements
		// using the InternalExecutor inside an external transaction; one might want
		// to do that to force p.avoidCachedDescriptors to be set below.
		ts, err := p.isAsOf(stmt.AST)
		if err != nil {
			return makeErrEvent(err)
		}
		if ts != nil {
			if origTs := ex.state.getOrigTimestamp(); *ts != origTs {
				err = pgerror.Newf(pgcode.Syntax,
					"inconsistent AS OF SYSTEM TIME timestamp; expected: %s", origTs)
				err = errors.WithHint(err,
					"Generally AS OF SYSTEM TIME cannot be used inside a transaction.")
				return makeErrEvent(err)
			}
			p.semaCtx.AsOfTimestamp = ts
		}
	}

	if err := p.semaCtx.Placeholders.Assign(pinfo, stmt.NumPlaceholders); err != nil {
		return makeErrEvent(err)
	}
	p.extendedEvalCtx.Placeholders = &p.semaCtx.Placeholders
	p.extendedEvalCtx.Annotations = &p.semaCtx.Annotations
	ex.phaseTimes[plannerStartExecStmt] = timeutil.Now()
	p.stmt = &stmt
	p.discardRows = discardRows

	// TODO(andrei): Ideally we'd like to fork off a context for each individual
	// statement. But the heartbeat loop in TxnCoordSender currently assumes that
	// the context of the first operation in a txn batch lasts at least as long as
	// the transaction itself. Once that sender is able to distinguish between
	// statement and transaction contexts, we should move to per-statement
	// contexts.
	p.cancelChecker = sqlbase.NewCancelChecker(ctx)

	p.autoCommit = os.ImplicitTxn.Get() && !ex.server.cfg.TestingKnobs.DisableAutoCommit
	if err := ex.dispatchToExecutionEngine(ctx, p, res); err != nil {
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
				err: roachpb.NewTransactionRetryWithProtoRefreshError(
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
	// No event was generated.
	return nil, nil, nil
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
	ex.extraTxnState.tables.releaseLeases(ctx)

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
	retryErr := roachpb.NewTransactionRetryWithProtoRefreshError(
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

func enhanceErrWithCorrelation(err error, isCorrelated bool) error {
	if err == nil || !isCorrelated {
		return err
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
	if code := pgerror.GetPGCode(err); code == pgcode.UndefinedColumn || code == pgcode.UndefinedTable {
		err = errors.WithHintf(err, "some correlated subqueries are not supported yet - see %s",
			"https://github.com/cockroachdb/cockroach/issues/3288")
	}
	return err
}

// dispatchToExecutionEngine executes the statement, writes the result to res
// and returns an event for the connection's state machine.
//
// If an error is returned, the connection needs to stop processing queries.
// Query execution errors are written to res; they are not returned; it is
// expected that the caller will inspect res and react to query errors by
// producing an appropriate state machine event.
func (ex *connExecutor) dispatchToExecutionEngine(
	ctx context.Context, planner *planner, res RestrictedCommandResult,
) error {
	stmt := planner.stmt
	ex.sessionTracing.TracePlanStart(ctx, stmt.AST.StatementTag())
	planner.statsCollector.PhaseTimes()[plannerStartLogicalPlan] = timeutil.Now()

	// Prepare the plan. Note, the error is processed below. Everything
	// between here and there needs to happen even if there's an error.
	err := ex.makeExecPlan(ctx, planner)
	// We'll be closing the plan manually below after execution; this
	// defer is a catch-all in case some other return path is taken.
	defer planner.curPlan.close(ctx)

	// Certain statements want their results to go to the client
	// directly. Configure this here.
	if planner.curPlan.avoidBuffering {
		res.DisableBuffering()
	}

	// Ensure that the plan is collected just before closing.
	if sampleLogicalPlans.Get(&ex.appStats.st.SV) {
		planner.curPlan.maybeSavePlan = func(ctx context.Context) *roachpb.ExplainTreePlanNode {
			return ex.maybeSavePlan(ctx, planner)
		}
	}

	defer func() {
		planner.maybeLogStatement(ctx, "exec", ex.extraTxnState.autoRetryCounter, res.RowsAffected(), res.Err())
	}()

	planner.statsCollector.PhaseTimes()[plannerEndLogicalPlan] = timeutil.Now()
	ex.sessionTracing.TracePlanEnd(ctx, err)

	// Finally, process the planning error from above.
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
		ex.server.cfg.TestingKnobs.BeforeExecute(ctx, stmt.String())
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

	// We need to set the "exec done" flag early because
	// curPlan.close(), which will need to observe it, may be closed
	// during execution (distsqlrun.PlanAndRun).
	//
	// TODO(knz): This is a mis-design. Andrei says "it's OK if
	// execution closes the plan" but it transfers responsibility to
	// run any "finalizers" on the plan (including plan sampling for
	// stats) to the execution engine. That's a lot of responsibility
	// to transfer! It would be better if this responsibility remained
	// around here.
	planner.curPlan.flags.Set(planFlagExecDone)

	if distributePlan {
		planner.curPlan.flags.Set(planFlagDistributed)
	} else {
		planner.curPlan.flags.Set(planFlagDistSQLLocal)
	}
	ex.sessionTracing.TraceExecStart(ctx, "distributed")
	err = ex.execWithDistSQLEngine(ctx, planner, stmt.AST.StatementType(), res, distributePlan)
	ex.sessionTracing.TraceExecEnd(ctx, res.Err(), res.RowsAffected())
	planner.statsCollector.PhaseTimes()[plannerEndExecStmt] = timeutil.Now()

	// Record the statement summary. This also closes the plan if the
	// plan has not been closed earlier.
	ex.recordStatementSummary(
		ctx, planner,
		ex.extraTxnState.autoRetryCounter, res.RowsAffected(), res.Err(),
	)
	if ex.server.cfg.TestingKnobs.AfterExecute != nil {
		ex.server.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), res.Err())
	}

	return err
}

// makeExecPlan creates an execution plan and populates planner.curPlan, using
// either the optimizer or the heuristic planner.
func (ex *connExecutor) makeExecPlan(ctx context.Context, planner *planner) error {
	stmt := planner.stmt
	// Initialize planner.curPlan.AST early; it might be used by maybeLogStatement
	// in error cases.
	planner.curPlan = planTop{AST: stmt.AST}

	var isCorrelated bool
	if optMode := ex.sessionData.OptimizerMode; optMode != sessiondata.OptimizerOff {
		log.VEvent(ctx, 2, "generating optimizer plan")
		var result *planTop
		var err error
		result, isCorrelated, err = planner.makeOptimizerPlan(ctx)
		if err == nil {
			planner.curPlan = *result
			return nil
		}
		if isCorrelated {
			// Note: we are setting isCorrelated here because
			// makeOptimizerPlan() can determine isCorrelated but fail with
			// a non-nil error and a nil result -- for example, when it runs
			// into an unsupported SQL feature that the HP supports, after
			// having processed a correlated subquery (which the heuristic
			// planner won't support).
			planner.curPlan.flags.Set(planFlagOptIsCorrelated)
		}
		log.VEventf(ctx, 1, "optimizer plan failed (isCorrelated=%t): %v", isCorrelated, err)
		if !canFallbackFromOpt(err, optMode, stmt) {
			return err
		}
		planner.curPlan.flags.Set(planFlagOptFallback)
		log.VEvent(ctx, 1, "optimizer falls back on heuristic planner")
	} else {
		log.VEvent(ctx, 2, "optimizer disabled")
	}
	// Use the heuristic planner.
	optFlags := planner.curPlan.flags
	err := planner.makePlan(ctx)
	planner.curPlan.flags |= optFlags
	err = enhanceErrWithCorrelation(err, isCorrelated)
	return err
}

// saveLogicalPlanDescription returns whether we should save this as a sample logical plan
// for its corresponding fingerprint. We use `logicalPlanCollectionPeriod`
// to assess how frequently to sample logical plans.
func (ex *connExecutor) saveLogicalPlanDescription(
	stmt *Statement, useDistSQL bool, optimizerUsed bool, err error,
) bool {
	stats := ex.appStats.getStatsForStmt(
		stmt, useDistSQL, optimizerUsed, err, false /* createIfNonexistent */)
	if stats == nil {
		// Save logical plan the first time we see new statement fingerprint.
		return true
	}
	now := timeutil.Now()
	period := logicalPlanCollectionPeriod.Get(&ex.appStats.st.SV)
	stats.Lock()
	defer stats.Unlock()
	timeLastSampled := stats.data.SensitiveInfo.MostRecentPlanTimestamp
	return now.Sub(timeLastSampled) >= period
}

// canFallbackFromOpt returns whether we can fallback on the heuristic planner
// when the optimizer hits an error.
func canFallbackFromOpt(err error, optMode sessiondata.OptimizerMode, stmt *Statement) bool {
	if !errors.HasUnimplementedError(err) {
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

	// Never fall back on PREPARE AS OPT PLAN.
	if _, ok := stmt.AST.(*tree.CannedOptPlan); ok {
		return false
	}
	return true
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
		var evalCtx extendedEvalContext
		ex.initEvalCtx(ctx, &evalCtx, planner)
		evalCtxFactory := func() *extendedEvalContext {
			ex.resetEvalCtx(&evalCtx, planner.txn, planner.ExtendedEvalContext().StmtTimestamp)
			evalCtx.Placeholders = &planner.semaCtx.Placeholders
			evalCtx.Annotations = &planner.semaCtx.Annotations
			return &evalCtx
		}
		if !ex.server.cfg.DistSQLPlanner.PlanAndRunSubqueries(
			ctx, planner, evalCtxFactory, planner.curPlan.subqueryPlans, recv, distribute,
		) {
			return recv.commErr
		}
	}
	recv.discardRows = planner.discardRows
	// We pass in whether or not we wanted to distribute this plan, which tells
	// the planner whether or not to plan remote table readers.
	ex.server.cfg.DistSQLPlanner.PlanAndRun(
		ctx, evalCtx, planCtx, planner.txn, planner.curPlan.plan, recv)
	return recv.commErr
}

// beginTransactionTimestampsAndReadMode computes the timestamps and
// ReadWriteMode to be used for the associated transaction state based on the
// values of the statement's Modes. Note that this method may reset the
// connExecutor's planner in order to compute the timestamp for the AsOf clause
// if it exists. The timestamps correspond to the timestamps passed to
// makeEventTxnStartPayload; txnSQLTimestamp propagates to become the
// TxnTimestamp while historicalTimestamp populated with a non-nil value only
// if the BeginTransaction statement has a non-nil AsOf clause expression. A
// non-nil historicalTimestamp implies a ReadOnly rwMode.
func (ex *connExecutor) beginTransactionTimestampsAndReadMode(
	ctx context.Context, s *tree.BeginTransaction,
) (
	rwMode tree.ReadWriteMode,
	txnSQLTimestamp time.Time,
	historicalTimestamp *hlc.Timestamp,
	err error,
) {
	now := ex.server.cfg.Clock.Now()
	if s.Modes.AsOf.Expr == nil {
		rwMode = ex.readWriteModeWithSessionDefault(s.Modes.ReadWriteMode)
		return rwMode, now.GoTime(), nil, nil
	}
	p := &ex.planner
	ex.resetPlanner(ctx, p, nil /* txn */, now.GoTime(), 0 /* numAnnotations */)
	ts, err := p.EvalAsOfTimestamp(s.Modes.AsOf)
	if err != nil {
		return 0, time.Time{}, nil, err
	}
	// NB: This check should never return an error because the parser should
	// disallow the creation of a TransactionModes struct which both has an
	// AOST clause and is ReadWrite but performing a check decouples this code
	// from that and hopefully adds clarity that the returning of ReadOnly with
	// a historical timestamp is intended.
	if s.Modes.ReadWriteMode == tree.ReadWrite {
		return 0, time.Time{}, nil, tree.ErrAsOfSpecifiedWithReadWrite
	}
	return tree.ReadOnly, ts.GoTime(), &ts, nil
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
) (_ fsm.Event, payload fsm.EventPayload) {
	switch s := stmt.AST.(type) {
	case *tree.BeginTransaction:
		ex.incrementStartedStmtCounter(stmt)
		defer func() {
			if !payloadHasError(payload) {
				ex.incrementExecutedStmtCounter(stmt)
			}
		}()
		pri, err := priorityToProto(s.Modes.UserPriority)
		if err != nil {
			return ex.makeErrEvent(err, s)
		}
		mode, sqlTs, historicalTs, err := ex.beginTransactionTimestampsAndReadMode(ctx, s)
		if err != nil {
			return ex.makeErrEvent(err, s)
		}
		return eventTxnStart{ImplicitTxn: fsm.False},
			makeEventTxnStartPayload(
				pri, mode, sqlTs,
				historicalTs,
				ex.transitionCtx)
	case *tree.CommitTransaction, *tree.ReleaseSavepoint,
		*tree.RollbackTransaction, *tree.SetTransaction, *tree.Savepoint:
		return ex.makeErrEvent(errNoTransactionInProgress, stmt.AST)
	default:
		mode := tree.ReadWrite
		if ex.sessionData.DefaultReadOnly {
			mode = tree.ReadOnly
		}
		// NB: Implicit transactions are created without a historical timestamp even
		// though the statement might contain an AOST clause. In these cases the
		// clause is evaluated and applied execStmtInOpenState.
		return eventTxnStart{ImplicitTxn: fsm.True},
			makeEventTxnStartPayload(
				roachpb.NormalUserPriority,
				mode,
				ex.server.cfg.Clock.PhysicalTime(),
				nil, /* historicalTimestamp */
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
			ex.state.priority, rwMode, ex.state.sqlTimestamp,
			nil /* historicalTimestamp */, ex.transitionCtx)
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
) (ev fsm.Event, payload fsm.EventPayload) {
	ex.incrementStartedStmtCounter(stmt)
	defer func() {
		if !payloadHasError(payload) {
			ex.incrementExecutedStmtCounter(stmt)
		}
	}()
	switch stmt.AST.(type) {
	case *tree.CommitTransaction, *tree.RollbackTransaction:
		// Reply to a rollback with the COMMIT tag, by analogy to what we do when we
		// get a COMMIT in state Aborted.
		res.ResetStmtType((*tree.CommitTransaction)(nil))
		return eventTxnFinish{}, eventTxnFinishPayload{commit: true}
	default:
		ev = eventNonRetriableErr{IsCommit: fsm.False}
		payload = eventNonRetriableErrPayload{
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
		res.SetError(pgerror.AssertionFailedf("unrecognized observer statement type %T", stmt.AST))
		return nil
	}
}

// runShowSyntax executes a SHOW SYNTAX <stmt> query.
//
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) runShowSyntax(
	ctx context.Context, stmt string, res RestrictedCommandResult,
) error {
	res.SetColumns(ctx, sqlbase.ShowSyntaxColumns)
	var commErr error
	if err := parser.RunShowSyntax(ctx, stmt,
		func(ctx context.Context, field, msg string) error {
			commErr = res.AddRow(ctx, tree.Datums{tree.NewDString(field), tree.NewDString(msg)})
			return nil
		},
		ex.recordError, /* reportErr */
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
		res.SetError(pgerror.AssertionFailedf("set tracing missing argument"))
		return
	}

	modes := make([]string, len(n.Values))
	for i, v := range n.Values {
		v = unresolvedNameToStrVal(v)
		strVal, ok := v.(*tree.StrVal)
		if !ok {
			res.SetError(pgerror.AssertionFailedf(
				"expected string for set tracing argument, not %T", v))
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
			return pgerror.Newf(pgerror.CodeSyntaxError,
				"set tracing: unknown mode %q", s)
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

// incrementStartedStmtCounter increments the appropriate started
// statement counter for stmt's type.
func (ex *connExecutor) incrementStartedStmtCounter(stmt Statement) {
	ex.metrics.StartedStatementCounters.incrementCount(ex, stmt.AST)
}

// incrementExecutedStmtCounter increments the appropriate executed
// statement counter for stmt's type.
func (ex *connExecutor) incrementExecutedStmtCounter(stmt Statement) {
	ex.metrics.ExecutedStatementCounters.incrementCount(ex, stmt.AST)
}

// payloadHasError returns true if the passed payload implements
// payloadWithError.
func payloadHasError(payload fsm.EventPayload) bool {
	_, hasErr := payload.(payloadWithError)
	return hasErr
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
		return pgerror.Newf(pgerror.CodeInvalidSavepointSpecificationError,
			`SAVEPOINT %q is in use`, tree.ErrString(&ex.state.activeSavepointName))
	}
	if !ex.sessionData.ForceSavepointRestart && !strings.HasPrefix(string(savepoint), RestartSavepointName) {
		return pgerror.UnimplementedWithIssueHint(10735,
			"SAVEPOINT not supported except for "+RestartSavepointName,
			"Retryable transactions with arbitrary SAVEPOINT names can be enabled "+
				"with SET force_savepoint_restart=true")
	}
	return nil
}
