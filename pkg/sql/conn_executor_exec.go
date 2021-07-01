// Copyright 2018 The Cockroach Authors.
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
	"runtime/pprof"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
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
func (ex *connExecutor) execStmt(
	ctx context.Context,
	parserStmt parser.Statement,
	prepared *PreparedStatement,
	pinfo *tree.PlaceholderInfo,
	res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload, error) {
	ast := parserStmt.AST
	if log.V(2) || logStatementsExecuteEnabled.Get(&ex.server.cfg.Settings.SV) ||
		log.HasSpanOrEvent(ctx) {
		log.VEventf(ctx, 2, "executing: %s in state: %s", ast, ex.machine.CurState())
	}

	// Stop the session idle timeout when a new statement is executed.
	ex.mu.IdleInSessionTimeout.Stop()
	ex.mu.IdleInTransactionSessionTimeout.Stop()

	// Run observer statements in a separate code path; their execution does not
	// depend on the current transaction state.
	if _, ok := ast.(tree.ObserverStatement); ok {
		ex.statsCollector.Reset(ex.statsWriter, ex.phaseTimes)
		err := ex.runObserverStatement(ctx, ast, res)
		// Note that regardless of res.Err(), these observer statements don't
		// generate error events; transactions are always allowed to continue.
		return nil, nil, err
	}

	// Dispatch the statement for execution based on the current state.
	var ev fsm.Event
	var payload fsm.EventPayload
	var err error

	switch ex.machine.CurState().(type) {
	case stateNoTxn:
		// Note: when not using explicit transactions, we go through this transition
		// for every statement. It is important to minimize the amount of work and
		// allocations performed up to this point.
		ev, payload = ex.execStmtInNoTxnState(ctx, ast)

	case stateOpen:
		if ex.server.cfg.Settings.CPUProfileType() == cluster.CPUProfileWithLabels {
			remoteAddr := "internal"
			if rAddr := ex.sessionData.RemoteAddr; rAddr != nil {
				remoteAddr = rAddr.String()
			}
			labels := pprof.Labels(
				"appname", ex.sessionData.ApplicationName,
				"addr", remoteAddr,
				"stmt.tag", ast.StatementTag(),
				"stmt.anonymized", anonymizeStmt(ast),
			)
			pprof.Do(ctx, labels, func(ctx context.Context) {
				ev, payload, err = ex.execStmtInOpenState(ctx, parserStmt, prepared, pinfo, res)
			})
		} else {
			ev, payload, err = ex.execStmtInOpenState(ctx, parserStmt, prepared, pinfo, res)
		}
		switch ev.(type) {
		case eventNonRetriableErr:
			ex.recordFailure()
		}

	case stateAborted:
		ev, payload = ex.execStmtInAbortedState(ctx, ast, res)

	case stateCommitWait:
		ev, payload = ex.execStmtInCommitWaitState(ast, res)

	default:
		panic(errors.AssertionFailedf("unexpected txn state: %#v", ex.machine.CurState()))
	}

	if ex.sessionData.IdleInSessionTimeout > 0 {
		// Cancel the session if the idle time exceeds the idle in session timeout.
		ex.mu.IdleInSessionTimeout = timeout{time.AfterFunc(
			ex.sessionData.IdleInSessionTimeout,
			ex.cancelSession,
		)}
	}

	if ex.sessionData.IdleInTransactionSessionTimeout > 0 {
		startIdleInTransactionSessionTimeout := func() {
			switch ast.(type) {
			case *tree.CommitTransaction, *tree.RollbackTransaction:
				// Do nothing, the transaction is completed, we do not want to start
				// an idle timer.
			default:
				ex.mu.IdleInTransactionSessionTimeout = timeout{time.AfterFunc(
					ex.sessionData.IdleInTransactionSessionTimeout,
					ex.cancelSession,
				)}
			}
		}
		switch ex.machine.CurState().(type) {
		case stateAborted, stateCommitWait:
			startIdleInTransactionSessionTimeout()
		case stateOpen:
			// Only start timeout if the statement is executed in an
			// explicit transaction.
			if !ex.implicitTxn() {
				startIdleInTransactionSessionTimeout()
			}
		}
	}

	return ev, payload, err
}

func (ex *connExecutor) recordFailure() {
	ex.metrics.EngineMetrics.FailureCount.Inc(1)
}

// execPortal executes a prepared statement. It is a "wrapper" around execStmt
// method that is performing additional work to track portal's state.
func (ex *connExecutor) execPortal(
	ctx context.Context,
	portal PreparedPortal,
	portalName string,
	stmtRes CommandResult,
	pinfo *tree.PlaceholderInfo,
) (ev fsm.Event, payload fsm.EventPayload, err error) {
	switch ex.machine.CurState().(type) {
	case stateOpen:
		// We're about to execute the statement in an open state which
		// could trigger the dispatch to the execution engine. However, it
		// is possible that we're trying to execute an already exhausted
		// portal - in such a scenario we should return no rows, but the
		// execution engine is not aware of that and would run the
		// statement as if it was running it for the first time. In order
		// to prevent such behavior, we check whether the portal has been
		// exhausted and execute the statement only if it hasn't. If it has
		// been exhausted, then we do not dispatch the query for execution,
		// but connExecutor will still perform necessary state transitions
		// which will emit CommandComplete messages and alike (in a sense,
		// by not calling execStmt we "execute" the portal in such a way
		// that it returns 0 rows).
		// Note that here we deviate from Postgres which returns an error
		// when attempting to execute an exhausted portal which has a
		// StatementReturnType() different from "Rows".
		if portal.exhausted {
			return nil, nil, nil
		}
		ev, payload, err = ex.execStmt(ctx, portal.Stmt.Statement, portal.Stmt, pinfo, stmtRes)
		// Portal suspension is supported via a "side" state machine
		// (see pgwire.limitedCommandResult for details), so when
		// execStmt returns, we know for sure that the portal has been
		// executed to completion, thus, it is exhausted.
		// Note that the portal is considered exhausted regardless of
		// the fact whether an error occurred or not - if it did, we
		// still don't want to re-execute the portal from scratch.
		// The current statement may have just closed and deleted the portal,
		// so only exhaust it if it still exists.
		if _, ok := ex.extraTxnState.prepStmtsNamespace.portals[portalName]; ok {
			ex.exhaustPortal(portalName)
		}
		return ev, payload, err

	default:
		return ex.execStmt(ctx, portal.Stmt.Statement, portal.Stmt, pinfo, stmtRes)
	}
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
	ctx context.Context,
	parserStmt parser.Statement,
	prepared *PreparedStatement,
	pinfo *tree.PlaceholderInfo,
	res RestrictedCommandResult,
) (retEv fsm.Event, retPayload fsm.EventPayload, retErr error) {
	ast := parserStmt.AST
	ctx = withStatement(ctx, ast)

	var stmt Statement
	queryID := ex.generateID()
	// Update the deadline on the transaction based on the collections.
	err := ex.extraTxnState.descCollection.MaybeUpdateDeadline(ctx, ex.state.mu.txn)
	if err != nil {
		return nil, nil, err
	}

	if prepared != nil {
		stmt = makeStatementFromPrepared(prepared, queryID)
	} else {
		stmt = makeStatement(parserStmt, queryID)
	}

	ex.incrementStartedStmtCounter(ast)
	defer func() {
		if retErr == nil && !payloadHasError(retPayload) {
			ex.incrementExecutedStmtCounter(ast)
		}
	}()

	ex.state.mu.Lock()
	ex.state.mu.stmtCount++
	ex.state.mu.Unlock()

	os := ex.machine.CurState().(stateOpen)

	var timeoutTicker *time.Timer
	queryTimedOut := false
	doneAfterFunc := make(chan struct{}, 1)

	// Early-associate placeholder info with the eval context,
	// so that we can fill in placeholder values in our call to addActiveQuery, below.
	if !ex.planner.EvalContext().HasPlaceholders() {
		ex.planner.EvalContext().Placeholders = pinfo
	}

	// Canceling a query cancels its transaction's context so we take a reference
	// to the cancelation function here.
	unregisterFn := ex.addActiveQuery(ast, formatWithPlaceholders(ast, ex.planner.EvalContext()), queryID, ex.state.cancel)

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
			// Even in the cases where the error is a retryable error, we want to
			// intercept the event and payload returned here to ensure that the query
			// is not retried.
			retEv = eventNonRetriableErr{
				IsCommit: fsm.FromBool(isCommit(ast)),
			}
			res.SetError(cancelchecker.QueryCanceledError)
			retPayload = eventNonRetriableErrPayload{err: cancelchecker.QueryCanceledError}
		}

		// If the query timed out, we intercept the error, payload, and event here
		// for the same reasons we intercept them for canceled queries above.
		// Overriding queries with a QueryTimedOut error needs to happen after
		// we've checked for canceled queries as some queries may be canceled
		// because of a timeout, in which case the appropriate error to return to
		// the client is one that indicates the timeout, rather than the more general
		// query canceled error. It's important to note that a timed out query may
		// not have been canceled (eg. We never even start executing a query
		// because the timeout has already expired), and therefore this check needs
		// to happen outside the canceled query check above.
		if queryTimedOut {
			// A timed out query should never produce retryable errors/events/payloads
			// so we intercept and overwrite them all here.
			retEv = eventNonRetriableErr{
				IsCommit: fsm.FromBool(isCommit(ast)),
			}
			res.SetError(sqlerrors.QueryTimeoutError)
			retPayload = eventNonRetriableErrPayload{err: sqlerrors.QueryTimeoutError}
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

	makeErrEvent := func(err error) (fsm.Event, fsm.EventPayload, error) {
		ev, payload := ex.makeErrEvent(err, ast)
		return ev, payload, nil
	}

	p := &ex.planner
	stmtTS := ex.server.cfg.Clock.PhysicalTime()
	ex.statsCollector.Reset(ex.statsWriter, ex.phaseTimes)
	ex.resetPlanner(ctx, p, ex.state.mu.txn, stmtTS)
	p.sessionDataMutator.paramStatusUpdater = res
	p.noticeSender = res
	ih := &p.instrumentation

	// Special top-level handling for EXPLAIN ANALYZE.
	if e, ok := ast.(*tree.ExplainAnalyze); ok {
		switch e.Mode {
		case tree.ExplainDebug:
			telemetry.Inc(sqltelemetry.ExplainAnalyzeDebugUseCounter)
			ih.SetOutputMode(explainAnalyzeDebugOutput, explain.Flags{})

		case tree.ExplainPlan:
			telemetry.Inc(sqltelemetry.ExplainAnalyzeUseCounter)
			flags := explain.MakeFlags(&e.ExplainOptions)
			if ex.server.cfg.TestingKnobs.DeterministicExplain {
				flags.Redact = explain.RedactAll
			}
			ih.SetOutputMode(explainAnalyzePlanOutput, flags)

		case tree.ExplainDistSQL:
			telemetry.Inc(sqltelemetry.ExplainAnalyzeDistSQLUseCounter)
			flags := explain.MakeFlags(&e.ExplainOptions)
			if ex.server.cfg.TestingKnobs.DeterministicExplain {
				flags.Redact = explain.RedactAll
			}
			ih.SetOutputMode(explainAnalyzeDistSQLOutput, flags)

		default:
			return makeErrEvent(errors.AssertionFailedf("unsupported EXPLAIN ANALYZE mode %s", e.Mode))
		}
		// Strip off the explain node to execute the inner statement.
		stmt.AST = e.Statement
		ast = e.Statement
		// TODO(radu): should we trim the "EXPLAIN ANALYZE (DEBUG)" part from
		// stmt.SQL?

		// Clear any ExpectedTypes we set if we prepared this statement (they
		// reflect the column types of the EXPLAIN itself and not those of the inner
		// statement).
		stmt.ExpectedTypes = nil
	}

	// Special top-level handling for EXECUTE. This must happen after the handling
	// for EXPLAIN ANALYZE (in order to support EXPLAIN ANALYZE EXECUTE) but
	// before setting up the instrumentation helper.
	if e, ok := ast.(*tree.Execute); ok {
		// Replace the `EXECUTE foo` statement with the prepared statement, and
		// continue execution.
		name := e.Name.String()
		ps, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[name]
		if !ok {
			err := pgerror.Newf(
				pgcode.InvalidSQLStatementName,
				"prepared statement %q does not exist", name,
			)
			return makeErrEvent(err)
		}
		var err error
		pinfo, err = fillInPlaceholders(ctx, ps, name, e.Params, ex.sessionData.SearchPath)
		if err != nil {
			return makeErrEvent(err)
		}

		// TODO(radu): what about .SQL, .NumAnnotations, .NumPlaceholders?
		stmt.Statement = ps.Statement
		stmt.Prepared = ps
		stmt.ExpectedTypes = ps.Columns
		stmt.AnonymizedStr = ps.AnonymizedStr
		res.ResetStmtType(ps.AST)

		if e.DiscardRows {
			ih.SetDiscardRows()
		}
		ast = stmt.Statement.AST
	}

	var needFinish bool
	ctx, needFinish = ih.Setup(
		ctx, ex.server.cfg, ex.statsCollector, p, ex.stmtDiagnosticsRecorder,
		stmt.AnonymizedStr, os.ImplicitTxn.Get(), ex.extraTxnState.shouldCollectTxnExecutionStats,
	)
	if needFinish {
		sql := stmt.SQL
		defer func() {
			retErr = ih.Finish(
				ex.server.cfg,
				ex.statsCollector,
				&ex.extraTxnState.accumulatedStats,
				ex.extraTxnState.shouldCollectTxnExecutionStats,
				p,
				ast,
				sql,
				res,
				retErr,
			)
		}()
		// TODO(radu): consider removing this if/when #46164 is addressed.
		p.extendedEvalCtx.Context = ctx
	}

	// We exempt `SET` statements from the statement timeout, particularly so as
	// not to block the `SET statement_timeout` command itself.
	if ex.sessionData.StmtTimeout > 0 && ast.StatementTag() != "SET" {
		timerDuration :=
			ex.sessionData.StmtTimeout - timeutil.Since(ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionQueryReceived))
		// There's no need to proceed with execution if the timer has already expired.
		if timerDuration < 0 {
			queryTimedOut = true
			return makeErrEvent(sqlerrors.QueryTimeoutError)
		}
		timeoutTicker = time.AfterFunc(
			timerDuration,
			func() {
				ex.cancelQuery(queryID)
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
			filter(ctx, ex.sessionData, ast.String(), execErr)
		}

		// Do the auto-commit, if necessary.
		if retEv != nil || retErr != nil {
			return
		}
		if os.ImplicitTxn.Get() {
			retEv, retPayload = ex.handleAutoCommit(ctx, ast)
			return
		}
	}()

	switch s := ast.(type) {
	case *tree.BeginTransaction:
		// BEGIN is always an error when in the Open state. It's legitimate only in
		// the NoTxn state.
		return makeErrEvent(errTransactionInProgress)

	case *tree.CommitTransaction:
		// CommitTransaction is executed fully here; there's no plan for it.
		ev, payload := ex.commitSQLTransaction(ctx, ast)
		return ev, payload, nil

	case *tree.RollbackTransaction:
		// RollbackTransaction is executed fully here; there's no plan for it.
		ev, payload := ex.rollbackSQLTransaction(ctx)
		return ev, payload, nil

	case *tree.Savepoint:
		return ex.execSavepointInOpenState(ctx, s, res)

	case *tree.ReleaseSavepoint:
		ev, payload := ex.execRelease(ctx, s, res)
		return ev, payload, nil

	case *tree.RollbackToSavepoint:
		ev, payload := ex.execRollbackToSavepointInOpenState(ctx, s, res)
		return ev, payload, nil

	case *tree.Prepare:
		// This is handling the SQL statement "PREPARE". See execPrepare for
		// handling of the protocol-level command for preparing statements.
		name := s.Name.String()
		if _, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts[name]; ok {
			err := pgerror.Newf(
				pgcode.DuplicatePreparedStatement,
				"prepared statement %q already exists", name,
			)
			return makeErrEvent(err)
		}
		var typeHints tree.PlaceholderTypes
		if len(s.Types) > 0 {
			if len(s.Types) > stmt.NumPlaceholders {
				err := pgerror.Newf(pgcode.Syntax, "too many types provided")
				return makeErrEvent(err)
			}
			typeHints = make(tree.PlaceholderTypes, stmt.NumPlaceholders)
			for i, t := range s.Types {
				resolved, err := tree.ResolveType(ctx, t, ex.planner.semaCtx.GetTypeResolver())
				if err != nil {
					return makeErrEvent(err)
				}
				typeHints[i] = resolved
			}
		}
		prepStmt := makeStatement(
			parser.Statement{
				// We need the SQL string just for the part that comes after
				// "PREPARE ... AS",
				// TODO(radu): it would be nice if the parser would figure out this
				// string and store it in tree.Prepare.
				SQL:             tree.AsStringWithFlags(s.Statement, tree.FmtParsable),
				AST:             s.Statement,
				NumPlaceholders: stmt.NumPlaceholders,
				NumAnnotations:  stmt.NumAnnotations,
			},
			ex.generateID(),
		)
		var rawTypeHints []oid.Oid
		if _, err := ex.addPreparedStmt(
			ctx, name, prepStmt, typeHints, rawTypeHints, PreparedStatementOriginSQL,
		); err != nil {
			return makeErrEvent(err)
		}
		return nil, nil, nil
	}

	p.semaCtx.Annotations = tree.MakeAnnotations(stmt.NumAnnotations)

	// For regular statements (the ones that get to this point), we
	// don't return any event unless an error happens.

	if os.ImplicitTxn.Get() {
		asOfTs, err := p.isAsOf(ctx, ast)
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
		ts, err := p.isAsOf(ctx, ast)
		if err != nil {
			return makeErrEvent(err)
		}
		if ts != nil {
			if readTs := ex.state.getReadTimestamp(); *ts != readTs {
				err = pgerror.Newf(pgcode.Syntax,
					"inconsistent AS OF SYSTEM TIME timestamp; expected: %s", readTs)
				err = errors.WithHint(err, "try SET TRANSACTION AS OF SYSTEM TIME")
				return makeErrEvent(err)
			}
			p.semaCtx.AsOfTimestamp = ts
		}
	}

	// The first order of business is to ensure proper sequencing
	// semantics.  As per PostgreSQL's dialect specs, the "read" part of
	// statements always see the data as per a snapshot of the database
	// taken the instant the statement begins to run. In particular a
	// mutation does not see its own writes. If a query contains
	// multiple mutations using CTEs (WITH) or a read part following a
	// mutation, all still operate on the same read snapshot.
	//
	// (To communicate data between CTEs and a main query, the result
	// set / RETURNING can be used instead. However this is not relevant
	// here.)

	// We first ensure stepping mode is enabled.
	//
	// This ought to be done just once when a txn gets initialized;
	// unfortunately, there are too many places where the txn object
	// is re-configured, re-set etc without using NewTxnWithSteppingEnabled().
	//
	// Manually hunting them down and calling ConfigureStepping() each
	// time would be error prone (and increase the chance that a future
	// change would forget to add the call).
	//
	// TODO(andrei): really the code should be rearchitected to ensure
	// that all uses of SQL execution initialize the client.Txn using a
	// single/common function. That would be where the stepping mode
	// gets enabled once for all SQL statements executed "underneath".
	prevSteppingMode := ex.state.mu.txn.ConfigureStepping(ctx, kv.SteppingEnabled)
	defer func() { _ = ex.state.mu.txn.ConfigureStepping(ctx, prevSteppingMode) }()

	// Then we create a sequencing point.
	//
	// This is not the only place where a sequencing point is
	// placed. There are also sequencing point after every stage of
	// constraint checks and cascading actions at the _end_ of a
	// statement's execution.
	//
	// TODO(knz): At the time of this writing CockroachDB performs
	// cascading actions and the corresponding FK existence checks
	// interleaved with mutations. This is incorrect; the correct
	// behavior, as described in issue
	// https://github.com/cockroachdb/cockroach/issues/33475, is to
	// execute cascading actions no earlier than after all the "main
	// effects" of the current statement (including all its CTEs) have
	// completed. There should be a sequence point between the end of
	// the main execution and the start of the cascading actions, as
	// well as in-between very stage of cascading actions.
	// This TODO can be removed when the cascading code is reorganized
	// accordingly and the missing call to Step() is introduced.
	if err := ex.state.mu.txn.Step(ctx); err != nil {
		return makeErrEvent(err)
	}

	if err := p.semaCtx.Placeholders.Assign(pinfo, stmt.NumPlaceholders); err != nil {
		return makeErrEvent(err)
	}
	p.extendedEvalCtx.Placeholders = &p.semaCtx.Placeholders
	p.extendedEvalCtx.Annotations = &p.semaCtx.Annotations
	p.stmt = stmt
	p.cancelChecker = cancelchecker.NewCancelChecker(ctx)
	p.autoCommit = os.ImplicitTxn.Get() && !ex.server.cfg.TestingKnobs.DisableAutoCommit

	var stmtThresholdSpan *tracing.Span
	alreadyRecording := ex.transitionCtx.sessionTracing.Enabled()
	stmtTraceThreshold := traceStmtThreshold.Get(&ex.planner.execCfg.Settings.SV)
	if !alreadyRecording && stmtTraceThreshold > 0 {
		ctx, stmtThresholdSpan = createRootOrChildSpan(ctx, "trace-stmt-threshold", ex.transitionCtx.tracer, tracing.WithForceRealSpan())
		stmtThresholdSpan.SetVerbose(true)
	}

	if err := ex.dispatchToExecutionEngine(ctx, p, res); err != nil {
		stmtThresholdSpan.Finish()
		return nil, nil, err
	}

	if stmtThresholdSpan != nil {
		stmtThresholdSpan.Finish()
		logTraceAboveThreshold(
			ctx,
			stmtThresholdSpan.GetRecording(),
			fmt.Sprintf("SQL stmt %s", stmt.AST.String()),
			stmtTraceThreshold,
			timeutil.Since(ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionQueryReceived)),
		)
	}

	if err := res.Err(); err != nil {
		return makeErrEvent(err)
	}

	txn := ex.state.mu.txn

	if !os.ImplicitTxn.Get() && txn.IsSerializablePushAndRefreshNotPossible() {
		rc, canAutoRetry := ex.getRewindTxnCapability()
		if canAutoRetry {
			ev := eventRetriableErr{
				IsCommit:     fsm.FromBool(isCommit(ast)),
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
		log.VEventf(ctx, 2, "push detected for non-refreshable txn but auto-retry not possible")
	}

	// No event was generated.
	return nil, nil, nil
}

func formatWithPlaceholders(ast tree.Statement, evalCtx *tree.EvalContext) string {
	var fmtCtx *tree.FmtCtx
	fmtFlags := tree.FmtSimple

	if evalCtx.HasPlaceholders() {
		fmtCtx = evalCtx.FmtCtx(
			fmtFlags,
			tree.FmtPlaceholderFormat(func(ctx *tree.FmtCtx, placeholder *tree.Placeholder) {
				d, err := placeholder.Eval(evalCtx)
				if err != nil {
					// Fall back to the default behavior if something goes wrong.
					ctx.Printf("$%d", placeholder.Idx+1)
					return
				}
				d.Format(ctx)
			}),
		)
	} else {
		fmtCtx = evalCtx.FmtCtx(fmtFlags)
	}

	fmtCtx.FormatNode(ast)

	return fmtCtx.CloseAndGetString()
}

func (ex *connExecutor) checkDescriptorTwoVersionInvariant(ctx context.Context) error {
	var inRetryBackoff func()
	if knobs := ex.server.cfg.SchemaChangerTestingKnobs; knobs != nil {
		inRetryBackoff = knobs.TwoVersionLeaseViolation
	}
	retryErr, err := descs.CheckTwoVersionInvariant(
		ctx,
		ex.server.cfg.Clock,
		ex.server.cfg.InternalExecutor,
		&ex.extraTxnState.descCollection,
		ex.state.mu.txn,
		inRetryBackoff,
	)
	if retryErr {
		// Create a new transaction to retry with a higher timestamp than the
		// timestamps used in the retry loop above.
		userPriority := ex.state.mu.txn.UserPriority()
		ex.state.mu.txn = kv.NewTxnWithSteppingEnabled(ctx, ex.transitionCtx.db, ex.transitionCtx.nodeIDOrZero)
		if err := ex.state.mu.txn.SetUserPriority(userPriority); err != nil {
			return err
		}
	}
	return err
}

// commitSQLTransaction executes a commit after the execution of a
// stmt, which can be any statement when executing a statement with an
// implicit transaction, or a COMMIT statement when using an explicit
// transaction.
func (ex *connExecutor) commitSQLTransaction(
	ctx context.Context, ast tree.Statement,
) (fsm.Event, fsm.EventPayload) {
	ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionStartTransactionCommit, timeutil.Now())
	err := ex.commitSQLTransactionInternal(ctx, ast)
	if err != nil {
		return ex.makeErrEvent(err, ast)
	}
	ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionEndTransactionCommit, timeutil.Now())
	return eventTxnFinishCommitted{}, nil
}

func (ex *connExecutor) commitSQLTransactionInternal(
	ctx context.Context, ast tree.Statement,
) error {
	if ex.extraTxnState.schemaChangerState.mode != sessiondata.UseNewSchemaChangerOff {
		if err := ex.runPreCommitStages(ctx); err != nil {
			return err
		}
	}

	if err := ex.extraTxnState.descCollection.ValidateUncommittedDescriptors(ctx, ex.state.mu.txn); err != nil {
		return err
	}

	if err := ex.checkDescriptorTwoVersionInvariant(ctx); err != nil {
		return err
	}

	if err := ex.state.mu.txn.Commit(ctx); err != nil {
		return err
	}

	// Now that we've committed, if we modified any descriptor we need to make sure
	// to release the leases for them so that the schema change can proceed and
	// we don't block the client.
	if descs := ex.extraTxnState.descCollection.GetDescriptorsWithNewVersion(); descs != nil {
		ex.extraTxnState.descCollection.ReleaseLeases(ctx)
	}
	return nil
}

// rollbackSQLTransaction executes a ROLLBACK statement: the KV transaction is
// rolled-back and an event is produced.
func (ex *connExecutor) rollbackSQLTransaction(ctx context.Context) (fsm.Event, fsm.EventPayload) {
	if err := ex.state.mu.txn.Rollback(ctx); err != nil {
		log.Warningf(ctx, "txn rollback failed: %s", err)
	}
	// We're done with this txn.
	return eventTxnFinishAborted{}, nil
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
	ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.PlannerStartLogicalPlan, timeutil.Now())

	// If adminAuditLogging is enabled, we want to check for HasAdminRole
	// before the deferred maybeLogStatement.
	// We must check prior to execution in the case the txn is aborted due to
	// an error. HasAdminRole can only be checked in a valid txn.
	if adminAuditLog := adminAuditLogEnabled.Get(
		&ex.planner.execCfg.Settings.SV,
	); adminAuditLog {
		if !ex.extraTxnState.hasAdminRoleCache.IsSet {
			hasAdminRole, err := ex.planner.HasAdminRole(ctx)
			if err != nil {
				return err
			}
			ex.extraTxnState.hasAdminRoleCache.HasAdminRole = hasAdminRole
			ex.extraTxnState.hasAdminRoleCache.IsSet = true
		}
	}
	// Prepare the plan. Note, the error is processed below. Everything
	// between here and there needs to happen even if there's an error.
	err := ex.makeExecPlan(ctx, planner)
	// We'll be closing the plan manually below after execution; this
	// defer is a catch-all in case some other return path is taken.
	defer planner.curPlan.close(ctx)

	if planner.autoCommit {
		planner.curPlan.flags.Set(planFlagImplicitTxn)
	}

	// Certain statements want their results to go to the client
	// directly. Configure this here.
	if planner.curPlan.avoidBuffering {
		res.DisableBuffering()
	}

	defer func() {
		planner.maybeLogStatement(
			ctx,
			ex.executorType,
			ex.extraTxnState.autoRetryCounter,
			ex.extraTxnState.txnCounter,
			res.RowsAffected(),
			res.Err(),
			ex.statsCollector.PhaseTimes().GetSessionPhaseTime(sessionphase.SessionQueryReceived),
			&ex.extraTxnState.hasAdminRoleCache,
		)
	}()

	ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.PlannerEndLogicalPlan, timeutil.Now())
	ex.sessionTracing.TracePlanEnd(ctx, err)

	// Finally, process the planning error from above.
	if err != nil {
		res.SetError(err)
		return nil
	}

	var cols colinfo.ResultColumns
	if stmt.AST.StatementReturnType() == tree.Rows {
		cols = planner.curPlan.main.planColumns()
	}
	if err := ex.initStatementResult(ctx, res, stmt.AST, cols); err != nil {
		res.SetError(err)
		return nil
	}

	ex.sessionTracing.TracePlanCheckStart(ctx)
	distributePlan := getPlanDistribution(
		ctx, planner, planner.execCfg.NodeID, ex.sessionData.DistSQLMode, planner.curPlan.main,
	)
	ex.sessionTracing.TracePlanCheckEnd(ctx, nil, distributePlan.WillDistribute())

	if ex.server.cfg.TestingKnobs.BeforeExecute != nil {
		ex.server.cfg.TestingKnobs.BeforeExecute(ctx, stmt.String())
	}

	ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.PlannerStartExecStmt, timeutil.Now())

	ex.mu.Lock()
	queryMeta, ok := ex.mu.ActiveQueries[stmt.QueryID]
	if !ok {
		ex.mu.Unlock()
		panic(errors.AssertionFailedf("query %d not in registry", stmt.QueryID))
	}
	queryMeta.phase = executing
	// TODO(yuzefovich): introduce ternary PlanDistribution into queryMeta.
	queryMeta.isDistributed = distributePlan.WillDistribute()
	progAtomic := &queryMeta.progressAtomic
	ex.mu.Unlock()

	// We need to set the "exec done" flag early because
	// curPlan.close(), which will need to observe it, may be closed
	// during execution (PlanAndRun).
	//
	// TODO(knz): This is a mis-design. Andrei says "it's OK if
	// execution closes the plan" but it transfers responsibility to
	// run any "finalizers" on the plan (including plan sampling for
	// stats) to the execution engine. That's a lot of responsibility
	// to transfer! It would be better if this responsibility remained
	// around here.
	planner.curPlan.flags.Set(planFlagExecDone)
	if !planner.ExecCfg().Codec.ForSystemTenant() {
		planner.curPlan.flags.Set(planFlagTenant)
	}

	switch distributePlan {
	case physicalplan.FullyDistributedPlan:
		planner.curPlan.flags.Set(planFlagFullyDistributed)
	case physicalplan.PartiallyDistributedPlan:
		planner.curPlan.flags.Set(planFlagPartiallyDistributed)
	default:
		planner.curPlan.flags.Set(planFlagNotDistributed)
	}
	ex.sessionTracing.TraceExecStart(ctx, "distributed")
	stats, err := ex.execWithDistSQLEngine(
		ctx, planner, stmt.AST.StatementReturnType(), res, distributePlan.WillDistribute(), progAtomic,
	)
	ex.sessionTracing.TraceExecEnd(ctx, res.Err(), res.RowsAffected())
	ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.PlannerEndExecStmt, timeutil.Now())

	ex.extraTxnState.rowsRead += stats.rowsRead
	ex.extraTxnState.bytesRead += stats.bytesRead

	// Record the statement summary. This also closes the plan if the
	// plan has not been closed earlier.
	ex.recordStatementSummary(
		ctx, planner,
		ex.extraTxnState.autoRetryCounter, res.RowsAffected(), res.Err(), stats,
	)
	if ex.server.cfg.TestingKnobs.AfterExecute != nil {
		ex.server.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), res.Err())
	}

	return err
}

// makeExecPlan creates an execution plan and populates planner.curPlan using
// the cost-based optimizer.
func (ex *connExecutor) makeExecPlan(ctx context.Context, planner *planner) error {
	if err := planner.makeOptimizerPlan(ctx); err != nil {
		log.VEventf(ctx, 1, "optimizer plan failed: %v", err)
		return err
	}

	flags := planner.curPlan.flags

	if flags.IsSet(planFlagContainsFullIndexScan) || flags.IsSet(planFlagContainsFullTableScan) {
		if ex.executorType == executorTypeExec && planner.EvalContext().SessionData.DisallowFullTableScans {
			// We don't execute the statement if:
			// - plan contains a full table or full index scan.
			// - the session setting disallows full table/index scans.
			// - the query is not an internal query.
			return errors.WithHint(
				pgerror.Newf(pgcode.TooManyRows,
					"query `%s` contains a full table/index scan which is explicitly disallowed",
					planner.stmt.SQL),
				"try overriding the `disallow_full_table_scans` cluster/session setting")
		}
		ex.metrics.EngineMetrics.FullTableOrIndexScanCount.Inc(1)
	}

	// TODO(knz): Remove this accounting if/when savepoint rollbacks
	// support rolling back over DDL.
	if flags.IsSet(planFlagIsDDL) {
		ex.extraTxnState.numDDL++
	}

	return nil
}

// topLevelQueryStats returns some basic statistics about the run of the query.
type topLevelQueryStats struct {
	// bytesRead is the number of bytes read from disk.
	bytesRead int64
	// rowsRead is the number of rows read from disk.
	rowsRead int64
}

// execWithDistSQLEngine converts a plan to a distributed SQL physical plan and
// runs it.
// If an error is returned, the connection needs to stop processing queries.
// Query execution errors are written to res; they are not returned.
func (ex *connExecutor) execWithDistSQLEngine(
	ctx context.Context,
	planner *planner,
	stmtType tree.StatementReturnType,
	res RestrictedCommandResult,
	distribute bool,
	progressAtomic *uint64,
) (topLevelQueryStats, error) {
	var testingPushCallback func(rowenc.EncDatumRow, *execinfrapb.ProducerMetadata)
	if ex.server.cfg.TestingKnobs.DistSQLReceiverPushCallbackFactory != nil {
		testingPushCallback = ex.server.cfg.TestingKnobs.DistSQLReceiverPushCallbackFactory(planner.stmt.SQL)
	}
	recv := MakeDistSQLReceiver(
		ctx, res, stmtType,
		ex.server.cfg.RangeDescriptorCache,
		planner.txn,
		ex.server.cfg.Clock,
		&ex.sessionTracing,
		ex.server.cfg.ContentionRegistry,
		testingPushCallback,
	)
	recv.progressAtomic = progressAtomic
	defer recv.Release()

	evalCtx := planner.ExtendedEvalContext()
	planCtx := ex.server.cfg.DistSQLPlanner.NewPlanningCtx(ctx, evalCtx, planner, planner.txn, distribute)
	planCtx.stmtType = recv.stmtType
	if ex.server.cfg.TestingKnobs.TestingSaveFlows != nil {
		planCtx.saveFlows = ex.server.cfg.TestingKnobs.TestingSaveFlows(planner.stmt.SQL)
	} else if planner.instrumentation.ShouldSaveFlows() {
		planCtx.saveFlows = planCtx.getDefaultSaveFlowsFunc(ctx, planner, planComponentTypeMainQuery)
	}
	planCtx.traceMetadata = planner.instrumentation.traceMetadata
	planCtx.collectExecStats = planner.instrumentation.ShouldCollectExecStats()

	var evalCtxFactory func() *extendedEvalContext
	if len(planner.curPlan.subqueryPlans) != 0 ||
		len(planner.curPlan.cascades) != 0 ||
		len(planner.curPlan.checkPlans) != 0 {
		// The factory reuses the same object because the contexts are not used
		// concurrently.
		var factoryEvalCtx extendedEvalContext
		ex.initEvalCtx(ctx, &factoryEvalCtx, planner)
		evalCtxFactory = func() *extendedEvalContext {
			ex.resetEvalCtx(&factoryEvalCtx, planner.txn, planner.ExtendedEvalContext().StmtTimestamp)
			factoryEvalCtx.Placeholders = &planner.semaCtx.Placeholders
			factoryEvalCtx.Annotations = &planner.semaCtx.Annotations
			// Query diagnostics can change the Context; make sure we are using the
			// same one.
			// TODO(radu): consider removing this if/when #46164 is addressed.
			factoryEvalCtx.Context = evalCtx.Context
			return &factoryEvalCtx
		}
	}

	if len(planner.curPlan.subqueryPlans) != 0 {
		// Create a separate memory account for the results of the subqueries.
		// Note that we intentionally defer the closure of the account until we
		// return from this method (after the main query is executed).
		subqueryResultMemAcc := planner.EvalContext().Mon.MakeBoundAccount()
		defer subqueryResultMemAcc.Close(ctx)
		if !ex.server.cfg.DistSQLPlanner.PlanAndRunSubqueries(
			ctx, planner, evalCtxFactory, planner.curPlan.subqueryPlans, recv, &subqueryResultMemAcc,
		) {
			return recv.stats, recv.commErr
		}
	}
	recv.discardRows = planner.instrumentation.ShouldDiscardRows()
	// We pass in whether or not we wanted to distribute this plan, which tells
	// the planner whether or not to plan remote table readers.
	cleanup := ex.server.cfg.DistSQLPlanner.PlanAndRun(
		ctx, evalCtx, planCtx, planner.txn, planner.curPlan.main, recv,
	)
	// Note that we're not cleaning up right away because postqueries might
	// need to have access to the main query tree.
	defer cleanup()
	if recv.commErr != nil || res.Err() != nil {
		return recv.stats, recv.commErr
	}

	ex.server.cfg.DistSQLPlanner.PlanAndRunCascadesAndChecks(
		ctx, planner, evalCtxFactory, &planner.curPlan.planComponents, recv,
	)

	return recv.stats, recv.commErr
}

// beginTransactionTimestampsAndReadMode computes the timestamps and
// ReadWriteMode to be used for the associated transaction state based on the
// values of the BeginTransaction statement's Modes, along with the session's
// default transaction settings. If no BeginTransaction statement is provided
// then the session's defaults are consulted alone.
//
// Note that this method may reset the connExecutor's planner in order to
// compute the timestamp for the AsOf clause if it exists. The timestamps
// correspond to the timestamps passed to makeEventTxnStartPayload;
// txnSQLTimestamp propagates to become the TxnTimestamp while
// historicalTimestamp populated with a non-nil value only if the
// BeginTransaction statement has a non-nil AsOf clause expression. A
// non-nil historicalTimestamp implies a ReadOnly rwMode.
func (ex *connExecutor) beginTransactionTimestampsAndReadMode(
	ctx context.Context, s *tree.BeginTransaction,
) (
	rwMode tree.ReadWriteMode,
	txnSQLTimestamp time.Time,
	historicalTimestamp *hlc.Timestamp,
	err error,
) {
	now := ex.server.cfg.Clock.PhysicalTime()
	var modes tree.TransactionModes
	if s != nil {
		modes = s.Modes
	}
	asOf := ex.asOfClauseWithSessionDefault(modes.AsOf)
	if asOf.Expr == nil {
		rwMode = ex.readWriteModeWithSessionDefault(modes.ReadWriteMode)
		return rwMode, now, nil, nil
	}
	ex.statsCollector.Reset(ex.statsWriter, ex.phaseTimes)
	p := &ex.planner

	// NB: this use of p.txn is totally bogus. The planner's txn should
	// definitely be finalized at this point. We preserve it here because we
	// need to make sure that the planner's txn is not made to be nil in the
	// case of an error below. The planner's txn is never written to nil at
	// any other point after the first prepare or exec has been run. We abuse
	// this transaction in bind and some other contexts for resolving types and
	// oids. Avoiding set this to nil side-steps a nil pointer panic but is still
	// awful. Instead we ought to clear the planner state when we clear the reset
	// the connExecutor in finishTxn.
	ex.resetPlanner(ctx, p, p.txn, now)
	ts, err := p.EvalAsOfTimestamp(ctx, asOf)
	if err != nil {
		return 0, time.Time{}, nil, err
	}
	// NB: This check should never return an error because the parser should
	// disallow the creation of a TransactionModes struct which both has an
	// AOST clause and is ReadWrite but performing a check decouples this code
	// from that and hopefully adds clarity that the returning of ReadOnly with
	// a historical timestamp is intended.
	if modes.ReadWriteMode == tree.ReadWrite {
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
	ctx context.Context, ast tree.Statement,
) (_ fsm.Event, payload fsm.EventPayload) {
	switch s := ast.(type) {
	case *tree.BeginTransaction:
		ex.incrementStartedStmtCounter(ast)
		defer func() {
			if !payloadHasError(payload) {
				ex.incrementExecutedStmtCounter(ast)
			}
		}()
		mode, sqlTs, historicalTs, err := ex.beginTransactionTimestampsAndReadMode(ctx, s)
		if err != nil {
			return ex.makeErrEvent(err, s)
		}
		return eventTxnStart{ImplicitTxn: fsm.False},
			makeEventTxnStartPayload(
				ex.txnPriorityWithSessionDefault(s.Modes.UserPriority),
				mode,
				sqlTs,
				historicalTs,
				ex.transitionCtx)
	case *tree.CommitTransaction, *tree.ReleaseSavepoint,
		*tree.RollbackTransaction, *tree.SetTransaction, *tree.Savepoint:
		return ex.makeErrEvent(errNoTransactionInProgress, ast)
	default:
		// NB: Implicit transactions are created with the session's default
		// historical timestamp even though the statement itself might contain
		// an AOST clause. In these cases the clause is evaluated and applied
		// execStmtInOpenState.
		noBeginStmt := (*tree.BeginTransaction)(nil)
		mode, sqlTs, historicalTs, err := ex.beginTransactionTimestampsAndReadMode(ctx, noBeginStmt)
		if err != nil {
			return ex.makeErrEvent(err, s)
		}
		return eventTxnStart{ImplicitTxn: fsm.True},
			makeEventTxnStartPayload(
				ex.txnPriorityWithSessionDefault(tree.UnspecifiedUserPriority),
				mode,
				sqlTs,
				historicalTs,
				ex.transitionCtx)
	}
}

// execStmtInAbortedState executes a statement in a txn that's in state
// Aborted or RestartWait. All statements result in error events except:
// - COMMIT / ROLLBACK: aborts the current transaction.
// - ROLLBACK TO SAVEPOINT / SAVEPOINT: reopens the current transaction,
//   allowing it to be retried.
func (ex *connExecutor) execStmtInAbortedState(
	ctx context.Context, ast tree.Statement, res RestrictedCommandResult,
) (_ fsm.Event, payload fsm.EventPayload) {
	ex.incrementStartedStmtCounter(ast)
	defer func() {
		if !payloadHasError(payload) {
			ex.incrementExecutedStmtCounter(ast)
		}
	}()

	reject := func() (fsm.Event, fsm.EventPayload) {
		ev := eventNonRetriableErr{IsCommit: fsm.False}
		payload := eventNonRetriableErrPayload{
			err: sqlerrors.NewTransactionAbortedError("" /* customMsg */),
		}
		return ev, payload
	}

	switch s := ast.(type) {
	case *tree.CommitTransaction, *tree.RollbackTransaction:
		if _, ok := s.(*tree.CommitTransaction); ok {
			// Note: Postgres replies to COMMIT of failed txn with "ROLLBACK" too.
			res.ResetStmtType((*tree.RollbackTransaction)(nil))
		}
		return ex.rollbackSQLTransaction(ctx)

	case *tree.RollbackToSavepoint:
		return ex.execRollbackToSavepointInAbortedState(ctx, s)

	case *tree.Savepoint:
		if ex.isCommitOnReleaseSavepoint(s.Name) {
			// We allow SAVEPOINT cockroach_restart as an alternative to ROLLBACK TO
			// SAVEPOINT cockroach_restart in the Aborted state. This is needed
			// because any client driver (that we know of) which links subtransaction
			// `ROLLBACK/RELEASE` to an object's lifetime will fail to `ROLLBACK` on a
			// failed `RELEASE`. Instead, we now can use the creation of another
			// subtransaction object (which will issue another `SAVEPOINT` statement)
			// to indicate retry intent. Specifically, this change was prompted by
			// subtransaction handling in `libpqxx` (C++ driver) and `rust-postgres`
			// (Rust driver).
			res.ResetStmtType((*tree.RollbackToSavepoint)(nil))
			return ex.execRollbackToSavepointInAbortedState(
				ctx, &tree.RollbackToSavepoint{Savepoint: s.Name})
		}
		return reject()

	default:
		return reject()
	}
}

// execStmtInCommitWaitState executes a statement in a txn that's in state
// CommitWait.
// Everything but COMMIT/ROLLBACK causes errors. ROLLBACK is treated like COMMIT.
func (ex *connExecutor) execStmtInCommitWaitState(
	ast tree.Statement, res RestrictedCommandResult,
) (ev fsm.Event, payload fsm.EventPayload) {
	ex.incrementStartedStmtCounter(ast)
	defer func() {
		if !payloadHasError(payload) {
			ex.incrementExecutedStmtCounter(ast)
		}
	}()
	switch ast.(type) {
	case *tree.CommitTransaction, *tree.RollbackTransaction:
		// Reply to a rollback with the COMMIT tag, by analogy to what we do when we
		// get a COMMIT in state Aborted.
		res.ResetStmtType((*tree.CommitTransaction)(nil))
		return eventTxnFinishCommitted{}, nil
	default:
		ev = eventNonRetriableErr{IsCommit: fsm.False}
		payload = eventNonRetriableErrPayload{
			err: sqlerrors.NewTransactionCommittedError(),
		}
		return ev, payload
	}
}

// runObserverStatement executes the given observer statement.
//
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) runObserverStatement(
	ctx context.Context, ast tree.Statement, res RestrictedCommandResult,
) error {
	switch sqlStmt := ast.(type) {
	case *tree.ShowTransactionStatus:
		return ex.runShowTransactionState(ctx, res)
	case *tree.ShowSavepointStatus:
		return ex.runShowSavepointState(ctx, res)
	case *tree.ShowSyntax:
		return ex.runShowSyntax(ctx, sqlStmt.Statement, res)
	case *tree.SetTracing:
		ex.runSetTracing(ctx, sqlStmt, res)
		return nil
	case *tree.ShowLastQueryStatistics:
		return ex.runShowLastQueryStatistics(ctx, res)
	default:
		res.SetError(errors.AssertionFailedf("unrecognized observer statement type %T", ast))
		return nil
	}
}

// runShowSyntax executes a SHOW SYNTAX <stmt> query.
//
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) runShowSyntax(
	ctx context.Context, stmt string, res RestrictedCommandResult,
) error {
	res.SetColumns(ctx, colinfo.ShowSyntaxColumns)
	var commErr error
	parser.RunShowSyntax(ctx, stmt,
		func(ctx context.Context, field, msg string) {
			commErr = res.AddRow(ctx, tree.Datums{tree.NewDString(field), tree.NewDString(msg)})
		},
		func(ctx context.Context, err error) {
			sqltelemetry.RecordError(ctx, err, &ex.server.cfg.Settings.SV)
		},
	)
	return commErr
}

// runShowTransactionState executes a SHOW TRANSACTION STATUS statement.
//
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) runShowTransactionState(
	ctx context.Context, res RestrictedCommandResult,
) error {
	res.SetColumns(ctx, colinfo.ResultColumns{{Name: "TRANSACTION STATUS", Typ: types.String}})

	state := fmt.Sprintf("%s", ex.machine.CurState())
	return res.AddRow(ctx, tree.Datums{tree.NewDString(state)})
}

func (ex *connExecutor) runShowLastQueryStatistics(
	ctx context.Context, res RestrictedCommandResult,
) error {
	res.SetColumns(ctx, colinfo.ShowLastQueryStatisticsColumns)

	phaseTimes := ex.statsCollector.PreviousPhaseTimes()
	runLat := phaseTimes.GetRunLatency().Seconds()
	parseLat := phaseTimes.GetParsingLatency().Seconds()
	planLat := phaseTimes.GetPlanningLatency().Seconds()
	// Since the last query has already finished, it is safe to retrieve its
	// total service latency.
	svcLat := phaseTimes.GetServiceLatencyTotal().Seconds()
	postCommitJobsLat := phaseTimes.GetPostCommitJobsLatency().Seconds()

	return res.AddRow(ctx,
		tree.Datums{
			tree.NewDInterval(duration.FromFloat64(parseLat), types.DefaultIntervalTypeMetadata),
			tree.NewDInterval(duration.FromFloat64(planLat), types.DefaultIntervalTypeMetadata),
			tree.NewDInterval(duration.FromFloat64(runLat), types.DefaultIntervalTypeMetadata),
			tree.NewDInterval(duration.FromFloat64(svcLat), types.DefaultIntervalTypeMetadata),
			tree.NewDInterval(duration.FromFloat64(postCommitJobsLat), types.DefaultIntervalTypeMetadata),
		})
}

func (ex *connExecutor) runSetTracing(
	ctx context.Context, n *tree.SetTracing, res RestrictedCommandResult,
) {
	if len(n.Values) == 0 {
		res.SetError(errors.AssertionFailedf("set tracing missing argument"))
		return
	}

	modes := make([]string, len(n.Values))
	for i, v := range n.Values {
		v = paramparse.UnresolvedNameToStrVal(v)
		var strMode string
		switch val := v.(type) {
		case *tree.StrVal:
			strMode = val.RawString()
		case *tree.DBool:
			if *val {
				strMode = "on"
			} else {
				strMode = "off"
			}
		default:
			res.SetError(pgerror.New(pgcode.Syntax,
				"expected string or boolean for set tracing argument"))
			return
		}
		modes[i] = strMode
	}

	if err := ex.enableTracing(modes); err != nil {
		res.SetError(err)
	}
}

func (ex *connExecutor) enableTracing(modes []string) error {
	traceKV := false
	recordingType := tracing.RecordingVerbose
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
		case "cluster":
			recordingType = tracing.RecordingVerbose
		default:
			return pgerror.Newf(pgcode.Syntax,
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
	ast tree.Statement, rawStmt string, queryID ClusterWideID, cancelFun context.CancelFunc,
) func() {
	_, hidden := ast.(tree.HiddenFromShowQueries)
	qm := &queryMeta{
		txnID:         ex.state.mu.txn.ID(),
		start:         ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionQueryReceived),
		rawStmt:       rawStmt,
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
			panic(errors.AssertionFailedf("query %d missing from ActiveQueries", queryID))
		}
		delete(ex.mu.ActiveQueries, queryID)
		ex.mu.LastActiveQuery = ast

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
		log.Event(ctx, "statement execution committed the txn")
		return eventTxnFinishCommitted{}, nil
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
func (ex *connExecutor) incrementStartedStmtCounter(ast tree.Statement) {
	ex.metrics.StartedStatementCounters.incrementCount(ex, ast)
}

// incrementExecutedStmtCounter increments the appropriate executed
// statement counter for stmt's type.
func (ex *connExecutor) incrementExecutedStmtCounter(ast tree.Statement) {
	ex.metrics.ExecutedStatementCounters.incrementCount(ex, ast)
}

// payloadHasError returns true if the passed payload implements
// payloadWithError.
func payloadHasError(payload fsm.EventPayload) bool {
	_, hasErr := payload.(payloadWithError)
	return hasErr
}

// recordTransactionStart records the start of the transaction and returns
// closures to be called once the transaction finishes or if the transaction
// restarts.
func (ex *connExecutor) recordTransactionStart() (
	onTxnFinish func(context.Context, txnEvent),
	onTxnRestart func(),
) {
	ex.state.mu.RLock()
	txnStart := ex.state.mu.txnStart
	ex.state.mu.RUnlock()
	implicit := ex.implicitTxn()

	// Transaction received time is the time at which the statement that prompted
	// the creation of this transaction was received.
	ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionTransactionReceived,
		ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionQueryReceived))
	ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionFirstStartExecTransaction, timeutil.Now())
	ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionMostRecentStartExecTransaction,
		ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionFirstStartExecTransaction))
	ex.extraTxnState.transactionStatementsHash = util.MakeFNV64()
	ex.extraTxnState.transactionStatementFingerprintIDs = nil
	ex.extraTxnState.numRows = 0
	ex.extraTxnState.shouldCollectTxnExecutionStats = false
	ex.extraTxnState.accumulatedStats = execstats.QueryLevelStats{}
	ex.extraTxnState.rowsRead = 0
	ex.extraTxnState.bytesRead = 0
	if txnExecStatsSampleRate := collectTxnStatsSampleRate.Get(&ex.server.GetExecutorConfig().Settings.SV); txnExecStatsSampleRate > 0 {
		ex.extraTxnState.shouldCollectTxnExecutionStats = txnExecStatsSampleRate > ex.rng.Float64()
	}

	ex.metrics.EngineMetrics.SQLTxnsOpen.Inc(1)

	onTxnFinish = func(ctx context.Context, ev txnEvent) {
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionEndExecTransaction, timeutil.Now())
		err := ex.recordTransaction(ctx, ev, implicit, txnStart)
		if err != nil {
			if log.V(1) {
				log.Warningf(ctx, "failed to record transaction stats: %s", err)
			}
			ex.metrics.StatsMetrics.DiscardedStatsCount.Inc(1)
		}
	}
	onTxnRestart = func() {
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionMostRecentStartExecTransaction, timeutil.Now())
		ex.extraTxnState.transactionStatementFingerprintIDs = nil
		ex.extraTxnState.transactionStatementsHash = util.MakeFNV64()
		ex.extraTxnState.numRows = 0
		// accumulatedStats are cleared, but shouldCollectTxnExecutionStats is
		// unchanged.
		ex.extraTxnState.accumulatedStats = execstats.QueryLevelStats{}
		ex.extraTxnState.rowsRead = 0
		ex.extraTxnState.bytesRead = 0
	}
	return onTxnFinish, onTxnRestart
}

func (ex *connExecutor) recordTransaction(
	ctx context.Context, ev txnEvent, implicit bool, txnStart time.Time,
) error {
	txnEnd := timeutil.Now()
	txnTime := txnEnd.Sub(txnStart)
	ex.metrics.EngineMetrics.SQLTxnsOpen.Dec(1)
	ex.metrics.EngineMetrics.SQLTxnLatency.RecordValue(txnTime.Nanoseconds())

	txnServiceLat := ex.phaseTimes.GetTransactionServiceLatency()
	txnRetryLat := ex.phaseTimes.GetTransactionRetryLatency()
	commitLat := ex.phaseTimes.GetCommitLatency()

	recordedTxnStats := sqlstats.RecordedTxnStats{
		TransactionTimeSec:      txnTime.Seconds(),
		Committed:               ev == txnCommit,
		ImplicitTxn:             implicit,
		RetryCount:              int64(ex.extraTxnState.autoRetryCounter),
		StatementFingerprintIDs: ex.extraTxnState.transactionStatementFingerprintIDs,
		ServiceLatency:          txnServiceLat,
		RetryLatency:            txnRetryLat,
		CommitLatency:           commitLat,
		RowsAffected:            ex.extraTxnState.numRows,
		CollectedExecStats:      ex.extraTxnState.shouldCollectTxnExecutionStats,
		ExecStats:               ex.extraTxnState.accumulatedStats,
		RowsRead:                ex.extraTxnState.rowsRead,
		BytesRead:               ex.extraTxnState.bytesRead,
	}

	return ex.statsCollector.RecordTransaction(
		ctx,
		roachpb.TransactionFingerprintID(ex.extraTxnState.transactionStatementsHash.Sum()),
		recordedTxnStats,
	)
}

// createRootOrChildSpan is used to create spans for txns and stmts. It inspects
// parentCtx for an existing span and creates a root span if none is found, or a
// child span if one is found. A context derived from parentCtx which
// additionally contains the new span is also returned.
func createRootOrChildSpan(
	parentCtx context.Context, opName string, tr *tracing.Tracer, os ...tracing.SpanOption,
) (context.Context, *tracing.Span) {
	return tracing.EnsureChildSpan(parentCtx, tr, opName, os...)
}

// logTraceAboveThreshold logs a span's recording if the duration is above a
// given threshold. It is used when txn or stmt threshold tracing is enabled.
// This function assumes that sp is non-nil and threshold tracing was enabled.
func logTraceAboveThreshold(
	ctx context.Context, r tracing.Recording, opName string, threshold, elapsed time.Duration,
) {
	if elapsed < threshold {
		return
	}
	if r == nil {
		log.Warning(ctx, "missing trace when threshold tracing was enabled")
		return
	}
	dump := r.String()
	if len(dump) == 0 {
		return
	}
	// Note that log lines larger than 65k are truncated in the debug zip (see
	// #50166).
	log.Infof(ctx, "%s took %s, exceeding threshold of %s:\n%s", opName, elapsed, threshold, dump)
}
