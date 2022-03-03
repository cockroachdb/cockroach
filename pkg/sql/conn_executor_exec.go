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
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/sql/delegate"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
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
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
	"go.opentelemetry.io/otel/attribute"
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
	canAutoCommit bool,
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
		ex.statsCollector.Reset(ex.applicationStats, ex.phaseTimes)
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
			if rAddr := ex.sessionData().RemoteAddr; rAddr != nil {
				remoteAddr = rAddr.String()
			}
			var stmtNoConstants string
			if prepared != nil {
				stmtNoConstants = prepared.StatementNoConstants
			} else {
				stmtNoConstants = formatStatementHideConstants(ast)
			}
			labels := pprof.Labels(
				"appname", ex.sessionData().ApplicationName,
				"addr", remoteAddr,
				"stmt.tag", ast.StatementTag(),
				"stmt.no.constants", stmtNoConstants,
			)
			pprof.Do(ctx, labels, func(ctx context.Context) {
				ev, payload, err = ex.execStmtInOpenState(ctx, parserStmt, prepared, pinfo, res, canAutoCommit)
			})
		} else {
			ev, payload, err = ex.execStmtInOpenState(ctx, parserStmt, prepared, pinfo, res, canAutoCommit)
		}
		switch ev.(type) {
		case eventNonRetriableErr:
			ex.recordFailure()
		}

	case stateAborted:
		ev, payload = ex.execStmtInAbortedState(ctx, ast, res)

	case stateCommitWait:
		ev, payload = ex.execStmtInCommitWaitState(ctx, ast, res)

	default:
		panic(errors.AssertionFailedf("unexpected txn state: %#v", ex.machine.CurState()))
	}

	if ex.sessionData().IdleInSessionTimeout > 0 {
		// Cancel the session if the idle time exceeds the idle in session timeout.
		ex.mu.IdleInSessionTimeout = timeout{time.AfterFunc(
			ex.sessionData().IdleInSessionTimeout,
			ex.cancelSession,
		)}
	}

	if ex.sessionData().IdleInTransactionSessionTimeout > 0 {
		startIdleInTransactionSessionTimeout := func() {
			switch ast.(type) {
			case *tree.CommitTransaction, *tree.RollbackTransaction:
				// Do nothing, the transaction is completed, we do not want to start
				// an idle timer.
			default:
				ex.mu.IdleInTransactionSessionTimeout = timeout{time.AfterFunc(
					ex.sessionData().IdleInTransactionSessionTimeout,
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
	canAutoCommit bool,
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
		ev, payload, err = ex.execStmt(ctx, portal.Stmt.Statement, portal.Stmt, pinfo, stmtRes, canAutoCommit)
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
		return ex.execStmt(ctx, portal.Stmt.Statement, portal.Stmt, pinfo, stmtRes, canAutoCommit)
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
	canAutoCommit bool,
) (retEv fsm.Event, retPayload fsm.EventPayload, retErr error) {
	ctx, sp := tracing.EnsureChildSpan(ctx, ex.server.cfg.AmbientCtx.Tracer, "sql query")
	// TODO(andrei): Consider adding the placeholders as tags too.
	sp.SetTag("statement", attribute.StringValue(parserStmt.SQL))
	defer sp.Finish()
	ast := parserStmt.AST
	ctx = withStatement(ctx, ast)

	makeErrEvent := func(err error) (fsm.Event, fsm.EventPayload, error) {
		ev, payload := ex.makeErrEvent(err, ast)
		return ev, payload, nil
	}

	var stmt Statement
	queryID := ex.generateID()
	// Update the deadline on the transaction based on the collections.
	err := ex.extraTxnState.descCollection.MaybeUpdateDeadline(ctx, ex.state.mu.txn)
	if err != nil {
		return makeErrEvent(err)
	}
	os := ex.machine.CurState().(stateOpen)

	isExtendedProtocol := prepared != nil
	if isExtendedProtocol {
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

	var timeoutTicker *time.Timer
	queryTimedOut := false
	// doneAfterFunc will be allocated only when timeoutTicker is non-nil.
	var doneAfterFunc chan struct{}

	// Early-associate placeholder info with the eval context,
	// so that we can fill in placeholder values in our call to addActiveQuery, below.
	if !ex.planner.EvalContext().HasPlaceholders() {
		ex.planner.EvalContext().Placeholders = pinfo
	}

	ex.addActiveQuery(ast, formatWithPlaceholders(ast, ex.planner.EvalContext()), queryID, ex.state.cancel)
	if ex.executorType != executorTypeInternal {
		ex.metrics.EngineMetrics.SQLActiveStatements.Inc(1)
	}

	// Make sure that we always unregister the query. It also deals with
	// overwriting res.Error to a more user-friendly message in case of query
	// cancellation.
	defer func(ctx context.Context, res RestrictedCommandResult) {
		if timeoutTicker != nil {
			if !timeoutTicker.Stop() {
				// Wait for the timer callback to complete to avoid a data race on
				// queryTimedOut.
				<-doneAfterFunc
			}
		}
		ex.removeActiveQuery(queryID, ast)
		if ex.executorType != executorTypeInternal {
			ex.metrics.EngineMetrics.SQLActiveStatements.Dec(1)
		}

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
	}(ctx, res)

	p := &ex.planner
	stmtTS := ex.server.cfg.Clock.PhysicalTime()
	ex.statsCollector.Reset(ex.applicationStats, ex.phaseTimes)
	ex.resetPlanner(ctx, p, ex.state.mu.txn, stmtTS)
	p.sessionDataMutatorIterator.paramStatusUpdater = res
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
		pinfo, err = ex.planner.fillInPlaceholders(ctx, ps, name, e.Params)
		if err != nil {
			return makeErrEvent(err)
		}

		// TODO(radu): what about .SQL, .NumAnnotations, .NumPlaceholders?
		stmt.Statement = ps.Statement
		stmt.Prepared = ps
		stmt.ExpectedTypes = ps.Columns
		stmt.StmtNoConstants = ps.StatementNoConstants
		res.ResetStmtType(ps.AST)

		if e.DiscardRows {
			ih.SetDiscardRows()
		}
		ast = stmt.Statement.AST
	}

	var needFinish bool
	ctx, needFinish = ih.Setup(
		ctx, ex.server.cfg, ex.statsCollector, p, ex.stmtDiagnosticsRecorder,
		stmt.StmtNoConstants, os.ImplicitTxn.Get(), ex.extraTxnState.shouldCollectTxnExecutionStats,
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
	if ex.sessionData().StmtTimeout > 0 && ast.StatementTag() != "SET" {
		timerDuration :=
			ex.sessionData().StmtTimeout - timeutil.Since(ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionQueryReceived))
		// There's no need to proceed with execution if the timer has already expired.
		if timerDuration < 0 {
			queryTimedOut = true
			return makeErrEvent(sqlerrors.QueryTimeoutError)
		}
		doneAfterFunc = make(chan struct{}, 1)
		timeoutTicker = time.AfterFunc(
			timerDuration,
			func() {
				ex.cancelQuery(queryID)
				queryTimedOut = true
				doneAfterFunc <- struct{}{}
			})
	}

	defer func(ctx context.Context) {
		if filter := ex.server.cfg.TestingKnobs.StatementFilter; retErr == nil && filter != nil {
			var execErr error
			if perr, ok := retPayload.(payloadWithError); ok {
				execErr = perr.errorCause()
			}
			filter(ctx, ex.sessionData(), ast.String(), execErr)
		}

		// Do the auto-commit, if necessary. In the extended protocol, the
		// auto-commit happens when the Sync message is handled.
		if retEv != nil || retErr != nil {
			return
		}
		if canAutoCommit && !isExtendedProtocol {
			retEv, retPayload = ex.handleAutoCommit(ctx, ast)
		}
	}(ctx)

	switch s := ast.(type) {
	case *tree.BeginTransaction:
		// BEGIN is only allowed if we are in an implicit txn that was started
		// in the extended protocol.
		if isExtendedProtocol && os.ImplicitTxn.Get() {
			ex.sessionDataStack.PushTopClone()
			return eventTxnUpgradeToExplicit{}, nil, nil
		}
		return makeErrEvent(errTransactionInProgress)

	case *tree.CommitTransaction:
		// CommitTransaction is executed fully here; there's no plan for it.
		ev, payload := ex.commitSQLTransaction(ctx, ast, ex.commitSQLTransactionInternal)
		return ev, payload, nil

	case *tree.RollbackTransaction:
		// RollbackTransaction is executed fully here; there's no plan for it.
		ev, payload := ex.rollbackSQLTransaction(ctx, s)
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

	if err := ex.handleAOST(ctx, ast); err != nil {
		return makeErrEvent(err)
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
	p.cancelChecker.Reset(ctx)

	p.autoCommit = canAutoCommit && !ex.server.cfg.TestingKnobs.DisableAutoCommitDuringExec

	var stmtThresholdSpan *tracing.Span
	alreadyRecording := ex.transitionCtx.sessionTracing.Enabled()
	stmtTraceThreshold := TraceStmtThreshold.Get(&ex.planner.execCfg.Settings.SV)
	var stmtCtx context.Context
	// TODO(andrei): I think we should do this even if alreadyRecording == true.
	if !alreadyRecording && stmtTraceThreshold > 0 {
		stmtCtx, stmtThresholdSpan = tracing.EnsureChildSpan(ctx, ex.server.cfg.AmbientCtx.Tracer, "trace-stmt-threshold", tracing.WithRecording(tracing.RecordingVerbose))
	} else {
		stmtCtx = ctx
	}

	if err := ex.dispatchToExecutionEngine(stmtCtx, p, res); err != nil {
		stmtThresholdSpan.Finish()
		return nil, nil, err
	}

	if stmtThresholdSpan != nil {
		stmtDur := timeutil.Since(ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionQueryReceived))
		needRecording := stmtTraceThreshold < stmtDur
		if needRecording {
			rec := stmtThresholdSpan.FinishAndGetRecording(tracing.RecordingVerbose)
			// NB: This recording does not include the commit for implicit
			// transactions if the statement didn't auto-commit.
			logTraceAboveThreshold(
				ctx,
				rec,
				fmt.Sprintf("SQL stmt %s", stmt.AST.String()),
				stmtTraceThreshold,
				stmtDur,
			)
		} else {
			stmtThresholdSpan.Finish()
		}
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
				err:    txn.PrepareRetryableError(ctx, "serializable transaction timestamp pushed (detected by connExecutor)"),
				rewCap: rc,
			}
			return ev, payload, nil
		}
		log.VEventf(ctx, 2, "push detected for non-refreshable txn but auto-retry not possible")
	}

	// No event was generated.
	return nil, nil, nil
}

// handleAOST gets the AsOfSystemTime clause from the statement, and sets
// the timestamps of the transaction accordingly.
func (ex *connExecutor) handleAOST(ctx context.Context, stmt tree.Statement) error {
	if _, isNoTxn := ex.machine.CurState().(stateNoTxn); isNoTxn {
		return errors.AssertionFailedf(
			"cannot handle AOST clause without a transaction",
		)
	}
	p := &ex.planner
	asOf, err := p.isAsOf(ctx, stmt)
	if err != nil {
		return err
	}
	if asOf == nil {
		return nil
	}
	if ex.implicitTxn() {
		if p.extendedEvalCtx.AsOfSystemTime == nil {
			p.extendedEvalCtx.AsOfSystemTime = asOf
			if !asOf.BoundedStaleness {
				p.extendedEvalCtx.SetTxnTimestamp(asOf.Timestamp.GoTime())
				if err := ex.state.setHistoricalTimestamp(ctx, asOf.Timestamp); err != nil {
					return err
				}
			}
			return nil
		}
		if *p.extendedEvalCtx.AsOfSystemTime == *asOf {
			// In most cases, the AOST timestamps are expected to match.
			return nil
		}
		if p.extendedEvalCtx.AsOfSystemTime.BoundedStaleness {
			if !p.extendedEvalCtx.AsOfSystemTime.MaxTimestampBound.IsEmpty() {
				// This has to be a bounded staleness read with nearest_only=True during
				// a retry. The AOST read timestamps are expected to differ.
				return nil
			}
			return errors.AssertionFailedf("expected bounded_staleness set with a max_timestamp_bound")
		}
		return errors.AssertionFailedf(
			"cannot specify AS OF SYSTEM TIME with different timestamps. expected: %s, got: %s",
			p.extendedEvalCtx.AsOfSystemTime.Timestamp, asOf.Timestamp,
		)
	}
	// If we're in an explicit txn, we allow AOST but only if it matches with
	// the transaction's timestamp. This is useful for running AOST statements
	// using the InternalExecutor inside an external transaction; one might want
	// to do that to force p.avoidLeasedDescriptors to be set below.
	if asOf.BoundedStaleness {
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"cannot use a bounded staleness query in a transaction",
		)
	}
	if readTs := ex.state.getReadTimestamp(); asOf.Timestamp != readTs {
		err = pgerror.Newf(pgcode.Syntax,
			"inconsistent AS OF SYSTEM TIME timestamp; expected: %s, got: %s", readTs, asOf.Timestamp)
		err = errors.WithHint(err, "try SET TRANSACTION AS OF SYSTEM TIME")
		return err
	}
	p.extendedEvalCtx.AsOfSystemTime = asOf
	return nil
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
		ex.state.mu.txn = kv.NewTxnWithSteppingEnabled(ctx, ex.transitionCtx.db,
			ex.transitionCtx.nodeIDOrZero, ex.QualityOfService())
		if err := ex.state.mu.txn.SetUserPriority(userPriority); err != nil {
			return err
		}
	}
	return err
}

// commitSQLTransaction executes a commit after the execution of a
// stmt, which can be any statement when executing a statement with an
// implicit transaction, or a COMMIT statement when using an explicit
// transaction. commitFn is passed as a separate function, so that we avoid
// executing transactional logic when handling COMMIT in the CommitWait state.
func (ex *connExecutor) commitSQLTransaction(
	ctx context.Context, ast tree.Statement, commitFn func(ctx context.Context) error,
) (fsm.Event, fsm.EventPayload) {
	ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionStartTransactionCommit, timeutil.Now())
	if err := commitFn(ctx); err != nil {
		return ex.makeErrEvent(err, ast)
	}
	ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionEndTransactionCommit, timeutil.Now())
	if err := ex.reportSessionDataChanges(func() error {
		ex.sessionDataStack.PopAll()
		return nil
	}); err != nil {
		return ex.makeErrEvent(err, ast)
	}
	return eventTxnFinishCommitted{}, nil
}

// reportSessionDataChanges reports ParamStatusUpdate changes and re-calls
// and relevant session data callbacks after the given fn has been executed.
func (ex *connExecutor) reportSessionDataChanges(fn func() error) error {
	before := ex.sessionDataStack.Top()
	if err := fn(); err != nil {
		return err
	}
	after := ex.sessionDataStack.Top()
	if ex.dataMutatorIterator.paramStatusUpdater != nil {
		for _, param := range bufferableParamStatusUpdates {
			_, v, err := getSessionVar(param.lowerName, false /* missingOk */)
			if err != nil {
				return err
			}
			if v.Equal == nil {
				return errors.AssertionFailedf("Equal for %s must be set", param.name)
			}
			if v.GetFromSessionData == nil {
				return errors.AssertionFailedf("GetFromSessionData for %s must be set", param.name)
			}
			if !v.Equal(before, after) {
				ex.dataMutatorIterator.paramStatusUpdater.BufferParamStatusUpdate(
					param.name,
					v.GetFromSessionData(after),
				)
			}
		}
	}
	if before.DefaultIntSize != after.DefaultIntSize && ex.dataMutatorIterator.onDefaultIntSizeChange != nil {
		ex.dataMutatorIterator.onDefaultIntSizeChange(after.DefaultIntSize)
	}
	if before.ApplicationName != after.ApplicationName && ex.dataMutatorIterator.onApplicationNameChange != nil {
		ex.dataMutatorIterator.onApplicationNameChange(after.ApplicationName)
	}
	return nil
}

func (ex *connExecutor) commitSQLTransactionInternal(ctx context.Context) error {
	ctx, sp := tracing.EnsureChildSpan(ctx, ex.server.cfg.AmbientCtx.Tracer, "commit sql txn")
	defer sp.Finish()

	if err := ex.createJobs(ctx); err != nil {
		return err
	}

	if ex.extraTxnState.schemaChangerState.mode != sessiondatapb.UseNewSchemaChangerOff {
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

// createJobs creates jobs for the records cached in schemaChangeJobRecords
// during this transaction.
func (ex *connExecutor) createJobs(ctx context.Context) error {
	if len(ex.extraTxnState.schemaChangeJobRecords) == 0 {
		return nil
	}
	var records []*jobs.Record
	for _, record := range ex.extraTxnState.schemaChangeJobRecords {
		records = append(records, record)
	}
	jobIDs, err := ex.server.cfg.JobRegistry.CreateJobsWithTxn(ctx, ex.planner.extendedEvalCtx.Txn, records)
	if err != nil {
		return err
	}
	ex.planner.extendedEvalCtx.Jobs.add(jobIDs...)
	return nil
}

// rollbackSQLTransaction executes a ROLLBACK statement: the KV transaction is
// rolled-back and an event is produced.
func (ex *connExecutor) rollbackSQLTransaction(
	ctx context.Context, stmt tree.Statement,
) (fsm.Event, fsm.EventPayload) {
	if err := ex.state.mu.txn.Rollback(ctx); err != nil {
		log.Warningf(ctx, "txn rollback failed: %s", err)
	}
	if err := ex.reportSessionDataChanges(func() error {
		ex.sessionDataStack.PopAll()
		return nil
	}); err != nil {
		return ex.makeErrEvent(err, stmt)
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
	if planner.curPlan.avoidBuffering || ex.sessionData().AvoidBuffering {
		res.DisableBuffering()
	}

	defer func() {
		planner.maybeLogStatement(
			ctx,
			ex.executorType,
			int(atomic.LoadInt32(ex.extraTxnState.atomicAutoRetryCounter)),
			ex.extraTxnState.txnCounter,
			res.RowsAffected(),
			res.Err(),
			ex.statsCollector.PhaseTimes().GetSessionPhaseTime(sessionphase.SessionQueryReceived),
			&ex.extraTxnState.hasAdminRoleCache,
			ex.server.TelemetryLoggingMetrics,
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
		ctx, planner, planner.execCfg.NodeID, ex.sessionData().DistSQLMode, planner.curPlan.main,
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

	ex.sessionTracing.TraceRetryInformation(ctx, int(atomic.LoadInt32(ex.extraTxnState.atomicAutoRetryCounter)), ex.extraTxnState.autoRetryReason)
	if ex.server.cfg.TestingKnobs.OnTxnRetry != nil && ex.extraTxnState.autoRetryReason != nil {
		ex.server.cfg.TestingKnobs.OnTxnRetry(ex.extraTxnState.autoRetryReason, planner.EvalContext())
	}
	distribute := DistributionType(DistributionTypeNone)
	if distributePlan.WillDistribute() {
		distribute = DistributionTypeSystemTenantOnly
	}
	ex.sessionTracing.TraceExecStart(ctx, "distributed")
	stats, err := ex.execWithDistSQLEngine(
		ctx, planner, stmt.AST.StatementReturnType(), res, distribute, progAtomic,
	)
	if res.Err() == nil {
		// numTxnRetryErrors is the number of times an error will be injected if
		// the transaction is retried using SAVEPOINTs.
		const numTxnRetryErrors = 3
		if ex.sessionData().InjectRetryErrorsEnabled && stmt.AST.StatementTag() != "SET" {
			if planner.Txn().Epoch() < ex.state.lastEpoch+numTxnRetryErrors {
				retryErr := planner.Txn().GenerateForcedRetryableError(
					ctx, "injected by `inject_retry_errors_enabled` session variable")
				res.SetError(retryErr)
			} else {
				ex.state.lastEpoch = planner.Txn().Epoch()
			}
		}
	}
	ex.sessionTracing.TraceExecEnd(ctx, res.Err(), res.RowsAffected())
	ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.PlannerEndExecStmt, timeutil.Now())

	ex.extraTxnState.rowsRead += stats.rowsRead
	ex.extraTxnState.bytesRead += stats.bytesRead
	ex.extraTxnState.rowsWritten += stats.rowsWritten

	// Record the statement summary. This also closes the plan if the
	// plan has not been closed earlier.
	ex.recordStatementSummary(
		ctx, planner,
		int(atomic.LoadInt32(ex.extraTxnState.atomicAutoRetryCounter)), res.RowsAffected(), res.Err(), stats,
	)
	if ex.server.cfg.TestingKnobs.AfterExecute != nil {
		ex.server.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), res.Err())
	}

	if limitsErr := ex.handleTxnRowsWrittenReadLimits(ctx); limitsErr != nil && res.Err() == nil {
		res.SetError(limitsErr)
	}

	return err
}

type txnRowsWrittenLimitErr struct {
	eventpb.CommonTxnRowsLimitDetails
}

var _ error = &txnRowsWrittenLimitErr{}
var _ fmt.Formatter = &txnRowsWrittenLimitErr{}
var _ errors.SafeFormatter = &txnRowsWrittenLimitErr{}

// Error is part of the error interface, which txnRowsWrittenLimitErr
// implements.
func (e *txnRowsWrittenLimitErr) Error() string {
	return e.CommonTxnRowsLimitDetails.Error("written")
}

// Format is part of the fmt.Formatter interface, which txnRowsWrittenLimitErr
// implements.
func (e *txnRowsWrittenLimitErr) Format(s fmt.State, verb rune) {
	errors.FormatError(e, s, verb)
}

// SafeFormatError is part of the errors.SafeFormatter interface, which
// txnRowsWrittenLimitErr implements.
func (e *txnRowsWrittenLimitErr) SafeFormatError(p errors.Printer) (next error) {
	return e.CommonTxnRowsLimitDetails.SafeFormatError(p, "written")
}

type txnRowsReadLimitErr struct {
	eventpb.CommonTxnRowsLimitDetails
}

var _ error = &txnRowsReadLimitErr{}
var _ fmt.Formatter = &txnRowsReadLimitErr{}
var _ errors.SafeFormatter = &txnRowsReadLimitErr{}

// Error is part of the error interface, which txnRowsReadLimitErr implements.
func (e *txnRowsReadLimitErr) Error() string {
	return e.CommonTxnRowsLimitDetails.Error("read")
}

// Format is part of the fmt.Formatter interface, which txnRowsReadLimitErr
// implements.
func (e *txnRowsReadLimitErr) Format(s fmt.State, verb rune) {
	errors.FormatError(e, s, verb)
}

// SafeFormatError is part of the errors.SafeFormatter interface, which
// txnRowsReadLimitErr implements.
func (e *txnRowsReadLimitErr) SafeFormatError(p errors.Printer) (next error) {
	return e.CommonTxnRowsLimitDetails.SafeFormatError(p, "read")
}

// handleTxnRowsGuardrails handles either "written" or "read" rows guardrails.
func (ex *connExecutor) handleTxnRowsGuardrails(
	ctx context.Context,
	numRows, logLimit, errLimit int64,
	alreadyLogged *bool,
	isRead bool,
	logCounter, errCounter *metric.Counter,
) error {
	var err error
	shouldLog := logLimit != 0 && numRows > logLimit
	shouldErr := errLimit != 0 && numRows > errLimit
	if !shouldLog && !shouldErr {
		return nil
	}
	commonTxnRowsLimitDetails := eventpb.CommonTxnRowsLimitDetails{
		TxnID:     ex.state.mu.txn.ID().String(),
		SessionID: ex.sessionID.String(),
		NumRows:   numRows,
	}
	if shouldErr && ex.executorType == executorTypeInternal {
		// Internal work should never err and always log if violating either
		// limit.
		shouldLog = true
		shouldErr = false
	}
	if *alreadyLogged {
		// We have already logged this kind of event about this transaction.
		shouldLog = false
	} else {
		*alreadyLogged = shouldLog
	}
	if shouldLog {
		commonSQLEventDetails := ex.planner.getCommonSQLEventDetails(defaultRedactionOptions)
		var event eventpb.EventPayload
		if ex.executorType == executorTypeInternal {
			if isRead {
				event = &eventpb.TxnRowsReadLimitInternal{
					CommonSQLEventDetails:     commonSQLEventDetails,
					CommonTxnRowsLimitDetails: commonTxnRowsLimitDetails,
				}
			} else {
				event = &eventpb.TxnRowsWrittenLimitInternal{
					CommonSQLEventDetails:     commonSQLEventDetails,
					CommonTxnRowsLimitDetails: commonTxnRowsLimitDetails,
				}
			}
		} else {
			if isRead {
				event = &eventpb.TxnRowsReadLimit{
					CommonSQLEventDetails:     commonSQLEventDetails,
					CommonTxnRowsLimitDetails: commonTxnRowsLimitDetails,
				}
			} else {
				event = &eventpb.TxnRowsWrittenLimit{
					CommonSQLEventDetails:     commonSQLEventDetails,
					CommonTxnRowsLimitDetails: commonTxnRowsLimitDetails,
				}
			}
			log.StructuredEvent(ctx, event)
			logCounter.Inc(1)
		}
	}
	if shouldErr {
		if isRead {
			err = &txnRowsReadLimitErr{CommonTxnRowsLimitDetails: commonTxnRowsLimitDetails}
		} else {
			err = &txnRowsWrittenLimitErr{CommonTxnRowsLimitDetails: commonTxnRowsLimitDetails}
		}
		err = pgerror.WithCandidateCode(err, pgcode.ProgramLimitExceeded)
		errCounter.Inc(1)
	}
	return err
}

// handleTxnRowsWrittenReadLimits checks whether the current transaction has
// reached the limits on the number of rows written/read and logs the
// corresponding event or returns an error. It should be called after executing
// a single statement.
func (ex *connExecutor) handleTxnRowsWrittenReadLimits(ctx context.Context) error {
	// Note that in many cases, the internal executor doesn't have the
	// sessionData properly set (i.e. the default values are used), so we'll
	// never log anything then. This seems acceptable since the focus of these
	// guardrails is on the externally initiated queries.
	sd := ex.sessionData()
	writtenErr := ex.handleTxnRowsGuardrails(
		ctx,
		ex.extraTxnState.rowsWritten,
		sd.TxnRowsWrittenLog,
		sd.TxnRowsWrittenErr,
		&ex.extraTxnState.rowsWrittenLogged,
		false, /* isRead */
		ex.metrics.GuardrailMetrics.TxnRowsWrittenLogCount,
		ex.metrics.GuardrailMetrics.TxnRowsWrittenErrCount,
	)
	readErr := ex.handleTxnRowsGuardrails(
		ctx,
		ex.extraTxnState.rowsRead,
		sd.TxnRowsReadLog,
		sd.TxnRowsReadErr,
		&ex.extraTxnState.rowsReadLogged,
		true, /* isRead */
		ex.metrics.GuardrailMetrics.TxnRowsReadLogCount,
		ex.metrics.GuardrailMetrics.TxnRowsReadErrCount,
	)
	return errors.CombineErrors(writtenErr, readErr)
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
		if ex.executorType == executorTypeExec && planner.EvalContext().SessionData().DisallowFullTableScans {
			hasLargeScan := flags.IsSet(planFlagContainsLargeFullIndexScan) || flags.IsSet(planFlagContainsLargeFullTableScan)
			if hasLargeScan {
				// We don't execute the statement if:
				// - plan contains a full table or full index scan.
				// - the session setting disallows full table/index scans.
				// - the scan is considered large.
				// - the query is not an internal query.
				ex.metrics.EngineMetrics.FullTableOrIndexScanRejectedCount.Inc(1)
				return errors.WithHint(
					pgerror.Newf(pgcode.TooManyRows,
						"query `%s` contains a full table/index scan which is explicitly disallowed",
						planner.stmt.SQL),
					"try overriding the `disallow_full_table_scans` or increasing the `large_full_scan_rows` cluster/session settings",
				)
			}
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
	// rowsWritten is the number of rows written.
	rowsWritten int64
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
	distribute DistributionType,
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
	planCtx := ex.server.cfg.DistSQLPlanner.NewPlanningCtx(ctx, evalCtx, planner,
		planner.txn, distribute)
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
			return *recv.stats, recv.commErr
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
		return *recv.stats, recv.commErr
	}

	ex.server.cfg.DistSQLPlanner.PlanAndRunCascadesAndChecks(
		ctx, planner, evalCtxFactory, &planner.curPlan.planComponents, recv,
	)

	return *recv.stats, recv.commErr
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
	asOfClause := ex.asOfClauseWithSessionDefault(modes.AsOf)
	if asOfClause.Expr == nil {
		rwMode = ex.readWriteModeWithSessionDefault(modes.ReadWriteMode)
		return rwMode, now, nil, nil
	}
	ex.statsCollector.Reset(ex.applicationStats, ex.phaseTimes)
	p := &ex.planner

	ex.resetPlanner(ctx, p, nil, now)
	asOf, err := p.EvalAsOfTimestamp(ctx, asOfClause)
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
	return tree.ReadOnly, asOf.Timestamp.GoTime(), &asOf.Timestamp, nil
}

var eventStartImplicitTxn fsm.Event = eventTxnStart{ImplicitTxn: fsm.True}
var eventStartExplicitTxn fsm.Event = eventTxnStart{ImplicitTxn: fsm.False}

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
		ex.sessionDataStack.PushTopClone()
		return eventStartExplicitTxn,
			makeEventTxnStartPayload(
				ex.txnPriorityWithSessionDefault(s.Modes.UserPriority),
				mode,
				sqlTs,
				historicalTs,
				ex.transitionCtx,
				ex.QualityOfService())
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
		return eventStartImplicitTxn,
			makeEventTxnStartPayload(
				ex.txnPriorityWithSessionDefault(tree.UnspecifiedUserPriority),
				mode,
				sqlTs,
				historicalTs,
				ex.transitionCtx,
				ex.QualityOfService())
	}
}

// beginImplicitTxn starts an implicit transaction. The fsm.Event that is
// returned does not cause the state machine to advance, so the same command
// will be executed again, but with an implicit transaction.
// Implicit transactions are created with the session's default
// historical timestamp even though the statement itself might contain
// an AOST clause. In these cases the clause is evaluated and applied
// when the command is executed again.
func (ex *connExecutor) beginImplicitTxn(
	ctx context.Context, ast tree.Statement,
) (fsm.Event, fsm.EventPayload) {
	// NB: Implicit transactions are created with the session's default
	// historical timestamp even though the statement itself might contain
	// an AOST clause. In these cases the clause is evaluated and applied
	// when the command is evaluated again.
	noBeginStmt := (*tree.BeginTransaction)(nil)
	mode, sqlTs, historicalTs, err := ex.beginTransactionTimestampsAndReadMode(ctx, noBeginStmt)
	if err != nil {
		return ex.makeErrEvent(err, ast)
	}
	return eventStartImplicitTxn,
		makeEventTxnStartPayload(
			ex.txnPriorityWithSessionDefault(tree.UnspecifiedUserPriority),
			mode,
			sqlTs,
			historicalTs,
			ex.transitionCtx,
			ex.QualityOfService(),
		)
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
		return ex.rollbackSQLTransaction(ctx, s)

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
	ctx context.Context, ast tree.Statement, res RestrictedCommandResult,
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
		return ex.commitSQLTransaction(
			ctx,
			ast,
			func(ctx context.Context) error {
				// COMMIT while in the CommitWait state is a no-op.
				return nil
			},
		)
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
		return ex.runShowLastQueryStatistics(ctx, res, sqlStmt)
	case *tree.ShowTransferState:
		return ex.runShowTransferState(ctx, res, sqlStmt)
	case *tree.ShowCompletions:
		return ex.runShowCompletions(ctx, sqlStmt, res)
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

// sessionStateBase64 returns the serialized session state in a base64 form.
// See runShowTransferState for more information.
//
// Note: Do not use a planner here because this is used in the context of an
// observer statement.
func (ex *connExecutor) sessionStateBase64() (tree.Datum, error) {
	// Observer statements do not use implicit transactions at all, so
	// we look at CurState() directly.
	_, isNoTxn := ex.machine.CurState().(stateNoTxn)
	state, err := serializeSessionState(
		!isNoTxn, ex.extraTxnState.prepStmtsNamespace, ex.sessionData(),
		ex.server.cfg,
	)
	if err != nil {
		return nil, err
	}
	return tree.NewDString(base64.StdEncoding.EncodeToString([]byte(*state))), nil
}

// sessionRevivalTokenBase64 creates a session revival token and returns it in
// a base64 form. See runShowTransferState for more information.
//
// Note: Do not use a planner here because this is used in the context of an
// observer statement.
func (ex *connExecutor) sessionRevivalTokenBase64() (tree.Datum, error) {
	cm, err := ex.server.cfg.RPCContext.SecurityContext.GetCertificateManager()
	if err != nil {
		return nil, err
	}
	token, err := createSessionRevivalToken(
		AllowSessionRevival.Get(&ex.server.cfg.Settings.SV) && !ex.server.cfg.Codec.ForSystemTenant(),
		ex.sessionData(),
		cm,
	)
	if err != nil {
		return nil, err
	}
	return tree.NewDString(base64.StdEncoding.EncodeToString([]byte(*token))), nil
}

// runShowTransferState executes a SHOW TRANSFER STATE statement.
//
// If an error is returned, the connection needs to stop processing queries.
func (ex *connExecutor) runShowTransferState(
	ctx context.Context, res RestrictedCommandResult, stmt *tree.ShowTransferState,
) error {
	// The transfer_key column must always be the last.
	colNames := []string{
		"error", "session_state_base64", "session_revival_token_base64",
	}
	if stmt.TransferKey != nil {
		colNames = append(colNames, "transfer_key")
	}
	cols := make(colinfo.ResultColumns, len(colNames))
	for i := 0; i < len(colNames); i++ {
		cols[i] = colinfo.ResultColumn{Name: colNames[i], Typ: types.String}
	}
	res.SetColumns(ctx, cols)

	var sessionState, sessionRevivalToken tree.Datum
	var row tree.Datums
	err := func() error {
		// NOTE: These functions are executed in the context of an observer
		// statement, and observer statements do not get planned, so a planner
		// should not be used.
		var err error
		if sessionState, err = ex.sessionStateBase64(); err != nil {
			return err
		}
		if sessionRevivalToken, err = ex.sessionRevivalTokenBase64(); err != nil {
			return err
		}
		return nil
	}()
	if err != nil {
		// When an error occurs, only show the error column (plus transfer_key
		// column if it exists), and NULL for everything else.
		row = []tree.Datum{tree.NewDString(err.Error()), tree.DNull, tree.DNull}
	} else {
		row = []tree.Datum{tree.DNull, sessionState, sessionRevivalToken}
	}
	if stmt.TransferKey != nil {
		row = append(row, tree.NewDString(stmt.TransferKey.RawString()))
	}
	return res.AddRow(ctx, row)
}

// runShowCompletions executes a SHOW COMPLETIONS statement.
func (ex *connExecutor) runShowCompletions(
	ctx context.Context, n *tree.ShowCompletions, res RestrictedCommandResult,
) error {
	res.SetColumns(ctx, colinfo.ResultColumns{{Name: "COMPLETIONS", Typ: types.String}})
	offsetVal, ok := n.Offset.AsConstantInt()
	if !ok {
		return errors.Newf("invalid offset %v", n.Offset)
	}
	offset, err := strconv.Atoi(offsetVal.String())
	if err != nil {
		return err
	}
	completions, err := delegate.RunShowCompletions(n.Statement.RawString(), offset)
	if err != nil {
		return err
	}

	for _, completion := range completions {
		err = res.AddRow(ctx, tree.Datums{tree.NewDString(completion)})
		if err != nil {
			return err
		}
	}
	return nil
}

// showQueryStatsFns maps column names as requested by the SQL clients
// to timing retrieval functions from the execution phase times.
var showQueryStatsFns = map[tree.Name]func(*sessionphase.Times) time.Duration{
	"parse_latency": func(phaseTimes *sessionphase.Times) time.Duration { return phaseTimes.GetParsingLatency() },
	"plan_latency":  func(phaseTimes *sessionphase.Times) time.Duration { return phaseTimes.GetPlanningLatency() },
	"exec_latency":  func(phaseTimes *sessionphase.Times) time.Duration { return phaseTimes.GetRunLatency() },
	// Since the last query has already finished, it is safe to retrieve its
	// total service latency.
	"service_latency":          func(phaseTimes *sessionphase.Times) time.Duration { return phaseTimes.GetServiceLatencyTotal() },
	"post_commit_jobs_latency": func(phaseTimes *sessionphase.Times) time.Duration { return phaseTimes.GetPostCommitJobsLatency() },
}

func (ex *connExecutor) runShowLastQueryStatistics(
	ctx context.Context, res RestrictedCommandResult, stmt *tree.ShowLastQueryStatistics,
) error {
	// Which columns were selected?
	resColumns := make(colinfo.ResultColumns, len(stmt.Columns))
	for i, n := range stmt.Columns {
		resColumns[i] = colinfo.ResultColumn{Name: string(n), Typ: types.String}
	}
	res.SetColumns(ctx, resColumns)

	phaseTimes := ex.statsCollector.PreviousPhaseTimes()

	// Now convert the durations to the string representation of intervals.
	// We do the conversion server-side using a fixed format, so that
	// the CLI code does not get different results depending on the
	// IntervalStyle session parameter. (See issue #67618.)
	// We also choose to convert to interval, and then convert to string
	// so that pre-v21.2 CLI can still parse the value as an interval
	// (INTERVAL was the result type in previous versions).
	// Using a simpler type (e.g. microseconds as an INT) would be simpler
	// but would be incompatible with previous version clients.
	strs := make(tree.Datums, len(stmt.Columns))
	var buf bytes.Buffer
	for i, cname := range stmt.Columns {
		fn := showQueryStatsFns[cname]
		if fn == nil {
			// If no function was defined with this column name, this means
			// that a client from the future is requesting a column that
			// does not exist yet in this version. Inform the client that
			// the column is not supported via a NULL value.
			strs[i] = tree.DNull
		} else {
			d := fn(phaseTimes)
			ival := tree.NewDInterval(duration.FromFloat64(d.Seconds()), types.DefaultIntervalTypeMetadata)
			buf.Reset()
			ival.Duration.FormatWithStyle(&buf, duration.IntervalStyle_POSTGRES)
			strs[i] = tree.NewDString(buf.String())
		}
	}

	return res.AddRow(ctx, strs)
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
func (ex *connExecutor) addActiveQuery(
	ast tree.Statement, rawStmt string, queryID ClusterWideID, cancelFun context.CancelFunc,
) {
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
}

func (ex *connExecutor) removeActiveQuery(queryID ClusterWideID, ast tree.Statement) {
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

	// Attempt to refresh the deadline before the autocommit.
	err := ex.extraTxnState.descCollection.MaybeUpdateDeadline(ctx, ex.state.mu.txn)
	if err != nil {
		return ex.makeErrEvent(err, stmt)
	}
	ev, payload := ex.commitSQLTransaction(ctx, stmt, ex.commitSQLTransactionInternal)
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

func (ex *connExecutor) onTxnFinish(ctx context.Context, ev txnEvent) {
	if ex.extraTxnState.shouldExecuteOnTxnFinish {
		ex.extraTxnState.shouldExecuteOnTxnFinish = false
		txnStart := ex.extraTxnState.txnFinishClosure.txnStartTime
		implicit := ex.extraTxnState.txnFinishClosure.implicit
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionEndExecTransaction, timeutil.Now())
		transactionFingerprintID :=
			roachpb.TransactionFingerprintID(ex.extraTxnState.transactionStatementsHash.Sum())
		if !implicit {
			ex.statsCollector.EndExplicitTransaction(
				ctx,
				transactionFingerprintID,
			)
		}
		if ex.server.cfg.TestingKnobs.BeforeTxnStatsRecorded != nil {
			ex.server.cfg.TestingKnobs.BeforeTxnStatsRecorded(
				ex.sessionData(),
				ev.txnID,
				transactionFingerprintID,
			)
		}
		err := ex.recordTransactionFinish(ctx, transactionFingerprintID, ev, implicit, txnStart)
		if err != nil {
			if log.V(1) {
				log.Warningf(ctx, "failed to record transaction stats: %s", err)
			}
			ex.server.ServerMetrics.StatsMetrics.DiscardedStatsCount.Inc(1)
		}
	}
}

func (ex *connExecutor) onTxnRestart(ctx context.Context) {
	if ex.extraTxnState.shouldExecuteOnTxnRestart {
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionMostRecentStartExecTransaction, timeutil.Now())
		ex.extraTxnState.transactionStatementFingerprintIDs = nil
		ex.extraTxnState.transactionStatementsHash = util.MakeFNV64()
		ex.extraTxnState.numRows = 0
		// accumulatedStats are cleared, but shouldCollectTxnExecutionStats is
		// unchanged.
		ex.extraTxnState.accumulatedStats = execstats.QueryLevelStats{}
		ex.extraTxnState.rowsRead = 0
		ex.extraTxnState.bytesRead = 0
		ex.extraTxnState.rowsWritten = 0

		if ex.server.cfg.TestingKnobs.BeforeRestart != nil {
			ex.server.cfg.TestingKnobs.BeforeRestart(ctx, ex.extraTxnState.autoRetryReason)
		}
	}
}

// recordTransactionStart records the start of the transaction.
func (ex *connExecutor) recordTransactionStart(txnID uuid.UUID) {
	// Transaction fingerprint ID will be available once transaction finishes
	// execution.
	ex.txnIDCacheWriter.Record(contentionpb.ResolvedTxnID{
		TxnID:            txnID,
		TxnFingerprintID: roachpb.InvalidTransactionFingerprintID,
	})

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
	ex.extraTxnState.rowsWritten = 0
	ex.extraTxnState.rowsWrittenLogged = false
	ex.extraTxnState.rowsReadLogged = false
	if txnExecStatsSampleRate := collectTxnStatsSampleRate.Get(&ex.server.GetExecutorConfig().Settings.SV); txnExecStatsSampleRate > 0 {
		ex.extraTxnState.shouldCollectTxnExecutionStats = txnExecStatsSampleRate > ex.rng.Float64()
	}

	if ex.executorType != executorTypeInternal {
		ex.metrics.EngineMetrics.SQLTxnsOpen.Inc(1)
	}

	ex.extraTxnState.shouldExecuteOnTxnFinish = true
	ex.extraTxnState.txnFinishClosure.txnStartTime = txnStart
	ex.extraTxnState.txnFinishClosure.implicit = implicit
	ex.extraTxnState.shouldExecuteOnTxnRestart = true

	if !implicit {
		ex.statsCollector.StartExplicitTransaction()
	}
}

func (ex *connExecutor) recordTransactionFinish(
	ctx context.Context,
	transactionFingerprintID roachpb.TransactionFingerprintID,
	ev txnEvent,
	implicit bool,
	txnStart time.Time,
) error {
	recordingStart := timeutil.Now()
	defer func() {
		recordingOverhead := timeutil.Since(recordingStart)
		ex.server.
			ServerMetrics.
			StatsMetrics.
			SQLTxnStatsCollectionOverhead.RecordValue(recordingOverhead.Nanoseconds())
	}()

	txnEnd := timeutil.Now()
	txnTime := txnEnd.Sub(txnStart)
	if ex.executorType != executorTypeInternal {
		ex.metrics.EngineMetrics.SQLTxnsOpen.Dec(1)
	}
	ex.metrics.EngineMetrics.SQLTxnLatency.RecordValue(txnTime.Nanoseconds())

	ex.txnIDCacheWriter.Record(contentionpb.ResolvedTxnID{
		TxnID:            ev.txnID,
		TxnFingerprintID: transactionFingerprintID,
	})

	if len(ex.extraTxnState.transactionStatementFingerprintIDs) == 0 {
		// If the slice of transaction statement fingerprint IDs is empty, this
		// means there is no statements that's being executed within this
		// transaction. Hence, recording stats for this transaction is not
		// meaningful.
		return nil
	}

	txnServiceLat := ex.phaseTimes.GetTransactionServiceLatency()
	txnRetryLat := ex.phaseTimes.GetTransactionRetryLatency()
	commitLat := ex.phaseTimes.GetCommitLatency()

	recordedTxnStats := sqlstats.RecordedTxnStats{
		TransactionTimeSec:      txnTime.Seconds(),
		Committed:               ev.eventType == txnCommit,
		ImplicitTxn:             implicit,
		RetryCount:              int64(atomic.LoadInt32(ex.extraTxnState.atomicAutoRetryCounter)),
		StatementFingerprintIDs: ex.extraTxnState.transactionStatementFingerprintIDs,
		ServiceLatency:          txnServiceLat,
		RetryLatency:            txnRetryLat,
		CommitLatency:           commitLat,
		RowsAffected:            ex.extraTxnState.numRows,
		CollectedExecStats:      ex.extraTxnState.shouldCollectTxnExecutionStats,
		ExecStats:               ex.extraTxnState.accumulatedStats,
		RowsRead:                ex.extraTxnState.rowsRead,
		RowsWritten:             ex.extraTxnState.rowsWritten,
		BytesRead:               ex.extraTxnState.bytesRead,
	}

	return ex.statsCollector.RecordTransaction(
		ctx,
		transactionFingerprintID,
		recordedTxnStats,
	)
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
