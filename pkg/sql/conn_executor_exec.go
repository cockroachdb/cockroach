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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/multitenant/multitenantcpu"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/contention"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/regions"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/asof"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/ctxlog"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/lib/pq/oid"
	"go.opentelemetry.io/otel/attribute"
)

// numTxnRetryErrors is the number of times an error will be injected if
// the transaction is retried using SAVEPOINTs.
const numTxnRetryErrors = 3

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
//
//	then the statement cannot have any placeholder.
func (ex *connExecutor) execStmt(
	ctx context.Context,
	parserStmt statements.Statement[tree.Statement],
	portal *PreparedPortal,
	pinfo *tree.PlaceholderInfo,
	res RestrictedCommandResult,
	canAutoCommit bool,
) (fsm.Event, fsm.EventPayload, error) {
	ast := parserStmt.AST
	if log.V(2) || logStatementsExecuteEnabled.Get(&ex.server.cfg.Settings.SV) ||
		log.HasSpan(ctx) {
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
		ev, payload = ex.execStmtInNoTxnState(ctx, parserStmt, res)

	case stateOpen:
		var preparedStmt *PreparedStatement
		if portal != nil {
			preparedStmt = portal.Stmt
		}
		err = ex.execWithProfiling(ctx, ast, preparedStmt, func(ctx context.Context) error {
			ev, payload, err = ex.execStmtInOpenState(ctx, parserStmt, portal, pinfo, res, canAutoCommit)
			return err
		})
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
			ex.CancelSession,
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
					ex.CancelSession,
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
) (ev fsm.Event, payload fsm.EventPayload, retErr error) {
	defer func() {
		if portal.isPausable() {
			if !portal.pauseInfo.exhaustPortal.cleanup.isComplete {
				portal.pauseInfo.exhaustPortal.cleanup.appendFunc(namedFunc{fName: "exhaust portal", f: func() {
					ex.exhaustPortal(portalName)
				}})
				portal.pauseInfo.exhaustPortal.cleanup.isComplete = true
			}
			// If we encountered an error when executing a pausable portal, clean up
			// the retained resources.
			if retErr != nil {
				portal.pauseInfo.cleanupAll()
			}
		}
	}()

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
		ev, payload, retErr = ex.execStmt(ctx, portal.Stmt.Statement, &portal, pinfo, stmtRes, canAutoCommit)
		// For a non-pausable portal, it is considered exhausted regardless of the
		// fact whether an error occurred or not - if it did, we still don't want
		// to re-execute the portal from scratch.
		// The current statement may have just closed and deleted the portal,
		// so only exhaust it if it still exists.
		if _, ok := ex.extraTxnState.prepStmtsNamespace.portals[portalName]; ok && !portal.isPausable() {
			defer ex.exhaustPortal(portalName)
		}
		return ev, payload, retErr

	default:
		return ex.execStmt(ctx, portal.Stmt.Statement, &portal, pinfo, stmtRes, canAutoCommit)
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
	parserStmt statements.Statement[tree.Statement],
	portal *PreparedPortal,
	pinfo *tree.PlaceholderInfo,
	res RestrictedCommandResult,
	canAutoCommit bool,
) (retEv fsm.Event, retPayload fsm.EventPayload, retErr error) {
	// We need this to be function rather than a static bool, because a portal's
	// "pausability" can be revoked in `dispatchToExecutionEngine()` if the
	// underlying statement contains sub/post queries. Thus, we should evaluate
	// whether a portal is pausable when executing the cleanup step.
	isPausablePortal := func() bool { return portal != nil && portal.isPausable() }
	// updateRetErrAndPayload ensures that the latest event payload and error is
	// always recorded by portal.pauseInfo.
	// TODO(janexing): add test for this.
	updateRetErrAndPayload := func(err error, payload fsm.EventPayload) {
		retPayload = payload
		retErr = err
		if isPausablePortal() {
			portal.pauseInfo.execStmtInOpenState.retPayload = payload
			portal.pauseInfo.execStmtInOpenState.retErr = err
		}
	}
	// For pausable portals, we delay the clean-up until closing the portal by
	// adding the function to the execStmtInOpenStateCleanup.
	// Otherwise, perform the clean-up step within every execution.
	processCleanupFunc := func(fName string, f func()) {
		if !isPausablePortal() {
			f()
		} else if !portal.pauseInfo.execStmtInOpenState.cleanup.isComplete {
			portal.pauseInfo.execStmtInOpenState.cleanup.appendFunc(namedFunc{
				fName: fName,
				f: func() {
					f()
					// Some cleanup steps modify the retErr and retPayload. We need to
					// ensure that cleanup after them can see the update.
					updateRetErrAndPayload(retErr, retPayload)
				},
			})
		}
	}
	defer func() {
		// This is the first defer, so it will always be called after any cleanup
		// func being added to the stack from the defers below.
		if isPausablePortal() && !portal.pauseInfo.execStmtInOpenState.cleanup.isComplete {
			portal.pauseInfo.execStmtInOpenState.cleanup.isComplete = true
		}
		// If there's any error, do the cleanup right here.
		if (retErr != nil || payloadHasError(retPayload)) && isPausablePortal() {
			updateRetErrAndPayload(retErr, retPayload)
			portal.pauseInfo.resumableFlow.cleanup.run()
			portal.pauseInfo.dispatchToExecutionEngine.cleanup.run()
			portal.pauseInfo.execStmtInOpenState.cleanup.run()
		}
	}()

	// We need this part so that when we check if we need to increment the count
	// of executed stmt, we are checking the latest error and payload. Otherwise,
	// we would be checking the ones evaluated at the portal's first-time
	// execution.
	defer func() {
		if isPausablePortal() {
			updateRetErrAndPayload(retErr, retPayload)
		}
	}()

	ast := parserStmt.AST
	var sp *tracing.Span
	if !isPausablePortal() || !portal.pauseInfo.execStmtInOpenState.cleanup.isComplete {
		ctx, sp = tracing.EnsureChildSpan(ctx, ex.server.cfg.AmbientCtx.Tracer, "sql query")
		// TODO(andrei): Consider adding the placeholders as tags too.
		sp.SetTag("statement", attribute.StringValue(parserStmt.SQL))
		ctx = withStatement(ctx, ast)
		if isPausablePortal() {
			portal.pauseInfo.execStmtInOpenState.spCtx = ctx
		}
		defer func() {
			processCleanupFunc("cleanup span", sp.Finish)
		}()
	} else {
		ctx = portal.pauseInfo.execStmtInOpenState.spCtx
	}

	makeErrEvent := func(err error) (fsm.Event, fsm.EventPayload, error) {
		ev, payload := ex.makeErrEvent(err, ast)
		return ev, payload, nil
	}

	var stmt Statement
	var queryID clusterunique.ID

	if isPausablePortal() {
		if !portal.pauseInfo.isQueryIDSet() {
			portal.pauseInfo.execStmtInOpenState.queryID = ex.server.cfg.GenerateID()
		}
		queryID = portal.pauseInfo.execStmtInOpenState.queryID
	} else {
		queryID = ex.server.cfg.GenerateID()
	}

	// Update the deadline on the transaction based on the collections.
	err := ex.extraTxnState.descCollection.MaybeUpdateDeadline(ctx, ex.state.mu.txn)
	if err != nil {
		return makeErrEvent(err)
	}
	os := ex.machine.CurState().(stateOpen)

	isExtendedProtocol := portal != nil && portal.Stmt != nil
	stmtFingerprintFmtMask := tree.FmtHideConstants | tree.FmtFlags(queryFormattingForFingerprintsMask.Get(&ex.server.cfg.Settings.SV))

	if isExtendedProtocol {
		stmt = makeStatementFromPrepared(portal.Stmt, queryID)
	} else {
		stmt = makeStatement(parserStmt, queryID, stmtFingerprintFmtMask)
	}
	stmtFingerprint := stmt.StmtNoConstants

	var queryTimeoutTicker *time.Timer
	var txnTimeoutTicker *time.Timer
	queryTimedOut := false
	txnTimedOut := false
	// queryDoneAfterFunc and txnDoneAfterFunc will be allocated only when
	// queryTimeoutTicker or txnTimeoutTicker is non-nil.
	var queryDoneAfterFunc chan struct{}
	var txnDoneAfterFunc chan struct{}

	var cancelQuery context.CancelFunc
	addActiveQuery := func() {
		ctx, cancelQuery = ctxlog.WithCancel(ctx)
		ex.incrementStartedStmtCounter(ast)
		func(st *txnState) {
			st.mu.Lock()
			defer st.mu.Unlock()
			st.mu.stmtCount++
		}(&ex.state)
		ex.addActiveQuery(parserStmt, pinfo, queryID, cancelQuery)
	}

	// For pausable portal, the active query needs to be set up only when
	// the portal is executed for the first time.
	if !isPausablePortal() || !portal.pauseInfo.execStmtInOpenState.cleanup.isComplete {
		addActiveQuery()
		if isPausablePortal() {
			portal.pauseInfo.execStmtInOpenState.cancelQueryFunc = cancelQuery
			portal.pauseInfo.execStmtInOpenState.cancelQueryCtx = ctx
		}
		defer func() {
			processCleanupFunc(
				"increment executed stmt cnt",
				func() {
					// We need to check the latest errors rather than the ones evaluated
					// when this function is created.
					if isPausablePortal() {
						retErr = portal.pauseInfo.execStmtInOpenState.retErr
						retPayload = portal.pauseInfo.execStmtInOpenState.retPayload
					}
					if retErr == nil && !payloadHasError(retPayload) {
						ex.incrementExecutedStmtCounter(ast)
					}
				},
			)
		}()
	} else {
		ctx = portal.pauseInfo.execStmtInOpenState.cancelQueryCtx
		cancelQuery = portal.pauseInfo.execStmtInOpenState.cancelQueryFunc
	}

	// Make sure that we always unregister the query. It also deals with
	// overwriting res.Error to a more user-friendly message in case of query
	// cancellation.
	defer func(ctx context.Context, res RestrictedCommandResult) {
		if queryTimeoutTicker != nil {
			if !queryTimeoutTicker.Stop() {
				// Wait for the timer callback to complete to avoid a data race on
				// queryTimedOut.
				<-queryDoneAfterFunc
			}
		}
		if txnTimeoutTicker != nil {
			if !txnTimeoutTicker.Stop() {
				// Wait for the timer callback to complete to avoid a data race on
				// txnTimedOut.
				<-txnDoneAfterFunc
			}
		}

		processCleanupFunc("cancel query", func() {
			cancelQueryCtx := ctx
			if isPausablePortal() {
				cancelQueryCtx = portal.pauseInfo.execStmtInOpenState.cancelQueryCtx
			}
			resToPushErr := res
			// For pausable portals, we retain the query but update the result for
			// each execution. When the query context is cancelled and we're in the
			// middle of an portal execution, push the error to the current result.
			if isPausablePortal() {
				resToPushErr = portal.pauseInfo.curRes
			}
			// Detect context cancelation and overwrite whatever error might have been
			// set on the result before. The idea is that once the query's context is
			// canceled, all sorts of actors can detect the cancelation and set all
			// sorts of errors on the result. Rather than trying to impose discipline
			// in that jungle, we just overwrite them all here with an error that's
			// nicer to look at for the client.
			if resToPushErr != nil && cancelQueryCtx.Err() != nil && resToPushErr.ErrAllowReleased() != nil {
				// Even in the cases where the error is a retryable error, we want to
				// intercept the event and payload returned here to ensure that the query
				// is not retried.
				retEv = eventNonRetriableErr{
					IsCommit: fsm.FromBool(isCommit(ast)),
				}
				errToPush := cancelchecker.QueryCanceledError
				// For pausable portal, we can arrive here after encountering a timeout
				// error and then perform a query-cleanup step. In this case, we don't
				// want to override the original timeout error with the query-cancelled
				// error.
				if isPausablePortal() && (errors.Is(resToPushErr.Err(), sqlerrors.QueryTimeoutError) ||
					errors.Is(resToPushErr.Err(), sqlerrors.TxnTimeoutError)) {
					errToPush = resToPushErr.Err()
				}
				resToPushErr.SetError(errToPush)
				retPayload = eventNonRetriableErrPayload{err: errToPush}
			}
			ex.removeActiveQuery(queryID, ast)
			cancelQuery()
		})

		// Note ex.metrics is Server.Metrics for the connExecutor that serves the
		// client connection, and is Server.InternalMetrics for internal executors.
		ex.metrics.EngineMetrics.SQLActiveStatements.Dec(1)

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
		} else if txnTimedOut {
			retEv = eventNonRetriableErr{
				IsCommit: fsm.FromBool(isCommit(ast)),
			}
			res.SetError(sqlerrors.TxnTimeoutError)
			retPayload = eventNonRetriableErrPayload{err: sqlerrors.TxnTimeoutError}
		}
	}(ctx, res)

	// Special handling for SET TRANSACTION statements within a stored procedure
	// that uses COMMIT or ROLLBACK. This has to happen before the call to
	// resetPlanner to ensure that the settings are propagated correctly.
	if txnModes := ex.planner.storedProcTxnState.getTxnModes(); txnModes != nil {
		_, err = ex.planner.SetTransaction(ctx, &tree.SetTransaction{Modes: *txnModes})
		if err != nil {
			return makeErrEvent(err)
		}
	}

	// Note ex.metrics is Server.Metrics for the connExecutor that serves the
	// client connection, and is Server.InternalMetrics for internal executors.
	ex.metrics.EngineMetrics.SQLActiveStatements.Inc(1)

	// TODO(sql-sessions): persist the planner for a pausable portal, and reuse
	// it for each re-execution.
	// https://github.com/cockroachdb/cockroach/issues/99625
	p := &ex.planner
	stmtTS := ex.server.cfg.Clock.PhysicalTime()
	ex.statsCollector.Reset(ex.applicationStats, ex.phaseTimes)
	ex.resetPlanner(ctx, p, ex.state.mu.txn, stmtTS)
	p.sessionDataMutatorIterator.paramStatusUpdater = res
	p.noticeSender = res
	ih := &p.instrumentation

	if maxOpen := maxOpenTransactions.Get(&ex.server.cfg.Settings.SV); maxOpen > 0 && ex.executorType != executorTypeInternal {
		// NB: ex.metrics includes internal executor transactions when executorType
		// is executorTypeInternal, so that's why we exclude internal executors
		// in the conditional.
		if ex.metrics.EngineMetrics.SQLTxnsOpen.Value() > maxOpen {
			hasAdmin, err := ex.planner.HasAdminRole(ctx)
			if err != nil {
				return makeErrEvent(err)
			}
			if !hasAdmin {
				return makeErrEvent(errors.WithHintf(
					pgerror.Newf(
						pgcode.ConfigurationLimitExceeded,
						"cannot execute operation due to server.max_open_transactions_per_gateway cluster setting",
					),
					"the maximum number of open transactions is %d", maxOpen,
				))
			}
		}
	}

	// Special top-level handling for EXPLAIN ANALYZE.
	if e, ok := ast.(*tree.ExplainAnalyze); ok {
		switch e.Mode {
		case tree.ExplainDebug:
			telemetry.Inc(sqltelemetry.ExplainAnalyzeDebugUseCounter)
			flags := explain.MakeFlags(&e.ExplainOptions)
			flags.Verbose = true
			flags.ShowTypes = true
			if ex.server.cfg.TestingKnobs.DeterministicExplain {
				flags.Deflake = explain.DeflakeAll
			}
			ih.SetOutputMode(explainAnalyzeDebugOutput, flags)

		case tree.ExplainPlan:
			telemetry.Inc(sqltelemetry.ExplainAnalyzeUseCounter)
			flags := explain.MakeFlags(&e.ExplainOptions)
			if ex.server.cfg.TestingKnobs.DeterministicExplain {
				flags.Deflake = explain.DeflakeAll
			}
			ih.SetOutputMode(explainAnalyzePlanOutput, flags)

		case tree.ExplainDistSQL:
			telemetry.Inc(sqltelemetry.ExplainAnalyzeDistSQLUseCounter)
			flags := explain.MakeFlags(&e.ExplainOptions)
			if ex.server.cfg.TestingKnobs.DeterministicExplain {
				flags.Deflake = explain.DeflakeAll
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

		// Recompute statement fingerprint since the AST has changed.
		flags := tree.FmtHideConstants | stmtFingerprintFmtMask
		f := tree.NewFmtCtx(flags)
		f.FormatNode(ast)
		stmtFingerprint = f.CloseAndGetString()

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
			return makeErrEvent(newPreparedStmtDNEError(ex.sessionData(), name))
		}
		ex.extraTxnState.prepStmtsNamespace.touchLRUEntry(name)

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
		stmt.StmtSummary = ps.StatementSummary
		stmtFingerprint = stmt.StmtNoConstants
		res.ResetStmtType(ps.AST)

		if e.DiscardRows {
			ih.SetDiscardRows()
		}
		ast = stmt.Statement.AST
	}

	// For pausable portal, the instrumentation helper needs to be set up only
	// when the portal is executed for the first time.
	if !isPausablePortal() || portal.pauseInfo.execStmtInOpenState.ihWrapper == nil {
		ctx = ih.Setup(
			ctx, ex.server.cfg, ex.statsCollector, p, ex.stmtDiagnosticsRecorder,
			stmt.StmtNoConstants, os.ImplicitTxn.Get(),
			// This goroutine is the only one that can modify
			// txnState.mu.priority, so we don't need to get a mutex here.
			ex.state.mu.priority,
			ex.extraTxnState.shouldCollectTxnExecutionStats,
		)
	} else {
		ctx = portal.pauseInfo.execStmtInOpenState.ihWrapper.ctx
	}
	// For pausable portals, we need to persist the instrumentationHelper as it
	// shares the ctx with the underlying flow. If it got cleaned up before we
	// clean up the flow, we will hit `span used after finished` whenever we log
	// an event when cleaning up the flow.
	// We need this seemingly weird wrapper here because we set the planner's ih
	// with its pointer. However, for pausable portal, we'd like to persist the
	// ih and reuse it for all re-executions. So the planner's ih and the portal's
	// ih should never have the same address, otherwise changing the former will
	// change the latter, and we will never be able to persist it.
	if isPausablePortal() {
		if portal.pauseInfo.execStmtInOpenState.ihWrapper == nil {
			portal.pauseInfo.execStmtInOpenState.ihWrapper = &instrumentationHelperWrapper{
				ctx: ctx,
				ih:  *ih,
			}
		} else {
			p.instrumentation = portal.pauseInfo.execStmtInOpenState.ihWrapper.ih
		}
	}

	// Note that here we always unconditionally defer a function that takes care
	// of finishing the instrumentation helper. This is needed since in order to
	// support plan-gist-matching of the statement diagnostics we might not know
	// right now whether Finish needs to happen.
	defer processCleanupFunc("finish instrumentation helper", func() {
		// We need this weird thing because we need to make sure we're
		// closing the correct instrumentation helper for the paused portal.
		ihToFinish := ih
		curRes := res
		if isPausablePortal() {
			ihToFinish = &portal.pauseInfo.execStmtInOpenState.ihWrapper.ih
			curRes = portal.pauseInfo.curRes
			retErr = portal.pauseInfo.execStmtInOpenState.retErr
			retPayload = portal.pauseInfo.execStmtInOpenState.retPayload
		}
		if ihToFinish.needFinish {
			retErr = ihToFinish.Finish(
				ex.server.cfg,
				ex.statsCollector,
				&ex.extraTxnState.accumulatedStats,
				ihToFinish.collectExecStats,
				p,
				ast,
				stmt.SQL,
				curRes,
				retPayload,
				retErr,
			)
		}
	})

	if ex.sessionData().TransactionTimeout > 0 && !ex.implicitTxn() && ex.executorType != executorTypeInternal {
		timerDuration :=
			ex.sessionData().TransactionTimeout - timeutil.Since(ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionTransactionStarted))

		// If the timer already expired, but the transaction is not yet aborted,
		// we should error immediately without executing. If the timer
		// expired but the transaction already is aborted, then we should still
		// proceed with executing the statement in order to get a
		// TransactionAbortedError.
		_, txnAborted := ex.machine.CurState().(stateAborted)

		if timerDuration < 0 && !txnAborted {
			txnTimedOut = true
			return makeErrEvent(sqlerrors.TxnTimeoutError)
		}

		if timerDuration > 0 {
			txnDoneAfterFunc = make(chan struct{}, 1)
			txnTimeoutTicker = time.AfterFunc(
				timerDuration,
				func() {
					cancelQuery()
					txnTimedOut = true
					txnDoneAfterFunc <- struct{}{}
				})
		}
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
		queryDoneAfterFunc = make(chan struct{}, 1)
		queryTimeoutTicker = time.AfterFunc(
			timerDuration,
			func() {
				cancelQuery()
				queryTimedOut = true
				queryDoneAfterFunc <- struct{}{}
			})
	}

	defer func(ctx context.Context) {
		if filter := ex.server.cfg.TestingKnobs.StatementFilter; retErr == nil && filter != nil {
			var execErr error
			if perr, ok := retPayload.(payloadWithError); ok {
				execErr = perr.errorCause()
			}
			filter(ctx, ex.sessionData(), stmt.AST.String(), execErr)
		}

		// Do the auto-commit, if necessary. In the extended protocol, the
		// auto-commit happens when the Sync message is handled.
		if retEv != nil || retErr != nil {
			return
		}
		// As portals are from extended protocol, we don't auto commit for them.
		if canAutoCommit && !isExtendedProtocol {
			retEv, retPayload = ex.handleAutoCommit(ctx, ast)
		}
	}(ctx)

	// If adminAuditLogging is enabled, we want to check for HasAdminRole
	// before maybeLogStatement.
	// We must check prior to execution in the case the txn is aborted due to
	// an error. HasAdminRole can only be checked in a valid txn.
	if adminAuditLog := adminAuditLogEnabled.Get(
		&ex.planner.execCfg.Settings.SV,
	); adminAuditLog {
		if !ex.extraTxnState.hasAdminRoleCache.IsSet {
			hasAdminRole, err := ex.planner.HasAdminRole(ctx)
			if err != nil {
				return makeErrEvent(err)
			}
			ex.extraTxnState.hasAdminRoleCache.HasAdminRole = hasAdminRole
			ex.extraTxnState.hasAdminRoleCache.IsSet = true
		}
	}

	p.stmt = stmt
	p.semaCtx.Annotations = tree.MakeAnnotations(stmt.NumAnnotations)
	p.extendedEvalCtx.Annotations = &p.semaCtx.Annotations
	p.semaCtx.Placeholders.Assign(pinfo, stmt.NumPlaceholders)
	p.extendedEvalCtx.Placeholders = &p.semaCtx.Placeholders

	shouldLogToExecAndAudit := true
	defer func() {
		if !shouldLogToExecAndAudit {
			// We don't want to log this statement, since another layer of the
			// conn_executor will handle the logging for this statement.
			return
		}

		p.curPlan.init(&p.stmt, &p.instrumentation)
		var execErr error
		if p, ok := retPayload.(payloadWithError); ok {
			execErr = p.errorCause()
		}
		stmtFingerprintID := appstatspb.ConstructStatementFingerprintID(
			stmtFingerprint,
			ex.implicitTxn(),
			p.CurrentDatabase(),
		)

		p.maybeLogStatement(
			ctx,
			ex.executorType,
			int(ex.state.mu.autoRetryCounter),
			int(ex.extraTxnState.txnCounter.Load()),
			0, /* rowsAffected */
			ex.state.mu.stmtCount,
			0, /* bulkJobId */
			execErr,
			ex.statsCollector.PhaseTimes().GetSessionPhaseTime(sessionphase.SessionQueryReceived),
			&ex.extraTxnState.hasAdminRoleCache,
			ex.server.TelemetryLoggingMetrics,
			stmtFingerprintID,
			&topLevelQueryStats{},
			ex.statsCollector,
			ex.extraTxnState.shouldLogToTelemetry)
	}()

	switch s := ast.(type) {
	case *tree.BeginTransaction:
		// BEGIN is only allowed if we are in an implicit txn.
		if os.ImplicitTxn.Get() {
			// When executing the BEGIN, we also need to set any transaction modes
			// that were specified on the BEGIN statement.
			if _, err := ex.planner.SetTransaction(ctx, &tree.SetTransaction{Modes: s.Modes}); err != nil {
				return makeErrEvent(err)
			}
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

	case *tree.ShowCommitTimestamp:
		ev, payload := ex.execShowCommitTimestampInOpenState(ctx, s, res, canAutoCommit)
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
		if _, ok := s.Statement.(*tree.ExplainAnalyze); ok {
			// Prohibit the explicit PREPARE ... AS EXPLAIN ANALYZE since we
			// won't be able to execute the prepared statement. This is also in
			// line with Postgres.
			err := pgerror.Newf(
				pgcode.Syntax,
				"EXPLAIN ANALYZE can only be used as a top-level statement",
			)
			return makeErrEvent(err)
		}
		var typeHints tree.PlaceholderTypes
		// We take max(len(s.Types), stmt.NumPlaceHolders) as the length of types.
		numParams := len(s.Types)
		if stmt.NumPlaceholders > numParams {
			numParams = stmt.NumPlaceholders
		}
		if len(s.Types) > 0 {
			typeHints = make(tree.PlaceholderTypes, numParams)
			for i, t := range s.Types {
				resolved, err := tree.ResolveType(ctx, t, ex.planner.semaCtx.GetTypeResolver())
				if err != nil {
					return makeErrEvent(err)
				}
				typeHints[i] = resolved
			}
		}
		prepStmt := makeStatement(
			statements.Statement[tree.Statement]{
				// We need the SQL string just for the part that comes after
				// "PREPARE ... AS",
				// TODO(radu): it would be nice if the parser would figure out this
				// string and store it in tree.Prepare.
				SQL:             tree.AsStringWithFlags(s.Statement, tree.FmtParsable),
				AST:             s.Statement,
				NumPlaceholders: stmt.NumPlaceholders,
				NumAnnotations:  stmt.NumAnnotations,
			},
			ex.server.cfg.GenerateID(),
			tree.FmtFlags(queryFormattingForFingerprintsMask.Get(&ex.server.cfg.Settings.SV)),
		)
		var rawTypeHints []oid.Oid

		// Placeholders should be part of the statement being prepared, not the
		// PREPARE statement itself.
		oldPlaceholders := p.extendedEvalCtx.Placeholders
		p.extendedEvalCtx.Placeholders = nil
		defer func() {
			// The call to addPreparedStmt changed the planner stmt to the
			// statement being prepared. Set it back to the PREPARE statement,
			// so that it's logged correctly.
			p.stmt = stmt
			p.extendedEvalCtx.Placeholders = oldPlaceholders
		}()
		if _, err := ex.addPreparedStmt(
			ctx, name, prepStmt, typeHints, rawTypeHints, PreparedStatementOriginSQL,
		); err != nil {
			return makeErrEvent(err)
		}
		return nil, nil, nil
	}

	// Don't write to the exec/audit logs here; it will be handled in
	// dispatchToExecutionEngine.
	shouldLogToExecAndAudit = false

	if tree.CanModifySchema(ast) &&
		(!ex.planner.EvalContext().TxnIsSingleStmt || !ex.implicitTxn()) &&
		ex.extraTxnState.firstStmtExecuted &&
		ex.sessionData().AutoCommitBeforeDDL &&
		ex.executorType != executorTypeInternal {
		if err := ex.planner.SendClientNotice(
			ctx,
			pgnotice.Newf("auto-committing transaction before processing DDL due to autocommit_before_ddl setting"),
		); err != nil {
			return nil, nil, err
		}
		retEv, retPayload = ex.handleAutoCommit(ctx, ast)
		if _, committed := retEv.(eventTxnFinishCommitted); committed && retPayload == nil {
			// Use eventTxnCommittedDueToDDL so that the current statement gets
			// executed again when the state machine advances.
			retEv = eventTxnCommittedDueToDDL{}
		}
		return
	}

	// For regular statements (the ones that get to this point), we
	// don't return any event unless an error happens, or a CALL statement
	// performs a nested transaction COMMIT or ROLLBACK.

	// For a portal (prepared stmt), since handleAOST() is called when preparing
	// the statement, and this function is idempotent, we don't need to
	// call it again during execution.
	if portal == nil {
		if err := ex.handleAOST(ctx, ast); err != nil {
			return makeErrEvent(err)
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
	// that all uses of SQL execution initialize the kv.Txn using a
	// single/common function. That would be where the stepping mode
	// gets enabled once for all SQL statements executed "underneath".
	prevSteppingMode := ex.state.mu.txn.ConfigureStepping(ctx, kv.SteppingEnabled)
	prevSeqNum := ex.state.mu.txn.GetReadSeqNum()
	delegatedFromOuterTxn := ex.executorType == executorTypeInternal && ex.extraTxnState.fromOuterTxn
	var origTs hlc.Timestamp
	defer func() {
		_ = ex.state.mu.txn.ConfigureStepping(ctx, prevSteppingMode)

		// If this is an internal executor that is running on behalf of an outer
		// txn, then we need to step back the txn so that the outer executor uses
		// the proper sequence number.
		if delegatedFromOuterTxn {
			if err := ex.state.mu.txn.SetReadSeqNum(prevSeqNum); err != nil {
				retEv, retPayload, retErr = makeErrEvent(err)
			}
		}
	}()

	// Then we create a sequencing point.
	//
	// This is not the only place where a sequencing point is placed. There are
	// also sequencing point after every stage of constraint checks and cascading
	// actions at the _end_ of a statement's execution.
	//
	// If this is an internal executor running on behalf of an outer txn, then we
	// also need to make sure the external read timestamp is not bumped. Normally,
	// that happens whenever a READ COMMITTED txn is stepped.
	//
	// Under test builds, we add a few extra assertions to ensure that the
	// external read timestamp does not change if it shouldn't, and that we use
	// the correct isolation level for internal operations.
	if buildutil.CrdbTestBuild {
		if delegatedFromOuterTxn {
			origTs = ex.state.mu.txn.ReadTimestamp()
		} else if ex.executorType == executorTypeInternal {
			if level := ex.state.mu.txn.IsoLevel(); level != isolation.Serializable {
				return nil, nil, errors.AssertionFailedf(
					"internal operation is not using SERIALIZABLE isolation; found=%s",
					level,
				)
			}
		}
	}
	if err := ex.state.mu.txn.Step(ctx, !delegatedFromOuterTxn /* allowReadTimestampStep */); err != nil {
		return makeErrEvent(err)
	}
	if buildutil.CrdbTestBuild && delegatedFromOuterTxn {
		newTs := ex.state.mu.txn.ReadTimestamp()
		if newTs != origTs {
			// This should never happen. If it does, it means that the internal
			// executor incorrectly moved the txn's read timestamp forward.
			return nil, nil, errors.AssertionFailedf(
				"internal executor advanced the txn read timestamp. origTs=%s, newTs=%s",
				origTs, newTs,
			)
		}
	}

	if isPausablePortal() {
		p.pausablePortal = portal
	}

	// Auto-commit is disallowed during statement execution if we previously
	// executed any DDL. This is because may potentially create jobs and do other
	// operations rather than a KV commit.
	// This prevents commit during statement execution, but the conn_executor
	// will still commit this transaction after this statement executes.
	p.autoCommit = canAutoCommit &&
		!ex.server.cfg.TestingKnobs.DisableAutoCommitDuringExec && ex.extraTxnState.numDDL == 0
	p.extendedEvalCtx.TxnIsSingleStmt = canAutoCommit && !ex.extraTxnState.firstStmtExecuted
	defer func() { ex.extraTxnState.firstStmtExecuted = true }()

	var stmtThresholdSpan *tracing.Span
	alreadyRecording := ex.transitionCtx.sessionTracing.Enabled()
	// TODO(sql-sessions): fix the stmtTraceThreshold for pausable portals, so
	// that it records all executions.
	// https://github.com/cockroachdb/cockroach/issues/99404
	stmtTraceThreshold := TraceStmtThreshold.Get(&ex.planner.execCfg.Settings.SV)
	var stmtCtx context.Context
	// TODO(andrei): I think we should do this even if alreadyRecording == true.
	if !alreadyRecording && stmtTraceThreshold > 0 {
		stmtCtx, stmtThresholdSpan = tracing.EnsureChildSpan(ctx, ex.server.cfg.AmbientCtx.Tracer, "trace-stmt-threshold", tracing.WithRecording(tracingpb.RecordingVerbose))
	} else {
		stmtCtx = ctx
	}

	var rollbackHomeRegionSavepoint *tree.RollbackToSavepoint
	var releaseHomeRegionSavepoint *tree.ReleaseSavepoint
	enforceHomeRegion := p.EnforceHomeRegion()
	_, isSelectStmt := stmt.AST.(*tree.Select)
	// TODO(sql-sessions): ensure this is not broken for pausable portals.
	// https://github.com/cockroachdb/cockroach/issues/99408
	if enforceHomeRegion && ex.state.mu.txn.IsOpen() && isSelectStmt {
		// Create a savepoint at a point before which rows were read so that we can
		// roll back to it, which will allow the txn to be modified with a
		// historical timestamp (so that the locality-optimized ops used for error
		// reporting can run locally and not incur latency). This is currently only
		// supported for SELECT statements.
		// Add some unprintable ASCII characters to the name of the savepoint to
		// decrease the likelihood of collision with a user-created savepoint.
		const enforceHomeRegionSavepointName = "enforce_home_region_sp\x11\x12\x13"
		s := &tree.Savepoint{Name: enforceHomeRegionSavepointName}
		var event fsm.Event
		var eventPayload fsm.EventPayload
		if event, eventPayload, err = ex.execSavepointInOpenState(ctx, s, res); err != nil {
			return event, eventPayload, err
		}

		releaseHomeRegionSavepoint = &tree.ReleaseSavepoint{Savepoint: enforceHomeRegionSavepointName}
		rollbackHomeRegionSavepoint = &tree.RollbackToSavepoint{Savepoint: enforceHomeRegionSavepointName}
		defer func() {
			// The default case is to roll back the internally-generated savepoint
			// after every request. We only need it if a retryable "query has no home
			// region" error occurs.
			ex.execRelease(ctx, releaseHomeRegionSavepoint, res)
		}()
	}

	if ex.state.mu.txn.IsoLevel() == isolation.ReadCommitted &&
		!ex.implicitTxn() &&
		ex.executorType != executorTypeInternal {
		// If an internal executor query that is run as part of a larger statement
		// throws a retryable error, that error should be returned up and retried by
		// the statement's dispatchReadCommittedStmtToExecutionEngine retry loop.
		// TODO(rafi): The above should be happening already, but find a way to
		// test it.
		if err := ex.dispatchReadCommittedStmtToExecutionEngine(stmtCtx, p, res); err != nil {
			stmtThresholdSpan.Finish()
			return nil, nil, err
		}
	} else {
		if err := ex.dispatchToExecutionEngine(stmtCtx, p, res); err != nil {
			stmtThresholdSpan.Finish()
			return nil, nil, err
		}
	}

	if stmtThresholdSpan != nil {
		stmtDur := timeutil.Since(ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionQueryReceived))
		if needRecording := stmtDur >= stmtTraceThreshold; needRecording {
			rec := stmtThresholdSpan.FinishAndGetRecording(tracingpb.RecordingVerbose)
			// NB: This recording does not include the commit for implicit
			// transactions if the statement didn't auto-commit.
			redactableStmt := p.FormatAstAsRedactableString(stmt.AST, &p.semaCtx.Annotations)
			logTraceAboveThreshold(
				ctx,
				rec,                /* recording */
				"SQL statement",    /* opName */
				redactableStmt,     /* detail */
				stmtTraceThreshold, /* threshold */
				stmtDur,            /* elapsed */
			)
		} else {
			stmtThresholdSpan.Finish()
		}
	}

	if err = res.Err(); err != nil {
		setErrorAndRestoreLocality := func(err error) {
			res.SetError(err)
			// We won't be faking the gateway region any more. Restore the original
			// locality.
			p.EvalContext().Locality = p.EvalContext().OriginalLocality
		}
		if execinfra.IsDynamicQueryHasNoHomeRegionError(err) {
			if rollbackHomeRegionSavepoint != nil {
				// A retryable "query has no home region" error has occurred.
				// Roll back to the internal savepoint in preparation for the next
				// planning and execution of this query with a different gateway region
				// (as considered by the optimizer).
				p.StmtNoConstantsWithHomeRegionEnforced = p.stmt.StmtNoConstants
				event, eventPayload := ex.execRollbackToSavepointInOpenState(
					ctx, rollbackHomeRegionSavepoint, res,
				)
				_, isTxnRestart := event.(eventTxnRestart)
				rollbackToSavepointFailed := !isTxnRestart || eventPayload != nil
				if ex.implicitTxn() && rollbackToSavepointFailed {
					err = errors.AssertionFailedf(
						"unable to roll back to internal savepoint for enforce_home_region",
					)
					setErrorAndRestoreLocality(err)
				} else if rollbackToSavepointFailed || int(ex.state.mu.autoRetryCounter) == len(ex.planner.EvalContext().RemoteRegions) {
					// If rollback to savepoint in the transaction failed (perhaps because
					// the txn was aborted) and we're in an explicit transaction, or we
					// have retried the statement using each remote region as a fake
					// gateway region, then give up and return the generic "query has no
					// home region" error message.
					err = execinfra.MaybeGetNonRetryableDynamicQueryHasNoHomeRegionError(err)
					setErrorAndRestoreLocality(err)
				}
			} else {
				err = execinfra.MaybeGetNonRetryableDynamicQueryHasNoHomeRegionError(err)
				setErrorAndRestoreLocality(err)
			}
		} else if execinfra.IsDynamicQueryHasNoHomeRegionError(ex.state.mu.autoRetryReason) {
			// If we are retrying a dynamic "query has no home region" error and
			// we get a different error message when executing with locality-optimized
			// ops using a different local region (for example, relation does not
			// exist, due to the AOST read), return the original error message in
			// non-retryable form.
			errorMessage := err.Error()
			if !strings.HasPrefix(errorMessage, execinfra.QueryNotRunningInHomeRegionMessagePrefix) {
				err = execinfra.MaybeGetNonRetryableDynamicQueryHasNoHomeRegionError(ex.state.mu.autoRetryReason)
				setErrorAndRestoreLocality(err)
			}
		}
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
			payload := eventRetriableErrPayload{
				err:    txn.GenerateForcedRetryableErr(ctx, "serializable transaction timestamp pushed (detected by connExecutor)"),
				rewCap: rc,
			}
			return ev, payload, nil
		}
		log.VEventf(ctx, 2, "push detected for non-refreshable txn but auto-retry not possible")
	}

	// Special handling for explicit transaction management in CALL statements.
	// A stored procedure has executed a COMMIT or ROLLBACK statement and
	// suspended its execution. Direct the connExecutor to commit/rollback the
	// current transaction, and then resume execution within the new transaction.
	switch ex.extraTxnState.storedProcTxnState.txnOp {
	case tree.StoredProcTxnCommit:
		// Commit the current transaction. The connExecutor will open a new
		// transaction, and then return to executing the same CALL statement.
		ev, payload := ex.commitSQLTransaction(ctx, ast, ex.commitSQLTransactionInternal)
		if payload != nil {
			return ev, payload, nil
		}
		return eventTxnFinishCommittedPLpgSQL{}, nil, nil
	case tree.StoredProcTxnRollback:
		// Abort the current transaction. The connExecutor will open a new
		// transaction, and then return to executing the same CALL statement.
		ev, payload := ex.rollbackSQLTransaction(ctx, ast)
		if payload != nil {
			return ev, payload, nil
		}
		return eventTxnFinishAbortedPLpgSQL{}, nil, nil
	}

	// No event was generated.
	return nil, nil, nil
}

// handleAOST gets the AsOfSystemTime clause from the statement, and sets
// the timestamps of the transaction accordingly.
func (ex *connExecutor) handleAOST(ctx context.Context, stmt tree.Statement) error {
	p := &ex.planner
	if _, isNoTxn := ex.machine.CurState().(stateNoTxn); isNoTxn {
		if _, ok := stmt.(*tree.ShowCommitTimestamp); ok {
			return nil
		}
		return errors.AssertionFailedf(
			"cannot handle AOST clause without a transaction",
		)
	} else if execinfra.IsDynamicQueryHasNoHomeRegionError(ex.state.mu.autoRetryReason) {
		asOfClause := tree.AsOfClause{Expr: followerReadTimestampExpr}
		// Set the timestamp used by current_timestamp().
		asOf, err := p.EvalAsOfTimestamp(ctx, asOfClause, asof.OptionAllowBoundedStaleness)
		if err != nil {
			return errors.AssertionFailedf(
				"problem evaluating follower read timestamp for enforce_home_region dynamic error checking",
			)
		}
		// Set up AOST in the txn so re-running of the query with different possible
		// home regions does not have to read rows from remote regions.
		p.extendedEvalCtx.SetTxnTimestamp(asOf.Timestamp.GoTime())
		if err := ex.state.setHistoricalTimestamp(ctx, asOf.Timestamp); err != nil {
			// If the table was just created, we may not be able to set a historical
			// timestamp.
			return execinfra.MaybeGetNonRetryableDynamicQueryHasNoHomeRegionError(ex.state.mu.autoRetryReason)
		}
	}
	asOf, err := p.isAsOf(ctx, stmt)
	if err != nil {
		return err
	}
	if asOf == nil {
		return nil
	}

	// Implicit transactions can have multiple statements, so we need to check
	// if one has already been executed.
	if ex.implicitTxn() && !ex.extraTxnState.firstStmtExecuted {
		if p.extendedEvalCtx.AsOfSystemTime == nil {
			p.extendedEvalCtx.AsOfSystemTime = asOf
			if !asOf.BoundedStaleness {
				p.extendedEvalCtx.SetTxnTimestamp(asOf.Timestamp.GoTime())
				if err := ex.state.setHistoricalTimestamp(ctx, asOf.Timestamp); err != nil {
					return err
				}
			}
			if err := ex.state.setReadOnlyMode(tree.ReadOnly); err != nil {
				return err
			}
			p.extendedEvalCtx.TxnReadOnly = ex.state.readOnly.Load()
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
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"cannot specify AS OF SYSTEM TIME with different timestamps. expected: %s, got: %s",
			p.extendedEvalCtx.AsOfSystemTime.Timestamp,
			asOf.Timestamp,
		)
	}
	// If we're in an explicit txn, we allow AOST but only if it matches with
	// the transaction's timestamp. This is useful for running AOST statements
	// using the Executor inside an external transaction; one might want
	// to do that to force p.avoidLeasedDescriptors to be set below.
	if asOf.BoundedStaleness {
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"cannot use a bounded staleness query in a transaction",
		)
	}
	if readTs := ex.state.getReadTimestamp(); asOf.Timestamp != readTs {
		err = pgerror.Newf(pgcode.FeatureNotSupported,
			"inconsistent AS OF SYSTEM TIME timestamp; expected: %s, got: %s", readTs, asOf.Timestamp)
		if !ex.implicitTxn() {
			err = errors.WithHint(err, "try SET TRANSACTION AS OF SYSTEM TIME")
		}
		return err
	}
	p.extendedEvalCtx.AsOfSystemTime = asOf
	return nil
}

func formatWithPlaceholders(ctx context.Context, ast tree.Statement, evalCtx *eval.Context) string {
	var fmtCtx *tree.FmtCtx
	fmtFlags := tree.FmtSimple

	if evalCtx.HasPlaceholders() {
		fmtCtx = evalCtx.FmtCtx(
			fmtFlags,
			tree.FmtPlaceholderFormat(func(fmtCtx *tree.FmtCtx, placeholder *tree.Placeholder) {
				d, err := eval.Expr(ctx, evalCtx, placeholder)
				if err != nil || d == nil {
					// Fall back to the default behavior if something goes wrong.
					fmtCtx.Printf("$%d", placeholder.Idx+1)
					return
				}
				d.Format(fmtCtx)
			}),
		)
	} else {
		fmtCtx = evalCtx.FmtCtx(fmtFlags)
	}

	fmtCtx.FormatNode(ast)

	return fmtCtx.CloseAndGetString()
}

// checkDescriptorTwoVersionInvariant ensures that the two version invariant is
// upheld. It calls descs.CheckTwoVersionInvariant, which will restart the
// underlying transaction in the case that the invariant is not upheld, and
// it will cleanup any intents due to that transaction. When this happens, the
// transaction will be reset internally.
func (ex *connExecutor) checkDescriptorTwoVersionInvariant(ctx context.Context) error {
	var inRetryBackoff func()
	if knobs := ex.server.cfg.SchemaChangerTestingKnobs; knobs != nil {
		inRetryBackoff = knobs.TwoVersionLeaseViolation
	}
	regionCache, err := regions.NewCachedDatabaseRegions(ctx, ex.server.cfg.DB, ex.server.cfg.LeaseManager)
	if err != nil {
		return err
	}
	return descs.CheckTwoVersionInvariant(
		ctx,
		ex.server.cfg.Clock,
		ex.server.cfg.InternalDB,
		ex.server.cfg.Codec,
		ex.extraTxnState.descCollection,
		regionCache,
		ex.server.cfg.Settings,
		ex.state.mu.txn,
		inRetryBackoff,
	)
}

// Create a new transaction to retry with a higher timestamp than the timestamps
// used in any retry loop above. Additionally, make sure to copy out the
// priority from the previous transaction to ensure that livelock does not
// occur.
func (ex *connExecutor) resetTransactionOnSchemaChangeRetry(ctx context.Context) error {
	ex.state.mu.Lock()
	defer ex.state.mu.Unlock()
	userPriority := ex.state.mu.txn.UserPriority()
	omitInRangefeeds := ex.state.mu.txn.GetOmitInRangefeeds()
	newTxn := kv.NewTxnWithSteppingEnabled(ctx, ex.transitionCtx.db,
		ex.transitionCtx.nodeIDOrZero, ex.QualityOfService())
	if err := newTxn.SetUserPriority(userPriority); err != nil {
		return err
	}
	if omitInRangefeeds {
		newTxn.SetOmitInRangefeeds()
	}
	ex.state.mu.txn = newTxn
	return nil
}

// commitSQLTransaction executes a commit after the execution of a
// stmt, which can be any statement when executing a statement with an
// implicit transaction, or a COMMIT statement when using an explicit
// transaction. commitFn is passed as a separate function, so that we avoid
// executing transactional logic when handling COMMIT in the CommitWait state.
func (ex *connExecutor) commitSQLTransaction(
	ctx context.Context, ast tree.Statement, commitFn func(context.Context) error,
) (fsm.Event, fsm.EventPayload) {
	ex.extraTxnState.idleLatency += ex.statsCollector.PhaseTimes().
		GetIdleLatency(ex.statsCollector.PreviousPhaseTimes())
	if ex.sessionData().InjectRetryErrorsOnCommitEnabled && ast.StatementTag() == "COMMIT" {
		if ex.state.injectedTxnRetryCounter < numTxnRetryErrors {
			retryErr := ex.state.mu.txn.GenerateForcedRetryableErr(
				ctx, "injected by `inject_retry_errors_on_commit_enabled` session variable")
			ex.state.injectedTxnRetryCounter++
			return ex.makeErrEvent(retryErr, ast)
		}
	}
	ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionStartTransactionCommit, timeutil.Now())
	if err := commitFn(ctx); err != nil {
		// For certain retryable errors, we should turn them into client visible
		// errors, since the client needs to retry now.
		var conversionError error
		err, conversionError = ex.convertRetriableErrorIntoUserVisibleError(ctx, err)
		if conversionError != nil {
			return ex.makeErrEvent(conversionError, ast)
		}
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

func (ex *connExecutor) commitSQLTransactionInternal(ctx context.Context) (retErr error) {
	ctx, sp := tracing.EnsureChildSpan(ctx, ex.server.cfg.AmbientCtx.Tracer, "commit sql txn")
	defer sp.Finish()

	defer func() {
		failed := retErr != nil
		ex.recordDDLTxnTelemetry(failed)
	}()

	if err := ex.extraTxnState.sqlCursors.closeAll(cursorCloseForTxnCommit); err != nil {
		return err
	}

	ex.extraTxnState.prepStmtsNamespace.closeAllPortals(ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc)

	// We need to step the transaction's internal read sequence before committing
	// if it has stepping enabled. If it doesn't have stepping enabled, then we
	// just set the stepping mode back to what it was.
	//
	// Even if we do step the transaction's internal read sequence, we do not
	// advance its external read timestamp (applicable only to read committed
	// transactions). This is because doing so is not needed before committing,
	// and it would cause the transaction to commit at a higher timestamp than
	// necessary. On heavily contended workloads like the one from #109628, this
	// can cause unnecessary write-write contention between transactions by
	// inflating the contention footprint of each transaction (i.e. the duration
	// measured in MVCC time that the transaction holds locks).
	prevSteppingMode := ex.state.mu.txn.ConfigureStepping(ctx, kv.SteppingEnabled)
	if prevSteppingMode == kv.SteppingEnabled {
		if err := ex.state.mu.txn.Step(ctx, false /* allowReadTimestampStep */); err != nil {
			return err
		}
	} else {
		ex.state.mu.txn.ConfigureStepping(ctx, prevSteppingMode)
	}

	if err := ex.createJobs(ctx); err != nil {
		return err
	}

	if ex.extraTxnState.schemaChangerState.mode != sessiondatapb.UseNewSchemaChangerOff {
		if err := ex.runPreCommitStages(ctx); err != nil {
			return err
		}
	}

	if ex.extraTxnState.descCollection.HasUncommittedDescriptors() {
		zoneConfigValidator := newZoneConfigValidator(ex.state.mu.txn,
			ex.extraTxnState.descCollection,
			ex.planner.regionsProvider(),
			ex.planner.execCfg)
		if err := ex.extraTxnState.descCollection.ValidateUncommittedDescriptors(ctx, ex.state.mu.txn, ex.extraTxnState.validateDbZoneConfig, zoneConfigValidator); err != nil {
			return err
		}

		if err := descs.CheckSpanCountLimit(
			ctx,
			ex.extraTxnState.descCollection,
			ex.server.cfg.SpanConfigSplitter,
			ex.server.cfg.SpanConfigLimiter,
			ex.state.mu.txn,
		); err != nil {
			return err
		}

		if err := ex.checkDescriptorTwoVersionInvariant(ctx); err != nil {
			return err
		}
	}

	if err := ex.state.mu.txn.Commit(ctx); err != nil {
		return err
	}

	// Now that we've committed, if we modified any descriptor we need to make sure
	// to release the leases for them so that the schema change can proceed and
	// we don't block the client.
	withNewVersion, err := ex.extraTxnState.descCollection.GetOriginalPreviousIDVersionsForUncommitted()
	if err != nil || withNewVersion == nil {
		return err
	}
	ex.extraTxnState.descCollection.ReleaseLeases(ctx)
	return nil
}

// recordDDLTxnTelemetry records telemetry for explicit transactions that
// contain DDL.
func (ex *connExecutor) recordDDLTxnTelemetry(failed bool) {
	numDDL, numStmts := ex.extraTxnState.numDDL, ex.state.mu.stmtCount
	if numDDL == 0 || ex.implicitTxn() {
		return
	}
	// Subtract 1 statement so the COMMIT/ROLLBACK is not counted.
	if numDDL == numStmts-1 {
		if failed {
			telemetry.Inc(sqltelemetry.DDLOnlyTransactionFailureCounter)
		} else {
			telemetry.Inc(sqltelemetry.DDLOnlyTransactionSuccessCounter)
		}
	} else /* numDDL != numStmts-1 */ {
		if failed {
			telemetry.Inc(sqltelemetry.MixedDDLDMLTransactionFailureCounter)
		} else {
			telemetry.Inc(sqltelemetry.MixedDDLDMLTransactionSuccessCounter)
		}
	}
}

// createJobs creates jobs for the records cached in schemaChangeJobRecords
// during this transaction.
func (ex *connExecutor) createJobs(ctx context.Context) error {
	if !ex.extraTxnState.jobs.hasAnyToCreate() {
		return nil
	}
	var records []*jobs.Record
	if err := ex.extraTxnState.jobs.forEachToCreate(func(jobRecord *jobs.Record) error {
		records = append(records, jobRecord)
		return nil
	}); err != nil {
		return err
	}
	jobIDs, err := ex.server.cfg.JobRegistry.CreateJobsWithTxn(
		ctx, ex.planner.InternalSQLTxn(), records,
	)
	if err != nil {
		return err
	}
	ex.planner.extendedEvalCtx.jobs.addCreatedJobID(jobIDs...)
	return nil
}

// rollbackSQLTransaction executes a ROLLBACK statement: the KV transaction is
// rolled-back and an event is produced.
func (ex *connExecutor) rollbackSQLTransaction(
	ctx context.Context, stmt tree.Statement,
) (fsm.Event, fsm.EventPayload) {
	if err := ex.extraTxnState.sqlCursors.closeAll(cursorCloseForTxnRollback); err != nil {
		return ex.makeErrEvent(err, stmt)
	}

	ex.extraTxnState.prepStmtsNamespace.closeAllPortals(ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc)
	ex.recordDDLTxnTelemetry(true /* failed */)

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

// Each statement in an explicit READ COMMITTED transaction has a SAVEPOINT.
// This allows for TransactionRetry errors to be retried automatically. We don't
// do this for implicit transactions because the conn_executor state machine
// already has retry logic for implicit transactions. To avoid having to
// implement the retry logic in the state machine, we use the KV savepoint API
// directly.
func (ex *connExecutor) dispatchReadCommittedStmtToExecutionEngine(
	ctx context.Context, p *planner, res RestrictedCommandResult,
) error {
	readCommittedSavePointToken, err := ex.state.mu.txn.CreateSavepoint(ctx)
	if err != nil {
		return err
	}

	maxRetries := int(ex.sessionData().MaxRetriesForReadCommitted)
	for attemptNum := 0; ; attemptNum++ {
		bufferPos := res.BufferedResultsLen()
		if err = ex.dispatchToExecutionEngine(ctx, p, res); err != nil {
			return err
		}
		maybeRetriableErr := res.Err()
		if maybeRetriableErr == nil {
			// If there was no error, then we must release the savepoint and break.
			if err := ex.state.mu.txn.ReleaseSavepoint(ctx, readCommittedSavePointToken); err != nil {
				return err
			}
			break
		}
		// If the error does not allow for a partial retry, then stop. The error
		// is already set on res.Err() and will be returned to the client.
		var txnRetryErr *kvpb.TransactionRetryWithProtoRefreshError
		if !errors.As(maybeRetriableErr, &txnRetryErr) || txnRetryErr.TxnMustRestartFromBeginning() {
			break
		}

		// If we reached the maximum number of retries, then we must stop.
		if attemptNum == maxRetries {
			res.SetError(errors.Wrapf(
				maybeRetriableErr,
				"read committed retry limit exceeded; set by max_retries_for_read_committed=%d",
				maxRetries,
			))
			break
		}

		// In order to retry the statement, we need to clear any results and
		// errors that were buffered, rollback to the savepoint, then prepare the
		// kv.txn for the partial txn retry.
		if ableToClear := res.TruncateBufferedResults(bufferPos); !ableToClear {
			// If the buffer exceeded the maximum size, then it might have been
			// flushed and sent back to the client already. In that case, we can't
			// retry the statement.
			res.SetError(errors.Wrapf(
				maybeRetriableErr,
				"cannot automatically retry since some results were already sent to the client",
			))
			break
		}
		if knob := ex.server.cfg.TestingKnobs.OnReadCommittedStmtRetry; knob != nil {
			knob(txnRetryErr)
		}
		res.SetError(nil)
		if err := ex.state.mu.txn.RollbackToSavepoint(ctx, readCommittedSavePointToken); err != nil {
			return err
		}
		if err := ex.state.mu.txn.PrepareForPartialRetry(ctx); err != nil {
			return err
		}
		ex.state.mu.autoRetryCounter++
		ex.state.mu.autoRetryReason = txnRetryErr
	}
	return nil
}

// dispatchToExecutionEngine executes the statement, writes the result to res
// and returns an event for the connection's state machine.
//
// If an error is returned, the connection needs to stop processing queries.`
// Query execution errors are written to res; they are not returned; it is`
// expected that the caller will inspect res and react to query errors by
// producing an appropriate state machine event.
func (ex *connExecutor) dispatchToExecutionEngine(
	ctx context.Context, planner *planner, res RestrictedCommandResult,
) (retErr error) {
	getPausablePortalInfo := func() *portalPauseInfo {
		if planner != nil && planner.pausablePortal != nil {
			return planner.pausablePortal.pauseInfo
		}
		return nil
	}
	defer func() {
		if ppInfo := getPausablePortalInfo(); ppInfo != nil {
			if !ppInfo.dispatchToExecutionEngine.cleanup.isComplete {
				ppInfo.dispatchToExecutionEngine.cleanup.isComplete = true
			}
			if retErr != nil || res.Err() != nil {
				ppInfo.resumableFlow.cleanup.run()
				ppInfo.dispatchToExecutionEngine.cleanup.run()
			}
		}
	}()

	stmt := planner.stmt
	ex.sessionTracing.TracePlanStart(ctx, stmt.AST.StatementTag())
	// TODO(sql-sessions): fix the phase time for pausable portals.
	// https://github.com/cockroachdb/cockroach/issues/99410
	ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.PlannerStartLogicalPlan, timeutil.Now())

	if execinfra.IncludeRUEstimateInExplainAnalyze.Get(ex.server.cfg.SV()) {
		if server := ex.server.cfg.DistSQLSrv; server != nil {
			// Begin measuring CPU usage for tenants. This is a no-op for non-tenants.
			ex.cpuStatsCollector.StartCollection(ctx, server.TenantCostController)
		}
	}

	var err error

	if ppInfo := getPausablePortalInfo(); ppInfo != nil {
		if !ppInfo.dispatchToExecutionEngine.cleanup.isComplete {
			err = ex.makeExecPlan(ctx, planner)
			if flags := planner.curPlan.flags; err == nil && (flags.IsSet(planFlagContainsMutation) || flags.IsSet(planFlagIsDDL)) {
				telemetry.Inc(sqltelemetry.NotReadOnlyStmtsTriedWithPausablePortals)
				// We don't allow mutations in a pausable portal. Set it back to
				// an un-pausable (normal) portal.
				planner.pausablePortal.pauseInfo = nil
				err = res.RevokePortalPausability()
				defer planner.curPlan.close(ctx)
			} else {
				ppInfo.dispatchToExecutionEngine.planTop = planner.curPlan
				ppInfo.dispatchToExecutionEngine.cleanup.appendFunc(namedFunc{
					fName: "close planTop",
					f:     func() { ppInfo.dispatchToExecutionEngine.planTop.close(ctx) },
				})
			}
		} else {
			planner.curPlan = ppInfo.dispatchToExecutionEngine.planTop
		}
	} else {
		// Prepare the plan. Note, the error is processed below. Everything
		// between here and there needs to happen even if there's an error.
		err = ex.makeExecPlan(ctx, planner)
		defer planner.curPlan.close(ctx)
	}

	// Include gist in error reports.
	planGist := planner.instrumentation.planGist.String()
	ctx = withPlanGist(ctx, planGist)
	if ppInfo := getPausablePortalInfo(); ppInfo == nil || !ppInfo.dispatchToExecutionEngine.cleanup.isComplete {
		// If we're not using pausable portals, or it's the first execution of
		// the pausable portal, and we're not collecting a bundle yet, check
		// whether we should get a bundle for this particular plan gist.
		if ih := &planner.instrumentation; !ih.collectBundle && ih.outputMode == unmodifiedOutput {
			ctx = ih.setupWithPlanGist(ctx, ex.server.cfg, stmt.StmtNoConstants, planGist, &planner.curPlan)
		}
	}

	if planner.extendedEvalCtx.TxnImplicit {
		planner.curPlan.flags.Set(planFlagImplicitTxn)
	}

	// Certain statements want their results to go to the client
	// directly. Configure this here.
	if ex.executorType != executorTypeInternal && (planner.curPlan.avoidBuffering || ex.sessionData().AvoidBuffering) {
		res.DisableBuffering()
	}

	var stmtFingerprintID appstatspb.StmtFingerprintID
	var stats topLevelQueryStats
	defer func() {
		var bulkJobId uint64
		if ppInfo := getPausablePortalInfo(); ppInfo != nil && !ppInfo.dispatchToExecutionEngine.cleanup.isComplete {
			ppInfo.dispatchToExecutionEngine.cleanup.appendFunc(namedFunc{
				fName: "log statement",
				f: func() {
					if planner.extendedEvalCtx.Annotations == nil {
						// This is a safety check in case resetPlanner() was
						// executed, but then we never set the annotations on
						// the planner. Formatting the stmt for logging requires
						// non-nil annotations.
						planner.extendedEvalCtx.Annotations = &planner.semaCtx.Annotations
					}
					planner.maybeLogStatement(
						ctx,
						ex.executorType,
						int(ex.state.mu.autoRetryCounter),
						int(ex.extraTxnState.txnCounter.Load()),
						ppInfo.dispatchToExecutionEngine.rowsAffected,
						ex.state.mu.stmtCount,
						bulkJobId,
						ppInfo.curRes.ErrAllowReleased(),
						ex.statsCollector.PhaseTimes().GetSessionPhaseTime(sessionphase.SessionQueryReceived),
						&ex.extraTxnState.hasAdminRoleCache,
						ex.server.TelemetryLoggingMetrics,
						ppInfo.dispatchToExecutionEngine.stmtFingerprintID,
						ppInfo.dispatchToExecutionEngine.queryStats,
						ex.statsCollector,
						ex.extraTxnState.shouldLogToTelemetry)
				},
			})
		} else {
			// Note that for bulk job query (IMPORT, BACKUP and RESTORE), we don't
			// use this numRows entry. We emit the number of changed rows when the job
			// completes. (see the usages of logutil.LogJobCompletion()).
			nonBulkJobNumRows := res.RowsAffected()
			switch planner.stmt.AST.(type) {
			case *tree.Import, *tree.Restore, *tree.Backup:
				bulkJobId = res.GetBulkJobId()
			}
			planner.maybeLogStatement(
				ctx,
				ex.executorType,
				int(ex.state.mu.autoRetryCounter),
				int(ex.extraTxnState.txnCounter.Load()),
				nonBulkJobNumRows,
				ex.state.mu.stmtCount,
				bulkJobId,
				res.Err(),
				ex.statsCollector.PhaseTimes().GetSessionPhaseTime(sessionphase.SessionQueryReceived),
				&ex.extraTxnState.hasAdminRoleCache,
				ex.server.TelemetryLoggingMetrics,
				stmtFingerprintID,
				&stats,
				ex.statsCollector,
				ex.extraTxnState.shouldLogToTelemetry)
		}
	}()

	// TODO(sql-sessions): fix the phase time for pausable portals.
	// https://github.com/cockroachdb/cockroach/issues/99410
	ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.PlannerEndLogicalPlan, timeutil.Now())
	ex.sessionTracing.TracePlanEnd(ctx, err)

	// Finally, process the planning error from above.
	if err != nil {
		err = addPlanningErrorHints(ctx, err, &stmt, ex.server.cfg.Settings, planner)
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

	distSQLMode := ex.sessionData().DistSQLMode
	if planner.pausablePortal != nil {
		if len(planner.curPlan.subqueryPlans) == 0 &&
			len(planner.curPlan.cascades) == 0 &&
			len(planner.curPlan.checkPlans) == 0 {
			// We only allow non-distributed plan for pausable portals.
			distSQLMode = sessiondatapb.DistSQLOff
		} else {
			telemetry.Inc(sqltelemetry.SubOrPostQueryStmtsTriedWithPausablePortals)
			// We don't allow sub / post queries for pausable portal. Set it back to an
			// un-pausable (normal) portal.
			// With pauseInfo is nil, no cleanup function will be added to the stack
			// and all clean-up steps will be performed as for normal portals.
			// TODO(#115887): We may need to move resetting pauseInfo before we add
			// the pausable portal cleanup step above.
			planner.pausablePortal.pauseInfo = nil
			// We need this so that the result consumption for this portal cannot be
			// paused either.
			if err := res.RevokePortalPausability(); err != nil {
				res.SetError(err)
				return nil
			}
		}
	}
	distributePlan := getPlanDistribution(
		ctx, planner.Descriptors().HasUncommittedTypes(),
		distSQLMode, planner.curPlan.main,
	)
	ex.sessionTracing.TracePlanCheckEnd(ctx, nil, distributePlan.WillDistribute())

	if ex.server.cfg.TestingKnobs.BeforeExecute != nil {
		ex.server.cfg.TestingKnobs.BeforeExecute(ctx, stmt.String(), planner.Descriptors())
	}

	// TODO(sql-sessions): fix the phase time for pausable portals.
	// https://github.com/cockroachdb/cockroach/issues/99410
	ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.PlannerStartExecStmt, timeutil.Now())

	progAtomic, err := func() (*uint64, error) {
		ex.mu.Lock()
		defer ex.mu.Unlock()
		queryMeta, ok := ex.mu.ActiveQueries[stmt.QueryID]
		if !ok {
			return nil, errors.AssertionFailedf("query %d not in registry", stmt.QueryID)
		}
		queryMeta.planGist = planner.instrumentation.planGist.String()
		queryMeta.phase = executing
		queryMeta.database = planner.CurrentDatabase()
		// TODO(yuzefovich): introduce ternary PlanDistribution into queryMeta.
		queryMeta.isDistributed = distributePlan.WillDistribute()
		progAtomic := &queryMeta.progressAtomic
		flags := planner.curPlan.flags
		queryMeta.isFullScan = flags.IsSet(planFlagContainsFullIndexScan) || flags.IsSet(planFlagContainsFullTableScan)
		return progAtomic, nil
	}()
	if err != nil {
		panic(err)
	}

	switch distributePlan {
	case physicalplan.FullyDistributedPlan:
		planner.curPlan.flags.Set(planFlagFullyDistributed)
	case physicalplan.PartiallyDistributedPlan:
		planner.curPlan.flags.Set(planFlagPartiallyDistributed)
	default:
		planner.curPlan.flags.Set(planFlagNotDistributed)
	}

	ex.sessionTracing.TraceRetryInformation(ctx, int(ex.state.mu.autoRetryCounter), ex.state.mu.autoRetryReason)
	if ex.server.cfg.TestingKnobs.OnTxnRetry != nil && ex.state.mu.autoRetryReason != nil {
		ex.server.cfg.TestingKnobs.OnTxnRetry(ex.state.mu.autoRetryReason, planner.EvalContext())
	}
	distribute := DistributionType(LocalDistribution)
	if distributePlan.WillDistribute() {
		distribute = FullDistribution
	}
	ex.sessionTracing.TraceExecStart(ctx, "distributed")
	stats, err = ex.execWithDistSQLEngine(
		ctx, planner, stmt.AST.StatementReturnType(), res, distribute, progAtomic,
	)
	if ppInfo := getPausablePortalInfo(); ppInfo != nil {
		// For pausable portals, we log the stats when closing the portal, so we need
		// to aggregate the stats for all executions.
		ppInfo.dispatchToExecutionEngine.queryStats.add(&stats)
	}

	if res.Err() == nil {
		isSetOrShow := stmt.AST.StatementTag() == "SET" || stmt.AST.StatementTag() == "SHOW"
		if ex.sessionData().InjectRetryErrorsEnabled && !isSetOrShow &&
			planner.Txn().Sender().TxnStatus() == roachpb.PENDING {
			if ex.state.injectedTxnRetryCounter < numTxnRetryErrors {
				retryErr := planner.Txn().GenerateForcedRetryableErr(
					ctx, "injected by `inject_retry_errors_enabled` session variable")
				res.SetError(retryErr)
				ex.state.injectedTxnRetryCounter++
			}
		}
	}
	ex.sessionTracing.TraceExecEnd(ctx, res.Err(), res.RowsAffected())
	// TODO(sql-sessions): fix the phase time for pausable portals.
	// https://github.com/cockroachdb/cockroach/issues/99410
	ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.PlannerEndExecStmt, timeutil.Now())

	ex.extraTxnState.rowsRead += stats.rowsRead
	ex.extraTxnState.bytesRead += stats.bytesRead
	ex.extraTxnState.rowsWritten += stats.rowsWritten

	if ppInfo := getPausablePortalInfo(); ppInfo != nil && !ppInfo.dispatchToExecutionEngine.cleanup.isComplete {
		// We need to ensure that we're using the planner bound to the first-time
		// execution of a portal.
		curPlanner := *planner
		ppInfo.dispatchToExecutionEngine.cleanup.appendFunc(namedFunc{
			fName: "populate query level stats and regions",
			f: func() {
				populateQueryLevelStats(ctx, &curPlanner, ex.server.cfg, ppInfo.dispatchToExecutionEngine.queryStats, &ex.cpuStatsCollector)
				ppInfo.dispatchToExecutionEngine.stmtFingerprintID = ex.recordStatementSummary(
					ctx, &curPlanner,
					int(ex.state.mu.autoRetryCounter), ppInfo.dispatchToExecutionEngine.rowsAffected, ppInfo.curRes.ErrAllowReleased(), *ppInfo.dispatchToExecutionEngine.queryStats,
				)
			},
		})
	} else {
		populateQueryLevelStats(ctx, planner, ex.server.cfg, &stats, &ex.cpuStatsCollector)
		stmtFingerprintID = ex.recordStatementSummary(
			ctx, planner,
			int(ex.state.mu.autoRetryCounter), res.RowsAffected(), res.Err(), stats,
		)
	}

	if ex.server.cfg.TestingKnobs.AfterExecute != nil {
		ex.server.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), ex.executorType == executorTypeInternal, res.Err())
	}

	if limitsErr := ex.handleTxnRowsWrittenReadLimits(ctx); limitsErr != nil && res.Err() == nil {
		res.SetError(limitsErr)
	}
	if res.Err() == nil && err == nil {
		autoRetryReason := ex.state.mu.autoRetryReason
		if execinfra.IsDynamicQueryHasNoHomeRegionError(autoRetryReason) {
			if homeRegion, ok := planner.EvalContext().Locality.Find("region"); ok &&
				planner.StmtNoConstantsWithHomeRegionEnforced == planner.stmt.StmtNoConstants {
				// If this is the same query as ran when the dynamic "query has no home
				// region" error occurred, but this time it didn't error out, report
				// back the query's home region.
				err = pgerror.Newf(pgcode.QueryNotRunningInHomeRegion,
					`%s. Try running the query from region '%s'. %s`,
					execinfra.QueryNotRunningInHomeRegionMessagePrefix,
					homeRegion,
					sqlerrors.EnforceHomeRegionFurtherInfo,
				)
				res.SetError(err)
				// We won't be faking the gateway region any more. Restore the original
				// locality.
				planner.EvalContext().Locality = planner.EvalContext().OriginalLocality
				return nil
			}
			// If for some reason we're not running the same query as before, report
			// the original "query has no home region" error in non-retryable form.
			err = execinfra.MaybeGetNonRetryableDynamicQueryHasNoHomeRegionError(autoRetryReason)
			res.SetError(err)
			// We won't be faking the gateway region any more. Restore the original
			// locality.
			planner.EvalContext().Locality = planner.EvalContext().OriginalLocality
			return nil
		}
	}

	return err
}

// populateQueryLevelStats collects query-level execution statistics
// and populates it in the instrumentationHelper's queryLevelStatsWithErr field.
// Query-level execution statistics are collected using the statement's trace
// and the plan's flow metadata.
func populateQueryLevelStats(
	ctx context.Context,
	p *planner,
	cfg *ExecutorConfig,
	topLevelStats *topLevelQueryStats,
	cpuStats *multitenantcpu.CPUUsageHelper,
) {
	ih := &p.instrumentation
	if _, ok := ih.Tracing(); !ok {
		return
	}
	// Get the query-level stats.
	var flowsMetadata []*execstats.FlowsMetadata
	for _, flowInfo := range p.curPlan.distSQLFlowInfos {
		flowsMetadata = append(flowsMetadata, flowInfo.flowsMetadata)
	}
	trace := ih.sp.GetRecording(tracingpb.RecordingStructured)
	var err error
	queryLevelStats, err := execstats.GetQueryLevelStats(
		trace, cfg.TestingKnobs.DeterministicExplain, flowsMetadata)
	queryLevelStatsWithErr := execstats.MakeQueryLevelStatsWithErr(queryLevelStats, err)
	ih.queryLevelStatsWithErr = &queryLevelStatsWithErr
	if err != nil {
		const msg = "error getting query level stats for statement: %s: %+v"
		if buildutil.CrdbTestBuild {
			panic(fmt.Sprintf(msg, ih.fingerprint, err))
		}
		log.VInfof(ctx, 1, msg, ih.fingerprint, err)
	} else {
		// If this query is being run by a tenant, record the RUs consumed by CPU
		// usage and network egress to the client.
		if execinfra.IncludeRUEstimateInExplainAnalyze.Get(cfg.SV()) && cfg.DistSQLSrv != nil {
			if costController := cfg.DistSQLSrv.TenantCostController; costController != nil {
				if costCfg := costController.GetCostConfig(); costCfg != nil {
					networkEgressRUEstimate := costCfg.PGWireEgressCost(topLevelStats.networkEgressEstimate)
					ih.queryLevelStatsWithErr.Stats.RUEstimate += float64(networkEgressRUEstimate)
					ih.queryLevelStatsWithErr.Stats.RUEstimate += cpuStats.EndCollection(ctx)
				}
			}
		}
		ih.queryLevelStatsWithErr.Stats.ClientTime = topLevelStats.clientTime
	}
	if ih.traceMetadata != nil && ih.explainPlan != nil {
		ih.traceMetadata.annotateExplain(
			ctx,
			ih.explainPlan,
			trace,
			cfg.TestingKnobs.DeterministicExplain,
			p,
		)
	}
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
		SessionID: ex.planner.extendedEvalCtx.SessionID.String(),
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
		var event logpb.EventPayload
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

var txnSchemaChangeErr = pgerror.Newf(
	pgcode.FeatureNotSupported,
	"to use multi-statement transactions involving a schema change under weak isolation levels, enable the autocommit_before_ddl setting",
)

// maybeUpgradeToSerializable checks if the statement is a schema change, and
// upgrades the transaction to serializable isolation if it is. If the
// transaction contains multiple statements, and an upgrade was attempted, an
// error is returned.
func (ex *connExecutor) maybeUpgradeToSerializable(ctx context.Context, stmt Statement) error {
	p := &ex.planner
	if tree.CanModifySchema(stmt.AST) {
		if ex.state.mu.txn.IsoLevel().ToleratesWriteSkew() {
			if !ex.extraTxnState.firstStmtExecuted {
				if err := ex.state.setIsolationLevel(isolation.Serializable); err != nil {
					return err
				}
				ex.extraTxnState.upgradedToSerializable = true
				p.BufferClientNotice(ctx, pgnotice.Newf("setting transaction isolation level to SERIALIZABLE due to schema change"))
			} else {
				return txnSchemaChangeErr
			}
		}
	}
	return nil
}

// makeExecPlan creates an execution plan and populates planner.curPlan using
// the cost-based optimizer. This is used to create the plan when executing a
// query in the "simple" pgwire protocol.
func (ex *connExecutor) makeExecPlan(ctx context.Context, planner *planner) error {
	if err := ex.maybeUpgradeToSerializable(ctx, planner.stmt); err != nil {
		return err
	}

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

	// Set index recommendations, so it can be saved on statement statistics.
	planner.instrumentation.SetIndexRecommendations(
		ctx, ex.server.idxRecommendationsCache, planner, ex.executorType == executorTypeInternal,
	)

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
	// networkEgressEstimate is an estimate for the number of bytes sent to the
	// client. It is used for estimating the number of RUs consumed by a query.
	networkEgressEstimate int64
	// clientTime is the amount of time query execution was blocked on the
	// client receiving the PGWire protocol messages (as well as construcing
	// those messages).
	clientTime time.Duration
}

func (s *topLevelQueryStats) add(other *topLevelQueryStats) {
	s.bytesRead += other.bytesRead
	s.rowsRead += other.rowsRead
	s.rowsWritten += other.rowsWritten
	s.networkEgressEstimate += other.networkEgressEstimate
	s.clientTime += other.clientTime
}

// execWithDistSQLEngine converts a plan to a distributed SQL physical plan and
// runs it.
// If an error is returned, the connection needs to stop processing queries.
// Query execution errors are written to res; they are not returned.
// NB: the plan (in planner.curPlan) is not closed, so it is the caller's
// responsibility to do so.
func (ex *connExecutor) execWithDistSQLEngine(
	ctx context.Context,
	planner *planner,
	stmtType tree.StatementReturnType,
	res RestrictedCommandResult,
	distribute DistributionType,
	progressAtomic *uint64,
) (topLevelQueryStats, error) {
	defer planner.curPlan.savePlanInfo()
	recv := MakeDistSQLReceiver(
		ctx, res, stmtType,
		ex.server.cfg.RangeDescriptorCache,
		planner.txn,
		ex.server.cfg.Clock,
		&ex.sessionTracing,
	)
	recv.measureClientTime = planner.instrumentation.ShouldCollectExecStats()
	recv.progressAtomic = progressAtomic
	if ex.server.cfg.TestingKnobs.DistSQLReceiverPushCallbackFactory != nil {
		recv.testingKnobs.pushCallback = ex.server.cfg.TestingKnobs.DistSQLReceiverPushCallbackFactory(ctx, planner.stmt.SQL)
	}
	defer recv.Release()

	var err error

	if planner.hasFlowForPausablePortal() {
		err = planner.resumeFlowForPausablePortal(recv)
	} else {
		evalCtx := planner.ExtendedEvalContext()
		planCtx := ex.server.cfg.DistSQLPlanner.NewPlanningCtx(ctx, evalCtx, planner, planner.txn, distribute)
		planCtx.setUpForMainQuery(ctx, planner, recv)

		var evalCtxFactory func(usedConcurrently bool) *extendedEvalContext
		if len(planner.curPlan.subqueryPlans) != 0 ||
			len(planner.curPlan.cascades) != 0 ||
			len(planner.curPlan.checkPlans) != 0 {
			var serialEvalCtx extendedEvalContext
			ex.initEvalCtx(ctx, &serialEvalCtx, planner)
			evalCtxFactory = func(usedConcurrently bool) *extendedEvalContext {
				// Reuse the same object if this factory is not used concurrently.
				factoryEvalCtx := &serialEvalCtx
				if usedConcurrently {
					factoryEvalCtx = &extendedEvalContext{}
					ex.initEvalCtx(ctx, factoryEvalCtx, planner)
				}
				ex.resetEvalCtx(factoryEvalCtx, planner.txn, planner.ExtendedEvalContext().StmtTimestamp)
				factoryEvalCtx.Placeholders = &planner.semaCtx.Placeholders
				factoryEvalCtx.Annotations = &planner.semaCtx.Annotations
				factoryEvalCtx.SessionID = planner.ExtendedEvalContext().SessionID
				return factoryEvalCtx
			}
		}
		err = ex.server.cfg.DistSQLPlanner.PlanAndRunAll(ctx, evalCtx, planCtx, planner, recv, evalCtxFactory)
	}
	return recv.stats, err
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
	p := &ex.planner
	ex.resetPlanner(ctx, p, nil, now)
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
	ctx context.Context, parserStmt statements.Statement[tree.Statement], res RestrictedCommandResult,
) (_ fsm.Event, payload fsm.EventPayload) {
	shouldLogToExecAndAudit := true
	defer func() {
		if !shouldLogToExecAndAudit {
			// We don't want to log this statement, since another layer of the
			// conn_executor will handle the logging for this statement.
			return
		}

		p := &ex.planner
		stmt := makeStatement(parserStmt, ex.server.cfg.GenerateID(),
			tree.FmtFlags(queryFormattingForFingerprintsMask.Get(&ex.server.cfg.Settings.SV)))
		p.stmt = stmt
		p.semaCtx.Annotations = tree.MakeAnnotations(stmt.NumAnnotations)
		p.extendedEvalCtx.Annotations = &p.semaCtx.Annotations
		p.extendedEvalCtx.Placeholders = &tree.PlaceholderInfo{}
		p.curPlan.init(&p.stmt, &p.instrumentation)
		var execErr error
		if p, ok := payload.(payloadWithError); ok {
			execErr = p.errorCause()
		}

		stmtFingerprintID := appstatspb.ConstructStatementFingerprintID(
			stmt.StmtNoConstants,
			ex.implicitTxn(),
			p.CurrentDatabase(),
		)

		p.maybeLogStatement(
			ctx,
			ex.executorType,
			int(ex.state.mu.autoRetryCounter),
			int(ex.extraTxnState.txnCounter.Load()),
			0, /* rowsAffected */
			0, /* stmtCount */
			0, /* bulkJobId */
			execErr,
			ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionQueryReceived),
			&ex.extraTxnState.hasAdminRoleCache,
			ex.server.TelemetryLoggingMetrics,
			stmtFingerprintID,
			&topLevelQueryStats{},
			ex.statsCollector,
			ex.extraTxnState.shouldLogToTelemetry)
	}()

	// We're in the NoTxn state, so no statements were executed earlier. Bump the
	// txn counter for logging.
	ex.extraTxnState.txnCounter.Add(1)
	ast := parserStmt.AST
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
				ex.QualityOfService(),
				ex.txnIsolationLevelToKV(ctx, s.Modes.Isolation),
				ex.omitInRangefeeds(),
			)
	case *tree.ShowCommitTimestamp:
		return ex.execShowCommitTimestampInNoTxnState(ctx, s, res)
	case *tree.CommitTransaction, *tree.ReleaseSavepoint,
		*tree.RollbackTransaction, *tree.SetTransaction, *tree.Savepoint:
		if ex.sessionData().AutoCommitBeforeDDL {
			// If autocommit_before_ddl is set, we allow these statements to be
			// executed, and send a warning rather than an error.
			if err := ex.planner.SendClientNotice(
				ctx,
				pgerror.WithSeverity(errNoTransactionInProgress, "WARNING"),
			); err != nil {
				return ex.makeErrEvent(err, ast)
			}
			return nil, nil
		}
		return ex.makeErrEvent(errNoTransactionInProgress, ast)
	default:
		// NB: Implicit transactions are created with the session's default
		// historical timestamp even though the statement itself might contain
		// an AOST clause. In these cases the clause is evaluated and applied
		// execStmtInOpenState.
		shouldLogToExecAndAudit = false
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
				ex.QualityOfService(),
				ex.txnIsolationLevelToKV(ctx, tree.UnspecifiedIsolation),
				ex.omitInRangefeeds(),
			)
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
	ctx context.Context, ast tree.Statement, qos sessiondatapb.QoSLevel,
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
			qos,
			ex.txnIsolationLevelToKV(ctx, tree.UnspecifiedIsolation),
			ex.omitInRangefeeds(),
		)
}

// execStmtInAbortedState executes a statement in a txn that's in state
// Aborted or RestartWait. All statements result in error events except:
//   - COMMIT / ROLLBACK: aborts the current transaction.
//   - ROLLBACK TO SAVEPOINT / SAVEPOINT: reopens the current transaction,
//     allowing it to be retried.
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
	switch s := ast.(type) {
	case *tree.ReleaseSavepoint:
		if ex.extraTxnState.shouldAcceptReleaseCockroachRestartInCommitWait &&
			s.Savepoint == commitOnReleaseSavepointName {
			ex.extraTxnState.shouldAcceptReleaseCockroachRestartInCommitWait = false
			return nil, nil
		}
	case *tree.ShowCommitTimestamp:
		return ex.execShowCommitTimestampInCommitWaitState(ctx, s, res)
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
	}
	return eventNonRetriableErr{IsCommit: fsm.False},
		eventNonRetriableErrPayload{
			err: sqlerrors.NewTransactionCommittedError(),
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
		!isNoTxn, &ex.extraTxnState.prepStmtsNamespace, ex.sessionData(),
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
	res.SetColumns(ctx, colinfo.ShowCompletionsColumns)
	log.Warningf(ctx, "COMPLETION GENERATOR FOR: %+v", *n)
	sd := ex.planner.SessionData()
	override := sessiondata.InternalExecutorOverride{
		SearchPath: &sd.SearchPath,
		Database:   sd.Database,
		User:       sd.User(),
	}
	// If a txn is currently open, reuse it. If not,
	// we will read in a fresh txn.
	//
	// TODO(janexing): better bind the internal executor with the txn.
	var txn *kv.Txn
	var ie isql.Executor
	if _, ok := ex.machine.CurState().(stateOpen); ok {
		ie = ex.planner.InternalSQLTxn()
		txn = ex.planner.Txn()
	} else {
		ie = ex.server.cfg.InternalDB.Executor()
	}
	queryIterFn := func(ctx context.Context, opName string, stmt string, args ...interface{}) (eval.InternalRows, error) {
		return ie.QueryIteratorEx(ctx, opName, txn,
			override,
			stmt, args...)
	}

	completions, err := newCompletionsGenerator(queryIterFn, n)
	if err != nil {
		log.Warningf(ctx, "COMPLETION GENERATOR FAILED: %v", err)
		return err
	}

	var hasNext bool
	for hasNext, err = completions.Next(ctx); hasNext; hasNext, err = completions.Next(ctx) {
		row := completions.Values()
		err = res.AddRow(ctx, row)
		if err != nil {
			log.Warningf(ctx, "COMPLETION ADDROW FAILED: %v", err)
			return err
		}
	}
	if err != nil {
		log.Warningf(ctx, "COMPLETION GENERATOR NEXT FAILED: %v", err)
	}
	return err
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
	recordingType := tracingpb.RecordingVerbose
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
			recordingType = tracingpb.RecordingVerbose
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
	stmt statements.Statement[tree.Statement],
	placeholders *tree.PlaceholderInfo,
	queryID clusterunique.ID,
	cancelQuery context.CancelFunc,
) {
	_, hidden := stmt.AST.(tree.HiddenFromShowQueries)
	qm := &queryMeta{
		txnID:         ex.state.mu.txn.ID(),
		start:         ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionQueryReceived),
		stmt:          stmt,
		placeholders:  placeholders,
		phase:         preparing,
		isDistributed: false,
		isFullScan:    false,
		cancelQuery:   cancelQuery,
		hidden:        hidden,
	}
	ex.mu.Lock()
	defer ex.mu.Unlock()
	ex.mu.ActiveQueries[queryID] = qm
}

func (ex *connExecutor) removeActiveQuery(queryID clusterunique.ID, ast tree.Statement) {
	ex.mu.Lock()
	defer ex.mu.Unlock()
	_, ok := ex.mu.ActiveQueries[queryID]
	if !ok {
		panic(errors.AssertionFailedf("query %d missing from ActiveQueries", queryID))
	}
	delete(ex.mu.ActiveQueries, queryID)
	ex.mu.LastActiveQuery = ast
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

func (ex *connExecutor) onTxnFinish(ctx context.Context, ev txnEvent, txnErr error) {
	if ex.extraTxnState.shouldExecuteOnTxnFinish {
		ex.extraTxnState.shouldExecuteOnTxnFinish = false
		txnStart := ex.extraTxnState.txnFinishClosure.txnStartTime
		implicit := ex.extraTxnState.txnFinishClosure.implicit
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionEndExecTransaction, timeutil.Now())
		transactionFingerprintID :=
			appstatspb.TransactionFingerprintID(ex.extraTxnState.transactionStatementsHash.Sum())

		err := ex.txnFingerprintIDCache.Add(ctx, transactionFingerprintID)
		if err != nil {
			if log.V(1) {
				log.Warningf(ctx, "failed to enqueue transactionFingerprintID = %d: %s", transactionFingerprintID, err)
			}
		}

		ex.statsCollector.EndTransaction(
			ctx,
			transactionFingerprintID,
		)

		if ex.server.cfg.TestingKnobs.BeforeTxnStatsRecorded != nil {
			ex.server.cfg.TestingKnobs.BeforeTxnStatsRecorded(
				ex.sessionData(),
				ev.txnID,
				transactionFingerprintID,
				txnErr,
			)
		}

		err = ex.recordTransactionFinish(ctx, transactionFingerprintID, ev, implicit, txnStart, txnErr)
		if err != nil {
			if log.V(1) {
				log.Warningf(ctx, "failed to record transaction stats: %s", err)
			}
			ex.server.ServerMetrics.StatsMetrics.DiscardedStatsCount.Inc(1)
		}

		// If we have a commitTimestamp, we should use it.
		ex.previousTransactionCommitTimestamp.Forward(ev.commitTimestamp)
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
		ex.extraTxnState.idleLatency = 0
		ex.extraTxnState.rowsRead = 0
		ex.extraTxnState.bytesRead = 0
		ex.extraTxnState.rowsWritten = 0

		if ex.server.cfg.TestingKnobs.BeforeRestart != nil {
			ex.server.cfg.TestingKnobs.BeforeRestart(ctx, ex.state.mu.autoRetryReason)
		}

	}
}

// recordTransactionStart records the start of the transaction.
func (ex *connExecutor) recordTransactionStart(txnID uuid.UUID) {
	// Transaction fingerprint ID will be available once transaction finishes
	// execution.
	ex.txnIDCacheWriter.Record(contentionpb.ResolvedTxnID{
		TxnID:            txnID,
		TxnFingerprintID: appstatspb.InvalidTransactionFingerprintID,
	})

	ex.state.mu.RLock()
	txnStart := ex.state.mu.txnStart
	ex.state.mu.RUnlock()

	ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionTransactionStarted, txnStart)
	ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionFirstStartExecTransaction, timeutil.Now())
	ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionMostRecentStartExecTransaction,
		ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionFirstStartExecTransaction))
	ex.extraTxnState.transactionStatementsHash = util.MakeFNV64()
	ex.extraTxnState.transactionStatementFingerprintIDs = nil
	ex.extraTxnState.numRows = 0
	ex.extraTxnState.shouldCollectTxnExecutionStats = false
	ex.extraTxnState.accumulatedStats = execstats.QueryLevelStats{}
	ex.extraTxnState.idleLatency = 0
	ex.extraTxnState.rowsRead = 0
	ex.extraTxnState.bytesRead = 0
	ex.extraTxnState.rowsWritten = 0
	ex.extraTxnState.rowsWrittenLogged = false
	ex.extraTxnState.rowsReadLogged = false
	if txnExecStatsSampleRate := collectTxnStatsSampleRate.Get(&ex.server.GetExecutorConfig().Settings.SV); txnExecStatsSampleRate > 0 {
		ex.extraTxnState.shouldCollectTxnExecutionStats = txnExecStatsSampleRate > ex.rng.Float64()
	}

	// Note ex.metrics is Server.Metrics for the connExecutor that serves the
	// client connection, and is Server.InternalMetrics for internal executors.
	ex.metrics.EngineMetrics.SQLTxnsOpen.Inc(1)

	ex.extraTxnState.shouldExecuteOnTxnFinish = true
	ex.extraTxnState.txnFinishClosure.txnStartTime = txnStart
	ex.extraTxnState.txnFinishClosure.implicit = ex.implicitTxn()
	ex.extraTxnState.shouldExecuteOnTxnRestart = true

	// Determine telemetry logging.
	isTracing := ex.planner.ExtendedEvalContext().Tracing.Enabled()
	ex.extraTxnState.shouldLogToTelemetry, ex.extraTxnState.telemetrySkippedTxns =
		ex.server.TelemetryLoggingMetrics.shouldEmitTransactionLog(isTracing,
			ex.executorType == executorTypeInternal,
			ex.applicationName.Load().(string))

	ex.statsCollector.StartTransaction()

	ex.previousTransactionCommitTimestamp = hlc.Timestamp{}
}

func (ex *connExecutor) recordTransactionFinish(
	ctx context.Context,
	transactionFingerprintID appstatspb.TransactionFingerprintID,
	ev txnEvent,
	implicit bool,
	txnStart time.Time,
	txnErr error,
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
	ex.totalActiveTimeStopWatch.Stop()

	if ex.server.cfg.TestingKnobs.OnRecordTxnFinish != nil {
		ex.server.cfg.TestingKnobs.OnRecordTxnFinish(
			ex.executorType == executorTypeInternal, ex.phaseTimes, ex.planner.stmt.SQL,
		)
	}

	// Note ex.metrics is Server.Metrics for the connExecutor that serves the
	// client connection, and is Server.InternalMetrics for internal executors.
	if contentionDuration := ex.extraTxnState.accumulatedStats.ContentionTime.Nanoseconds(); contentionDuration > 0 {
		ex.metrics.EngineMetrics.SQLContendedTxns.Inc(1)
	}
	ex.metrics.EngineMetrics.SQLTxnsOpen.Dec(1)
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
		SessionID:               ex.planner.extendedEvalCtx.SessionID,
		TransactionID:           ev.txnID,
		TransactionTimeSec:      txnTime.Seconds(),
		StartTime:               txnStart,
		EndTime:                 txnEnd,
		Committed:               ev.eventType == txnCommit,
		ImplicitTxn:             implicit,
		RetryCount:              int64(ex.state.mu.autoRetryCounter),
		AutoRetryReason:         ex.state.mu.autoRetryReason,
		StatementFingerprintIDs: ex.extraTxnState.transactionStatementFingerprintIDs,
		ServiceLatency:          txnServiceLat,
		RetryLatency:            txnRetryLat,
		CommitLatency:           commitLat,
		IdleLatency:             ex.extraTxnState.idleLatency,
		RowsAffected:            ex.extraTxnState.numRows,
		CollectedExecStats:      ex.planner.instrumentation.collectExecStats,
		ExecStats:               ex.extraTxnState.accumulatedStats,
		RowsRead:                ex.extraTxnState.rowsRead,
		RowsWritten:             ex.extraTxnState.rowsWritten,
		BytesRead:               ex.extraTxnState.bytesRead,
		Priority:                ex.state.mu.priority,
		// TODO(107318): add isolation level
		// TODO(107318): add qos
		// TODO(107318): add asoftime or ishistorical
		// TODO(107318): add readonly
		SessionData: ex.sessionData(),
		TxnErr:      txnErr,
	}

	ex.maybeRecordRetrySerializableContention(ev.txnID, transactionFingerprintID, txnErr)

	if ex.extraTxnState.shouldLogToTelemetry {
		ex.planner.logTransaction(ctx,
			int(ex.extraTxnState.txnCounter.Load()),
			transactionFingerprintID,
			&recordedTxnStats,
			ex.extraTxnState.telemetrySkippedTxns,
		)
	}

	return ex.statsCollector.RecordTransaction(
		ctx,
		transactionFingerprintID,
		recordedTxnStats,
	)
}

// Records a SERIALIZATION_CONFLICT contention event to the contention registry event
// store if we have a known conflicting txn meta for a serialization conflict error.
func (ex *connExecutor) maybeRecordRetrySerializableContention(
	txnID uuid.UUID, txnFingerprintID appstatspb.TransactionFingerprintID, txnErr error,
) {
	if !contention.EnableSerializationConflictEvents.Get(&ex.server.cfg.Settings.SV) {
		return
	}

	var retryErr *kvpb.TransactionRetryWithProtoRefreshError
	if txnErr != nil && errors.As(txnErr, &retryErr) && retryErr.ConflictingTxn != nil {
		contentionEvent := contentionpb.ExtendedContentionEvent{
			ContentionType: contentionpb.ContentionType_SERIALIZATION_CONFLICT,
			BlockingEvent: kvpb.ContentionEvent{
				Key:     retryErr.ConflictingTxn.Key,
				TxnMeta: *retryErr.ConflictingTxn,
				// Duration is not relevant for SERIALIZATION conflicts.
			},
			WaitingTxnID:            txnID,
			WaitingTxnFingerprintID: txnFingerprintID,
			// Waiting statement fields are not relevant at this stage.
		}
		ex.server.cfg.ContentionRegistry.AddContentionEvent(contentionEvent)
	}
}

// logTraceAboveThreshold logs a span's recording. It is used when txn or stmt threshold tracing is enabled.
// This function assumes that sp is non-nil and threshold tracing was enabled.
// The caller is responsible for only calling the function when elapsed >= threshold.
func logTraceAboveThreshold(
	ctx context.Context,
	r tracingpb.Recording,
	opName redact.RedactableString,
	detail redact.RedactableString,
	threshold, elapsed time.Duration,
) {
	if r == nil {
		log.Warning(ctx, "missing trace when threshold tracing was enabled")
	}
	log.SqlExec.Infof(ctx, "%s took %s, exceeding threshold of %s:\n%s\n%s", opName, elapsed, threshold, detail, r)
}

func (ex *connExecutor) execWithProfiling(
	ctx context.Context,
	ast tree.Statement,
	prepared *PreparedStatement,
	op func(context.Context) error,
) error {
	var err error
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
			err = op(ctx)
		})
	} else {
		err = op(ctx)
	}
	return err
}
