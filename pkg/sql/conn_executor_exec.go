// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/multitenant/multitenantcpu"
	"github.com/cockroachdb/cockroach/pkg/obs/workloadid"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catsessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/contention"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/hints"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/prep"
	"github.com/cockroachdb/cockroach/pkg/sql/regions"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/ctxlog"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	// This import is needed here to properly inject tree.ValidateJSONPath from
	// pkg/util/jsonpath/parser/parse.go.
	_ "github.com/cockroachdb/cockroach/pkg/util/jsonpath/parser"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/pprofutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"github.com/lib/pq/oid"
	"go.opentelemetry.io/otel/attribute"
)

// numTxnRetryErrors is the number of times an error will be injected if
// the transaction is retried using SAVEPOINTs.
const numTxnRetryErrors = 3

// metamorphicForceExecWithPausablePortal is used to force
// execStmtInOpenStateWithPausablePortal to be used instead of
// execStmtInOpenState, even for non-pausable portals. This ensures that the
// behavior of the former does not diverge from the latter.
var metamorphicForceExecWithPausablePortal = metamorphic.ConstantWithTestBool(
	"conn-executor-force-exec-with-pausable-portal",
	false,
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
	ex.mu.TransactionTimeout.Stop()

	// Run observer statements in a separate code path; their execution does not
	// depend on the current transaction state.
	if _, ok := ast.(tree.ObserverStatement); ok {
		ex.statsCollector.Reset(ex.applicationStats, ex.phaseTimes)
		err := ex.runObserverStatement(ctx, ast, res)
		ex.extraTxnState.idleLatency += ex.phaseTimes.GetIdleLatency(ex.statsCollector.PreviousPhaseTimes())
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionQueryServiced, crtime.NowMono())
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
		var preparedStmt *prep.Statement
		if portal != nil {
			preparedStmt = portal.Stmt
		}
		usePausableCodePath := portal.isPausable()
		if buildutil.CrdbTestBuild && metamorphicForceExecWithPausablePortal {
			usePausableCodePath = true
		}
		if usePausableCodePath {
			err = ex.execWithProfiling(ctx, ast, preparedStmt, func(ctx context.Context) error {
				ev, payload, err = ex.execStmtInOpenStateWithPausablePortal(
					ctx, parserStmt, portal, pinfo, res, canAutoCommit,
				)
				return err
			})
		} else {
			err = ex.execWithProfiling(ctx, ast, preparedStmt, func(ctx context.Context) error {
				ev, payload, err = ex.execStmtInOpenState(ctx, parserStmt, preparedStmt, pinfo, res, canAutoCommit)
				return err
			})
		}
		switch p := payload.(type) {
		case eventNonRetryableErrPayload:
			ex.recordFailure(p)
		}
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionQueryServiced, crtime.NowMono())

	case stateAborted:
		ev, payload = ex.execStmtInAbortedState(ctx, ast, res)

	case stateCommitWait:
		ev, payload = ex.execStmtInCommitWaitState(ctx, ast, res)

	default:
		panic(errors.AssertionFailedf("unexpected txn state: %#v", ex.machine.CurState()))
	}

	ex.startIdleInSessionTimeout()

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

	txnTimeoutRemaining :=
		ex.sessionData().TransactionTimeout - ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionTransactionStarted).Elapsed()
	if txnTimeoutRemaining > 0 {
		startTransactionTimeout := func() {
			switch ast.(type) {
			case *tree.CommitTransaction, *tree.RollbackTransaction:
				// Do nothing, the transaction is completed, we do not want to start
				// an idle timer.
			default:
				// The transaction_timeout setting should move the transaction to the
				// aborted state.
				// NOTE: In Postgres, the transaction_timeout causes the entire session
				// to be terminated. We intentionally diverge from that behavior.
				ex.mu.TransactionTimeout = timeout{time.AfterFunc(
					txnTimeoutRemaining,
					func() {
						// An error is only returned if stmtBuf was closed already, so
						// there's nothing else to do in that case.
						_ = ex.stmtBuf.Push(ctx, SendError{Err: sqlerrors.TxnTimeoutError})
					},
				)}
			}
		}
		switch ex.machine.CurState().(type) {
		case stateOpen:
			// Only start timeout if the statement is executed in an
			// explicit transaction.
			if !ex.implicitTxn() {
				startTransactionTimeout()
			}
		}
	}

	return ev, payload, err
}

// startIdleInSessionTimeout will start the timer for the idle in session timeout.
func (ex *connExecutor) startIdleInSessionTimeout() {
	if ex.sessionData().IdleInSessionTimeout > 0 {
		// Cancel the session if the idle time exceeds the idle in session timeout.
		ex.mu.IdleInSessionTimeout = timeout{time.AfterFunc(
			ex.sessionData().IdleInSessionTimeout,
			ex.CancelSession,
		)}
	}
}

func (ex *connExecutor) recordFailure(p eventNonRetryableErrPayload) {
	ex.metrics.EngineMetrics.FailureCount.Inc(1, ex.sessionData().Database, ex.sessionData().ApplicationName)
	switch {
	case errors.Is(p.errorCause(), sqlerrors.QueryTimeoutError):
		ex.metrics.EngineMetrics.StatementTimeoutCount.Inc(1)
	case errors.Is(p.errorCause(), sqlerrors.TxnTimeoutError):
		ex.metrics.EngineMetrics.TransactionTimeoutCount.Inc(1)
	}
}

// execPortal executes a prepared statement. It is a "wrapper" around execStmt
// method that is performing additional work to track portal's state.
func (ex *connExecutor) execPortal(
	ctx context.Context,
	portal PreparedPortal,
	stmtRes CommandResult,
	pinfo *tree.PlaceholderInfo,
	canAutoCommit bool,
) (ev fsm.Event, payload fsm.EventPayload, retErr error) {
	defer func() {
		if portal.isPausable() {
			if !portal.pauseInfo.exhaustPortal.cleanup.isComplete {
				portal.pauseInfo.exhaustPortal.cleanup.appendFunc(func(context.Context) {
					ex.exhaustPortal(portal.Name)
				})
				portal.pauseInfo.exhaustPortal.cleanup.isComplete = true
			}
			// If we encountered an error when executing a pausable portal, clean up
			// the retained resources.
			if retErr != nil {
				portal.pauseInfo.cleanupAll(ctx)
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
		if _, ok := ex.extraTxnState.prepStmtsNamespace.portals[portal.Name]; ok && !portal.isPausable() {
			defer ex.exhaustPortal(portal.Name)
		}
		return ev, payload, retErr

	default:
		return ex.execStmt(ctx, portal.Stmt.Statement, &portal, pinfo, stmtRes, canAutoCommit)
	}
}

func (ex *connExecutor) checkUDTStaleness(
	ctx context.Context, prep *prep.Statement,
) (stale bool, staleEnumNames []string, err error) {
	if len(prep.UDTs) == 0 {
		return false, nil, nil
	}
	staleEnumNames = make([]string, 0)
	for i, typ := range prep.UDTs {
		toCheck, err := ex.planner.ResolveTypeByOID(ctx, typ.Oid())
		if err != nil {
			return stale, nil, err
		}
		if typ.TypeMeta.Version != toCheck.TypeMeta.Version {
			stale = true
			prep.UDTs[i] = toCheck
			staleEnumNames = append(staleEnumNames, typ.Name())
		}
	}
	return stale, staleEnumNames, nil
}

// maybeReparsePrepStmt is to reparse the prepared statement so that the
// stored udt datum is up-to-date. The reparse is needed because AST can
// be modified at type chekcing, with tree.StrVal component replaced
// with a versioned tree.DEnum. If the UDT's version changed, this
// tree.DEnum would be outdated.
func (ex *connExecutor) maybeReparsePrepStmt(
	ctx context.Context, prep *prep.Statement, name string,
) error {
	stale, staleEnumNames, err := ex.checkUDTStaleness(ctx, prep)
	if err != nil {
		return err
	}
	if !stale {
		return nil
	}
	log.Eventf(ctx, "reparsing prepared statament %q for the user defined types are stale: %s", name, staleEnumNames)
	newStmt, err := parser.ParseOne(prep.SQL)
	if err != nil {
		return err
	}
	prep.Statement = newStmt
	prep.AST = newStmt.AST
	// Since the udt is outdated, the stored memo will be stale and we will need to
	// regenerate the memo anyways, so we just reset all the memo related fields as well.
	prep.BaseMemo = nil
	prep.GenericMemo = nil
	prep.IdealGenericPlan = false
	prep.Costs.Reset()
	prep.HintsGeneration = 0
	return nil
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
	prepared *prep.Statement,
	pinfo *tree.PlaceholderInfo,
	res RestrictedCommandResult,
	canAutoCommit bool,
) (retEv fsm.Event, retPayload fsm.EventPayload, retErr error) {
	ast := parserStmt.AST
	var sp *tracing.Span
	ctx, sp = tracing.ChildSpan(ctx, "sql query")
	// TODO(andrei): Consider adding the placeholders as tags too.
	sp.SetTag("statement", attribute.StringValue(parserStmt.SQL))
	defer sp.Finish()

	makeErrEvent := func(err error) (fsm.Event, fsm.EventPayload, error) {
		ev, payload := ex.makeErrEvent(err, ast)
		return ev, payload, nil
	}

	queryID := ex.server.cfg.GenerateID()

	// Update the deadline on the transaction based on the collections.
	err := ex.extraTxnState.descCollection.MaybeUpdateDeadline(ctx, ex.state.mu.txn)
	if err != nil {
		return makeErrEvent(err)
	}
	os := ex.machine.CurState().(stateOpen)

	isExtendedProtocol := prepared != nil
	stmtFingerprintFmtMask := tree.FmtHideConstants | tree.FmtFlags(tree.QueryFormattingForFingerprintsMask.Get(&ex.server.cfg.Settings.SV))
	var statementHintsCache *hints.StatementHintsCache
	if ex.executorType != executorTypeInternal {
		statementHintsCache = ex.server.cfg.StatementHintsCache
	}

	var stmt Statement
	if isExtendedProtocol {
		stmt = makeStatementFromPrepared(ctx, prepared, queryID, stmtFingerprintFmtMask, statementHintsCache)
	} else {
		stmt = makeStatement(ctx, parserStmt, queryID, stmtFingerprintFmtMask, statementHintsCache)
	}

	if len(stmt.QueryTags) > 0 {
		tags := logtags.BuildBuffer()
		for _, tag := range stmt.QueryTags {
			tags.Add("querytag-"+tag.Key, tag.Value)
		}
		ctx = logtags.AddTags(ctx, tags.Finish())
	}

	var queryTimeoutTicker *time.Timer
	var txnTimeoutTicker *time.Timer
	queryTimedOut := false
	txnTimedOut := false
	// queryDoneAfterFunc and txnDoneAfterFunc will be allocated only when
	// queryTimeoutTicker or txnTimeoutTicker is non-nil.
	var queryDoneAfterFunc chan struct{}
	var txnDoneAfterFunc chan struct{}

	var cancelQuery context.CancelFunc
	ctx, cancelQuery = ctxlog.WithCancel(ctx)
	ex.incrementStartedStmtCounter(ast)
	ex.state.mu.Lock()
	ex.state.mu.stmtCount++
	ex.state.mu.Unlock()
	ex.addActiveQuery(parserStmt, pinfo, queryID, cancelQuery)
	defer func() {
		if retErr == nil && !payloadHasError(retPayload) {
			ex.incrementExecutedStmtCounter(ast)
		}
	}()

	// Make sure that we always unregister the query.
	defer func() {
		ex.removeActiveQuery(queryID, ast)
		cancelQuery()

		// Note ex.metrics is Server.Metrics for the connExecutor that serves the
		// client connection, and is Server.InternalMetrics for internal executors.
		ex.metrics.EngineMetrics.SQLActiveStatements.Dec(1,
			ex.sessionData().Database, ex.sessionData().ApplicationName)
	}()

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
	ex.metrics.EngineMetrics.SQLActiveStatements.Inc(1,
		ex.sessionData().Database, ex.sessionData().ApplicationName)

	p := &ex.planner
	stmtTS := ex.server.cfg.Clock.PhysicalTime()
	ex.statsCollector.Reset(ex.applicationStats, ex.phaseTimes)
	ex.resetPlanner(ctx, p, ex.state.mu.txn, stmtTS)
	p.sessionDataMutatorIterator.ParamStatusUpdater = res
	p.noticeSender = res
	ih := &p.instrumentation

	if ex.executorType != executorTypeInternal {
		// NB: ex.metrics includes internal executor transactions when executorType
		// is executorTypeInternal, so that's why we exclude internal executors
		// in the conditional.
		curOpen := ex.metrics.EngineMetrics.SQLTxnsOpen.Value()
		if maxOpen := maxOpenTransactions.Get(&ex.server.cfg.Settings.SV); maxOpen > 0 {
			if curOpen > maxOpen {
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

		// Enforce license policies. Throttling can occur if there is no valid
		// license or if the existing one has expired.
		if isSQLOkayToThrottle(ast) {
			if notice, err := ex.server.cfg.LicenseEnforcer.MaybeFailIfThrottled(ctx, curOpen); err != nil {
				return makeErrEvent(err)
			} else if notice != nil {
				p.BufferClientNotice(ctx, notice)
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
		if e2, ok := stmt.ASTWithInjectedHints.(*tree.ExplainAnalyze); ok {
			stmt.ASTWithInjectedHints = e2.Statement
		}
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
		ps, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts.Get(name)
		if !ok {
			return makeErrEvent(newPreparedStmtDNEError(ex.sessionData(), name))
		}

		if err := ex.maybeReparsePrepStmt(ctx, ps, name); err != nil {
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
		stmt.StmtSummary = ps.StatementSummary
		stmt.Hints = ps.Hints
		stmt.HintIDs = ps.HintIDs
		stmt.HintsGeneration = ps.HintsGeneration
		stmt.ASTWithInjectedHints = ps.ASTWithInjectedHints
		stmt.ReloadHintsIfStale(ctx, stmtFingerprintFmtMask, statementHintsCache)
		// Don't reset the statement type if we're within EXPLAIN ANALYZE, as this
		// would break the special case handling in GetFormatCode that relies on
		// cmdCompleteTag being "EXPLAIN". For EXPLAIN ANALYZE EXECUTE, the format
		// codes are set up for the EXPLAIN output (1 column), but the inner query
		// may have more columns. See issue #161382.
		if ih.outputMode == unmodifiedOutput {
			res.ResetStmtType(ps.AST)
		}

		if e.DiscardRows {
			ih.SetDiscardRows()
		}
		ast = stmt.Statement.AST
	}

	if len(stmt.Hints) > 0 {
		telemetry.Inc(sqltelemetry.StatementHintsCounter)
		ex.metrics.EngineMetrics.QueryWithStatementHintsCount.Inc(1)
	}

	// This goroutine is the only one that can modify txnState.mu.priority and
	// txnState.mu.autoRetryCounter, so we don't need to get a mutex here.
	ctx = ih.Setup(ctx, ex, p, &stmt, os.ImplicitTxn.Get(),
		ex.state.mu.priority, ex.state.mu.autoRetryCounter)

	// Note that here we always unconditionally defer a function that takes care
	// of finishing the instrumentation helper. This is needed since in order to
	// support plan-gist-matching of the statement diagnostics we might not know
	// right now whether Finish needs to happen.
	defer func() {
		if ih.needFinish {
			retErr = ih.Finish(
				ex,
				ih.collectExecStats,
				p,
				ast,
				stmt.SQL,
				res,
				retPayload,
				retErr,
			)
		}
	}()

	if ex.executorType != executorTypeInternal && ex.sessionData().TransactionTimeout > 0 && !ex.implicitTxn() {
		timerDuration :=
			ex.sessionData().TransactionTimeout - ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionTransactionStarted).Elapsed()

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
			ex.sessionData().StmtTimeout - ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionQueryReceived).Elapsed()
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
				// Also cancel the transactions context, so that there is no danger
				// getting stuck rolling back.
				ex.state.txnCancelFn()
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

	if buildutil.CrdbTestBuild {
		// Ensure that each statement is formatted regardless of logging
		// settings.
		_ = p.FormatAstAsRedactableString(stmt.AST, p.extendedEvalCtx.Annotations)
	}

	var logErr error
	defer func() {
		// Do not log if this is an eventTxnCommittedDueToDDL event. In that case,
		// the transaction is committed, and the current statement is executed
		// again.
		if _, ok := retEv.(eventTxnCommittedDueToDDL); ok {
			return
		}

		if p.curPlan.stmt == nil || p.curPlan.instrumentation == nil {
			// We short-circuited before we could initialize some fields that
			// are needed for logging, so do that here.
			p.curPlan.init(&p.stmt, &p.instrumentation)
		}
		if logErr == nil {
			if p, ok := retPayload.(payloadWithError); ok {
				logErr = p.errorCause()
			}
		}

		var bulkJobId uint64
		var rowsAffected int
		switch p.stmt.AST.(type) {
		case *tree.Import, *tree.Restore, *tree.Backup:
			bulkJobId = res.GetBulkJobId()
		}
		// Note that for bulk job query (IMPORT, BACKUP and RESTORE), we don't
		// use this numRows entry. We emit the number of changed rows when the job
		// completes. (see the usages of logutil.LogJobCompletion()).
		rowsAffected = res.RowsAffected()

		p.maybeLogStatement(
			ctx,
			ex.executorType,
			int(ex.state.mu.autoRetryCounter)+p.autoRetryStmtCounter,
			int(ex.extraTxnState.txnCounter.Load()),
			rowsAffected,
			ex.state.mu.stmtCount,
			bulkJobId,
			logErr,
			ex.statsCollector.PhaseTimes().GetSessionPhaseTime(sessionphase.SessionQueryReceived),
			&ex.extraTxnState.hasAdminRoleCache,
			ex.server.TelemetryLoggingMetrics,
			ex.implicitTxn(),
			ex.statsCollector,
			ex.extraTxnState.shouldLogToTelemetry)
	}()

	// Overwrite res.Error to a more user-friendly message in case of query
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

		logErr = res.Err()
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
			retEv = eventNonRetryableErr{
				IsCommit: fsm.FromBool(isCommit(ast)),
			}
			errToPush := cancelchecker.QueryCanceledError
			res.SetError(errToPush)
			retPayload = eventNonRetryableErrPayload{err: errToPush}
			logErr = errToPush
			// Cancel the txn if we are inside an implicit txn too.
			if ex.implicitTxn() && ex.state.txnCancelFn != nil {
				ex.state.txnCancelFn()
			}
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
			retEv = eventNonRetryableErr{
				IsCommit: fsm.FromBool(isCommit(ast)),
			}
			res.SetError(sqlerrors.QueryTimeoutError)
			retPayload = eventNonRetryableErrPayload{err: sqlerrors.QueryTimeoutError}
			logErr = sqlerrors.QueryTimeoutError
		} else if txnTimedOut {
			retEv = eventNonRetryableErr{
				IsCommit: fsm.FromBool(isCommit(ast)),
			}
			res.SetError(sqlerrors.TxnTimeoutError)
			retPayload = eventNonRetryableErrPayload{err: sqlerrors.TxnTimeoutError}
			logErr = sqlerrors.TxnTimeoutError
		}

	}(ctx, res)

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

	case *tree.PrepareTransaction:
		ev, payload := ex.execPrepareTransactionInOpenState(ctx, s)
		return ev, payload, nil

	case *tree.ShowCommitTimestamp:
		ev, payload := ex.execShowCommitTimestampInOpenState(ctx, s, res, canAutoCommit)
		return ev, payload, nil

	case *tree.Prepare:
		// This is handling the SQL statement "PREPARE". See execPrepare for
		// handling of the protocol-level command for preparing statements.
		name := s.Name.String()
		if ex.extraTxnState.prepStmtsNamespace.prepStmts.Has(name) {
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
			ctx,
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
			tree.FmtFlags(tree.QueryFormattingForFingerprintsMask.Get(&ex.server.cfg.Settings.SV)),
			statementHintsCache,
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
			ctx, name, prepStmt, typeHints, rawTypeHints, prep.StatementOriginSQL,
		); err != nil {
			return makeErrEvent(err)
		}
		return nil, nil, nil
	}

	// Check if we need to auto-commit the transaction due to DDL.
	if ev, payload := ex.maybeAutoCommitBeforeDDL(ctx, ast); ev != nil {
		return ev, payload, nil
	}

	// For regular statements (the ones that get to this point), we
	// don't return any event unless an error happens, or a CALL statement
	// performs a nested transaction COMMIT or ROLLBACK.

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
	// that all uses of SQL execution initialize the kv.Txn using a
	// single/common function. That would be where the stepping mode
	// gets enabled once for all SQL statements executed "underneath".
	prevSteppingMode := ex.state.mu.txn.ConfigureStepping(ctx, kv.SteppingEnabled)
	prevSeqNum := ex.state.mu.txn.GetReadSeqNum()
	delegatedUnderOuterTxn := ex.executorType == executorTypeInternal && ex.extraTxnState.underOuterTxn
	var origTs hlc.Timestamp
	defer func() {
		_ = ex.state.mu.txn.ConfigureStepping(ctx, prevSteppingMode)

		// If this is an internal executor that is running on behalf of an outer
		// txn, then we need to step back the txn so that the outer executor uses
		// the proper sequence number.
		if delegatedUnderOuterTxn {
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
		if delegatedUnderOuterTxn {
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
	if err := ex.state.mu.txn.Step(ctx, !delegatedUnderOuterTxn /* allowReadTimestampStep */); err != nil {
		return makeErrEvent(err)
	}
	if buildutil.CrdbTestBuild && delegatedUnderOuterTxn {
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
	stmtTraceThreshold := TraceStmtThreshold.Get(&ex.planner.execCfg.Settings.SV)
	var stmtCtx context.Context
	// TODO(andrei): I think we should do this even if alreadyRecording == true.
	if !alreadyRecording && stmtTraceThreshold > 0 {
		stmtCtx, stmtThresholdSpan = tracing.EnsureChildSpan(ctx, ex.server.cfg.AmbientCtx.Tracer, "trace-stmt-threshold", tracing.WithRecording(tracingpb.RecordingVerbose))
	} else {
		stmtCtx = ctx
	}

	if ex.state.mu.autoRetryReason != nil {
		p.autoRetryCounter = int(ex.state.mu.autoRetryCounter)
		ex.sessionTracing.TraceRetryInformation(
			ctx, "transaction", int(ex.state.mu.autoRetryCounter), ex.state.mu.autoRetryReason,
		)
		if ex.server.cfg.TestingKnobs.OnTxnRetry != nil {
			ex.server.cfg.TestingKnobs.OnTxnRetry(ex.state.mu.autoRetryReason, p.EvalContext())
		}
	}

	if ex.executorType != executorTypeInternal &&
		ex.state.mu.txn.IsoLevel() == isolation.ReadCommitted &&
		!ex.implicitTxn() {
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
		stmtDur := ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionQueryReceived).Elapsed()
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
				false,              /* outputJaegerJSON */
			)
		} else {
			stmtThresholdSpan.Finish()
		}
	}

	if err = res.Err(); err != nil {
		return makeErrEvent(err)
	}

	txn := ex.state.mu.txn

	if !os.ImplicitTxn.Get() && txn.IsSerializablePushAndRefreshNotPossible() {
		rc, canAutoRetry := ex.getRewindTxnCapability()
		if canAutoRetry {
			ev := eventRetryableErr{
				IsCommit:     fsm.FromBool(isCommit(ast)),
				CanAutoRetry: fsm.FromBool(canAutoRetry),
			}
			payload := eventRetryableErrPayload{
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

// execStmtInOpenStateWithPausablePortal is similar to execStmtInOpenState, but
// handles the special case of a pausable portal.
func (ex *connExecutor) execStmtInOpenStateWithPausablePortal(
	ctx context.Context,
	parserStmt statements.Statement[tree.Statement],
	portal *PreparedPortal,
	pinfo *tree.PlaceholderInfo,
	res RestrictedCommandResult,
	canAutoCommit bool,
) (retEv fsm.Event, retPayload fsm.EventPayload, retErr error) {
	type localVars struct {
		logErr      error
		cancelQuery context.CancelFunc
		ast         tree.Statement
		stmt        Statement
	}

	// vars contains local variables that are heap allocated, usually because
	// they are referenced by closures. Grouping them into a single struct
	// requires only a single heap allocation.
	var vars localVars

	// updateRetErrAndPayload ensures that the latest event payload and error is
	// always recorded by portal.pauseInfo.
	// TODO(janexing): add test for this.
	updateRetErrAndPayload := func(err error, payload fsm.EventPayload) {
		retPayload = payload
		retErr = err
		if portal.isPausable() {
			portal.pauseInfo.execStmtInOpenState.retPayload = payload
			portal.pauseInfo.execStmtInOpenState.retErr = err
		}
	}
	// For pausable portals, we delay the clean-up until closing the portal by
	// adding the function to the execStmtInOpenStateCleanup.
	// Otherwise, perform the clean-up step within every execution.
	processCleanupFunc := func(f func()) {
		if !portal.isPausable() {
			f()
		} else if !portal.pauseInfo.execStmtInOpenState.cleanup.isComplete {
			portal.pauseInfo.execStmtInOpenState.cleanup.appendFunc(func(context.Context) {
				f()
				// Some cleanup steps modify the retErr and retPayload. We need to
				// ensure that cleanup after them can see the update.
				updateRetErrAndPayload(retErr, retPayload)
			})
		}
	}
	defer func() {
		// This is the first defer, so it will always be called after any cleanup
		// func being added to the stack from the defers below.
		if portal.isPausable() && !portal.pauseInfo.execStmtInOpenState.cleanup.isComplete {
			portal.pauseInfo.execStmtInOpenState.cleanup.isComplete = true
		}
		// If there's any error, do the cleanup right here.
		if (retErr != nil || payloadHasError(retPayload)) && portal.isPausable() {
			updateRetErrAndPayload(retErr, retPayload)
			portal.pauseInfo.resumableFlow.cleanup.run(ctx)
			portal.pauseInfo.dispatchToExecutionEngine.cleanup.run(ctx)
			portal.pauseInfo.execStmtInOpenState.cleanup.run(ctx)
		}
	}()

	// We need this part so that when we check if we need to increment the count
	// of executed stmt, we are checking the latest error and payload. Otherwise,
	// we would be checking the ones evaluated at the portal's first-time
	// execution.
	defer func() {
		if portal.isPausable() {
			updateRetErrAndPayload(retErr, retPayload)
		}
	}()

	vars.ast = parserStmt.AST
	var sp *tracing.Span
	if !portal.isPausable() || !portal.pauseInfo.execStmtInOpenState.cleanup.isComplete {
		ctx, sp = tracing.ChildSpan(ctx, "sql query")
		// TODO(andrei): Consider adding the placeholders as tags too.
		sp.SetTag("statement", attribute.StringValue(parserStmt.SQL))
		if portal.isPausable() {
			portal.pauseInfo.execStmtInOpenState.spCtx = ctx
		}
		defer func() {
			processCleanupFunc(sp.Finish)
		}()
	} else {
		ctx = portal.pauseInfo.execStmtInOpenState.spCtx
	}

	makeErrEvent := func(err error) (fsm.Event, fsm.EventPayload, error) {
		ev, payload := ex.makeErrEvent(err, vars.ast)
		return ev, payload, nil
	}

	var queryID clusterunique.ID
	if portal.isPausable() {
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
	stmtFingerprintFmtMask := tree.FmtHideConstants | tree.FmtFlags(tree.QueryFormattingForFingerprintsMask.Get(&ex.server.cfg.Settings.SV))
	var statementHintsCache *hints.StatementHintsCache
	if ex.executorType != executorTypeInternal {
		statementHintsCache = ex.server.cfg.StatementHintsCache
	}

	if isExtendedProtocol {
		vars.stmt = makeStatementFromPrepared(ctx, portal.Stmt, queryID, stmtFingerprintFmtMask, statementHintsCache)
	} else {
		vars.stmt = makeStatement(ctx, parserStmt, queryID, stmtFingerprintFmtMask, statementHintsCache)
	}

	var queryTimeoutTicker *time.Timer
	var txnTimeoutTicker *time.Timer
	queryTimedOut := false
	txnTimedOut := false
	// queryDoneAfterFunc and txnDoneAfterFunc will be allocated only when
	// queryTimeoutTicker or txnTimeoutTicker is non-nil.
	var queryDoneAfterFunc chan struct{}
	var txnDoneAfterFunc chan struct{}

	// For pausable portal, the active query needs to be set up only when
	// the portal is executed for the first time.
	if !portal.isPausable() || !portal.pauseInfo.execStmtInOpenState.cleanup.isComplete {
		ctx, vars.cancelQuery = ctxlog.WithCancel(ctx)
		ex.incrementStartedStmtCounter(vars.ast)
		ex.state.mu.Lock()
		ex.state.mu.stmtCount++
		ex.state.mu.Unlock()
		ex.addActiveQuery(parserStmt, pinfo, queryID, vars.cancelQuery)

		if portal.isPausable() {
			portal.pauseInfo.execStmtInOpenState.cancelQueryFunc = vars.cancelQuery
			portal.pauseInfo.execStmtInOpenState.cancelQueryCtx = ctx
		}
		defer func() {
			processCleanupFunc(
				func() {
					// We need to check the latest errors rather than the ones evaluated
					// when this function is created.
					if portal.isPausable() {
						retErr = portal.pauseInfo.execStmtInOpenState.retErr
						retPayload = portal.pauseInfo.execStmtInOpenState.retPayload
					}
					if retErr == nil && !payloadHasError(retPayload) {
						ex.incrementExecutedStmtCounter(vars.ast)
					}
				},
			)
		}()
	} else {
		ctx = portal.pauseInfo.execStmtInOpenState.cancelQueryCtx
		vars.cancelQuery = portal.pauseInfo.execStmtInOpenState.cancelQueryFunc
	}

	// Make sure that we always unregister the query.
	defer func() {
		processCleanupFunc(func() {
			ex.removeActiveQuery(queryID, vars.ast)
			vars.cancelQuery()
		})

		// Note ex.metrics is Server.Metrics for the connExecutor that serves the
		// client connection, and is Server.InternalMetrics for internal executors.
		ex.metrics.EngineMetrics.SQLActiveStatements.Dec(1,
			ex.sessionData().Database, ex.sessionData().ApplicationName)
	}()

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
	ex.metrics.EngineMetrics.SQLActiveStatements.Inc(1,
		ex.sessionData().Database, ex.sessionData().ApplicationName)

	// TODO(sql-sessions): persist the planner for a pausable portal, and reuse
	// it for each re-execution.
	// https://github.com/cockroachdb/cockroach/issues/99625
	p := &ex.planner
	stmtTS := ex.server.cfg.Clock.PhysicalTime()
	ex.statsCollector.Reset(ex.applicationStats, ex.phaseTimes)
	ex.resetPlanner(ctx, p, ex.state.mu.txn, stmtTS)
	p.sessionDataMutatorIterator.ParamStatusUpdater = res
	p.noticeSender = res
	ih := &p.instrumentation

	if ex.executorType != executorTypeInternal {
		// NB: ex.metrics includes internal executor transactions when executorType
		// is executorTypeInternal, so that's why we exclude internal executors
		// in the conditional.
		curOpen := ex.metrics.EngineMetrics.SQLTxnsOpen.Value()
		if maxOpen := maxOpenTransactions.Get(&ex.server.cfg.Settings.SV); maxOpen > 0 {
			if curOpen > maxOpen {
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

		// Enforce license policies. Throttling can occur if there is no valid
		// license or if the existing one has expired.
		if isSQLOkayToThrottle(vars.ast) {
			if notice, err := ex.server.cfg.LicenseEnforcer.MaybeFailIfThrottled(ctx, curOpen); err != nil {
				return makeErrEvent(err)
			} else if notice != nil {
				p.BufferClientNotice(ctx, notice)
			}
		}
	}

	// Special top-level handling for EXPLAIN ANALYZE.
	if e, ok := vars.ast.(*tree.ExplainAnalyze); ok {
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
		vars.stmt.AST = e.Statement
		vars.ast = e.Statement
		if e2, ok := vars.stmt.ASTWithInjectedHints.(*tree.ExplainAnalyze); ok {
			vars.stmt.ASTWithInjectedHints = e2.Statement
		}
		// TODO(radu): should we trim the "EXPLAIN ANALYZE (DEBUG)" part from
		// stmt.SQL?

		// Clear any ExpectedTypes we set if we prepared this statement (they
		// reflect the column types of the EXPLAIN itself and not those of the inner
		// statement).
		vars.stmt.ExpectedTypes = nil
	}

	// Special top-level handling for EXECUTE. This must happen after the handling
	// for EXPLAIN ANALYZE (in order to support EXPLAIN ANALYZE EXECUTE) but
	// before setting up the instrumentation helper.
	if e, ok := vars.ast.(*tree.Execute); ok {
		// Replace the `EXECUTE foo` statement with the prepared statement, and
		// continue execution.
		name := e.Name.String()
		ps, ok := ex.extraTxnState.prepStmtsNamespace.prepStmts.Get(name)
		if !ok {
			return makeErrEvent(newPreparedStmtDNEError(ex.sessionData(), name))
		}

		if err := ex.maybeReparsePrepStmt(ctx, ps, name); err != nil {
			return makeErrEvent(err)
		}

		var err error
		pinfo, err = ex.planner.fillInPlaceholders(ctx, ps, name, e.Params)
		if err != nil {
			return makeErrEvent(err)
		}

		// TODO(radu): what about .SQL, .NumAnnotations, .NumPlaceholders?
		vars.stmt.Statement = ps.Statement
		vars.stmt.Prepared = ps
		vars.stmt.ExpectedTypes = ps.Columns
		vars.stmt.StmtNoConstants = ps.StatementNoConstants
		vars.stmt.StmtSummary = ps.StatementSummary
		vars.stmt.Hints = ps.Hints
		vars.stmt.HintIDs = ps.HintIDs
		vars.stmt.HintsGeneration = ps.HintsGeneration
		vars.stmt.ASTWithInjectedHints = ps.ASTWithInjectedHints
		vars.stmt.ReloadHintsIfStale(ctx, stmtFingerprintFmtMask, statementHintsCache)
		// Don't reset the statement type if we're within EXPLAIN ANALYZE, as this
		// would break the special case handling in GetFormatCode that relies on
		// cmdCompleteTag being "EXPLAIN". For EXPLAIN ANALYZE EXECUTE, the format
		// codes are set up for the EXPLAIN output (1 column), but the inner query
		// may have more columns. See issue #161382.
		if ih.outputMode == unmodifiedOutput {
			res.ResetStmtType(ps.AST)
		}

		if e.DiscardRows {
			ih.SetDiscardRows()
		}
		vars.ast = vars.stmt.Statement.AST
	}

	if len(vars.stmt.Hints) > 0 {
		telemetry.Inc(sqltelemetry.StatementHintsCounter)
		ex.metrics.EngineMetrics.QueryWithStatementHintsCount.Inc(1)
	}

	// For pausable portal, the instrumentation helper needs to be set up only
	// when the portal is executed for the first time.
	//
	// This goroutine is the only one that can modify txnState.mu.priority and
	// txnState.mu.autoRetryCounter, so we don't need to get a mutex here.
	if !portal.isPausable() || portal.pauseInfo.execStmtInOpenState.ihWrapper == nil {
		ctx = ih.Setup(ctx, ex, p, &vars.stmt, os.ImplicitTxn.Get(),
			ex.state.mu.priority, ex.state.mu.autoRetryCounter)
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
	if portal.isPausable() {
		if portal.pauseInfo.execStmtInOpenState.ihWrapper == nil {
			portal.pauseInfo.execStmtInOpenState.ihWrapper = &instrumentationHelperWrapper{
				ctx: ctx,
				// TODO(yuzefovich): we're capturing the instrumentationHelper
				// by value here, meaning that modifications that happen later
				// (notably in makeExecPlan) aren't captured. For example,
				// explainPlan field will remain unset. However, so far we've
				// only observed this impact EXPLAIN ANALYZE which doesn't run
				// through the pausable portal path.
				ih: *ih,
			}
		} else {
			p.instrumentation = portal.pauseInfo.execStmtInOpenState.ihWrapper.ih
		}
	}

	// Note that here we always unconditionally defer a function that takes care
	// of finishing the instrumentation helper. This is needed since in order to
	// support plan-gist-matching of the statement diagnostics we might not know
	// right now whether Finish needs to happen.
	defer processCleanupFunc(func() {
		// We need this weird thing because we need to make sure we're
		// closing the correct instrumentation helper for the paused portal.
		ihToFinish := ih
		curRes := res
		if portal.isPausable() {
			ihToFinish = &portal.pauseInfo.execStmtInOpenState.ihWrapper.ih
			curRes = portal.pauseInfo.curRes
			retErr = portal.pauseInfo.execStmtInOpenState.retErr
			retPayload = portal.pauseInfo.execStmtInOpenState.retPayload
		}
		if ihToFinish.needFinish {
			retErr = ihToFinish.Finish(
				ex,
				ihToFinish.collectExecStats,
				p,
				vars.ast,
				vars.stmt.SQL,
				curRes,
				retPayload,
				retErr,
			)
		}
	})

	if ex.executorType != executorTypeInternal && ex.sessionData().TransactionTimeout > 0 && !ex.implicitTxn() {
		timerDuration :=
			ex.sessionData().TransactionTimeout - ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionTransactionStarted).Elapsed()

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
					vars.cancelQuery()
					txnTimedOut = true
					txnDoneAfterFunc <- struct{}{}
				})
		}
	}

	// We exempt `SET` statements from the statement timeout, particularly so as
	// not to block the `SET statement_timeout` command itself.
	if ex.sessionData().StmtTimeout > 0 && vars.ast.StatementTag() != "SET" {
		timerDuration :=
			ex.sessionData().StmtTimeout - ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionQueryReceived).Elapsed()
		// There's no need to proceed with execution if the timer has already expired.
		if timerDuration < 0 {
			queryTimedOut = true
			return makeErrEvent(sqlerrors.QueryTimeoutError)
		}
		queryDoneAfterFunc = make(chan struct{}, 1)
		queryTimeoutTicker = time.AfterFunc(
			timerDuration,
			func() {
				vars.cancelQuery()
				// Also cancel the transactions context, so that there is no danger
				// getting stuck rolling back.
				ex.state.txnCancelFn()
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
			filter(ctx, ex.sessionData(), vars.stmt.AST.String(), execErr)
		}

		// Do the auto-commit, if necessary. In the extended protocol, the
		// auto-commit happens when the Sync message is handled.
		if retEv != nil || retErr != nil {
			return
		}
		// As portals are from extended protocol, we don't auto commit for them.
		if canAutoCommit && !isExtendedProtocol {
			retEv, retPayload = ex.handleAutoCommit(ctx, vars.ast)
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

	p.stmt = vars.stmt
	p.semaCtx.Annotations = tree.MakeAnnotations(vars.stmt.NumAnnotations)
	p.extendedEvalCtx.Annotations = &p.semaCtx.Annotations
	p.semaCtx.Placeholders.Assign(pinfo, vars.stmt.NumPlaceholders)
	p.extendedEvalCtx.Placeholders = &p.semaCtx.Placeholders

	if buildutil.CrdbTestBuild {
		// Ensure that each statement is formatted regardless of logging
		// settings.
		_ = p.FormatAstAsRedactableString(vars.stmt.AST, p.extendedEvalCtx.Annotations)
	}

	defer processCleanupFunc(func() {
		// Do not log if this is an eventTxnCommittedDueToDDL event. In that case,
		// the transaction is committed, and the current statement is executed
		// again.
		if _, ok := retEv.(eventTxnCommittedDueToDDL); ok {
			return
		}

		if p.curPlan.stmt == nil || p.curPlan.instrumentation == nil {
			// We short-circuited before we could initialize some fields that
			// are needed for logging, so do that here.
			p.curPlan.init(&p.stmt, &p.instrumentation)
		}
		if vars.logErr == nil {
			if p, ok := retPayload.(payloadWithError); ok {
				vars.logErr = p.errorCause()
			}
		}

		var bulkJobId uint64
		var rowsAffected int
		if portal.isPausable() {
			ppInfo := portal.pauseInfo
			if p.extendedEvalCtx.Annotations == nil {
				// This is a safety check in case resetPlanner() was
				// executed, but then we never set the annotations on
				// the planner. Formatting the stmt for logging requires
				// non-nil annotations.
				p.extendedEvalCtx.Annotations = &p.semaCtx.Annotations
			}
			rowsAffected = ppInfo.dispatchToExecutionEngine.rowsAffected
		} else {
			switch p.stmt.AST.(type) {
			case *tree.Import, *tree.Restore, *tree.Backup:
				bulkJobId = res.GetBulkJobId()
			}
			// Note that for bulk job query (IMPORT, BACKUP and RESTORE), we don't
			// use this numRows entry. We emit the number of changed rows when the job
			// completes. (see the usages of logutil.LogJobCompletion()).
			rowsAffected = res.RowsAffected()
		}

		p.maybeLogStatement(
			ctx,
			ex.executorType,
			int(ex.state.mu.autoRetryCounter)+p.autoRetryStmtCounter,
			int(ex.extraTxnState.txnCounter.Load()),
			rowsAffected,
			ex.state.mu.stmtCount,
			bulkJobId,
			vars.logErr,
			ex.statsCollector.PhaseTimes().GetSessionPhaseTime(sessionphase.SessionQueryReceived),
			&ex.extraTxnState.hasAdminRoleCache,
			ex.server.TelemetryLoggingMetrics,
			ex.implicitTxn(),
			ex.statsCollector,
			ex.extraTxnState.shouldLogToTelemetry)
	})

	// Overwrite res.Error to a more user-friendly message in case of query
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

		// Note that here we process the cleanup function (which will append it
		// to the cleanup queue) without a defer since there is no more code
		// relevant to pausable portals model below.
		processCleanupFunc(func() {
			cancelQueryCtx := ctx
			if portal.isPausable() {
				cancelQueryCtx = portal.pauseInfo.execStmtInOpenState.cancelQueryCtx
			}
			resToPushErr := res
			// For pausable portals, we retain the query but update the result for
			// each execution. When the query context is cancelled and we're in the
			// middle of an portal execution, push the error to the current result.
			if portal.isPausable() {
				resToPushErr = portal.pauseInfo.curRes
			}
			vars.logErr = resToPushErr.ErrAllowReleased()
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
				retEv = eventNonRetryableErr{
					IsCommit: fsm.FromBool(isCommit(vars.ast)),
				}
				errToPush := cancelchecker.QueryCanceledError
				// For pausable portal, we can arrive here after encountering a timeout
				// error and then perform a query-cleanup step. In this case, we don't
				// want to override the original timeout error with the query-cancelled
				// error.
				if portal.isPausable() && (errors.Is(resToPushErr.Err(), sqlerrors.QueryTimeoutError) ||
					errors.Is(resToPushErr.Err(), sqlerrors.TxnTimeoutError)) {
					errToPush = resToPushErr.Err()
				}
				resToPushErr.SetError(errToPush)
				retPayload = eventNonRetryableErrPayload{err: errToPush}
				vars.logErr = errToPush
				// Cancel the txn if we are inside an implicit txn too.
				if ex.implicitTxn() && ex.state.txnCancelFn != nil {
					ex.state.txnCancelFn()
				}
			}
		})

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
			retEv = eventNonRetryableErr{
				IsCommit: fsm.FromBool(isCommit(vars.ast)),
			}
			res.SetError(sqlerrors.QueryTimeoutError)
			retPayload = eventNonRetryableErrPayload{err: sqlerrors.QueryTimeoutError}
			vars.logErr = sqlerrors.QueryTimeoutError
		} else if txnTimedOut {
			retEv = eventNonRetryableErr{
				IsCommit: fsm.FromBool(isCommit(vars.ast)),
			}
			res.SetError(sqlerrors.TxnTimeoutError)
			retPayload = eventNonRetryableErrPayload{err: sqlerrors.TxnTimeoutError}
			vars.logErr = sqlerrors.TxnTimeoutError
		}

	}(ctx, res)

	switch s := vars.ast.(type) {
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
		ev, payload := ex.commitSQLTransaction(ctx, vars.ast, ex.commitSQLTransactionInternal)
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

	case *tree.PrepareTransaction:
		ev, payload := ex.execPrepareTransactionInOpenState(ctx, s)
		return ev, payload, nil

	case *tree.ShowCommitTimestamp:
		ev, payload := ex.execShowCommitTimestampInOpenState(ctx, s, res, canAutoCommit)
		return ev, payload, nil

	case *tree.Prepare:
		// This is handling the SQL statement "PREPARE". See execPrepare for
		// handling of the protocol-level command for preparing statements.
		name := s.Name.String()
		if ex.extraTxnState.prepStmtsNamespace.prepStmts.Has(name) {
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
		if vars.stmt.NumPlaceholders > numParams {
			numParams = vars.stmt.NumPlaceholders
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
			ctx,
			statements.Statement[tree.Statement]{
				// We need the SQL string just for the part that comes after
				// "PREPARE ... AS",
				// TODO(radu): it would be nice if the parser would figure out this
				// string and store it in tree.Prepare.
				SQL:             tree.AsStringWithFlags(s.Statement, tree.FmtParsable),
				AST:             s.Statement,
				NumPlaceholders: vars.stmt.NumPlaceholders,
				NumAnnotations:  vars.stmt.NumAnnotations,
			},
			ex.server.cfg.GenerateID(),
			tree.FmtFlags(tree.QueryFormattingForFingerprintsMask.Get(&ex.server.cfg.Settings.SV)),
			statementHintsCache,
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
			p.stmt = vars.stmt
			p.extendedEvalCtx.Placeholders = oldPlaceholders
		}()
		if _, err := ex.addPreparedStmt(
			ctx, name, prepStmt, typeHints, rawTypeHints, prep.StatementOriginSQL,
		); err != nil {
			return makeErrEvent(err)
		}
		return nil, nil, nil
	}

	// Check if we need to auto-commit the transaction due to DDL.
	if ev, payload := ex.maybeAutoCommitBeforeDDL(ctx, vars.ast); ev != nil {
		return ev, payload, nil
	}

	// For regular statements (the ones that get to this point), we
	// don't return any event unless an error happens, or a CALL statement
	// performs a nested transaction COMMIT or ROLLBACK.

	// For a portal (prepared stmt), since handleAOST() is called when preparing
	// the statement, and this function is idempotent, we don't need to
	// call it again during execution.
	if portal == nil {
		if err := ex.handleAOST(ctx, vars.ast); err != nil {
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
	delegatedUnderOuterTxn := ex.executorType == executorTypeInternal && ex.extraTxnState.underOuterTxn
	var origTs hlc.Timestamp
	defer func() {
		_ = ex.state.mu.txn.ConfigureStepping(ctx, prevSteppingMode)

		// If this is an internal executor that is running on behalf of an outer
		// txn, then we need to step back the txn so that the outer executor uses
		// the proper sequence number.
		if delegatedUnderOuterTxn {
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
		if delegatedUnderOuterTxn {
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
	if err := ex.state.mu.txn.Step(ctx, !delegatedUnderOuterTxn /* allowReadTimestampStep */); err != nil {
		return makeErrEvent(err)
	}
	if buildutil.CrdbTestBuild && delegatedUnderOuterTxn {
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

	if portal.isPausable() {
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

	if ex.state.mu.autoRetryReason != nil {
		p.autoRetryCounter = int(ex.state.mu.autoRetryCounter)
		ex.sessionTracing.TraceRetryInformation(
			ctx, "transaction", int(ex.state.mu.autoRetryCounter), ex.state.mu.autoRetryReason,
		)
		if ex.server.cfg.TestingKnobs.OnTxnRetry != nil {
			ex.server.cfg.TestingKnobs.OnTxnRetry(ex.state.mu.autoRetryReason, p.EvalContext())
		}
	}

	if ex.executorType != executorTypeInternal &&
		ex.state.mu.txn.IsoLevel() == isolation.ReadCommitted &&
		!ex.implicitTxn() {
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
		stmtDur := ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionQueryReceived).Elapsed()
		if needRecording := stmtDur >= stmtTraceThreshold; needRecording {
			rec := stmtThresholdSpan.FinishAndGetRecording(tracingpb.RecordingVerbose)
			// NB: This recording does not include the commit for implicit
			// transactions if the statement didn't auto-commit.
			redactableStmt := p.FormatAstAsRedactableString(vars.stmt.AST, &p.semaCtx.Annotations)
			logTraceAboveThreshold(
				ctx,
				rec,                /* recording */
				"SQL statement",    /* opName */
				redactableStmt,     /* detail */
				stmtTraceThreshold, /* threshold */
				stmtDur,            /* elapsed */
				false,              /* outputJaegerJSON */
			)
		} else {
			stmtThresholdSpan.Finish()
		}
	}

	if err = res.Err(); err != nil {
		return makeErrEvent(err)
	}

	txn := ex.state.mu.txn

	if !os.ImplicitTxn.Get() && txn.IsSerializablePushAndRefreshNotPossible() {
		rc, canAutoRetry := ex.getRewindTxnCapability()
		if canAutoRetry {
			ev := eventRetryableErr{
				IsCommit:     fsm.FromBool(isCommit(vars.ast)),
				CanAutoRetry: fsm.FromBool(canAutoRetry),
			}
			payload := eventRetryableErrPayload{
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
		ev, payload := ex.commitSQLTransaction(ctx, vars.ast, ex.commitSQLTransactionInternal)
		if payload != nil {
			return ev, payload, nil
		}
		return eventTxnFinishCommittedPLpgSQL{}, nil, nil
	case tree.StoredProcTxnRollback:
		// Abort the current transaction. The connExecutor will open a new
		// transaction, and then return to executing the same CALL statement.
		ev, payload := ex.rollbackSQLTransaction(ctx, vars.ast)
		if payload != nil {
			return ev, payload, nil
		}
		return eventTxnFinishAbortedPLpgSQL{}, nil, nil
	}

	// No event was generated.
	return nil, nil, nil
}

func (ex *connExecutor) stepReadSequence(ctx context.Context) error {
	prevSteppingMode := ex.state.mu.txn.ConfigureStepping(ctx, kv.SteppingEnabled)
	if prevSteppingMode == kv.SteppingEnabled {
		if err := ex.state.mu.txn.Step(ctx, false /* allowReadTimestampStep */); err != nil {
			return err
		}
	} else {
		ex.state.mu.txn.ConfigureStepping(ctx, prevSteppingMode)
	}
	return nil
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
			if !asOf.ForBackfill {
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
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"cannot specify AS OF SYSTEM TIME with different timestamps. expected: %s, got: %s",
			p.extendedEvalCtx.AsOfSystemTime.Timestamp,
			asOf.Timestamp,
		)
	}
	// Bounded staleness and backfills with a historical timestamp are both not
	// allowed in explicit transactions.
	if asOf.BoundedStaleness {
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"cannot use a bounded staleness query in a transaction",
		)
	}
	if asOf.ForBackfill {
		return unimplemented.NewWithIssuef(
			35712,
			"cannot run a backfill with AS OF SYSTEM TIME in a transaction",
		)
	}
	// If we're in an explicit txn, we allow AOST but only if it matches with
	// the transaction's timestamp. This is useful for running AOST statements
	// using the Executor inside an external transaction; one might want
	// to do that to force p.avoidLeasedDescriptors to be set below.
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
	regionCache, err := regions.NewCachedDatabaseRegionsAt(ctx, ex.server.cfg.DB, ex.server.cfg.LeaseManager, ex.state.mu.txn.ProvisionalCommitTimestamp())
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
	if buildutil.CrdbTestBuild {
		// For now, we explicitly disable buffered writes before executing DDLs.
		// TODO(#140695): we should consider allowing this in the future.
		if ex.state.mu.txn.BufferedWritesEnabled() {
			return errors.AssertionFailedf("buffered writes should have been disabled on a DDL")
		}
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
	ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionStartTransactionCommit, crtime.NowMono())
	if err := commitFn(ctx); err != nil {
		// For certain retryable errors, we should turn them into client visible
		// errors, since the client needs to retry now.
		var conversionError error
		err, conversionError = ex.convertRetryableErrorIntoUserVisibleError(ctx, err)
		if conversionError != nil {
			return ex.makeErrEvent(conversionError, ast)
		}
		return ex.makeErrEvent(err, ast)
	}
	ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionEndTransactionCommit, crtime.NowMono())
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
	if ex.dataMutatorIterator.ParamStatusUpdater != nil {
		for _, param := range bufferableParamStatusUpdates {
			if param.sv.Equal == nil {
				return errors.AssertionFailedf("Equal for %s must be set", param.name)
			}
			if param.sv.GetFromSessionData == nil {
				return errors.AssertionFailedf("GetFromSessionData for %s must be set", param.name)
			}
			if !param.sv.Equal(before, after) {
				ex.dataMutatorIterator.ParamStatusUpdater.BufferParamStatusUpdate(
					param.name,
					param.sv.GetFromSessionData(after),
				)
			}
		}
	}
	if before.DefaultIntSize != after.DefaultIntSize && ex.dataMutatorIterator.OnDefaultIntSizeChange != nil {
		ex.dataMutatorIterator.OnDefaultIntSizeChange(after.DefaultIntSize)
	}
	if before.ApplicationName != after.ApplicationName && ex.dataMutatorIterator.OnApplicationNameChange != nil {
		ex.dataMutatorIterator.OnApplicationNameChange(after.ApplicationName)
	}
	return nil
}

func (ex *connExecutor) commitSQLTransactionInternal(ctx context.Context) (retErr error) {
	ctx, sp := tracing.ChildSpan(ctx, "commit sql txn")
	defer sp.Finish()

	defer func() {
		failed := retErr != nil
		ex.recordDDLTxnTelemetry(failed)
	}()

	if err := ex.extraTxnState.sqlCursors.closeAll(&ex.planner, cursorCloseForTxnCommit); err != nil {
		return err
	}

	ex.extraTxnState.prepStmtsNamespace.closePortals(ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc)

	// We need to step the transaction's read sequence before committing if it has
	// stepping enabled. If it doesn't have stepping enabled, then we just set the
	// stepping mode back to what it was.
	//
	// Even if we do step the transaction's read sequence, we do not advance its
	// read timestamp (applicable only to read committed transactions). This is
	// because doing so is not needed before committing, and it would cause the
	// transaction to commit at a higher timestamp than necessary. On heavily
	// contended workloads like the one from #109628, this can cause unnecessary
	// write-write contention between transactions by inflating the contention
	// footprint of each transaction (i.e. the duration measured in MVCC time that
	// the transaction holds locks).
	if err := ex.stepReadSequence(ctx); err != nil {
		return err
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

	if err := ex.extraTxnState.descCollection.EmitDescriptorUpdatesKey(ctx, ex.state.mu.txn); err != nil {
		return err
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
	ex.extraTxnState.idleLatency += ex.statsCollector.PhaseTimes().
		GetIdleLatency(ex.statsCollector.PreviousPhaseTimes())

	if err := ex.extraTxnState.sqlCursors.closeAll(&ex.planner, cursorCloseForTxnRollback); err != nil {
		return ex.makeErrEvent(err, stmt)
	}

	ex.extraTxnState.prepStmtsNamespace.closePortals(ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc)
	ex.recordDDLTxnTelemetry(true /* failed */)

	// A non-retryable error automatically rolls-back the transaction if there are
	// no savepoints; see the state transition logic in conn_fsm.go. In that case,
	// we can skip rolling-back the transaction here.
	isKVTxnOpen := true
	if _, isAbortedTxn := ex.machine.CurState().(stateAborted); isAbortedTxn {
		isKVTxnOpen = ex.state.mu.txn.IsOpen()
	}
	if isKVTxnOpen {
		// Step the read sequence before rolling back because the read sequence
		// may be in the span reverted by a savepoint.
		err := ex.stepReadSequence(ctx)
		err = errors.CombineErrors(err, ex.state.mu.txn.Rollback(ctx))
		if err != nil {
			if buildutil.CrdbTestBuild && errors.IsAssertionFailure(err) {
				log.Dev.Fatalf(ctx, "txn rollback failed: %+v", err)
			}
			log.Dev.Warningf(ctx, "txn rollback failed: %s", err)
		}
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

func getPausablePortalInfo(p *planner) *portalPauseInfo {
	if p != nil && p.pausablePortal != nil {
		return p.pausablePortal.pauseInfo
	}
	return nil
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
	if ex.executorType == executorTypeInternal {
		// Because we step the read timestamp below, this is not safe to call within
		// internal executor.
		return errors.AssertionFailedf(
			"call of dispatchReadCommittedStmtToExecutionEngine within internal executor",
		)
	}

	if ppInfo := getPausablePortalInfo(p); ppInfo != nil {
		p.autoRetryStmtReason = ppInfo.dispatchReadCommittedStmtToExecutionEngine.autoRetryStmtReason
		p.autoRetryStmtCounter = ppInfo.dispatchReadCommittedStmtToExecutionEngine.autoRetryStmtCounter
	}

	readCommittedSavePointToken, err := ex.state.mu.txn.CreateSavepoint(ctx)
	if err != nil {
		return err
	}

	// Use retry with exponential backoff and full jitter to reduce collisions for
	// high-contention workloads. See https://en.wikipedia.org/wiki/Exponential_backoff and
	// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
	maxRetries := int(ex.sessionData().MaxRetriesForReadCommitted)
	initialBackoff := ex.sessionData().InitialRetryBackoffForReadCommitted
	useBackoff := initialBackoff > 0
	opts := retry.Options{
		InitialBackoff:      initialBackoff,
		MaxBackoff:          1024 * initialBackoff,
		Multiplier:          2.0,
		RandomizationFactor: 1.0,
	}
	for attemptNum, r := 0, retry.StartWithCtx(ctx, opts); !useBackoff || r.Next(); attemptNum++ {
		// TODO(99410): Fix the phase time for pausable portals.
		startExecTS := crtime.NowMono()
		ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.PlannerMostRecentStartExecStmt, startExecTS)
		if attemptNum == 0 {
			ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.PlannerFirstStartExecStmt, startExecTS)
		} else {
			ex.sessionTracing.TraceRetryInformation(
				ctx, "statement", p.autoRetryStmtCounter, p.autoRetryStmtReason,
			)
			// Step both the sequence number and the read timestamp so that we can see
			// the results of the conflicting transactions that caused us to fail and
			// any other transactions that occurred in the meantime.
			if err := ex.state.mu.txn.Step(ctx, true /* allowReadTimestampStep */); err != nil {
				return err
			}
			// Also step statement_timestamp so that any SQL using it is up-to-date.
			stmtTS := ex.server.cfg.Clock.PhysicalTime()
			p.extendedEvalCtx.StmtTimestamp = stmtTS
		}
		bufferPos := res.BufferedResultsLen()
		if err = ex.dispatchToExecutionEngine(ctx, p, res); err != nil {
			return err
		}
		maybeRetryableErr := res.Err()
		if maybeRetryableErr == nil {
			// If there was no error, then we must release the savepoint and break.
			if err := ex.state.mu.txn.ReleaseSavepoint(ctx, readCommittedSavePointToken); err != nil {
				return err
			}
			break
		}
		// If the error does not allow for a partial retry, then stop. The error
		// is already set on res.Err() and will be returned to the client.
		var txnRetryErr *kvpb.TransactionRetryWithProtoRefreshError
		if !errors.As(maybeRetryableErr, &txnRetryErr) || txnRetryErr.TxnMustRestartFromBeginning() {
			break
		}

		// If we reached the maximum number of retries, then we must stop.
		if attemptNum == maxRetries {
			res.SetError(errors.Wrapf(
				maybeRetryableErr,
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
				maybeRetryableErr,
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
		p.autoRetryStmtCounter++
		p.autoRetryStmtReason = maybeRetryableErr
		if ppInfo := getPausablePortalInfo(p); ppInfo != nil {
			ppInfo.dispatchReadCommittedStmtToExecutionEngine.autoRetryStmtReason = p.autoRetryStmtReason
			ppInfo.dispatchReadCommittedStmtToExecutionEngine.autoRetryStmtCounter = p.autoRetryStmtCounter
		}
		ex.metrics.EngineMetrics.StatementRetryCount.Inc(1)
	}
	// Check if we exited the loop due to cancelation.
	if useBackoff {
		select {
		case <-ctx.Done():
			res.SetError(cancelchecker.QueryCanceledError)
		default:
		}
	}
	return nil
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
) (retErr error) {
	pausablePortalInfo := getPausablePortalInfo(planner)
	if pausablePortalInfo != nil {
		// This is ugly, but we need to override the execMon to the specific one
		// owned by the pausable portals.
		planner.execMon = ex.ppExecMon
	} else {
		// Guarantee that we use the global execMon in case we had some pausable
		// portal executions in between.
		planner.execMon = ex.execMon
		planner.execMon.StartNoReserved(ctx, planner.txnMon)
	}
	defer func() {
		if ppInfo := getPausablePortalInfo(planner); ppInfo != nil {
			if !ppInfo.dispatchToExecutionEngine.cleanup.isComplete {
				ppInfo.dispatchToExecutionEngine.cleanup.appendFunc(func(context.Context) {
					// Restore the original execMon since we overrode it.
					planner.execMon = ex.execMon
				})
				ppInfo.dispatchToExecutionEngine.cleanup.isComplete = true
			}
			if retErr != nil || res.Err() != nil {
				ppInfo.resumableFlow.cleanup.run(ctx)
				ppInfo.dispatchToExecutionEngine.cleanup.run(ctx)
			}
		} else {
			planner.execMon.Stop(ctx)
		}
	}()

	var cpuProvider admission.SQLCPUProvider
	if server := ex.server.cfg.DistSQLSrv; server != nil {
		cpuProvider = server.SQLCPUProvider
	}
	// TODO(yuzefovich): support CPU reporting for pausable portal execution,
	// which does not call Flow.Wait, which we rely on below for closing the
	// cpuHandle.
	if cpuProvider != nil && pausablePortalInfo == nil {
		var cpuHandle *admission.SQLCPUHandle
		var mainGoroutineCPUHandle *admission.GoroutineCPUHandle
		var err error
		ctx, cpuHandle, mainGoroutineCPUHandle, err = flowinfra.MakeCPUHandle(
			ctx, cpuProvider, planner.extendedEvalCtx.Codec.TenantID, planner.txn, true /* atGateway */)
		if err != nil {
			return err
		}
		defer func() {
			// Close the main goroutine's CPU handle at the flow boundary. This must
			// happen before cpuHandle.Close() which will pool the GoroutineCPUHandle.
			mainGoroutineCPUHandle.Close(ctx)
			// Close the SQLCPUHandle after all GoroutineCPUHandles are closed.
			// At this point, all flow goroutines have exited (Wait() was called in
			// flow.Cleanup), and the main goroutine's handle was just closed above.
			// NB: there isn't a memory safety issue if some GoroutineCPUHandles are not
			// yet closed, in that the pooling logic only returns closed GoroutineCPUHandles
			// to the pool. But it is preferable for performance, and for full accounting
			// of cpu consumtion.
			cpuHandle.Close()
		}()
	}

	stmt := planner.stmt
	ex.sessionTracing.TracePlanStart(ctx, stmt.AST.StatementTag())
	// TODO(sql-sessions): fix the phase time for pausable portals.
	// https://github.com/cockroachdb/cockroach/issues/99410
	ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.PlannerStartLogicalPlan, crtime.NowMono())

	if execinfra.IncludeRUEstimateInExplainAnalyze.Get(ex.server.cfg.SV()) {
		if server := ex.server.cfg.DistSQLSrv; server != nil {
			// Begin measuring CPU usage for tenants. This is a no-op for non-tenants.
			ex.cpuStatsCollector.StartCollection(ctx, server.TenantCostController)
		}
	}

	// If we've been tasked with backfilling a schema change operation at a
	// particular system time, it's important that we do planning for the
	// operation at the timestamp that we're expecting to perform the backfill at,
	// in case the schema of the objects that we read have changed in between the
	// present transaction timestamp and the user-defined backfill timestamp.
	//
	// Set the planner's transaction to a new historical transaction pinned at
	// that timestamp, and give it a new collection. We'll restore it after
	// planning.
	var restoreOriginalPlanner func() error
	if asOf := planner.extendedEvalCtx.AsOfSystemTime; asOf != nil && asOf.ForBackfill {
		nodeID, _ := planner.execCfg.NodeInfo.NodeID.OptionalNodeID()
		historicalTxn := kv.NewTxnWithSteppingEnabled(ctx, planner.execCfg.DB, nodeID, ex.QualityOfService())
		if err := historicalTxn.SetFixedTimestamp(ctx, asOf.Timestamp); err != nil {
			res.SetError(err)
			return nil
		}
		originalTxn := planner.txn
		planner.txn = historicalTxn
		planner.schemaResolver.txn = historicalTxn
		dsdp := catsessiondata.NewDescriptorSessionDataStackProvider(planner.sessionDataStack)
		historicalCollection := planner.execCfg.CollectionFactory.NewCollection(
			ctx, descs.WithDescriptorSessionDataProvider(dsdp),
		)
		planner.descCollection = historicalCollection
		planner.extendedEvalCtx.Descs = historicalCollection
		restoreOriginalPlanner = func() error {
			planner.txn = originalTxn
			planner.schemaResolver.txn = originalTxn
			planner.descCollection = ex.extraTxnState.descCollection
			planner.extendedEvalCtx.Descs = ex.extraTxnState.descCollection
			historicalCollection.ReleaseAll(ctx)
			if err := historicalTxn.Commit(ctx); err != nil {
				return err
			}
			return nil
		}
	}

	var err error
	if ppInfo := getPausablePortalInfo(planner); ppInfo != nil {
		if !ppInfo.dispatchToExecutionEngine.cleanup.isComplete {
			ctx, err = ex.makeExecPlan(ctx, planner)
			if err == nil {
				// TODO(janexing): This is a temporary solution to disallow procedure
				// call statements that contain mutations for pausable portals. Since
				// relational.CanMutate is not yet propagated from the function body
				// via builder.BuildCall(), we must temporarily disallow all
				// TCL statements, which includes the CALL statements.
				// This should be removed once CanMutate is fully propagated.
				// (pending https://github.com/cockroachdb/cockroach/issues/147568)
				isTCL := planner.curPlan.stmt.AST.StatementType() == tree.TypeTCL
				// We don't allow mutations in a pausable portal.
				notReadOnly := isTCL || planner.curPlan.flags.IsSet(planFlagContainsMutation) || planner.curPlan.flags.IsSet(planFlagIsDDL)
				// We don't allow sub / post queries for pausable portal.
				hasSubOrPostQuery := len(planner.curPlan.subqueryPlans) != 0 || len(planner.curPlan.cascades) != 0 ||
					len(planner.curPlan.checkPlans) != 0 || len(planner.curPlan.triggers) != 0
				if notReadOnly || hasSubOrPostQuery {
					if notReadOnly {
						telemetry.Inc(sqltelemetry.NotReadOnlyStmtsTriedWithPausablePortals)
					} else {
						telemetry.Inc(sqltelemetry.SubOrPostQueryStmtsTriedWithPausablePortals)
					}
					// This stmt is not supported via the pausable portals model
					// - set it back to an un-pausable (normal) portal.
					//
					// But before we do that, we need to set the right execMon.
					// Note that we might have made reservations against the
					// pausable portal execMon when creating the logical plan,
					// and that's ok - those will be released when the plan is
					// closed in a defer below.
					planner.execMon = ex.execMon
					planner.execMon.StartNoReserved(ctx, planner.txnMon)
					ex.disablePortalPausability(planner.pausablePortal)
					planner.pausablePortal = nil
					err = res.RevokePortalPausability()
					// If this plan is a transaction control statement, we don't
					// even execute it but just early exit.
					if isTCL {
						err = errors.CombineErrors(err, ErrStmtNotSupportedForPausablePortal)
					}
					defer planner.curPlan.close(ctx)
				} else {
					ppInfo.dispatchToExecutionEngine.planTop = planner.curPlan
					defer func() {
						ppInfo.dispatchToExecutionEngine.cleanup.appendFunc(
							ppInfo.dispatchToExecutionEngine.planTop.close,
						)
					}()
				}
			} else {
				defer planner.curPlan.close(ctx)
			}
		} else {
			planner.curPlan = ppInfo.dispatchToExecutionEngine.planTop
		}
	} else {
		// Prepare the plan. Note, the error is processed below. Everything
		// between here and there needs to happen even if there's an error.
		ctx, err = ex.makeExecPlan(ctx, planner)
		defer planner.curPlan.close(ctx)
	}

	if planner.extendedEvalCtx.TxnImplicit {
		planner.curPlan.flags.Set(planFlagImplicitTxn)
	}

	// Certain statements want their results to go to the client
	// directly. Configure this here.
	if ex.executorType != executorTypeInternal && (planner.curPlan.avoidBuffering || ex.sessionData().AvoidBuffering) {
		res.DisableBuffering()
	}

	// TODO(sql-sessions): fix the phase time for pausable portals.
	// https://github.com/cockroachdb/cockroach/issues/99410
	ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.PlannerEndLogicalPlan, crtime.NowMono())
	ex.sessionTracing.TracePlanEnd(ctx, err)

	if restoreOriginalPlanner != nil {
		// Reset the planner's transaction to the current-timestamp, original
		// transaction.
		if err := restoreOriginalPlanner(); err != nil {
			res.SetError(err)
			return nil
		}
	}

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

	var afterGetPlanDistribution func()
	if getPausablePortalInfo(planner) != nil {
		// We don't allow a distributed plan for pausable portals.
		origDistSQLMode := ex.sessionData().DistSQLMode
		ex.sessionData().DistSQLMode = sessiondatapb.DistSQLOff
		afterGetPlanDistribution = func() {
			ex.sessionData().DistSQLMode = origDistSQLMode
		}
	}
	distributePlan, blockers := planner.getPlanDistribution(ctx, planner.curPlan.main, notPostquery)
	if afterGetPlanDistribution != nil {
		afterGetPlanDistribution()
	}
	ex.sessionTracing.TracePlanCheckEnd(ctx, nil, distributePlan.WillDistribute())

	if ex.server.cfg.TestingKnobs.BeforeExecute != nil {
		ex.server.cfg.TestingKnobs.BeforeExecute(ctx, stmt.String(), planner.Descriptors())
	}

	// TODO(sql-sessions): fix the phase time for pausable portals.
	// https://github.com/cockroachdb/cockroach/issues/99410
	ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.PlannerStartExecStmt, crtime.NowMono())

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
	}

	distribute := DistributionType(LocalDistribution)
	if distributePlan.WillDistribute() {
		distribute = FullDistribution
	}
	ex.sessionTracing.TraceExecStart(ctx, "distributed")
	stats, err := ex.execWithDistSQLEngine(
		ctx, planner, stmt.AST.StatementReturnType(), res, distribute, progAtomic, blockers,
	)
	if ppInfo := getPausablePortalInfo(planner); ppInfo != nil {
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
	ex.statsCollector.PhaseTimes().SetSessionPhaseTime(sessionphase.PlannerEndExecStmt, crtime.NowMono())

	ex.extraTxnState.rowsRead += stats.rowsRead
	ex.extraTxnState.bytesRead += stats.bytesRead
	ex.extraTxnState.rowsWritten += stats.rowsWritten
	ex.extraTxnState.kvCPUTimeNanos += stats.kvCPUTimeNanos

	if ppInfo := getPausablePortalInfo(planner); ppInfo != nil && !ppInfo.dispatchToExecutionEngine.cleanup.isComplete {
		// We need to ensure that we're using the planner bound to the first-time
		// execution of a portal.
		curPlanner := *planner
		// Note that here we append the cleanup function without a defer since
		// there is no more code relevant to pausable portals model below.
		ppInfo.dispatchToExecutionEngine.cleanup.appendFunc(func(ctx context.Context) {
			populateQueryLevelStats(ctx, &curPlanner, ex.server.cfg, ppInfo.dispatchToExecutionEngine.queryStats, &ex.cpuStatsCollector)
			ppInfo.dispatchToExecutionEngine.stmtFingerprintID = ex.recordStatementSummary(
				ctx, &curPlanner, int(ex.state.mu.autoRetryCounter), planner.autoRetryStmtCounter,
				ppInfo.dispatchToExecutionEngine.rowsAffected, ppInfo.curRes.ErrAllowReleased(),
				*ppInfo.dispatchToExecutionEngine.queryStats,
			)
		})
	} else {
		populateQueryLevelStats(ctx, planner, ex.server.cfg, &stats, &ex.cpuStatsCollector)
		ex.recordStatementSummary(
			ctx, planner, int(ex.state.mu.autoRetryCounter), planner.autoRetryStmtCounter,
			res.RowsAffected(), res.Err(), stats,
		)
	}

	if ex.server.cfg.TestingKnobs.AfterExecute != nil {
		ex.server.cfg.TestingKnobs.AfterExecute(ctx, stmt.String(), ex.executorType == executorTypeInternal, res.Err())
	}

	if limitsErr := ex.handleTxnRowsWrittenReadLimits(ctx); limitsErr != nil && res.Err() == nil {
		res.SetError(limitsErr)
	}

	return err
}

// populateQueryLevelStats collects query-level execution statistics
// and populates it in the instrumentationHelper's fields:
//   - topLevelStats contains the top-level execution statistics.
//   - queryLevelStatsWithErr contains query-level execution statistics are
//     collected using the statement's trace and the plan's flow metadata.
func populateQueryLevelStats(
	ctx context.Context,
	p *planner,
	cfg *ExecutorConfig,
	topLevelStats *topLevelQueryStats,
	cpuStats *multitenantcpu.CPUUsageHelper,
) {
	ih := &p.instrumentation
	ih.topLevelStats = *topLevelStats

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
		trace, cfg.TestingKnobs.DeterministicExplain, flowsMetadata,
	)
	queryLevelStatsWithErr := execstats.MakeQueryLevelStatsWithErr(queryLevelStats, err)
	ih.queryLevelStatsWithErr = &queryLevelStatsWithErr
	if err != nil {
		const msg = "error getting query level stats for statement: %s: %+v"
		if buildutil.CrdbTestBuild {
			panic(fmt.Sprintf(msg, ih.fingerprint, err))
		}
		log.Dev.VInfof(ctx, 1, msg, ih.fingerprint, err)
	} else {
		// If this query is being run by a tenant, record the RUs consumed by CPU
		// usage and network egress to the client.
		if execinfra.IncludeRUEstimateInExplainAnalyze.Get(cfg.SV()) && cfg.DistSQLSrv != nil {
			if costController := cfg.DistSQLSrv.TenantCostController; costController != nil {
				if costCfg := costController.GetRequestUnitModel(); costCfg != nil {
					networkEgressRUEstimate := costCfg.PGWireEgressCost(topLevelStats.networkEgressEstimate)
					ih.queryLevelStatsWithErr.Stats.RUEstimate += float64(networkEgressRUEstimate)
					ih.queryLevelStatsWithErr.Stats.RUEstimate += cpuStats.EndCollection(ctx)
				}
			}
		}
		ih.queryLevelStatsWithErr.Stats.ClientTime = topLevelStats.clientTime
		if cfg.TestingKnobs.DeterministicExplain {
			// We only show AdmissionWaitTime when it's non-zero, yet its value
			// is non-deterministic, so if we need deterministic EXPLAIN, then
			// we need to zero it out.
			ih.queryLevelStatsWithErr.Stats.AdmissionWaitTime = 0
		}
		if p.ExecMon().MaximumBytes() > ih.queryLevelStatsWithErr.Stats.MaxMemUsage {
			// For the query-level stats, for MaxMemUsage we want to include the
			// memory usage during the logical planning, so we check whether the
			// exec monitor (which includes that in addition to the gateway
			// flow's memory usage) has higher watermark than any of the flows.
			ih.queryLevelStatsWithErr.Stats.MaxMemUsage = p.execMon.MaximumBytes()
		}
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
		commonSQLEventDetails := ex.planner.getCommonSQLEventDetails()
		var event logpb.EventPayload
		var migrator log.StructuredEventMigrator
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
			migrator = log.NewStructuredEventMigrator(func() bool {
				return log.ShouldMigrateEvent(ex.planner.ExecCfg().SV())
			}, logpb.Channel_SQL_INTERNAL_PERF)
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
			migrator = log.NewStructuredEventMigrator(func() bool {
				return log.ShouldMigrateEvent(ex.planner.ExecCfg().SV())
			}, logpb.Channel_SQL_PERF)
		}
		migrator.StructuredEvent(ctx, severity.INFO, event)
		logCounter.Inc(1)
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

// makeExecPlan creates an execution plan and populates planner.curPlan using
// the cost-based optimizer. This is used to create the plan when executing a
// query in the "simple" pgwire protocol.
func (ex *connExecutor) makeExecPlan(
	ctx context.Context, planner *planner,
) (context.Context, error) {
	if err := ex.maybeAdjustTxnForDDL(ctx, planner.stmt); err != nil {
		return ctx, err
	}

	// For each non-internal query, we roll the dice to decide to use
	// canary stats or stable stats for planning.
	if !planner.SessionData().Internal {
		planner.EvalContext().UseCanaryStats = canaryRollDice(planner.EvalContext(), ex.rng.internal)
	}

	if err := planner.makeOptimizerPlan(ctx); err != nil {
		log.VEventf(ctx, 1, "optimizer plan failed: %v", err)
		return ctx, err
	}

	flags := planner.curPlan.flags

	if flags.IsSet(planFlagContainsFullIndexScan) || flags.IsSet(planFlagContainsFullTableScan) {
		if ex.executorType == executorTypeExec && planner.EvalContext().SessionData().DisallowFullTableScans {
			hasLargeScan := flags.IsSet(planFlagContainsLargeFullIndexScan) || flags.IsSet(planFlagContainsLargeFullTableScan)
			if hasLargeScan {
				// We don't execute the statement if:
				// - plan contains a full table or full index scan.
				//   TODO(#123783): this currently doesn't apply to full scans
				//   of virtual tables.
				// - the session setting disallows full table/index scans.
				// - the scan is considered large.
				// - the query is not an internal query.
				ex.metrics.EngineMetrics.FullTableOrIndexScanRejectedCount.Inc(1)
				return ctx, errors.WithHint(
					pgerror.Newf(pgcode.TooManyRows,
						"query `%s` contains a full table/index scan which is explicitly disallowed",
						planner.stmt.SQL),
					"to permit this scan, set disallow_full_table_scans to false or increase the large_full_scan_rows threshold",
				)
			}
		}
		ex.metrics.EngineMetrics.FullTableOrIndexScanCount.Inc(1, ex.sessionData().Database, ex.sessionData().ApplicationName)
	}

	if flags.IsSet(planFlagUsesRLS) {
		ex.metrics.EngineMetrics.RLSPoliciesAppliedCount.Inc(1)
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

	// Include gist in error reports.
	ih := &planner.instrumentation
	ex.curStmtPlanGist = redact.SafeString(ih.planGist.String())
	if buildutil.CrdbTestBuild && ih.planGist.String() != "" {
		// Ensure that the gist can be decoded in test builds.
		//
		// In 50% cases, use nil catalog.
		var catalog cat.Catalog
		if ex.rng.internal.Float64() < 0.5 && !planner.SessionData().AllowRoleMembershipsToChangeDuringTransaction {
			// For some reason, TestAllowRoleMembershipsToChangeDuringTransaction
			// times out with non-nil catalog, so we'll keep it as nil when the
			// session var is set to 'true' ('false' is the default).
			catalog = planner.optPlanningCtx.catalog
		}
		_, err := explain.DecodePlanGistToRows(ctx, &planner.extendedEvalCtx.Context, ih.planGist.String(), catalog)
		if err != nil {
			// Serialization failures can occur from the lease manager if a
			// consistent view of descriptors was not observed.
			if pgerror.GetPGCode(err) == pgcode.SerializationFailure {
				return ctx, err
			}
			return ctx, errors.NewAssertionErrorWithWrappedErrf(err, "failed to decode plan gist: %q", ih.planGist.String())
		}
	}

	// Now that we have the plan gist, check whether we should get a bundle for
	// it.
	if !ih.collectBundle && ih.outputMode == unmodifiedOutput {
		ctx = ih.setupWithPlanGist(ctx, planner, ex.server.cfg)
	}
	if !ih.collectBundle {
		// We won't need the memo and the catalog, so free it up.
		planner.curPlan.mem = nil
		planner.curPlan.catalog = nil
	}

	return ctx, nil
}

// topLevelQueryStats returns some basic statistics about the run of the query.
type topLevelQueryStats struct {
	// rowsRead is the number of rows read from primary and secondary indexes.
	rowsRead int64
	// bytesRead is the number of bytes read from primary and secondary indexes.
	bytesRead int64
	// rowsWritten is the number of rows written to the primary index. It does not
	// include rows written to secondary indexes.
	// NB: There is an asymmetry between rowsRead and rowsWritten - rowsRead
	// includes rows read from secondary indexes, while rowsWritten does not
	// include rows written to secondary indexes. This matches the behavior of
	// EXPLAIN ANALYZE and SQL "rows affected".
	rowsWritten int64
	// indexRowsWritten is the number of rows written to primary and secondary
	// indexes. It is always >= rowsWritten.
	indexRowsWritten int64
	// indexBytesWritten is the number of bytes written to primary and secondary
	// indexes.
	indexBytesWritten int64
	// networkEgressEstimate is an estimate for the number of bytes sent to the
	// client. It is used for estimating the number of RUs consumed by a query.
	networkEgressEstimate int64
	// clientTime is the amount of time query execution was blocked on the
	// client receiving the PGWire protocol messages (as well as construcing
	// those messages).
	clientTime time.Duration
	// kvCPUTimeNanos is the CPU time consumed by KV operations during query execution.
	kvCPUTimeNanos time.Duration
	// NB: when adding another field here, consider whether
	// forwardInnerQueryStats method needs an adjustment.
}

func (s *topLevelQueryStats) add(other *topLevelQueryStats) {
	s.bytesRead += other.bytesRead
	s.rowsRead += other.rowsRead
	s.rowsWritten += other.rowsWritten
	s.indexBytesWritten += other.indexBytesWritten
	s.indexRowsWritten += other.indexRowsWritten
	s.networkEgressEstimate += other.networkEgressEstimate
	s.clientTime += other.clientTime
	s.kvCPUTimeNanos += other.kvCPUTimeNanos
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
	distSQLBlockers distSQLBlockers,
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
		planCtx.distSQLBlockers = distSQLBlockers

		var evalCtxFactory func(usedConcurrently bool) *extendedEvalContext
		if len(planner.curPlan.subqueryPlans) != 0 ||
			len(planner.curPlan.cascades) != 0 ||
			len(planner.curPlan.checkPlans) != 0 ||
			len(planner.curPlan.triggers) != 0 {
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

	if err == nil && res.Err() == nil {
		recv.maybeLogMisestimates(ctx, planner)
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
	// Note that here we don't use the txnMon (we use its parent) given that
	// we're in the NoTxn txn state, so the txnMon hasn't been started yet.
	ex.execMon.StartNoReserved(ctx, ex.sessionMon)
	defer ex.execMon.Stop(ctx)
	var modes tree.TransactionModes
	if s != nil {
		modes = s.Modes
	}
	asOfClause := ex.asOfClauseWithSessionDefault(modes.AsOf)
	if asOfClause.Expr == nil {
		rwMode = ex.readWriteModeWithSessionDefault(modes.ReadWriteMode)
		if ex.executorType == executorTypeExec {
			// Check if a PCR reader catalog timestamp is set, which
			// will cause to turn all txns into system time queries.
			if newTS := ex.GetPCRReaderTimestamp(); !newTS.IsEmpty() {
				return tree.ReadOnly, now, &newTS, nil
			}
		}
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
		stmt := makeStatement(
			ctx, parserStmt, ex.server.cfg.GenerateID(),
			tree.FmtFlags(tree.QueryFormattingForFingerprintsMask.Get(&ex.server.cfg.Settings.SV)),
			nil, /* statementHintsCache */
		)
		p.stmt = stmt
		p.semaCtx.Annotations = tree.MakeAnnotations(stmt.NumAnnotations)
		p.extendedEvalCtx.Annotations = &p.semaCtx.Annotations
		p.extendedEvalCtx.Placeholders = &tree.PlaceholderInfo{}
		p.curPlan.init(&p.stmt, &p.instrumentation)
		var execErr error
		if p, ok := payload.(payloadWithError); ok {
			execErr = p.errorCause()
		}

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
			ex.implicitTxn(),
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
				ex.bufferedWritesEnabled(ctx),
				ex.rng.internal,
			)
	case *tree.ShowCommitTimestamp:
		return ex.execShowCommitTimestampInNoTxnState(ctx, s, res)
	case *tree.CommitTransaction, *tree.RollbackTransaction, *tree.PrepareTransaction,
		*tree.SetTransaction, *tree.Savepoint, *tree.ReleaseSavepoint:
		if ex.sessionData().AutoCommitBeforeDDL {
			// If autocommit_before_ddl is set, we allow these statements to be
			// executed, and send a warning rather than an error.
			ex.planner.BufferClientNotice(ctx, pgerror.WithSeverity(errNoTransactionInProgress, "WARNING"))
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
				ex.bufferedWritesEnabled(ctx),
				ex.rng.internal,
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
			ex.bufferedWritesEnabled(ctx),
			ex.rng.internal,
		)
}

// execStmtInAbortedState executes a statement in a txn that's in state
// Aborted or RestartWait. All statements result in error events except:
//   - COMMIT / ROLLBACK / PREPARE TRANSACTION: aborts the current transaction.
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
		ev := eventNonRetryableErr{IsCommit: fsm.False}
		payload := eventNonRetryableErrPayload{
			err: sqlerrors.NewTransactionAbortedError("" /* customMsg */),
		}
		return ev, payload
	}

	switch s := ast.(type) {
	case *tree.CommitTransaction, *tree.RollbackTransaction, *tree.PrepareTransaction:
		if _, ok := s.(*tree.RollbackTransaction); !ok {
			// Note: Postgres replies to COMMIT and PREPARE TRANSACTION of failed
			// transactions with "ROLLBACK" too.
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
	return eventNonRetryableErr{IsCommit: fsm.False},
		eventNonRetryableErrPayload{
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
	res.SetColumns(ctx, colinfo.ShowSyntaxColumns, false /* skipRowDescription */)
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
	res.SetColumns(ctx, colinfo.ResultColumns{{Name: "TRANSACTION STATUS", Typ: types.String}}, false /* skipRowDescription */)

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
	res.SetColumns(ctx, cols, false /* skipRowDescription */)

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
	res.SetColumns(ctx, colinfo.ShowCompletionsColumns, false /* skipRowDescription */)
	log.Dev.Warningf(ctx, "COMPLETION GENERATOR FOR: %+v", *n)
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
	queryIterFn := func(ctx context.Context, opName redact.RedactableString, stmt string, args ...interface{}) (eval.InternalRows, error) {
		return ie.QueryIteratorEx(ctx, opName, txn,
			override,
			stmt, args...)
	}

	completions, err := newCompletionsGenerator(queryIterFn, n)
	if err != nil {
		log.Dev.Warningf(ctx, "COMPLETION GENERATOR FAILED: %v", err)
		return err
	}

	var hasNext bool
	for hasNext, err = completions.Next(ctx); hasNext; hasNext, err = completions.Next(ctx) {
		row := completions.Values()
		err = res.AddRow(ctx, row)
		if err != nil {
			log.Dev.Warningf(ctx, "COMPLETION ADDROW FAILED: %v", err)
			return err
		}
	}
	if err != nil {
		log.Dev.Warningf(ctx, "COMPLETION GENERATOR NEXT FAILED: %v", err)
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
	res.SetColumns(ctx, resColumns, false /* skipRowDescription */)

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
		case "compact":
			// compact modifies the output format.
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
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionEndExecTransaction, crtime.NowMono())
		transactionFingerprintID :=
			appstatspb.TransactionFingerprintID(ex.extraTxnState.transactionStatementsHash.Sum())

		err := ex.txnFingerprintIDCache.Add(ctx, transactionFingerprintID)
		if err != nil {
			if log.V(1) {
				log.Dev.Warningf(ctx, "failed to enqueue transactionFingerprintID = %d: %s", transactionFingerprintID, err)
			}
		}

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
				log.Dev.Warningf(ctx, "failed to record transaction stats: %s", err)
			}
			ex.server.ServerMetrics.StatsMetrics.DiscardedStatsCount.Inc(1)
		}

		// If we have a commitTimestamp, we should use it.
		ex.previousTransactionCommitTimestamp.Forward(ev.commitTimestamp)
	}
}

func (ex *connExecutor) onTxnRestart(ctx context.Context) {
	if ex.extraTxnState.shouldExecuteOnTxnRestart {
		ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionMostRecentStartExecTransaction, crtime.NowMono())
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
		ex.extraTxnState.kvCPUTimeNanos = 0

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
	ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionFirstStartExecTransaction, crtime.NowMono())
	ex.phaseTimes.SetSessionPhaseTime(sessionphase.SessionMostRecentStartExecTransaction,
		ex.phaseTimes.GetSessionPhaseTime(sessionphase.SessionFirstStartExecTransaction))
	ex.extraTxnState.transactionStatementsHash = util.MakeFNV64()
	ex.extraTxnState.transactionStatementFingerprintIDs = nil
	ex.extraTxnState.numRows = 0
	ex.extraTxnState.accumulatedStats = execstats.QueryLevelStats{}
	ex.extraTxnState.idleLatency = 0
	ex.extraTxnState.rowsRead = 0
	ex.extraTxnState.kvCPUTimeNanos = 0
	ex.extraTxnState.bytesRead = 0
	ex.extraTxnState.rowsWritten = 0
	ex.extraTxnState.rowsWrittenLogged = false
	ex.extraTxnState.rowsReadLogged = false

	txnExecStatsSampleRate := collectTxnStatsSampleRate.Get(&ex.server.GetExecutorConfig().Settings.SV)
	ex.extraTxnState.shouldCollectTxnExecutionStats = !ex.server.cfg.TestingKnobs.DisableProbabilisticSampling &&
		txnExecStatsSampleRate > ex.rng.internal.Float64()

	// Note ex.metrics is Server.Metrics for the connExecutor that serves the
	// client connection, and is Server.InternalMetrics for internal executors.
	ex.metrics.EngineMetrics.SQLTxnsOpen.Inc(1,
		ex.sessionData().Database, ex.sessionData().ApplicationName)

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
	txnStart crtime.Mono,
	txnErr error,
) error {
	recordingStart := crtime.NowMono()
	defer func() {
		ex.server.
			ServerMetrics.
			StatsMetrics.
			SQLTxnStatsCollectionOverhead.RecordValue(recordingStart.Elapsed().Nanoseconds())
	}()

	txnEnd := timeutil.Now()
	elapsedTime := crtime.MonoFromTime(txnEnd).Sub(txnStart)
	ex.totalActiveTimeStopWatch.Stop()

	// Note ex.metrics is Server.Metrics for the connExecutor that serves the
	// client connection, and is Server.InternalMetrics for internal executors.
	if contentionDuration := ex.extraTxnState.accumulatedStats.ContentionTime.Nanoseconds(); contentionDuration > 0 {
		ex.metrics.EngineMetrics.SQLContendedTxns.Inc(1)
	}
	ex.metrics.EngineMetrics.SQLTxnsOpen.Dec(1,
		ex.sessionData().Database, ex.sessionData().ApplicationName)
	ex.metrics.EngineMetrics.SQLTxnLatency.RecordValue(elapsedTime.Nanoseconds(),
		ex.sessionData().Database, ex.sessionData().ApplicationName)
	ex.metrics.EngineMetrics.TxnRetryCount.Inc(int64(ex.state.mu.autoRetryCounter))

	ex.txnIDCacheWriter.Record(contentionpb.ResolvedTxnID{
		TxnID:            ev.txnID,
		TxnFingerprintID: transactionFingerprintID,
	})

	if len(ex.extraTxnState.transactionStatementFingerprintIDs) == 0 {
		// If the slice of transaction statement fingerprint IDs is empty, this
		// means there is no statements that's being executed within this
		// transaction. Hence, recording stats for this transaction is not
		// meaningful.
		// TODO(#124935): Yahor thinks that this is wrong for internal executors
		// with outer txns.
		return nil
	}

	txnServiceLat := ex.phaseTimes.GetTransactionServiceLatency()
	txnRetryLat := ex.phaseTimes.GetTransactionRetryLatency()
	commitLat := ex.phaseTimes.GetCommitLatency()

	isInternaleExec := ex.executorType == executorTypeInternal

	ex.maybeRecordRetrySerializableContention(ev.txnID, transactionFingerprintID, txnErr)

	if !ex.statsCollector.EnabledForTransaction() && !ex.extraTxnState.shouldLogToTelemetry {
		// No need to create a RecordedTxnStats.
		return nil
	}

	recordedTxnStats := &sqlstats.RecordedTxnStats{
		FingerprintID:           transactionFingerprintID,
		SessionID:               ex.planner.extendedEvalCtx.SessionID,
		TransactionID:           ev.txnID,
		TransactionTimeSec:      elapsedTime.Seconds(),
		StartTime:               txnEnd.Add(-elapsedTime),
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
		KVCPUTimeNanos:          ex.extraTxnState.kvCPUTimeNanos,
		Priority:                ex.state.mu.priority,
		// TODO(107318): add isolation level
		// TODO(107318): add qos
		// TODO(107318): add asoftime or ishistorical
		// TODO(107318): add readonly
		TxnErr:           txnErr,
		Application:      ex.statsCollector.CurrentApplicationName(),
		UserNormalized:   ex.sessionData().User().Normalized(),
		InternalExecutor: isInternaleExec,
	}

	if ex.server.cfg.TestingKnobs.OnRecordTxnFinish != nil {
		ex.server.cfg.TestingKnobs.OnRecordTxnFinish(
			isInternaleExec, ex.phaseTimes, ex.planner.stmt.SQL, recordedTxnStats,
		)
	}

	if ex.extraTxnState.shouldLogToTelemetry {
		ex.planner.logTransaction(ctx,
			int(ex.extraTxnState.txnCounter.Load()),
			transactionFingerprintID,
			recordedTxnStats,
			ex.extraTxnState.telemetrySkippedTxns,
		)
	}

	ex.statsCollector.RecordTransaction(ctx, recordedTxnStats)
	return nil
}

// Records a SERIALIZATION_CONFLICT contention event to the contention registry event
// store if we have a known conflicting txn meta for a serialization conflict error.
func (ex *connExecutor) maybeRecordRetrySerializableContention(
	txnID uuid.UUID, txnFingerprintID appstatspb.TransactionFingerprintID, txnErr error,
) {
	if !contention.EnableSerializationConflictEvents.Get(&ex.server.cfg.Settings.SV) {
		return
	}

	if txnErr != nil {
		var retryErr *kvpb.TransactionRetryWithProtoRefreshError
		if errors.As(txnErr, &retryErr) && retryErr.ConflictingTxn != nil {
			contentionEvent := contentionpb.ExtendedContentionEvent{
				ContentionType: contentionpb.ContentionType_SERIALIZATION_CONFLICT,
				BlockingEvent: kvpb.ContentionEvent{
					Key:     retryErr.ConflictKey,
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
	outputJaegerJSON bool,
) {
	if r == nil {
		log.Dev.Warning(ctx, "missing trace when threshold tracing was enabled")
	}
	output := ""
	var err error
	if outputJaegerJSON {
		output, err = r.ToJaegerJSON("unknown stmt", "no comment", "unknown node", false /* indent */)
		if err != nil {
			log.Dev.Warningf(ctx, "trace could not be converted to jaeger JSON: %s", err.Error())
			output = r.String()
		}
	} else {
		output = r.String()
	}
	log.SqlExec.Infof(ctx, "%s took %s, exceeding threshold of %s:\n%s\n%s", opName, elapsed, threshold, detail, output)
}

func (ex *connExecutor) execWithProfiling(
	ctx context.Context, ast tree.Statement, prepared *prep.Statement, op func(context.Context) error,
) error {
	var err error
	if ex.server.cfg.Settings.CPUProfileType() == cluster.CPUProfileWithLabels || log.HasSpan(ctx) {
		remoteAddr := "internal"
		if rAddr := ex.sessionData().RemoteAddr; rAddr != nil {
			remoteAddr = rAddr.String()
		}
		// Compute stmtNoConstants with proper FmtFlags for consistency with
		// makeStatement and ih.Setup.
		fmtFlags := tree.FmtFlags(tree.QueryFormattingForFingerprintsMask.Get(&ex.server.cfg.Settings.SV))
		var stmtNoConstants string
		if prepared != nil {
			stmtNoConstants = prepared.StatementNoConstants
		} else {
			stmtNoConstants = tree.FormatStatementHideConstants(ast, fmtFlags)
		}
		// Compute fingerprint ID here since ih.Setup hasn't been called yet.
		fingerprintID := appstatspb.ConstructStatementFingerprintID(
			stmtNoConstants, ex.implicitTxn(), ex.sessionData().Database,
		)
		labels := make([]string, 0, 12)
		labels = append(labels,
			workloadid.ProfileTag, sqlstatsutil.EncodeStmtFingerprintIDToString(fingerprintID),
			"appname", ex.sessionData().ApplicationName,
			"addr", remoteAddr,
			"stmt.tag", ast.StatementTag(),
			"stmt.no.constants", stmtNoConstants,
		)
		if opName, ok := GetInternalOpName(ctx); ok {
			labels = append(labels, "opname", opName)
		}
		pprofutil.Do(
			ctx,
			func(ctx context.Context) {
				err = op(ctx)
			},
			labels...,
		)
	} else {
		err = op(ctx)
	}
	return err
}

// isSQLOkayToThrottle will return true if the given statement is allowed to be throttled.
func isSQLOkayToThrottle(ast tree.Statement) bool {
	switch ast.(type) {
	// We do not throttle the SET CLUSTER command, as this is how a new license
	// would be installed to disable throttling. We want to avoid the situation
	// where the action to disable throttling is itself throttled.
	case *tree.SetClusterSetting:
		return false
	default:
		return true
	}
}
