// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	plpgsqlparser "github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var defaultAutocommitBeforeDDL = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.defaults.autocommit_before_ddl.enabled",
	"default value for autocommit_before_ddl session setting; "+
		"forces transactions to autocommit before running any DDL statement",
	true,
)

// maybeAutoCommitBeforeDDL checks if the current transaction needs to be
// auto-committed before processing a DDL statement. If so, it auto-commits the
// transaction and advances the state machine so that the current command gets
// processed again.
//
// Two conditions can trigger an auto-commit:
//
//  1. The statement itself is a DDL (CanModifySchema) and either we are in an
//     explicit transaction or a prior statement has already executed; the
//     auto-commit is gated on the autocommit_before_ddl session setting.
//
//  2. The statement is a CALL whose procedure body contains DDL, the txn
//     isolation tolerates write skew, and the txn is implicit. In-place
//     isolation upgrade is unavailable in the routine body (the txn is
//     already active by the time CALL planning completes), so we
//     auto-commit and arrange for the next implicit txn to start at
//     SERIALIZABLE via forceNextTxnSerializable. This case is gated on
//     implicit txns only — explicit-txn CALLs with DDL are handled
//     elsewhere (#170019) or rejected by the in-routine safety net.
func (ex *connExecutor) maybeAutoCommitBeforeDDL(
	ctx context.Context, ast tree.Statement,
) (fsm.Event, fsm.EventPayload) {
	if ex.executorType == executorTypeInternal {
		return nil, nil
	}
	explicitTxn := !ex.implicitTxn()

	// Case 1: top-level DDL with autocommit_before_ddl enabled.
	if tree.CanModifySchema(ast) &&
		ex.sessionData().AutoCommitBeforeDDL &&
		(explicitTxn || ex.extraTxnState.firstStmtExecuted) {
		if err := ex.planner.SendClientNotice(
			ctx,
			pgnotice.Newf("auto-committing transaction before processing DDL due to autocommit_before_ddl setting"),
			false, /* immediateFlush */
		); err != nil {
			return ex.makeErrEvent(err, ast)
		}
		return ex.autoCommitForDDL(ctx, ast)
	}

	// Case 2: implicit-txn CALL whose body contains DDL under weak isolation.
	// Skip when:
	// - A prior statement in the same implicit-txn batch has already
	//   executed (mirrors Case 1's reluctance to split a multi-statement
	//   batch with an auto-commit; the in-routine safety net rejects
	//   instead).
	// - The planner is not bound to the current txn (prepare path:
	//   descriptor resolution there hits a stale planner state).
	// - There are active portals: auto-committing would invalidate them,
	//   and the extended-query protocol does not handle a portal
	//   disappearing mid-execute. Users hitting this path fall back to
	//   the in-routine safety net.
	if call, ok := ast.(*tree.Call); ok &&
		!explicitTxn &&
		!ex.extraTxnState.firstStmtExecuted &&
		ex.state.mu.txn != nil &&
		ex.planner.txn == ex.state.mu.txn &&
		!ex.extraTxnState.prepStmtsNamespace.HasActivePortals() &&
		ex.state.mu.txn.IsoLevel().ToleratesWriteSkew() {
		hasDDL, err := ex.callBodyContainsDDL(ctx, call)
		if err != nil {
			// Any error from the pre-plan walker (descriptor lookup, body
			// parse, etc.) is swallowed: the same error will be raised by
			// normal planning with proper context, and surfacing it from
			// the auto-commit hook would replace a clearer planning error
			// with one tagged at the wrong source line.
			log.VEventf(ctx, 2, "callBodyContainsDDL: %v", err)
			return nil, nil
		}
		if !hasDDL {
			return nil, nil
		}
		if err := ex.planner.SendClientNotice(
			ctx,
			pgnotice.Newf("setting transaction isolation level to SERIALIZABLE due to schema change"),
			false, /* immediateFlush */
		); err != nil {
			return ex.makeErrEvent(err, ast)
		}
		// Only set forceNextTxnSerializable after confirming the auto-commit
		// rewrote the event to eventTxnCommittedDueToDDL — meaning the
		// statement will be re-processed in a fresh txn that should pick up
		// the override. Otherwise the flag would leak into an unrelated
		// future implicit txn.
		retEv, retPayload := ex.autoCommitForDDL(ctx, ast)
		if _, ok := retEv.(eventTxnCommittedDueToDDL); ok && retPayload == nil {
			ex.forceNextTxnSerializable = true
		}
		return retEv, retPayload
	}

	return nil, nil
}

// autoCommitForDDL commits the current txn and rewrites a successful commit
// into eventTxnCommittedDueToDDL so the current statement is picked up again
// once the state machine advances.
func (ex *connExecutor) autoCommitForDDL(
	ctx context.Context, ast tree.Statement,
) (fsm.Event, fsm.EventPayload) {
	retEv, retPayload := ex.handleAutoCommit(ctx, ast)
	if _, committed := retEv.(eventTxnFinishCommitted); committed && retPayload == nil {
		retEv = eventTxnCommittedDueToDDL{}
	}
	return retEv, retPayload
}

// callBodyContainsDDL resolves the procedure named by the CALL and returns
// true if any of its overload bodies contains a DDL statement reachable
// from the top-level body. Nested CALLs inside the body are not followed;
// if such a nested CALL has DDL in its own body, the in-routine reject in
// routineGenerator.startInternal catches it at runtime.
//
// The function may activate the txn (descriptor reads); callers must be
// prepared to auto-commit. If a procedure has multiple overloads, any
// overload with DDL triggers the upgrade — we cannot identify the selected
// overload pre-planning without re-doing argument type-checking.
//
// The AST's resolved function reference is restored before returning, so
// the subsequent optbuilder pass re-resolves against the post-restart txn
// instead of inheriting a stale cached descriptor from this peek.
func (ex *connExecutor) callBodyContainsDDL(ctx context.Context, call *tree.Call) (bool, error) {
	// Resolve mutates both FunctionReference (always) and ReferenceByName
	// (in the *UnresolvedName branch). Snapshot the whole struct so the
	// post-restart re-plan re-resolves against the new txn instead of
	// inheriting cached state from this peek.
	savedRef := call.Proc.Func
	defer func() { call.Proc.Func = savedRef }()
	def, err := call.Proc.Func.Resolve(ctx, ex.planner.semaCtx.SearchPath, ex.planner.semaCtx.FunctionResolver)
	if err != nil {
		return false, err
	}
	for _, ov := range def.Overloads {
		if ov.Type != tree.ProcedureRoutine {
			continue
		}
		// Resolve typically returns a signature-only overload cached on the
		// schema descriptor. Fetch the full descriptor by OID to obtain the
		// body, mirroring the path the optbuilder takes later.
		full := ov.Overload
		if ov.UDFContainsOnlySignature || ov.Body == "" {
			_, fullOv, resolveErr := ex.planner.ResolveFunctionByOID(ctx, ov.Oid)
			if resolveErr != nil {
				return false, resolveErr
			}
			full = fullOv
		}
		if full == nil || full.Body == "" {
			continue
		}
		hasDDL, err := routineBodyContainsDDL(full.Body, full.Language)
		if err != nil {
			return false, err
		}
		if hasDDL {
			return true, nil
		}
	}
	return false, nil
}

// routineBodyContainsDDL parses the routine body and returns true if any
// SQL statement reachable from the top-level body is DDL. PL/pgSQL bodies
// are walked via plpgsqltree so that SQL statements embedded as plain
// statements inside the block (wrapped in *plpgsqltree.Execute) are
// inspected.
func routineBodyContainsDDL(body string, lang tree.RoutineLanguage) (bool, error) {
	switch lang {
	case tree.RoutineLangSQL:
		stmts, err := parser.Parse(body)
		if err != nil {
			return false, err
		}
		for _, s := range stmts {
			if tree.CanModifySchema(s.AST) {
				return true, nil
			}
		}
		return false, nil
	case tree.RoutineLangPLpgSQL:
		parsed, err := plpgsqlparser.Parse(body)
		if err != nil {
			return false, err
		}
		v := &plpgsqlDDLFinder{}
		plpgsqltree.Walk(v, parsed.AST)
		return v.found, nil
	default:
		return false, errors.AssertionFailedf("unknown routine language %q", lang)
	}
}

// plpgsqlDDLFinder is a plpgsqltree.StatementVisitor that sets found to true
// the first time it encounters a PL/pgSQL statement wrapping a DDL SQL
// statement. Note: *plpgsqltree.Execute is the AST node for any plain SQL
// statement embedded in a PL/pgSQL block (e.g. a top-level CREATE TABLE),
// not for the PL/pgSQL EXECUTE keyword (that is *plpgsqltree.DynamicExecute).
// Dynamic SQL is not inspected because the SQL string is opaque at parse
// time. Other constructs that wrap queries — CursorDeclaration, Open,
// ReturnQuery — never carry DDL, so they are not inspected.
type plpgsqlDDLFinder struct {
	found bool
}

var _ plpgsqltree.StatementVisitor = (*plpgsqlDDLFinder)(nil)

func (v *plpgsqlDDLFinder) Visit(stmt plpgsqltree.Statement) (plpgsqltree.Statement, bool) {
	if v.found {
		return stmt, false
	}
	if e, ok := stmt.(*plpgsqltree.Execute); ok && e.SqlStmt != nil {
		if tree.CanModifySchema(e.SqlStmt) {
			v.found = true
			return stmt, false
		}
	}
	return stmt, true
}

// maybeAdjustTxnForDDL checks if the statement is a schema change and adjusts
// the txn if it is. The following adjustments will be performed:
// - upgrading to serializable isolation. If the txn contains multiple
// statements, and an upgrade was attempted, an error is returned.
// - disabling buffered writes.
// TODO(#140695): we disable buffered writes out of caution. We should consider
// allowing this in the future.
func (ex *connExecutor) maybeAdjustTxnForDDL(ctx context.Context, stmt Statement) error {
	p := &ex.planner
	if tree.CanModifySchema(stmt.AST) {
		if ex.state.mu.txn.IsoLevel().ToleratesWriteSkew() {
			if !ex.extraTxnState.firstStmtExecuted {
				if err := ex.state.setIsolationLevel(isolation.Serializable); err != nil {
					return err
				}
				ex.extraTxnState.upgradedToSerializable = true
				if err := p.SendClientNotice(
					ctx,
					pgnotice.Newf("setting transaction isolation level to SERIALIZABLE due to schema change"),
					false, /* immediateFlush */
				); err != nil {
					return err
				}
			} else {
				return txnSchemaChangeErr
			}
		}
		if ex.state.mu.txn.BufferedWritesEnabled() {
			ex.state.mu.txn.SetBufferedWritesEnabled(false /* enabled */)
		}
	}
	return nil
}

// runPreCommitStages is part of the new schema changer infrastructure to
// mutate descriptors prior to committing a SQL transaction.
func (ex *connExecutor) runPreCommitStages(ctx context.Context) error {
	scs := ex.extraTxnState.schemaChangerState
	if len(scs.state.Targets) == 0 {
		return nil
	}
	deps := newSchemaChangerTxnRunDependencies(
		ctx,
		ex.planner.SessionData(),
		ex.planner.User(),
		ex.server.cfg,
		ex.planner.InternalSQLTxn(),
		ex.extraTxnState.descCollection,
		ex.planner.EvalContext(),
		ex.planner.ExtendedEvalContext().Tracing.KVTracingEnabled(),
		scs.jobID,
		scs.stmts,
	)
	ex.extraTxnState.descCollection.ResetSyntheticDescriptors()
	after, jobID, err := scrun.RunPreCommitPhase(
		ctx, ex.server.cfg.DeclarativeSchemaChangerTestingKnobs, deps, scs.state,
	)
	if err != nil {
		return err
	}
	scs.state = after
	scs.jobID = jobID
	if jobID != jobspb.InvalidJobID {
		ex.extraTxnState.jobs.addCreatedJobID(jobID)
		log.Dev.Infof(ctx, "queued new schema change job %d using the new schema changer", jobID)
	}
	return nil
}

func (ex *connExecutor) handleWaitingForConcurrentSchemaChanges(
	ctx context.Context, descID descpb.ID,
) error {
	// If we encountered a missing or dropped / offline descriptor waiting for the schema
	// change then lets ignore the error, and let the FSM retry, since concurrentSchemaChangeError
	// errors are retryable. Otherwise, allow the error to bubble back up and kill
	// the connection.
	if err := ex.planner.waitForDescriptorSchemaChanges(
		ctx, descID, *ex.extraTxnState.schemaChangerState,
	); err != nil &&
		!catalog.HasInactiveDescriptorError(err) &&
		!errors.Is(err, catalog.ErrDescriptorNotFound) {
		return err
	}
	return ex.resetTransactionOnSchemaChangeRetry(ctx)
}
