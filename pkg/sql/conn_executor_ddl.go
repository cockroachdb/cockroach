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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
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
func (ex *connExecutor) maybeAutoCommitBeforeDDL(
	ctx context.Context, ast tree.Statement,
) (fsm.Event, fsm.EventPayload) {
	// We will auto-commit if all of the following conditions are met:
	// - The query is not internal.
	// - Auto-commit before DDL is enabled.
	// - We have an explicit transaction or have executed at least one
	//   statement in the transaction. The former check ensures that we commit
	//   if we haven't executed the first statement, which can happen if only the
	//   BEGIN statement was executed.
	explicitTxn := !ex.implicitTxn()
	if ex.executorType != executorTypeInternal &&
		tree.CanModifySchema(ast) &&
		ex.sessionData().AutoCommitBeforeDDL &&
		(explicitTxn || ex.extraTxnState.firstStmtExecuted) {
		if err := ex.planner.SendClientNotice(
			ctx,
			pgnotice.Newf("auto-committing transaction before processing DDL due to autocommit_before_ddl setting"),
			false, /* immediateFlush */
		); err != nil {
			return ex.makeErrEvent(err, ast)
		}
		retEv, retPayload := ex.handleAutoCommit(ctx, ast)
		if _, committed := retEv.(eventTxnFinishCommitted); committed && retPayload == nil {
			// Use eventTxnCommittedDueToDDL so that the current statement gets
			// picked up again when the state machine advances.
			retEv = eventTxnCommittedDueToDDL{}
		}
		return retEv, retPayload
	}
	return nil, nil
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
				p.BufferClientNotice(ctx, pgnotice.Newf("setting transaction isolation level to SERIALIZABLE due to schema change"))
			} else {
				return txnSchemaChangeErr
			}
		}
		if ex.state.mu.txn.BufferedWritesEnabled() {
			ex.state.mu.txn.SetBufferedWritesEnabled(false /* enabled */)
			p.BufferClientNotice(ctx, pgnotice.Newf("disabling buffered writes on the current txn due to schema change"))
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
		log.Infof(ctx, "queued new schema change job %d using the new schema changer", jobID)
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
