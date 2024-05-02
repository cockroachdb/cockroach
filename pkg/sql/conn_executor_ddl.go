// Copyright 2024 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// maybeAutoCommitBeforeDDL checks if the current transaction needs to be
// auto-committed before processing a DDL statement. If so, it auto-commits the
// transaction and advances the state machine so that the current command gets
// processed again.
func (ex *connExecutor) maybeAutoCommitBeforeDDL(
	ctx context.Context, ast tree.Statement,
) (fsm.Event, fsm.EventPayload) {
	if tree.CanModifySchema(ast) &&
		ex.executorType != executorTypeInternal &&
		ex.sessionData().AutoCommitBeforeDDL &&
		(!ex.planner.EvalContext().TxnIsSingleStmt || !ex.implicitTxn()) &&
		ex.extraTxnState.firstStmtExecuted {
		if err := ex.planner.SendClientNotice(
			ctx,
			pgnotice.Newf("auto-committing transaction before processing DDL due to autocommit_before_ddl setting"),
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
	if err := ex.planner.waitForDescriptorSchemaChanges(
		ctx, descID, *ex.extraTxnState.schemaChangerState,
	); err != nil {
		return err
	}
	return ex.resetTransactionOnSchemaChangeRetry(ctx)
}
