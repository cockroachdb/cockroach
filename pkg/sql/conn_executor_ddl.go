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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
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
