// Copyright 2022 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
)

// handleCommitWithCausalityToken deals with the special statement
// SELECT commit_with_causality_token while in the OpenState of an
// explicit transaction.
func (ex *connExecutor) handleCommitWithCausalityToken(
	ctx context.Context, res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload, error) {
	res.ResetStmtType((*tree.CommitTransaction)(nil))
	err := ex.commitSQLTransactionInternal(ctx)
	if err == nil {

		res.SetColumns(ctx, colinfo.ResultColumns{
			{
				Name:   "causality_token",
				Typ:    types.Decimal,
				Hidden: false,
			},
		})
		if err := res.AddRow(ctx, tree.Datums{
			eval.TimestampToDecimalDatum(ex.planner.Txn().CommitTimestamp()),
		}); err != nil {
			return nil, nil, err
		}

		// If we have a SAVEPOINT cockroach_restart, then we need to note that
		// fact now, as the SAVEPOINT stack will be destroyed as the state
		// machine moves into COMMIT. This state in extraTxnState will be cleaned
		// up as we process any statement in CommitWait.
		if entry, _ := ex.extraTxnState.savepoints.find(
			commitOnReleaseSavepointName,
		); entry != nil && entry.commitOnRelease {
			ex.extraTxnState.shouldAcceptReleaseSavepointCockroachRestart = true
		}

		return eventTxnCommittedWithCausalityToken{}, nil, nil
	}

	// Committing the transaction failed. We'll go to state RestartWait if
	// it's a retriable error, or to state RollbackWait otherwise.
	if errIsRetriable(err) {
		rc, canAutoRetry := ex.getRewindTxnCapability()
		ev := eventRetriableErr{
			IsCommit:     fsm.FromBool(false /* isCommit */),
			CanAutoRetry: fsm.FromBool(canAutoRetry),
		}
		payload := eventRetriableErrPayload{err: err, rewCap: rc}
		return ev, payload, nil
	}

	ev := eventNonRetriableErr{IsCommit: fsm.FromBool(false)}
	payload := eventNonRetriableErrPayload{err: err}
	return ev, payload, nil
}

// isSelectWithCausalityToken returns true if the statement is exactly the
// following, modulo capitalization:
//
//   SELECT crdb_internal.commit_with_causality_token
//
func isSelectCommitWithCausalityToken(ast tree.Statement) bool {
	sel, ok := ast.(*tree.Select)
	if !ok {
		return false
	}
	selStmt := sel.Select
	if sel.With != nil || sel.Locking != nil || sel.Limit != nil || sel.OrderBy != nil {
		return false
	}
	// We intentionally don't open up ParenSelect clauses.
	sc, ok := selStmt.(*tree.SelectClause)
	if !ok {
		return false
	}
	// TODO(ajwerner): Find a more exhaustive way to do this.
	if len(sc.From.Tables) != 0 || len(sc.Exprs) != 1 || sc.Distinct ||
		sc.Where != nil || sc.GroupBy != nil || sc.Having != nil || sc.Window != nil ||
		sc.From.AsOf.Expr != nil {
		return false
	}
	funcExpr, isFuncExpr := sc.Exprs[0].Expr.(*tree.FuncExpr)
	if !isFuncExpr || len(funcExpr.Exprs) != 0 {
		return false
	}
	name, isName := funcExpr.Func.FunctionReference.(*tree.UnresolvedName)
	if !isName || name.NumParts != 2 ||
		name.Parts[1] != catconstants.CRDBInternalSchemaName ||
		name.Parts[0] != builtins.CommitWithCausalityTokenName {
		return false
	}
	return true
}
