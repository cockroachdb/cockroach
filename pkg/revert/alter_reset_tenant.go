// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revert

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/asof"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

const (
	alterTenantResetOp = "ALTER VIRTUAL CLUSTER RESET"
)

func alterTenantResetHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, bool, error) {
	alterTenantStmt, ok := stmt.(*tree.AlterTenantReset)
	if !ok {
		return nil, nil, false, nil
	}
	if !p.ExecCfg().Codec.ForSystemTenant() {
		return nil, nil, false, pgerror.Newf(pgcode.InsufficientPrivilege, "only the system tenant can alter tenant")
	}

	timestamp, err := asof.EvalSystemTimeExpr(ctx, &p.ExtendedEvalContext().Context, p.SemaCtx(), alterTenantStmt.Timestamp,
		alterTenantResetOp, asof.ReplicationCutover)
	if err != nil {
		return nil, nil, false, err
	}

	fn := func(ctx context.Context, resultsCh chan<- tree.Datums) error {
		if err := sql.CanManageTenant(ctx, p); err != nil {
			return err
		}

		tenInfo, err := p.LookupTenantInfo(ctx, alterTenantStmt.TenantSpec, alterTenantResetOp)
		if err != nil {
			return err
		}
		return RevertTenantToTimestamp(ctx, &p.ExtendedEvalContext().Context, tenInfo.Name, timestamp, p.ExtendedEvalContext().SessionID)
	}
	return fn, nil, false, nil
}

func alterTenantResetHookTypeCheck(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (bool, colinfo.ResultColumns, error) {
	alterStmt, ok := stmt.(*tree.AlterTenantReset)
	if !ok {
		return false, nil, nil
	}
	if err := exprutil.TypeCheck(
		ctx, alterTenantResetOp, p.SemaCtx(), exprutil.TenantSpec{TenantSpec: alterStmt.TenantSpec},
	); err != nil {
		return false, nil, err
	}
	if _, err := asof.TypeCheckSystemTimeExpr(
		ctx, p.SemaCtx(), alterStmt.Timestamp, alterTenantResetOp,
	); err != nil {
		return false, nil, err
	}
	return true, nil, nil
}

func init() {
	sql.AddPlanHook("alter virtual cluster reset", alterTenantResetHook, alterTenantResetHookTypeCheck)
}
