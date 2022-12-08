// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

const alterReplicationJobOp = "ALTER TENANT REPLICATION"

var alterReplicationJobHeader = colinfo.ResultColumns{
	{Name: "replication_job_id", Typ: types.Int},
}

func alterReplicationJobTypeCheck(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (matched bool, header colinfo.ResultColumns, _ error) {
	alterStmt, ok := stmt.(*tree.AlterTenantReplication)
	if !ok {
		return false, nil, nil
	}
	tenantNameStrVal := paramparse.UnresolvedNameToStrVal(alterStmt.TenantName)
	if err := exprutil.TypeCheck(
		ctx, alterReplicationJobOp, p.SemaCtx(), exprutil.Strings{tenantNameStrVal},
	); err != nil {
		return false, nil, err
	}
	return true, alterReplicationJobHeader, nil
}

func alterReplicationJobHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	alterTenantStmt, ok := stmt.(*tree.AlterTenantReplication)
	if !ok {
		return nil, nil, nil, false, nil
	}

	if err := p.RequireAdminRole(ctx, "ALTER TENANT REPLICATION"); err != nil {
		return nil, nil, nil, false, err
	}

	if !p.ExecCfg().Codec.ForSystemTenant() {
		return nil, nil, nil, false, pgerror.Newf(pgcode.InsufficientPrivilege,
			"only the system tenant can alter tenant")
	}

	exprEval := p.ExprEvaluator(alterReplicationJobOp)
	tenantNameStrVal := paramparse.UnresolvedNameToStrVal(alterTenantStmt.TenantName)
	name, err := exprEval.String(ctx, tenantNameStrVal)
	if err != nil {
		return nil, nil, nil, false, err
	}
	tenantName := roachpb.TenantName(name)

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		if err := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings, p.ExecCfg().NodeInfo.LogicalClusterID(),
			"ALTER TENANT REPLICATION",
		); err != nil {
			return err
		}

		tenInfo, err := sql.GetTenantRecordByName(ctx, p.ExecCfg(), p.Txn(), tenantName)
		if err != nil {
			return err
		}
		if tenInfo.TenantReplicationJobID == 0 {
			return errors.Newf("tenant %q does not have an active replication job", tenantName)
		}
		switch alterTenantStmt.Command {
		case tree.ResumeJob:
			if err := p.ExecCfg().JobRegistry.Unpause(ctx, p.Txn(), tenInfo.TenantReplicationJobID); err != nil {
				return err
			}
		case tree.PauseJob:
			if err := p.ExecCfg().JobRegistry.PauseRequested(ctx, p.Txn(), tenInfo.TenantReplicationJobID,
				"ALTER TENANT PAUSE REPLICATION"); err != nil {
				return err
			}
		default:
			return errors.New("unsupported job command in ALTER TENANT REPLICATION")
		}
		resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(tenInfo.TenantReplicationJobID))}
		return nil
	}
	return fn, alterReplicationJobHeader, nil, false, nil
}

func init() {
	sql.AddPlanHook("alter replication job", alterReplicationJobHook, alterReplicationJobTypeCheck)
}
