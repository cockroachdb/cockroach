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

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/asof"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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

	if cutoverTime := alterStmt.Cutover; cutoverTime != nil {
		if cutoverTime.Timestamp != nil {
			evalCtx := &p.ExtendedEvalContext().Context
			if _, err := typeCheckCutoverTime(ctx, evalCtx,
				p.SemaCtx(), cutoverTime.Timestamp); err != nil {
				return false, nil, err
			}
		}
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

	var cutoverTime hlc.Timestamp
	if alterTenantStmt.Cutover != nil {
		if !alterTenantStmt.Cutover.Latest {
			if alterTenantStmt.Cutover.Timestamp == nil {
				return nil, nil, nil, false, errors.AssertionFailedf("unexpected nil cutover expression")
			}

			evalCtx := &p.ExtendedEvalContext().Context
			ct, err := evalCutoverTime(ctx, evalCtx, p.SemaCtx(), alterTenantStmt.Cutover.Timestamp)
			if err != nil {
				return nil, nil, nil, false, err
			}
			cutoverTime = ct
		}
	}
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
		jobRegistry := p.ExecCfg().JobRegistry
		if alterTenantStmt.Cutover != nil {
			job, err := jobRegistry.LoadJobWithTxn(ctx, tenInfo.TenantReplicationJobID, p.Txn())
			if err != nil {
				return err
			}
			details, ok := job.Details().(jobspb.StreamIngestionDetails)
			if !ok {
				return errors.Newf("job with id %d is not a stream ingestion job", job.ID())
			}
			progress := job.Progress()
			if alterTenantStmt.Cutover.Latest {
				ts := progress.GetHighWater()
				if ts == nil || ts.IsEmpty() {
					return errors.Newf("replicated tenant %q has not yet recorded a safe replication time", tenantName)
				}
				cutoverTime = *ts
			}
			// TODO(ssd): We could use the replication manager here, but that embeds a priviledge check which is already completed.

			// Check that the timestamp is above our retained timestamp.
			stats, err := replicationutils.GetStreamIngestionStatsNoHeartbeat(ctx, details, progress)
			if err != nil {
				return err
			}
			if stats.IngestionDetails.ProtectedTimestampRecordID == nil {
				return errors.Newf("replicated tenant %q has not yet recorded a retained timestamp", tenantName)
			} else {
				ptp := p.ExecCfg().ProtectedTimestampProvider
				record, err := ptp.GetRecord(ctx, p.Txn(), *stats.IngestionDetails.ProtectedTimestampRecordID)
				if err != nil {
					return err
				}
				if cutoverTime.Less(record.Timestamp) {
					return errors.Newf("cutover time %s is before earliest safe cutover time %s", cutoverTime, record.Timestamp)
				}
			}
			if err := completeStreamIngestion(ctx, jobRegistry, p.Txn(), tenInfo.TenantReplicationJobID, cutoverTime); err != nil {
				return err
			}
			// Unpause the job if it is paused
			if err := jobRegistry.Unpause(ctx, p.Txn(), tenInfo.TenantReplicationJobID); err != nil {
				return err
			}
		} else {
			switch alterTenantStmt.Command {
			case tree.ResumeJob:
				if err := jobRegistry.Unpause(ctx, p.Txn(), tenInfo.TenantReplicationJobID); err != nil {
					return err
				}
			case tree.PauseJob:
				if err := jobRegistry.PauseRequested(ctx, p.Txn(), tenInfo.TenantReplicationJobID,
					"ALTER TENANT PAUSE REPLICATION"); err != nil {
					return err
				}
			default:
				return errors.New("unsupported job command in ALTER TENANT REPLICATION")
			}
		}
		resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(tenInfo.TenantReplicationJobID))}
		return nil
	}
	return fn, alterReplicationJobHeader, nil, false, nil
}

func typeCheckCutoverTime(
	ctx context.Context, evalCtx *eval.Context, semaCtx *tree.SemaContext, cutoverExpr tree.Expr,
) (tree.TypedExpr, error) {
	typedExpr, err := tree.TypeCheckAndRequire(ctx, cutoverExpr, semaCtx, types.Any, alterReplicationJobOp)
	if err != nil {
		return nil, err
	}
	// TODO(ssd): AOST and SPLIT are restricted to the use of constant expressions
	// or particular follower-read related functions. Do we want to do that here as well?
	// One nice side effect of allowing functions is that users can use NOW().

	// These are the types currently supported by asof.DatumToHLC.
	switch typedExpr.ResolvedType().Family() {
	case types.IntervalFamily, types.TimestampTZFamily, types.TimestampFamily, types.StringFamily, types.DecimalFamily, types.IntFamily:
		return typedExpr, nil
	default:
		return nil, errors.Errorf("expected string, timestamp, decimal, interval, or integer, got %s", typedExpr.ResolvedType())
	}
}

func evalCutoverTime(
	ctx context.Context, evalCtx *eval.Context, semaCtx *tree.SemaContext, cutoverExpr tree.Expr,
) (hlc.Timestamp, error) {
	typedExpr, err := typeCheckCutoverTime(ctx, evalCtx, semaCtx, cutoverExpr)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	d, err := eval.Expr(ctx, evalCtx, typedExpr)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	if d == tree.DNull {
		return hlc.MaxTimestamp, nil
	}
	stmtTimestamp := evalCtx.GetStmtTimestamp()
	return asof.DatumToHLC(evalCtx, stmtTimestamp, d, asof.ReplicationCutover)
}

func init() {
	sql.AddPlanHook("alter replication job", alterReplicationJobHook, alterReplicationJobTypeCheck)
}
