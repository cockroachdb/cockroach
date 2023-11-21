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
	"math"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/asof"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

const (
	alterReplicationJobOp = "ALTER VIRTUAL CLUSTER REPLICATION"
	createReplicationOp   = "CREATE VIRTUAL CLUSTER FROM REPLICATION"
)

var alterReplicationCutoverHeader = colinfo.ResultColumns{
	{Name: "cutover_time", Typ: types.Decimal},
}

// ResolvedTenantReplicationOptions represents options from an
// evaluated CREATE VIRTUAL CLUSTER FROM REPLICATION command.
type resolvedTenantReplicationOptions struct {
	resumeTimestamp hlc.Timestamp
	retention       *int32
}

func evalTenantReplicationOptions(
	ctx context.Context,
	options tree.TenantReplicationOptions,
	eval exprutil.Evaluator,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	op string,
) (*resolvedTenantReplicationOptions, error) {
	r := &resolvedTenantReplicationOptions{}
	if options.Retention != nil {
		dur, err := eval.Duration(ctx, options.Retention)
		if err != nil {
			return nil, err
		}
		retSeconds64, ok := dur.AsInt64()
		if !ok {
			return nil, errors.Newf("interval conversion error: %v", dur)
		}
		if retSeconds64 > math.MaxInt32 || retSeconds64 < 0 {
			return nil, errors.Newf("retention should result in a number of seconds between 0 and %d",
				math.MaxInt32)
		}
		retSeconds := int32(retSeconds64)
		r.retention = &retSeconds
	}
	if options.ResumeTimestamp != nil {
		ts, err := asof.EvalSystemTimeExpr(ctx, evalCtx, semaCtx, options.ResumeTimestamp, op, asof.ReplicationCutover)
		if err != nil {
			return nil, err
		}
		r.resumeTimestamp = ts
	}

	return r, nil
}

func (r *resolvedTenantReplicationOptions) GetRetention() (int32, bool) {
	if r == nil || r.retention == nil {
		return 0, false
	}
	return *r.retention, true
}

func alterReplicationJobTypeCheck(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (matched bool, header colinfo.ResultColumns, _ error) {
	alterStmt, ok := stmt.(*tree.AlterTenantReplication)
	if !ok {
		return false, nil, nil
	}
	if err := exprutil.TypeCheck(
		ctx, alterReplicationJobOp, p.SemaCtx(),
		exprutil.TenantSpec{TenantSpec: alterStmt.TenantSpec},
		exprutil.Strings{alterStmt.Options.Retention},
	); err != nil {
		return false, nil, err
	}
	if alterStmt.Options.ResumeTimestamp != nil {
		if _, err := asof.TypeCheckSystemTimeExpr(ctx, p.SemaCtx(),
			alterStmt.Options.ResumeTimestamp, alterReplicationJobOp); err != nil {
			return false, nil, err
		}
	}

	if cutoverTime := alterStmt.Cutover; cutoverTime != nil {
		if cutoverTime.Timestamp != nil {
			if _, err := asof.TypeCheckSystemTimeExpr(ctx, p.SemaCtx(),
				cutoverTime.Timestamp, alterReplicationJobOp); err != nil {
				return false, nil, err
			}
		}
		return true, alterReplicationCutoverHeader, nil
	}

	return true, nil, nil
}

var physicalReplicationDisabledErr = errors.WithTelemetry(
	pgerror.WithCandidateCode(
		errors.WithHint(
			errors.Newf("physical replication is disabled"),
			"You can enable physical replication by running `SET CLUSTER SETTING physical_replication.enabled = true`.",
		),
		pgcode.ExperimentalFeature,
	),
	"physical_replication.enabled")

func alterReplicationJobHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	alterTenantStmt, ok := stmt.(*tree.AlterTenantReplication)
	if !ok {
		return nil, nil, nil, false, nil
	}

	if !streamingccl.CrossClusterReplicationEnabled.Get(&p.ExecCfg().Settings.SV) {
		return nil, nil, nil, false, physicalReplicationDisabledErr
	}

	if !p.ExecCfg().Codec.ForSystemTenant() {
		return nil, nil, nil, false, pgerror.Newf(pgcode.InsufficientPrivilege,
			"only the system tenant can alter tenant")
	}

	if alterTenantStmt.Options.ResumeTimestamp != nil {
		return nil, nil, nil, false, pgerror.New(pgcode.InvalidParameterValue, "resume timestamp cannot be altered")
	}

	evalCtx := &p.ExtendedEvalContext().Context
	var cutoverTime hlc.Timestamp
	if alterTenantStmt.Cutover != nil {
		if !alterTenantStmt.Cutover.Latest {
			if alterTenantStmt.Cutover.Timestamp == nil {
				return nil, nil, nil, false, errors.AssertionFailedf("unexpected nil cutover expression")
			}

			ct, err := asof.EvalSystemTimeExpr(ctx, evalCtx, p.SemaCtx(), alterTenantStmt.Cutover.Timestamp,
				alterReplicationJobOp, asof.ReplicationCutover)
			if err != nil {
				return nil, nil, nil, false, err
			}
			cutoverTime = ct
		}
	}

	exprEval := p.ExprEvaluator(alterReplicationJobOp)
	options, err := evalTenantReplicationOptions(ctx, alterTenantStmt.Options, exprEval, evalCtx, p.SemaCtx(), alterReplicationJobOp)
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		if err := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings, p.ExecCfg().NodeInfo.LogicalClusterID(),
			alterReplicationJobOp,
		); err != nil {
			return err
		}

		if err := sql.CanManageTenant(ctx, p); err != nil {
			return err
		}

		tenInfo, err := p.LookupTenantInfo(ctx, alterTenantStmt.TenantSpec, alterReplicationJobOp)
		if err != nil {
			return err
		}
		if tenInfo.PhysicalReplicationConsumerJobID == 0 {
			return errors.Newf("tenant %q (%d) does not have an active replication job",
				tenInfo.Name, tenInfo.ID)
		}
		jobRegistry := p.ExecCfg().JobRegistry
		if alterTenantStmt.Cutover != nil {
			pts := p.ExecCfg().ProtectedTimestampProvider.WithTxn(p.InternalSQLTxn())
			actualCutoverTime, err := alterTenantJobCutover(
				ctx, p.InternalSQLTxn(), jobRegistry, pts, alterTenantStmt, tenInfo, cutoverTime)
			if err != nil {
				return err
			}
			resultsCh <- tree.Datums{eval.TimestampToDecimalDatum(actualCutoverTime)}
		} else if !alterTenantStmt.Options.IsDefault() {
			if err := alterTenantOptions(ctx, p.InternalSQLTxn(), jobRegistry, options, tenInfo); err != nil {
				return err
			}
		} else {
			switch alterTenantStmt.Command {
			case tree.ResumeJob:
				if err := jobRegistry.Unpause(ctx, p.InternalSQLTxn(), tenInfo.PhysicalReplicationConsumerJobID); err != nil {
					return err
				}
			case tree.PauseJob:
				if err := jobRegistry.PauseRequested(ctx, p.InternalSQLTxn(), tenInfo.PhysicalReplicationConsumerJobID,
					"ALTER VIRTUAL CLUSTER PAUSE REPLICATION"); err != nil {
					return err
				}
			default:
				return errors.New("unsupported job command in ALTER VIRTUAL CLUSTER REPLICATION")
			}
		}
		return nil
	}
	if alterTenantStmt.Cutover != nil {
		return fn, alterReplicationCutoverHeader, nil, false, nil
	}
	return fn, nil, nil, false, nil
}

// alterTenantJobCutover returns the cutover timestamp that was used to initiate
// the cutover process - if the command is 'ALTER VIRTUAL CLUSTER .. COMPLETE REPLICATION
// TO LATEST' then the frontier high water timestamp is used.
func alterTenantJobCutover(
	ctx context.Context,
	txn isql.Txn,
	jobRegistry *jobs.Registry,
	ptp protectedts.Storage,
	alterTenantStmt *tree.AlterTenantReplication,
	tenInfo *mtinfopb.TenantInfo,
	cutoverTime hlc.Timestamp,
) (hlc.Timestamp, error) {
	if alterTenantStmt == nil || alterTenantStmt.Cutover == nil {
		return hlc.Timestamp{}, errors.AssertionFailedf("unexpected nil ALTER VIRTUAL CLUSTER cutover expression")
	}

	tenantName := tenInfo.Name
	job, err := jobRegistry.LoadJobWithTxn(ctx, tenInfo.PhysicalReplicationConsumerJobID, txn)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	details, ok := job.Details().(jobspb.StreamIngestionDetails)
	if !ok {
		return hlc.Timestamp{}, errors.Newf("job with id %d is not a stream ingestion job", job.ID())
	}
	progress := job.Progress()

	if alterTenantStmt.Cutover.Latest {
		replicatedTime := replicationutils.ReplicatedTimeFromProgress(&progress)
		if replicatedTime.IsEmpty() {
			return hlc.Timestamp{},
				errors.Newf("replicated tenant %q has not yet recorded a safe replication time", tenantName)
		}
		cutoverTime = replicatedTime
	}

	// TODO(ssd): We could use the replication manager here, but
	// that embeds a priviledge check which is already completed.
	//
	// Check that the timestamp is above our retained timestamp.
	stats, err := replicationutils.GetStreamIngestionStats(ctx, details, progress)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	if stats.IngestionDetails.ProtectedTimestampRecordID == nil {
		return hlc.Timestamp{}, errors.Newf("replicated tenant %q (%d) has not yet recorded a retained timestamp",
			tenantName, tenInfo.ID)
	} else {
		record, err := ptp.GetRecord(ctx, *stats.IngestionDetails.ProtectedTimestampRecordID)
		if err != nil {
			return hlc.Timestamp{}, err
		}
		if cutoverTime.Less(record.Timestamp) {
			return hlc.Timestamp{}, errors.Newf("cutover time %s is before earliest safe cutover time %s",
				cutoverTime, record.Timestamp)
		}
	}
	if err := applyCutoverTime(ctx, job, txn, cutoverTime); err != nil {
		return hlc.Timestamp{}, err
	}

	return cutoverTime, nil
}

// applyCutoverTime modifies the consumer job record with a cutover time and
// unpauses the job if necessary.
func applyCutoverTime(
	ctx context.Context, job *jobs.Job, txn isql.Txn, cutoverTimestamp hlc.Timestamp,
) error {
	log.Infof(ctx, "adding cutover time %s to job record", cutoverTimestamp)
	if err := job.WithTxn(txn).Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		progress := md.Progress.GetStreamIngest()
		details := md.Payload.GetStreamIngestion()
		if progress.ReplicationStatus == jobspb.ReplicationCuttingOver {
			return errors.Newf("job %d already started cutting over to timestamp %s",
				job.ID(), progress.CutoverTime)
		}

		progress.ReplicationStatus = jobspb.ReplicationPendingCutover
		// Update the sentinel being polled by the stream ingestion job to
		// check if a complete has been signaled.
		progress.CutoverTime = cutoverTimestamp
		progress.RemainingCutoverSpans = roachpb.Spans{details.Span}
		ju.UpdateProgress(md.Progress)
		return nil
	}); err != nil {
		return err
	}
	// Unpause the job if it is paused.
	return job.WithTxn(txn).Unpaused(ctx)
}

func alterTenantOptions(
	ctx context.Context,
	txn isql.Txn,
	jobRegistry *jobs.Registry,
	options *resolvedTenantReplicationOptions,
	tenInfo *mtinfopb.TenantInfo,
) error {
	return jobRegistry.UpdateJobWithTxn(ctx, tenInfo.PhysicalReplicationConsumerJobID, txn, false, /* useReadLock */
		func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			streamIngestionDetails := md.Payload.GetStreamIngestion()
			if ret, ok := options.GetRetention(); ok {
				streamIngestionDetails.ReplicationTTLSeconds = ret
			}
			ju.UpdatePayload(md.Payload)
			return nil
		})

}

func init() {
	sql.AddPlanHook("alter replication job", alterReplicationJobHook, alterReplicationJobTypeCheck)
}
