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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type streamIngestManagerImpl struct {
	evalCtx     *eval.Context
	jobRegistry *jobs.Registry
	txn         isql.Txn
}

// CompleteStreamIngestion implements streaming.StreamIngestManager interface.
func (r *streamIngestManagerImpl) CompleteStreamIngestion(
	ctx context.Context, ingestionJobID jobspb.JobID, cutoverTimestamp hlc.Timestamp,
) error {
	return applyCutoverTime(ctx, r.jobRegistry, r.txn, ingestionJobID, cutoverTimestamp)
}

// GetStreamIngestionStats implements streaming.StreamIngestManager interface.
func (r *streamIngestManagerImpl) GetStreamIngestionStats(
	ctx context.Context,
	streamIngestionDetails jobspb.StreamIngestionDetails,
	jobProgress jobspb.Progress,
) (*streampb.StreamIngestionStats, error) {
	return replicationutils.GetStreamIngestionStats(ctx, streamIngestionDetails, jobProgress)
}

func (r *streamIngestManagerImpl) GetReplicationStatsAndStatus(
	ctx context.Context, ingestionJobID jobspb.JobID,
) (*streampb.StreamIngestionStats, string, error) {
	return getReplicationStatsAndStatus(ctx, r.jobRegistry, r.txn, ingestionJobID)
}

func newStreamIngestManagerWithPrivilegesCheck(
	ctx context.Context, evalCtx *eval.Context, txn isql.Txn,
) (eval.StreamIngestManager, error) {
	execCfg := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)
	enterpriseCheckErr := utilccl.CheckEnterpriseEnabled(
		execCfg.Settings, execCfg.NodeInfo.LogicalClusterID(), "REPLICATION")
	if enterpriseCheckErr != nil {
		return nil, pgerror.Wrap(enterpriseCheckErr,
			pgcode.InsufficientPrivilege, "replication requires enterprise license")
	}

	isAdmin, err := evalCtx.SessionAccessor.HasAdminRole(ctx)
	if err != nil {
		return nil, err
	}
	if !isAdmin {
		if err := evalCtx.SessionAccessor.CheckPrivilege(ctx,
			syntheticprivilege.GlobalPrivilegeObject,
			privilege.MANAGETENANT); err != nil {
			return nil, err
		}
	}

	return &streamIngestManagerImpl{
		evalCtx:     evalCtx,
		txn:         txn,
		jobRegistry: execCfg.JobRegistry,
	}, nil
}

// applyCutoverTime modifies the consumer job record with a cutover time and
// unpauses the job if necessary.
func applyCutoverTime(
	ctx context.Context,
	jobRegistry *jobs.Registry,
	txn isql.Txn,
	ingestionJobID jobspb.JobID,
	cutoverTimestamp hlc.Timestamp,
) error {
	log.Infof(ctx, "adding cutover time %s to job record", cutoverTimestamp)
	if err := jobRegistry.UpdateJobWithTxn(ctx, ingestionJobID, txn, false,
		func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			progress := md.Progress.GetStreamIngest()
			details := md.Payload.GetStreamIngestion()
			if progress.ReplicationStatus == jobspb.ReplicationCuttingOver {
				return errors.Newf("job %d already started cutting over to timestamp %s",
					ingestionJobID, progress.CutoverTime)
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
	return jobRegistry.Unpause(ctx, txn, ingestionJobID)
}

func getReplicationStatsAndStatus(
	ctx context.Context, jobRegistry *jobs.Registry, txn isql.Txn, ingestionJobID jobspb.JobID,
) (*streampb.StreamIngestionStats, string, error) {
	job, err := jobRegistry.LoadJobWithTxn(ctx, ingestionJobID, txn)
	if err != nil {
		return nil, jobspb.ReplicationError.String(), err
	}
	details, ok := job.Details().(jobspb.StreamIngestionDetails)
	if !ok {
		return nil, jobspb.ReplicationError.String(),
			errors.Newf("job with id %d is not a stream ingestion job", job.ID())
	}

	details.StreamAddress, err = redactSourceURI(details.StreamAddress)
	if err != nil {
		return nil, jobspb.ReplicationError.String(), err
	}

	stats, err := replicationutils.GetStreamIngestionStatsNoHeartbeat(ctx, details, job.Progress())
	if err != nil {
		return nil, jobspb.ReplicationError.String(), err
	}
	if job.Status() == jobs.StatusPaused {
		return stats, jobspb.ReplicationPaused.String(), nil
	}
	return stats, stats.IngestionProgress.ReplicationStatus.String(), nil
}

func init() {
	repstream.GetStreamIngestManagerHook = newStreamIngestManagerWithPrivilegesCheck
}
