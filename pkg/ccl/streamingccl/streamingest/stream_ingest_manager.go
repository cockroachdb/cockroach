// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamingest

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/revertccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type streamIngestManagerImpl struct {
	evalCtx     *eval.Context
	jobRegistry *jobs.Registry
	txn         isql.Txn
	sessionID   clusterunique.ID
}

// GetReplicationStatsAndStatus implements streaming.StreamIngestManager interface.
func (r *streamIngestManagerImpl) GetReplicationStatsAndStatus(
	ctx context.Context, ingestionJobID jobspb.JobID,
) (*streampb.StreamIngestionStats, string, error) {
	return getReplicationStatsAndStatus(ctx, r.jobRegistry, r.txn, ingestionJobID)
}

// RevertTenantToTimestamp  implements streaming.StreamIngestManager interface.
func (r *streamIngestManagerImpl) RevertTenantToTimestamp(
	ctx context.Context, tenantName roachpb.TenantName, revertTo hlc.Timestamp,
) error {
	return revertccl.RevertTenantToTimestamp(ctx, r.evalCtx, tenantName, revertTo, r.sessionID)
}

func newStreamIngestManagerWithPrivilegesCheck(
	ctx context.Context, evalCtx *eval.Context, txn isql.Txn, sessionID clusterunique.ID,
) (eval.StreamIngestManager, error) {
	execCfg := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)
	enterpriseCheckErr := utilccl.CheckEnterpriseEnabled(
		execCfg.Settings, "REPLICATION")
	if enterpriseCheckErr != nil {
		return nil, pgerror.Wrap(enterpriseCheckErr,
			pgcode.CCLValidLicenseRequired, "physical replication requires an enterprise license on the secondary (and primary) cluster")
	}

	if err := evalCtx.SessionAccessor.CheckPrivilege(ctx,
		syntheticprivilege.GlobalPrivilegeObject,
		privilege.MANAGEVIRTUALCLUSTER); err != nil {
		return nil, err
	}

	return &streamIngestManagerImpl{
		evalCtx:     evalCtx,
		txn:         txn,
		jobRegistry: execCfg.JobRegistry,
		sessionID:   sessionID,
	}, nil
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

	details.StreamAddress, err = streamclient.RedactSourceURI(details.StreamAddress)
	if err != nil {
		return nil, jobspb.ReplicationError.String(), err
	}

	stats, err := replicationutils.GetStreamIngestionStats(ctx, details, job.Progress())
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
