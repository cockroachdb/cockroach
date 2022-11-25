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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/repstream"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type streamIngestManagerImpl struct {
	evalCtx *eval.Context
	txn     *kv.Txn
}

// CompleteStreamIngestion implements streaming.StreamIngestManager interface.
func (r *streamIngestManagerImpl) CompleteStreamIngestion(
	ctx context.Context, ingestionJobID jobspb.JobID, cutoverTimestamp hlc.Timestamp,
) error {
	jobRegistry := r.evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig).JobRegistry
	return completeStreamIngestion(ctx, jobRegistry, r.txn, ingestionJobID, cutoverTimestamp)
}

// GetStreamIngestionStats implements streaming.StreamIngestManager interface.
func (r *streamIngestManagerImpl) GetStreamIngestionStats(
	ctx context.Context,
	streamIngestionDetails jobspb.StreamIngestionDetails,
	jobProgress jobspb.Progress,
) (*streampb.StreamIngestionStats, error) {
	return replicationutils.GetStreamIngestionStats(ctx, streamIngestionDetails, jobProgress)
}

func newStreamIngestManagerWithPrivilegesCheck(
	ctx context.Context, evalCtx *eval.Context, txn *kv.Txn,
) (eval.StreamIngestManager, error) {
	isAdmin, err := evalCtx.SessionAccessor.HasAdminRole(ctx)
	if err != nil {
		return nil, err
	}

	if !isAdmin {
		return nil,
			pgerror.New(pgcode.InsufficientPrivilege, "replication restricted to ADMIN role")
	}

	execCfg := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)
	enterpriseCheckErr := utilccl.CheckEnterpriseEnabled(
		execCfg.Settings, execCfg.NodeInfo.LogicalClusterID(), "REPLICATION")
	if enterpriseCheckErr != nil {
		return nil, pgerror.Wrap(enterpriseCheckErr,
			pgcode.InsufficientPrivilege, "replication requires enterprise license")
	}

	return &streamIngestManagerImpl{evalCtx: evalCtx, txn: txn}, nil
}

func init() {
	repstream.GetStreamIngestManagerHook = newStreamIngestManagerWithPrivilegesCheck
}
