// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamproducer

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streampb"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type replicationStreamManagerImpl struct{}

// StartReplicationStream implements streaming.ReplicationStreamManager interface.
func (r *replicationStreamManagerImpl) StartReplicationStream(
	ctx context.Context, evalCtx *eval.Context, txn *kv.Txn, tenantID uint64,
) (streaming.StreamID, error) {
	return startReplicationStreamJob(ctx, evalCtx, txn, tenantID)
}

// HeartbeatReplicationStream implements streaming.ReplicationStreamManager interface.
func (r *replicationStreamManagerImpl) HeartbeatReplicationStream(
	ctx context.Context,
	evalCtx *eval.Context,
	streamID streaming.StreamID,
	frontier hlc.Timestamp,
	txn *kv.Txn,
) (streampb.StreamReplicationStatus, error) {
	return heartbeatReplicationStream(ctx, evalCtx, streamID, frontier, txn)
}

// StreamPartition implements streaming.ReplicationStreamManager interface.
func (r *replicationStreamManagerImpl) StreamPartition(
	evalCtx *eval.Context, streamID streaming.StreamID, opaqueSpec []byte,
) (eval.ValueGenerator, error) {
	return streamPartition(evalCtx, streamID, opaqueSpec)
}

// GetReplicationStreamSpec implements streaming.ReplicationStreamManager interface.
func (r *replicationStreamManagerImpl) GetReplicationStreamSpec(
	ctx context.Context, evalCtx *eval.Context, txn *kv.Txn, streamID streaming.StreamID,
) (*streampb.ReplicationStreamSpec, error) {
	return getReplicationStreamSpec(ctx, evalCtx, txn, streamID)
}

// CompleteReplicationStream implements ReplicationStreamManager interface.
func (r *replicationStreamManagerImpl) CompleteReplicationStream(
	ctx context.Context,
	evalCtx *eval.Context,
	txn *kv.Txn,
	streamID streaming.StreamID,
	successfulIngestion bool,
) error {
	return completeReplicationStream(ctx, evalCtx, txn, streamID, successfulIngestion)
}

func newReplicationStreamManagerWithPrivilegesCheck(
	ctx context.Context, evalCtx *eval.Context,
) (streaming.ReplicationStreamManager, error) {
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
		execCfg.Settings, execCfg.NodeInfo.LogicalClusterID(), execCfg.Organization(), "REPLICATION")
	if enterpriseCheckErr != nil {
		return nil, pgerror.Wrap(enterpriseCheckErr,
			pgcode.InsufficientPrivilege, "replication requires enterprise license")
	}

	return &replicationStreamManagerImpl{}, nil
}

func init() {
	streaming.GetReplicationStreamManagerHook = newReplicationStreamManagerWithPrivilegesCheck
}
