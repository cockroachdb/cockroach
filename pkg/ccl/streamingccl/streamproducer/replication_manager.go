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

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
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
)

type replicationStreamManagerImpl struct {
	evalCtx *eval.Context
	txn     isql.Txn
}

// StartReplicationStream implements streaming.ReplicationStreamManager interface.
func (r *replicationStreamManagerImpl) StartReplicationStream(
	ctx context.Context, tenantName roachpb.TenantName,
) (streampb.ReplicationProducerSpec, error) {
	return startReplicationProducerJob(ctx, r.evalCtx, r.txn, tenantName)
}

// HeartbeatReplicationStream implements streaming.ReplicationStreamManager interface.
func (r *replicationStreamManagerImpl) HeartbeatReplicationStream(
	ctx context.Context, streamID streampb.StreamID, frontier hlc.Timestamp,
) (streampb.StreamReplicationStatus, error) {
	return heartbeatReplicationStream(ctx, r.evalCtx, r.txn, streamID, frontier)
}

// StreamPartition implements streaming.ReplicationStreamManager interface.
func (r *replicationStreamManagerImpl) StreamPartition(
	streamID streampb.StreamID, opaqueSpec []byte,
) (eval.ValueGenerator, error) {
	return streamPartition(r.evalCtx, streamID, opaqueSpec)
}

// GetReplicationStreamSpec implements streaming.ReplicationStreamManager interface.
func (r *replicationStreamManagerImpl) GetReplicationStreamSpec(
	ctx context.Context, streamID streampb.StreamID,
) (*streampb.ReplicationStreamSpec, error) {
	return getReplicationStreamSpec(ctx, r.evalCtx, streamID)
}

// CompleteReplicationStream implements ReplicationStreamManager interface.
func (r *replicationStreamManagerImpl) CompleteReplicationStream(
	ctx context.Context, streamID streampb.StreamID, successfulIngestion bool,
) error {
	return completeReplicationStream(ctx, r.evalCtx, r.txn, streamID, successfulIngestion)
}

func newReplicationStreamManagerWithPrivilegesCheck(
	ctx context.Context, evalCtx *eval.Context, txn isql.Txn,
) (eval.ReplicationStreamManager, error) {
	hasAdminRole, err := evalCtx.SessionAccessor.HasAdminRole(ctx)
	if err != nil {
		return nil, err
	}

	if !hasAdminRole {
		if err := evalCtx.SessionAccessor.CheckPrivilege(ctx,
			syntheticprivilege.GlobalPrivilegeObject,
			privilege.REPLICATION); err != nil {
			return nil, err
		}
	}

	execCfg := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)
	enterpriseCheckErr := utilccl.CheckEnterpriseEnabled(
		execCfg.Settings, execCfg.NodeInfo.LogicalClusterID(), "REPLICATION")
	if enterpriseCheckErr != nil {
		return nil, pgerror.Wrap(enterpriseCheckErr,
			pgcode.InsufficientPrivilege, "replication requires enterprise license")
	}

	return &replicationStreamManagerImpl{evalCtx: evalCtx, txn: txn}, nil
}

func init() {
	repstream.GetReplicationStreamManagerHook = newReplicationStreamManagerWithPrivilegesCheck
}
