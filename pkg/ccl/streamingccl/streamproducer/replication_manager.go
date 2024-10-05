// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamproducer

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/repstream"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type replicationStreamManagerImpl struct {
	evalCtx   *eval.Context
	txn       isql.Txn
	sessionID clusterunique.ID
}

// StartReplicationStream implements streaming.ReplicationStreamManager interface.
func (r *replicationStreamManagerImpl) StartReplicationStream(
	ctx context.Context, tenantName roachpb.TenantName, req streampb.ReplicationProducerRequest,
) (streampb.ReplicationProducerSpec, error) {
	if err := r.checkLicense(); err != nil {
		return streampb.ReplicationProducerSpec{}, err
	}
	return StartReplicationProducerJob(ctx, r.evalCtx, r.txn, tenantName, req, false)
}

// HeartbeatReplicationStream implements streaming.ReplicationStreamManager interface.
func (r *replicationStreamManagerImpl) HeartbeatReplicationStream(
	ctx context.Context, streamID streampb.StreamID, frontier hlc.Timestamp,
) (streampb.StreamReplicationStatus, error) {
	if err := r.checkLicense(); err != nil {
		return streampb.StreamReplicationStatus{}, err
	}
	return heartbeatReplicationStream(ctx, r.evalCtx, r.txn, streamID, frontier)
}

// StreamPartition implements streaming.ReplicationStreamManager interface.
func (r *replicationStreamManagerImpl) StreamPartition(
	streamID streampb.StreamID, opaqueSpec []byte,
) (eval.ValueGenerator, error) {
	if err := r.checkLicense(); err != nil {
		return nil, err
	}
	return streamPartition(r.evalCtx, streamID, opaqueSpec)
}

// GetReplicationStreamSpec implements streaming.ReplicationStreamManager interface.
func (r *replicationStreamManagerImpl) GetReplicationStreamSpec(
	ctx context.Context, streamID streampb.StreamID,
) (*streampb.ReplicationStreamSpec, error) {
	if err := r.checkLicense(); err != nil {
		return nil, err
	}
	return getReplicationStreamSpec(ctx, r.evalCtx, r.txn, streamID)
}

// CompleteReplicationStream implements ReplicationStreamManager interface.
func (r *replicationStreamManagerImpl) CompleteReplicationStream(
	ctx context.Context, streamID streampb.StreamID, successfulIngestion bool,
) error {
	if err := r.checkLicense(); err != nil {
		return err
	}
	return completeReplicationStream(ctx, r.evalCtx, r.txn, streamID, successfulIngestion)
}

func (r *replicationStreamManagerImpl) SetupSpanConfigsStream(
	ctx context.Context, tenantName roachpb.TenantName,
) (eval.ValueGenerator, error) {
	if err := r.checkLicense(); err != nil {
		return nil, err
	}
	return setupSpanConfigsStream(ctx, r.evalCtx, r.txn, tenantName)
}

func (r *replicationStreamManagerImpl) DebugGetProducerStatuses(
	ctx context.Context,
) []*streampb.DebugProducerStatus {
	// NB: we don't check license here since if a stream started but the license
	// expired or was removed, we still was visibility into it during debugging.

	// TODO(dt): ideally we store pointers to open readers in a map in some field
	// of some struct off of server (job registry?) so that each VC just sees the
	// ones it is running, but for now we're using a global singleton map but that
	// is not the end of the world since only the system tenant can run these so
	// as long as it is the only one that can see into the singleton we're ok.
	if !r.evalCtx.Codec.ForSystemTenant() {
		return nil
	}
	activeStreams.Lock()
	defer activeStreams.Unlock()
	res := make([]*streampb.DebugProducerStatus, 0, len(activeStreams.m))
	for _, e := range activeStreams.m {
		res = append(res, e.DebugGetProducerStatus())
	}
	return res
}

func newReplicationStreamManagerWithPrivilegesCheck(
	ctx context.Context, evalCtx *eval.Context, txn isql.Txn, sessionID clusterunique.ID,
) (eval.ReplicationStreamManager, error) {
	if err := evalCtx.SessionAccessor.CheckPrivilege(ctx,
		syntheticprivilege.GlobalPrivilegeObject,
		privilege.REPLICATION); err != nil {
		return nil, err
	}
	return &replicationStreamManagerImpl{evalCtx: evalCtx, txn: txn, sessionID: sessionID}, nil
}

func (r *replicationStreamManagerImpl) checkLicense() error {
	if err := utilccl.CheckEnterpriseEnabled(r.evalCtx.Settings, "REPLICATION"); err != nil {
		return pgerror.Wrap(err,
			pgcode.CCLValidLicenseRequired, "physical replication requires an enterprise license on the primary (and secondary) cluster")
	}
	return nil
}

func init() {
	repstream.GetReplicationStreamManagerHook = newReplicationStreamManagerWithPrivilegesCheck
}
