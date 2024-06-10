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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/repstream"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

type replicationStreamManagerImpl struct {
	evalCtx   *eval.Context
	resolver  resolver.SchemaResolver
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

// StartReplicationStreamForTables implements streaming.ReplicationStreamManager interface.
func (r *replicationStreamManagerImpl) StartReplicationStreamForTables(
	ctx context.Context, req streampb.ReplicationProducerRequest,
) (streampb.ReplicationProducerSpec, error) {
	if err := r.checkLicense(); err != nil {
		return streampb.ReplicationProducerSpec{}, err
	}

	var replicationStartTime hlc.Timestamp
	if !req.ReplicationStartTime.IsEmpty() {
		replicationStartTime = req.ReplicationStartTime
	} else {
		replicationStartTime = hlc.Timestamp{
			WallTime: r.evalCtx.GetStmtTimestamp().UnixNano(),
		}
	}

	// Resolve table names to spans.
	//
	// TODO(ssd): Sort out the rules we want for this resolution. Right now
	// the source sends fully qualified names and we accept them. I think we
	// should probably do resolution
	spans := make([]roachpb.Span, 0, len(req.TableNames))
	tableDescs := make(map[string]descpb.TableDescriptor, len(req.TableNames))
	for _, name := range req.TableNames {
		parts := strings.SplitN(name, ".", 3)
		dbName, schemaName, tblName := parts[0], parts[1], parts[2]
		tn := tree.MakeTableNameWithSchema(tree.Name(dbName), tree.Name(schemaName), tree.Name(tblName))
		_, td, err := resolver.ResolveMutableExistingTableObject(ctx, r.resolver, &tn, true, tree.ResolveRequireTableDesc)
		if err != nil {
			return streampb.ReplicationProducerSpec{}, err
		}
		spans = append(spans, td.PrimaryIndexSpan(r.evalCtx.Codec))
		tableDescs[name] = td.TableDescriptor
	}

	execConfig := r.evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)
	registry := execConfig.JobRegistry
	ptsID := uuid.MakeV4()
	jr := makeProducerJobRecordForLogicalReplication(
		registry,
		defaultExpirationWindow,
		r.evalCtx.SessionData().User(),
		ptsID,
		spans,
		strings.Join(req.TableNames, ","))

	// TODO(ssd): Update this to protect the right set of
	// tables. Perhaps we can just protect the tables and depend
	// on something else to protect the namespace and descriptor
	// tables.
	deprecatedSpansToProtect := roachpb.Spans(spans)
	targetToProtect := ptpb.MakeClusterTarget()
	pts := jobsprotectedts.MakeRecord(ptsID, int64(jr.JobID), replicationStartTime,
		deprecatedSpansToProtect, jobsprotectedts.Jobs, targetToProtect)

	ptp := execConfig.ProtectedTimestampProvider.WithTxn(r.txn)
	if err := ptp.Protect(ctx, pts); err != nil {
		return streampb.ReplicationProducerSpec{}, err
	}

	if _, err := registry.CreateAdoptableJobWithTxn(ctx, jr, jr.JobID, r.txn); err != nil {
		return streampb.ReplicationProducerSpec{}, err
	}

	return streampb.ReplicationProducerSpec{
		StreamID:             streampb.StreamID(jr.JobID),
		SourceClusterID:      r.evalCtx.ClusterID,
		ReplicationStartTime: replicationStartTime,
		TableSpans:           spans,
		TableDescriptors:     tableDescs,
	}, nil
}

// TODO(ssd): This should probably just be an overload of
// getReplicationStreamSpec once we are re-using the producer job rather than
// the start history retention job.
func (r *replicationStreamManagerImpl) PartitionSpans(
	ctx context.Context, spans []roachpb.Span,
) (*streampb.ReplicationStreamSpec, error) {
	_, tenID, err := keys.DecodeTenantPrefix(r.evalCtx.Codec.TenantPrefix())
	if err != nil {
		return nil, err
	}
	return buildReplicationStreamSpec(ctx, r.evalCtx, tenID, false, spans)
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
	// expired or was removed, we still want visibility into it during debugging.

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

// DebugGetLogicalConsumerStatuses gets all logical consumer debug statuses in
// active in this process.
func (r *replicationStreamManagerImpl) DebugGetLogicalConsumerStatuses(
	ctx context.Context,
) []*streampb.DebugLogicalConsumerStatus {
	// NB: we don't check license here since if a stream started but the license
	// expired or was removed, we still want visibility into it during debugging.

	// TODO(dt): since this is per-process, not per-server, we can only let the
	// the sys tenant inspect it; remove this when we move this into job registry.
	if !r.evalCtx.Codec.ForSystemTenant() {
		return nil
	}
	return streampb.GetActiveLogicalConsumerStatuses()
}

func newReplicationStreamManagerWithPrivilegesCheck(
	ctx context.Context,
	evalCtx *eval.Context,
	sc resolver.SchemaResolver,
	txn isql.Txn,
	sessionID clusterunique.ID,
) (eval.ReplicationStreamManager, error) {
	if err := evalCtx.SessionAccessor.CheckPrivilege(ctx,
		syntheticprivilege.GlobalPrivilegeObject,
		privilege.REPLICATION); err != nil {
		return nil, err
	}
	return &replicationStreamManagerImpl{evalCtx: evalCtx, txn: txn, sessionID: sessionID, resolver: sc}, nil
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
