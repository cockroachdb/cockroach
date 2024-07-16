// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package producer

import (
	"context"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/repstream"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

type replicationStreamManagerImpl struct {
	evalCtx   *eval.Context
	resolver  resolver.SchemaResolver
	txn       descs.Txn
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

	execConfig := r.evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)

	if execConfig.Codec.IsSystem() && !kvserver.RangefeedEnabled.Get(&execConfig.Settings.SV) {
		return streampb.ReplicationProducerSpec{}, errors.Errorf("kv.rangefeed.enabled must be enabled on the source cluster for logical replication")
	}

	var replicationStartTime hlc.Timestamp
	if !req.ReplicationStartTime.IsEmpty() {
		replicationStartTime = req.ReplicationStartTime
	} else {
		replicationStartTime = hlc.Timestamp{
			WallTime: r.evalCtx.GetStmtTimestamp().UnixNano(),
		}
	}

	// Resolve table names to tableIDs and spans.
	spans := make([]roachpb.Span, 0, len(req.TableNames))
	tableDescs := make(map[string]descpb.TableDescriptor, len(req.TableNames))
	for _, name := range req.TableNames {
		uon, err := parser.ParseTableName(name)
		if err != nil {
			return streampb.ReplicationProducerSpec{}, err
		}
		tn := uon.ToTableName()
		_, td, err := resolver.ResolveMutableExistingTableObject(ctx, r.resolver, &tn, true, tree.ResolveRequireTableDesc)
		if err != nil {
			return streampb.ReplicationProducerSpec{}, err
		}
		spans = append(spans, td.PrimaryIndexSpan(r.evalCtx.Codec))
		tableDescs[name] = td.TableDescriptor
	}

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
		TableDescriptors:     tableDescs,
	}, nil
}

func (r *replicationStreamManagerImpl) PlanLogicalReplication(
	ctx context.Context, req streampb.LogicalReplicationPlanRequest,
) (*streampb.ReplicationStreamSpec, error) {
	_, tenID, err := keys.DecodeTenantPrefix(r.evalCtx.Codec.TenantPrefix())
	if err != nil {
		return nil, err
	}

	spans := make([]roachpb.Span, 0, len(req.TableIDs))
	tableDescs := make([]descpb.TableDescriptor, 0, len(req.TableIDs))
	for _, requestedTableID := range req.TableIDs {
		td, err := r.txn.Descriptors().MutableByID(r.txn.KV()).Table(ctx, descpb.ID(requestedTableID))
		if err != nil {
			return nil, err
		}
		spans = append(spans, td.PrimaryIndexSpan(r.evalCtx.Codec))
		tableDescs = append(tableDescs, td.TableDescriptor)
	}
	spec, err := buildReplicationStreamSpec(ctx, r.evalCtx, tenID, false, spans)
	if err != nil {
		return nil, err
	}
	spec.TableDescriptors = tableDescs
	spec.TableSpans = spans
	return spec, nil
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
	ctx context.Context, streamID streampb.StreamID, opaqueSpec []byte,
) (eval.ValueGenerator, error) {
	if err := r.checkLicense(); err != nil {
		return nil, err
	}
	// We release the descriptor collection state because stream_partitions
	// runs forever.
	r.txn.Descriptors().ReleaseAll(ctx)

	// We also commit the planner's transaction. Nothing should be using it after
	// this point and the stream itself is non-transactional.
	if err := r.txn.KV().Commit(ctx); err != nil {
		return nil, err
	}
	return streamPartition(r.evalCtx, streamID, opaqueSpec)
}

// GetPhysicalReplicationStreamSpec implements streaming.ReplicationStreamManager interface.
func (r *replicationStreamManagerImpl) GetPhysicalReplicationStreamSpec(
	ctx context.Context, streamID streampb.StreamID,
) (*streampb.ReplicationStreamSpec, error) {
	if err := r.checkLicense(); err != nil {
		return nil, err
	}
	return getPhysicalReplicationStreamSpec(ctx, r.evalCtx, r.txn, streamID)
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

	res := streampb.GetActiveProducerStatuses()
	sort.Slice(res, func(i, j int) bool {
		return res[i].Spec.ConsumerNode < res[j].Spec.ConsumerNode ||
			(res[i].Spec.ConsumerNode == res[j].Spec.ConsumerNode && res[i].Spec.ConsumerProc < res[j].Spec.ConsumerProc)
	})
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

	res := streampb.GetActiveLogicalConsumerStatuses()

	sort.Slice(res, func(i, j int) bool {
		return res[i].ProcessorID < res[j].ProcessorID
	})

	return res
}

func newReplicationStreamManagerWithPrivilegesCheck(
	ctx context.Context,
	evalCtx *eval.Context,
	sc resolver.SchemaResolver,
	txn descs.Txn,
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
