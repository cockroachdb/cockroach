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
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvfollowerreadsccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// defaultExpirationWindowSeconds the default producer job expiration without a heartbeat is 1 day
//
// TODO(msbutler): for the post cutover dummy producer job, the default
// expiration window will be 24 hours.
const defaultExpirationWindow = time.Hour * 24

// notAReplicationJobError returns an error that is returned anytime
// the user passes a job ID not related to a replication stream job.
func notAReplicationJobError(id jobspb.JobID) error {
	return pgerror.Newf(pgcode.InvalidParameterValue, "job %d is not a replication stream job", id)
}

// jobIsNotRunningError returns an error that is returned by
// operations that require a running producer side job.
func jobIsNotRunningError(id jobspb.JobID, status jobs.Status, op string) error {
	return pgerror.Newf(pgcode.InvalidParameterValue, "replication job %d must be running (is %s) to %s",
		id, status, op,
	)
}

// StartReplicationProducerJob initializes a replication stream producer job on
// the source cluster that:
//
// 1. Tracks the liveness of the replication stream consumption.
// 2. Updates the protected timestamp for spans being replicated.
func StartReplicationProducerJob(
	ctx context.Context,
	evalCtx *eval.Context,
	txn isql.Txn,
	tenantName roachpb.TenantName,
	req streampb.ReplicationProducerRequest,
) (streampb.ReplicationProducerSpec, error) {
	execConfig := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)

	tenantRecord, err := sql.GetTenantRecordByName(ctx, evalCtx.Settings, txn, tenantName)
	if err != nil {
		return streampb.ReplicationProducerSpec{}, err
	}
	tenantID, err := roachpb.MakeTenantID(tenantRecord.ID)
	if err != nil {
		return streampb.ReplicationProducerSpec{}, err
	}

	if tenantID.IsSystem() && !kvserver.RangefeedEnabled.Get(&evalCtx.Settings.SV) {
		return streampb.ReplicationProducerSpec{}, errors.Errorf("kv.rangefeed.enabled must be true to start a replication job")
	}

	var replicationStartTime hlc.Timestamp
	if !req.ReplicationStartTime.IsEmpty() {
		if tenantRecord.PreviousSourceTenant != nil {
			cid := tenantRecord.PreviousSourceTenant.ClusterID
			if !req.ClusterID.Equal(uuid.UUID{}) && !cid.Equal(uuid.UUID{}) {
				if !req.ClusterID.Equal(cid) {
					return streampb.ReplicationProducerSpec{}, errors.Errorf("requesting cluster ID %s does not match previous source cluster ID %s",
						req.ClusterID, cid)
				}
			}

			tid := tenantRecord.PreviousSourceTenant.TenantID
			if !req.TenantID.Equal(roachpb.TenantID{}) && !tid.Equal(roachpb.TenantID{}) {
				if !req.TenantID.Equal(tid) {
					return streampb.ReplicationProducerSpec{}, errors.Errorf("requesting tenant ID %s does not match previous source tenant ID %s",
						req.TenantID, tid)
				}
			}
		}
		replicationStartTime = req.ReplicationStartTime
	} else {
		replicationStartTime = hlc.Timestamp{
			WallTime: evalCtx.GetStmtTimestamp().UnixNano(),
		}
	}

	registry := execConfig.JobRegistry
	ptsID := uuid.MakeV4()

	jr := makeProducerJobRecord(registry, tenantRecord, defaultExpirationWindow, evalCtx.SessionData().User(), ptsID)
	if _, err := registry.CreateAdoptableJobWithTxn(ctx, jr, jr.JobID, txn); err != nil {
		return streampb.ReplicationProducerSpec{}, err
	}

	tenantRecord.PhysicalReplicationProducerJobIDs = append(tenantRecord.PhysicalReplicationProducerJobIDs, jr.JobID)
	if err := sql.UpdateTenantRecord(ctx, evalCtx.Settings, txn, tenantRecord); err != nil {
		return streampb.ReplicationProducerSpec{}, err
	}

	ptp := execConfig.ProtectedTimestampProvider.WithTxn(txn)
	deprecatedSpansToProtect := roachpb.Spans{keys.MakeTenantSpan(tenantID)}
	targetToProtect := ptpb.MakeTenantsTarget([]roachpb.TenantID{tenantID})
	pts := jobsprotectedts.MakeRecord(ptsID, int64(jr.JobID), replicationStartTime,
		deprecatedSpansToProtect, jobsprotectedts.Jobs, targetToProtect)

	if err := ptp.Protect(ctx, pts); err != nil {
		return streampb.ReplicationProducerSpec{}, err
	}
	if req.TenantID.Equal(roachpb.TenantID{}) && req.ClusterID.Equal(uuid.UUID{}) {
		log.Infof(ctx, "started post cutover producer job %d", jr.JobID)
	}

	return streampb.ReplicationProducerSpec{
		StreamID:             streampb.StreamID(jr.JobID),
		SourceTenantID:       tenantID,
		SourceClusterID:      evalCtx.ClusterID,
		ReplicationStartTime: replicationStartTime,
	}, nil
}

// Convert the producer job's status into corresponding replication
// stream status.
func convertProducerJobStatusToStreamStatus(
	jobStatus jobs.Status,
) streampb.StreamReplicationStatus_StreamStatus {
	switch {
	case jobStatus == jobs.StatusRunning:
		return streampb.StreamReplicationStatus_STREAM_ACTIVE
	case jobStatus == jobs.StatusPaused:
		return streampb.StreamReplicationStatus_STREAM_PAUSED
	case jobStatus.Terminal():
		return streampb.StreamReplicationStatus_STREAM_INACTIVE
	default:
		// This means the producer job is in transient state, the call site
		// has to retry until other states are reached.
		return streampb.StreamReplicationStatus_UNKNOWN_STREAM_STATUS_RETRY
	}
}

// updateReplicationStreamProgress updates the job progress for an active replication
// stream specified by 'streamID'.
func updateReplicationStreamProgress(
	ctx context.Context,
	updateBegin time.Time,
	ptsProvider protectedts.Manager,
	registry *jobs.Registry,
	streamID streampb.StreamID,
	consumedTime hlc.Timestamp,
	txn isql.Txn,
) (status streampb.StreamReplicationStatus, err error) {
	updateJob := func() (streampb.StreamReplicationStatus, error) {
		j, err := registry.LoadJobWithTxn(ctx, jobspb.JobID(streamID), txn)
		if err != nil {
			return status, err
		}
		details, ok := j.Details().(jobspb.StreamReplicationDetails)
		if !ok {
			return status, notAReplicationJobError(jobspb.JobID(streamID))
		}
		expiration := updateBegin.Add(details.ExpirationWindow)
		if err := j.WithTxn(txn).Update(ctx, func(
			txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
		) error {
			status = streampb.StreamReplicationStatus{}
			pts := ptsProvider.WithTxn(txn)
			status.StreamStatus = convertProducerJobStatusToStreamStatus(md.Status)
			// Skip checking PTS record in cases that it might already be released
			if status.StreamStatus != streampb.StreamReplicationStatus_STREAM_ACTIVE &&
				status.StreamStatus != streampb.StreamReplicationStatus_STREAM_PAUSED {
				return nil
			}

			ptsID := md.Payload.GetStreamReplication().ProtectedTimestampRecordID
			ptsRecord, err := pts.GetRecord(ctx, ptsID)
			if err != nil {
				return err
			}
			status.ProtectedTimestamp = &ptsRecord.Timestamp

			if status.StreamStatus != streampb.StreamReplicationStatus_STREAM_ACTIVE {
				return nil
			}

			// TODO(casper): Error out when the protected timestamp moves backward as the ingestion
			// processors may consume kv changes that are not protected. We are fine for now
			// for the sake of long GC window.
			// Now this can happen because the frontier processor moves forward the protected timestamp
			// in the source cluster through heartbeats before it reports the new frontier to the
			// ingestion job resumer which later updates the job high watermark. When we retry another
			// ingestion using the previous ingestion high watermark, it can fall behind the
			// source cluster protected timestamp.
			if shouldUpdatePTS := ptsRecord.Timestamp.Less(consumedTime); shouldUpdatePTS {
				if err = pts.UpdateTimestamp(ctx, ptsID, consumedTime); err != nil {
					return err
				}
				status.ProtectedTimestamp = &consumedTime
			}
			// Allow expiration time to go backwards as user may set a smaller timeout.
			md.Progress.GetStreamReplication().Expiration = expiration
			ju.UpdateProgress(md.Progress)
			return nil
		}); err != nil {
			return streampb.StreamReplicationStatus{}, err
		}
		return status, nil
	}

	status, err = updateJob()
	if jobs.HasJobNotFoundError(err) || testutils.IsError(err, "not found in system.jobs table") {
		status.StreamStatus = streampb.StreamReplicationStatus_STREAM_INACTIVE
		err = nil
	}
	return status, err
}

// heartbeatReplicationStream updates replication stream progress and advances protected timestamp
// record to the specified frontier.
func heartbeatReplicationStream(
	ctx context.Context,
	evalCtx *eval.Context,
	txn isql.Txn,
	streamID streampb.StreamID,
	frontier hlc.Timestamp,
) (streampb.StreamReplicationStatus, error) {
	execConfig := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)
	if frontier == hlc.MaxTimestamp {
		// NB: We used to allow this as a no-op update to get
		// the status. That code was removed.
		return streampb.StreamReplicationStatus{}, pgerror.Newf(pgcode.InvalidParameterValue, "MaxTimestamp no longer accepted as frontier")
	}
	updateBegin := timeutil.Now()
	return updateReplicationStreamProgress(ctx, updateBegin, execConfig.ProtectedTimestampProvider, execConfig.JobRegistry,
		streamID, frontier, txn)
}

// getReplicationStreamSpec gets a replication stream specification for the specified stream.
func getReplicationStreamSpec(
	ctx context.Context, evalCtx *eval.Context, txn isql.Txn, streamID streampb.StreamID,
) (*streampb.ReplicationStreamSpec, error) {
	jobExecCtx := evalCtx.JobExecContext.(sql.JobExecContext)
	jobID := jobspb.JobID(streamID)
	// Returns error if the replication stream is not active
	j, err := jobExecCtx.ExecCfg().JobRegistry.LoadJobWithTxn(ctx, jobID, txn)
	if err != nil {
		return nil, errors.Wrapf(err, "could not load job for replication stream %d", streamID)
	}
	details, ok := j.Details().(jobspb.StreamReplicationDetails)
	if !ok {
		return nil, notAReplicationJobError(jobspb.JobID(streamID))
	}
	if j.Status() != jobs.StatusRunning {
		return nil, jobIsNotRunningError(jobID, j.Status(), "create stream spec")
	}
	return buildReplicationStreamSpec(ctx, evalCtx, details.TenantID, false, details.Spans)

}

func buildReplicationStreamSpec(
	ctx context.Context,
	evalCtx *eval.Context,
	tenantID roachpb.TenantID,
	forSpanConfigs bool,
	targetSpans roachpb.Spans,
) (*streampb.ReplicationStreamSpec, error) {
	jobExecCtx := evalCtx.JobExecContext.(sql.JobExecContext)

	// Partition the spans with SQLPlanner
	dsp := jobExecCtx.DistSQLPlanner()
	noLoc := roachpb.Locality{}
	oracle := kvfollowerreadsccl.NewBulkOracle(
		dsp.ReplicaOracleConfig(evalCtx.Locality), noLoc, kvfollowerreadsccl.StreakConfig{
			Min: 10, SmallPlanMin: 3, SmallPlanThreshold: 3, MaxSkew: 0.95,
		},
	)

	planCtx := dsp.NewPlanningCtxWithOracle(
		ctx, jobExecCtx.ExtendedEvalContext(), nil /* planner */, nil /* txn */, sql.FullDistribution, oracle, noLoc,
	)

	spanPartitions, err := dsp.PartitionSpans(ctx, planCtx, targetSpans)
	if err != nil {
		return nil, err
	}

	var spanConfigsStreamID streampb.StreamID
	if forSpanConfigs {
		spanConfigsStreamID = streampb.StreamID(builtins.GenerateUniqueInt(builtins.ProcessUniqueID(evalCtx.NodeID.SQLInstanceID())))
	}

	res := &streampb.ReplicationStreamSpec{
		Partitions:         make([]streampb.ReplicationStreamSpec_Partition, 0, len(spanPartitions)),
		SourceTenantID:     tenantID,
		SpanConfigStreamID: spanConfigsStreamID,
	}

	for _, sp := range spanPartitions {
		nodeInfo, err := dsp.GetSQLInstanceInfo(sp.SQLInstanceID)
		if err != nil {
			return nil, err
		}
		res.Partitions = append(res.Partitions, streampb.ReplicationStreamSpec_Partition{
			NodeID:     roachpb.NodeID(sp.SQLInstanceID),
			SQLAddress: nodeInfo.SQLAddress,
			Locality:   nodeInfo.Locality,
			PartitionSpec: &streampb.StreamPartitionSpec{
				Spans: sp.Spans,
				Config: streampb.StreamPartitionSpec_ExecutionConfig{
					MinCheckpointFrequency: streamingccl.StreamReplicationMinCheckpointFrequency.Get(&evalCtx.Settings.SV),
				},
			},
		})
	}
	return res, nil
}

func completeReplicationStream(
	ctx context.Context,
	evalCtx *eval.Context,
	txn isql.Txn,
	streamID streampb.StreamID,
	successfulIngestion bool,
) error {
	jobExecCtx := evalCtx.JobExecContext.(sql.JobExecContext)
	registry := jobExecCtx.ExecCfg().JobRegistry
	j, err := registry.LoadJobWithTxn(ctx, jobspb.JobID(streamID), txn)
	if err != nil {
		return err
	}
	if _, ok := j.Details().(jobspb.StreamReplicationDetails); !ok {
		return notAReplicationJobError(jobspb.JobID(streamID))
	}
	return j.WithTxn(txn).Update(ctx, func(
		txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
	) error {
		// Updates the stream ingestion status, make the job resumer exit running
		// when picking up the new status.
		if (md.Status == jobs.StatusRunning || md.Status == jobs.StatusPending) &&
			md.Progress.GetStreamReplication().StreamIngestionStatus ==
				jobspb.StreamReplicationProgress_NOT_FINISHED {
			if successfulIngestion {
				md.Progress.GetStreamReplication().StreamIngestionStatus =
					jobspb.StreamReplicationProgress_FINISHED_SUCCESSFULLY
				md.Progress.RunningStatus = "succeeding this producer job as the corresponding " +
					"stream ingestion finished successfully"
			} else {
				md.Progress.GetStreamReplication().StreamIngestionStatus =
					jobspb.StreamReplicationProgress_FINISHED_UNSUCCESSFULLY
				md.Progress.RunningStatus = "canceling this producer job as the corresponding " +
					"stream ingestion did not finish successfully"
			}
			ju.UpdateProgress(md.Progress)
		}
		return nil
	})
}

func setupSpanConfigsStream(
	ctx context.Context, evalCtx *eval.Context, txn isql.Txn, tenantName roachpb.TenantName,
) (eval.ValueGenerator, error) {

	tenantRecord, err := sql.GetTenantRecordByName(ctx, evalCtx.Settings, txn, tenantName)
	if err != nil {
		return nil, err
	}
	tenantID := roachpb.MustMakeTenantID(tenantRecord.ID)
	var spanConfigID descpb.ID
	execConfig := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)

	spanConfigName := systemschema.SpanConfigurationsTableName
	if knobs := execConfig.StreamingTestingKnobs; knobs != nil && knobs.MockSpanConfigTableName != nil {
		spanConfigName = knobs.MockSpanConfigTableName
	}

	if err := sql.DescsTxn(ctx, execConfig, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		g := col.ByName(txn.KV()).Get()
		_, imm, err := descs.PrefixAndTable(ctx, g, spanConfigName)
		if err != nil {
			return err
		}
		spanConfigID = imm.GetID()
		return nil
	}); err != nil {
		return nil, err
	}
	spanConfigKey := evalCtx.Codec.TablePrefix(uint32(spanConfigID))

	// TODO(msbutler): crop this span to the keyspan within the span config
	// table relevant to this specific tenant once I teach the client.Subscribe()
	// to stream span configs, which will make testing easier.
	span := roachpb.Span{Key: spanConfigKey, EndKey: spanConfigKey.PrefixEnd()}

	spec := streampb.SpanConfigEventStreamSpec{
		Span:                   span,
		TenantID:               tenantID,
		MinCheckpointFrequency: streamingccl.StreamReplicationMinCheckpointFrequency.Get(&evalCtx.Settings.SV),
	}
	return streamSpanConfigs(evalCtx, spec)
}
