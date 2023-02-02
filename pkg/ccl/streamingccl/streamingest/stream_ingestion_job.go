// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// completeStreamIngestion terminates the stream as of specified time.
func completeStreamIngestion(
	ctx context.Context,
	jobRegistry *jobs.Registry,
	txn isql.Txn,
	ingestionJobID jobspb.JobID,
	cutoverTimestamp hlc.Timestamp,
) error {
	if err := jobRegistry.UpdateJobWithTxn(ctx, ingestionJobID, txn, false,
		func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			progress := md.Progress.GetStreamIngest()
			if progress.ReplicationStatus == jobspb.ReplicationCuttingOver {
				return errors.Newf("job %d already started cutting over to timestamp %s",
					ingestionJobID, progress.CutoverTime)
			}

			progress.ReplicationStatus = jobspb.ReplicationPendingCutover
			// Update the sentinel being polled by the stream ingestion job to
			// check if a complete has been signaled.
			progress.CutoverTime = cutoverTimestamp
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
	stats, err := replicationutils.GetStreamIngestionStatsNoHeartbeat(ctx, details, job.Progress())
	if err != nil {
		return nil, jobspb.ReplicationError.String(), err
	}
	if job.Status() == jobs.StatusPaused {
		return stats, jobspb.ReplicationPaused.String(), nil
	}
	return stats, stats.IngestionProgress.ReplicationStatus.String(), nil
}

type streamIngestionResumer struct {
	job *jobs.Job
}

func connectToActiveClient(
	ctx context.Context, ingestionJob *jobs.Job,
) (streamclient.Client, error) {
	details := ingestionJob.Details().(jobspb.StreamIngestionDetails)
	progress := ingestionJob.Progress()
	streamAddresses := progress.GetStreamIngest().StreamAddresses

	if len(streamAddresses) > 0 {
		log.Infof(ctx, "ingestion job %d attempting to connect to existing stream addresses", ingestionJob.ID())
		client, err := streamclient.GetFirstActiveClient(ctx, streamAddresses)
		if err == nil {
			return client, err
		}

		// fall through to streamAddress, as even though it is likely part of the
		// topology it may have been changed to a new valid address via an ALTER
		// statement
	}

	// Without a list of addresses from existing progress we use the stream
	// address from the creation statement
	streamAddress := streamingccl.StreamAddress(details.StreamAddress)
	client, err := streamclient.NewStreamClient(ctx, streamAddress)

	return client, errors.Wrapf(err, "ingestion job %d failed to connect to stream address or existing topology for planning", ingestionJob.ID())
}

// Ping the producer job and waits until it is active/running, returns nil when
// the job is active.
func waitUntilProducerActive(
	ctx context.Context,
	client streamclient.Client,
	streamID streampb.StreamID,
	heartbeatTimestamp hlc.Timestamp,
	ingestionJobID jobspb.JobID,
) error {
	ro := retry.Options{
		InitialBackoff: 1 * time.Second,
		Multiplier:     2,
		MaxBackoff:     5 * time.Second,
		MaxRetries:     4,
	}
	// Make sure the producer job is active before start the stream replication.
	var status streampb.StreamReplicationStatus
	var err error
	for r := retry.Start(ro); r.Next(); {
		status, err = client.Heartbeat(ctx, streamID, heartbeatTimestamp)
		if err != nil {
			return errors.Wrapf(err, "failed to resume ingestion job %d due to producer job error",
				ingestionJobID)
		}
		if status.StreamStatus != streampb.StreamReplicationStatus_UNKNOWN_STREAM_STATUS_RETRY {
			break
		}
		log.Warningf(ctx, "producer job %d has status %s, retrying", streamID, status.StreamStatus)
	}
	if status.StreamStatus != streampb.StreamReplicationStatus_STREAM_ACTIVE {
		return jobs.MarkAsPermanentJobError(errors.Errorf("failed to resume ingestion job %d "+
			"as the producer job is not active and in status %s", ingestionJobID, status.StreamStatus))
	}
	return nil
}

func updateRunningStatus(
	ctx context.Context,
	execCtx sql.JobExecContext,
	ingestionJob *jobs.Job,
	status jobspb.ReplicationStatus,
	runningStatus string,
) {
	execCfg := execCtx.ExecCfg()
	err := execCfg.InternalDB.Txn(ctx, func(
		ctx context.Context, txn isql.Txn,
	) error {
		return ingestionJob.WithTxn(txn).Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			md.Progress.GetStreamIngest().ReplicationStatus = status
			md.Progress.RunningStatus = runningStatus
			ju.UpdateProgress(md.Progress)
			return nil
		})
	})
	if err != nil {
		log.Warningf(ctx, "error when updating job running status: %s", err)
	}
}

func ingest(ctx context.Context, execCtx sql.JobExecContext, ingestionJob *jobs.Job) error {
	// Cutover should be the *first* thing checked upon resumption as it is the
	// most critical task in disaster recovery.
	reverted, err := maybeRevertToCutoverTimestamp(ctx, execCtx, ingestionJob.ID())
	if err != nil {
		return err
	}
	if reverted {
		log.Infof(ctx, "job completed cutover on resume")
		return nil
	}

	if knobs := execCtx.ExecCfg().StreamingTestingKnobs; knobs != nil && knobs.BeforeIngestionStart != nil {
		if err := knobs.BeforeIngestionStart(ctx); err != nil {
			return err
		}
	}

	details := ingestionJob.Details().(jobspb.StreamIngestionDetails)
	progress := ingestionJob.Progress()
	streamAddress := streamingccl.StreamAddress(details.StreamAddress)

	var previousHighWater, heartbeatTimestamp hlc.Timestamp
	initialScanTimestamp := details.ReplicationStartTime
	// Start from the last checkpoint if it exists.
	if h := progress.GetHighWater(); h != nil && !h.IsEmpty() {
		previousHighWater = *h
		heartbeatTimestamp = previousHighWater
	} else {
		heartbeatTimestamp = initialScanTimestamp
	}

	// Initialize a stream client and resolve topology.
	client, err := connectToActiveClient(ctx, ingestionJob)
	if err != nil {
		return err
	}
	ingestWithClient := func() error {
		streamID := streampb.StreamID(details.StreamID)
		updateRunningStatus(ctx, execCtx, ingestionJob, jobspb.InitializingReplication,
			fmt.Sprintf("connecting to the producer job %d and creating a stream replication plan", streamID))
		if err := waitUntilProducerActive(ctx, client, streamID, heartbeatTimestamp, ingestionJob.ID()); err != nil {
			return err
		}

		log.Infof(ctx, "producer job %d is active, creating a stream replication plan", streamID)
		topology, err := client.Plan(ctx, streamID)
		if err != nil {
			return err
		}

		// TODO(casper): update running status
		err = ingestionJob.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			md.Progress.GetStreamIngest().StreamAddresses = topology.StreamAddresses()
			ju.UpdateProgress(md.Progress)
			return nil
		})
		if err != nil {
			return errors.Wrap(err, "failed to update job progress")
		}

		if previousHighWater.IsEmpty() {
			log.Infof(ctx, "ingestion job %d resumes stream ingestion from start time %s",
				ingestionJob.ID(), initialScanTimestamp)
		} else {
			log.Infof(ctx, "ingestion job %d resumes stream ingestion from start time %s",
				ingestionJob.ID(), previousHighWater)
		}

		ingestProgress := progress.Details.(*jobspb.Progress_StreamIngest).StreamIngest
		checkpoint := ingestProgress.Checkpoint

		evalCtx := execCtx.ExtendedEvalContext()
		dsp := execCtx.DistSQLPlanner()

		planCtx, sqlInstanceIDs, err := dsp.SetupAllNodesPlanning(ctx, evalCtx, execCtx.ExecCfg())

		if err != nil {
			return err
		}

		// Construct stream ingestion processor specs.
		streamIngestionSpecs, streamIngestionFrontierSpec, err := distStreamIngestionPlanSpecs(
			streamAddress, topology, sqlInstanceIDs, initialScanTimestamp, previousHighWater, checkpoint,
			ingestionJob.ID(), streamID, topology.SourceTenantID, details.DestinationTenantID)
		if err != nil {
			return err
		}

		if knobs := execCtx.ExecCfg().StreamingTestingKnobs; knobs != nil && knobs.AfterReplicationFlowPlan != nil {
			knobs.AfterReplicationFlowPlan(streamIngestionSpecs, streamIngestionFrontierSpec)
		}

		// Plan and run the DistSQL flow.
		log.Infof(ctx, "starting to run DistSQL flow for stream ingestion job %d",
			ingestionJob.ID())
		updateRunningStatus(ctx, execCtx, ingestionJob, jobspb.Replicating,
			"running the SQL flow for the stream ingestion job")
		if err = distStreamIngest(ctx, execCtx, sqlInstanceIDs, planCtx, dsp,
			streamIngestionSpecs, streamIngestionFrontierSpec); err != nil {
			return err
		}

		// A nil error is only possible if the job was signaled to cutover and the
		// processors shut down gracefully, i.e stopped ingesting any additional
		// events from the replication stream. At this point it is safe to revert to
		// the cutoff time to leave the cluster in a consistent state.
		log.Infof(ctx,
			"starting to revert to the specified cutover timestamp for stream ingestion job %d",
			ingestionJob.ID())
		if err = revertToCutoverTimestamp(ctx, execCtx, ingestionJob.ID()); err != nil {
			return err
		}

		log.Infof(ctx, "activating destination tenant %d", details.DestinationTenantID)
		// Activate the tenant as it is now in a usable state.
		if err = activateTenant(ctx, execCtx, details.DestinationTenantID); err != nil {
			return err
		}

		log.Infof(ctx, "starting to complete the producer job %d", streamID)
		updateRunningStatus(ctx, execCtx, ingestionJob, jobspb.ReplicationCuttingOver,
			"completing the producer job in the source cluster")
		// Completes the producer job in the source cluster on best effort.
		if err = client.Complete(ctx, streamID, true /* successfulIngestion */); err != nil {
			log.Warningf(ctx, "encountered error when completing the source cluster producer job %d", streamID)
		}

		// Now that we have completed the cutover we can release the protected
		// timestamp record on the destination tenant's keyspace.
		if details.ProtectedTimestampRecordID != nil {
			if err := execCtx.ExecCfg().InternalDB.Txn(ctx, func(
				ctx context.Context, txn isql.Txn,
			) error {
				ptp := execCtx.ExecCfg().ProtectedTimestampProvider.WithTxn(txn)
				return releaseDestinationTenantProtectedTimestamp(
					ctx, ptp, *details.ProtectedTimestampRecordID,
				)
			}); err != nil {
				return err
			}
		}

		return nil
	}
	return errors.CombineErrors(ingestWithClient(), client.Close(ctx))
}

func ingestWithRetries(
	ctx context.Context, execCtx sql.JobExecContext, ingestionJob *jobs.Job,
) error {
	ro := retry.Options{
		InitialBackoff: 3 * time.Second,
		Multiplier:     2,
		MaxBackoff:     1 * time.Minute,
		MaxRetries:     60,
	}

	var err error
	retryCount := 0
	for r := retry.Start(ro); r.Next(); {
		err = ingest(ctx, execCtx, ingestionJob)
		if err == nil {
			break
		}
		// By default, all errors are retryable unless it's marked as
		// permanent job error in which case we pause the job.
		// We also stop the job when this is a context cancellation error
		// as requested pause or cancel will trigger a context cancellation.
		if jobs.IsPermanentJobError(err) || errors.Is(err, context.Canceled) {
			break
		}
		const msgFmt = "stream ingestion waits for retrying after error %s"
		log.Warningf(ctx, msgFmt, err)
		updateRunningStatus(ctx, execCtx, ingestionJob, jobspb.ReplicationError,
			fmt.Sprintf(msgFmt, err))
		retryCount++
	}
	if err != nil {
		updateRunningStatus(ctx, execCtx, ingestionJob, jobspb.ReplicationError,
			fmt.Sprintf("stream ingestion encountered error and is to be paused: %s", err))
	} else {
		updateRunningStatus(ctx, execCtx, ingestionJob, jobspb.ReplicationCuttingOver,
			"stream ingestion finished successfully")
	}
	return err
}

// The ingestion job should never fail, only pause, as progress should never be lost.
func (s *streamIngestionResumer) handleResumeError(resumeCtx context.Context, err error) error {
	const errorFmt = "ingestion job failed (%v) but is being paused"
	errorMessage := fmt.Sprintf(errorFmt, err)
	log.Warningf(resumeCtx, errorFmt, err)

	// The ingestion job is paused but the producer job will keep
	// running until it times out. Users can still resume ingestion before
	// the producer job times out.
	return s.job.NoTxn().PauseRequestedWithFunc(resumeCtx, func(
		ctx context.Context, planHookState interface{}, txn isql.Txn,
		progress *jobspb.Progress,
	) error {
		progress.RunningStatus = errorMessage
		return nil
	}, errorMessage)
}

// Resume is part of the jobs.Resumer interface.  Ensure that any errors
// produced here are returned as s.handleResumeError.
func (s *streamIngestionResumer) Resume(resumeCtx context.Context, execCtx interface{}) error {
	// Protect the destination tenant's keyspan from garbage collection.
	err := s.protectDestinationTenant(resumeCtx, execCtx)
	if err != nil {
		return s.handleResumeError(resumeCtx, err)
	}

	// Start ingesting KVs from the replication stream.
	err = ingestWithRetries(resumeCtx, execCtx.(sql.JobExecContext), s.job)
	if err != nil {
		return s.handleResumeError(resumeCtx, err)
	}
	return nil
}

func releaseDestinationTenantProtectedTimestamp(
	ctx context.Context, ptp protectedts.Storage, ptsID uuid.UUID,
) error {
	if err := ptp.Release(ctx, ptsID); err != nil {
		if errors.Is(err, protectedts.ErrNotExists) {
			// No reason to return an error which might cause problems if it doesn't
			// seem to exist.
			log.Warningf(ctx, "failed to release protected which seems not to exist: %v", err)
			err = nil
		}
		return err
	}
	return nil
}

// protectDestinationTenant writes a protected timestamp record protecting the
// destination tenant's keyspace from garbage collection. This protected
// timestamp record is updated everytime the replication job records a new
// frontier timestamp, and is released OnFailOrCancel.
//
// The method persists the ID of the protected timestamp record in the
// replication job's Payload.
func (s *streamIngestionResumer) protectDestinationTenant(
	ctx context.Context, execCtx interface{},
) error {
	oldDetails := s.job.Details().(jobspb.StreamIngestionDetails)

	// If we have already protected the destination tenant keyspan in a previous
	// resumption of the stream ingestion job, then there is nothing to do.
	if oldDetails.ProtectedTimestampRecordID != nil {
		return nil
	}

	execCfg := execCtx.(sql.JobExecContext).ExecCfg()
	target := ptpb.MakeTenantsTarget([]roachpb.TenantID{oldDetails.DestinationTenantID})
	ptsID := uuid.MakeV4()
	now := execCfg.Clock.Now()
	return execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		ptp := execCfg.ProtectedTimestampProvider.WithTxn(txn)
		pts := jobsprotectedts.MakeRecord(ptsID, int64(s.job.ID()), now,
			nil /* deprecatedSpans */, jobsprotectedts.Jobs, target)
		if err := ptp.Protect(ctx, pts); err != nil {
			return err
		}
		return s.job.WithTxn(txn).Update(ctx, func(
			txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
		) error {
			if err := md.CheckRunningOrReverting(); err != nil {
				return err
			}

			details := md.Payload.GetStreamIngestion()
			details.ProtectedTimestampRecordID = &ptsID
			oldDetails.ProtectedTimestampRecordID = &ptsID

			ju.UpdatePayload(md.Payload)
			return nil
		})
	})
}

// revertToCutoverTimestamp attempts a cutover and errors out if one was not
// executed.
func revertToCutoverTimestamp(
	ctx context.Context, execCtx interface{}, ingestionJobID jobspb.JobID,
) error {
	reverted, err := maybeRevertToCutoverTimestamp(ctx, execCtx, ingestionJobID)
	if err != nil {
		return err
	}
	if !reverted {
		return errors.Errorf("required cutover was not completed")
	}

	return nil
}

// maybeRevertToCutoverTimestamp reads the job progress for the cutover time and
// if the job has progressed passed the cutover time issues a RevertRangeRequest
// with the target time set to that cutover time, to bring the ingesting cluster
// to a consistent state.
func maybeRevertToCutoverTimestamp(
	ctx context.Context, execCtx interface{}, ingestionJobID jobspb.JobID,
) (bool, error) {
	ctx, span := tracing.ChildSpan(ctx, "streamingest.revertToCutoverTimestamp")
	defer span.Finish()

	p := execCtx.(sql.JobExecContext)
	db := p.ExecCfg().DB
	j, err := p.ExecCfg().JobRegistry.LoadJob(ctx, ingestionJobID)
	if err != nil {
		return false, err
	}
	details := j.Details()
	var sd jobspb.StreamIngestionDetails
	var ok bool
	if sd, ok = details.(jobspb.StreamIngestionDetails); !ok {
		return false, errors.Newf("unknown details type %T in stream ingestion job %d",
			details, ingestionJobID)
	}
	progress := j.Progress()
	var sp *jobspb.Progress_StreamIngest
	if sp, ok = progress.GetDetails().(*jobspb.Progress_StreamIngest); !ok {
		return false, errors.Newf("unknown progress type %T in stream ingestion job %d",
			j.Progress().Progress, ingestionJobID)
	}

	cutoverTime := sp.StreamIngest.CutoverTime
	if cutoverTime.IsEmpty() {
		log.Infof(ctx, "empty cutover time, no revert required")
		return false, nil
	}
	if progress.GetHighWater() == nil || progress.GetHighWater().Less(cutoverTime) {
		log.Infof(ctx, "job with highwater %s not yet ready to revert to cutover at %s", progress.GetHighWater(), cutoverTime.String())
		return false, nil
	}

	updateRunningStatus(ctx, p, j, jobspb.ReplicationCuttingOver,
		fmt.Sprintf("starting to cut over to the given timestamp %s", cutoverTime))

	if p.ExecCfg().StreamingTestingKnobs != nil && p.ExecCfg().StreamingTestingKnobs.AfterCutoverStarted != nil {
		p.ExecCfg().StreamingTestingKnobs.AfterCutoverStarted()
	}

	origNRanges := -1
	spans := []roachpb.Span{sd.Span}
	updateJobProgress := func() error {
		if spans == nil {
			return nil
		}
		nRanges, err := sql.NumRangesInSpans(ctx, p.ExecCfg().DB, p.DistSQLPlanner(), spans)
		if err != nil {
			return err
		}
		if origNRanges == -1 {
			origNRanges = nRanges
		}
		return p.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			if nRanges < origNRanges {
				fractionRangesFinished := float32(origNRanges-nRanges) / float32(origNRanges)
				if err := j.WithTxn(txn).FractionProgressed(
					ctx, jobs.FractionUpdater(fractionRangesFinished),
				); err != nil {
					return jobs.SimplifyInvalidStatusError(err)
				}
			}
			return nil
		})
	}

	for len(spans) != 0 {
		if err := updateJobProgress(); err != nil {
			log.Warningf(ctx, "failed to update replication job progress: %+v", err)
		}
		var b kv.Batch
		for _, span := range spans {
			b.AddRawRequest(&roachpb.RevertRangeRequest{
				RequestHeader: roachpb.RequestHeader{
					Key:    span.Key,
					EndKey: span.EndKey,
				},
				TargetTime: sp.StreamIngest.CutoverTime,
			})
		}
		b.Header.MaxSpanRequestKeys = sql.RevertTableDefaultBatchSize
		if p.ExecCfg().StreamingTestingKnobs != nil && p.ExecCfg().StreamingTestingKnobs.OverrideRevertRangeBatchSize != 0 {
			b.Header.MaxSpanRequestKeys = p.ExecCfg().StreamingTestingKnobs.OverrideRevertRangeBatchSize
		}
		if err := db.Run(ctx, &b); err != nil {
			return false, err
		}

		spans = spans[:0]
		for _, raw := range b.RawResponse().Responses {
			r := raw.GetRevertRange()
			if r.ResumeSpan != nil {
				if !r.ResumeSpan.Valid() {
					return false, errors.Errorf("invalid resume span: %s", r.ResumeSpan)
				}
				spans = append(spans, *r.ResumeSpan)
			}
		}
	}
	return true, updateJobProgress()
}

func activateTenant(ctx context.Context, execCtx interface{}, newTenantID roachpb.TenantID) error {
	p := execCtx.(sql.JobExecContext)
	execCfg := p.ExecCfg()
	return execCfg.InternalDB.Txn(ctx, func(
		ctx context.Context, txn isql.Txn,
	) error {
		info, err := sql.GetTenantRecordByID(ctx, txn, newTenantID, execCfg.Settings)
		if err != nil {
			return err
		}

		info.DataState = mtinfopb.DataStateReady
		info.TenantReplicationJobID = 0
		return sql.UpdateTenantRecord(ctx, p.ExecCfg().Settings, txn, info)
	})
}

func (s *streamIngestionResumer) cancelProducerJob(
	ctx context.Context, details jobspb.StreamIngestionDetails,
) {
	streamID := streampb.StreamID(details.StreamID)
	addr := streamingccl.StreamAddress(details.StreamAddress)
	client, err := streamclient.NewStreamClient(ctx, addr)
	if err != nil {
		log.Warningf(ctx, "encountered error when creating the stream client: %v", err)
		return
	}
	log.Infof(ctx, "canceling the producer job %d as stream ingestion job %d is being canceled",
		streamID, s.job.ID())
	if err = client.Complete(ctx, streamID, false /* successfulIngestion */); err != nil {
		log.Warningf(ctx, "encountered error when canceling the producer job: %v", err)
	}
	if err = client.Close(ctx); err != nil {
		log.Warningf(ctx, "encountered error when closing the stream client: %v", err)
	}
}

// OnFailOrCancel is part of the jobs.Resumer interface.
// There is a know race between the ingestion processors shutting down, and
// OnFailOrCancel being invoked. As a result of which we might see some keys
// leftover in the keyspace if a ClearRange were to be issued here. In general
// the tenant keyspace of a failed/canceled ingestion job should be treated as
// corrupted, and the tenant should be dropped before resuming the ingestion.
func (s *streamIngestionResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, _ error,
) error {
	// Cancel the producer job on best effort. The source job's protected timestamp is no
	// longer needed as this ingestion job is in 'reverting' status and we won't resume
	// ingestion anymore.
	jobExecCtx := execCtx.(sql.JobExecContext)
	details := s.job.Details().(jobspb.StreamIngestionDetails)
	s.cancelProducerJob(ctx, details)

	execCfg := jobExecCtx.ExecCfg()
	return execCfg.InternalDB.Txn(ctx, func(
		ctx context.Context, txn isql.Txn,
	) error {
		tenInfo, err := sql.GetTenantRecordByID(ctx, txn, details.DestinationTenantID, execCfg.Settings)
		if err != nil {
			return errors.Wrap(err, "fetch tenant info")
		}

		tenInfo.TenantReplicationJobID = 0
		if err := sql.UpdateTenantRecord(ctx, execCfg.Settings, txn, tenInfo); err != nil {
			return errors.Wrap(err, "update tenant record")
		}

		if details.ProtectedTimestampRecordID != nil {
			ptp := execCfg.ProtectedTimestampProvider.WithTxn(txn)
			if err := releaseDestinationTenantProtectedTimestamp(
				ctx, ptp, *details.ProtectedTimestampRecordID,
			); err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *streamIngestionResumer) ForceRealSpan() bool { return true }

var _ jobs.Resumer = &streamIngestionResumer{}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeStreamIngestion,
		func(job *jobs.Job,
			settings *cluster.Settings) jobs.Resumer {
			return &streamIngestionResumer{job: job}
		},
		jobs.UsesTenantCostControl,
	)
}
