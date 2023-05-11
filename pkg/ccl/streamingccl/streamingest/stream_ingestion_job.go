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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

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

type streamIngestionResumer struct {
	job *jobs.Job
}

func connectToActiveClient(
	ctx context.Context, ingestionJob *jobs.Job, db isql.DB,
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
	client, err := streamclient.NewStreamClient(ctx, streamAddress, db)

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
			updateRunningStatusInternal(md, ju, status, runningStatus)
			return nil
		})
	})
	if err != nil {
		log.Warningf(ctx, "error when updating job running status: %s", err)
	}
}

func updateRunningStatusInternal(
	md jobs.JobMetadata, ju *jobs.JobUpdater, status jobspb.ReplicationStatus, runningStatus string,
) {
	md.Progress.GetStreamIngest().ReplicationStatus = status
	md.Progress.RunningStatus = runningStatus
	ju.UpdateProgress(md.Progress)
}

func completeIngestion(
	ctx context.Context, execCtx sql.JobExecContext, ingestionJob *jobs.Job,
) error {
	details := ingestionJob.Details().(jobspb.StreamIngestionDetails)
	log.Infof(ctx, "activating destination tenant %d", details.DestinationTenantID)
	if err := activateTenant(ctx, execCtx, details.DestinationTenantID); err != nil {
		return err
	}

	streamID := details.StreamID
	log.Infof(ctx, "completing the producer job %d", streamID)
	updateRunningStatus(ctx, execCtx, ingestionJob, jobspb.ReplicationCuttingOver,
		"completing the producer job in the source cluster")
	completeProducerJob(ctx, ingestionJob, execCtx.ExecCfg().InternalDB, true)

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

// completeProducerJob on the source cluster is best effort. In a real
// disaster recovery scenario, who knows what state the source cluster will be
// in; thus, we should not fail the cutover step on the consumer side if we
// cannot complete the producer job.
func completeProducerJob(
	ctx context.Context, ingestionJob *jobs.Job, internalDB *sql.InternalDB, successfulIngestion bool,
) {
	streamID := ingestionJob.Details().(jobspb.StreamIngestionDetails).StreamID
	if err := contextutil.RunWithTimeout(ctx, "complete producer job", 30*time.Second,
		func(ctx context.Context) error {
			client, err := connectToActiveClient(ctx, ingestionJob, internalDB)
			if err != nil {
				return err
			}
			defer func() {
				if err := client.Close(ctx); err != nil {
					log.Warningf(ctx, "error encountered when closing stream client: %s",
						err.Error())
				}
			}()
			return client.Complete(ctx, streampb.StreamID(streamID), successfulIngestion)
		},
	); err != nil {
		log.Warningf(ctx, `encountered error when completing the source cluster producer job %d: %s`, streamID, err.Error())
	}
}

func ingest(ctx context.Context, execCtx sql.JobExecContext, ingestionJob *jobs.Job) error {
	// Cutover should be the *first* thing checked upon resumption as it is the
	// most critical task in disaster recovery.
	reverted, err := maybeRevertToCutoverTimestamp(ctx, execCtx, ingestionJob)
	if err != nil {
		return err
	}
	if reverted {
		log.Infof(ctx, "job completed cutover on resume")
		return completeIngestion(ctx, execCtx, ingestionJob)
	}
	if knobs := execCtx.ExecCfg().StreamingTestingKnobs; knobs != nil && knobs.BeforeIngestionStart != nil {
		if err := knobs.BeforeIngestionStart(ctx); err != nil {
			return err
		}
	}
	// A nil error is only possible if the job was signaled to cutover and the
	// processors shut down gracefully, i.e stopped ingesting any additional
	// events from the replication stream. At this point it is safe to revert to
	// the cutoff time to leave the cluster in a consistent state.
	if err = startDistIngestion(ctx, execCtx, ingestionJob); err != nil {
		return err
	}
	if err := revertToCutoverTimestamp(ctx, execCtx, ingestionJob); err != nil {
		return err
	}
	return completeIngestion(ctx, execCtx, ingestionJob)
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
		const msgFmt = "stream ingestion waits for retrying after error: %q"
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

	// Note that the protected timestamps are in the context of the source cluster
	// clock, not the destination. This is because the data timestamps are also
	// decided on the source cluster. Replication start time is picked on the
	// producer job on the source cluster.
	replicationStartTime := oldDetails.ReplicationStartTime
	return execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		ptp := execCfg.ProtectedTimestampProvider.WithTxn(txn)
		pts := jobsprotectedts.MakeRecord(ptsID, int64(s.job.ID()), replicationStartTime,
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
	ctx context.Context, execCtx sql.JobExecContext, ingestionJob *jobs.Job,
) error {
	reverted, err := maybeRevertToCutoverTimestamp(ctx, execCtx, ingestionJob)
	if err != nil {
		return err
	}
	if !reverted {
		return errors.Errorf("required cutover was not completed")
	}

	return nil
}

func cutoverTimeIsEligibleForCutover(
	ctx context.Context, cutoverTime hlc.Timestamp, progress *jobspb.Progress,
) bool {
	if cutoverTime.IsEmpty() {
		log.Infof(ctx, "empty cutover time, no revert required")
		return false
	}
	if progress.GetHighWater() == nil || progress.GetHighWater().Less(cutoverTime) {
		log.Infof(ctx, "job with highwater %s not yet ready to revert to cutover at %s",
			progress.GetHighWater(), cutoverTime.String())
		return false
	}
	return true
}

// maybeRevertToCutoverTimestamp reads the job progress for the cutover time and
// if the job has progressed passed the cutover time issues a RevertRangeRequest
// with the target time set to that cutover time, to bring the ingesting cluster
// to a consistent state.
func maybeRevertToCutoverTimestamp(
	ctx context.Context, p sql.JobExecContext, ingestionJob *jobs.Job,
) (bool, error) {

	ctx, span := tracing.ChildSpan(ctx, "streamingest.revertToCutoverTimestamp")
	defer span.Finish()

	// The update below sets the ReplicationStatus to
	// CuttingOver. Once set, the cutoverTimestamp cannot be
	// changed. We want to be sure to read the timestamp that
	// existed in the record at the point of the update rather the
	// value that may be in the job record before the update.
	var (
		shouldRevertToCutover bool
		cutoverTimestamp      hlc.Timestamp
		spanToRevert          roachpb.Span
	)
	if err := ingestionJob.NoTxn().Update(ctx,
		func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			streamIngestionDetails := md.Payload.GetStreamIngestion()
			if streamIngestionDetails == nil {
				return errors.AssertionFailedf("unknown payload %v in stream ingestion job %d",
					md.Payload, ingestionJob.ID())
			}

			streamIngestionProgress := md.Progress.GetStreamIngest()
			if streamIngestionProgress == nil {
				return errors.AssertionFailedf("unknown progress %v in stream ingestion job %d",
					md.Progress, ingestionJob.ID())
			}

			cutoverTimestamp = streamIngestionProgress.CutoverTime
			spanToRevert = streamIngestionDetails.Span
			shouldRevertToCutover = cutoverTimeIsEligibleForCutover(ctx, cutoverTimestamp, md.Progress)

			if shouldRevertToCutover {
				updateRunningStatusInternal(md, ju, jobspb.ReplicationCuttingOver,
					fmt.Sprintf("starting to cut over to the given timestamp %s", cutoverTimestamp))
			} else {
				if streamIngestionProgress.ReplicationStatus == jobspb.ReplicationCuttingOver {
					return errors.AssertionFailedf("cutover already started but cutover time %s is not eligible for cutover",
						cutoverTimestamp)
				}
			}
			return nil
		}); err != nil {
		return false, err
	}
	if !shouldRevertToCutover {
		return false, nil
	}
	log.Infof(ctx,
		"reverting to cutover timestamp %s for stream ingestion job %d",
		cutoverTimestamp, ingestionJob.ID())
	if p.ExecCfg().StreamingTestingKnobs != nil && p.ExecCfg().StreamingTestingKnobs.AfterCutoverStarted != nil {
		p.ExecCfg().StreamingTestingKnobs.AfterCutoverStarted()
	}

	minProgressUpdateInterval := 15 * time.Second
	progMetric := p.ExecCfg().JobRegistry.MetricsStruct().StreamIngest.(*Metrics).ReplicationCutoverProgress
	progUpdater, err := newCutoverProgressTracker(ctx, p, spanToRevert, ingestionJob, progMetric, minProgressUpdateInterval)
	if err != nil {
		return false, err
	}

	batchSize := int64(sql.RevertTableDefaultBatchSize)
	if p.ExecCfg().StreamingTestingKnobs != nil && p.ExecCfg().StreamingTestingKnobs.OverrideRevertRangeBatchSize != 0 {
		batchSize = p.ExecCfg().StreamingTestingKnobs.OverrideRevertRangeBatchSize
	}
	if err := sql.RevertSpansFanout(ctx,
		p.ExecCfg().DB,
		p,
		[]roachpb.Span{spanToRevert},
		cutoverTimestamp,
		// TODO(ssd): It should be safe for us to ingore the
		// GC threshold. Why aren't we?
		false, /* ignoreGCThreshold */
		batchSize,
		progUpdater.onCompletedCallback); err != nil {
		return false, err
	}
	return true, nil
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
	completeProducerJob(ctx, s.job, jobExecCtx.ExecCfg().InternalDB, false)

	details := s.job.Details().(jobspb.StreamIngestionDetails)
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

// cutoverProgressTracker updates the job progress and the given
// metric with the number of ranges still remainng to revert during
// the cutover process.
type cutoverProgressTracker struct {
	minProgressUpdateInterval time.Duration
	progMetric                *metric.Gauge
	job                       *jobs.Job

	remainingSpans     roachpb.SpanGroup
	lastUpdatedAt      time.Time
	originalRangeCount int

	getRangeCount                   func(context.Context, roachpb.Spans) (int, error)
	onJobProgressUpdate             func()
	overrideShouldUpdateJobProgress func() bool
}

func newCutoverProgressTracker(
	ctx context.Context,
	p sql.JobExecContext,
	spanToRevert roachpb.Span,
	job *jobs.Job,
	progMetric *metric.Gauge,
	minProgressUpdateInterval time.Duration,
) (*cutoverProgressTracker, error) {
	var sg roachpb.SpanGroup
	sg.Add(spanToRevert)

	nRanges, err := sql.NumRangesInSpans(ctx, p.ExecCfg().DB, p.DistSQLPlanner(), sg.Slice())
	if err != nil {
		return nil, err
	}
	c := &cutoverProgressTracker{
		job:                       job,
		progMetric:                progMetric,
		minProgressUpdateInterval: minProgressUpdateInterval,

		remainingSpans:     sg,
		originalRangeCount: nRanges,

		getRangeCount: func(ctx context.Context, sps roachpb.Spans) (int, error) {
			return sql.NumRangesInSpans(ctx, p.ExecCfg().DB, p.DistSQLPlanner(), sg.Slice())
		},
	}
	if testingKnobs := p.ExecCfg().StreamingTestingKnobs; testingKnobs != nil {
		c.overrideShouldUpdateJobProgress = testingKnobs.CutoverProgressShouldUpdate
		c.onJobProgressUpdate = testingKnobs.OnCutoverProgressUpdate
	}
	return c, nil

}

func (c *cutoverProgressTracker) shouldUpdateJobProgress() bool {
	if c.overrideShouldUpdateJobProgress != nil {
		return c.overrideShouldUpdateJobProgress()
	}
	return timeutil.Since(c.lastUpdatedAt) >= c.minProgressUpdateInterval
}

func (c *cutoverProgressTracker) updateJobProgress(ctx context.Context, sp []roachpb.Span) error {
	nRanges, err := c.getRangeCount(ctx, sp)
	if err != nil {
		return err
	}

	c.progMetric.Update(int64(nRanges))

	// We set lastUpdatedAt even though we might not actually
	// update the job record below. We do this to avoid asking for
	// the range count too often.
	c.lastUpdatedAt = timeutil.Now()

	// If our fraction is going to actually move, avoid touching
	// the job record.
	if nRanges >= c.originalRangeCount {
		return nil
	}

	fractionRangesFinished := float32(c.originalRangeCount-nRanges) / float32(c.originalRangeCount)
	if err := c.job.NoTxn().FractionProgressed(ctx, jobs.FractionUpdater(fractionRangesFinished)); err != nil {
		return jobs.SimplifyInvalidStatusError(err)
	}
	if c.onJobProgressUpdate != nil {
		c.onJobProgressUpdate()
	}
	return nil
}

func (c *cutoverProgressTracker) onCompletedCallback(
	ctx context.Context, completed roachpb.Span,
) error {
	c.remainingSpans.Sub(completed)
	if !c.shouldUpdateJobProgress() {
		return nil
	}

	if err := c.updateJobProgress(ctx, c.remainingSpans.Slice()); err != nil {
		log.Warningf(ctx, "failed to update job progress: %v", err)
	}
	return nil
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
