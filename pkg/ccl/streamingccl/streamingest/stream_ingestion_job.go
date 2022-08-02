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
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streampb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// completeStreamIngestion terminates the stream as of specified time.
func completeStreamIngestion(
	evalCtx *eval.Context, txn *kv.Txn, ingestionJobID jobspb.JobID, cutoverTimestamp hlc.Timestamp,
) error {
	jobRegistry := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig).JobRegistry
	return jobRegistry.UpdateJobWithTxn(evalCtx.Ctx(), ingestionJobID, txn, false, /* useReadLock */
		func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			// TODO(adityamaru): This should change in the future, a user should be
			// allowed to correct their cutover time if the process of reverting the job
			// has not started.
			if jobCutoverTime := md.Progress.GetStreamIngest().CutoverTime; !jobCutoverTime.IsEmpty() {
				return errors.Newf("cutover timestamp already set to %s, "+
					"job %d is in the process of cutting over", jobCutoverTime.String(), ingestionJobID)
			}

			// Update the sentinel being polled by the stream ingestion job to
			// check if a complete has been signaled.
			md.Progress.GetStreamIngest().CutoverTime = cutoverTimestamp
			ju.UpdateProgress(md.Progress)
			return nil
		})
}

func getStreamIngestionStats(
	evalCtx *eval.Context, txn *kv.Txn, ingestionJobID jobspb.JobID,
) (*streampb.StreamIngestionStats, error) {
	registry := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig).JobRegistry
	j, err := registry.LoadJobWithTxn(evalCtx.Ctx(), ingestionJobID, txn)
	if err != nil {
		return nil, err
	}
	details := j.Details().(jobspb.StreamIngestionDetails)
	progress := j.Progress()
	stats := &streampb.StreamIngestionStats{
		IngestionDetails:  &details,
		IngestionProgress: progress.GetStreamIngest(),
	}
	if highwater := progress.GetHighWater(); highwater != nil && !highwater.IsEmpty() {
		lagInfo := &streampb.StreamIngestionStats_ReplicationLagInfo{
			MinIngestedTimestamp: *highwater,
		}
		lagInfo.EarliestCheckpointedTimestamp = hlc.MaxTimestamp
		lagInfo.LatestCheckpointedTimestamp = hlc.MinTimestamp
		// TODO(casper): track spans that the slowest partition is associated
		for _, resolvedSpan := range progress.GetStreamIngest().Checkpoint.ResolvedSpans {
			if resolvedSpan.Timestamp.Less(lagInfo.EarliestCheckpointedTimestamp) {
				lagInfo.EarliestCheckpointedTimestamp = resolvedSpan.Timestamp
			}

			if lagInfo.LatestCheckpointedTimestamp.Less(resolvedSpan.Timestamp) {
				lagInfo.LatestCheckpointedTimestamp = resolvedSpan.Timestamp
			}
		}
		lagInfo.SlowestFastestIngestionLag = lagInfo.LatestCheckpointedTimestamp.GoTime().
			Sub(lagInfo.EarliestCheckpointedTimestamp.GoTime())
		lagInfo.ReplicationLag = timeutil.Since(highwater.GoTime())
		stats.ReplicationLagInfo = lagInfo
	}

	client, err := streamclient.GetFirstActiveClient(evalCtx.Ctx(), progress.GetStreamIngest().StreamAddresses)
	if err != nil {
		return nil, err
	}
	streamStatus, err := client.Heartbeat(evalCtx.Ctx(), streaming.StreamID(details.StreamID), hlc.MaxTimestamp)
	if err != nil {
		stats.ProducerError = err.Error()
	} else {
		stats.ProducerStatus = &streamStatus
	}
	return stats, client.Close(evalCtx.Ctx())
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
	streamID streaming.StreamID,
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
		return errors.Errorf("failed to resume ingestion job %d as the producer job is not active "+
			"and in status %s", ingestionJobID, status.StreamStatus)
	}
	return nil
}

func ingest(ctx context.Context, execCtx sql.JobExecContext, ingestionJob *jobs.Job) error {
	details := ingestionJob.Details().(jobspb.StreamIngestionDetails)
	progress := ingestionJob.Progress()
	streamAddress := streamingccl.StreamAddress(details.StreamAddress)

	startTime := progress.GetStreamIngest().StartTime
	// Start from the last checkpoint if it exists.
	if h := progress.GetHighWater(); h != nil && !h.IsEmpty() {
		startTime = *h
	}

	// If there is an existing stream ID, reconnect to it.
	streamID := streaming.StreamID(details.StreamID)
	// Initialize a stream client and resolve topology.
	client, err := connectToActiveClient(ctx, ingestionJob)
	if err != nil {
		return err
	}
	ingestWithClient := func() error {
		if err := waitUntilProducerActive(ctx, client, streamID, startTime, ingestionJob.ID()); err != nil {
			return err
		}

		log.Infof(ctx, "producer job %d is active, creating a stream replication plan", streamID)
		topology, err := client.Plan(ctx, streamID)
		if err != nil {
			return err
		}

		// TODO(casper): update running status
		err = ingestionJob.Update(ctx, nil, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			if md.Progress.GetStreamIngest().StartTime.Less(startTime) {
				md.Progress.GetStreamIngest().StartTime = startTime
			}
			md.Progress.GetStreamIngest().StreamAddresses = topology.StreamAddresses()
			ju.UpdateProgress(md.Progress)
			return nil
		})
		if err != nil {
			return errors.Wrap(err, "failed to update job progress")
		}

		log.Infof(ctx, "ingestion job %d resumes stream ingestion from start time %s",
			ingestionJob.ID(), progress.GetStreamIngest().StartTime)
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
			streamAddress, topology, sqlInstanceIDs, progress.GetStreamIngest().StartTime, checkpoint,
			ingestionJob.ID(), streamID, details.TenantID, details.NewTenantID)
		if err != nil {
			return err
		}

		// Plan and run the DistSQL flow.
		log.Infof(ctx, "starting to plan and run DistSQL flow for stream ingestion job %d",
			ingestionJob.ID())
		if err = distStreamIngest(ctx, execCtx, sqlInstanceIDs, ingestionJob.ID(), planCtx, dsp,
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

		log.Infof(ctx, "activating destination tenant %d", details.NewTenantID)
		// Activate the tenant as it is now in a usable state.
		if err = activateTenant(ctx, execCtx, details.NewTenantID); err != nil {
			return err
		}

		log.Infof(ctx, "starting to complete the producer job %d", streamID)
		// Completes the producer job in the source cluster on best effort.
		if err = client.Complete(ctx, streamID, true /* successfulIngestion */); err != nil {
			log.Warningf(ctx, "encountered error when completing the source cluster producer job %d", streamID)
		}
		return nil
	}
	return errors.CombineErrors(ingestWithClient(), client.Close(ctx))
}

// The ingestion job should never fail, only pause, as progress should never be lost.
func (s *streamIngestionResumer) handleResumeError(
	resumeCtx context.Context, execCtx interface{}, err error,
) error {
	const errorFmt = "ingestion job failed (%v) but is being paused"
	errorMessage := fmt.Sprintf(errorFmt, err)
	log.Warningf(resumeCtx, errorFmt, err)

	// The ingestion job is paused but the producer job will keep
	// running until it times out. Users can still resume ingestion before
	// the producer job times out.
	jobExecCtx := execCtx.(sql.JobExecContext)
	return s.job.PauseRequested(resumeCtx, jobExecCtx.Txn(), func(ctx context.Context,
		planHookState interface{}, txn *kv.Txn, progress *jobspb.Progress) error {
		progress.RunningStatus = errorMessage
		return nil
	}, errorMessage)
}

// Resume is part of the jobs.Resumer interface.  Ensure that any errors
// produced here are returned as s.handleResumeError.
func (s *streamIngestionResumer) Resume(resumeCtx context.Context, execCtx interface{}) error {
	// Cutover should be the *first* thing checked upon resumption as it is the
	// most critical task in disaster recovery.
	reverted, err := maybeRevertToCutoverTimestamp(resumeCtx, execCtx, s.job.ID())
	if err != nil {
		return s.handleResumeError(resumeCtx, execCtx, err)
	}

	if reverted {
		log.Infof(resumeCtx, "job completed cutover on resume")
		return nil
	}

	// Start ingesting KVs from the replication stream.
	// TODO(casper): retry stream ingestion with exponential
	// backoff and finally pause on error.
	err = ingest(resumeCtx, execCtx.(sql.JobExecContext), s.job)
	if err != nil {
		return s.handleResumeError(resumeCtx, execCtx, err)
	}
	return nil
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

	spans := []roachpb.Span{sd.Span}
	for len(spans) != 0 {
		var b kv.Batch
		for _, span := range spans {
			b.AddRawRequest(&roachpb.RevertRangeRequest{
				RequestHeader: roachpb.RequestHeader{
					Key:    span.Key,
					EndKey: span.EndKey,
				},
				TargetTime:                          sp.StreamIngest.CutoverTime,
				EnableTimeBoundIteratorOptimization: true, // NB: Must set for 22.1 compatibility.
			})
		}
		b.Header.MaxSpanRequestKeys = sql.RevertTableDefaultBatchSize
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
	return true, j.SetProgress(ctx, nil /* txn */, *sp.StreamIngest)
}

func activateTenant(ctx context.Context, execCtx interface{}, newTenantID roachpb.TenantID) error {
	p := execCtx.(sql.JobExecContext)
	execCfg := p.ExecCfg()
	return execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return sql.ActivateTenant(ctx, execCfg, txn, newTenantID.ToUint64())
	})
}

func (s *streamIngestionResumer) cancelProducerJob(
	ctx context.Context, details jobspb.StreamIngestionDetails,
) {
	streamID := streaming.StreamID(details.StreamID)
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
		fmt.Println("canceled failure", err)
	}
	fmt.Println("cancel sent")
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
// TODO(adityamaru): Add ClearRange logic once we have introduced
// synchronization between the flow tearing down and the job transitioning to a
// failed/canceled state.
func (s *streamIngestionResumer) OnFailOrCancel(ctx context.Context, _ interface{}, _ error) error {
	// Cancel the producer job on best effort. The source job's protected timestamp is no
	// longer needed as this ingestion job is in 'reverting' status and we won't resume
	// ingestion anymore.
	details := s.job.Details().(jobspb.StreamIngestionDetails)
	s.cancelProducerJob(ctx, details)
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
