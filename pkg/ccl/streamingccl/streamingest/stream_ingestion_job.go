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
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// completeStreamIngestion terminates the stream as of specified time.
func completeStreamIngestion(
	evalCtx *eval.Context, txn *kv.Txn, ingestionJobID jobspb.JobID, cutoverTimestamp hlc.Timestamp,
) error {
	// Get the job payload for job_id.
	const jobsQuery = `SELECT progress FROM system.jobs WHERE id=$1 FOR UPDATE`
	row, err := evalCtx.Planner.QueryRowEx(evalCtx.Context,
		"get-stream-ingestion-job-metadata", sessiondata.NodeUserSessionDataOverride, jobsQuery, ingestionJobID)
	if err != nil {
		return err
	}
	// If an entry does not exist for the provided job_id we return an
	// error.
	if row == nil {
		return errors.Newf("job %d: not found in system.jobs table", ingestionJobID)
	}

	progress, err := jobs.UnmarshalProgress(row[0])
	if err != nil {
		return err
	}
	var sp *jobspb.Progress_StreamIngest
	var ok bool
	if sp, ok = progress.GetDetails().(*jobspb.Progress_StreamIngest); !ok {
		return errors.Newf("job %d: not of expected type StreamIngest", ingestionJobID)
	}

	// Check that the supplied cutover time is a valid one.
	// TODO(adityamaru): This will change once we allow a future cutover time to
	// be specified.
	hw := progress.GetHighWater()
	if hw == nil || hw.Less(cutoverTimestamp) {
		var highWaterTimestamp hlc.Timestamp
		if hw != nil {
			highWaterTimestamp = *hw
		}
		return errors.Newf("cannot cutover to a timestamp %s that is after the latest resolved time"+
			" %s for job %d", cutoverTimestamp.String(), highWaterTimestamp.String(), ingestionJobID)
	}

	// Reject setting a cutover time, if an earlier request to cutover has already
	// been set.
	// TODO(adityamaru): This should change in the future, a user should be
	// allowed to correct their cutover time if the process of reverting the job
	// has not started.
	if !sp.StreamIngest.CutoverTime.IsEmpty() {
		return errors.Newf("cutover timestamp already set to %s, "+
			"job %d is in the process of cutting over", sp.StreamIngest.CutoverTime.String(), ingestionJobID)
	}

	// Update the sentinel being polled by the stream ingestion job to
	// check if a complete has been signaled.
	sp.StreamIngest.CutoverTime = cutoverTimestamp
	progress.ModifiedMicros = timeutil.ToUnixMicros(txn.ReadTimestamp().GoTime())
	progressBytes, err := protoutil.Marshal(progress)
	if err != nil {
		return err
	}
	updateJobQuery := `UPDATE system.jobs SET progress=$1 WHERE id=$2`
	_, err = evalCtx.Planner.QueryRowEx(evalCtx.Context,
		"set-stream-ingestion-job-metadata",
		sessiondata.NodeUserSessionDataOverride, updateJobQuery, progressBytes, ingestionJobID)
	return err
}

func getStreamIngestionStats(
	evalCtx *eval.Context, txn *kv.Txn, ingestionJobID jobspb.JobID,
) (*streampb.StreamIngestionStats, error) {
	registry := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig).JobRegistry
	j, err := registry.LoadJob(evalCtx.Ctx(), ingestionJobID)
	if err != nil {
		return nil, err
	}
	details := j.Details().(jobspb.StreamIngestionDetails)
	progress := j.Progress()

	client, err := streamclient.NewStreamClient(streamingccl.StreamAddress(details.StreamAddress))
	if err != nil {
		return nil, err
	}
	streamStatus, err := client.Heartbeat(evalCtx.Ctx(), streaming.StreamID(details.StreamID), hlc.MinTimestamp)
	err = errors.CombineErrors(err, client.Close())
	if err != nil {
		return nil, err
	}
	return &streampb.StreamIngestionStats{
		StreamId:                details.StreamID,
		StreamReplicationStatus: &streamStatus,
		IngestionProgress:       progress.GetStreamIngest(),
	}, nil
}

type streamIngestionResumer struct {
	job *jobs.Job
}

func ingest(
	ctx context.Context,
	execCtx sql.JobExecContext,
	streamAddress streamingccl.StreamAddress,
	oldTenantID roachpb.TenantID,
	newTenantID roachpb.TenantID,
	streamID streaming.StreamID,
	startTime hlc.Timestamp,
	progress jobspb.Progress,
	ingestionJobID jobspb.JobID,
) error {
	// Initialize a stream client and resolve topology.
	client, err := streamclient.NewStreamClient(streamAddress)
	if err != nil {
		return err
	}
	ingestWithClient := func() error {
		// TODO(dt): if there is an existing stream ID, reconnect to it.
		topology, err := client.Plan(ctx, streamID)
		if err != nil {
			return err
		}

		// TODO(adityamaru): If the job is being resumed it is possible that it has
		// check-pointed a resolved ts up to which all of its processors had ingested
		// KVs. We can skip to ingesting after this resolved ts. Plumb the
		// initialHighwatermark to the ingestion processor spec based on what we read
		// from the job progress.
		initialHighWater := startTime
		if h := progress.GetHighWater(); h != nil && !h.IsEmpty() {
			initialHighWater = *h
		}

		evalCtx := execCtx.ExtendedEvalContext()
		dsp := execCtx.DistSQLPlanner()

		planCtx, sqlInstanceIDs, err := dsp.SetupAllNodesPlanning(ctx, evalCtx, execCtx.ExecCfg())
		if err != nil {
			return err
		}

		// Construct stream ingestion processor specs.
		streamIngestionSpecs, streamIngestionFrontierSpec, err := distStreamIngestionPlanSpecs(
			streamAddress, topology, sqlInstanceIDs, initialHighWater, ingestionJobID, streamID, oldTenantID, newTenantID)
		if err != nil {
			return err
		}

		// Plan and run the DistSQL flow.
		if err = distStreamIngest(ctx, execCtx, sqlInstanceIDs, ingestionJobID, planCtx, dsp, streamIngestionSpecs,
			streamIngestionFrontierSpec); err != nil {
			return err
		}

		// A nil error is only possible if the job was signaled to cutover and the
		// processors shut down gracefully, i.e stopped ingesting any additional
		// events from the replication stream. At this point it is safe to revert to
		// the cutoff time to leave the cluster in a consistent state.
		if err = revertToCutoverTimestamp(ctx, execCtx, ingestionJobID); err != nil {
			return err
		}
		// Completes the producer job in the source cluster.
		return client.Complete(ctx, streamID)
	}
	return errors.CombineErrors(ingestWithClient(), client.Close())
}

// Resume is part of the jobs.Resumer interface.
func (s *streamIngestionResumer) Resume(resumeCtx context.Context, execCtx interface{}) error {
	details := s.job.Details().(jobspb.StreamIngestionDetails)
	p := execCtx.(sql.JobExecContext)

	// Start ingesting KVs from the replication stream.
	streamAddress := streamingccl.StreamAddress(details.StreamAddress)
	return ingest(resumeCtx, p, streamAddress, details.TenantID, details.NewTenantID,
		streaming.StreamID(details.StreamID), details.StartTime, s.job.Progress(), s.job.ID())
}

// revertToCutoverTimestamp reads the job progress for the cutover time and
// issues a RevertRangeRequest with the target time set to that cutover time, to
// bring the ingesting cluster to a consistent state.
func revertToCutoverTimestamp(
	ctx context.Context, execCtx interface{}, ingestionJobID jobspb.JobID,
) error {
	p := execCtx.(sql.JobExecContext)
	db := p.ExecCfg().DB
	j, err := p.ExecCfg().JobRegistry.LoadJob(ctx, ingestionJobID)
	if err != nil {
		return err
	}
	details := j.Details()
	var sd jobspb.StreamIngestionDetails
	var ok bool
	if sd, ok = details.(jobspb.StreamIngestionDetails); !ok {
		return errors.Newf("unknown details type %T in stream ingestion job %d",
			details, ingestionJobID)
	}
	progress := j.Progress()
	var sp *jobspb.Progress_StreamIngest
	if sp, ok = progress.GetDetails().(*jobspb.Progress_StreamIngest); !ok {
		return errors.Newf("unknown progress type %T in stream ingestion job %d",
			j.Progress().Progress, ingestionJobID)
	}

	if sp.StreamIngest.CutoverTime.IsEmpty() {
		return errors.AssertionFailedf("cutover time is unexpectedly empty, " +
			"cannot revert to a consistent state")
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
				EnableTimeBoundIteratorOptimization: true,
			})
		}
		b.Header.MaxSpanRequestKeys = sql.RevertTableDefaultBatchSize
		if err := db.Run(ctx, &b); err != nil {
			return err
		}

		spans = spans[:0]
		for _, raw := range b.RawResponse().Responses {
			r := raw.GetRevertRange()
			if r.ResumeSpan != nil {
				if !r.ResumeSpan.Valid() {
					return errors.Errorf("invalid resume span: %s", r.ResumeSpan)
				}
				spans = append(spans, *r.ResumeSpan)
			}
		}
	}

	return nil
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
func (s *streamIngestionResumer) OnFailOrCancel(_ context.Context, _ interface{}) error {
	return nil
}

var _ jobs.Resumer = &streamIngestionResumer{}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeStreamIngestion,
		func(job *jobs.Job,
			settings *cluster.Settings) jobs.Resumer {
			return &streamIngestionResumer{job: job}
		})
}
