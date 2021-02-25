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
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type streamIngestionResumer struct {
	job *jobs.Job
}

// checkForCutoverSignalFrequency is the frequency at which the resumer polls
// the system.jobs table to check whether the stream ingestion job has been
// signaled to cutover.
var cutoverSignalPollInterval = settings.RegisterDurationSetting(
	"bulkio.stream_ingestion.cutover_signal_poll_interval",
	"the interval at which the stream ingestion job checks if it has been signaled to cutover",
	30*time.Second,
	settings.NonNegativeDuration,
)

func ingest(
	ctx context.Context,
	execCtx sql.JobExecContext,
	streamAddress streamingccl.StreamAddress,
	progress jobspb.Progress,
	jobID int64,
) error {
	// Initialize a stream client and resolve topology.
	client, err := streamclient.NewStreamClient(streamAddress)
	if err != nil {
		return err
	}
	topology, err := client.GetTopology(streamAddress)
	if err != nil {
		return err
	}

	// TODO(adityamaru): If the job is being resumed it is possible that it has
	// check-pointed a resolved ts up to which all of its processors had ingested
	// KVs. We can skip to ingesting after this resolved ts. Plumb the
	// initialHighwatermark to the ingestion processor spec based on what we read
	// from the job progress.
	var initialHighWater hlc.Timestamp
	if h := progress.GetHighWater(); h != nil && !h.IsEmpty() {
		initialHighWater = *h
	}

	evalCtx := execCtx.ExtendedEvalContext()
	dsp := execCtx.DistSQLPlanner()

	planCtx, nodes, err := dsp.SetupAllNodesPlanning(ctx, evalCtx, execCtx.ExecCfg())
	if err != nil {
		return err
	}

	// Construct stream ingestion processor specs.
	streamIngestionSpecs, streamIngestionFrontierSpec, err := distStreamIngestionPlanSpecs(
		streamAddress, topology, nodes, initialHighWater, jobID)
	if err != nil {
		return err
	}

	// Plan and run the DistSQL flow.
	return distStreamIngest(ctx, execCtx, nodes, jobID, planCtx, dsp, streamIngestionSpecs,
		streamIngestionFrontierSpec)
}

// Resume is part of the jobs.Resumer interface.
func (s *streamIngestionResumer) Resume(resumeCtx context.Context, execCtx interface{}) error {
	details := s.job.Details().(jobspb.StreamIngestionDetails)
	p := execCtx.(sql.JobExecContext)

	// ingestCtx is used to plan and run the DistSQL flow.

	// Start ingesting KVs from the replication stream.
	err := ingest(resumeCtx, p, details.StreamAddress, s.job.Progress(), *s.job.ID())
	if err != nil {
		return err
	}

	// A non-nil error is only possible if the job was signaled to cutover and the
	// processors shut down gracefully, i.e stopped ingesting any additional
	// events from the replication stream.
	//At this point it is safe to revert to the cutoff time to leave the cluster
	//in a consistent state.
	return s.revertToCutoverTimestamp(resumeCtx, execCtx)
}

// revertToCutoverTimestamp reads the job progress for the cutover time and
// issues a RevertRangeRequest with the target time set to that cutover time, to
// bring the ingesting cluster to a consistent state.
func (s *streamIngestionResumer) revertToCutoverTimestamp(
	ctx context.Context, execCtx interface{},
) error {
	p := execCtx.(sql.JobExecContext)
	db := p.ExecCfg().DB
	j, err := p.ExecCfg().JobRegistry.LoadJob(ctx, *s.job.ID())
	if err != nil {
		return err
	}
	details := j.Details()
	var sd jobspb.StreamIngestionDetails
	var ok bool
	if sd, ok = details.(jobspb.StreamIngestionDetails); !ok {
		return errors.Newf("unknown details type %T in stream ingestion job %d",
			details, *s.job.ID())
	}
	progress := j.Progress()
	var sp *jobspb.Progress_StreamIngest
	if sp, ok = progress.GetDetails().(*jobspb.Progress_StreamIngest); !ok {
		return errors.Newf("unknown progress type %T in stream ingestion job %d",
			j.Progress().Progress, *s.job.ID())
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
				TargetTime: sp.StreamIngest.CutoverTime,
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
// leftover in the keyspace even after the ClearRange is issued. In general the
// tenant keyspace of a failed/canceled ingestion job should be treated as
// corrupted, and the tenant should be dropped before resuming the ingestion.
func (s *streamIngestionResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(sql.JobExecContext)
	db := p.ExecCfg().DB
	j, err := p.ExecCfg().JobRegistry.LoadJob(ctx, *s.job.ID())
	if err != nil {
		return err
	}
	details := j.Details()
	var sd jobspb.StreamIngestionDetails
	var ok bool
	if sd, ok = details.(jobspb.StreamIngestionDetails); !ok {
		return errors.Newf("unknown details type %T in stream ingestion job %d",
			details, *s.job.ID())
	}
	var b kv.Batch
	b.AddRawRequest(&roachpb.ClearRangeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    sd.Span.Key,
			EndKey: sd.Span.EndKey,
		},
	})
	return db.Run(ctx, &b)
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
