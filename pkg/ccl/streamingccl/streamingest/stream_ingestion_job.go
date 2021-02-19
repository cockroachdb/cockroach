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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type streamIngestionResumer struct {
	job *jobs.Job
}

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
	streamIngestionPollingSpec, streamIngestionSpecs, streamIngestionFrontierSpec,
		err := distStreamIngestionPlanSpecs(streamAddress, topology, nodes, initialHighWater, jobID)
	if err != nil {
		return err
	}

	// Plan and run the DistSQL flow.
	return distStreamIngest(ctx, execCtx, nodes, jobID, planCtx, dsp, streamIngestionSpecs,
		streamIngestionFrontierSpec, streamIngestionPollingSpec)
}

// Resume is part of the jobs.Resumer interface.
func (s *streamIngestionResumer) Resume(ctx context.Context, execCtx interface{}) error {
	details := s.job.Details().(jobspb.StreamIngestionDetails)
	p := execCtx.(sql.JobExecContext)

	err := ingest(ctx, p, details.StreamAddress, s.job.Progress(),
		*s.job.ID())
	if err != nil {
		if errors.Is(err, errCutoverRequested) {
			revertErr := s.revertToLatestResolvedTimestamp(ctx, execCtx)
			if revertErr != nil {
				return errors.Wrap(revertErr, "error while reverting stream ingestion job")
			}
			// The job should transition to a succeeded state once the cutover is
			// complete.

			return nil
		}
		return err
	}

	// TODO(adityamaru): We probably want to use the resultsCh to indicate that
	// the processors have completed setup. We can then return the job ID in the
	// plan hook similar to how changefeeds do it.

	return nil
}

// revertToLatestResolvedTimestamp reads the job progress for the high watermark
// and issues a RevertRangeRequest with the target time set to that high
// watermark.
func (s *streamIngestionResumer) revertToLatestResolvedTimestamp(
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
	streamIngestProgress := progress.GetStreamIngest()

	revertTargetTime := sd.StartTime
	if streamIngestProgress != nil {
		revertTargetTime = streamIngestProgress.CutoverTime
	}

	if highWatermark := progress.GetHighWater(); highWatermark != nil {
		if highWatermark.Less(revertTargetTime) {
			return errors.Newf("progress timestamp %+v cannot be older than the requested "+
				"cutover time %+v", highWatermark, revertTargetTime)
		}
	}

	// Sanity check that the resolvedTime is not less than the time at which the
	// ingestion job was started.
	if revertTargetTime.Less(sd.StartTime) {
		return errors.Newf("revert target time %+v cannot be older than the start time "+
			"cutover time %+v", revertTargetTime, sd.StartTime)
	}

	var b kv.Batch
	b.AddRawRequest(&roachpb.RevertRangeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    sd.Span.Key,
			EndKey: sd.Span.EndKey,
		},
		TargetTime:                          revertTargetTime,
		EnableTimeBoundIteratorOptimization: true,
	})
	b.Header.MaxSpanRequestKeys = sql.RevertTableDefaultBatchSize
	return db.Run(ctx, &b)
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (s *streamIngestionResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	// TODO(adityamaru): Add ClearRange logic.
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
