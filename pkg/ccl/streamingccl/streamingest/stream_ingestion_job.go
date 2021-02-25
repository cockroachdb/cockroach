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
	jobID jobspb.JobID,
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
		streamAddress, topology, nodes, initialHighWater)
	if err != nil {
		return err
	}

	// Plan and run the DistSQL flow.
	err = distStreamIngest(ctx, execCtx, nodes, jobID, planCtx, dsp, streamIngestionSpecs,
		streamIngestionFrontierSpec)
	if err != nil {
		return err
	}

	return nil
}

// Resume is part of the jobs.Resumer interface.
func (s *streamIngestionResumer) Resume(ctx context.Context, execCtx interface{}) error {
	details := s.job.Details().(jobspb.StreamIngestionDetails)
	p := execCtx.(sql.JobExecContext)

	err := ingest(ctx, p, details.StreamAddress, s.job.Progress(),
		s.job.ID())
	if err != nil {
		return err
	}

	// TODO(adityamaru): We probably want to use the resultsCh to indicate that
	// the processors have completed setup. We can then return the job ID in the
	// plan hook similar to how changefeeds do it.

	return nil
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (s *streamIngestionResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(sql.JobExecContext)
	db := p.ExecCfg().DB
	details := s.job.Details().(jobspb.StreamIngestionDetails)

	resolvedTime := details.StartTime
	prog := s.job.Progress()
	if highWatermark := prog.GetHighWater(); highWatermark != nil {
		if highWatermark.Less(resolvedTime) {
			return errors.Newf("progress timestamp %+v cannot be older than start time %+v",
				highWatermark, resolvedTime)
		}
		resolvedTime = *highWatermark
	}

	// TODO(adityamaru): If the job progress was not set then we should
	// probably ClearRange. Take this into account when writing the ClearRange
	// OnFailOrCancel().
	if resolvedTime.IsEmpty() {
		return nil
	}

	var b kv.Batch
	b.AddRawRequest(&roachpb.RevertRangeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    details.Span.Key,
			EndKey: details.Span.EndKey,
		},
		TargetTime:                          resolvedTime,
		EnableTimeBoundIteratorOptimization: true,
	})
	b.Header.MaxSpanRequestKeys = sql.RevertTableDefaultBatchSize
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
