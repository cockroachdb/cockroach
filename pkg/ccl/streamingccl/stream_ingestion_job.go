// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type streamIngestionResumer struct {
	job *jobs.Job
}

func ingest(
	ctx context.Context,
	execCtx sql.JobExecContext,
	streamAddress streamclient.PartitionAddress,
	job *jobs.Job,
) error {
	// Initialize a stream client and resolve topology.
	client := streamclient.NewStreamClient()
	sa := streamclient.StreamAddress(streamAddress)
	topology, err := client.GetTopology(sa)
	if err != nil {
		return err
	}

	evalCtx := execCtx.ExtendedEvalContext()
	dsp := execCtx.DistSQLPlanner()

	planCtx, nodes, err := dsp.SetupAllNodesPlanning(ctx, evalCtx, execCtx.ExecCfg())
	if err != nil {
		return err
	}

	// Construct stream ingestion processor specs.
	streamIngestionSpecs, err := distStreamIngestionPlanSpecs(topology, nodes)
	if err != nil {
		return err
	}

	// Plan and run the DistSQL flow.
	// TODO: Updates from this flow need to feed back into the job to update the
	// progress.
	err = distStreamIngest(ctx, execCtx, nodes, planCtx, dsp, streamIngestionSpecs)
	if err != nil {
		return err
	}

	return nil
}

// Resume is part of the jobs.Resumer interface.
func (s *streamIngestionResumer) Resume(
	ctx context.Context, execCtx interface{}, resultsCh chan<- tree.Datums,
) error {
	details := s.job.Details().(jobspb.StreamIngestionDetails)
	p := execCtx.(sql.JobExecContext)

	err := ingest(ctx, p, streamclient.PartitionAddress(details.StreamAddress), s.job)
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
