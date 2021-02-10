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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// streamContext is a custom context that allows setting custom errors on
// context cancellation. These custom errors can then be special cased to run
// the relevant cleanup logic.
type streamContext struct {
	context.Context
	childCtxCancel context.CancelFunc

	mu        syncutil.Mutex // protects the following field
	customErr error
}

var (
	errCutoverRequested = errors.New("cutover requested")
)

func newStreamContext(parentCtx context.Context) *streamContext {
	childCtx, childCtxCancel := context.WithCancel(parentCtx)
	return &streamContext{
		Context:        childCtx,
		childCtxCancel: childCtxCancel,
	}
}

// Err implements the context.Context interface.
func (sc *streamContext) Err() error {
	var err error
	sc.mu.Lock()
	err = sc.customErr
	if err == nil {
		err = sc.Context.Err()
	}
	sc.mu.Unlock()
	return err
}

// cutover sets a custom error indicating that the job has been signaled to
// complete.
func (sc *streamContext) cutover() {
	sc.mu.Lock()
	sc.customErr = errors.Mark(context.Canceled, errCutoverRequested)
	sc.mu.Unlock()
}

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
	streamIngestionSpecs, streamIngestionFrontierSpec, err := distStreamIngestionPlanSpecs(
		streamAddress, topology, nodes, initialHighWater)
	if err != nil {
		return err
	}

	// Plan and run the DistSQL flow.
	return distStreamIngest(ctx, execCtx, nodes, jobID, planCtx, dsp, streamIngestionSpecs,
		streamIngestionFrontierSpec)
}

// checkForCutoverSignal periodically loads the job progress to check for the
// sentinel value that signals the ingestion job to complete.
func (s *streamIngestionResumer) checkForCutoverSignal(
	streamCtx *streamContext, stopPoller chan struct{}, registry *jobs.Registry,
) error {
	tick := time.NewTicker(time.Second * 10)
	defer tick.Stop()
	for {
		select {
		case <-stopPoller:
			return nil
		case <-tick.C:
			j, err := registry.LoadJob(streamCtx, *s.job.ID())
			if err != nil {
				return err
			}
			progress := j.Progress()
			var sp *jobspb.Progress_StreamIngest
			var ok bool
			if sp, ok = progress.GetDetails().(*jobspb.Progress_StreamIngest); !ok {
				return errors.Newf("unknown progress type %T in stream ingestion job %d",
					j.Progress().Progress, *s.job.ID())
			}
			// Job has been signaled to complete.
			if sp.StreamIngest.MarkedForCompletion {
				streamCtx.cutover()
				streamCtx.childCtxCancel()
				return nil
			}
		}
	}
}

// Resume is part of the jobs.Resumer interface.
func (s *streamIngestionResumer) Resume(ctx context.Context, execCtx interface{}) error {
	details := s.job.Details().(jobspb.StreamIngestionDetails)
	p := execCtx.(sql.JobExecContext)

	// Create a custom streamContext that is plumbed through the flow. Refer to
	// comments on streamContext methods for more information.
	streamCtx := newStreamContext(ctx)

	// Start a poller to check if the job has been requested to cutover.
	stopPoller := make(chan struct{})
	g := ctxgroup.WithContext(streamCtx)
	g.GoCtx(func(ctx context.Context) error {
		// TODO: Do we need to cancel the streamCtx.childCtx if this call runs into
		// an error?
		return s.checkForCutoverSignal(streamCtx, stopPoller, p.ExecCfg().JobRegistry)
	})

	g.GoCtx(func(ctx context.Context) error {
		defer close(stopPoller)
		return ingest(streamCtx, p, details.StreamAddress, s.job.Progress(), *s.job.ID())
	})

	if err := g.Wait(); err != nil {
		if errors.Is(err, errCutoverRequested) {
			return s.revertToLatestResolvedTimestamp(ctx, execCtx)
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

	resolvedTime := sd.StartTime
	if highWatermark := progress.GetHighWater(); highWatermark != nil {
		if highWatermark.Less(resolvedTime) {
			return errors.Newf("progress timestamp %+v cannot be older than start time %+v",
				highWatermark, resolvedTime)
		}
		resolvedTime = *highWatermark
	}
	var b kv.Batch
	b.AddRawRequest(&roachpb.RevertRangeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    sd.Span.Key,
			EndKey: sd.Span.EndKey,
		},
		TargetTime:                          resolvedTime,
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
