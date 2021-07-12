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
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamingutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type streamIngestionResumer struct {
	job *jobs.Job
}

func (s *streamIngestionResumer) ingest(
	ctx context.Context, execCtx sql.JobExecContext, resultCh chan error, ingestionDone chan struct{},
) error {
	// Fetch a stream address that we can establish connection with
	streamAddress := s.getStreamAddress()
	details := s.job.Details().(jobspb.StreamIngestionDetails)

	// Initialize a stream client and resolve topology.
	client, err := streamclient.NewStreamClient(streamAddress)
	if err != nil {
		return err
	}
	topology, err := client.GetTopology(streamAddress)
	if err != nil {
		return err
	}
	eventCh, err := client.ConsumeGeneration()
	if err != nil {
		return err
	}

	go func() {
		err := s.planDistFlow(ctx, execCtx, details.StartTime, streamAddress, topology)
		select {
		case <-ingestionDone:
		default:
			resultCh <- err
		}
	}()

	go func() {
		err := checkForGenerationEvent(ctx, execCtx, eventCh, int(s.job.ID()))
		if err == nil {
			return
		}
		select {
		case <-ingestionDone:
		default:
			resultCh <- err
		}
	}()

	return nil
}

// Resume is part of the jobs.Resumer interface.
func (s *streamIngestionResumer) Resume(resumeCtx context.Context, execCtx interface{}) error {
	errCh := make(chan error)
	defer close(errCh)

	ingestionDone := make(chan struct{}, 1)
	markIngestionDone := func() {
		ingestionDone <- struct{}{}
		close(ingestionDone)
	}
	defer markIngestionDone()

	jobID := s.job.ID()
	p := execCtx.(sql.JobExecContext)

	// Start ingesting KVs from the replication stream.
	err := s.ingest(resumeCtx, p, errCh, ingestionDone)
	if err != nil {
		return err
	}

	for {
		select {
		case err := <-errCh:
			if err != nil {
				return err
			}

			sp, _, err := getJobProgress(resumeCtx, p, jobID)
			if err != nil {
				return err
			}
			switch sp.StreamIngest.CutoverTriggeredBy {
			case jobspb.StreamIngestionProgress_Generation:
				err = s.ingest(resumeCtx, p, errCh, ingestionDone)
				if err != nil {
					return err
				}
			case jobspb.StreamIngestionProgress_JobCompletion:
				// A nil error is only possible if the job was signaled to cutover and the
				// processors shut down gracefully, i.e stopped ingesting any additional
				// events from the replication stream. At this point it is safe to revert to
				// the cutoff time to leave the cluster in a consistent state.
				return s.revertToCutoverTimestamp(resumeCtx, execCtx)
			default:
				return errors.Newf("unknown cutover trigger event type: %v", sp.StreamIngest.CutoverTriggeredBy)
			}
		}
	}
}

// revertToCutoverTimestamp reads the job progress for the cutover time and
// issues a RevertRangeRequest with the target time set to that cutover time, to
// bring the ingesting cluster to a consistent state.
func (s *streamIngestionResumer) revertToCutoverTimestamp(
	ctx context.Context, execCtx interface{},
) error {
	p := execCtx.(sql.JobExecContext)
	db := p.ExecCfg().DB
	sp, sd, err := getJobProgress(ctx, p, s.job.ID())
	if err != nil {
		return err
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

// TODO(azhu): round-robin all stream addresses
func (s *streamIngestionResumer) getStreamAddress() streamingccl.StreamAddress {
	details := s.job.Details().(jobspb.StreamIngestionDetails)
	return streamingccl.StreamAddress(details.StreamAddress)
}

func getJobProgress(
	ctx context.Context, execCtx sql.JobExecContext, jobID jobspb.JobID,
) (*jobspb.Progress_StreamIngest, *jobspb.StreamIngestionDetails, error) {
	j, err := execCtx.ExecCfg().JobRegistry.LoadJob(ctx, jobID)
	if err != nil {
		return nil, nil, err
	}
	details := j.Details()
	var sd jobspb.StreamIngestionDetails
	var ok bool
	if sd, ok = details.(jobspb.StreamIngestionDetails); !ok {
		return nil, nil, errors.Newf("unknown details type %T in stream ingestion"+
			" job %d", details, jobID)
	}
	progress := j.Progress()
	var sp *jobspb.Progress_StreamIngest
	if sp, ok = progress.GetDetails().(*jobspb.Progress_StreamIngest); !ok {
		return nil, nil, errors.New("unknown progress type")
	}
	return sp, &sd, nil
}

func checkForGenerationEvent(
	ctx context.Context, execCtx interface{}, eventCh chan streamingccl.Event, jobID int,
) error {
	for {
		select {
		case event, ok := <-eventCh:
			if !ok {
				return nil
			}
			switch event.Type() {
			case streamingccl.GenerationEvent:
				log.Infof(ctx,
					"stream ingestion job %d switching over to the next generation",
					jobID)
				p := execCtx.(sql.JobExecContext)
				evalCtx := p.ExtendedEvalContext().EvalContext
				txn := p.ExtendedEvalContext().Txn
				return streamingutils.DoGenerationSwitchover(&evalCtx, txn, jobID)
			default:
				return errors.Newf("unknown streaming event type %v", event.Type())
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *streamIngestionResumer) planDistFlow(
	ctx context.Context,
	execCtx sql.JobExecContext,
	startTime hlc.Timestamp,
	streamAddress streamingccl.StreamAddress,
	topology streamingccl.Topology,
) error {
	// TODO(adityamaru): If the job is being resumed it is possible that it has
	// check-pointed a resolved ts up to which all of its processors had ingested
	// KVs. We can skip to ingesting after this resolved ts. Plumb the
	// initialHighwatermark to the ingestion processor spec based on what we read
	// from the job progress.
	initialHighWater := startTime
	progress := s.job.Progress()
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
	jobID := s.job.ID()
	streamIngestionSpecs, streamIngestionFrontierSpec, err := distStreamIngestionPlanSpecs(
		streamAddress, topology, nodes, initialHighWater, jobID)
	if err != nil {
		return err
	}

	return distStreamIngest(ctx, execCtx, nodes, jobID, planCtx, dsp, streamIngestionSpecs, streamIngestionFrontierSpec)
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
