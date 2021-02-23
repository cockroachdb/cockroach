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
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
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
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	stopPoller chan struct{},
	cancelIngestionCtx func(),
) error {
	sv := &execCfg.Settings.SV
	registry := execCfg.JobRegistry
	tick := time.NewTicker(cutoverSignalPollInterval.Get(sv))
	defer tick.Stop()
	for {
		select {
		case <-stopPoller:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			j, err := registry.LoadJob(ctx, *s.job.ID())
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
			if !sp.StreamIngest.CutoverTime.IsEmpty() {
				// Sanity check that the requested cutover time is less than equal to
				// the resolved ts recorded in the job progress. This should already
				// have been enforced when the cutover was signaled via the builtin.
				// TODO(adityamaru): Remove this when we allow users to specify a
				// cutover time in the future.
				resolvedTimestamp := progress.GetHighWater()
				if resolvedTimestamp == nil {
					return errors.AssertionFailedf("cutover has been requested before job %d has had a chance to"+
						" record a resolved ts", *s.job.ID())
				}
				if resolvedTimestamp.Less(sp.StreamIngest.CutoverTime) {
					return errors.AssertionFailedf("requested cutover time %s is before the resolved time %s recorded"+
						" in job %d", sp.StreamIngest.CutoverTime.String(), resolvedTimestamp.String(),
						*s.job.ID())
				}
				cancelIngestionCtx()
				return nil
			}
		}
	}
}

// Resume is part of the jobs.Resumer interface.
func (s *streamIngestionResumer) Resume(resumeCtx context.Context, execCtx interface{}) error {
	details := s.job.Details().(jobspb.StreamIngestionDetails)
	p := execCtx.(sql.JobExecContext)

	// ingestCtx is used to plan and run the DistSQL flow.
	ingestCtx, cancelIngest := context.WithCancel(resumeCtx)
	defer cancelIngest()
	g := ctxgroup.WithContext(ingestCtx)

	// Start a poller to check if the job has been requested to cutover.
	stopPoller := make(chan struct{})
	g.GoCtx(func(ctx context.Context) error {
		return s.checkForCutoverSignal(ctx, p.ExecCfg(), stopPoller, cancelIngest)
	})

	// Start ingesting KVs from the replication stream.
	g.GoCtx(func(ctx context.Context) error {
		defer close(stopPoller)
		return ingest(ctx, p, details.StreamAddress, s.job.Progress(), *s.job.ID())
	})

	if err := g.Wait(); err != nil {
		// Check if the ingestCtx has been canceled while the resumeCtx does not
		// have an error set on it. This is only possible if the resumer observed a
		// cutover and explicitly requested a teardown via the ingestCtx, in which
		// case we should revert the data to the cutover time to get the cluster
		// into a consistent state.
		// In all other cases we should treat the context cancellation as an error.
		if errors.Is(err, context.Canceled) && resumeCtx.Err() == nil {
			return s.revertToLatestResolvedTimestamp(resumeCtx, execCtx)
		}
		return err
	}

	return nil
}

// revertToLatestResolvedTimestamp reads the job progress for the cutover time
// and issues a RevertRangeRequest with the target time set to that cutover
// time, to bring the ingesting cluster to a consistent state.
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
	var sp *jobspb.Progress_StreamIngest
	if sp, ok = progress.GetDetails().(*jobspb.Progress_StreamIngest); !ok {
		return errors.Newf("unknown progress type %T in stream ingestion job %d",
			j.Progress().Progress, *s.job.ID())
	}

	if sp.StreamIngest.CutoverTime.IsEmpty() {
		return errors.AssertionFailedf("cutover time is unexpectedly empty, " +
			"cannot revert to a consistent state")
	}

	var b kv.Batch
	b.AddRawRequest(&roachpb.RevertRangeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    sd.Span.Key,
			EndKey: sd.Span.EndKey,
		},
		TargetTime:                          sp.StreamIngest.CutoverTime,
		EnableTimeBoundIteratorOptimization: true,
	})
	b.Header.MaxSpanRequestKeys = sql.RevertTableDefaultBatchSize

	return db.Run(ctx, &b)
}

// OnFailOrCancel is part of the jobs.Resumer interface.
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
