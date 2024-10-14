// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// For both backups and restores, we compute progress as the number of completed
// export or import requests, respectively, divided by the total number of
// requests. To avoid hammering the system.jobs table, when a response comes
// back, we issue a progress update only if a) it's been a duration of
// progressTimeThreshold since the last update, or b) the difference between the
// last logged fractionCompleted and the current fractionCompleted is more than
// progressFractionThreshold.
var (
	progressTimeThreshold             = 15 * time.Second
	progressFractionThreshold float32 = 0.05
)

// TestingSetProgressThresholds overrides batching limits to update more often.
func TestingSetProgressThresholds() func() {
	oldFraction := progressFractionThreshold
	oldDuration := progressTimeThreshold

	progressFractionThreshold = 0.0001
	progressTimeThreshold = time.Microsecond

	return func() {
		progressFractionThreshold = oldFraction
		progressTimeThreshold = oldDuration
	}
}

// ChunkProgressLogger is a helper for managing the progress state on a job. For
// a given job, it assumes there are some number of chunks of work to do and
// tracks the completion progress as chunks are reported as done (via Loop).
// It then updates the actual job periodically using a ProgressUpdateBatcher.
type ChunkProgressLogger struct {
	// These fields must be externally initialized.
	expectedChunks  int
	completedChunks int

	batcher ProgressUpdateBatcher
}

// ProgressUpdateOnly is for use with NewChunkProgressLogger to just update job
// progress fraction (ie. when a custom func with side-effects is not needed).
var ProgressUpdateOnly func(context.Context, jobspb.ProgressDetails)

func NewChunkProgressLoggerForJob(
	j *Job,
	expectedChunks int,
	startFraction float32,
	progressedFn func(context.Context, jobspb.ProgressDetails),
) *ChunkProgressLogger {
	return NewChunkProgressLogger(
		func(ctx context.Context, pct float32) error {
			return j.NoTxn().FractionProgressed(ctx, func(ctx context.Context, details jobspb.ProgressDetails) float32 {
				if progressedFn != nil {
					progressedFn(ctx, details)
				}
				return pct
			})
		}, expectedChunks, startFraction)
}

// NewChunkProgressLogger returns a ChunkProgressLogger.
func NewChunkProgressLogger(
	report func(ctx context.Context, pct float32) error, expectedChunks int, startFraction float32,
) *ChunkProgressLogger {
	return &ChunkProgressLogger{
		expectedChunks: expectedChunks,
		batcher: ProgressUpdateBatcher{
			perChunkContribution: (1.0 - startFraction) * 1.0 / float32(expectedChunks),
			start:                startFraction,
			reported:             startFraction,
			Report:               report,
		},
	}
}

// chunkFinished marks one chunk of the job as completed. If either the time or
// fraction threshold has been reached, the progress update will be persisted to
// system.jobs.
func (jpl *ChunkProgressLogger) chunkFinished(ctx context.Context) error {
	jpl.completedChunks++
	return jpl.batcher.Add(ctx)
}

// Loop calls chunkFinished for every message received over chunkCh. It exits
// when chunkCh is closed or when the context is canceled.
func (jpl *ChunkProgressLogger) Loop(ctx context.Context, chunkCh <-chan struct{}) error {
	for {
		select {
		case _, ok := <-chunkCh:
			if !ok {
				return nil
			}
			if err := jpl.chunkFinished(ctx); err != nil && !startup.IsRetryableReplicaError(err) {
				return err
			}
			if jpl.completedChunks == jpl.expectedChunks {
				if err := jpl.batcher.Done(ctx); err != nil && !startup.IsRetryableReplicaError(err) {
					return err
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ProgressUpdateBatcher is a helper for tracking progress as it is made and
// calling a progress update function when it has meaningfully advanced (e.g. by
// more than 5%), while ensuring updates also are not done too often (by default
// not less than 30s apart).
type ProgressUpdateBatcher struct {
	// Report is the function called to record progress
	Report func(context.Context, float32) error

	// The following are set at initialization time.
	// start is the starting percentage complete.
	start                float32
	perChunkContribution float32

	syncutil.Mutex

	// reported is the most recently reported value of completed
	reported  float32
	completed int
	// lastReported is when we last called report
	lastReported time.Time
}

// Add records some additional progress made and checks there has been enough
// change in the completed progress (and enough time has passed) to report the
// new progress amount.
func (p *ProgressUpdateBatcher) Add(ctx context.Context) error {
	shouldReport, completed := func() (bool, float32) {
		p.Lock()
		defer p.Unlock()
		p.completed += 1

		next := p.start + (float32(p.completed) * p.perChunkContribution)
		sReport := next-p.reported > progressFractionThreshold
		sReport = sReport || (next > p.reported && p.lastReported.Add(progressTimeThreshold).Before(timeutil.Now()))
		if sReport {
			p.reported = next
			p.lastReported = timeutil.Now()
		}
		return sReport, next
	}()
	if shouldReport {
		return p.Report(ctx, completed)
	}
	return nil
}

// Done allows the batcher to report any meaningful unreported progress, without
// worrying about update frequency now that it is done.
func (p *ProgressUpdateBatcher) Done(ctx context.Context) error {
	p.Lock()
	next := p.start + (float32(p.completed) * p.perChunkContribution)
	shouldReport := next-p.reported > progressFractionThreshold
	p.Unlock()
	if shouldReport {
		return p.Report(ctx, next)
	}
	return nil
}
