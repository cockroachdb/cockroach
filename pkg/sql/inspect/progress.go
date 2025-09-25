// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

type inspectProgressTracker struct {
	job             *jobs.Job
	totalChecks     int64
	completedChecks atomic.Int64

	// Update timing
	fractionInterval time.Duration
	lastUpdateTime   time.Time

	// Background goroutine management
	stopper *stop.Stopper
	mu      struct {
		syncutil.Mutex
		stopped bool
	}
}

// initJobProgress initializes the job progress to 0% completed.
func (p *inspectProgressTracker) initJobProgress(ctx context.Context, totalCheckCount int64) error {
	p.totalChecks = totalCheckCount
	// TODO(148297): when we have checkpointing, we should load existing progress here
	// and not reset it to 0 if the job is resuming.
	p.completedChecks.Store(0)

	return p.job.NoTxn().Update(ctx,
		func(_ isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			progress := md.Progress
			progress.Progress = &jobspb.Progress_FractionCompleted{
				FractionCompleted: 0,
			}
			progress.Details = &jobspb.Progress_Inspect{
				Inspect: &jobspb.InspectProgress{
					JobTotalCheckCount:     totalCheckCount,
					JobCompletedCheckCount: 0,
				},
			}
			ju.UpdateProgress(progress)
			return nil
		},
	)
}

// handleProcessorProgress processes processor progress metadata and updates the job progress.
func (p *inspectProgressTracker) handleProcessorProgress(
	ctx context.Context, meta *execinfrapb.ProducerMetadata,
) error {
	if meta.BulkProcessorProgress == nil {
		return nil
	}

	var incomingProcProgress jobspb.InspectProcessorProgress
	if err := pbtypes.UnmarshalAny(&meta.BulkProcessorProgress.ProgressDetails, &incomingProcProgress); err != nil {
		return errors.Wrapf(err, "unable to unmarshal inspect progress details")
	}

	// Update completed checks count
	if incomingProcProgress.ChecksCompleted > 0 {
		newCompleted := p.completedChecks.Add(incomingProcProgress.ChecksCompleted)
		log.Dev.Infof(ctx, "INSPECT progress: %d/%d checks completed", newCompleted, p.totalChecks)
	}

	// If processor is finished, update job progress immediately
	if incomingProcProgress.Finished {
		return p.updateJobProgress(ctx)
	}

	return nil
}

// updateJobProgress writes the current progress fraction to the job table.
func (p *inspectProgressTracker) updateJobProgress(ctx context.Context) error {
	completed := p.completedChecks.Load()
	var fraction float32 = 0
	if p.totalChecks > 0 {
		fraction = float32(completed) / float32(p.totalChecks)
	}

	if fraction > 1.0 {
		return errors.AssertionFailedf("progress fraction %.2f exceeds 1.0 (completed=%d, total=%d)",
			fraction, completed, p.totalChecks)
	}

	err := p.job.NoTxn().Update(ctx,
		func(_ isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			progress := md.Progress
			progress.Progress = &jobspb.Progress_FractionCompleted{
				FractionCompleted: fraction,
			}
			if inspectDetails, ok := progress.Details.(*jobspb.Progress_Inspect); ok {
				inspectDetails.Inspect.JobCompletedCheckCount = completed
			}
			ju.UpdateProgress(progress)
			return nil
		},
	)

	if err == nil {
		p.lastUpdateTime = timeutil.Now()
		log.Dev.Infof(ctx, "INSPECT progress updated: %.2f%% (%d/%d checks)",
			fraction*100, completed, p.totalChecks)
	}

	return err
}

// startBackgroundUpdates launches a background goroutine that periodically updates job progress.
func (p *inspectProgressTracker) startBackgroundUpdates(ctx context.Context) {
	_ = p.stopper.RunAsyncTask(ctx, "inspect-progress-updater", func(ctx context.Context) {
		ticker := time.NewTicker(p.fractionInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-p.stopper.ShouldQuiesce():
				return
			case <-ticker.C:
				p.mu.Lock()
				stopped := p.mu.stopped
				p.mu.Unlock()

				if stopped {
					return
				}

				if err := p.updateJobProgress(ctx); err != nil {
					log.Dev.Warningf(ctx, "failed to update INSPECT progress: %v", err)
				}
			}
		}
	})
}

// terminateTracker stops the background progress update goroutine.
func (p *inspectProgressTracker) terminateTracker(ctx context.Context) {
	p.mu.Lock()
	p.mu.stopped = true
	p.mu.Unlock()

	p.stopper.Stop(ctx)
}

// newInspectProgressTracker creates a new progress tracker for the given job.
func newInspectProgressTracker(
	job *jobs.Job, fractionInterval time.Duration,
) *inspectProgressTracker {
	return &inspectProgressTracker{
		job:              job,
		fractionInterval: fractionInterval,
		lastUpdateTime:   timeutil.Now(),
		stopper:          stop.NewStopper(),
	}
}

// countApplicableChecks determines how many checks will actually run across all spans.
// This provides accurate progress calculation by only counting checks that apply to each span.
func countApplicableChecks(
	pkSpans []roachpb.Span, checkers []inspectCheckApplicability, codec keys.SQLCodec,
) (int64, error) {
	var totalApplicableChecks int64 = 0

	for _, span := range pkSpans {
		for _, checker := range checkers {
			applies, err := checker.AppliesTo(codec, span)
			if err != nil {
				return 0, err
			}
			if applies {
				totalApplicableChecks++
			}
		}
	}

	return totalApplicableChecks, nil
}
