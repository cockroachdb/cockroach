// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobfrontier"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

// Name for the INSPECT completed spans frontier in jobfrontier.
const inspectCompletedSpansKey = "inspect_completed_spans"

// inspectProgressTracker tracks INSPECT job progress with span checkpointing.
// It maintains completed spans to enable job restarts without reprocessing.
// Progress updates occur at two frequencies: fraction complete updates happen
// more frequently than full checkpoint updates to minimize writes.
type inspectProgressTracker struct {
	job                *jobs.Job
	jobID              jobspb.JobID
	clock              timeutil.TimeSource
	checkpointInterval func() time.Duration
	fractionInterval   func() time.Duration
	internalDB         isql.DB

	mu struct {
		syncutil.Mutex
		// cachedProgress holds the latest progress update from processors.
		cachedProgress *jobspb.Progress
		// completedSpans tracks all completed spans with automatic deduplication.
		completedSpans roachpb.SpanGroup
	}

	// Goroutine management.
	stopFunc func()
}

func newInspectProgressTracker(
	job *jobs.Job, sv *settings.Values, internalDB isql.DB,
) *inspectProgressTracker {
	return &inspectProgressTracker{
		job:                job,
		jobID:              job.ID(),
		clock:              timeutil.DefaultTimeSource{},
		fractionInterval:   func() time.Duration { return fractionUpdateInterval.Get(sv) },
		checkpointInterval: func() time.Duration { return checkpointInterval.Get(sv) },
		internalDB:         internalDB,
	}
}

// loadCompletedSpansFromStorage loads completed spans from jobfrontier.
func (t *inspectProgressTracker) loadCompletedSpansFromStorage(
	ctx context.Context,
) ([]roachpb.Span, error) {
	var completedSpans []roachpb.Span

	err := t.internalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		resolvedSpans, found, err := jobfrontier.GetResolvedSpans(ctx, txn, t.jobID, inspectCompletedSpansKey)
		if err != nil {
			return err
		}
		if !found {
			return nil // No completed spans stored yet
		}

		// Extract just the spans (we don't care about timestamps for INSPECT).
		completedSpans = make([]roachpb.Span, len(resolvedSpans))
		for i, rs := range resolvedSpans {
			completedSpans[i] = rs.Span
		}
		return nil
	})

	return completedSpans, err
}

// initTracker sets up the progress tracker and returns completed spans from any previous
// job execution. This should be called before planning to enable span optimization.
func (t *inspectProgressTracker) initTracker(ctx context.Context) ([]roachpb.Span, error) {
	completedSpans, err := t.loadCompletedSpansFromStorage(ctx)
	if err != nil {
		return nil, err
	}

	// Add loaded spans to our cached SpanGroup.
	if len(completedSpans) > 0 {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.mu.completedSpans.Add(completedSpans...)
		log.Dev.Infof(ctx, "INSPECT job restarting with %d existing completed spans", len(completedSpans))
	}

	return completedSpans, nil
}

// initJobProgress writes the initial job progress to the job record with the correct
// check counts determined after planning. This should be called after planning.
func (t *inspectProgressTracker) initJobProgress(
	ctx context.Context, totalCheckCount int64, completedCheckCount int64,
) error {
	inspectProgress := &jobspb.InspectProgress{
		JobTotalCheckCount:     totalCheckCount,
		JobCompletedCheckCount: completedCheckCount,
	}

	// Calculate initial fraction complete based on check counts.
	var fractionComplete float32
	if totalCheckCount > 0 {
		fractionComplete = float32(completedCheckCount) / float32(totalCheckCount)
	}

	progress := &jobspb.Progress{
		Details: &jobspb.Progress_Inspect{Inspect: inspectProgress},
		Progress: &jobspb.Progress_FractionCompleted{
			FractionCompleted: fractionComplete,
		},
	}

	func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.mu.cachedProgress = progress
	}()
	if err := t.flushProgress(ctx); err != nil {
		return err
	}

	// Start the progress update goroutines.
	t.stopFunc = t.startPeriodicUpdates(ctx)
	return nil
}

// handleProgressUpdate handles incoming processor metadata and performs any necessary
// job updates. Determines how to handle the update (immediate vs deferred).
func (t *inspectProgressTracker) handleProgressUpdate(
	ctx context.Context, meta *execinfrapb.ProducerMetadata,
) error {
	needsImmediatePersistence, err := t.updateProgressCache(meta)
	if err != nil {
		return err
	}

	// If updateProgressCache returned true (indicating immediate persistence needed),
	// flush the progress to storage. This happens when a processor signals completion (Drained=true).
	if needsImmediatePersistence {
		// Checkpoints are stored separately from general progress, so we need to
		// flush them separately.
		if err := t.flushCheckpointUpdate(ctx); err != nil {
			return err
		}
		if err := t.flushProgress(ctx); err != nil {
			return err
		}
	}

	// Otherwise, the background goroutines will handle the persistence
	return nil
}

// updateProgressCache computes updated job progress from processor metadata and updates
// the internal cache. Returns true when immediate persistence is needed.
func (t *inspectProgressTracker) updateProgressCache(
	meta *execinfrapb.ProducerMetadata,
) (bool, error) {
	if meta.BulkProcessorProgress == nil {
		return false, nil
	}

	var incomingProcProgress jobspb.InspectProcessorProgress
	if err := pbtypes.UnmarshalAny(&meta.BulkProcessorProgress.ProgressDetails, &incomingProcProgress); err != nil {
		return false, errors.Wrapf(err, "unable to unmarshal inspect progress details")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.mu.cachedProgress == nil {
		return false, errors.AssertionFailedf("progress not initialized")
	}

	orig := t.mu.cachedProgress.GetInspect()
	if orig == nil {
		return false, errors.AssertionFailedf("cached progress does not contain Inspect details")
	}
	inspectProgress := protoutil.Clone(orig).(*jobspb.InspectProgress)

	// The CompletedSpans in the progress message is the delta of spans
	// completed since the last progress update. Add them to our SpanGroup for
	// automatic deduplication and merging.
	if len(meta.BulkProcessorProgress.CompletedSpans) > 0 {
		t.mu.completedSpans.Add(meta.BulkProcessorProgress.CompletedSpans...)
	}

	// Update check count.
	if incomingProcProgress.ChecksCompleted > 0 {
		inspectProgress.JobCompletedCheckCount += incomingProcProgress.ChecksCompleted
	}

	// Update cached progress - the goroutine will handle actual persistence.
	t.mu.cachedProgress = &jobspb.Progress{
		Details: &jobspb.Progress_Inspect{
			Inspect: inspectProgress,
		},
	}

	// If the incoming progress indicates the processor is drained, we tell the
	// caller that progress needs to be written immediately. This ensures we
	// capture the final state when a processor completes its work, rather than
	// waiting for the next periodic checkpoint update.
	return meta.BulkProcessorProgress.Drained, nil
}

// terminateTracker performs any necessary cleanup when the job completes or fails.
func (t *inspectProgressTracker) terminateTracker() {
	if t.stopFunc != nil {
		t.stopFunc()
		t.stopFunc = nil
	}
}

// startPeriodicUpdates launches background goroutines to periodically flush
// progress updates at different intervals. Returns a stop function to terminate
// the goroutines and wait for their completion.
func (t *inspectProgressTracker) startPeriodicUpdates(ctx context.Context) (stop func()) {
	stopCh := make(chan struct{})
	runPeriodicWrite := func(
		ctx context.Context,
		write func(context.Context) error,
		interval func() time.Duration,
	) error {
		timer := t.clock.NewTimer()
		defer timer.Stop()
		for {
			timer.Reset(interval())
			select {
			case <-stopCh:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.Ch():
				if err := write(ctx); err != nil {
					log.Dev.Warningf(ctx, "could not flush progress: %v", err)
				}
			}
		}
	}

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		return runPeriodicWrite(
			ctx, t.flushProgress, t.fractionInterval)
	})
	g.GoCtx(func(ctx context.Context) error {
		return runPeriodicWrite(
			ctx, t.flushCheckpointUpdate, t.checkpointInterval)
	})

	toClose := stopCh // make the returned function idempotent
	return func() {
		if toClose != nil {
			close(toClose)
			toClose = nil
		}
		if err := g.Wait(); err != nil {
			log.Dev.Warningf(ctx, "waiting for progress flushing goroutines: %v", err)
		}
	}
}

// flushProgress updates the complete progress including processor details and fraction complete.
func (t *inspectProgressTracker) flushProgress(ctx context.Context) error {
	if t.job == nil { // Job can be nil for tests.
		return nil
	}

	var cachedInspectProgress *jobspb.InspectProgress
	func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		cachedInspectProgress = t.mu.cachedProgress.GetInspect()
	}()

	// Calculate fraction complete based on check counts from cached progress.
	var fractionComplete float32
	if cachedInspectProgress.JobTotalCheckCount > 0 {
		fractionComplete = float32(cachedInspectProgress.JobCompletedCheckCount) / float32(cachedInspectProgress.JobTotalCheckCount)
	}

	return t.job.NoTxn().Update(ctx, func(_ isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		newProgress := &jobspb.Progress{
			Details: &jobspb.Progress_Inspect{
				Inspect: cachedInspectProgress,
			},
			Progress: &jobspb.Progress_FractionCompleted{
				FractionCompleted: fractionComplete,
			},
		}
		ju.UpdateProgress(newProgress)
		return nil
	})
}

// flushCheckpointUpdate performs a progress update to include completed spans.
// The completed spans are stored via jobfrontier.
func (t *inspectProgressTracker) flushCheckpointUpdate(ctx context.Context) error {
	var completedSpans []roachpb.Span
	func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		completedSpans = t.mu.completedSpans.Slice()
	}()

	// If no completed spans, nothing to store.
	if len(completedSpans) == 0 {
		return nil
	}

	return t.internalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// Create a frontier for the spans (using zero timestamps since INSPECT doesn't need timing).
		frontier, err := span.MakeFrontier(completedSpans...)
		if err != nil {
			return err
		}
		defer frontier.Release()
		return jobfrontier.Store(ctx, txn, t.jobID, inspectCompletedSpansKey, frontier)
	})
}

// getCachedCheckCounts returns the current cached check counts (total and completed).
// This is useful for testing to verify progress state without accessing the mutex directly.
func (t *inspectProgressTracker) getCachedCheckCounts() (totalChecks int64, completedChecks int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.mu.cachedProgress == nil {
		return 0, 0
	}

	inspectProgress := t.mu.cachedProgress.GetInspect()
	if inspectProgress == nil {
		return 0, 0
	}

	return inspectProgress.JobTotalCheckCount, inspectProgress.JobCompletedCheckCount
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
