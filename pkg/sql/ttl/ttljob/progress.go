// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttljob

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
	"golang.org/x/sync/errgroup"
)

var checkpointInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.ttl.checkpoint_interval",
	"the amount of time between row-level TTL checkpoint updates",
	30*time.Second,
	settings.DurationWithMinimum(1*time.Millisecond),
)

var fractionUpdateInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.ttl.fraction_update_interval",
	"the amount of time between row-level TTL percent complete progress updates",
	10*time.Second,
	settings.DurationWithMinimum(1*time.Millisecond),
)

type progressTracker interface {
	// initTracker sets up the progress tracker and returns completed spans from any previous
	// job execution. This should be called before planning to enable span optimization.
	initTracker(ctx context.Context, existingProgress *jobspb.RowLevelTTLProgress, sv *settings.Values) ([]roachpb.Span, error)
	// initJobProgress writes the initial job progress to the job record with the correct
	// span count determined after planning. This should be called after makePlan.
	initJobProgress(ctx context.Context, jobSpanCount int64) error
	// handleProgressUpdate handles incoming processor metadata and performs any necessary
	// job updates. Each implementation determines how to handle the update (immediate vs deferred).
	handleProgressUpdate(ctx context.Context, meta *execinfrapb.ProducerMetadata) error
	// termTracker performs any necessary cleanup when the job completes or fails.
	termTracker()
}

// legacyProgressTracker tracks TTL job progress without span checkpointing.
// Progress is computed by counting processed spans against the total span count.
// Job restarts reset progress to 0%. Updates are gated to occur approximately
// every 1% of spans processed and at least 60 seconds apart.
type legacyProgressTracker struct {
	job *jobs.Job

	mu struct {
		syncutil.Mutex
		// lastUpdateTime is the wall time of the last job progress update.
		// Used to gate how often we persist job progress in refreshJobProgress.
		lastUpdateTime time.Time
		// lastSpanCount is the number of spans processed as of the last persisted update.
		lastSpanCount int64
		// updateEvery determines how many spans must be processed before we persist a new update.
		updateEvery int64
		// updateEveryDuration is the minimum time that must pass before allowing another progress update.
		updateEveryDuration time.Duration
	}
}

var _ progressTracker = (*legacyProgressTracker)(nil)

func newLegacyProgressTracker(job *jobs.Job) *legacyProgressTracker {
	return &legacyProgressTracker{
		job: job,
	}
}

// initTracker implements the progressTracker interface.
func (t *legacyProgressTracker) initTracker(
	context.Context, *jobspb.RowLevelTTLProgress, *settings.Values,
) ([]roachpb.Span, error) {
	return nil, nil
}

// setupUpdateFrequency configures the progress update frequency settings.
// To avoid too many progress updates, especially if a lot of the spans don't
// have expired rows, we will gate the updates to approximately every 1% of
// spans processed, and at least 60 seconds apart with jitter.
func (t *legacyProgressTracker) setupUpdateFrequency(jobSpanCount int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.updateEvery = max(1, jobSpanCount/100)
	t.mu.updateEveryDuration = 60*time.Second + time.Duration(rand.Int63n(10*1000))*time.Millisecond
	t.mu.lastUpdateTime = timeutil.Now()
	t.mu.lastSpanCount = 0
}

// initJobProgress implements the progressTracker interface.
func (t *legacyProgressTracker) initJobProgress(ctx context.Context, jobSpanCount int64) error {
	t.setupUpdateFrequency(jobSpanCount)

	// Write initial progress to job record
	return t.job.NoTxn().Update(ctx, func(_ isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		rowLevelTTL := &jobspb.RowLevelTTLProgress{
			JobTotalSpanCount:     jobSpanCount,
			JobProcessedSpanCount: 0,
		}

		progress := &jobspb.Progress{
			Details: &jobspb.Progress_RowLevelTTL{RowLevelTTL: rowLevelTTL},
			Progress: &jobspb.Progress_FractionCompleted{
				FractionCompleted: 0,
			},
		}
		ju.UpdateProgress(progress)
		return nil
	})
}

// refreshJobProgress computes updated job progress from processor metadata.
// It may return nil to skip immediate persistence, or a Progress to trigger an update.
func (t *legacyProgressTracker) refreshJobProgress(
	_ context.Context, md *jobs.JobMetadata, meta *execinfrapb.ProducerMetadata,
) (*jobspb.Progress, error) {
	if meta.BulkProcessorProgress == nil {
		return nil, nil
	}
	var incomingProcProgress jobspb.RowLevelTTLProcessorProgress
	if err := pbtypes.UnmarshalAny(&meta.BulkProcessorProgress.ProgressDetails, &incomingProcProgress); err != nil {
		return nil, errors.Wrapf(err, "unable to unmarshal ttl progress details")
	}

	orig := md.Progress.GetRowLevelTTL()
	if orig == nil {
		return nil, errors.New("job progress does not contain RowLevelTTL details")
	}
	rowLevelTTL := protoutil.Clone(orig).(*jobspb.RowLevelTTLProgress)

	// Update or insert the incoming processor progress.
	foundMatchingProcessor := false
	for i := range rowLevelTTL.ProcessorProgresses {
		if rowLevelTTL.ProcessorProgresses[i].ProcessorID == incomingProcProgress.ProcessorID {
			rowLevelTTL.ProcessorProgresses[i] = incomingProcProgress
			foundMatchingProcessor = true
			break
		}
	}
	if !foundMatchingProcessor {
		rowLevelTTL.ProcessorProgresses = append(rowLevelTTL.ProcessorProgresses, incomingProcProgress)
	}

	// Recompute job level counters from scratch.
	rowLevelTTL.JobDeletedRowCount = 0
	rowLevelTTL.JobProcessedSpanCount = 0
	totalSpanCount := int64(0)
	for i := range rowLevelTTL.ProcessorProgresses {
		pp := &rowLevelTTL.ProcessorProgresses[i]
		rowLevelTTL.JobDeletedRowCount += pp.DeletedRowCount
		rowLevelTTL.JobProcessedSpanCount += pp.ProcessedSpanCount
		totalSpanCount += pp.TotalSpanCount
	}

	if totalSpanCount > rowLevelTTL.JobTotalSpanCount {
		return nil, errors.Errorf(
			"computed span total cannot exceed job total: computed=%d jobRecorded=%d",
			totalSpanCount, rowLevelTTL.JobTotalSpanCount)
	}

	// Avoid the update if doing this too frequently.
	t.mu.Lock()
	defer t.mu.Unlock()
	processedDelta := rowLevelTTL.JobProcessedSpanCount - t.mu.lastSpanCount
	processorComplete := incomingProcProgress.ProcessedSpanCount == incomingProcProgress.TotalSpanCount
	firstProgressForProcessor := !foundMatchingProcessor

	if !(processedDelta >= t.mu.updateEvery ||
		timeutil.Since(t.mu.lastUpdateTime) >= t.mu.updateEveryDuration ||
		processorComplete ||
		firstProgressForProcessor) {
		return nil, nil // Skip the update
	}
	t.mu.lastSpanCount = rowLevelTTL.JobProcessedSpanCount
	t.mu.lastUpdateTime = timeutil.Now()

	newProgress := &jobspb.Progress{
		Details: &jobspb.Progress_RowLevelTTL{
			RowLevelTTL: rowLevelTTL,
		},
		Progress: &jobspb.Progress_FractionCompleted{
			FractionCompleted: float32(rowLevelTTL.JobProcessedSpanCount) /
				float32(rowLevelTTL.JobTotalSpanCount),
		},
	}
	return newProgress, nil
}

// handleProgressUpdate implements the progressTracker interface.
func (t *legacyProgressTracker) handleProgressUpdate(
	ctx context.Context, meta *execinfrapb.ProducerMetadata,
) error {
	return t.job.NoTxn().Update(ctx, func(_ isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		progress, err := t.refreshJobProgress(ctx, &md, meta)
		if err != nil {
			return err
		}
		if progress != nil {
			ju.UpdateProgress(progress)
		}
		return nil
	})
}

// termTracker implements the progressTracker interface.
func (t *legacyProgressTracker) termTracker() {
}

// checkpointProgressTracker tracks TTL job progress with span checkpointing.
// It maintains completed spans to enable job restarts without reprocessing.
// Progress updates occur at two frequencies: fraction complete updates happen
// more frequently than full checkpoint updates to minimize job record writes.
type checkpointProgressTracker struct {
	job                *jobs.Job
	settings           *cluster.Settings
	clock              timeutil.TimeSource
	checkpointInterval func() time.Duration
	fractionInterval   func() time.Duration

	// Range counting capability for accurate progress calculation
	db                *kv.DB
	distSQLPlanner    *sql.DistSQLPlanner
	originalTableSpan roachpb.Span // Store original span for range counting

	mu struct {
		syncutil.Mutex
		// cachedProgress holds the latest progress update from processors
		cachedProgress *jobspb.Progress
		// lastFractionUpdate tracks when we last sent a fraction-only update
		lastFractionUpdate time.Time
		// lastCheckpointUpdate tracks when we last sent a full checkpoint update
		lastCheckpointUpdate time.Time
		// completedSpans tracks all completed spans with automatic deduplication
		completedSpans roachpb.SpanGroup
	}

	// Goroutine management
	stopFunc func()
}

var _ progressTracker = (*checkpointProgressTracker)(nil)

func newCheckpointProgressTracker(
	job *jobs.Job,
	sv *settings.Values,
	db *kv.DB,
	distSQLPlanner *sql.DistSQLPlanner,
	originalTableSpan roachpb.Span,
) *checkpointProgressTracker {
	return &checkpointProgressTracker{
		job:                job,
		clock:              timeutil.DefaultTimeSource{},
		fractionInterval:   func() time.Duration { return fractionUpdateInterval.Get(sv) },
		checkpointInterval: func() time.Duration { return checkpointInterval.Get(sv) },
		db:                 db,
		distSQLPlanner:     distSQLPlanner,
		originalTableSpan:  originalTableSpan,
	}
}

// initTracker implements the progressTracker interface.
func (t *checkpointProgressTracker) initTracker(
	ctx context.Context, existingProgress *jobspb.RowLevelTTLProgress, sv *settings.Values,
) ([]roachpb.Span, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Initialize the SpanGroup with any existing completed spans for restart scenarios
	var completedSpans []roachpb.Span
	if existingProgress != nil && len(existingProgress.CompletedSpans) > 0 {
		t.mu.completedSpans.Add(existingProgress.CompletedSpans...)
		completedSpans = existingProgress.CompletedSpans
		log.Dev.Infof(ctx, "TTL job restarting with %d existing completed spans", len(completedSpans))
	}

	return completedSpans, nil
}

// initJobProgress implements the progressTracker interface.
func (t *checkpointProgressTracker) initJobProgress(ctx context.Context, jobSpanCount int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	completedSpans := t.mu.completedSpans.Slice()
	rowLevelTTL := &jobspb.RowLevelTTLProgress{
		UseCheckpointing: true,
		CompletedSpans:   completedSpans,
	}

	// Calculate initial fraction complete based on existing completed spans
	fractionComplete, err := t.calculateRangeFractionComplete(ctx, completedSpans)
	if err != nil {
		return err
	}

	progress := &jobspb.Progress{
		Details: &jobspb.Progress_RowLevelTTL{RowLevelTTL: rowLevelTTL},
		Progress: &jobspb.Progress_FractionCompleted{
			FractionCompleted: fractionComplete,
		},
	}

	t.mu.cachedProgress = progress
	now := timeutil.Now()
	t.mu.lastFractionUpdate = now
	t.mu.lastCheckpointUpdate = now

	// Start the progress update goroutines
	t.stopFunc = t.startPeriodicUpdates(ctx)

	return nil
}

// updateProgressCache computes updated job progress from processor metadata and updates
// the internal cache. Returns progress only when immediate persistence is needed.
func (t *checkpointProgressTracker) updateProgressCache(
	meta *execinfrapb.ProducerMetadata,
) (*jobspb.Progress, error) {
	if meta.BulkProcessorProgress == nil {
		return nil, nil
	}

	var incomingProcProgress jobspb.RowLevelTTLProcessorProgress
	if err := pbtypes.UnmarshalAny(&meta.BulkProcessorProgress.ProgressDetails, &incomingProcProgress); err != nil {
		return nil, errors.Wrapf(err, "unable to unmarshal ttl progress details")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.mu.cachedProgress == nil {
		return nil, errors.AssertionFailedf("progress not initialized")
	}

	orig := t.mu.cachedProgress.GetRowLevelTTL()
	if orig == nil {
		return nil, errors.AssertionFailedf("cached progress does not contain RowLevelTTL details")
	}
	rowLevelTTL := protoutil.Clone(orig).(*jobspb.RowLevelTTLProgress)

	// Update or insert the incoming processor progress
	foundMatchingProcessor := false
	for i := range rowLevelTTL.ProcessorProgresses {
		if rowLevelTTL.ProcessorProgresses[i].ProcessorID == incomingProcProgress.ProcessorID {
			rowLevelTTL.ProcessorProgresses[i] = incomingProcProgress
			foundMatchingProcessor = true
			break
		}
	}
	if !foundMatchingProcessor {
		rowLevelTTL.ProcessorProgresses = append(rowLevelTTL.ProcessorProgresses, incomingProcProgress)
	}

	// The CompletedSpans in the progress message is the delta of number of spans
	// completed since the last progress update. Add them to our SpanGroup for
	// automatic deduplication and merging.
	t.mu.completedSpans.Add(meta.BulkProcessorProgress.CompletedSpans...)
	rowLevelTTL.CompletedSpans = t.mu.completedSpans.Slice()

	// Recompute job level counters from scratch
	rowLevelTTL.JobDeletedRowCount = 0
	for i := range rowLevelTTL.ProcessorProgresses {
		rowLevelTTL.JobDeletedRowCount += rowLevelTTL.ProcessorProgresses[i].DeletedRowCount
	}

	// Update cached progress - the goroutine will handle actual persistence
	t.mu.cachedProgress = &jobspb.Progress{
		Details: &jobspb.Progress_RowLevelTTL{
			RowLevelTTL: rowLevelTTL,
		},
	}

	// We denote the final progress from a producer using the Drained field. If
	// that's set, then return the progress to have the caller update the job
	// record immediately. This ensures we capture the final state when a
	// processor completes its work, rather than waiting for the next periodic
	// checkpoint update.
	if meta.BulkProcessorProgress.Drained {
		return t.mu.cachedProgress, nil
	}

	// Return nil to indicate we're not immediately persisting
	return nil, nil
}

// handleProgressUpdate implements the progressTracker interface.
func (t *checkpointProgressTracker) handleProgressUpdate(
	ctx context.Context, meta *execinfrapb.ProducerMetadata,
) error {
	progress, err := t.updateProgressCache(meta)
	if err != nil {
		return err
	}

	// If updateProgressCache returned a progress (indicating immediate persistence needed),
	// write it to the job record. This happens when a processor signals completion (Drained=true).
	if progress != nil {
		return t.job.NoTxn().Update(ctx, func(_ isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			ju.UpdateProgress(progress)
			return nil
		})
	}

	// Otherwise, the background goroutines will handle the persistence
	return nil
}

// termTracker implements the progressTracker interface.
func (t *checkpointProgressTracker) termTracker() {
	if t.stopFunc != nil {
		t.stopFunc()
		t.stopFunc = nil
	}
}

// startPeriodicUpdates launches background goroutines to periodically flush
// progress updates at different intervals. Returns a stop function to terminate
// the goroutines and wait for their completion.
func (t *checkpointProgressTracker) startPeriodicUpdates(ctx context.Context) (stop func()) {
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
					return errors.Wrap(err, "could not flush progress")
				}
			}
		}
	}

	var g errgroup.Group
	g.Go(func() error {
		return runPeriodicWrite(
			ctx, t.flushFractionUpdate, t.fractionInterval)
	})
	g.Go(func() error {
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

// flushFractionUpdate updates only the fraction complete progress without
// modifying the expensive completed spans data.
func (t *checkpointProgressTracker) flushFractionUpdate(ctx context.Context) error {
	fractionComplete, err := t.calculateCurrentFractionComplete(ctx)
	if err != nil {
		return err
	}

	return t.job.NoTxn().Update(ctx, func(_ isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		newProgress := &jobspb.Progress{
			Details: &jobspb.Progress_RowLevelTTL{
				RowLevelTTL: md.Progress.GetRowLevelTTL(),
			},
			Progress: &jobspb.Progress_FractionCompleted{
				FractionCompleted: fractionComplete,
			},
		}
		ju.UpdateProgress(newProgress)
		return nil
	})
}

// calculateRangeFractionComplete calculates accurate progress based on range counting.
func (t *checkpointProgressTracker) calculateRangeFractionComplete(
	ctx context.Context, completedSpans []roachpb.Span,
) (float32, error) {
	if t.originalTableSpan.ZeroLength() {
		return 1.0, nil
	}

	total, completed, err := sql.NumRangesInSpanContainedBy(
		ctx,
		t.db,
		t.distSQLPlanner,
		t.originalTableSpan,
		completedSpans,
	)
	if err != nil {
		return 0, err
	}

	if total == 0 {
		return 1.0, nil
	}

	return float32(completed) / float32(total), nil
}

// calculateCurrentFractionComplete provides a helper for consistent calculation
// across both flush methods.
func (t *checkpointProgressTracker) calculateCurrentFractionComplete(
	ctx context.Context,
) (float32, error) {
	t.mu.Lock()
	cachedProgress := t.mu.cachedProgress
	t.mu.Unlock()

	if cachedProgress == nil {
		return 0, nil
	}

	cachedTTL := cachedProgress.GetRowLevelTTL()
	if cachedTTL == nil {
		return 0, nil
	}

	return t.calculateRangeFractionComplete(ctx, cachedTTL.CompletedSpans)
}

// addCompleted adds the given spans to the completed spans set with automatic
// deduplication and merging of adjacent spans.
func (t *checkpointProgressTracker) addCompleted(spans ...roachpb.Span) []roachpb.Span {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.completedSpans.Add(spans...)
	return t.mu.completedSpans.Slice()
}

// flushCheckpointUpdate performs a full progress update including completed spans
// and fraction complete. This is more expensive than flushFractionUpdate as it
// updates the completed spans data which can be large for jobs with many spans.
func (t *checkpointProgressTracker) flushCheckpointUpdate(ctx context.Context) error {
	t.mu.Lock()
	cachedProgress := t.mu.cachedProgress
	t.mu.Unlock()

	if cachedProgress == nil {
		return nil
	}

	fractionComplete, err := t.calculateCurrentFractionComplete(ctx)
	if err != nil {
		return err
	}

	return t.job.NoTxn().Update(ctx, func(_ isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		cachedTTL := cachedProgress.GetRowLevelTTL()
		if cachedTTL == nil {
			return errors.AssertionFailedf("cached progress does not contain RowLevelTTL details")
		}
		newProgress := &jobspb.Progress{
			Details: &jobspb.Progress_RowLevelTTL{
				RowLevelTTL: protoutil.Clone(cachedTTL).(*jobspb.RowLevelTTLProgress),
			},
			Progress: &jobspb.Progress_FractionCompleted{
				FractionCompleted: fractionComplete,
			},
		}
		ju.UpdateProgress(newProgress)
		return nil
	})
}
