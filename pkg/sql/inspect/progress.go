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
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
//
// When AsOf is empty (using "now" timestamps), the tracker also manages
// protected timestamps for active spans. It sets up PTS when a span starts
// processing and cleans up when the span completes.
type inspectProgressTracker struct {
	job                *jobs.Job
	jobID              jobspb.JobID
	clock              timeutil.TimeSource
	checkpointInterval func() time.Duration
	fractionInterval   func() time.Duration
	internalDB         descs.DB
	codec              keys.SQLCodec
	ptsManager         scexec.ProtectedTimestampManager

	mu struct {
		syncutil.Mutex
		// cachedProgress holds the latest progress update from processors.
		cachedProgress *jobspb.Progress
		// completedSpans tracks all completed spans with automatic deduplication.
		completedSpans roachpb.SpanGroup
		// receivedSpanCount tracks the total number of spans received from metadata.
		// This is used to determine if we have new spans to checkpoint.
		receivedSpanCount int
		// lastCheckpointedSpanCount tracks the span count at the last checkpoint write.
		lastCheckpointedSpanCount int
		// activeSpanTimestamps tracks the timestamp for each active span (keyed by
		// span.String()). Used to determine the minimum timestamp for PTS
		// protection.
		// Note: we add spans to this map even if PTS setup fails, since this
		// is only meant to indicate which spans are being processed.
		activeSpanTimestamps map[string]hlc.Timestamp
		// currentPTSCleaner is the cleaner for the current PTS record (if any).
		// We maintain at most one PTS record, protecting the minimum timestamp.
		currentPTSCleaner jobsprotectedts.Cleaner
		// currentPTSTimestamp is the timestamp currently being protected.
		currentPTSTimestamp hlc.Timestamp
		// lastLoggedPercent tracks the last percentage milestone we logged, to avoid
		// spamming logs with progress updates. We log at every 1% increment.
		lastLoggedPercent int
	}

	// Goroutine management.
	stopFunc func()

	// testingPTSProtector is a testing hook that overrides the normal PTS setup.
	// When set, it is called instead of the real TryToProtectBeforeGC logic.
	// The returned cleaner is stored and called when the span completes.
	testingPTSProtector func(ctx context.Context, span roachpb.Span, ts hlc.Timestamp) jobsprotectedts.Cleaner
}

func newInspectProgressTracker(
	job *jobs.Job,
	sv *settings.Values,
	internalDB descs.DB,
	codec keys.SQLCodec,
	ptsManager scexec.ProtectedTimestampManager,
) *inspectProgressTracker {
	t := &inspectProgressTracker{
		job:                job,
		jobID:              job.ID(),
		clock:              timeutil.DefaultTimeSource{},
		fractionInterval:   func() time.Duration { return fractionUpdateInterval.Get(sv) },
		checkpointInterval: func() time.Duration { return checkpointInterval.Get(sv) },
		internalDB:         internalDB,
		codec:              codec,
		ptsManager:         ptsManager,
	}
	t.mu.activeSpanTimestamps = make(map[string]hlc.Timestamp)
	return t
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
// Also handles protected timestamp setup for span started events and cleanup for
// span completed events.
func (t *inspectProgressTracker) handleProgressUpdate(
	ctx context.Context, meta *execinfrapb.ProducerMetadata,
) error {
	if meta.BulkProcessorProgress == nil {
		return nil
	}

	var incomingProcProgress jobspb.InspectProcessorProgress
	if err := pbtypes.UnmarshalAny(&meta.BulkProcessorProgress.ProgressDetails, &incomingProcProgress); err != nil {
		return errors.Wrapf(err, "unable to unmarshal inspect progress details")
	}

	// Handle span started: set up PTS protection.
	if !incomingProcProgress.SpanStarted.Equal(roachpb.Span{}) {
		t.setupSpanPTS(ctx, incomingProcProgress.SpanStarted, incomingProcProgress.StartedAt)
		return nil
	}

	// Update the progress cache (with lock).
	needsImmediatePersistence, err := t.updateProgressCache(meta, incomingProcProgress)
	if err != nil {
		return err
	}

	// Handle span completed: call cleaner and pick a new PTS if needed.
	for _, completedSpan := range meta.BulkProcessorProgress.CompletedSpans {
		t.cleanupSpanPTS(ctx, completedSpan)
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
	meta *execinfrapb.ProducerMetadata, incomingProcProgress jobspb.InspectProcessorProgress,
) (bool, error) {
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
		t.mu.receivedSpanCount += len(meta.BulkProcessorProgress.CompletedSpans)
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
// This includes cleaning up the PTS record if one exists.
func (t *inspectProgressTracker) terminateTracker(ctx context.Context) {
	if t.stopFunc != nil {
		t.stopFunc()
		t.stopFunc = nil
	}

	// Clean up the PTS record if one exists (in case of early termination).
	var cleaner jobsprotectedts.Cleaner
	func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		cleaner = t.mu.currentPTSCleaner
		t.mu.currentPTSCleaner = nil
		t.mu.currentPTSTimestamp = hlc.Timestamp{}
		t.mu.activeSpanTimestamps = make(map[string]hlc.Timestamp)
	}()

	if cleaner != nil {
		if err := cleaner(ctx); err != nil {
			log.Dev.Warningf(ctx, "failed to clean up PTS during termination: %v", err)
		}
	}
}

// setupSpanPTS sets up protected timestamp protection for a span that has started
// processing. This is called when the coordinator receives a "span started" message
// from a processor.
//
// We maintain at most one PTS record, protecting the minimum timestamp across all
// active spans. Since PROTECT_AFTER mode protects all data at or after the specified
// timestamp, protecting at the minimum covers all active spans. When a new span
// starts with an older timestamp than the current PTS, we update the PTS to protect
// the new minimum.
func (t *inspectProgressTracker) setupSpanPTS(
	ctx context.Context, spanStarted roachpb.Span, tsToProtect hlc.Timestamp,
) {
	spanKey := spanStarted.String()

	// Check if we need to update the PTS (first span or new minimum timestamp).
	var needsNewPTS bool
	var oldCleaner jobsprotectedts.Cleaner
	var currentPTSTimestamp hlc.Timestamp
	func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.mu.activeSpanTimestamps[spanKey] = tsToProtect
		currentPTSTimestamp = t.mu.currentPTSTimestamp
		if t.mu.currentPTSCleaner == nil || tsToProtect.Less(currentPTSTimestamp) {
			needsNewPTS = true
			oldCleaner = t.mu.currentPTSCleaner
		}
	}()

	if !needsNewPTS {
		log.VEventf(ctx, 2, "INSPECT: span %s at %s covered by existing PTS at %s",
			spanStarted, tsToProtect, currentPTSTimestamp)
		return
	}

	// Clean up old PTS before setting new one.
	if oldCleaner != nil {
		if err := oldCleaner(ctx); err != nil {
			log.Dev.Warningf(ctx, "failed to clean up old PTS: %v", err)
		}
	}

	// Create new PTS at the new minimum timestamp.
	var cleaner jobsprotectedts.Cleaner
	if t.testingPTSProtector != nil {
		cleaner = t.testingPTSProtector(ctx, spanStarted, tsToProtect)
	} else {
		// Extract table ID from the span key prefix.
		_, tableID, err := t.codec.DecodeTablePrefix(spanStarted.Key)
		if err != nil {
			log.Dev.Warningf(ctx, "failed to decode table ID from span %s: %v", spanStarted, err)
			return
		}
		cleaner = t.ptsManager.TryToProtectBeforeGC(ctx, t.job, descpb.ID(tableID), tsToProtect)
	}

	func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.mu.currentPTSCleaner = cleaner
		t.mu.currentPTSTimestamp = tsToProtect
	}()

	log.VEventf(ctx, 2, "INSPECT: set up PTS protection at minimum timestamp %s (triggered by span %s)",
		tsToProtect, spanStarted)
}

// cleanupSpanPTS handles PTS management when a span completes processing.
// This is called when the coordinator receives a "span completed" message.
//
// When the oldest span (with the minimum timestamp) completes, we update the PTS
// to protect the new minimum timestamp among remaining active spans. This allows
// GC of data between the old and new minimum timestamps.
func (t *inspectProgressTracker) cleanupSpanPTS(ctx context.Context, completedSpan roachpb.Span) {
	spanKey := completedSpan.String()

	// Determine what action to take based on current state.
	var action ptsCleanupAction
	var oldCleaner jobsprotectedts.Cleaner
	var newMinTimestamp hlc.Timestamp
	func() {
		t.mu.Lock()
		defer t.mu.Unlock()

		completedTimestamp, ok := t.mu.activeSpanTimestamps[spanKey]
		if !ok {
			// Span not tracked - either PTS was never set up (e.g., AsOf was specified),
			// or it was already cleaned up.
			action = ptsActionNone
			return
		}
		delete(t.mu.activeSpanTimestamps, spanKey)

		if len(t.mu.activeSpanTimestamps) == 0 {
			// No more active spans - clean up PTS entirely.
			action = ptsActionCleanup
			oldCleaner = t.mu.currentPTSCleaner
			t.mu.currentPTSCleaner = nil
			t.mu.currentPTSTimestamp = hlc.Timestamp{}
			return
		}

		// Check if the completed span was the one with the minimum timestamp.
		if completedTimestamp.Equal(t.mu.currentPTSTimestamp) {
			// Find the new minimum timestamp among remaining spans.
			var foundMin bool
			var minSpanKey string
			for sk, ts := range t.mu.activeSpanTimestamps {
				if !foundMin || ts.Less(newMinTimestamp) {
					newMinTimestamp = ts
					minSpanKey = sk
					foundMin = true
				}
			}
			if foundMin && !newMinTimestamp.Less(t.mu.currentPTSTimestamp) {
				// New minimum is newer than current PTS - need to update.
				action = ptsActionUpdate
				oldCleaner = t.mu.currentPTSCleaner
				t.mu.currentPTSCleaner = nil
				// We'll extract the table ID from the span key outside the lock.
				// Store the span key for later processing.
				_ = minSpanKey // Used for logging if needed.
			}
		}
		// If the completed span wasn't the minimum, no PTS change needed.
	}()

	switch action {
	case ptsActionNone:
		return

	case ptsActionCleanup:
		if oldCleaner != nil {
			if err := oldCleaner(ctx); err != nil {
				log.Dev.Warningf(ctx, "failed to clean up PTS: %v", err)
			} else {
				log.VEventf(ctx, 2, "INSPECT: cleaned up PTS protection (no more active spans)")
			}
		}

	case ptsActionUpdate:
		// Clean up old PTS.
		if oldCleaner != nil {
			if err := oldCleaner(ctx); err != nil {
				log.Dev.Warningf(ctx, "failed to clean up old PTS: %v", err)
			}
		}

		// Create new PTS at the new minimum timestamp.
		var cleaner jobsprotectedts.Cleaner
		if t.testingPTSProtector != nil {
			cleaner = t.testingPTSProtector(ctx, completedSpan, newMinTimestamp)
		} else {
			// Extract table ID from the completed span. All spans in an INSPECT job
			// typically belong to the same table, so this is safe.
			_, tableID, err := t.codec.DecodeTablePrefix(completedSpan.Key)
			if err != nil {
				log.Dev.Warningf(ctx, "failed to decode table ID from span %s: %v", completedSpan, err)
				return
			}
			cleaner = t.ptsManager.TryToProtectBeforeGC(ctx, t.job, descpb.ID(tableID), newMinTimestamp)
		}
		func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.mu.currentPTSCleaner = cleaner
			t.mu.currentPTSTimestamp = newMinTimestamp
		}()

		log.VEventf(ctx, 2, "INSPECT: updated PTS protection to new minimum timestamp %s", newMinTimestamp)
	}
}

// ptsCleanupAction indicates what action to take when a span completes.
type ptsCleanupAction int

const (
	ptsActionNone    ptsCleanupAction = iota // No action needed.
	ptsActionCleanup                         // Clean up PTS entirely (no more active spans).
	ptsActionUpdate                          // Update PTS to new minimum timestamp.
)

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

	return t.job.NoTxn().Update(ctx, func(_ isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		// Only write if any part of the inspect progress has changed.
		currentInspectProgress := md.Progress.GetInspect()
		if currentInspectProgress != nil &&
			currentInspectProgress.JobTotalCheckCount == cachedInspectProgress.JobTotalCheckCount &&
			currentInspectProgress.JobCompletedCheckCount == cachedInspectProgress.JobCompletedCheckCount &&
			md.Progress.GetProgress() != nil { // Ensure fraction complete has been initialized.
			return nil
		}

		// Calculate fraction complete based on check counts from cached progress.
		var fractionComplete float32
		if cachedInspectProgress.JobTotalCheckCount > 0 {
			fractionComplete = float32(cachedInspectProgress.JobCompletedCheckCount) / float32(cachedInspectProgress.JobTotalCheckCount)
		}

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
	if !t.hasUncheckpointedSpans() {
		return nil
	}

	var completedSpans []roachpb.Span
	var capturedReceivedCount int
	func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		completedSpans = t.mu.completedSpans.Slice()
		capturedReceivedCount = t.mu.receivedSpanCount
	}()

	// If no completed spans, nothing to store.
	if len(completedSpans) == 0 {
		return nil
	}

	err := t.internalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// Create a frontier for the spans (using zero timestamps since INSPECT doesn't need timing).
		frontier, err := span.MakeFrontier(completedSpans...)
		if err != nil {
			return err
		}
		defer frontier.Release()
		return jobfrontier.Store(ctx, txn, t.jobID, inspectCompletedSpansKey, frontier)
	})
	if err != nil {
		return err
	}

	// If the checkpoint write succeeded, update the last checkpointed span count.
	// This prevents unnecessary future writes when no new spans have been
	// completed. Also check if we should log a progress milestone.
	var shouldLog bool
	var currentPercent int
	func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		if capturedReceivedCount > t.mu.lastCheckpointedSpanCount {
			t.mu.lastCheckpointedSpanCount = capturedReceivedCount
		}
		// Check if we've crossed a 1% threshold since last log.
		inspectProgress := t.mu.cachedProgress.GetInspect()
		if inspectProgress != nil && inspectProgress.JobTotalCheckCount > 0 {
			currentPercent = int(float64(inspectProgress.JobCompletedCheckCount) / float64(inspectProgress.JobTotalCheckCount) * 100)
			if currentPercent > t.mu.lastLoggedPercent {
				t.mu.lastLoggedPercent = currentPercent
				shouldLog = true
			}
		}
	}()

	// Log progress at 1% milestones so operators can monitor long-running jobs.
	if shouldLog {
		totalChecks, completedChecks := t.getCachedCheckCounts()
		log.Dev.Infof(ctx, "INSPECT job %d progress: %d%% complete (%d/%d checks, %d spans checkpointed)",
			t.jobID, currentPercent, completedChecks, totalChecks, len(completedSpans))
	}

	return nil
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

// hasUncheckpointedSpans returns true if there are completed spans that have not been
// checkpointed yet. This is used to determine if a checkpoint flush is needed.
func (t *inspectProgressTracker) hasUncheckpointedSpans() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.mu.receivedSpanCount > t.mu.lastCheckpointedSpanCount
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
