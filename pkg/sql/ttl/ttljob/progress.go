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
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

type progressTracker interface {
	// initJobProgress writes the initial job progress to the job record with the correct
	// span count determined after planning. This should be called after makePlan.
	initJobProgress(ctx context.Context, jobSpanCount int64) error
	// handleProgressUpdate handles incoming processor metadata and performs any necessary
	// job updates. Each implementation determines how to handle the update (immediate vs deferred).
	handleProgressUpdate(ctx context.Context, meta *execinfrapb.ProducerMetadata) error
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
