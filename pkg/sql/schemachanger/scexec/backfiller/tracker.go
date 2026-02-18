// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backfiller

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/bulkutil"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// RangeCounter abstracts the process of counting the number of ranges in
// a span, potentially also counting the set of ranges in spans which overlap
// with that span.
type RangeCounter interface {

	// NumRangesInSpanContainedBy is implemented by sql.NumRangesInSpanContainedBy.
	// See the comment there.
	NumRangesInSpanContainedBy(
		ctx context.Context, span roachpb.Span, containedBy []roachpb.Span,
	) (total, inContainedBy int, _ error)
}

// Tracker is used to receive backfillProgress updates on index
// backfills and merges and periodically write them.
//
// The data structure supports receiving updates from multiple concurrent
// backfills and merges. Additionally, it supports writing two different types
// of backfillProgress and mergeProgress updates: a small fraction completed
// update, which is written at a higher frequency, and a larger checkpoint which
// records the remaining spans of the source index to scan.
type Tracker struct {
	trackerConfig
	codec   keys.SQLCodec
	cleaner *phaseTransitionCleaner // Optional: nil if no cleanup needed

	mu struct {
		syncutil.Mutex

		backfillProgress map[backfillKey]*backfillProgress
		mergeProgress    map[mergeKey]*mergeProgress
	}
}

var _ scexec.BackfillerTracker = (*Tracker)(nil)

// NewTracker constructs a new Tracker.
//
// If cleaner is non-nil, the tracker will perform SST cleanup when phase
// transitions are detected. The caller is responsible for closing the cleaner.
// If nil, cleanup is disabled.
func NewTracker(
	codec keys.SQLCodec,
	counter RangeCounter,
	job *jobs.Job,
	db isql.DB,
	jobBackfillProgress []jobspb.BackfillProgress,
	jobMergeProgress []jobspb.MergeProgress,
	cleaner *bulkutil.BulkJobCleaner,
) *Tracker {
	tr := newTracker(
		codec,
		newTrackerConfig(codec, counter, job, db),
		convertFromJobBackfillProgress(codec, jobBackfillProgress),
		convertFromJobMergeProgress(codec, jobMergeProgress),
	)

	// Configure cleanup if cleaner provided.
	if cleaner != nil {
		tr.cleaner = &phaseTransitionCleaner{
			jobID:   job.ID(),
			cleaner: cleaner,
		}
	}

	return tr
}

func newTracker(
	codec keys.SQLCodec, cfg trackerConfig, ibp []scexec.BackfillProgress, imp []scexec.MergeProgress,
) *Tracker {
	bt := &Tracker{
		codec:         codec,
		trackerConfig: cfg,
	}
	{
		bt.mu.backfillProgress = make(map[backfillKey]*backfillProgress)
		for _, bp := range ibp {
			bt.mu.backfillProgress[toBackfillKey(bp.Backfill)] = newBackfillProgress(codec, bp)
		}
	}
	{
		bt.mu.mergeProgress = make(map[mergeKey]*mergeProgress)
		for _, mp := range imp {
			bt.mu.mergeProgress[toMergeKey(mp.Merge)] = newMergeProgress(codec, mp)
		}
	}
	return bt
}

func newTrackerConfig(
	codec keys.SQLCodec, rc RangeCounter, job *jobs.Job, db isql.DB,
) trackerConfig {
	return trackerConfig{
		numRangesInSpanContainedBy: rc.NumRangesInSpanContainedBy,
		writeProgressFraction: func(ctx context.Context, fractionProgressed float32) error {
			if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				return jobs.ProgressStorage(job.ID()).SetFraction(ctx, txn, float64(fractionProgressed))
			}); err != nil {
				return jobs.SimplifyInvalidStateError(err)
			}
			return nil
		},
		writeCheckpoint: func(ctx context.Context, bps []scexec.BackfillProgress, mps []scexec.MergeProgress) error {
			return job.NoTxn().Update(ctx, func(
				txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
			) error {
				pl := md.Payload
				backfillJobProgress, err := convertToJobBackfillProgress(codec, bps)
				if err != nil {
					return err
				}
				mergeJobProgress, err := convertToJobMergeProgress(codec, mps)
				if err != nil {
					return err
				}
				sc := pl.GetNewSchemaChange()
				sc.BackfillProgress = backfillJobProgress
				sc.MergeProgress = mergeJobProgress
				ju.UpdatePayload(pl)
				return nil
			})
		},
	}
}

// trackerConfig represents the underlying dependencies of the
// tracker. It exists in this abstracted form largely to facilitate
// testing and to make dependency injection convenient.
type trackerConfig struct {

	// numRangesInSpanContainedBy returns the total number of ranges in the span
	// and the number of ranges in that span which are fully covered by the set
	// of spans provided.
	numRangesInSpanContainedBy func(
		context.Context, roachpb.Span, []roachpb.Span,
	) (total, contained int, _ error)

	// writeProgressFraction writes the backfillProgress fraction for presentation.
	writeProgressFraction func(_ context.Context, fractionProgressed float32) error

	// writeCheckpoint write the checkpoint the underlying store.
	writeCheckpoint func(context.Context, []scexec.BackfillProgress, []scexec.MergeProgress) error
}

// GetBackfillProgress is part of the scexec.BackfillerProgressReader interface.
func (b *Tracker) GetBackfillProgress(
	ctx context.Context, bf scexec.Backfill,
) (scexec.BackfillProgress, error) {
	p, ok := b.getTableIndexBackfillProgress(bf)
	if !ok {
		return scexec.BackfillProgress{
			Backfill: bf,
		}, nil
	}
	if err := p.matches(bf); err != nil {
		return scexec.BackfillProgress{}, err
	}
	return p.BackfillProgress, nil
}

// GetMergeProgress is part of the scexec.BackfillerProgressReader interface.
func (b *Tracker) GetMergeProgress(
	ctx context.Context, m scexec.Merge,
) (scexec.MergeProgress, error) {
	p, ok := b.getTableIndexMergeProgress(m)
	if !ok {
		return scexec.MakeMergeProgress(m), nil
	}
	if err := p.matches(m); err != nil {
		return scexec.MergeProgress{}, err
	}
	return p.MergeProgress, nil
}

// SetBackfillProgress is part of the scexec.BackfillerProgressWriter interface.
func (b *Tracker) SetBackfillProgress(ctx context.Context, progress scexec.BackfillProgress) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	p, err := b.getBackfillProgressLocked(progress.Backfill)
	if err != nil {
		return err
	}
	if p == nil {
		p = newBackfillProgress(b.codec, progress)
		b.mu.backfillProgress[toBackfillKey(progress.Backfill)] = p
	} else {
		p.BackfillProgress = progress
	}
	p.needsCheckpointFlush = true
	p.needsFractionFlush = true
	return nil
}

// SetMergeProgress is part of the scexec.BackfillerProgressWriter interface.
func (b *Tracker) SetMergeProgress(ctx context.Context, progress scexec.MergeProgress) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	p, err := b.getMergeProgressLocked(progress.Merge)
	if err != nil {
		return err
	}
	if p == nil {
		p = newMergeProgress(b.codec, progress)
		b.mu.mergeProgress[toMergeKey(progress.Merge)] = p
	} else {
		p.MergeProgress = progress
	}
	p.needsCheckpointFlush = true
	p.needsFractionFlush = true
	return nil
}

// FlushFractionCompleted is part of the scexec.BackfillerProgressFlusher interface.
func (b *Tracker) FlushFractionCompleted(ctx context.Context) error {
	updated, fractionRangesFinished, err := b.getFractionRangesFinished(ctx)
	if err != nil {
		return err
	}
	if !updated {
		log.Dev.VInfof(ctx, 2, "backfill has no fraction completed to flush")
		return nil
	}
	log.Dev.Infof(ctx, "backfill fraction completed is %.3f / 1.000", fractionRangesFinished)
	return b.writeProgressFraction(ctx, fractionRangesFinished)
}

// FlushCheckpoint is part of the scexec.BackfillerProgressFlusher interface.
func (b *Tracker) FlushCheckpoint(ctx context.Context) error {
	needsFlush, bps, mps := b.collectProgressForCheckpointFlush()
	if !needsFlush {
		log.Dev.VInfof(ctx, 2, "backfill has no checkpoint to flush")
		return nil
	}
	sort.Slice(bps, func(i, j int) bool {
		if bps[i].TableID != bps[j].TableID {
			return bps[i].TableID < bps[j].TableID
		}
		return bps[i].SourceIndexID < bps[j].SourceIndexID
	})
	sort.Slice(mps, func(i, j int) bool {
		if mps[i].TableID != mps[j].TableID {
			return mps[i].TableID < mps[j].TableID
		}
		if len(mps[i].SourceIndexIDs) != len(mps[j].SourceIndexIDs) {
			return len(mps[i].SourceIndexIDs) < len(mps[j].SourceIndexIDs)
		}
		for k, id := range mps[i].SourceIndexIDs {
			if id != mps[j].SourceIndexIDs[k] {
				return id < mps[j].SourceIndexIDs[k]
			}
		}
		return false
	})
	log.Dev.Infof(ctx, "writing %d backfill checkpoints and %d merge checkpoints", len(bps), len(mps))
	if err := b.writeCheckpoint(ctx, bps, mps); err != nil {
		return err
	}

	// After successful write, detect phase transitions and handle cleanup.
	if b.cleaner != nil {
		transitions := b.detectPhaseTransitions(ctx, bps)
		for _, transition := range transitions {
			if err := b.cleaner.cleanupTransition(ctx, transition); err != nil {
				log.Ops.Warningf(ctx, "phase transition cleanup failed: %v", err)
			}
		}
	}

	return nil
}

// detectPhaseTransitions detects and records phase transitions that occurred
// after checkpoint persistence. It returns the list of detected transitions.
func (b *Tracker) detectPhaseTransitions(
	ctx context.Context, bps []scexec.BackfillProgress,
) []phaseTransition {
	b.mu.Lock()
	defer b.mu.Unlock()

	var transitions []phaseTransition
	for _, bp := range bps {
		key := toBackfillKey(bp.Backfill)
		p, exists := b.mu.backfillProgress[key]
		if !exists {
			continue
		}

		oldPhase := p.lastPersistedDistributedMergePhase
		newPhase := bp.DistributedMergePhase

		if oldPhase != newPhase {
			log.Dev.Infof(ctx, "detected persisted distributed merge phase transition for table %d: phase %d → %d",
				bp.Backfill.TableID, oldPhase, newPhase)

			transitions = append(transitions, phaseTransition{
				TableID:            bp.Backfill.TableID,
				OldPhase:           oldPhase,
				NewPhase:           newPhase,
				SSTStoragePrefixes: bp.SSTStoragePrefixes,
			})
			// Update last persisted phase.
			p.lastPersistedDistributedMergePhase = newPhase
		}
	}

	return transitions
}

func (b *Tracker) getTableIndexBackfillProgress(bf scexec.Backfill) (backfillProgress, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if p, ok := b.getTableIndexBackfillProgressLocked(bf); ok {
		return *p, true
	}
	return backfillProgress{}, false
}

func (b *Tracker) getTableIndexMergeProgress(m scexec.Merge) (mergeProgress, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if p, ok := b.getTableIndexMergeProgressLocked(m); ok {
		return *p, true
	}
	return mergeProgress{}, false
}

// getBackfillProgressLocked is used to get a mutable handle to the backfill
// backfillProgress for a given backfill. It will return nil, nil if no such entry
// exists. It will return an error if an entry exists for the source index
// with a different set of dest indexes.
func (b *Tracker) getBackfillProgressLocked(bf scexec.Backfill) (*backfillProgress, error) {
	p, ok := b.getTableIndexBackfillProgressLocked(bf)
	if !ok {
		return nil, nil
	}
	if err := p.matches(bf); err != nil {
		return nil, err
	}
	return p, nil
}

func (b *Tracker) getTableIndexBackfillProgressLocked(
	bf scexec.Backfill,
) (*backfillProgress, bool) {
	if p, ok := b.mu.backfillProgress[toBackfillKey(bf)]; ok {
		return p, true
	}
	return nil, false
}

// getMergeProgressLocked is used to get a mutable handle to the merge
// mergeProgress for a given merge. It will return nil, nil if no such entry
// exists. It will return an error if an entry exists for the source indexes
// with a different set of dest indexes.
func (b *Tracker) getMergeProgressLocked(m scexec.Merge) (*mergeProgress, error) {
	p, ok := b.getTableIndexMergeProgressLocked(m)
	if !ok {
		return nil, nil
	}
	if err := p.matches(m); err != nil {
		return nil, err
	}
	return p, nil
}

func (b *Tracker) getTableIndexMergeProgressLocked(m scexec.Merge) (*mergeProgress, bool) {
	if p, ok := b.mu.mergeProgress[toMergeKey(m)]; ok {
		return p, true
	}
	return nil, false
}

// getFractionRangesFinished will compute the fraction of ranges finished
// relative to the set of ranges in each backfill or merge being tracked since
// the tracker was constructed. It is not adjusted to deal with
// origFractionCompleted. If updated is false, no usable fraction is
// returned.
//
// The computation of the fraction works by seeing how many ranges remain
// for each backfill and for each merge  and comparing that to the initial
// calculation of the number of ranges for the backfill and merges as computed
// by this function.
func (b *Tracker) getFractionRangesFinished(
	ctx context.Context,
) (updated bool, _ float32, _ error) {
	needsFlush, progresses := b.collectFractionProgressSpansForFlush()
	if !needsFlush {
		return false, 0, nil
	}
	var totalRanges int
	var completedRanges int
	for _, p := range progresses {
		total, completed, err := b.numRangesInSpanContainedBy(ctx, p.total, p.completed)
		if err != nil {
			return false, 0, err
		}
		totalRanges += total
		completedRanges += completed
	}
	if totalRanges == 0 {
		return true, 0, nil
	}

	// Check if any backfill is using distributed merge and calculate phase-aware
	// progress.
	for _, p := range progresses {
		if p.useDistributedMerge {
			progress := calculatePhasedProgress(
				p.distributedMergePhase,
				completedRanges, totalRanges,
				p.mergeIterationTasksCompleted, p.mergeIterationTasksTotal,
			)
			return true, progress, nil
		}
	}

	return true, float32(completedRanges) / float32(totalRanges), nil
}

// calculatePhasedProgress computes the overall fraction complete for a
// distributed merge backfill using a 3-phase model:
//   - Phase 0 (map): 0-33%
//   - Phase 1 (merge iteration 1): 33-66%
//   - Phase 2 (merge iteration 2): 66-100%
//
// The completedPhase parameter represents the last COMPLETED iteration:
//   - 0: Map phase complete, merge iterations not started or in progress
//   - 1: Merge iteration 1 complete, iteration 2 not started or in progress
//   - 2: Merge iteration 2 complete (distributed merge finished)
//
// To determine if we're actively processing the next iteration, we check if
// tasksTotal > 0. If so, progress is calculated for iteration completedPhase+1.
func calculatePhasedProgress(
	completedPhase int32, completedRanges, totalRanges int, tasksCompleted, tasksTotal int64,
) float32 {
	const phaseWidth = float32(1.0) / 3.0

	// Handle final complete state.
	if completedPhase == 2 {
		return 1.0
	}

	// completedPhase == 1: Iteration 1 complete.
	if completedPhase == 1 {
		// Base progress: 2 phases complete = 66%.
		base := 2.0 * phaseWidth
		if tasksTotal > 0 {
			// Actively processing iteration 2.
			return base + (float32(tasksCompleted)/float32(tasksTotal))*phaseWidth
		}
		// Iteration 1 just finished, iteration 2 not yet started.
		return base
	}

	// completedPhase == 0: Map complete, iteration 1 may be in progress.
	if tasksTotal > 0 {
		// Actively processing iteration 1.
		base := phaseWidth // Map phase complete = 33%
		return base + (float32(tasksCompleted)/float32(tasksTotal))*phaseWidth
	}

	// Map phase in progress (span-based progress).
	if totalRanges > 0 {
		return (float32(completedRanges) / float32(totalRanges)) * phaseWidth
	}
	return 0
}

type fractionProgressSpans struct {
	total     roachpb.Span
	completed []roachpb.Span

	// useDistributedMerge indicates whether the distributed merge pipeline is
	// active for this backfill.
	useDistributedMerge bool

	// distributedMergePhase tracks which phase of distributed merge we're in.
	// 0 = map phase, 1 = merge iteration 1, 2 = merge iteration 2.
	distributedMergePhase int32

	// mergeIterationTasksTotal is the total number of tasks in current iteration.
	mergeIterationTasksTotal int64

	// mergeIterationTasksCompleted is len(MergeIterationCompletedTasks).
	mergeIterationTasksCompleted int64
}

func (b *Tracker) collectFractionProgressSpansForFlush() (
	needsFlush bool,
	progress []fractionProgressSpans,
) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, p := range b.mu.backfillProgress {
		needsFlush = needsFlush || p.needsFractionFlush
	}
	for _, p := range b.mu.mergeProgress {
		needsFlush = needsFlush || p.needsFractionFlush
	}
	if !needsFlush {
		return false, nil
	}
	progress = make([]fractionProgressSpans, 0, len(b.mu.backfillProgress)+len(b.mu.mergeProgress))
	for _, p := range b.mu.backfillProgress {
		p.needsFractionFlush = false
		progress = append(progress, fractionProgressSpans{
			total:                        p.totalSpan,
			completed:                    p.CompletedSpans,
			useDistributedMerge:          len(p.SSTStoragePrefixes) > 0,
			distributedMergePhase:        p.DistributedMergePhase,
			mergeIterationTasksTotal:     p.MergeIterationTasksTotal,
			mergeIterationTasksCompleted: int64(len(p.MergeIterationCompletedTasks)),
		})
	}
	for _, p := range b.mu.mergeProgress {
		p.needsFractionFlush = false
		for i, s := range p.totalSpans {
			progress = append(progress, fractionProgressSpans{
				total:     s,
				completed: p.CompletedSpans[i],
			})
		}
	}
	return true, progress
}

func (b *Tracker) collectProgressForCheckpointFlush() (
	needsFlush bool,
	backfillProgress []scexec.BackfillProgress,
	mergeProgress []scexec.MergeProgress,
) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, p := range b.mu.backfillProgress {
		if p.needsCheckpointFlush {
			needsFlush = true
			break
		}
	}
	if !needsFlush {
		for _, p := range b.mu.mergeProgress {
			if p.needsCheckpointFlush {
				needsFlush = true
				break
			}
		}
	}
	if !needsFlush {
		return false, nil, nil
	}
	for _, p := range b.mu.backfillProgress {
		p.needsCheckpointFlush = false
		backfillProgress = append(backfillProgress, p.BackfillProgress)
	}
	for _, p := range b.mu.mergeProgress {
		p.needsCheckpointFlush = false
		mergeProgress = append(mergeProgress, p.MergeProgress)
	}
	return needsFlush, backfillProgress, mergeProgress
}

type progressReportFlags struct {
	// needsCheckpointFlush is set when the backfillProgress or mergeProgress
	// is updated before any call to FlushCheckpoint has occurred.
	// It is cleared when collecting the progresses for flushing.
	needsCheckpointFlush bool
	// needsFractionFlush is parallel to needsCheckpointFlush.
	needsFractionFlush bool
}

func sameIndexIDSet(ds []descpb.IndexID, ds2 []descpb.IndexID) bool {
	toSet := func(ids []descpb.IndexID) (s intsets.Fast) {
		for _, id := range ids {
			s.Add(int(id))
		}
		return s
	}
	return toSet(ds).Equals(toSet(ds2))
}

type backfillKey struct {
	tableID       descpb.ID
	sourceIndexID descpb.IndexID
}

func toBackfillKey(bf scexec.Backfill) backfillKey {
	return backfillKey{
		tableID:       bf.TableID,
		sourceIndexID: bf.SourceIndexID,
	}
}

type mergeKey struct {
	tableID        descpb.ID
	sourceIndexIDs string
}

func toMergeKey(m scexec.Merge) mergeKey {
	var ids intsets.Fast
	for _, id := range m.SourceIndexIDs {
		ids.Add(int(id))
	}
	return mergeKey{
		tableID:        m.TableID,
		sourceIndexIDs: ids.String(),
	}
}
