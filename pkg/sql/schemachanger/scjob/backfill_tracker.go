// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scjob

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// TODO(ajwerner): Deal with setting the running status to indicate scanning
// and backfilling.

// backfillTracker is used to receive progress updates on index backfills
// and periodically write them. The data structure supports receiving updates
// from multiple concurrent backfills. Additionally, it supports writing two
// different types of progress updates: a small fraction completed update,
// which is written at a higher frequency, and a larger checkpoint which
// records the remaining spans of the source index to scan.
type backfillTracker struct {
	backfillTrackerConfig

	// origFractionCompleted corresponds to the fraction completed at the
	// time the tracker was constructed. Any new work is assumed to not
	// include work performed prior to the construction of the tracker.
	origFractionCompleted float32

	mu struct {
		syncutil.Mutex

		progress map[tableIndexKey]*backfillProgress
	}
}

// backfillTrackerConfig represents the underlying dependencies of the
// backfillTracker. It exists in this abstracted form largely to facilitate
// testing.
type backfillTrackerConfig struct {
	numRangesInSpans      func(context.Context, []roachpb.Span) (nRanges int, _ error)
	writeProgressFraction func(_ context.Context, fractionProgressed float32) error
	writeCheckpoint       func(context.Context, []scexec.BackfillProgress) error
}

var _ scexec.BackfillTracker = (*backfillTracker)(nil)

func newBackfillTracker(
	cfg backfillTrackerConfig,
	initialProgress []scexec.BackfillProgress,
	origFractionCompleted float32,
) *backfillTracker {
	bt := &backfillTracker{
		backfillTrackerConfig: cfg,
		origFractionCompleted: origFractionCompleted,
	}
	bt.mu.progress = make(map[tableIndexKey]*backfillProgress)
	for _, p := range initialProgress {
		bp := &backfillProgress{
			BackfillProgress: p,
			origRanges:       -1,
		}

		bt.mu.progress[toKey(p.Backfill)] = bp
	}
	return bt
}

func (b *backfillTracker) GetBackfillProgress(
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

func (b *backfillTracker) SetBackfillProgress(
	ctx context.Context, progress scexec.BackfillProgress,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	p, err := b.getBackfillProgressLocked(progress.Backfill)
	if err != nil {
		return err
	}
	if p == nil {
		p = &backfillProgress{
			BackfillProgress: progress,
			origRanges:       -1,
		}
		b.mu.progress[toKey(progress.Backfill)] = p
	} else {
		if len(p.SpansToDo) == 0 && len(progress.SpansToDo) > 0 {
			p.origRanges = -1
		}
		p.BackfillProgress = progress
	}
	p.needsCheckpointFlush = true
	return nil
}

func (b *backfillTracker) FlushFractionCompleted(ctx context.Context) error {
	updated, fractionRangesFinished, err := b.getFractionRangesFinished(ctx)
	if err != nil || !updated {
		return err
	}
	return b.writeProgressFraction(ctx,
		b.origFractionCompleted+(1-b.origFractionCompleted)*fractionRangesFinished)
}

func (b *backfillTracker) FlushCheckpoint(ctx context.Context) error {
	needsFlush, progress := b.collectProgressForCheckpointFlush()
	if !needsFlush {
		return nil
	}
	sort.Slice(progress, func(i, j int) bool {
		if progress[i].TableID != progress[j].TableID {
			return progress[i].TableID < progress[j].TableID
		}
		return progress[i].SourceIndexID < progress[j].SourceIndexID
	})
	return b.writeCheckpoint(ctx, progress)
}

func (b *backfillTracker) getTableIndexBackfillProgress(
	bf scexec.Backfill,
) (backfillProgress, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if p, ok := b.getTableIndexBackfillProgressLocked(bf); ok {
		return *p, true
	}
	return backfillProgress{}, false
}

// getBackfillProgressLocked is used to get a mutable handle to the backfill
// progress for a given backfill. It will return nil, nil if no such entry
// exists. It will return an error if an entry exists for the source index
// with a different set of dest indexes.
func (b *backfillTracker) getBackfillProgressLocked(bf scexec.Backfill) (*backfillProgress, error) {
	p, ok := b.getTableIndexBackfillProgressLocked(bf)
	if !ok {
		return nil, nil
	}
	if err := p.matches(bf); err != nil {
		return nil, err
	}
	return p, nil
}

func (b *backfillTracker) getTableIndexBackfillProgressLocked(
	bf scexec.Backfill,
) (*backfillProgress, bool) {
	if p, ok := b.mu.progress[toKey(bf)]; ok {
		return p, true
	}
	return nil, false
}

// getFractionRangesFinished will compute the fraction of ranges finished
// relative to the set of ranges in each backfill being tracked since the
// tracker was constructed. It is not adjusted to deal with
// origFractionCompleted. If updated is false, no usable fraction is
// returned.
//
// The computation of the fraction works by seeing how many ranges remain
// for each backfill and comparing that to the initial calculation of the
// number of ranges for the backfill as computed by this function.
func (b *backfillTracker) getFractionRangesFinished(
	ctx context.Context,
) (updated bool, _ float32, _ error) {
	progresses := b.collectProgress()

	var origNumRanges int
	var updatedNumRanges int
	var updatedAny bool
	for i := range progresses {
		p := &progresses[i]
		numRanges, err := b.numRangesInSpans(ctx, p.SpansToDo)
		if err != nil {
			return false, 0, err
		}
		origRanges, updated := b.updateNumRanges(p.Backfill, numRanges)
		updatedAny = updatedAny || updated
		origNumRanges += origRanges
		updatedNumRanges += numRanges
	}
	if origNumRanges == 0 || !updatedAny {
		return false, 0, nil
	}
	return true, float32(origNumRanges-updatedNumRanges) / float32(origNumRanges), nil
}

func (b *backfillTracker) collectProgress() (progress []scexec.BackfillProgress) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, p := range b.mu.progress {
		progress = append(progress, p.BackfillProgress)
	}
	return progress
}

func (b *backfillTracker) collectProgressForCheckpointFlush() (
	needsFlush bool,
	progress []scexec.BackfillProgress,
) {
	for _, p := range b.mu.progress {
		if p.needsCheckpointFlush {
			needsFlush = true
			break
		}
	}
	if !needsFlush {
		return false, nil
	}
	for _, p := range b.mu.progress {
		p.needsCheckpointFlush = false
		progress = append(progress, p.BackfillProgress)
	}
	return needsFlush, progress
}

// updateNumRanges will update the number of ranges remaining for
// a given backfill. If there has never been an update to the backfill
// progress for the range count, this number is used as the original
// number of ranges. The updated return parameter will be true if the
// original number of ranges was already set and the provided range
// count has changed since the last call.
func (b *backfillTracker) updateNumRanges(
	backfill scexec.Backfill, ranges int,
) (origRanges int, updated bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// It's safe to assume that this exists because entries cannot be removed
	// from the set and we only call this after getting the entry from the set.
	p := b.mu.progress[toKey(backfill)]
	if p.origRanges == -1 {
		p.origRanges = ranges
		p.nRanges = ranges
		return p.origRanges, false // updated
	}
	if p.nRanges == ranges {
		return p.origRanges, false
	}
	p.nRanges = ranges
	return p.origRanges, true
}

type backfillProgress struct {
	scexec.BackfillProgress

	// needsCheckpointFlush is set when the progress is updated before any
	// call to FlushCheckpoint has occurred. It is cleared when collecting
	// the progresses for flushing.
	needsCheckpointFlush bool

	// origRanges is initialized to -1 as a sentinel. The first time that
	// nRanges is set for this progress, origRanges is set to that initial
	// value.
	origRanges, nRanges int
}

func (p backfillProgress) matches(bf scexec.Backfill) error {
	if bf.TableID == p.TableID &&
		bf.SourceIndexID == p.SourceIndexID &&
		sameIndexIDSet(bf.DestIndexIDs, p.DestIndexIDs) {
		return nil
	}
	return errors.AssertionFailedf(
		"backfill %v does not match stored progress for %v",
		bf, p.Backfill,
	)
}

func sameIndexIDSet(ds []descpb.IndexID, ds2 []descpb.IndexID) bool {
	toSet := func(ids []descpb.IndexID) (s util.FastIntSet) {
		for _, id := range ids {
			s.Add(int(id))
		}
		return s
	}
	return toSet(ds).Equals(toSet(ds2))
}

type tableIndexKey struct {
	tableID       descpb.ID
	sourceIndexID descpb.IndexID
}

func toKey(bf scexec.Backfill) tableIndexKey {
	return tableIndexKey{
		tableID:       bf.TableID,
		sourceIndexID: bf.SourceIndexID,
	}
}
