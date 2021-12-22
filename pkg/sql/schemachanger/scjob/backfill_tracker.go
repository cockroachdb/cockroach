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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// backfillTracker is used to receive progress updates on index backfills
// and periodically write them. The data structure supports receiving updates
// from multiple concurrent backfills. Additionally, it supports writing two
// different types of progress updates: a small fraction completed update,
// which is written at a higher frequency, and a larger checkpoint which
// records the remaining spans of the source index to scan.
type backfillTracker struct {
	backfillTrackerConfig
	codec keys.SQLCodec

	mu struct {
		syncutil.Mutex

		progress map[tableIndexKey]*progress
	}
}

// backfillTrackerConfig represents the underlying dependencies of the
// backfillTracker. It exists in this abstracted form largely to facilitate
// testing.
type backfillTrackerConfig struct {
	numRangesInSpans      func(context.Context, roachpb.Span, []roachpb.Span) (total, contained int, _ error)
	writeProgressFraction func(_ context.Context, fractionProgressed float32) error
	writeCheckpoint       func(context.Context, []scexec.BackfillProgress) error
}

var _ scexec.BackfillTracker = (*backfillTracker)(nil)

func newBackfillTracker(
	codec keys.SQLCodec, cfg backfillTrackerConfig, initialProgress []scexec.BackfillProgress,
) *backfillTracker {
	bt := &backfillTracker{
		codec:                 codec,
		backfillTrackerConfig: cfg,
	}
	bt.mu.progress = make(map[tableIndexKey]*progress)
	for _, p := range initialProgress {
		bp := newProgress(codec, p)

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
		p = newProgress(b.codec, progress)
		b.mu.progress[toKey(progress.Backfill)] = p
	} else {
		p.BackfillProgress = progress
	}
	p.needsCheckpointFlush = true
	p.needsFractionFlush = true
	return nil
}

func newProgress(codec keys.SQLCodec, bp scexec.BackfillProgress) *progress {
	indexPrefix := codec.IndexPrefix(uint32(bp.TableID), uint32(bp.SourceIndexID))
	indexSpan := roachpb.Span{
		Key:    indexPrefix,
		EndKey: indexPrefix.PrefixEnd(),
	}
	return &progress{
		BackfillProgress: bp,
		totalSpan:        indexSpan,
	}
}

func (b *backfillTracker) FlushFractionCompleted(ctx context.Context) error {
	updated, fractionRangesFinished, err := b.getFractionRangesFinished(ctx)
	if err != nil || !updated {
		return err
	}
	return b.writeProgressFraction(ctx, fractionRangesFinished)
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

func (b *backfillTracker) getTableIndexBackfillProgress(bf scexec.Backfill) (progress, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if p, ok := b.getTableIndexBackfillProgressLocked(bf); ok {
		return *p, true
	}
	return progress{}, false
}

// getBackfillProgressLocked is used to get a mutable handle to the backfill
// progress for a given backfill. It will return nil, nil if no such entry
// exists. It will return an error if an entry exists for the source index
// with a different set of dest indexes.
func (b *backfillTracker) getBackfillProgressLocked(bf scexec.Backfill) (*progress, error) {
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
) (*progress, bool) {
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
	needsFlush, progresses := b.collectProgressForFractionFlush()
	if !needsFlush {
		return false, 0, nil
	}
	var totalRanges int
	var completedRanges int
	for _, p := range progresses {
		total, completed, err := b.numRangesInSpans(ctx, p.total, p.completed)
		if err != nil {
			return false, 0, err
		}
		totalRanges += total
		completedRanges += completed
	}
	if totalRanges == 0 {
		return true, 0, nil
	}
	return true, float32(completedRanges) / float32(totalRanges), nil
}

type fractionProgressState struct {
	total     roachpb.Span
	completed []roachpb.Span
}

func (b *backfillTracker) collectProgressForFractionFlush() (
	needsFlush bool,
	progress []fractionProgressState,
) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, p := range b.mu.progress {
		needsFlush = needsFlush || p.needsFractionFlush
	}
	if !needsFlush {
		return false, nil
	}
	progress = make([]fractionProgressState, 0, len(b.mu.progress))
	for _, p := range b.mu.progress {
		p.needsFractionFlush = false
		progress = append(progress, fractionProgressState{
			total:     p.totalSpan,
			completed: p.CompletedSpans,
		})
	}
	return true, progress
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

type progress struct {
	scexec.BackfillProgress

	// needsCheckpointFlush is set when the progress is updated before any
	// call to FlushCheckpoint has occurred. It is cleared when collecting
	// the progresses for flushing.
	needsCheckpointFlush bool
	needsFractionFlush   bool

	// origRanges is initialized to -1 as a sentinel. The first time that
	// nRanges is set for this progress, origRanges is set to that initial
	// value.
	totalSpan roachpb.Span
}

func (p progress) matches(bf scexec.Backfill) error {
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
