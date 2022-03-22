// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scdeps

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
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

func newBackfillTrackerConfig(
	ctx context.Context, codec keys.SQLCodec, db *kv.DB, rc RangeCounter, job *jobs.Job,
) backfillTrackerConfig {
	return backfillTrackerConfig{
		numRangesInSpanContainedBy: rc.NumRangesInSpanContainedBy,
		writeProgressFraction: func(_ context.Context, fractionProgressed float32) error {
			if err := job.FractionProgressed(
				ctx, nil /* txn */, jobs.FractionUpdater(fractionProgressed),
			); err != nil {
				return jobs.SimplifyInvalidStatusError(err)
			}
			return nil
		},
		writeCheckpoint: func(ctx context.Context, progresses []scexec.BackfillProgress) error {
			return job.Update(ctx, nil /* txn */, func(
				txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
			) error {
				pl := md.Payload
				jobProgress, err := convertToJobBackfillProgress(codec, progresses)
				if err != nil {
					return err
				}
				sc := pl.GetNewSchemaChange()
				sc.BackfillProgress = jobProgress
				ju.UpdatePayload(pl)
				return nil
			})
		},
	}
}

func convertToJobBackfillProgress(
	codec keys.SQLCodec, progresses []scexec.BackfillProgress,
) ([]jobspb.BackfillProgress, error) {
	ret := make([]jobspb.BackfillProgress, 0, len(progresses))
	for _, bp := range progresses {
		strippedSpans, err := removeTenantPrefixFromSpans(codec, bp.CompletedSpans)
		if err != nil {
			return nil, err
		}
		ret = append(ret, jobspb.BackfillProgress{
			TableID:        bp.TableID,
			SourceIndexID:  bp.SourceIndexID,
			DestIndexIDs:   bp.DestIndexIDs,
			WriteTimestamp: bp.MinimumWriteTimestamp,
			CompletedSpans: strippedSpans,
		})
	}
	return ret, nil
}

func convertFromJobBackfillProgress(
	codec keys.SQLCodec, progresses []jobspb.BackfillProgress,
) []scexec.BackfillProgress {
	ret := make([]scexec.BackfillProgress, 0, len(progresses))
	for _, bp := range progresses {
		ret = append(ret, scexec.BackfillProgress{
			Backfill: scexec.Backfill{
				TableID:       bp.TableID,
				SourceIndexID: bp.SourceIndexID,
				DestIndexIDs:  bp.DestIndexIDs,
			},
			MinimumWriteTimestamp: bp.WriteTimestamp,
			CompletedSpans:        addTenantPrefixToSpans(codec, bp.CompletedSpans),
		})
	}
	return ret
}

func addTenantPrefixToSpans(codec keys.SQLCodec, spans []roachpb.Span) []roachpb.Span {
	prefix := codec.TenantPrefix()
	prefix = prefix[:len(prefix):len(prefix)] // force realloc on append
	ret := make([]roachpb.Span, 0, len(spans))
	for _, sp := range spans {
		ret = append(ret, roachpb.Span{
			Key:    append(prefix, sp.Key...),
			EndKey: append(prefix, sp.EndKey...),
		})
	}
	return ret
}

func removeTenantPrefixFromSpans(
	codec keys.SQLCodec, spans []roachpb.Span,
) (ret []roachpb.Span, err error) {
	ret = make([]roachpb.Span, 0, len(spans))
	for _, sp := range spans {
		var stripped roachpb.Span
		if stripped.Key, err = codec.StripTenantPrefix(sp.Key); err != nil {
			return nil, err
		}
		if stripped.EndKey, err = codec.StripTenantPrefix(sp.EndKey); err != nil {
			return nil, err
		}
		ret = append(ret, stripped)
	}
	return ret, nil
}

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
// testing and to make dependency injection convenient.
type backfillTrackerConfig struct {

	// numRangesInSpanContainedBy returns the total number of ranges in the span
	// and the number of ranges in that span which are fully covered by the set
	// of spans provided.
	numRangesInSpanContainedBy func(
		context.Context, roachpb.Span, []roachpb.Span,
	) (total, contained int, _ error)

	// writeProgressFraction writes the progress fraction for presentation.
	writeProgressFraction func(_ context.Context, fractionProgressed float32) error

	// writeCheckpoint write the checkpoint the underlying store.
	writeCheckpoint func(context.Context, []scexec.BackfillProgress) error
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
	return true, float32(completedRanges) / float32(totalRanges), nil
}

type fractionProgressSpans struct {
	total     roachpb.Span
	completed []roachpb.Span
}

func (b *backfillTracker) collectFractionProgressSpansForFlush() (
	needsFlush bool,
	progress []fractionProgressSpans,
) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, p := range b.mu.progress {
		needsFlush = needsFlush || p.needsFractionFlush
	}
	if !needsFlush {
		return false, nil
	}
	progress = make([]fractionProgressSpans, 0, len(b.mu.progress))
	for _, p := range b.mu.progress {
		p.needsFractionFlush = false
		progress = append(progress, fractionProgressSpans{
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
	// needsFractionFlush is parallel to needsCheckpointFlush.
	needsFractionFlush bool

	// totalSpan represents the complete span of the source index being
	// backfilled.
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
