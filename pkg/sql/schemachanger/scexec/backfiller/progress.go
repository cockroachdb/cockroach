// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package backfiller contains logic for reading, writing, and tracking
// backfiller progress.
package backfiller

import (
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/errors"
)

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

func convertToJobMergeProgress(
	codec keys.SQLCodec, progresses []scexec.MergeProgress,
) ([]jobspb.MergeProgress, error) {
	ret := make([]jobspb.MergeProgress, len(progresses))
	for _, mp := range progresses {
		pairs := make([]jobspb.MergeProgress_MergePair, len(mp.SourceIndexIDs))
		for i, sourceID := range mp.SourceIndexIDs {
			strippedSpans, err := removeTenantPrefixFromSpans(codec, mp.CompletedSpans[i])
			if err != nil {
				return nil, err
			}
			pairs[i] = jobspb.MergeProgress_MergePair{
				SourceIndexID:  sourceID,
				DestIndexID:    mp.DestIndexIDs[i],
				CompletedSpans: strippedSpans,
			}
		}
		ret = append(ret, jobspb.MergeProgress{
			TableID:    mp.TableID,
			MergePairs: pairs,
		})
	}
	return ret, nil
}

func convertFromJobMergeProgress(
	codec keys.SQLCodec, progresses []jobspb.MergeProgress,
) []scexec.MergeProgress {
	ret := make([]scexec.MergeProgress, len(progresses))
	for _, jmp := range progresses {
		mp := scexec.MergeProgress{
			Merge: scexec.Merge{
				TableID:        jmp.TableID,
				SourceIndexIDs: make([]descpb.IndexID, len(jmp.MergePairs)),
				DestIndexIDs:   make([]descpb.IndexID, len(jmp.MergePairs)),
			},
			CompletedSpans: make([][]roachpb.Span, len(jmp.MergePairs)),
		}
		for i, pair := range jmp.MergePairs {
			mp.SourceIndexIDs[i] = pair.SourceIndexID
			mp.DestIndexIDs[i] = pair.DestIndexID
			mp.CompletedSpans[i] = addTenantPrefixToSpans(codec, pair.CompletedSpans)
		}
		ret = append(ret, mp)
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

func newBackfillProgress(codec keys.SQLCodec, bp scexec.BackfillProgress) *backfillProgress {
	indexPrefix := codec.IndexPrefix(uint32(bp.TableID), uint32(bp.SourceIndexID))
	indexSpan := roachpb.Span{
		Key:    indexPrefix,
		EndKey: indexPrefix.PrefixEnd(),
	}
	return &backfillProgress{
		BackfillProgress: bp,
		totalSpan:        indexSpan,
	}
}

func newMergeProgress(codec keys.SQLCodec, mp scexec.MergeProgress) *mergeProgress {
	p := &mergeProgress{
		MergeProgress: mp,
		totalSpans:    make([]roachpb.Span, len(mp.SourceIndexIDs)),
	}
	for i, sourceID := range mp.SourceIndexIDs {
		prefix := codec.IndexPrefix(uint32(mp.TableID), uint32(sourceID))
		p.totalSpans[i] = roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}
	}
	return p
}

type backfillProgress struct {
	progressReportFlags
	scexec.BackfillProgress

	// totalSpan represents the complete span of the source index being
	// backfilled.
	totalSpan roachpb.Span
}

func (p backfillProgress) matches(bf scexec.Backfill) error {
	if bf.TableID == p.TableID &&
		bf.SourceIndexID == p.SourceIndexID &&
		sameIndexIDSet(bf.DestIndexIDs, p.DestIndexIDs) {
		return nil
	}
	return errors.AssertionFailedf(
		"backfill %v does not match stored backfillProgress for %v",
		bf, p.Backfill,
	)
}

type mergeProgress struct {
	progressReportFlags
	scexec.MergeProgress

	// totalSpans represents the complete span of each temporary index being
	// merged.
	totalSpans []roachpb.Span
}

func (p mergeProgress) matches(m scexec.Merge) error {
	if m.TableID == p.TableID &&
		sameIndexIDSet(m.SourceIndexIDs, p.SourceIndexIDs) &&
		sameIndexIDSet(m.DestIndexIDs, p.DestIndexIDs) {
		return nil
	}
	return errors.AssertionFailedf(
		"merge %v does not match stored mergeProgress for %v",
		m, p.Merge,
	)
}
