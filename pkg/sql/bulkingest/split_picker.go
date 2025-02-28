// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkingest

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/errors"
)

func newSSTError(message string, sst1, sst2 execinfrapb.BulkMergeSpec_SST) error {
	return errors.Newf(
		"%s: (uri:'%s'[start:%s, end:%s]) and (uri:'%s'[start:%s, end:%s])",
		message, sst1.Uri, sst1.StartKey, sst1.EndKey, sst2.Uri, sst2.StartKey, sst2.EndKey,
	)
}

// pickSplits picks which spans to split on based on the input SSTs. The splits are chosen
// so that each SST is contained within exactly one output span. The output spans are contiguous
// and non-overlapping. Splits are chosen based on the start key of the following SST.
//
// Every SST must be contained within exactly one of the input spans.
//
// The input spans must be ordered by start key and be non-overlapping. The
// input SSTs must be contained within the input spans, must be ordered by start
// key, and must be non-overlapping.
func pickSplits(
	spans []roachpb.Span, ssts []execinfrapb.BulkMergeSpec_SST,
) ([]roachpb.Span, error) {
	if len(ssts) == 0 {
		return spans, nil
	}
	if len(spans) == 0 {
		return nil, errors.New("no spans provided")
	}

	// Validate spans are ordered and non-overlapping
	for i := 1; i < len(spans); i++ {
		if !less(spans[i-1].Key, spans[i].Key) {
			return nil, errors.Newf("spans not ordered: %s > %s", spans[i-1].Key, spans[i].Key)
		}
		if overlaps(spans[i-1], spans[i]) {
			return nil, errors.Newf("spans are overlapping: %s overlaps with %s", spans[i-1].EndKey, spans[i].Key)
		}
	}

	// Validate SSTs are ordered and non-overlapping
	for i := 1; i < len(ssts); i++ {
		prev, curr := (ssts[i-1].StartKey), ssts[i].StartKey
		if !less(prev, curr) {
			return nil, newSSTError("out of order ingest sst", ssts[i-1], ssts[i])
		}
		if overlaps(spanFromSST(ssts[i-1]), spanFromSST(ssts[i])) {
			return nil, newSSTError("overlapping ingest sst", ssts[i-1], ssts[i])
		}
	}

	result := make([]roachpb.Span, 0, len(ssts))
	sstIdx := 0

	for _, span := range spans {
		spanSSTStartIdx := sstIdx
		for ; sstIdx < len(ssts); sstIdx++ {
			sstStart := (ssts[sstIdx].StartKey)
			if !less(sstStart, span.EndKey) {
				break
			}

			sstEnd := (ssts[sstIdx].EndKey)
			if !less(sstEnd, span.EndKey) && !sstEnd.Equal(span.EndKey) {
				return nil, errors.Newf("SST ending at %s extends beyond containing span ending at %s",
					sstEnd, span.EndKey)
			}
			if less(sstStart, span.Key) {
				return nil, errors.Newf("SST starting at %s begins before containing span starting at %s",
					sstStart, span.Key)
			}
		}

		if spanSSTStartIdx == sstIdx {
			result = append(result, span)
			continue
		}

		spanSplits := pickSplitsForSpan(span, ssts[spanSSTStartIdx:sstIdx])
		result = append(result, spanSplits...)
	}

	return result, nil
}

// pickSplitsForSpan splits a single span based on the SSTs that overlap with
// it. The output spans cover the entire input span, are non-overlapping, and
// are contiguous. Each output span contains exactly one SST.
func pickSplitsForSpan(span roachpb.Span, ssts []execinfrapb.BulkMergeSpec_SST) []roachpb.Span {
	if len(ssts) == 0 {
		return []roachpb.Span{span}
	}

	result := make([]roachpb.Span, 0, len(ssts))

	spanStart := span.Key

	for i := 1; i < len(ssts); i++ {
		result = append(result, roachpb.Span{
			Key:    spanStart,
			EndKey: (ssts[i].StartKey),
		})
		spanStart = (ssts[i].StartKey)
	}

	result = append(result, roachpb.Span{
		Key:    spanStart,
		EndKey: span.EndKey,
	})

	return result
}

// less returns true if a is less than b
func less(a, b roachpb.Key) bool {
	return a.Compare(b) < 0
}

// overlaps returns true if span a overlaps with span b
func overlaps(a, b roachpb.Span) bool {
	// Two spans overlap if one's end key is greater than the other's start key
	return a.EndKey.Compare(b.Key) > 0 && b.EndKey.Compare(a.Key) > 0
}

// spanFromSST returns the span that matches the SST's start and end keys.
func spanFromSST(sst execinfrapb.BulkMergeSpec_SST) roachpb.Span {
	return roachpb.Span{
		Key:    (sst.StartKey),
		EndKey: (sst.EndKey),
	}
}
