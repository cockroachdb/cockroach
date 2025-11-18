// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst

import (
	"bytes"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/errors"
)

// CombineFileInfo combines SST file metadata from multiple workers and
// determines how to split merge work across processors.
//
// The function takes SST files and row samples collected earlier, and produces:
//  1. A list of all SST files to be merged (consolidating metadata from all workers)
//  2. Merge task spans that partition the work across merge processors
//
// The row samples represent keys seen during import/backfill and are used to
// split schema spans into smaller merge tasks. Each merge processor will be
// assigned one or more spans and will merge only the SST data within those
// spans.
//
// For example, if schema spans are [a,z) and samples are [c, m, r], the merge
// spans will be [a,c), [c,m), [m,r), [r,z), allowing four processors to work in
// parallel on roughly equal amounts of data.
func CombineFileInfo(
	files []SSTFiles, schemaSpans []roachpb.Span,
) ([]execinfrapb.BulkMergeSpec_SST, []roachpb.Span, error) {
	// Validate that schema spans are properly ordered. This is a critical
	// precondition for the algorithm to work correctly.
	if err := validateSchemaSpansOrdered(schemaSpans); err != nil {
		return nil, nil, err
	}

	result := make([]execinfrapb.BulkMergeSpec_SST, 0)
	samples := make([]roachpb.Key, 0)
	for _, file := range files {
		for _, sst := range file.SST {
			result = append(result, execinfrapb.BulkMergeSpec_SST{
				StartKey: string(sst.StartKey),
				EndKey:   string(sst.EndKey),
				URI:      sst.URI,
			})
		}
		for _, sample := range file.RowSamples {
			samples = append(samples, roachpb.Key(sample))
		}
	}
	// Sort samples to ensure merge spans are non-overlapping and contiguous.
	// Samples are collected from multiple workers and arrive in arbitrary order.
	// getMergeSpans uses these samples as split points to create merge task spans.
	// If samples are unsorted (e.g., ["k", "d", "a"]), getMergeSpans would create
	// overlapping spans that cause the same keys to be processed multiple times,
	// resulting in duplicate data in the output SSTs.
	slices.SortFunc(samples, func(i, j roachpb.Key) int {
		return bytes.Compare(i, j)
	})

	mergeSpans, err := getMergeSpans(schemaSpans, samples)
	if err != nil {
		return nil, nil, err
	}

	return result, mergeSpans, nil
}

// getMergeSpans determines which spans should be used as merge tasks. The
// output spans must fully cover the input spans. The samples are used to
// determine where schema spans should be split.
//
// Precondition: schemaSpans must be sorted by start key and non-overlapping.
// This precondition is validated in validateSchemaSpansOrdered.
func getMergeSpans(schemaSpans []roachpb.Span, sortedSample []roachpb.Key) ([]roachpb.Span, error) {
	result := make([]roachpb.Span, 0, len(schemaSpans)+len(sortedSample))

	for _, span := range schemaSpans {
		samples, consumed := getCoveredSamples(span, sortedSample)

		// Validate: if we consumed more samples than we returned, it means some
		// samples were outside this span (either before it or in a gap). Since
		// schema spans are processed in order, any sample before this span indicates
		// a sample not covered by the schema spans.
		if consumed > len(samples) {
			// Find the first skipped sample for a clear error message
			for i := 0; i < consumed; i++ {
				if i >= len(samples) || !sortedSample[i].Equal(samples[i]) {
					return nil, errors.AssertionFailedf(
						"sample %q is before schema span [%q, %q); this indicates samples were collected for keys outside schema spans",
						sortedSample[i], span.Key, span.EndKey)
				}
			}
		}

		sortedSample = sortedSample[consumed:]

		startKey := span.Key
		for _, sample := range samples {
			// Skip samples that would create invalid (zero-length) spans.
			// This handles duplicates and samples at span boundaries.
			if bytes.Compare(sample, startKey) <= 0 {
				continue
			}
			result = append(result, roachpb.Span{
				Key:    startKey,
				EndKey: sample,
			})
			startKey = sample
		}
		result = append(result, roachpb.Span{
			Key:    startKey,
			EndKey: span.EndKey,
		})
	}

	// Validate that all samples were contained within schema spans. Any remaining
	// samples indicate they were collected after the last span.
	if len(sortedSample) > 0 {
		return nil, errors.AssertionFailedf(
			"samples outside schema spans: %d samples remain after processing all spans, first uncovered sample: %q",
			len(sortedSample), sortedSample[0])
	}

	return result, nil
}

// getCoveredSamples returns the samples within the given span and the total
// number of samples consumed (including any that were before the span start).
func getCoveredSamples(schemaSpan roachpb.Span, sortedSamples []roachpb.Key) ([]roachpb.Key, int) {
	// Count how many samples are before the span start.
	// Since sortedSamples are sorted and schema spans are processed in order,
	// samples before this span's start either:
	// 1. Should have been covered by a previous span, or
	// 2. Fall in a gap between spans.
	startIdx := 0
	for startIdx < len(sortedSamples) && bytes.Compare(sortedSamples[startIdx], schemaSpan.Key) < 0 {
		startIdx++
	}

	// Find samples within this span: [schemaSpan.Key, schemaSpan.EndKey)
	endIdx := startIdx
	for endIdx < len(sortedSamples) && bytes.Compare(sortedSamples[endIdx], schemaSpan.EndKey) < 0 {
		endIdx++
	}

	return sortedSamples[startIdx:endIdx], endIdx
}

// validateSchemaSpansOrdered checks that schema spans are sorted by start key
// and non-overlapping. This is a precondition for getMergeSpans to work correctly.
func validateSchemaSpansOrdered(schemaSpans []roachpb.Span) error {
	for i := 1; i < len(schemaSpans); i++ {
		if bytes.Compare(schemaSpans[i-1].Key, schemaSpans[i].Key) >= 0 {
			return errors.AssertionFailedf(
				"schema spans not ordered: span %d [%q, %q) >= span %d [%q, %q)",
				i-1, schemaSpans[i-1].Key, schemaSpans[i-1].EndKey,
				i, schemaSpans[i].Key, schemaSpans[i].EndKey)
		}
		if bytes.Compare(schemaSpans[i-1].EndKey, schemaSpans[i].Key) > 0 {
			return errors.AssertionFailedf(
				"schema spans overlapping: span %d ends at %q but span %d starts at %q",
				i-1, schemaSpans[i-1].EndKey, i, schemaSpans[i].Key)
		}
	}
	return nil
}
