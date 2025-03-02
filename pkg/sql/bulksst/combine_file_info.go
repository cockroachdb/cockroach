// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst

import (
	"bytes"
	"math/rand"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
)

// CombineFileInfo combines the SST files and picks splits based on the key sample.
func CombineFileInfo(
	files []SSTFiles, tableSpans []roachpb.Span,
) ([]execinfrapb.BulkMergeSpec_SST, []roachpb.Span) {
	totalSize := uint64(0)
	result := make([]execinfrapb.BulkMergeSpec_SST, 0)
	samples := make([]roachpb.Key, 0)
	for _, file := range files {
		for _, sst := range file.SST {
			totalSize += sst.FileSize
			result = append(result, execinfrapb.BulkMergeSpec_SST{
				StartKey: []byte(sst.StartKey),
				EndKey:   []byte(sst.EndKey),
				Uri:      sst.URI,
			})
		}
		for _, sample := range file.RowSamples {
			samples = append(samples, roachpb.Key(sample))
		}
	}

	shuffle(samples)

	targetSize := uint64(256 << 20)
	samples = samples[:totalSize/targetSize]

	// BUGFIX: We need to sort the samples for merge, otherwise the merge can end
	// up with overlapping spans and duplicate data.
	slices.SortFunc(samples, func(i, j roachpb.Key) int {
		return bytes.Compare(i, j)
	})

	slices.SortFunc(tableSpans, func(i, j roachpb.Span) int {
		return bytes.Compare(i.Key, j.Key)
	})

	spans := getMergeSpans(tableSpans, samples)

	return result, spans
}

func shuffle[T any](s []T) {
	for i := range s {
		j := rand.Intn(len(s))
		s[i], s[j] = s[j], s[i]
	}
}

// getMergeSpans determines which spans should be used as merge tasks. The
// output spans must fully cover the input spans. The samples are used to
// determine where schema spans should be split.
func getMergeSpans(schemaSpans []roachpb.Span, sortedSample []roachpb.Key) []roachpb.Span {
	// TODO(jeffswenson): validate that every sample is contained within a schema span
	result := make([]roachpb.Span, 0, len(schemaSpans)+len(sortedSample))

	for _, span := range schemaSpans {
		samples := getCoveredSamples(span, sortedSample)
		sortedSample = sortedSample[len(samples):]

		startKey := span.Key
		for _, sample := range samples {
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

	return result
}

func getCoveredSamples(span roachpb.Span, sortedSamples []roachpb.Key) []roachpb.Key {
	for i, sample := range sortedSamples {
		if bytes.Compare(span.EndKey, sample) <= 0 {
			return sortedSamples[:i]
		}
	}
	return sortedSamples
}
