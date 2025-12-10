// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkingest

// This file exports internal symbols for testing purposes.

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
)

// Export unexported functions for testing.
var (
	PickSplits           = pickSplits
	PickSplitsForSpan    = pickSplitsForSpan
	SplitAndScatterSpans = splitAndScatterSpans
)

// TaskSlice exports the generic taskSlice function for testing.
func TaskSlice[T any](tasks []T, workerId, numWorkers int) []T {
	return taskSlice(tasks, workerId, numWorkers)
}

// PickSplitsFunc is the exported type for pickSplits.
type PickSplitsFunc = func(spans []roachpb.Span, ssts []execinfrapb.BulkMergeSpec_SST) ([]roachpb.Span, error)

// PickSplitsForSpanFunc is the exported type for pickSplitsForSpan.
type PickSplitsForSpanFunc = func(span roachpb.Span, ssts []execinfrapb.BulkMergeSpec_SST) ([]roachpb.Span, error)

// SplitAndScatterSpansFunc is the exported type for splitAndScatterSpans.
type SplitAndScatterSpansFunc = func(ctx context.Context, db *kv.DB, spans []roachpb.Span) error
