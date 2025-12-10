// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkingest

// This file exports internal symbols for testing purposes.

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
