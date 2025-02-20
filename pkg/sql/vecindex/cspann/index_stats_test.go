// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/floats/scalar"
)

func TestStatsManager(t *testing.T) {
	ctx := context.Background()

	globalStats := IndexStats{NumPartitions: 1}
	var skippedMerge bool
	mergeStats := func(ctx context.Context, stats *IndexStats, skipMerge bool) error {
		skippedMerge = skipMerge
		if !skipMerge {
			globalStats = *stats
		}
		*stats = globalStats
		return nil
	}

	var stats statsManager
	require.NoError(t, stats.Init(ctx, mergeStats))
	require.Equal(t, "1 levels, 1 partitions, 0.00 vectors/partition.\nCV stats:\n", stats.Format())
	require.True(t, skippedMerge)

	// Get zero, negative, positive z-scores.
	zscore := stats.ReportSearch(2, []float64{1, 2, 3}, true /* updateStats */)
	require.Equal(t, float64(0), scalar.Round(zscore, 2))
	zscore = stats.ReportSearch(2, []float64{2, 2.5, 3}, true /* updateStats */)
	require.Equal(t, float64(-1), scalar.Round(zscore, 2))
	zscore = stats.ReportSearch(2, []float64{1, 3, 5}, true /* updateStats */)
	require.Equal(t, float64(0.57), scalar.Round(zscore, 2))

	// updateStats=false.
	zscore = stats.ReportSearch(2, []float64{1, 2, 10}, false /* updateStats */)
	require.Equal(t, float64(2.16), scalar.Round(zscore, 2))
	zscore = stats.ReportSearch(2, []float64{1, 2, 10}, false /* updateStats */)
	require.Equal(t, float64(2.16), scalar.Round(zscore, 2))

	// Not enough distances to compute stats.
	zscore = stats.ReportSearch(2, []float64{1}, true /* updateStats */)
	require.Equal(t, float64(0), zscore)

	// All distances are zero.
	zscore = stats.ReportSearch(2, []float64{0, 0, 0}, true /* updateStats */)
	require.Equal(t, float64(0), zscore)

	// Use a higher level.
	zscore = stats.ReportSearch(3, []float64{1, 2, 3}, true /* updateStats */)
	require.Equal(t, float64(0), scalar.Round(zscore, 2))

	// Format stats.
	require.Equal(t,
		"3 levels, 1 partitions, 0.00 vectors/partition.\n"+
			"CV stats:\n"+
			"  level 2 - mean: 0.4987, stdev: 0.2960\n"+
			"  level 3 - mean: 0.5000, stdev: 0.0000\n", stats.Format())
}
