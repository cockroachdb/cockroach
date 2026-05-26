// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"context"
	"math"
	"slices"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"gonum.org/v1/gonum/stat"
)

// statsAlpha is the weight applied to a new sample of search distances when
// computing exponentially weighted moving averages.
const statsAlpha = 0.01

// statsReportingInterval specifies how many vectors need to be inserted or
// deleted before local statistics will be merged with global statistics.
const statsReportingInterval = 100

// mergeStatsFunc defines the function called by the stats manager when it needs
// to read and update global statistics.
type mergeStatsFunc func(ctx context.Context, stats *IndexStats, skipMerge bool) error

// statsManager maintains locally-cached statistics about the vector index that
// are used by adaptive search to improve search accuracy. Local statistics are
// updated as the index is searched during Insert and Delete operations.
// Periodically, the local statistics maintained by various processes are merged
// with global statistics that are centrally stored.
//
// All methods in statsManager are thread-safe.
type statsManager struct {
	// mergeStats is called to read and update global statistics.
	mergeStats mergeStatsFunc

	// addRemoveCount counts the number of vectors added to the index or removed
	// from it since the last stats merge.
	addRemoveCount atomic.Int64

	// mu protects its fields from concurrent access on multiple goroutines.
	// The lock must be acquired before using these fields.
	mu struct {
		syncutil.Mutex

		// stats maintains locally-updated statistics. These are periodically
		// merged with global statistics.
		stats IndexStats
	}
}

// Init initializes the stats manager for use.
func (sm *statsManager) Init(ctx context.Context, mergeStats mergeStatsFunc) error {
	sm.mergeStats = mergeStats

	// Fetch global statistics to be used as the initial starting point for local
	// statistics.
	err := sm.mergeStats(ctx, &sm.mu.stats, true /* skipMerge */)
	if err != nil {
		return errors.Wrap(err, "fetching starting stats")
	}
	return nil
}

// Format returns the local statistics as a formatted string.
func (sm *statsManager) Format() string {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	return sm.mu.stats.String()
}

// OnAddOrRemoveVector is called when vectors are added to the index or removed
// from it. Every N adds/removes, local statistics are merged with global
// statistics.
func (sm *statsManager) OnAddOrRemoveVector(ctx context.Context) error {
	// Determine whether to merge local statistics with global statistics. Do
	// this in a separate function to avoid holding the lock during the call to
	// MergeStats.
	stats, shouldMerge := func() (stats IndexStats, shouldMerge bool) {
		// Determine if it's time to merge statistics.
		if sm.addRemoveCount.Add(1) != statsReportingInterval {
			return IndexStats{}, false
		}

		// Copy CVStats while holding the lock.
		sm.mu.Lock()
		defer sm.mu.Unlock()
		return sm.mu.stats.Clone(), true
	}()
	if !shouldMerge {
		return nil
	}

	// Merge local stats with store stats.
	err := sm.mergeStats(ctx, &stats, false /* skipMerge */)
	if err != nil {
		return errors.Wrap(err, "merging stats")
	}

	// Update local stats with the merged stats, within scope of lock.
	// NOTE: This will lose any updates that have been made to local stats
	// during the merge. This is typically a short interval, and exact stats
	// aren't necessary, so this is OK.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.mu.stats = stats
	sm.addRemoveCount.Store(0)

	return nil
}

// ReportSearch returns a Z-score that is statistically correlated with the
// difficulty of the search. It measures how "spread out" search candidates are,
// in terms of distance to one another, relative to past searches at the same
// level of the K-means tree. A negative Z-score indicates that candidates were
// more bunched up than usual. This means that the search could be more
// difficult, with many good candidates scattered across many partitions. A
// positive Z-score indicates the opposite, that candidates are more spread out
// than usual - less effort is probably needed to find the best matches.
//
// If "updateStats" is true, then per-level coefficient of variation (CV)
// statistics are updated to reflect this search. CV statistics record the
// "spread" of distances at a given level of the tree and are used to calculate
// the Z-score of a particular search.
func (sm *statsManager) ReportSearch(
	level Level, squaredDistances []float64, updateStats bool,
) float64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if len(squaredDistances) < 2 {
		// Not enough distances to compute stats, so return Z-score of zero.
		return 0
	}

	offset := int(level - SecondLevel)
	if offset < 0 {
		panic(errors.AssertionFailedf("ReportSearch should never be called for the leaf level"))
	} else if offset >= len(sm.mu.stats.CVStats) {
		// Need to add more Z-Score levels.
		sm.mu.stats.CVStats = slices.Grow(sm.mu.stats.CVStats, offset+1-len(sm.mu.stats.CVStats))
		sm.mu.stats.CVStats = sm.mu.stats.CVStats[:offset+1]
	}

	return deriveZScore(&sm.mu.stats.CVStats[offset], squaredDistances, updateStats)
}

// deriveZScore calculates the Z-score of a search, which is given by this
// formula:
//
//	ZScore = (CV - Mean_CV) / StdDev_CV
//
// CV stands for coefficient of variation, and measures the normalized spread
// of distances between search candidates:
//
//	CV = StdDev_Distances / Mean_Distances
//
// The Z-score compares the CV of this search with the average, normalized CV of
// previous searches.
func deriveZScore(cvstats *CVStats, squaredDistances []float64, updateStats bool) float64 {
	// Need at least 2 distance values to calculate the CV.
	if len(squaredDistances) < 2 {
		// Return zero Z-score, meaning no variation from the mean.
		return 0
	}

	// Compute the coefficient of variation (CV) for the set of distances using
	// this formula: cv = stdev / mean. CV gives the variation of values relative
	// to the mean so that different distance scales are more comparable.
	mean, stdev := stat.MeanStdDev(squaredDistances, nil)
	if mean == 0 {
		// Mean of zero could happen if all distances were zero. In this
		// pathological case, just return a Z-score of zero.
		return 0
	}
	cv := stdev / mean

	if updateStats {
		if cvstats.Mean == 0 {
			// Use first CV value as initial mean.
			cvstats.Mean = cv
		} else {
			// Calculate the exponentially weighted moving average and standard
			// deviation for the last ~100 CV samples. Formulas can be found in
			// the paper "Incremental calculation of weighted mean and variance":
			// https://fanf2.user.srcf.net/hermes/doc/antiforgery/stats.pdf
			cvstats.Mean = cv*statsAlpha + (1-statsAlpha)*cvstats.Mean

			diff := cv - cvstats.Mean
			if cvstats.Variance == 0 {
				// Compute variance of first 2 CV values.
				cvstats.Variance = diff * diff
			} else {
				incr := statsAlpha * diff
				cvstats.Variance = (1 - statsAlpha) * (cvstats.Variance + diff*incr)
			}
		}
	}

	// Calculate the Z-score.
	if cvstats.Variance == 0 {
		// Variance of zero could happen if all distances have been the same. In
		// this pathological case, just return a Z-score of zero.
		return 0
	}
	return (cv - cvstats.Mean) / math.Sqrt(cvstats.Variance)
}
