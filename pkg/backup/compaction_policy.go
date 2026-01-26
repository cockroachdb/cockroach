// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/errors"
)

var (
	backupCompactionWindow = settings.RegisterIntSetting(
		settings.ApplicationLevel,
		"backup.compaction.window_size",
		"the number of backups to compact per compaction (must be greater than two and less than threshold)",
		3,
		settings.WithVisibility(settings.Reserved),
		settings.IntWithMinimum(3),
	)

	backupCompactionConcurrency = settings.RegisterIntSetting(
		settings.ApplicationLevel,
		"backup.compaction.concurrency",
		"the max number of concurrent compaction jobs to run per schedule",
		1,
		settings.WithVisibility(settings.Reserved),
		settings.IntWithMinimum(1),
	)
)

// compactionPolicy is a function that determines what backups to compact when
// given a chain of backups. It returns a set of [start, end) indices that
// represent the windows of backups to compact.
type compactionPolicy func(
	context.Context, *sql.ExecutorConfig, []backuppb.BackupManifest,
) ([][2]int, error)

// minSizeDeltaHeuristic is a heuristic that selects a set of backup windows
// with the smallest delta in data size between each backup.
func minSizeDeltaHeuristic(
	_ context.Context, execCfg *sql.ExecutorConfig, backupChain []backuppb.BackupManifest,
) ([][2]int, error) {
	windowSize, concurrency, err := compactionWindowAndConcurrency(execCfg, int64(len(backupChain)))
	if err != nil {
		return nil, err
	}
	// We only care about the data size deltas between incremental backups.
	dataDeltas := make([]int64, len(backupChain)-2)
	for i := range len(backupChain) {
		if i <= 1 {
			continue
		}
		dataDeltas[i-2] = int64(math.Abs(float64(
			backupChain[i].EntryCounts.DataSize - backupChain[i-1].EntryCounts.DataSize,
		)))
	}
	windows := make([][2]int, 0, concurrency)
	// We iteratively find the min delta window, invalidate them, and repeat until
	// we have up to concurrency *non-overlapping* windows.
	// NB: There is a fun leetcode-style dynamic programming solution to find the
	// optimal minimum total sum of non-overlapping windows instead of this greedy
	// approach. We leave this as an exercise to an eager reader.
	for i := int64(0); i < concurrency; i++ {
		start, end, ok := minSumWindow(dataDeltas, int(windowSize-1))
		if !ok {
			break
		}
		// Because dataDeltas represents the deltas between incremental backups but
		// the windows are based on the full backups, so we need to correct the
		windows = append(windows, [2]int{start + 1, end + 2})
		// Invalidate the selected window so it is not selected again.
		for j := start; j < min(len(dataDeltas), end+1); j++ {
			dataDeltas[j] = math.MaxInt64
		}
	}
	return windows, nil
}

// minSumWindow finds the start and end index of a contiguous window that has
// the minimum total sum of values in the window. If no valid window exists, ok
// is false. Invalid values that cannot be included in a window are represented
// with math.MaxInt64.
func minSumWindow(nums []int64, windowSize int) (start, end int, ok bool) {
	minIdx := -1
	minSum := int64(math.MaxInt64)
	currSum := minSum
	// idx represents the start of the current widnow, such that at idx = i, we
	// are looking at the window [i, i + windowSize).
	idx := 0
	for idx <= len(nums)-windowSize {
		if currSum == math.MaxInt64 {
			// This is either the first window, or we've jumped forward to skip
			// invalid values. Must manually recompute the sum of the window.
			// Computing the sum be iterating backwards allows us to early exit and
			// jump forward if we encounter an invalid value.
			currSum = 0
			for endIdx := idx + windowSize - 1; endIdx >= idx; endIdx-- {
				if nums[endIdx] == math.MaxInt64 {
					// Encountered an invalid value, this window is not valid. We can jump
					// to the next window starting after the invalid value and skip
					// everything in between.
					currSum = math.MaxInt64
					idx = endIdx + 1
					break
				}
				currSum += nums[endIdx]
			}
			if currSum == math.MaxInt64 {
				continue
			}
		} else {
			// The previous window had no invalid values, we can adjust the sum with a
			// sliding window here.
			removedVal := nums[idx-1]
			addedVal := nums[idx+windowSize-1]
			if addedVal == math.MaxInt64 {
				// New value is invalid, this window is not valid.
				currSum = math.MaxInt64
				idx++
				continue
			}
			currSum = currSum - removedVal + addedVal
		}

		if currSum < minSum {
			minSum = currSum
			minIdx = idx
		}
		idx++
	}

	if minIdx == -1 {
		return 0, 0, false
	}
	return minIdx, minIdx + windowSize, true
}

// compactionWindowAndConcurrency retrieves the compaction window size and
// concurrency for the given backup chain length.
func compactionWindowAndConcurrency(
	execCfg *sql.ExecutorConfig, chainLen int64,
) (int64, int64, error) {
	windowSize := backupCompactionWindow.Get(&execCfg.Settings.SV)
	// Compaction does not compact the full backup, so windowSize must be < len(backupChain).
	if windowSize >= chainLen {
		return 0, 0, errors.New("compaction window size must be less than backup chain length")
	}
	maxConcurrency := backupCompactionConcurrency.Get(&execCfg.Settings.SV)
	maxPossibleConcurrency := (chainLen - 1) / windowSize
	return windowSize, min(maxConcurrency, maxPossibleConcurrency), nil
}
