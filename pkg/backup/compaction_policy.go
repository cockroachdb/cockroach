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
	"github.com/cockroachdb/cockroach/pkg/util"
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
)

// compactionPolicy is a function that determines what backups to compact when
// given a chain of backups. It returns the inclusive start and exclusive end of
// the window of backups to compact, as well as an error if one occurs.
type compactionPolicy func(context.Context, *sql.ExecutorConfig, []backuppb.BackupManifest) (int, int, error)

// minSizeDeltaHeuristic is a heuristic that selects a window of backups with the
// smallest delta in data size between each backup.
func minSizeDeltaHeuristic(
	_ context.Context, execCfg *sql.ExecutorConfig, backupChain []backuppb.BackupManifest,
) (int, int, error) {
	windowSize := int(backupCompactionWindow.Get(&execCfg.Settings.SV))
	// Compaction does not compact the full backup, so windowSize must be < len(backupChain).
	if windowSize >= len(backupChain) {
		return 0, 0, errors.New("window size must be less than backup chain length")
	}
	// We do not compact the full backup, so exclude it from the data sizes.
	dataSizes := make([]int64, len(backupChain)-1)
	for i := 1; i < len(backupChain); i++ {
		dataSizes[i-1] = backupChain[i].EntryCounts.DataSize
	}
	start, end := minDeltaWindow(dataSizes, windowSize)
	// We excluded the full backup, so adjust the indices by 1.
	return start + 1, end + 1, nil
}

// minDeltaWindow finds the start and end index of a window that has the minimum
// total delta between each value in the window.
func minDeltaWindow(nums []int64, windowSize int) (int, int) {
	currDiff := util.Reduce(
		nums,
		func(diff int64, n int64, idx int) int64 {
			if idx == 0 {
				return 0
			}
			return diff + n - nums[idx-1]
		},
		0,
	)
	minDiff := currDiff
	var minIdx int
	// Move sliding window and adjust total diff size as we go.
	for i := 1; i <= len(nums)-windowSize; i++ {
		removedDiff := int64(math.Abs(float64(nums[i] - nums[i-1])))
		addedDiff := int64(math.Abs(float64(nums[i+windowSize-1] - nums[i+windowSize-2])))
		currDiff = currDiff - removedDiff + addedDiff
		if currDiff < minDiff {
			minDiff = currDiff
			minIdx = i
		}
	}
	return minIdx, minIdx + windowSize
}
