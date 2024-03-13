// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build linux
// +build linux

package status

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/storage/disk"
)

// GetMonitorCounters returns DiskStats for all monitored disks.
func GetMonitorCounters(monitors map[string]disk.Monitor) (map[string]DiskStats, error) {
	output := make(map[string]DiskStats, len(monitors))
	for path, monitor := range monitors {
		stats, err := monitor.CumulativeStats()
		if err != nil {
			return map[string]DiskStats{}, err
		}
		output[path] = DiskStats{
			Name:           stats.DeviceName,
			ReadBytes:      int64(stats.BytesRead()),
			readCount:      int64(stats.ReadsCount),
			readTime:       stats.ReadsDuration * time.Millisecond,
			WriteBytes:     int64(stats.BytesWritten()),
			writeCount:     int64(stats.WritesCount),
			writeTime:      stats.WritesDuration * time.Millisecond,
			ioTime:         stats.CumulativeDuration * time.Millisecond,
			weightedIOTime: stats.WeightedIODuration * time.Millisecond,
			iopsInProgress: int64(stats.InProgressCount),
		}
	}
	return output, nil
}
