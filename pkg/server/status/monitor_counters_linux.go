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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/disk"
)

// GetMonitorCounters returns DiskStats for all monitored disks.
func GetMonitorCounters(
	monitors map[roachpb.StoreID]disk.Monitor,
) (map[roachpb.StoreID]DiskStats, error) {
	output := make(map[roachpb.StoreID]DiskStats, len(monitors))
	for id, monitor := range monitors {
		stats, err := monitor.CumulativeStats()
		if err != nil {
			return map[string]DiskStats{}, err
		}
		output[id] = DiskStats{
			Name:           stats.DeviceName,
			ReadBytes:      int64(stats.BytesRead()),
			readCount:      int64(stats.ReadsCount),
			readTime:       stats.ReadsDuration,
			WriteBytes:     int64(stats.BytesWritten()),
			writeCount:     int64(stats.WritesCount),
			writeTime:      stats.WritesDuration,
			ioTime:         stats.CumulativeDuration,
			weightedIOTime: stats.WeightedIODuration,
			iopsInProgress: int64(stats.InProgressCount),
		}
	}
	return output, nil
}
