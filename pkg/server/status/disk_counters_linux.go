// Copyright 2018 The Cockroach Authors.
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

func GetDiskCounters(diskMonitors map[string]disk.Monitor) (map[string]DiskStats, error) {
	output := make(map[string]DiskStats, len(diskMonitors))
	i := 0
	for path, monitor := range diskMonitors {
		stats, err := monitor.LatestStats()
		if err != nil {
			return map[string]DiskStats{}, err
		}
		output[path] = DiskStats{
			Name:           path,
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
		i++
	}
	return output, nil
}
