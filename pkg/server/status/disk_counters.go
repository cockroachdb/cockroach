// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !darwin

package status

import (
	"context"
	"time"

	"github.com/shirou/gopsutil/v3/disk"
)

// GetDiskCounters returns DiskStats for all disks.
func GetDiskCounters(ctx context.Context) ([]DiskStats, error) {
	driveStats, err := disk.IOCountersWithContext(ctx)
	if err != nil {
		return nil, err
	}

	output := make([]DiskStats, len(driveStats))
	i := 0
	for _, counters := range driveStats {
		output[i] = DiskStats{
			Name:           counters.Name,
			ReadBytes:      int64(counters.ReadBytes),
			readCount:      int64(counters.ReadCount),
			readTime:       time.Duration(counters.ReadTime) * time.Millisecond,
			WriteBytes:     int64(counters.WriteBytes),
			writeCount:     int64(counters.WriteCount),
			writeTime:      time.Duration(counters.WriteTime) * time.Millisecond,
			ioTime:         time.Duration(counters.IoTime) * time.Millisecond,
			weightedIOTime: time.Duration(counters.WeightedIO) * time.Millisecond,
			iopsInProgress: int64(counters.IopsInProgress),
		}
		i++
	}

	return output, nil
}
