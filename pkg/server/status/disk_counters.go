// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !darwin

package status

import (
	"context"
	"time"

	"github.com/shirou/gopsutil/disk"
)

func getDiskCounters(ctx context.Context) ([]diskStats, error) {
	driveStats, err := disk.IOCountersWithContext(ctx)
	if err != nil {
		return nil, err
	}

	output := make([]diskStats, len(driveStats))
	i := 0
	for _, counters := range driveStats {
		output[i] = diskStats{
			readBytes:      int64(counters.ReadBytes),
			readCount:      int64(counters.ReadCount),
			readTime:       time.Duration(counters.ReadTime) * time.Millisecond,
			writeBytes:     int64(counters.WriteBytes),
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
