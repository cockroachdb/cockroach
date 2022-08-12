// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build darwin
// +build darwin

package status

import (
	"context"

	"github.com/lufia/iostat"
)

// GetDiskCounters returns DiskStats for all disks.
func GetDiskCounters(context.Context) ([]DiskStats, error) {
	driveStats, err := iostat.ReadDriveStats()
	if err != nil {
		return nil, err
	}

	output := make([]DiskStats, len(driveStats))
	for i, counters := range driveStats {
		output[i] = DiskStats{
			Name:           counters.Name,
			ReadBytes:      counters.BytesRead,
			readCount:      counters.NumRead,
			readTime:       counters.TotalReadTime,
			WriteBytes:     counters.BytesWritten,
			writeCount:     counters.NumWrite,
			writeTime:      counters.TotalWriteTime,
			ioTime:         0, // Not reported by this library.
			weightedIOTime: 0, // Not reported by this library.
			iopsInProgress: 0, // Not reported by this library. (#27927)
		}
	}

	return output, nil
}
