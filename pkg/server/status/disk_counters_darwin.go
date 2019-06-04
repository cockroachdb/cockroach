// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

// +build darwin

package status

import (
	"context"

	"github.com/lufia/iostat"
)

func getDiskCounters(context.Context) ([]diskStats, error) {
	driveStats, err := iostat.ReadDriveStats()
	if err != nil {
		return nil, err
	}

	output := make([]diskStats, len(driveStats))
	for i, counters := range driveStats {
		output[i] = diskStats{
			readBytes:      counters.BytesRead,
			readCount:      counters.NumRead,
			readTime:       counters.TotalReadTime,
			writeBytes:     counters.BytesWritten,
			writeCount:     counters.NumWrite,
			writeTime:      counters.TotalWriteTime,
			ioTime:         0, // Not reported by this library.
			weightedIOTime: 0, // Not reported by this library.
			iopsInProgress: 0, // Not reported by this library. (#27927)
		}
	}

	return output, nil
}
