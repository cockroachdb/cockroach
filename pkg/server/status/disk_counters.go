// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// +build !darwin

package status

import (
	"context"

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
			writeBytes:     int64(counters.WriteBytes),
			writeCount:     int64(counters.WriteCount),
			iopsInProgress: int64(counters.IopsInProgress),
		}
		i++
	}

	return output, nil
}
