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
			writeBytes:     counters.BytesWritten,
			writeCount:     counters.NumWrite,
			iopsInProgress: 0, // Not reported by this library. (#27927)
		}
	}

	return output, nil
}
