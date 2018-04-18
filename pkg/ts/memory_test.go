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

package ts

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestGetMaxTimespan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		r                   Resolution
		memoryBudget        int64
		estimatedSources    int64
		interpolationLimit  int64
		expectedTimespan    int64
		expectedErrorString string
	}{
		// Simplest case: One series, room for exactly one hour of query (need two
		// slabs of memory budget, as queried time span may stagger across two
		// slabs)
		{
			Resolution10s,
			2 * (sizeOfTimeSeriesData + sizeOfTimeSeriesData*360),
			1,
			0,
			(1 * time.Hour).Nanoseconds(),
			"",
		},
		// Not enough room for to make query.
		{
			Resolution10s,
			sizeOfTimeSeriesData + sizeOfTimeSeriesData*360,
			1,
			0,
			0,
			"insufficient",
		},
		// Not enough room because of multiple sources.
		{
			Resolution10s,
			2 * (sizeOfTimeSeriesData + sizeOfTimeSeriesData*360),
			2,
			0,
			0,
			"insufficient",
		},
		// 6 sources, room for 1 hour.
		{
			Resolution10s,
			12 * (sizeOfTimeSeriesData + sizeOfTimeSeriesData*360),
			6,
			0,
			(1 * time.Hour).Nanoseconds(),
			"",
		},
		// 6 sources, room for 2 hours.
		{
			Resolution10s,
			18 * (sizeOfTimeSeriesData + sizeOfTimeSeriesData*360),
			6,
			0,
			(2 * time.Hour).Nanoseconds(),
			"",
		},
		// Not enough room due to interpolation buffer.
		{
			Resolution10s,
			12 * (sizeOfTimeSeriesData + sizeOfTimeSeriesData*360),
			6,
			1,
			0,
			"insufficient",
		},
		// Sufficient room even with interpolation buffer.
		{
			Resolution10s,
			18 * (sizeOfTimeSeriesData + sizeOfTimeSeriesData*360),
			6,
			1,
			(1 * time.Hour).Nanoseconds(),
			"",
		},
		// Insufficient room for interpolation buffer (due to straddling)
		{
			Resolution10s,
			18 * (sizeOfTimeSeriesData + sizeOfTimeSeriesData*360),
			6,
			int64(float64(Resolution10s.SlabDuration()) * 0.75),
			0,
			"insufficient",
		},
		// Sufficient room even with interpolation buffer.
		{
			Resolution10s,
			24 * (sizeOfTimeSeriesData + sizeOfTimeSeriesData*360),
			6,
			int64(float64(Resolution10s.SlabDuration()) * 0.75),
			(1 * time.Hour).Nanoseconds(),
			"",
		},
		// 1ns test resolution.
		{
			resolution1ns,
			3 * (sizeOfTimeSeriesData + sizeOfTimeSeriesData*10),
			1,
			1,
			10,
			"",
		},
	} {
		t.Run("", func(t *testing.T) {
			mem := QueryMemoryContext{
				budget:                  tc.memoryBudget,
				estimatedSources:        tc.estimatedSources,
				interpolationLimitNanos: tc.interpolationLimit,
			}
			actual, err := mem.GetMaxTimespan(tc.r)
			if !testutils.IsError(err, tc.expectedErrorString) {
				t.Fatalf("got error %s, wanted error matching %s", err, tc.expectedErrorString)
			}
			if tc.expectedErrorString == "" {
				return
			}
			if a, e := actual, tc.expectedTimespan; a != e {
				t.Fatalf("got max timespan %d, wanted %d", a, e)
			}
		})
	}
}
