// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ts

import (
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestGetMaxTimespan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		r                   Resolution
		opts                QueryMemoryOptions
		expectedTimespan    int64
		expectedErrorString string
	}{
		// Simplest case: One series, room for exactly one hour of query (need two
		// slabs of memory budget, as queried time span may stagger across two
		// slabs)
		{
			Resolution10s,
			QueryMemoryOptions{
				BudgetBytes:             2 * (sizeOfTimeSeriesData + sizeOfSample*360),
				EstimatedSources:        1,
				InterpolationLimitNanos: 0,
			},
			(1 * time.Hour).Nanoseconds(),
			"",
		},
		// Not enough room for to make query.
		{
			Resolution10s,
			QueryMemoryOptions{
				BudgetBytes:             sizeOfTimeSeriesData + sizeOfSample*360,
				EstimatedSources:        1,
				InterpolationLimitNanos: 0,
			},
			0,
			"insufficient",
		},
		// Not enough room because of multiple sources.
		{
			Resolution10s,
			QueryMemoryOptions{
				BudgetBytes:             2 * (sizeOfTimeSeriesData + sizeOfSample*360),
				EstimatedSources:        2,
				InterpolationLimitNanos: 0,
			},
			0,
			"insufficient",
		},
		// 6 sources, room for 1 hour.
		{
			Resolution10s,
			QueryMemoryOptions{
				BudgetBytes:             12 * (sizeOfTimeSeriesData + sizeOfSample*360),
				EstimatedSources:        6,
				InterpolationLimitNanos: 0,
			},
			(1 * time.Hour).Nanoseconds(),
			"",
		},
		// 6 sources, room for 2 hours.
		{
			Resolution10s,
			QueryMemoryOptions{
				BudgetBytes:             18 * (sizeOfTimeSeriesData + sizeOfSample*360),
				EstimatedSources:        6,
				InterpolationLimitNanos: 0,
			},
			(2 * time.Hour).Nanoseconds(),
			"",
		},
		// Not enough room due to interpolation buffer.
		{
			Resolution10s,
			QueryMemoryOptions{
				BudgetBytes:             12 * (sizeOfTimeSeriesData + sizeOfSample*360),
				EstimatedSources:        6,
				InterpolationLimitNanos: 1,
			},
			0,
			"insufficient",
		},
		// Sufficient room even with interpolation buffer.
		{
			Resolution10s,
			QueryMemoryOptions{
				BudgetBytes:             18 * (sizeOfTimeSeriesData + sizeOfSample*360),
				EstimatedSources:        6,
				InterpolationLimitNanos: 1,
			},
			(1 * time.Hour).Nanoseconds(),
			"",
		},
		// Insufficient room for interpolation buffer (due to straddling)
		{
			Resolution10s,
			QueryMemoryOptions{
				BudgetBytes:             18 * (sizeOfTimeSeriesData + sizeOfSample*360),
				EstimatedSources:        6,
				InterpolationLimitNanos: int64(float64(Resolution10s.SlabDuration()) * 0.75),
			},
			0,
			"insufficient",
		},
		// Sufficient room even with interpolation buffer.
		{
			Resolution10s,
			QueryMemoryOptions{
				BudgetBytes:             24 * (sizeOfTimeSeriesData + sizeOfSample*360),
				EstimatedSources:        6,
				InterpolationLimitNanos: int64(float64(Resolution10s.SlabDuration()) * 0.75),
			},
			(1 * time.Hour).Nanoseconds(),
			"",
		},
		// 1ns test resolution.
		{
			resolution1ns,
			QueryMemoryOptions{
				BudgetBytes:             3 * (sizeOfTimeSeriesData + sizeOfSample*10),
				EstimatedSources:        1,
				InterpolationLimitNanos: 1,
			},
			10,
			"",
		},
		// Overflow.
		{
			Resolution10s,
			QueryMemoryOptions{
				BudgetBytes:             math.MaxInt64,
				EstimatedSources:        1,
				InterpolationLimitNanos: math.MaxInt64,
			},
			math.MaxInt64,
			"",
		},
	} {
		t.Run("", func(t *testing.T) {
			mem := QueryMemoryContext{
				QueryMemoryOptions: tc.opts,
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
