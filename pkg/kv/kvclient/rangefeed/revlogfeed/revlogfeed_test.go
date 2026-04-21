// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogfeed

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// ts is a wall-clock-only timestamp helper for tests.
func ts(wall int64) hlc.Timestamp { return hlc.Timestamp{WallTime: wall} }

// TestCheckCoverage verifies the two halves of the pre-flight check:
// contiguity (no holes between ticks, none at the front) and freshness
// (the freshest tick must be within FreshnessBudget of now). Each case
// sets FreshnessBudget explicitly so the test inputs (small wall-time
// integers) are comparable to the budget without any unit conversion
// surprises.
func TestCheckCoverage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	tests := []struct {
		name        string
		ticks       []revlog.Tick
		startFrom   hlc.Timestamp
		now         hlc.Timestamp
		budget      time.Duration
		expectedErr string // substring; empty means expect nil
	}{
		{
			name:      "contiguous chain reaches now",
			ticks:     []revlog.Tick{mkTick(100, 200), mkTick(200, 300), mkTick(300, 400)},
			startFrom: ts(150),
			now:       ts(400),
			budget:    100 * time.Nanosecond,
		},
		{
			name:        "no ticks at all",
			ticks:       nil,
			startFrom:   ts(100),
			now:         ts(200),
			budget:      100 * time.Nanosecond,
			expectedErr: "missing coverage",
		},
		{
			name:        "gap at start of window",
			ticks:       []revlog.Tick{mkTick(200, 300)},
			startFrom:   ts(100),
			now:         ts(300),
			budget:      100 * time.Nanosecond,
			expectedErr: "missing coverage",
		},
		{
			name:        "gap between adjacent ticks",
			ticks:       []revlog.Tick{mkTick(100, 200), mkTick(250, 300)},
			startFrom:   ts(150),
			now:         ts(300),
			budget:      100 * time.Nanosecond,
			expectedErr: "missing coverage",
		},
		{
			// Freshest tick at 350; now=400; residual 50ns ≤ budget 100ns.
			// The residual (350, 400] is the live KV rangefeed's job
			// after cutover.
			name:      "tick chain stops short but within freshness budget",
			ticks:     []revlog.Tick{mkTick(100, 200), mkTick(200, 350)},
			startFrom: ts(150),
			now:       ts(400),
			budget:    100 * time.Nanosecond,
		},
		{
			// Freshest tick at 200; now=10000; residual 9800ns >> budget 100ns.
			// Models a producer that stalled long ago.
			name:        "tick chain too stale for freshness budget",
			ticks:       []revlog.Tick{mkTick(100, 200)},
			startFrom:   ts(150),
			now:         ts(10000),
			budget:      100 * time.Nanosecond,
			expectedErr: "exceeds budget",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := New(
				&fakeTickSource{ticks: tc.ticks},
				nil, /* live */
				Options{FreshnessBudget: tc.budget},
			)
			err := d.checkCoverage(ctx, tc.startFrom, tc.now)
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.expectedErr)
			}
		})
	}
}
