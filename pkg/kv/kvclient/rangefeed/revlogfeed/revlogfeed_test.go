// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogfeed

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// ts is a wall-clock-only timestamp helper for tests.
func ts(wall int64) hlc.Timestamp { return hlc.Timestamp{WallTime: wall} }

// TestCheckCoverage verifies that checkCoverage returns nil exactly
// when the tick source provides a contiguous chain of ticks covering
// (startFrom, end], and otherwise returns an error mentioning the
// missing window.
func TestCheckCoverage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	tests := []struct {
		name         string
		ticks        []revlog.Tick
		startFrom    hlc.Timestamp
		end          hlc.Timestamp
		expectedErr  string // substring; empty means expect nil
	}{
		{
			name:      "contiguous chain covers the window",
			ticks:     []revlog.Tick{mkTick(100, 200), mkTick(200, 300), mkTick(300, 400)},
			startFrom: ts(150),
			end:       ts(400),
		},
		{
			name:        "no ticks at all",
			ticks:       nil,
			startFrom:   ts(100),
			end:         ts(200),
			expectedErr: "missing coverage",
		},
		{
			name:        "gap at start of window",
			ticks:       []revlog.Tick{mkTick(200, 300)},
			startFrom:   ts(100),
			end:         ts(300),
			expectedErr: "missing coverage",
		},
		{
			name:        "gap between adjacent ticks",
			ticks:       []revlog.Tick{mkTick(100, 200), mkTick(250, 300)},
			startFrom:   ts(150),
			end:         ts(300),
			expectedErr: "missing coverage",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := New(&fakeTickSource{ticks: tc.ticks}, nil /* live */, Options{})
			err := d.checkCoverage(ctx, tc.startFrom, tc.end)
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.expectedErr)
			}
		})
	}
}
