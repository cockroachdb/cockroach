// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSummaryStatsToJSON(t *testing.T) {
	tests := []struct {
		name     string
		stats    AggregatedPerfMetrics
		expected []summaryStatJSON
	}{
		{
			name: "single stat",
			stats: AggregatedPerfMetrics{
				{Name: "throughput", Value: 1234, Unit: "ops/sec"},
			},
			expected: []summaryStatJSON{
				{Name: "throughput", Value: 1234, Unit: "ops/sec"},
			},
		},
		{
			name: "multiple stats",
			stats: AggregatedPerfMetrics{
				{Name: "copy_row_rate", Value: 5000, Unit: "rows/sec"},
				{Name: "p99_latency", Value: 12.5, Unit: "ms"},
			},
			expected: []summaryStatJSON{
				{Name: "copy_row_rate", Value: 5000, Unit: "rows/sec"},
				{Name: "p99_latency", Value: 12.5, Unit: "ms"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf, err := summaryStatsToJSON(tc.stats)
			require.NoError(t, err)

			var got []summaryStatJSON
			require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
			require.Equal(t, tc.expected, got)
		})
	}
}

func TestSummaryStatsToJSONEmpty(t *testing.T) {
	buf, err := summaryStatsToJSON(AggregatedPerfMetrics{})
	require.NoError(t, err)

	var got []summaryStatJSON
	require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
	require.Empty(t, got)
}
