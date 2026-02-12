// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverpb

import (
	"context"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTableStatsResponseAdd verifies that TableStatsResponse.Add()
// correctly represents the result of combining stats from two spans.
// Specifically, most TableStatsResponse's stats are a straight-forward sum,
// but NodeCount should decrement as more missing nodes are added.
func TestTableStatsResponseAdd(t *testing.T) {

	// Initial object: no missing nodes.
	underTest := TableStatsResponse{
		RangeCount:           4,
		ReplicaCount:         4,
		ApproximateDiskBytes: 1000,
		NodeCount:            8,
	}

	// Add stats: no missing nodes, so NodeCount should stay the same.
	underTest.Add(&TableStatsResponse{
		RangeCount:           1,
		ReplicaCount:         2,
		ApproximateDiskBytes: 2345,
		NodeCount:            8,
	})
	assert.Equal(t, int64(5), underTest.RangeCount)
	assert.Equal(t, int64(6), underTest.ReplicaCount)
	assert.Equal(t, uint64(3345), underTest.ApproximateDiskBytes)
	assert.Equal(t, int64(8), underTest.NodeCount)

	// Add more stats: this time "node1" is missing. NodeCount should decrement.
	underTest.Add(&TableStatsResponse{
		RangeCount:           0,
		ReplicaCount:         0,
		ApproximateDiskBytes: 0,
		NodeCount:            7,
		MissingNodes: []TableStatsResponse_MissingNode{
			{
				NodeID:       "node1",
				ErrorMessage: "error msg",
			},
		},
	})
	assert.Equal(t, int64(5), underTest.RangeCount)
	assert.Equal(t, int64(6), underTest.ReplicaCount)
	assert.Equal(t, uint64(3345), underTest.ApproximateDiskBytes)
	assert.Equal(t, int64(7), underTest.NodeCount)
	assert.Equal(t, []TableStatsResponse_MissingNode{
		{
			NodeID:       "node1",
			ErrorMessage: "error msg",
		},
	}, underTest.MissingNodes)

	// Add more stats: "node1" is missing again. NodeCount shouldn't decrement.
	underTest.Add(&TableStatsResponse{
		RangeCount:           0,
		ReplicaCount:         0,
		ApproximateDiskBytes: 0,
		NodeCount:            7,
		MissingNodes: []TableStatsResponse_MissingNode{
			{
				NodeID:       "node1",
				ErrorMessage: "different error msg",
			},
		},
	})
	assert.Equal(t, int64(5), underTest.RangeCount)
	assert.Equal(t, int64(6), underTest.ReplicaCount)
	assert.Equal(t, uint64(3345), underTest.ApproximateDiskBytes)
	assert.Equal(t, int64(7), underTest.NodeCount)

	// Add more stats: new node is missing ("node2"). NodeCount should decrement.
	underTest.Add(&TableStatsResponse{
		RangeCount:           0,
		ReplicaCount:         0,
		ApproximateDiskBytes: 0,
		NodeCount:            7,
		MissingNodes: []TableStatsResponse_MissingNode{
			{
				NodeID:       "node2",
				ErrorMessage: "totally new error msg",
			},
		},
	})
	assert.Equal(t, int64(5), underTest.RangeCount)
	assert.Equal(t, int64(6), underTest.ReplicaCount)
	assert.Equal(t, uint64(3345), underTest.ApproximateDiskBytes)
	assert.Equal(t, int64(6), underTest.NodeCount)
	assert.Equal(t, []TableStatsResponse_MissingNode{
		{
			NodeID:       "node1",
			ErrorMessage: "error msg",
		},
		{
			NodeID:       "node2",
			ErrorMessage: "totally new error msg",
		},
	}, underTest.MissingNodes)

}

// mockAdminClient is a mock implementation of RPCAdminClient
// that only implements AllMetricMetadata for testing purposes.
type mockAdminClient struct {
	RPCAdminClient // Embed the interface
	metadata       map[string]metric.Metadata
}

// AllMetricMetadata returns the mocked metadata.
func (m *mockAdminClient) AllMetricMetadata(
	ctx context.Context, req *MetricMetadataRequest,
) (*MetricMetadataResponse, error) {
	return &MetricMetadataResponse{Metadata: m.metadata}, nil
}

// TestApplyMetricsFilter tests the applyMetricsFilter function which handles
// filtering metrics by literal names and regex patterns.
func TestApplyMetricsFilter(t *testing.T) {
	counterType := io_prometheus_client.MetricType_COUNTER

	metadata := map[string]metric.Metadata{
		"sql.query.count":      {MetricType: counterType},
		"sql.exec.count":       {MetricType: counterType},
		"kv.range.count":       {MetricType: counterType},
		"changefeed.running":   {MetricType: counterType},
		"distsender.rpc.count": {MetricType: counterType},
	}

	testCases := []struct {
		name                string
		filter              []MetricsFilterEntry
		expectedNames       []string
		expectedMatchCounts map[string]int
		expectedUnmatched   []string
		expectError         bool
		errorContains       string
	}{
		{
			name: "literal entries",
			filter: []MetricsFilterEntry{
				{Value: "sql.query.count", IsRegex: false},
				{Value: "kv.range.count", IsRegex: false},
			},
			expectedNames:       []string{"kv.range.count", "sql.query.count"},
			expectedMatchCounts: map[string]int{},
		},
		{
			name: "literal entry not found returns unmatched",
			filter: []MetricsFilterEntry{
				{Value: "sql.query.count", IsRegex: false},
				{Value: "nonexistent.metric", IsRegex: false},
			},
			expectedNames:       []string{"sql.query.count"},
			expectedMatchCounts: map[string]int{},
			expectedUnmatched:   []string{"nonexistent.metric"},
		},
		{
			name: "regex entry matches multiple",
			filter: []MetricsFilterEntry{
				{Value: `sql\..*`, IsRegex: true},
			},
			expectedNames:       []string{"sql.exec.count", "sql.query.count"},
			expectedMatchCounts: map[string]int{`sql\..*`: 2},
		},
		{
			name: "regex matches anywhere in string",
			filter: []MetricsFilterEntry{
				{Value: `count`, IsRegex: true},
			},
			expectedNames:       []string{"distsender.rpc.count", "kv.range.count", "sql.exec.count", "sql.query.count"},
			expectedMatchCounts: map[string]int{`count`: 4},
		},
		{
			name: "regex with no matches",
			filter: []MetricsFilterEntry{
				{Value: `nonexistent\..*`, IsRegex: true},
			},
			expectedNames:       []string{},
			expectedMatchCounts: map[string]int{`nonexistent\..*`: 0},
		},
		{
			name: "mixed literal and regex",
			filter: []MetricsFilterEntry{
				{Value: "changefeed.running", IsRegex: false},
				{Value: `sql\..*`, IsRegex: true},
			},
			expectedNames:       []string{"changefeed.running", "sql.exec.count", "sql.query.count"},
			expectedMatchCounts: map[string]int{`sql\..*`: 2},
		},
		{
			name: "invalid regex returns error",
			filter: []MetricsFilterEntry{
				{Value: `[invalid`, IsRegex: true},
			},
			expectError:   true,
			errorContains: "invalid regex pattern",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, stats, err := applyMetricsFilter(metadata, tc.filter)

			if tc.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errorContains)
				return
			}

			require.NoError(t, err)
			sort.Strings(result)
			require.Equal(t, tc.expectedNames, result)

			for pattern, expectedCount := range tc.expectedMatchCounts {
				require.Equal(t, expectedCount, stats.RegexMatchCounts[pattern], "match count for pattern %s", pattern)
			}

			if tc.expectedUnmatched != nil {
				require.Equal(t, tc.expectedUnmatched, stats.UnmatchedLiterals)
			} else {
				require.Empty(t, stats.UnmatchedLiterals)
			}
		})
	}
}

// TestGetInternalTimeseriesNamesFromServer tests the full pipeline:
// filtering -> histogram expansion -> prefix addition -> sorting.
func TestGetInternalTimeseriesNamesFromServer(t *testing.T) {
	counterType := io_prometheus_client.MetricType_COUNTER
	gaugeType := io_prometheus_client.MetricType_GAUGE
	histogramType := io_prometheus_client.MetricType_HISTOGRAM

	mockMetadata := map[string]metric.Metadata{
		"sql.query.count":      {MetricType: counterType},
		"changefeed.running":   {MetricType: gaugeType},
		"request.latency":      {MetricType: histogramType},
		"distsender.rpc.count": {MetricType: counterType},
	}

	mockClient := &mockAdminClient{metadata: mockMetadata}
	ctx := context.Background()

	// Helper to build expected output with prefixes
	buildExpected := func(baseNames ...string) []string {
		var result []string
		for _, prefix := range []string{"cr.node.", "cr.store."} {
			for _, name := range baseNames {
				result = append(result, prefix+name)
			}
		}
		sort.Strings(result)
		return result
	}

	// Helper to build expected histogram output
	buildHistogramExpected := func(baseName string) []string {
		var result []string
		for _, prefix := range []string{"cr.node.", "cr.store."} {
			for _, q := range metric.HistogramMetricComputers {
				result = append(result, prefix+baseName+q.Suffix)
			}
		}
		sort.Strings(result)
		return result
	}

	// Build expected output for "all metrics" (nil/empty filter)
	var allMetricsExpanded []string
	for _, prefix := range []string{"cr.node.", "cr.store."} {
		allMetricsExpanded = append(allMetricsExpanded, prefix+"sql.query.count")
		allMetricsExpanded = append(allMetricsExpanded, prefix+"changefeed.running")
		allMetricsExpanded = append(allMetricsExpanded, prefix+"distsender.rpc.count")
		for _, q := range metric.HistogramMetricComputers {
			allMetricsExpanded = append(allMetricsExpanded, prefix+"request.latency"+q.Suffix)
		}
	}
	sort.Strings(allMetricsExpanded)

	testCases := []struct {
		name                string
		filter              []MetricsFilterEntry
		expectedNames       []string
		expectedMatchCounts map[string]int
		expectedUnmatched   []string
	}{
		{
			name:          "nil filter returns all metrics",
			filter:        nil,
			expectedNames: allMetricsExpanded,
		},
		{
			name:          "empty filter returns all metrics",
			filter:        []MetricsFilterEntry{},
			expectedNames: allMetricsExpanded,
		},
		{
			name: "histogram expansion with all quantile suffixes",
			filter: []MetricsFilterEntry{
				{Value: "request.latency", IsRegex: false},
			},
			expectedNames: buildHistogramExpected("request.latency"),
		},
		{
			name: "multiple non-histogram metrics gets both prefixes without expansion",
			filter: []MetricsFilterEntry{
				{Value: "sql.query.count", IsRegex: false},
				{Value: "changefeed.running", IsRegex: false},
			},
			expectedNames: buildExpected("changefeed.running", "sql.query.count"),
		},
		{
			name: "filter with regex returns match counts",
			filter: []MetricsFilterEntry{
				{Value: `.*count`, IsRegex: true},
			},
			expectedNames:       buildExpected("distsender.rpc.count", "sql.query.count"),
			expectedMatchCounts: map[string]int{`.*count`: 2},
		},
		{
			name: "unmatched literal returns in unmatchedLiterals",
			filter: []MetricsFilterEntry{
				{Value: "sql.query.count", IsRegex: false},
				{Value: "nonexistent.metric", IsRegex: false},
			},
			expectedNames:     buildExpected("sql.query.count"),
			expectedUnmatched: []string{"nonexistent.metric"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			names, stats, err := GetInternalTimeseriesNamesFromServer(ctx, mockClient, tc.filter)
			require.NoError(t, err)

			require.Equal(t, tc.expectedNames, names)

			if tc.expectedMatchCounts != nil {
				for pattern, expectedCount := range tc.expectedMatchCounts {
					require.Equal(t, expectedCount, stats.RegexMatchCounts[pattern], "match count for pattern %s", pattern)
				}
			}

			if tc.expectedUnmatched != nil {
				require.Equal(t, tc.expectedUnmatched, stats.UnmatchedLiterals)
			} else {
				require.Empty(t, stats.UnmatchedLiterals)
			}
		})
	}
}
