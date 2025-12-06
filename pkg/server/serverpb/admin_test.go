// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverpb

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	prometheusgo "github.com/prometheus/client_model/go"
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

// mockAdminClient mocks RPCAdminClient by embedding the interface and
// implementing AllMetricMetadata.
type mockAdminClient struct {
	RPCAdminClient
	metadata map[string]metric.Metadata
}

// AllMetricMetadata returns the mocked metadata.
func (m *mockAdminClient) AllMetricMetadata(
	ctx context.Context, req *MetricMetadataRequest,
) (*MetricMetadataResponse, error) {
	return &MetricMetadataResponse{
		Metadata: m.metadata,
	}, nil
}

// TestGetInternalTimeseriesNamesFromServer_NonVerboseFiltering tests that
// the nonVerbose parameter correctly filters metrics by visibility.
func TestGetInternalTimeseriesNamesFromServer_NonVerboseFiltering(t *testing.T) {
	ctx := context.Background()

	mockMetadata := map[string]metric.Metadata{
		"sys.cpu.user.percent": {
			Name:       "sys.cpu.user.percent",
			Visibility: metric.Metadata_ESSENTIAL,
			MetricType: prometheusgo.MetricType_GAUGE,
		},
		"sql.txn.commit.count": {
			Name:       "sql.txn.commit.count",
			Visibility: metric.Metadata_ESSENTIAL,
			MetricType: prometheusgo.MetricType_COUNTER,
		},
		"queue.gc.pending": {
			Name:       "queue.gc.pending",
			Visibility: metric.Metadata_SUPPORT,
			MetricType: prometheusgo.MetricType_GAUGE,
		},
		"storage.disk-slow": {
			Name:       "storage.disk-slow",
			Visibility: metric.Metadata_SUPPORT,
			MetricType: prometheusgo.MetricType_COUNTER,
		},
		"internal.debug.metric1": {
			Name:       "internal.debug.metric1",
			Visibility: metric.Metadata_INTERNAL,
			MetricType: prometheusgo.MetricType_GAUGE,
		},
		"internal.debug.metric2": {
			Name:       "internal.debug.metric2",
			Visibility: metric.Metadata_INTERNAL,
			MetricType: prometheusgo.MetricType_COUNTER,
		},
	}

	testCases := []struct {
		name       string
		nonVerbose bool
		expected   []string
	}{
		{
			name:       "verbose mode includes all metrics",
			nonVerbose: false,
			expected: []string{
				"cr.node.sys.cpu.user.percent", "cr.store.sys.cpu.user.percent",
				"cr.node.sql.txn.commit.count", "cr.store.sql.txn.commit.count",
				"cr.node.queue.gc.pending", "cr.store.queue.gc.pending",
				"cr.node.storage.disk-slow", "cr.store.storage.disk-slow",
				"cr.node.internal.debug.metric1", "cr.store.internal.debug.metric1",
				"cr.node.internal.debug.metric2", "cr.store.internal.debug.metric2",
			},
		},
		{
			name:       "non-verbose mode excludes internal metrics",
			nonVerbose: true,
			expected: []string{
				"cr.node.sys.cpu.user.percent", "cr.store.sys.cpu.user.percent",
				"cr.node.sql.txn.commit.count", "cr.store.sql.txn.commit.count",
				"cr.node.queue.gc.pending", "cr.store.queue.gc.pending",
				"cr.node.storage.disk-slow", "cr.store.storage.disk-slow",
			},
		},
	}

	mockClient := &mockAdminClient{metadata: mockMetadata}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := GetInternalTimeseriesNamesFromServer(ctx, mockClient, tc.nonVerbose)
			require.NoError(t, err)
			require.ElementsMatch(t, tc.expected, actual)
		})
	}
}

// TestGetInternalTimeseriesNamesFromServer_OutputValidation verifies empty metadata
// handling, complete filtering when all metrics are INTERNAL, and alphabetical sorting.
func TestGetInternalTimeseriesNamesFromServer_OutputValidation(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name       string
		metadata   map[string]metric.Metadata
		nonVerbose bool
		validate   func(t *testing.T, names []string, err error)
	}{
		{
			name:       "empty metadata returns empty list",
			metadata:   map[string]metric.Metadata{},
			nonVerbose: false,
			validate: func(t *testing.T, names []string, err error) {
				require.NoError(t, err)
				require.Empty(t, names)
			},
		},
		{
			name: "all internal metrics filtered in non-verbose",
			metadata: map[string]metric.Metadata{
				"internal1": {
					Name:       "internal1",
					Help:       "Internal metric 1",
					Visibility: metric.Metadata_INTERNAL,
					MetricType: prometheusgo.MetricType_GAUGE,
				},
				"internal2": {
					Name:       "internal2",
					Help:       "Internal metric 2",
					Visibility: metric.Metadata_INTERNAL,
					MetricType: prometheusgo.MetricType_COUNTER,
				},
			},
			nonVerbose: true,
			validate: func(t *testing.T, names []string, err error) {
				require.NoError(t, err)
				require.Empty(t, names, "all internal metrics should be filtered out")
			},
		},
		{
			name: "metrics are sorted alphabetically",
			metadata: map[string]metric.Metadata{
				"zebra": {
					Name:       "zebra",
					Help:       "Z metric",
					Visibility: metric.Metadata_ESSENTIAL,
					MetricType: prometheusgo.MetricType_GAUGE,
				},
				"alpha": {
					Name:       "alpha",
					Help:       "A metric",
					Visibility: metric.Metadata_ESSENTIAL,
					MetricType: prometheusgo.MetricType_GAUGE,
				},
				"beta": {
					Name:       "beta",
					Help:       "B metric",
					Visibility: metric.Metadata_ESSENTIAL,
					MetricType: prometheusgo.MetricType_GAUGE,
				},
			},
			nonVerbose: false,
			validate: func(t *testing.T, names []string, err error) {
				require.NoError(t, err)
				for i := 1; i < len(names); i++ {
					require.Less(t, names[i-1], names[i], "names should be sorted alphabetically")
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := &mockAdminClient{metadata: tc.metadata}
			names, err := GetInternalTimeseriesNamesFromServer(ctx, mockClient, tc.nonVerbose)
			tc.validate(t, names, err)
		})
	}
}
