// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/stretchr/testify/require"
)

type stubTest struct {
	test.Test
	name         string
	artifactsDir string
	l            *logger.Logger
}

func (s stubTest) Name() string            { return s.name }
func (s stubTest) ArtifactsDir() string    { return s.artifactsDir }
func (s stubTest) GetRunId() string        { return "test-run-id" }
func (s stubTest) Owner() string           { return "test-owner" }
func (s stubTest) L() *logger.Logger       { return s.l }
func (s stubTest) ExportOpenmetrics() bool { return false }

type stubCluster struct {
	cluster.Cluster
	cloud spec.Cloud
}

func (s stubCluster) Cloud() spec.Cloud { return s.cloud }

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

func TestWritePerfSummaryStats(t *testing.T) {
	artifactsDir := t.TempDir()
	l, err := logger.RootLogger(filepath.Join(artifactsDir, "test.log"), false /* tee */)
	require.NoError(t, err)

	st := stubTest{
		name:         "test/perf-summary",
		artifactsDir: artifactsDir,
		l:            l,
	}
	sc := stubCluster{cloud: spec.GCE}

	stats := AggregatedPerfMetrics{
		{Name: "restore_runtime", Value: 5.5, Unit: "minutes", IsHigherBetter: false},
	}

	require.NoError(t, WritePerfSummaryStats(st, sc, stats))

	summaryDir := filepath.Join(artifactsDir, "summary.perf")

	// Verify the JSON file.
	jsonBytes, err := os.ReadFile(filepath.Join(summaryDir, "summary_stats.json"))
	require.NoError(t, err)
	var jsonStats []summaryStatJSON
	require.NoError(t, json.Unmarshal(jsonBytes, &jsonStats))
	require.Len(t, jsonStats, 1)
	require.Equal(t, "restore_runtime", jsonStats[0].Name)
	require.Equal(t, 5.5, jsonStats[0].Value)
	require.Equal(t, "minutes", jsonStats[0].Unit)

	// Verify the OpenMetrics file.
	omBytes, err := os.ReadFile(filepath.Join(summaryDir, "summary_stats.om"))
	require.NoError(t, err)
	omContent := string(omBytes)
	require.Contains(t, omContent, "restore_runtime")
	require.Contains(t, omContent, "# EOF")
	require.True(t, strings.Contains(omContent, `owner="test-owner"`))
}

func TestWritePerfSummaryStatsEmpty(t *testing.T) {
	st := stubTest{artifactsDir: t.TempDir()}
	sc := stubCluster{cloud: spec.GCE}

	err := WritePerfSummaryStats(st, sc, AggregatedPerfMetrics{})
	require.ErrorContains(t, err, "no summary stats provided")
}
