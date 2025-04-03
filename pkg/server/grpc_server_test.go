// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

func TestRequestMetricRegistered(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	ts := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer ts.Stopper().Stop(ctx)

	requestMetrics := rpc.NewRequestMetrics()

	var histogramVec *metric.HistogramVec
	registry := ts.MetricsRecorder().AppRegistry()
	registry.Select(
		map[string]struct{}{requestMetrics.Duration.Name: {}},
		func(name string, val interface{}) {
			histogramVec = val.(*metric.HistogramVec)
		})
	require.NotEmpty(t, histogramVec)

	_, _ = ts.GetAdminClient(t).Settings(ctx, &serverpb.SettingsRequest{})
	require.Len(t, histogramVec.ToPrometheusMetrics(), 0, "Should not have recorded any metrics yet")
	serverGRPCRequestMetricsEnabled.Override(context.Background(), &ts.ClusterSettings().SV, true)
	_, _ = ts.GetAdminClient(t).Settings(ctx, &serverpb.SettingsRequest{})
	require.Len(t, histogramVec.ToPrometheusMetrics(), 1, "Should have recorded metrics for request")
}

func TestShouldRecordRequestDuration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tests := []struct {
		methodName     string
		metricsEnabled bool
		expected       bool
	}{
		{fmt.Sprintf("%s/test/method", serverPrefix), true, true},
		{fmt.Sprintf("%s/test/method", tsdbPrefix), true, true},
		{fmt.Sprintf("%s/test/method", serverPrefix), false, false},
		{fmt.Sprintf("%s/test/method", tsdbPrefix), false, false},
		{"test/noPrefix/metricsEnabled", true, false},
		{"test/noPrefix/metricsDisabled", false, false},
	}

	settings := cluster.MakeTestingClusterSettings()
	for _, tt := range tests {
		t.Run(tt.methodName, func(t *testing.T) {
			serverGRPCRequestMetricsEnabled.Override(context.Background(), &settings.SV, tt.metricsEnabled)
			require.Equal(t, tt.expected, shouldRecordRequestDuration(settings, tt.methodName))
		})
	}
}
