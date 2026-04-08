// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
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

	requestMetrics := rpc.NewServerRequestMetrics()

	var histogramVec *metric.HistogramVec
	registry := ts.MetricsRecorder().AppRegistry()
	registry.Select(
		map[string]struct{}{requestMetrics.RequestDuration.Name: {}},
		func(name string, val interface{}) {
			histogramVec = val.(*metric.HistogramVec)
		})
	require.NotEmpty(t, histogramVec)

	_, _ = ts.GetAdminClient(t).Settings(ctx, &serverpb.SettingsRequest{})
	require.Len(t, histogramVec.ToPrometheusMetrics(), 0, "Should not have recorded any metrics yet")
	rpc.ServerRPCRequestMetricsEnabled.Override(context.Background(),
		&ts.ClusterSettings().SV, true)
	_, _ = ts.GetAdminClient(t).Settings(ctx, &serverpb.SettingsRequest{})
	require.Greater(t, len(histogramVec.ToPrometheusMetrics()), 0,
		"Should have recorded metrics for request")
}

func TestShouldRecordRequestDuration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tests := []struct {
		methodName     string
		metricsEnabled bool
		expected       bool
	}{
		{rpc.ServerPrefix + "/test/method", true, true},
		{rpc.TsdbPrefix + "/test/method", true, true},
		{rpc.ServerPrefix + "/test/method", false, false},
		{rpc.TsdbPrefix + "/test/method", false, false},
		{"test/noPrefix/metricsEnabled", true, false},
		{"test/noPrefix/metricsDisabled", false, false},
	}

	settings := cluster.MakeTestingClusterSettings()
	for _, tt := range tests {
		t.Run(tt.methodName, func(t *testing.T) {
			rpc.ServerRPCRequestMetricsEnabled.Override(context.Background(), &settings.SV, tt.metricsEnabled)
			require.Equal(t, tt.expected, rpc.ShouldRecordRequestDuration(settings, tt.methodName))
		})
	}
}
