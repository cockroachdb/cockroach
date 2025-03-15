// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGrpcServerMetricsInterceptorRecordsMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	reg := metric.NewRegistry()
	settings := cluster.MakeTestingClusterSettings()
	metrics := NewGrpcServerMetrics(reg, settings)
	interceptor := metrics.NewGrpcServerInterceptor()

	ctx := context.Background()
	req := struct{}{}

	testcase := []struct {
		methodName      string
		statusCode      codes.Code
		metricEnabled   bool
		resultsExpected bool
	}{
		{fmt.Sprintf("%s/test/method", serverPrefix), codes.OK, true, true},
		{fmt.Sprintf("%s/test/method", tsdbPrefix), codes.OK, true, true},
		{fmt.Sprintf("%s/test/method", serverPrefix), codes.Internal, true, true},
		{fmt.Sprintf("%s/test/method", serverPrefix), codes.Aborted, true, true},
		{fmt.Sprintf("%s/test/metricDisabled", serverPrefix), codes.OK, false, false},
		{"test/noPrefix", codes.OK, true, false},
	}

	for _, tc := range testcase {
		t.Run(fmt.Sprintf("%s %s", tc.methodName, tc.statusCode), func(t *testing.T) {
			serverGrpcMetricsEnabled.Override(ctx, &settings.SV, tc.metricEnabled)
			info := &grpc.UnaryServerInfo{FullMethod: tc.methodName}
			handler := func(ctx context.Context, req interface{}) (interface{}, error) {
				if tc.statusCode == codes.OK {
					time.Sleep(time.Millisecond)
					return struct{}{}, nil
				}
				return nil, status.Error(tc.statusCode, tc.statusCode.String())
			}
			_, err := interceptor(ctx, req, info, handler)
			if err != nil {
				require.Equal(t, tc.statusCode, status.Code(err))
			}
			var expectedCount uint64
			if tc.resultsExpected {
				expectedCount = 1
			}
			assertGrpcMetrics(t, metrics.RequestMetrics.ToPrometheusMetrics(), map[string]uint64{
				fmt.Sprintf("%s %s", tc.methodName, tc.statusCode): expectedCount,
			})
		})
	}
}

func assertGrpcMetrics(t *testing.T, metrics []*prometheusgo.Metric, expected map[string]uint64) {
	t.Helper()
	actual := map[string]*prometheusgo.Histogram{}
	for _, m := range metrics {
		var method, statusCode string
		for _, l := range m.Label {
			switch *l.Name {
			case GrpcMethodLabel:
				method = *l.Value
			case GrpcStatusCodeLabel:
				statusCode = *l.Value
			}
		}
		histogram := m.Histogram
		require.NotNil(t, histogram, "expected histogram")
		key := fmt.Sprintf("%s %s", method, statusCode)
		actual[key] = histogram
	}

	for key, val := range expected {
		histogram, ok := actual[key]
		if val == 0 {
			require.False(t, ok, "expected `%s` to not exist", key)
		} else {
			require.True(t, ok)
			require.Greater(t, *histogram.SampleSum, float64(0), "expected `%s` to have a SampleSum > 0", key)
			require.Equal(t, val, *histogram.SampleCount, "expected `%s` to have SampleCount of %d", key, val)
		}
	}
}
