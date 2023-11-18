// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
)

type testMetricSource struct{}

func (t testMetricSource) MarshalJSON() ([]byte, error)                                  { return nil, nil }
func (t testMetricSource) PrintAsText(writer io.Writer, contentType expfmt.Format) error { return nil }
func (t testMetricSource) ScrapeIntoPrometheus(pm *metric.PrometheusExporter)            {}

func extractPrometheusMetrics(
	t *testing.T, w *httptest.ResponseRecorder,
) (cpuUserNanos float64, cpuSysNanos float64, uptimeNanos float64) {
	var parser expfmt.TextParser
	metrics, err := parser.TextToMetricFamilies(w.Body)
	require.NoError(t, err)

	userCPU, found := metrics["sys_cpu_user_ns"]
	require.True(t, found)
	require.Len(t, userCPU.GetMetric(), 1)
	require.Equal(t, io_prometheus_client.MetricType_GAUGE, userCPU.GetType())
	cpuUserNanos = userCPU.Metric[0].GetGauge().GetValue()

	sysCPU, found := metrics["sys_cpu_sys_ns"]
	require.True(t, found)
	require.Len(t, sysCPU.GetMetric(), 1)
	require.Equal(t, io_prometheus_client.MetricType_GAUGE, sysCPU.GetType())
	cpuSysNanos = sysCPU.Metric[0].GetGauge().GetValue()

	uptime, found := metrics["sys_uptime"]
	require.True(t, found)
	require.Len(t, uptime.GetMetric(), 1)
	require.Equal(t, io_prometheus_client.MetricType_GAUGE, uptime.GetType())
	uptimeNanos = uptime.Metric[0].GetGauge().GetValue() * 1.e9
	return
}

// does 10ms stress on a single CPU core
func stressCpu() {
	startTime := timeutil.Now()
	for timeutil.Since(startTime) < 10*time.Millisecond {
		p := 423897
		p = p * p
		p++
	}
}

func Test_loadEndpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	clock := timeutil.NewTestTimeSource()
	rss := status.NewRuntimeStatSampler(ctx, clock)
	metricSource := testMetricSource{}
	le, err := newLoadEndpoint(rss, metricSource)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodGet, "/_status/load", nil)
	w := httptest.NewRecorder()

	startUptimeNanos := timeutil.Now().UnixNano() - le.initTimeNanos
	startUserTimeMillis, startSysTimeMillis, err := status.GetProcCPUTime(ctx)
	require.NoError(t, err)
	startCpuTimeMillis := startUserTimeMillis + startSysTimeMillis -
		(le.initUserTimeMillis + le.initSysTimeMillis)

	stressCpu()
	le.ServeHTTP(w, req)
	stressCpu()

	endUptimeNanos := timeutil.Now().UnixNano() - le.initTimeNanos
	endUserTimeMillis, endSysTimeMillis, err := status.GetProcCPUTime(ctx)
	require.NoError(t, err)
	endCpuTimeMillis := endUserTimeMillis + endSysTimeMillis - (le.initUserTimeMillis + le.initSysTimeMillis)

	cpuUserNanos, cpuSysNanos, uptimeNanos := extractPrometheusMetrics(t, w)
	// Ensure that the reported times are bracketed by what was observed before
	// and after the call.
	require.LessOrEqual(t, float64(startUptimeNanos), uptimeNanos)
	require.GreaterOrEqual(t, float64(endUptimeNanos), uptimeNanos)
	require.LessOrEqual(t, float64(startCpuTimeMillis)*1e6, cpuUserNanos+cpuSysNanos)
	require.GreaterOrEqual(t, float64(endCpuTimeMillis)*1e6, cpuUserNanos+cpuSysNanos)

	w = httptest.NewRecorder()

	stressCpu()
	le.ServeHTTP(w, req)
	stressCpu()

	cpuUserNanos, cpuSysNanos, uptimeNanos = extractPrometheusMetrics(t, w)

	require.LessOrEqual(t, float64(endUptimeNanos), uptimeNanos)
	require.LessOrEqual(t, float64(endCpuTimeMillis)*1e6, cpuUserNanos+cpuSysNanos)
}
