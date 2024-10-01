// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
)

func newTestMetricSource(clock hlc.WallClock) metricMarshaler {
	st := cluster.MakeTestingClusterSettings()
	metricSource := status.NewMetricsRecorder(
		roachpb.SystemTenantID,
		roachpb.NewTenantNameContainer(catconstants.SystemTenantName),
		nil, /* nodeLiveness */
		nil, /* remoteClocks */
		clock,
		st,
	)
	nodeDesc := roachpb.NodeDescriptor{
		NodeID: roachpb.NodeID(7),
	}
	reg1 := metric.NewRegistry()
	appReg := metric.NewRegistry()
	logReg := metric.NewRegistry()
	sysReg := metric.NewRegistry()
	metricSource.AddNode(reg1, appReg, logReg, sysReg, nodeDesc, 50, "foo:26257", "foo:26258", "foo:5432")

	m := struct {
		// Metrics that we need.
		Conns              *metric.Gauge
		NewConns           *metric.Counter
		QueryExecuted      *metric.Counter
		RunningNonIdleJobs *metric.Gauge
		// Metrics that we don't need.
		BytesInCount *metric.Counter
	}{
		// Metrics that we need.
		Conns:              metric.NewGauge(pgwire.MetaConns),
		NewConns:           metric.NewCounter(pgwire.MetaNewConns),
		QueryExecuted:      metric.NewCounter(sql.MetaQueryExecuted),
		RunningNonIdleJobs: metric.NewGauge(jobs.MetaRunningNonIdleJobs),
		// Metrics that we don't need.
		BytesInCount: metric.NewCounter(pgwire.MetaBytesIn),
	}
	regTenant := metric.NewRegistry()
	regTenant.AddMetricStruct(&m)
	metricSource.AddTenantRegistry(roachpb.SystemTenantID, regTenant)
	return metricSource
}

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

	munge := func(s string) string {
		return strings.ReplaceAll(s, ".", "_")
	}

	// Validate that all custom metrics are exported.
	additionalMetrics := additionalLoadEndpointMetricsSet()
	for name := range additionalMetrics {
		_, found = metrics[munge(name)]
		require.True(t, found)
	}

	// Non custom metrics should not be exported.
	_, found = metrics[munge(pgwire.MetaBytesIn.Name)]
	require.False(t, found)
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
	metricSource := newTestMetricSource(clock)
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
