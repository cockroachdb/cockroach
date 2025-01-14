// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverccl

import (
	"context"
	"crypto/tls"
	"net/http"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/elastic/gosigar"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
)

func TestTenantVars(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer srv.Stopper().Stop(ctx)

	testutils.RunTrueAndFalse(t, "shared-process", func(t *testing.T, sharedProcess bool) {
		var tenant serverutils.ApplicationLayerInterface
		if !sharedProcess {
			tenant, _ = serverutils.StartTenant(t, srv, base.TestTenantArgs{
				TenantID: roachpb.MustMakeTenantID(10 /* id */),
			})
		} else {
			var err error
			tenant, _, err = srv.TenantController().StartSharedProcessTenant(ctx,
				base.TestSharedProcessTenantArgs{
					TenantName: roachpb.TenantName("test"),
					TenantID:   roachpb.MustMakeTenantID(20),
				})
			require.NoError(t, err)
		}

		startNowNanos := timeutil.Now().UnixNano()
		url := tenant.AdminURL().WithPath("/_status/load").String()
		client := http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		}
		resp, err := client.Get(url)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode,
			"invalid non-200 status code %v from tenant", resp.StatusCode)

		var parser expfmt.TextParser
		metrics, err := parser.TextToMetricFamilies(resp.Body)
		require.NoError(t, err)

		tenantUserCPU, found := metrics["sys_cpu_user_ns"]
		require.True(t, found)
		require.Len(t, tenantUserCPU.GetMetric(), 1)
		require.Equal(t, io_prometheus_client.MetricType_GAUGE, tenantUserCPU.GetType())
		tenantCpuUserNanos := tenantUserCPU.Metric[0].GetGauge().GetValue()

		tenantSysCPU, found := metrics["sys_cpu_sys_ns"]
		require.True(t, found)
		require.Len(t, tenantSysCPU.GetMetric(), 1)
		require.Equal(t, io_prometheus_client.MetricType_GAUGE, tenantSysCPU.GetType())
		tenantCpuSysNanos := tenantSysCPU.Metric[0].GetGauge().GetValue()

		now, found := metrics["sys_cpu_now_ns"]
		require.True(t, found)
		require.Len(t, now.GetMetric(), 1)
		require.Equal(t, io_prometheus_client.MetricType_GAUGE, now.GetType())
		nowNanos := now.Metric[0].GetGauge().GetValue()

		tenantUptime, found := metrics["sys_uptime"]
		require.True(t, found)
		require.Len(t, tenantUptime.GetMetric(), 1)
		require.Equal(t, io_prometheus_client.MetricType_GAUGE, tenantUptime.GetType())
		uptimeSeconds := tenantUptime.Metric[0].GetGauge().GetValue()

		// The values are between zero and whatever User/Sys time is observed after the get.
		require.LessOrEqual(t, float64(startNowNanos), nowNanos)
		require.LessOrEqual(t, nowNanos, float64(timeutil.Now().UnixNano()))

		testCpuTime := gosigar.ProcTime{}
		require.NoError(t, testCpuTime.Get(os.Getpid()))
		require.LessOrEqual(t, 0., tenantCpuUserNanos)
		require.LessOrEqual(t, tenantCpuUserNanos, float64(testCpuTime.User)*1e6)
		require.LessOrEqual(t, 0., tenantCpuSysNanos)
		require.LessOrEqual(t, tenantCpuSysNanos, float64(testCpuTime.Sys)*1e6)
		require.LessOrEqual(t, 0., uptimeSeconds)

		resp, err = client.Get(url)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode,
			"invalid non-200 status code %v from tenant", resp.StatusCode)

		metrics, err = parser.TextToMetricFamilies(resp.Body)
		require.NoError(t, err)

		tenantUserCPU, found = metrics["sys_cpu_user_ns"]
		require.True(t, found)
		require.Len(t, tenantUserCPU.GetMetric(), 1)
		require.Equal(t, io_prometheus_client.MetricType_GAUGE, tenantUserCPU.GetType())
		tenantCpuUserNanos2 := tenantUserCPU.Metric[0].GetGauge().GetValue()

		tenantSysCPU, found = metrics["sys_cpu_sys_ns"]
		require.True(t, found)
		require.Len(t, tenantSysCPU.GetMetric(), 1)
		require.Equal(t, io_prometheus_client.MetricType_GAUGE, tenantSysCPU.GetType())
		tenantCpuSysNanos2 := tenantSysCPU.Metric[0].GetGauge().GetValue()

		tenantUptime, found = metrics["sys_uptime"]
		require.True(t, found)
		require.Len(t, tenantUptime.GetMetric(), 1)
		require.Equal(t, io_prometheus_client.MetricType_GAUGE, tenantUptime.GetType())
		uptimeSeconds2 := tenantUptime.Metric[0].GetGauge().GetValue()

		cpuTime2 := gosigar.ProcTime{}
		require.NoError(t, cpuTime2.Get(os.Getpid()))

		require.Less(t, tenantCpuUserNanos2, float64(cpuTime2.User)*1e6)
		require.LessOrEqual(t, tenantCpuSysNanos2, float64(cpuTime2.Sys)*1e6)
		require.LessOrEqual(t, uptimeSeconds, uptimeSeconds2)

		_, found = metrics["jobs_running_non_idle"]
		require.True(t, found)
		_, found = metrics["sql_query_count"]
		require.True(t, found)
		_, found = metrics["sql_conns"]
		require.True(t, found)
	})
}
