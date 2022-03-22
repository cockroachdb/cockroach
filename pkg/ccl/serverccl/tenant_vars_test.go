// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
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

	serverParams, _ := tests.CreateTestServerParams()
	testCluster := serverutils.StartNewTestCluster(t, 1 /* numNodes */, base.TestClusterArgs{
		ServerArgs: serverParams,
	})
	defer testCluster.Stopper().Stop(ctx)

	server := testCluster.Server(0 /* idx */)

	tenant, _ := serverutils.StartTenant(t, server, base.TestTenantArgs{
		TenantID: roachpb.MakeTenantID(10 /* id */),
	})

	startNowNanos := timeutil.Now().UnixNano()
	url := tenant.AdminURL() + "/_status/load"
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

	userCPU, found := metrics["sys_cpu_user_ns"]
	require.True(t, found)
	require.Len(t, userCPU.GetMetric(), 1)
	require.Equal(t, io_prometheus_client.MetricType_GAUGE, userCPU.GetType())
	cpuUserNanos := userCPU.Metric[0].GetGauge().GetValue()

	sysCPU, found := metrics["sys_cpu_sys_ns"]
	require.True(t, found)
	require.Len(t, sysCPU.GetMetric(), 1)
	require.Equal(t, io_prometheus_client.MetricType_GAUGE, sysCPU.GetType())
	cpuSysNanos := sysCPU.Metric[0].GetGauge().GetValue()

	now, found := metrics["sys_cpu_now_ns"]
	require.True(t, found)
	require.Len(t, now.GetMetric(), 1)
	require.Equal(t, io_prometheus_client.MetricType_GAUGE, now.GetType())
	nowNanos := now.Metric[0].GetGauge().GetValue()

	// The values are between zero and whatever User/Sys time is observed after the get.
	require.Positive(t, cpuUserNanos)
	require.Positive(t, cpuSysNanos)
	require.Positive(t, nowNanos)
	cpuTime := gosigar.ProcTime{}
	require.NoError(t, cpuTime.Get(os.Getpid()))
	require.LessOrEqual(t, cpuUserNanos, float64(cpuTime.User)*1e6)
	require.LessOrEqual(t, cpuSysNanos, float64(cpuTime.Sys)*1e6)
	require.GreaterOrEqual(t, nowNanos, float64(startNowNanos))
	require.LessOrEqual(t, nowNanos, float64(timeutil.Now().UnixNano()))

	resp, err = client.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode,
		"invalid non-200 status code %v from tenant", resp.StatusCode)

	metrics, err = parser.TextToMetricFamilies(resp.Body)
	require.NoError(t, err)

	userCPU, found = metrics["sys_cpu_user_ns"]
	require.True(t, found)
	require.Len(t, userCPU.GetMetric(), 1)
	require.Equal(t, io_prometheus_client.MetricType_GAUGE, userCPU.GetType())
	cpuUserNanos2 := userCPU.Metric[0].GetGauge().GetValue()

	sysCPU, found = metrics["sys_cpu_sys_ns"]
	require.True(t, found)
	require.True(t, found)
	require.Len(t, sysCPU.GetMetric(), 1)
	require.Equal(t, io_prometheus_client.MetricType_GAUGE, sysCPU.GetType())
	cpuSysNanos2 := sysCPU.Metric[0].GetGauge().GetValue()

	require.LessOrEqual(t, float64(cpuTime.User)*1e6, cpuUserNanos2)
	require.LessOrEqual(t, float64(cpuTime.Sys)*1e6, cpuSysNanos2)
}
