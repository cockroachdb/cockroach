// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package serverccl

import (
	"bufio"
	"context"
	"crypto/tls"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/elastic/gosigar"
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

	url := "https://" + tenant.HTTPAddr() + "/_status/load"
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

	prometheusMetricStringPattern := `^(?P<metric>\w+)(?:\{` +
		`(?P<labelvalues>(\w+=\".*\",)*(\w+=\".*\")?)\})?\s+(?P<value>.*)$`
	promethusMetricStringRE := regexp.MustCompile(prometheusMetricStringPattern)

	var cpuUserNS, cpuSysNS float64
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		matches := promethusMetricStringRE.FindStringSubmatch(scanner.Text())
		if matches != nil {
			if matches[1] == "sys_cpu_user_ns" {
				cpuUserNS, err = strconv.ParseFloat(matches[5], 64)
				require.NoError(t, err)
			}
			if matches[1] == "sys_cpu_sys_ns" {
				cpuSysNS, err = strconv.ParseFloat(matches[5], 64)
				require.NoError(t, err)
			}
		}
	}
	// The values are between zero and whatever User/Sys time is observed after the get.
	require.Positive(t, cpuUserNS)
	require.Positive(t, cpuSysNS)
	cpuTime := gosigar.ProcTime{}
	require.NoError(t, cpuTime.Get(os.Getpid()))
	require.LessOrEqual(t, cpuUserNS, float64(cpuTime.User)*1e6)
	require.LessOrEqual(t, cpuSysNS, float64(cpuTime.Sys)*1e6)

	resp, err = client.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode,
		"invalid non-200 status code %v from tenant", resp.StatusCode)

	var cpuUserNS2, cpuSysNS2 float64
	scanner = bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		matches := promethusMetricStringRE.FindStringSubmatch(scanner.Text())
		if matches != nil {
			if matches[1] == "sys_cpu_user_ns" {
				cpuUserNS2, err = strconv.ParseFloat(matches[5], 64)
				require.NoError(t, err)
			}
			if matches[1] == "sys_cpu_sys_ns" {
				cpuSysNS2, err = strconv.ParseFloat(matches[5], 64)
				require.NoError(t, err)
			}
		}
	}
	require.LessOrEqual(t, float64(cpuTime.User)*1e6, cpuUserNS2)
	require.LessOrEqual(t, float64(cpuTime.Sys)*1e6, cpuSysNS2)
}
