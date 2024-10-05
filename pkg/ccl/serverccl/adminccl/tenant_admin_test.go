// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package adminccl

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/serverccl"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestTenantAdminAPI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The liveness session might expire before the stress race can finish.
	skip.UnderRace(t, "expensive tests")

	ctx := context.Background()

	var knobs base.TestingKnobs
	knobs.SpanConfig = &spanconfig.TestingKnobs{
		// Some of these subtests expect multiple (uncoalesced) tenant ranges.
		StoreDisableCoalesceAdjacent: true,
	}

	testHelper := serverccl.NewTestTenantHelper(t, 3 /* tenantClusterSize */, knobs)
	defer testHelper.Cleanup(ctx, t)

	t.Run("tenant_jobs", func(t *testing.T) {
		testJobsRPCs(ctx, t, testHelper)
	})

	t.Run("tenant_unimplemented", func(t *testing.T) {
		testUnimplementedRPCs(ctx, t, testHelper)
	})

	t.Run("tenant_metricmetadata", func(t *testing.T) {
		testMetricMetadataRPC(ctx, t, testHelper)
	})

	t.Run("tenant_metrics_capability", func(t *testing.T) {
		testTenantMetricsCapabilityRPC(ctx, t, testHelper)
	})
}

func testTenantMetricsCapabilityRPC(
	ctx context.Context, t *testing.T, helper serverccl.TenantTestHelper,
) {
	http := helper.TestCluster().TenantAdminHTTPClient(t, 1)
	defer http.Close()

	query := tspb.TimeSeriesQueryRequest{
		StartNanos: 0,
		EndNanos:   timeutil.Now().UnixNano(),
		Queries: []tspb.Query{
			{
				Name: "cr.node.sql.select.count",
			},
		},
		SampleNanos: 0,
	}
	queryResp := tspb.TimeSeriesQueryResponse{}
	err := http.PostJSONChecked("/ts/query", &query, &queryResp)
	require.Error(t, err)

	s := helper.HostCluster().Server(0)
	db := helper.HostCluster().ServerConn(0)
	_, err = db.Exec("ALTER TENANT [10] GRANT CAPABILITY can_view_tsdb_metrics=true\n")
	require.NoError(t, err)
	capability := map[tenantcapabilities.ID]string{tenantcapabilities.CanViewTSDBMetrics: "true"}
	serverutils.WaitForTenantCapabilities(t, s, serverutils.TestTenantID(), capability, "")

	err = http.PostJSONChecked("/ts/query", &query, &queryResp)
	require.NoError(t, err)
	require.Greater(t, len(queryResp.Results), 0)

	// Check system tenant metric retrieval.
	query = tspb.TimeSeriesQueryRequest{
		StartNanos: 0,
		EndNanos:   timeutil.Now().UnixNano(),
		Queries: []tspb.Query{
			{
				Name: "cr.node.sys.rss",
			},
		},
		SampleNanos: 0,
	}
	err = http.PostJSONChecked("/ts/query", &query, &queryResp)
	require.Error(t, err)

	_, err = db.Exec("ALTER TENANT [10] GRANT CAPABILITY can_view_all_metrics=true\n")
	require.NoError(t, err)
	capability = map[tenantcapabilities.ID]string{tenantcapabilities.CanViewAllMetrics: "true"}
	serverutils.WaitForTenantCapabilities(t, s, serverutils.TestTenantID(), capability, "")

	err = http.PostJSONChecked("/ts/query", &query, &queryResp)
	require.NoError(t, err)
	require.Greater(t, len(queryResp.Results), 0)
}

func testMetricMetadataRPC(ctx context.Context, t *testing.T, helper serverccl.TenantTestHelper) {
	http := helper.TestCluster().TenantAdminHTTPClient(t, 1)
	defer http.Close()

	metricMetadataResp := serverpb.MetricMetadataResponse{}
	http.GetJSON("/_admin/v1/metricmetadata", &metricMetadataResp)
	require.NotEmpty(t, metricMetadataResp.Metadata)
}

func testUnimplementedRPCs(ctx context.Context, t *testing.T, helper serverccl.TenantTestHelper) {
	http := helper.TestCluster().TenantAdminHTTPClient(t, 1)
	defer http.Close()

	client := http.GetClient()
	baseURL := http.GetBaseURL()

	resp, err := client.Post(baseURL+"/_admin/v1/enqueue_range", "application/json", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 501, resp.StatusCode)
}

func testJobsRPCs(ctx context.Context, t *testing.T, helper serverccl.TenantTestHelper) {
	http := helper.TestCluster().TenantAdminHTTPClient(t, 1)
	defer http.Close()

	_ = helper.TestCluster().TenantConn(1).Exec(t, "CREATE TABLE test (id INT)")
	_ = helper.TestCluster().TenantConn(1).Exec(t, "ALTER TABLE test ADD COLUMN name STRING")

	jobsResp := serverpb.JobsResponse{}
	http.GetJSON("/_admin/v1/jobs", &jobsResp)
	require.NotEmpty(t, jobsResp.Jobs)

	jobResp := serverpb.JobResponse{}
	job := jobsResp.Jobs[0]
	http.GetJSON(fmt.Sprintf("/_admin/v1/jobs/%d", job.ID), &jobResp)

	require.Equal(t, jobResp.ID, job.ID)
}
