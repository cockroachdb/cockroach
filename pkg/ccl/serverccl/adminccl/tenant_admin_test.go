// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package adminccl

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/serverccl"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTenantAdminAPI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The liveness session might expire before the stress race can finish.
	skip.UnderStressRace(t, "expensive tests")

	ctx := context.Background()

	knobs := tests.CreateTestingKnobs()
	knobs.SpanConfig = &spanconfig.TestingKnobs{
		// Some of these subtests expect multiple (uncoalesced) tenant ranges.
		StoreDisableCoalesceAdjacent: true,
	}

	testHelper := serverccl.NewTestTenantHelper(t, 3 /* tenantClusterSize */, knobs)
	defer testHelper.Cleanup(ctx, t)

	t.Run("tenant_jobs", func(t *testing.T) {
		testJobsRPCs(ctx, t, testHelper)
	})
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
