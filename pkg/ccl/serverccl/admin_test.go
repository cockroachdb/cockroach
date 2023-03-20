// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package serverccl

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

var adminPrefix = "/_admin/v1/"

func getAdminJSONProto(
	ts serverutils.TestTenantInterface, path string, response protoutil.Message,
) error {
	return getAdminJSONProtoWithAdminOption(ts, path, response, true)
}

func getAdminJSONProtoWithAdminOption(
	ts serverutils.TestTenantInterface, path string, response protoutil.Message, isAdmin bool,
) error {
	return serverutils.GetJSONProtoWithAdminOption(ts, adminPrefix+path, response, isAdmin)
}

// TestAdminAPIDataDistributionPartitioning partitions a table and verifies
// that we see all zone configs (#27718).
func TestAdminAPIDataDistributionPartitioning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Need to disable the test tenant because this test fails
	// when run through a tenant (with internal server error).
	// More investigation is required. Tracked with #76387.
	disableDefaultTestTenant := true
	testCluster := serverutils.StartNewTestCluster(t, 3,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				DisableDefaultTestTenant: disableDefaultTestTenant,
			},
		})
	defer testCluster.Stopper().Stop(context.Background())

	firstServer := testCluster.Server(0)
	sqlDB := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))

	sqlDB.Exec(t, `CREATE DATABASE roachblog`)
	sqlDB.Exec(t, `USE roachblog`)
	sqlDB.Exec(t, `CREATE TABLE posts (id INT PRIMARY KEY, title text, body text)`)
	sqlDB.Exec(t, `CREATE TABLE comments (
		id INT,
		post_id INT REFERENCES posts,
		user_region STRING,
		body text,
		PRIMARY KEY (user_region, id)
	) PARTITION BY LIST (user_region) (
		PARTITION us VALUES IN ('US'),
		PARTITION eu VALUES IN ('EU'),
		PARTITION DEFAULT VALUES IN (default)
	)`)

	// Create a zone config for each partition.
	// Would use locality constraints except this test cluster hasn't been started up with localities.
	sqlDB.Exec(t, `ALTER PARTITION us OF TABLE comments CONFIGURE ZONE USING gc.ttlseconds = 9001`)
	sqlDB.Exec(t, `ALTER PARTITION eu OF TABLE comments CONFIGURE ZONE USING gc.ttlseconds = 9002`)

	if disableDefaultTestTenant {
		// Make sure secondary tenants don't cause the endpoint to error.
		sqlDB.Exec(t, "CREATE TENANT 'app'")
	}

	// Assert that we get all roachblog zone configs back.
	expectedZoneConfigNames := map[string]struct{}{
		"PARTITION eu OF INDEX roachblog.public.comments@comments_pkey": {},
		"PARTITION us OF INDEX roachblog.public.comments@comments_pkey": {},
	}

	var resp serverpb.DataDistributionResponse
	err := serverutils.GetJSONProto(firstServer, adminPrefix+"data_distribution", &resp)
	require.NoError(t, err)

	actualZoneConfigNames := map[string]struct{}{}
	for name := range resp.ZoneConfigs {
		if strings.Contains(name, "roachblog") {
			actualZoneConfigNames[name] = struct{}{}
		}
	}
	if !reflect.DeepEqual(actualZoneConfigNames, expectedZoneConfigNames) {
		t.Fatalf("expected zone config names %v; got %v", expectedZoneConfigNames, actualZoneConfigNames)
	}
}

// TestAdminAPIChartCatalog verifies that an error doesn't happen.
func TestAdminAPIChartCatalog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.Background())

	firstServer := testCluster.Server(0)

	var resp serverpb.ChartCatalogResponse
	err := serverutils.GetJSONProto(firstServer, adminPrefix+"chartcatalog", &resp)
	require.NoError(t, err)
}

func TestAdminAPIJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Fails with the default test tenant. Tracked with #76378.
		DisableDefaultTestTenant: true,
		ExternalIODir:            dir})
	defer s.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `BACKUP INTO 'nodelocal://1/backup/1?AWS_SECRET_ACCESS_KEY=neverappears'`)

	var jobsRes serverpb.JobsResponse
	err := getAdminJSONProto(s, "jobs", &jobsRes)
	require.NoError(t, err)

	var backups []serverpb.JobResponse
	for _, job := range jobsRes.Jobs {
		if job.Type != "BACKUP" {
			continue
		}
		backups = append(backups, job)

	}

	if len(backups) != 1 {
		t.Errorf("Expected 1 Backup job, got %d", len(backups))
	}

	jobID := backups[0].ID

	var jobRes serverpb.JobResponse
	path := fmt.Sprintf("jobs/%v", jobID)
	err = getAdminJSONProto(s, path, &jobRes)
	require.NoError(t, err)

	require.Equal(t, backups[0], jobRes)
}

func TestListTenants(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		DisableDefaultTestTenant: true,
	})
	defer s.Stopper().Stop(ctx)

	_, _, err := s.(*server.TestServer).StartSharedProcessTenant(ctx,
		base.TestSharedProcessTenantArgs{
			TenantName: "test",
		})
	require.NoError(t, err)

	const path = "tenants"
	var response serverpb.ListTenantsResponse

	if err := getAdminJSONProto(s, path, &response); err != nil {
		t.Fatalf("unexpected error: %v\n", err)
	}

	require.NotEmpty(t, response.Tenants)
	appTenantFound := false
	for _, tenant := range response.Tenants {
		if tenant.TenantName == "test" {
			appTenantFound = true
		}
		require.NotNil(t, tenant.TenantId)
		require.NotEmpty(t, tenant.TenantName)
		require.NotEmpty(t, tenant.RpcAddr)
		require.NotEmpty(t, tenant.SqlAddr)
	}
	require.True(t, appTenantFound, "test tenant not found")
}

func TestTableAndDatabaseDetailsAndStats(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	st, db := serverutils.StartTenant(t, s, base.TestTenantArgs{
		TenantID: serverutils.TestTenantID(),
	})
	_, err := db.Exec("CREATE TABLE test (id int)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO test VALUES (1)")
	require.NoError(t, err)

	// DatabaseDetails
	dbResp := &serverpb.DatabaseDetailsResponse{}
	err = getAdminJSONProto(st, "databases/defaultdb", dbResp)
	require.NoError(t, err)

	require.Equal(t, dbResp.TableNames[0], "public.test")

	// TableStats
	tableStatsResp := &serverpb.TableStatsResponse{}
	err = getAdminJSONProto(st, "databases/defaultdb/tables/public.test/stats", tableStatsResp)
	require.NoError(t, err)

	require.Greater(t, tableStatsResp.Stats.LiveBytes, int64(0))

	// TableDetails
	tableDetailsResp := &serverpb.TableDetailsResponse{}
	err = getAdminJSONProto(st, "databases/defaultdb/tables/public.test", tableDetailsResp)
	require.NoError(t, err)

	require.Greater(t, tableDetailsResp.DataLiveBytes, int64(0))
}
