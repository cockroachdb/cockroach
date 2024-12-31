// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverccl

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

var adminPrefix = "/_admin/v1/"

func getAdminJSONProto(
	ts serverutils.ApplicationLayerInterface, path string, response protoutil.Message,
) error {
	return getAdminJSONProtoWithAdminOption(ts, path, response, true)
}

func getAdminJSONProtoWithAdminOption(
	ts serverutils.ApplicationLayerInterface, path string, response protoutil.Message, isAdmin bool,
) error {
	return serverutils.GetJSONProtoWithAdminOption(ts, adminPrefix+path, response, isAdmin)
}

// TestAdminAPIDataDistributionPartitioning partitions a table and verifies
// that we see all zone configs (#27718).
func TestAdminAPIDataDistributionPartitioning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "this test creates a 3 node cluster; too resource intensive.")

	// TODO(clust-obs): This test should work with just a single node,
	// i.e. using serverutils.StartServer` instead of
	// `StartCluster`.
	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
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

	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
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
		DefaultTestTenant: base.TODOTestTenantDisabled,
		ExternalIODir:     dir})
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

	// Messages are not equal, since they only appear in the single job response,
	// so the deep-equal check would fail; copy it so the overall check passes.
	jobRes.Messages = backups[0].Messages
	require.Equal(t, backups[0], jobRes)
}

func TestListTenants(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	_, _, err := s.TenantController().StartSharedProcessTenant(ctx,
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	st := s.ApplicationLayer()
	_, err := db.Exec("CREATE TABLE test (id int)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO test VALUES (1)")
	require.NoError(t, err)

	// DatabaseDetails
	dbResp := &serverpb.DatabaseDetailsResponse{}
	err = getAdminJSONProto(st, "databases/defaultdb", dbResp)
	require.NoError(t, err)

	require.Equal(t, dbResp.TableNames[0], "public.test")

	var dbDetailsResp serverpb.DatabaseDetailsResponse
	err = getAdminJSONProto(st, "databases/defaultdb?include_stats=true", &dbDetailsResp)
	require.NoError(t, err)

	require.Greater(t, dbDetailsResp.Stats.RangeCount, int64(0))

	// TableStats
	tableStatsResp := &serverpb.TableStatsResponse{}
	err = getAdminJSONProto(st, "databases/defaultdb/tables/public.test/stats", tableStatsResp)
	require.NoError(t, err)

	require.Greater(t, tableStatsResp.Stats.LiveBytes, int64(0))

	// TableDetails
	// Call to endpoint is wrapped with retry logic to potentially avoid flakiness of
	// returned results (issue #112387).
	testutils.SucceedsSoon(t, func() error {
		tableDetailsResp := &serverpb.TableDetailsResponse{}
		if err := getAdminJSONProto(st, "databases/defaultdb/tables/public.test", tableDetailsResp); err != nil {
			return err
		}
		if tableDetailsResp.DataLiveBytes <= 0 {
			return fmt.Errorf("expected DataLiveBytes to be greater than 0 but got %d", tableDetailsResp.DataLiveBytes)
		}
		return nil
	})
}
