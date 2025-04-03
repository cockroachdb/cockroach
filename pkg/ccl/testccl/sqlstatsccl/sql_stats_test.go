// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlstatsccl_test

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSQLStatsRegions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDuress(t, "test is too heavy for special configs")

	// We build a small multiregion cluster, with the proper settings for
	// multi-region tenants, then run tests over both the system tenant
	// and a secondary tenant, ensuring that a distsql query across multiple
	// regions sees those regions reported in sqlstats.
	ctx := context.Background()

	numServers := 9
	regionNames := []string{
		"gcp-us-west1",
		"gcp-us-central1",
		"gcp-us-east1",
	}

	serverArgs := make(map[int]base.TestServerArgs)
	signalAfter := make([]chan struct{}, numServers)
	for i := 0; i < numServers; i++ {
		signalAfter[i] = make(chan struct{})
		args := base.TestServerArgs{
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{{Key: "region", Value: regionNames[i%len(regionNames)]}},
			},
			// We'll start our own test tenant manually below.
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		}

		serverKnobs := &server.TestingKnobs{
			SignalAfterGettingRPCAddress: signalAfter[i],
		}

		args.Knobs.Server = serverKnobs
		serverArgs[i] = args
	}

	host := testcluster.StartTestCluster(t, numServers, base.TestClusterArgs{
		ServerArgsPerNode: serverArgs,
		ParallelStart:     true,
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		},
	})
	defer host.Stopper().Stop(ctx)

	go func() {
		for _, c := range signalAfter {
			<-c
		}
	}()

	tdb := sqlutils.MakeSQLRunner(host.ServerConn(0))
	tdb.Exec(t, `ALTER RANGE meta configure zone using constraints = '{"+region=gcp-us-west1": 1, "+region=gcp-us-central1": 1, "+region=gcp-us-east1": 1}';`)

	// Create secondary tenants
	var tenantDbs []*gosql.DB
	for _, server := range host.Servers {
		_, tenantDb := serverutils.StartTenant(t, server, base.TestTenantArgs{
			TenantID: roachpb.MustMakeTenantID(11),
			Locality: server.Locality(),
		})
		tenantDbs = append(tenantDbs, tenantDb)
	}

	systemDbName := "testDbSystem"
	createMultiRegionDbAndTable(t, tdb, regionNames, systemDbName)
	tenantRunner := sqlutils.MakeSQLRunner(tenantDbs[1])
	tenantDbName := "testDbTenant"
	createMultiRegionDbAndTable(t, tenantRunner, regionNames, tenantDbName)

	testCases := []struct {
		name   string
		dbName string
		db     *sqlutils.SQLRunner
	}{{
		// This test runs against the system tenant, opening a database
		// connection to the first node in the cluster.
		name:   "system tenant",
		dbName: systemDbName,
		db:     tdb,
	}, {
		// This test runs against a secondary tenant, launching a SQL instance
		// for each node in the underlying cluster and returning a database
		// connection to the first one.
		name:   "secondary tenant",
		dbName: tenantDbName,
		db:     tenantRunner,
	}}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := tc.db
			db.Exec(t, `SET CLUSTER SETTING sql.txn_stats.sample_rate = 1;`)

			testutils.SucceedsWithin(t, func() (err error) {
				var expectedRegions []string
				_, err = db.DB.ExecContext(ctx, fmt.Sprintf(`USE %s`, tc.dbName))
				if err != nil {
					return err
				}
				_, err = db.DB.ExecContext(ctx, `SET distsql = on;`)
				if err != nil {
					return err
				}
				// Use EXPLAIN ANALYSE (DISTSQL) to get the accurate list of nodes.
				explainInfo, err := db.DB.QueryContext(ctx, `EXPLAIN ANALYSE (DISTSQL) SELECT * FROM test`)
				if err != nil {
					return err
				}
				for explainInfo.Next() {
					var explainStr string
					if err := explainInfo.Scan(&explainStr); err != nil {
						t.Fatal(err)
					}

					explainStr = strings.ReplaceAll(explainStr, " ", "")
					// Example str "  regions: cp-us-central1,gcp-us-east1,gcp-us-west1"
					if strings.HasPrefix(explainStr, "regions:") {
						explainStr = strings.ReplaceAll(explainStr, "regions:", "")
						explainStr = strings.ReplaceAll(explainStr, " ", "")
						expectedRegions = strings.Split(explainStr, ",")
						if len(expectedRegions) < len(regionNames) {
							return fmt.Errorf("rows are not replicated to all regions."+
								" Expected replication to following regions %s but got %s\n", regionNames, expectedRegions)
						}
					}
				}

				// Select from the table and see what statement statistics were written.
				db.Exec(t, "SET application_name = $1", t.Name())
				db.Exec(t, "SELECT * FROM test")
				row := db.QueryRow(t, `
				SELECT statistics->>'statistics'
				  FROM crdb_internal.statement_statistics
				 WHERE app_name = $1`, t.Name())

				var actualJSON string
				row.Scan(&actualJSON)
				var actual appstatspb.StatementStatistics
				err = json.Unmarshal([]byte(actualJSON), &actual)
				require.NoError(t, err)

				foundRegions := 0
				for _, r := range expectedRegions {
					if slices.Contains(actual.Regions, r) {
						foundRegions += 1
					}
				}
				// As long as we find more than 1 region, we pass the test.
				// Asserting that all 3 regions are present times out
				// frequently and makes this test less useful.
				require.Greater(t, foundRegions, 1, "expect at least 2 regions present in the statement stats, found: %v", actual.Regions)

				return nil
			}, 3*time.Minute)
		})
	}
}

func createMultiRegionDbAndTable(
	t *testing.T, db *sqlutils.SQLRunner, regionNames []string, dbName string,
) {
	db.Exec(t, "SET enable_multiregion_placement_policy = true")

	db.Exec(t, fmt.Sprintf(`CREATE DATABASE %s PRIMARY REGION "%s" PLACEMENT RESTRICTED`, dbName, regionNames[0]))
	for i := 1; i < len(regionNames); i++ {
		db.Exec(t, fmt.Sprintf(`ALTER DATABASE %s ADD region "%s"`, dbName, regionNames[i]))
	}

	// Make a multi-region table and split its ranges across regions.
	db.Exec(t, fmt.Sprintf("USE %s", dbName))
	db.Exec(t, "CREATE TABLE test (a INT) LOCALITY REGIONAL BY ROW")

	// Add some data to each region.
	for i, regionName := range regionNames {
		db.Exec(t, "INSERT INTO test (a, crdb_region) VALUES ($1, $2)", i, regionName)
	}
}
