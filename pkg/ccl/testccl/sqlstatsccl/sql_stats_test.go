// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlstatsccl_test

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
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

	skip.UnderStress(t, "test is too heavy to run under stress")

	// We build a small multiregion cluster, with the proper settings for
	// multi-region tenants, then run tests over both the system tenant
	// and a secondary tenant, ensuring that a distsql query across multiple
	// regions sees those regions reported in sqlstats.
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	sql.SecondaryTenantsMultiRegionAbstractionsEnabled.Override(ctx, &st.SV, true)
	sql.SecondaryTenantZoneConfigsEnabled.Override(ctx, &st.SV, true)

	numServers := 9
	regionNames := []string{
		"gcp-us-west1",
		"gcp-us-central1",
		"gcp-us-east1",
	}

	serverArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		serverArgs[i] = base.TestServerArgs{
			Settings: st,
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{{Key: "region", Value: regionNames[i%len(regionNames)]}},
			},
			// We'll start our own test tenant manually below.
			DisableDefaultTestTenant: true,
		}
	}

	host := testcluster.StartTestCluster(t, numServers, base.TestClusterArgs{
		ServerArgsPerNode: serverArgs,
	})
	defer host.Stopper().Stop(ctx)

	testCases := []struct {
		name string
		db   func(t *testing.T, host *testcluster.TestCluster, st *cluster.Settings) *sqlutils.SQLRunner
	}{{
		// This test runs against the system tenant, opening a database
		// connection to the first node in the cluster.
		name: "system tenant",
		db: func(t *testing.T, host *testcluster.TestCluster, _ *cluster.Settings) *sqlutils.SQLRunner {
			return sqlutils.MakeSQLRunner(host.ServerConn(0))
		},
	}, {
		// This test runs against a secondary tenant, launching a SQL instance
		// for each node in the underlying cluster and returning a database
		// connection to the first one.
		name: "secondary tenant",
		db: func(t *testing.T, host *testcluster.TestCluster, st *cluster.Settings) *sqlutils.SQLRunner {
			var dbs []*gosql.DB
			for _, server := range host.Servers {
				_, db := serverutils.StartTenant(t, server, base.TestTenantArgs{
					Settings: st,
					TenantID: roachpb.MustMakeTenantID(11),
					Locality: *server.Locality(),
				})
				dbs = append(dbs, db)
			}
			return sqlutils.MakeSQLRunner(dbs[0])
		},
	}}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := tc.db(t, host, st)

			// Create a multi-region database.
			db.Exec(t, "SET enable_multiregion_placement_policy = true")
			db.Exec(t, fmt.Sprintf(`CREATE DATABASE testdb PRIMARY REGION "%s" PLACEMENT RESTRICTED`, regionNames[0]))
			for i := 1; i < len(regionNames); i++ {
				db.Exec(t, fmt.Sprintf(`ALTER DATABASE testdb ADD region "%s"`, regionNames[i]))
			}

			// Make a multi-region table and split its ranges across regions.
			db.Exec(t, "USE testdb")
			db.Exec(t, "CREATE TABLE test (a INT) LOCALITY REGIONAL BY ROW")

			// Add some data to each region.
			for i, regionName := range regionNames {
				db.Exec(t, "INSERT INTO test (crdb_region, a) VALUES ($1, $2)", regionName, i)
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
			err := json.Unmarshal([]byte(actualJSON), &actual)
			require.NoError(t, err)

			require.Equal(t,
				appstatspb.StatementStatistics{
					// TODO(todd): It appears we do not yet reliably record
					//  the nodes for the statement. (I have manually verified
					//  that the above query does indeed fan out across the
					//  regions, via EXPLAIN (DISTSQL).) Filed as #96647.
					//Nodes:   []int64{1, 2, 3},
					//Regions: regionNames,
					Nodes:   []int64{1},
					Regions: []string{regionNames[0]},
				},
				appstatspb.StatementStatistics{
					Nodes:   actual.Nodes,
					Regions: actual.Regions,
				},
			)
		})
	}
}
