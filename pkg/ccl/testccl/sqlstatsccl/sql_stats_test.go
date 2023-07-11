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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
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

	// Shorten the closed timestamp target duration so that span configs
	// propagate more rapidly.
	//closedts.TargetDuration.Override(ctx, &st.SV, 200*time.Millisecond)
	//kvserver.LoadBasedRebalancingObjective.Override(ctx, &st.SV, 0)
	//kvserver.MinLeaseTransferInterval.Override(ctx, &st.SV, 10*time.Millisecond)

	// Lengthen the lead time for the global tables to prevent overload from
	// resulting in delays in propagating closed timestamps and, ultimately
	// forcing requests from being redirected to the leaseholder. Without this
	// change, the test sometimes is flakey because the latency budget allocated
	// to closed timestamp propagation proves to be insufficient. This value is
	// very cautious, and makes this already slow test even slower.
	//closedts.SideTransportCloseInterval.Override(ctx, &st.SV, 50*time.Millisecond)
	//closedts.LeadForGlobalReadsOverride.Override(ctx, &st.SV, 1500*time.Millisecond)

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
			DefaultTestTenant: base.TestTenantDisabled,
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
			db.Exec(t, `SET CLUSTER SETTING sql.txn_stats.sample_rate = 1;`)

			db.Exec(t, fmt.Sprintf(`CREATE DATABASE testdb PRIMARY REGION "%s" PLACEMENT RESTRICTED`, regionNames[0]))
			for i := 1; i < len(regionNames); i++ {
				db.Exec(t, fmt.Sprintf(`ALTER DATABASE testdb ADD region "%s"`, regionNames[i]))
			}

			// Make a multi-region table and split its ranges across regions.
			db.Exec(t, "USE testdb")
			db.Exec(t, "CREATE TABLE test (a INT) LOCALITY REGIONAL BY ROW")

			// Add some data to each region.
			for i, regionName := range regionNames {
				db.Exec(t, "INSERT INTO test (a, crdb_region) VALUES ($1, $2)", i, regionName)
			}

			// It takes a while for the region replication to complete.
			testutils.SucceedsWithin(t, func() error {
				var expectedNodes []int64
				var expectedRegions []string
				_, err := db.DB.ExecContext(ctx, `USE testdb`)
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
							return fmt.Errorf("rows are not replicated to all regions %s\n", expectedRegions)
						}
					}

					// Example str " nodes: n1, n2, n4, n9"
					if strings.HasPrefix(explainStr, "nodes:") {
						explainStr = strings.ReplaceAll(explainStr, "nodes:", "")
						explainStr = strings.ReplaceAll(explainStr, "n", "")

						split := strings.Split(explainStr, ",")
						if len(split) < len(regionNames) {
							return fmt.Errorf("rows are not replicated to all regions %s\n", split)
						}

						// Gateway node was not included in the explain plan. Add it to the list
						if split[0] != "1" {
							expectedNodes = append(expectedNodes, int64(1))
						}

						for _, val := range split {
							node, err := strconv.Atoi(val)
							require.NoError(t, err)
							expectedNodes = append(expectedNodes, int64(node))
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

				// Replication to all regions can take some time to complete. During
				// this time a incomplete list will be returned.
				if !assert.ObjectsAreEqual(expectedNodes, actual.Nodes) {
					return fmt.Errorf("nodes are not equal. Expected: %d, Actual: %d", expectedNodes, actual.Nodes)
				}

				require.Equal(t, expectedRegions, actual.Regions)
				return nil
			}, 4*time.Minute)
		})
	}
}
