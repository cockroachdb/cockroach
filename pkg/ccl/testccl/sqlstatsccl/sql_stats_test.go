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
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// In the past, we were able to wait to map nodeIDs to regions until UI
// rendering time, since nodes were generally long-lived and nodeID->region
// assignments were stable. Now that we are moving toward multiregion
// support in serverless, those conditions no longer hold, and we need to
// remember the regions for a statement as the statement executes.
// This test demonstrates that behavior.
func TestSQLStatsRegions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We build a small multiregion cluster, then run tests over both the
	// system tenant and a secondary tenant.
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	sql.SecondaryTenantsMultiRegionAbstractionsEnabled.Override(ctx, &st.SV, true)
	sql.SecondaryTenantZoneConfigsEnabled.Override(ctx, &st.SV, true)

	regionNames := []string{
		"gcp-us-west1",
		"gcp-us-central1",
		"gcp-us-east1",
	}

	serverArgs := make(map[int]base.TestServerArgs)

	for i := 0; i < len(regionNames); i++ {
		serverArgs[i] = base.TestServerArgs{
			Settings: st,
			Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: regionNames[i]}}},
			// We'll start our own test tenant manually below.
			DisableDefaultTestTenant: true,
		}
	}

	host := testcluster.StartTestCluster(t, len(regionNames), base.TestClusterArgs{
		ServerArgsPerNode: serverArgs,
	})

	defer host.Stopper().Stop(ctx)

	testCases := []struct {
		name  string
		conns func(t *testing.T, host *testcluster.TestCluster, st *cluster.Settings) []*gosql.DB
	}{{
		// This test runs against the system tenant, opening a database
		// connection for each node in the cluster.
		name: "system tenant",
		conns: func(t *testing.T, host *testcluster.TestCluster, _ *cluster.Settings) []*gosql.DB {
			return host.Conns
		},
	}, {
		// This test runs against a secondary tenant, launching a SQL instance
		// for each node in the underlying cluster and opening a database
		// connection to it.
		name: "secondary tenant",
		conns: func(t *testing.T, host *testcluster.TestCluster, st *cluster.Settings) []*gosql.DB {
			var conns []*gosql.DB
			for _, server := range host.Servers {
				_, db := serverutils.StartTenant(t, server, base.TestTenantArgs{
					Settings: st,
					TenantID: roachpb.MustMakeTenantID(11),
					Locality: *server.Locality(),
				})
				conns = append(conns, db)
			}
			return conns
		},
	}}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conns := tc.conns(t, host, st)
			defer func() {
				for _, conn := range conns {
					_ = conn.Close()
				}
			}()

			dbs := make([]*sqlutils.SQLRunner, 0, len(conns))
			for _, conn := range conns {
				dbs = append(dbs, sqlutils.MakeSQLRunner(conn))
			}

			// Spread defaultdb across the regions.
			dbs[0].Exec(t, fmt.Sprintf(`ALTER DATABASE defaultdb SET PRIMARY REGION "%s"`, regionNames[0]))
			for i := 1; i < len(regionNames); i++ {
				dbs[0].Exec(t, fmt.Sprintf(`ALTER DATABASE defaultdb ADD region "%s"`, regionNames[i]))
			}

			// Make a multi-region table and write to it from each region.
			dbs[0].Exec(t, "CREATE TABLE test (a INT) LOCALITY REGIONAL BY ROW")
			for i := 0; i < len(dbs); i++ {
				dbs[i].Exec(t, "INSERT INTO test (a) VALUES ($1)", i)
			}

			// Update all its rows and see what statement statistics were written.
			dbs[0].Exec(t, "SET application_name = $1", t.Name())
			dbs[0].Exec(t, "UPDATE test SET a = a + 1")
			row := dbs[0].QueryRow(t, `
				SELECT statistics->'statistics'->>'nodes',
				       statistics->'statistics'->>'regions'
				  FROM crdb_internal.statement_statistics
				 WHERE app_name = $1`, t.Name())

			// The regions in statement_statistics should match the nodes.
			// (Ideally, we would directly assert on the regions we expect,
			//  but I need help crafting a query to hit more than one node.
			//  Indeed, I am not sure how distsql-y we can yet be in a
			//  tenant context anyway. In the meantime, this property (that
			//  regions should be derived from nodes) should always be true.)
			var nodesJSON, regionsJSON string
			row.Scan(&nodesJSON, &regionsJSON)
			var nodes []int
			var regions []string
			err := json.Unmarshal([]byte(nodesJSON), &nodes)
			require.NoError(t, err)
			err = json.Unmarshal([]byte(regionsJSON), &regions)
			require.NoError(t, err)

			knownRegions := make(map[int]string)
			for _, server := range host.Servers {
				if region, ok := server.Locality().Find("region"); ok {
					knownRegions[int(server.NodeID())] = region
				}
			}

			expectedRegionsSet := make(map[string]struct{})
			for _, node := range nodes {
				if region, ok := knownRegions[node]; ok {
					expectedRegionsSet[region] = struct{}{}
				}
			}

			var expectedRegions []string
			for region := range expectedRegionsSet {
				expectedRegions = append(expectedRegions, region)
			}
			sort.Strings(expectedRegions)
			require.Equal(t, expectedRegions, regions)
		})
	}
}
