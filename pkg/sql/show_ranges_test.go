// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/deprecatedshowranges"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShowRangesWithLocality(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numNodes = 3
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE TABLE t (x INT PRIMARY KEY)`)
	sqlDB.Exec(t, `ALTER TABLE t SPLIT AT SELECT i FROM generate_series(0, 20) AS g(i)`)

	const (
		leaseHolderIdx = iota
		leaseHolderLocalityIdx
		replicasColIdx
		localitiesColIdx
		votingReplicasIdx
		nonVotingReplicasIdx
	)
	replicas := make([]int, 3)

	// TestClusters get some localities by default.
	q := `SELECT lease_holder, lease_holder_locality, replicas, replica_localities, voting_replicas, non_voting_replicas
FROM [SHOW RANGES FROM TABLE t WITH DETAILS]`
	result := sqlDB.QueryStr(t, q)
	for _, row := range result {
		// Verify the leaseholder localities.
		leaseHolder := row[leaseHolderIdx]
		leaseHolderLocalityExpected := fmt.Sprintf(`region=test,dc=dc%s`, leaseHolder)
		require.Equal(t, leaseHolderLocalityExpected, row[leaseHolderLocalityIdx])

		// Verify the replica localities.
		_, err := fmt.Sscanf(row[replicasColIdx], "{%d,%d,%d}", &replicas[0], &replicas[1], &replicas[2])
		require.NoError(t, err)

		votingReplicas := sqltestutils.ArrayStringToSlice(t, row[votingReplicasIdx])
		sort.Strings(votingReplicas)
		require.Equal(t, []string{"1", "2", "3"}, votingReplicas)
		nonVotingReplicas := sqltestutils.ArrayStringToSlice(t, row[nonVotingReplicasIdx])
		require.Equal(t, []string{}, nonVotingReplicas)

		var builder strings.Builder
		builder.WriteString("{")
		for i, replica := range replicas {
			builder.WriteString(fmt.Sprintf(`"region=test,dc=dc%d"`, replica))
			if i != len(replicas)-1 {
				builder.WriteString(",")
			}
		}
		builder.WriteString("}")
		expected := builder.String()
		require.Equal(t, expected, row[localitiesColIdx])
	}
}

// TestRangeLocalityBasedOnNodeIDs tests that the leaseholder_locality shown in
// SHOW RANGES works correctly.
func TestShowRangesMultipleStores(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// NodeID=1, StoreID=1,2
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Locality:   roachpb.Locality{Tiers: []roachpb.Tier{{Key: "node", Value: "1"}}},
				StoreSpecs: []base.StoreSpec{base.DefaultTestStoreSpec, base.DefaultTestStoreSpec},
			},

			ReplicationMode: base.ReplicationAuto,
		},
	)
	defer tc.Stopper().Stop(ctx)
	// NodeID=2, StoreID=3,4
	tc.AddAndStartServer(t,
		base.TestServerArgs{
			Locality:   roachpb.Locality{Tiers: []roachpb.Tier{{Key: "node", Value: "2"}}},
			StoreSpecs: []base.StoreSpec{base.DefaultTestStoreSpec, base.DefaultTestStoreSpec},
		},
	)
	// NodeID=3, StoreID=5,6
	tc.AddAndStartServer(t,
		base.TestServerArgs{
			Locality:   roachpb.Locality{Tiers: []roachpb.Tier{{Key: "node", Value: "3"}}},
			StoreSpecs: []base.StoreSpec{base.DefaultTestStoreSpec, base.DefaultTestStoreSpec},
		},
	)
	assert.NoError(t, tc.WaitForFullReplication())

	// Scatter a system table so that the lease is unlike to be on node 1.
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, "ALTER TABLE system.jobs SCATTER")
	// Ensure that the localities line up.
	for _, q := range []string{
		"SHOW RANGES FROM DATABASE system WITH DETAILS",
		"SHOW RANGES FROM TABLE system.jobs WITH DETAILS",
		"SHOW RANGES FROM INDEX system.jobs@jobs_status_created_idx WITH DETAILS",
		"SHOW RANGE FROM TABLE system.jobs FOR ROW (0)",
		"SHOW RANGE FROM INDEX system.jobs@jobs_status_created_idx FOR ROW ('running', now(), 0)",
	} {
		t.Run(q, func(t *testing.T) {
			// Retry because if there's not a leaseholder, you can NULL.
			sqlDB.CheckQueryResultsRetry(t,
				fmt.Sprintf(`
SELECT DISTINCT
		(
		array_position(replica_localities, lease_holder_locality)
		= array_position(replicas, lease_holder)
		)
	FROM [%s]`, q), [][]string{{"true"}})
		})
	}
}

// Regression test for #102183 and #102218.
func TestDeprecatedShowRangesWithClusterSettingChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(sqlDB)

	// Initialize the plan cache with the default (modern) behavior.
	deprecatedshowranges.ShowRangesDeprecatedBehaviorSetting.Override(ctx, &s.ClusterSettings().SV, false)
	db.Exec(t, `TABLE crdb_internal.ranges_no_leases`)
	db.Exec(t, `TABLE crdb_internal.ranges`)

	// Now change the setting and verify that the plan cache is invalidated.
	deprecatedshowranges.ShowRangesDeprecatedBehaviorSetting.Override(ctx, &s.ClusterSettings().SV, true)
	db.Exec(t, `TABLE crdb_internal.ranges_no_leases`)
	db.Exec(t, `TABLE crdb_internal.ranges`)

	// Now change the setting back and verify that the plan cache is invalidated again.
	deprecatedshowranges.ShowRangesDeprecatedBehaviorSetting.Override(ctx, &s.ClusterSettings().SV, false)
	db.Exec(t, `TABLE crdb_internal.ranges_no_leases`)
	db.Exec(t, `TABLE crdb_internal.ranges`)
}
