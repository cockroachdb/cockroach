// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
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

// TestShowRangesMultipleStores tests that the leaseholder_locality shown in
// SHOW RANGES works correctly.
func TestShowRangesMultipleStores(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "the test is too heavy")

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

	// Scatter a system table so that the lease is unlikely to be on node 1.
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
			// Retry because if there's not a leaseholder, you can get a NULL.
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

func TestShowRangesWithDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, "CREATE DATABASE test")
	sqlDB.Exec(t, "USE test")
	sqlDB.Exec(t, `
		CREATE TABLE users (
		    id INTEGER PRIMARY KEY,
		    name STRING
		)
	`)

	// Assert the required keys are present.
	res := sqlDB.Query(t, `
		SELECT
		    span_stats->'approximate_disk_bytes',
		    span_stats->'key_count',
		    span_stats->'key_bytes',
		    span_stats->'val_count',
		    span_stats->'val_bytes',
		    span_stats->'sys_count',
		    span_stats->'sys_bytes',
		    span_stats->'live_count',
		    span_stats->'live_bytes',
		    span_stats->'intent_count',
		    span_stats->'intent_bytes'
		FROM [SHOW RANGES FROM DATABASE test WITH DETAILS]`)

	res.Next()
	vals := make([]interface{}, 11)
	for i := range vals {
		vals[i] = new(interface{})
	}
	err := res.Scan(vals...)
	// Every key should be present, and the scan should be successful.
	require.NoError(t, err)

	// This invocation of SHOW RANGES should have only returned a single row.
	require.Equal(t, false, res.NextResultSet())

	// Assert the counterpoint: Scan should return an error for a key that
	// does not exist.
	badQuery := sqlDB.Query(t, `
		SELECT span_stats->'key_does_not_exist'
		FROM [SHOW RANGES FROM DATABASE test WITH DETAILS]`)

	badQuery.Next()
	var keyDoesNotExistVal int
	err = badQuery.Scan(&keyDoesNotExistVal)
	require.Error(t, err)

	// Now, let's add some users, and query the table's val_bytes.
	sqlDB.Exec(t, "INSERT INTO test.users (id, name) VALUES (1, 'ab'), (2, 'cd')")

	valBytesPreSplitRes := sqlDB.QueryRow(t, `
		SELECT span_stats->'val_bytes'
		FROM [SHOW RANGES FROM DATABASE test WITH DETAILS]`,
	)

	var valBytesPreSplit int
	valBytesPreSplitRes.Scan(&valBytesPreSplit)

	// Split the table at the second row, so it occupies a second range.
	sqlDB.Exec(t, `ALTER TABLE test.users SPLIT AT VALUES (2)`)
	afterSplit := sqlDB.Query(t, `
		SELECT span_stats->'val_bytes'
		FROM [SHOW RANGES FROM TABLE test.users WITH DETAILS]
	`)

	var valBytesR1 int
	var valBytesR2 int

	afterSplit.Next()
	err = afterSplit.Scan(&valBytesR1)
	require.NoError(t, err)

	afterSplit.Next()
	err = afterSplit.Scan(&valBytesR2)
	require.NoError(t, err)

	// Assert that the sum of val_bytes for each range equals the
	// val_bytes for the whole table.
	require.Equal(t, valBytesPreSplit, valBytesR1+valBytesR2)
}

// TestShowRangesUnavailableReplicas tests that SHOW RANGES does not return an
// error if it encounters an unavailable range. Moreover, crdb_internal.ranges
// includes the encountered error.
func TestShowRangesUnavailableReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numNodes = 3
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	kvserver.OverrideLeaderLeaseMetamorphism(ctx, &st.SV)
	tc := testcluster.StartTestCluster(
		// Manual replication will prevent the leaseholder for the unavailable range
		// from moving a different node.
		t, numNodes, base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Settings: st,
			},
		},
	)
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.replica_circuit_breaker.slow_replication_threshold='1s'`)
	sqlDB.Exec(t, `CREATE TABLE t (x INT PRIMARY KEY)`)
	// Split the table's range to have a better chance of moving some leaseholders
	// off of node 1 in the scatter below.
	sqlDB.Exec(t, `ALTER TABLE t SPLIT AT SELECT i FROM generate_series(0, 20) AS g(i)`)
	sqlDB.Exec(t, `ALTER TABLE t SCATTER`)

	// Server 0 includes the leaseholders for all system ranges, but the other two
	// are safe to stop to create some unavailable ranges that belong to table t.
	tc.StopServer(1)
	tc.StopServer(2)

	q := `SELECT range_id, lease_holder, range_size FROM [SHOW RANGES FROM TABLE t WITH DETAILS]`
	result := sqlDB.QueryStr(t, q)
	unavailableRangeID := ""
	// Iterate over the results to find an unavailable range.
	for _, row := range result {
		// crdb_internal.ranges powers the lease_holder and range_size fields in
		// SHOW RANGES. If a range is unavailable, the former returns NULL for both
		// fields but the latter converts the NULL leaseholder to 0.
		if row[1] == "0" {
			unavailableRangeID = row[0]
			require.Equal(t, "NULL", row[2])
			break
		}
	}
	// Ensure there it at least one unavailable range.
	require.NotEqual(t, "", unavailableRangeID)

	// crdb_internal.ranges also has an "errors" field that includes any errors
	// encountered while fetching the leaseholder and range stats. For the
	// unavailable range, we expect a "replica unavailable" error.
	q = fmt.Sprintf(`SELECT errors FROM crdb_internal.ranges WHERE range_id = %s`, unavailableRangeID)
	result = sqlDB.QueryStr(t, q)
	expectedError := fmt.Sprintf(
		"replica unavailable.*unable to serve request to r%s", unavailableRangeID,
	)
	require.Regexp(t, expectedError, result[0][0])
}
