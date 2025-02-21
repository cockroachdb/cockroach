// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colflow_test

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/stretchr/testify/require"
)

// TestDrainingAfterRemoteError verifies that the draining is fully completed if
// an error occurs on a remote node. The verification is done by checking that
// the trace from a distributed query contains spans of the remote processors.
func TestDrainingAfterRemoteError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	// Create a disk monitor for the temp storage only with 1 byte of space.
	// This ensures that the query will run into "out of temporary storage"
	// error.
	diskMonitor := mon.NewMonitor(mon.Options{
		Name:     mon.MakeMonitorName("test-disk"),
		Res:      mon.DiskResource,
		Settings: st,
	})
	diskMonitor.Start(ctx, nil /* pool */, mon.NewStandaloneBudget(1))

	// Set up a two node cluster.
	tempStorageConfig := base.TempStorageConfig{InMemory: true, Mon: diskMonitor, Settings: st, Spec: base.DefaultTestStoreSpec}
	args := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings:          st,
			TempStorageConfig: tempStorageConfig,
		},
		ReplicationMode: base.ReplicationManual,
	}
	tc := testcluster.StartTestCluster(t, 2 /* nodes */, args)
	defer tc.Stopper().Stop(ctx)

	if tc.DefaultTenantDeploymentMode().IsExternal() {
		tc.GrantTenantCapabilities(
			ctx, t, serverutils.TestTenantID(),
			map[tenantcapabilities.ID]string{tenantcapabilities.CanAdminRelocateRange: "true"})
	}

	// Create two tables, one with small values, and another with large rows.
	// Relocate the range for the small table to node 2.
	conn := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(conn)
	sqlDB.Exec(t, "CREATE TABLE small (k INT PRIMARY KEY);")
	sqlDB.Exec(t, "INSERT INTO small SELECT generate_series(1, 100);")
	sqlDB.Exec(t, "ANALYZE small;")
	sqlDB.Exec(t, "ALTER TABLE small EXPERIMENTAL_RELOCATE VALUES (ARRAY[2], 2)")
	sqlDB.Exec(t, "SELECT count(*) FROM small;")
	sqlDB.Exec(t, "CREATE TABLE large (k INT PRIMARY KEY, v STRING);")
	sqlDB.Exec(t, "INSERT INTO large SELECT generate_series(1, 100), repeat('a', 100000);")
	sqlDB.Exec(t, "ANALYZE large;")

	// Make sure that the query is fully distributed (i.e. all execution happens
	// on node 2).
	sqlDB.Exec(t, "SET distsql = always;")

	// Sanity check that, indeed, node 2 is part of the physical plan.
	rows, err := conn.Query("EXPLAIN (VEC) SELECT sum(length(v)) FROM large, small WHERE small.k = large.k GROUP BY large.k;")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()
	var foundNode2 bool
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		if strings.Contains(line, "Node 2") {
			foundNode2 = true
		}
	}
	require.True(t, foundNode2, "expected that most of the work is done on node 2")

	// Lower the workmem setting so that the join reader gets a memory error
	// first, followed by the temp disk storage error.
	sqlDB.Exec(t, "SET distsql_workmem = '8MiB';")
	// Enable the tracing since we'll check that it contains spans for the join
	// reader.
	sqlDB.Exec(t, "SET tracing = on;")

	// Perform a query that uses the join reader when ordering has to be
	// maintained. Ensure that it encounters the error that we expect.
	sqlDB.ExpectErr(t, ".*joinreader-disk.*", "SELECT sum(length(v)) FROM large, small WHERE small.k = large.k GROUP BY large.k;")
	sqlDB.Exec(t, "SET tracing = off;")

	// Now, the crux of the test - verify that the spans for the join reader on
	// the remote node have been imported into the trace on the gateway. If we
	// see no such spans, then the draining wasn't fully performed.
	row := conn.QueryRow(`SELECT count(*) FROM [SHOW TRACE FOR SESSION] WHERE operation = 'join reader'`)
	var count int
	require.NoError(t, row.Scan(&count))
	require.True(t, count > 0, "expected to find some spans for join reader in the trace")
}
