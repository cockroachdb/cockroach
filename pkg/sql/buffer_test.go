// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestConcurrentScanBufferNodesOnSpilledBuffer is a regression test for the
// "pebble: batch already committing" node crash (#172716): multiple
// scanBufferNodes reading the same spilled bufferNode used to create their
// iterators concurrently (when running as inputs of a parallel unordered
// synchronizer or as parts of concurrent FK check plans), racing on the flush
// of the shared row container's pebble write batch.
func TestConcurrentScanBufferNodesOnSpilledBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numNodes = 3
	ctx := context.Background()
	tc := serverutils.StartCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			UseDatabase: "test",
			Knobs: base.TestingKnobs{
				DistSQL: &execinfra.TestingKnobs{
					// Force the row buffers (and everything else) to spill to
					// disk so that scanBufferNodes hit the DiskRowContainer
					// iterator path.
					ForceDiskSpill: true,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)
	s := tc.ApplicationLayer(0)

	// EXPERIMENTAL_RELOCATE below requires the can_admin_relocate_range
	// capability.
	if tc.DefaultTenantDeploymentMode().IsExternal() {
		tc.GrantTenantCapabilities(ctx, t, serverutils.TestTenantID(),
			map[tenantcapabilitiespb.ID]string{tenantcapabilitiespb.CanAdminRelocateRange: "true"})
	}

	// Create a table with 30 rows split into 3 ranges with each node having
	// one so that the plan can be distributed.
	db := s.SQLConn(t, serverutils.DBName("test"))
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlutils.CreateTable(
		t, db, "foo",
		"k INT PRIMARY KEY, v INT, extra INT",
		30,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowModuloFn(2), sqlutils.RowModuloFn(3)),
	)
	sqlDB.Exec(t, "CREATE INDEX vidx ON test.foo (v)")
	sqlDB.Exec(t, "ALTER TABLE test.foo SPLIT AT VALUES (10), (20)")
	sqlDB.Exec(
		t,
		fmt.Sprintf("ALTER TABLE test.foo EXPERIMENTAL_RELOCATE VALUES (ARRAY[%d], 0), (ARRAY[%d], 10), (ARRAY[%d], 20)",
			tc.Server(0).GetFirstStoreID(),
			tc.Server(1).GetFirstStoreID(),
			tc.Server(2).GetFirstStoreID(),
		),
	)

	for name, testCase := range map[string]struct {
		// setup runs once before the query loop (e.g. to create the tables
		// referenced by a mutation).
		setup []string
		query string
	}{
		"lookup join": {
			query: `WITH w AS MATERIALIZED (SELECT k, v FROM test.foo)
SELECT t.k FROM w INNER LOOKUP JOIN test.foo AS t ON w.k = t.k
UNION
SELECT t.k FROM w INNER LOOKUP JOIN test.foo AS t ON w.v = t.k`,
		},
		"index join": {
			query: `WITH w AS MATERIALIZED (SELECT k, v FROM test.foo)
SELECT k FROM w
UNION
SELECT v FROM w
UNION
SELECT extra FROM test.foo@vidx WHERE v = 1`,
		},
		// A mutation with multiple FK checks runs the checks as separate
		// concurrent plans that each scan the buffered mutation input, so
		// the scanBufferNodes on the same buffer run concurrently across
		// the check plans.
		"parallel FK checks": {
			setup: []string{
				"CREATE TABLE p1 (id INT PRIMARY KEY)",
				"CREATE TABLE p2 (id INT PRIMARY KEY)",
				"INSERT INTO p1 SELECT generate_series(1, 30)",
				"INSERT INTO p2 SELECT generate_series(0, 1)",
				"CREATE TABLE c (a INT REFERENCES p1(id), b INT REFERENCES p2(id))",
			},
			query: `INSERT INTO test.c SELECT k, v FROM test.foo`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			for _, stmt := range testCase.setup {
				sqlDB.Exec(t, stmt)
			}
			// Run the query repeatedly to give the race a chance to be hit.
			for range 30 {
				sqlDB.Exec(t, testCase.query)
			}
		})
	}
}
