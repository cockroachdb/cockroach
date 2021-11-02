// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvtenantccl

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestTenantRangeQPSStat verifies that queries on a tenant range are
// reflected in the range's QPS.
func TestTenantRangeQPSStat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DisableWebSessionAuthentication: true,
		},
	})
	defer tc.Stopper().Stop(ctx)
	ts := tc.Server(0)

	_, db := serverutils.StartTenant(
		t, ts, base.TestTenantArgs{TenantID: serverutils.TestTenantID()},
	)
	defer db.Close()

	// Tenant connection.
	r := sqlutils.MakeSQLRunner(db)
	r.Exec(t, `CREATE DATABASE foo`)
	r.Exec(t, `CREATE TABLE foo.qps_test (k STRING PRIMARY KEY)`)
	r.Exec(t, `INSERT INTO foo.qps_test VALUES('abc')`)

	// Host connection.
	conn := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(conn)

	var rangeID int
	stmt := fmt.Sprintf(
		"SELECT range_id FROM crdb_internal.ranges WHERE start_pretty='/Tenant/%s'",
		serverutils.TestTenantID(),
	)
	sqlDB.QueryRow(t, stmt).Scan(&rangeID)
	require.NotEqualf(t, 0, rangeID, "Unable to determine test table range id")

	store, err := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
	require.NoError(t, err)
	repl, err := store.GetReplica(roachpb.RangeID(rangeID))
	require.NoError(t, err)

	qpsBefore, durationBefore := repl.QueriesPerSecond()
	queriesBefore := qpsBefore * durationBefore.Seconds()
	for i := 0; i < 110; i++ {
		r.Exec(t, `SELECT k FROM foo.qps_test`)
	}
	qpsAfter, durationAfter := repl.QueriesPerSecond()
	queriesAfter := qpsAfter * durationAfter.Seconds()
	queriesIncrease := int(queriesAfter - queriesBefore)
	// If queries are correctly recorded, we should see increase in query count by
	// 110. As it is possible due to rounding and conversion from QPS to query count
	// to get a slightly higher or lower number - we expect the increase to be at
	// least 100.
	require.Greater(t, queriesIncrease, 100)
}
