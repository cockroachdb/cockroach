// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvtenant_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/load"
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

	ts, hostDB, _ := serverutils.StartServer(t,
		base.TestServerArgs{
			InsecureWebAccess: true,
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					// We disable the split queue as an untimely split can cause the QPS
					// stat to be split over multiple ranges for the tenant.
					DisableSplitQueue: true,
				},
			},
		})
	defer ts.Stopper().Stop(ctx)

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
	sqlDB := sqlutils.MakeSQLRunner(hostDB)

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
	// NB: We call directly into the load tracking struct, in order to avoid
	// flakes due to timing differences affecting the result
	loadStats := repl.GetLoadStatsForTesting()
	qpsBefore := loadStats.TestingGetSum(load.Queries)
	for i := 0; i < 110; i++ {
		r.Exec(t, `SELECT k FROM foo.qps_test`)
	}
	qpsAfter := loadStats.TestingGetSum(load.Queries)
	queriesIncrease := int(qpsAfter - qpsBefore)
	// If queries are correctly recorded, we should see increase in query count by
	// 110. As it is possible due to rounding and conversion from QPS to query count
	// to get a slightly higher or lower number - we expect the increase to be at
	// least 100.
	require.Greater(t, queriesIncrease, 100)
}
