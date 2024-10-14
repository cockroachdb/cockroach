// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestDropTenantSynchronous tests that destroying a tenant synchronously
// with DROP TENANT IMMEDIATE works.
func TestDropTenantSynchronous(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	t.Run("latest version", func(t *testing.T) {
		s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(ctx)
		testDestroyTenantSynchronous(ctx, t, sqlDB, kvDB)
	})

	t.Run("mixed version", func(t *testing.T) {
		clusterArgs := base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						DisableAutomaticVersionUpgrade: make(chan struct{}),
						ClusterVersionOverride:         clusterversion.MinSupported.Version(),
					},
				},
			},
		}
		var (
			tc    = testcluster.StartTestCluster(t, 1, clusterArgs)
			s     = tc.Server(0)
			sqlDB = tc.ServerConn(0)
		)
		defer tc.Stopper().Stop(ctx)
		testDestroyTenantSynchronous(ctx, t, sqlDB, s.DB())
	})
}

func testDestroyTenantSynchronous(ctx context.Context, t *testing.T, sqlDB *gosql.DB, kvDB *kv.DB) {
	tenantID := roachpb.MustMakeTenantID(10)
	codec := keys.MakeSQLCodec(tenantID)
	const tenantStateQuery = `
SELECT id, active FROM system.tenants WHERE id = 10
`
	tenantSpan := codec.TenantSpan()
	checkKVsExistForTenant := func(t *testing.T, shouldExist bool) {
		rows, err := kvDB.Scan(ctx, tenantSpan.Key, tenantSpan.EndKey, 1 /* maxRows */)
		require.NoError(t, err)
		require.Equal(t, shouldExist, len(rows) > 0)
	}

	tdb := sqlutils.MakeSQLRunner(sqlDB)

	// Create the tenant, make sure it has data and state.
	tdb.Exec(t, "SELECT crdb_internal.create_tenant(10)")
	tdb.CheckQueryResults(t, tenantStateQuery, [][]string{{"10", "true"}})
	checkKVsExistForTenant(t, true /* shouldExist */)

	// Destroy the tenant, make sure it does not have data and state.
	tdb.Exec(t, "ALTER TENANT [10] STOP SERVICE")
	tdb.Exec(t, "DROP TENANT [10] IMMEDIATE")
	tdb.CheckQueryResults(t, tenantStateQuery, [][]string{})
	checkKVsExistForTenant(t, false /* shouldExist */)
}

func TestGetTenantIds(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	idb := s.ExecutorConfig().(sql.ExecutorConfig).InternalDB
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	// Create 2 tenants in addition to the system tenant.
	tdb.Exec(t, "CREATE TENANT t1")
	tdb.Exec(t, "CREATE TENANT t2")

	var ids []roachpb.TenantID
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
		ids, err = sql.GetAllNonDropTenantIDs(ctx, txn, s.ClusterSettings())
		return err
	}))
	expectedIds := []roachpb.TenantID{
		roachpb.MustMakeTenantID(1),
		roachpb.MustMakeTenantID(3),
		roachpb.MustMakeTenantID(4),
	}
	require.Equal(t, expectedIds, ids)

	// Drop tenant 2.
	tdb.Exec(t, "DROP TENANT t1")

	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
		ids, err = sql.GetAllNonDropTenantIDs(ctx, txn, s.ClusterSettings())
		return err
	}))
	expectedIds = []roachpb.TenantID{
		roachpb.MustMakeTenantID(1),
		roachpb.MustMakeTenantID(4),
	}
	require.Equal(t, expectedIds, ids)
}
