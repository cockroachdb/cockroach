// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/obs/ash"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestASHMultiTenantIsolation verifies that secondary tenants can only
// see their own ASH samples, not samples from other tenants or the
// system tenant. Regression test for #166456.
func TestASHMultiTenantIsolation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Reset the global ASH sampler so that the server started below gets
	// a fresh sampler goroutine. Without this, a previous test in the
	// package may have consumed the sync.Once and then stopped the
	// sampler goroutine when its server was torn down.
	ash.ResetGlobalSamplerForTesting()

	ctx := context.Background()
	srv, systemDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer srv.Stopper().Stop(ctx)

	systemSQL := sqlutils.MakeSQLRunner(systemDB)
	systemSQL.Exec(t, `SET CLUSTER SETTING obs.ash.enabled = true`)
	systemSQL.Exec(t, `SET CLUSTER SETTING obs.ash.sample_interval = '10ms'`)

	_, tenant1DB := serverutils.StartSharedProcessTenant(t, srv,
		base.TestSharedProcessTenantArgs{
			TenantName: "tenant-one",
			TenantID:   serverutils.TestTenantID(),
		})
	tenant1SQL := sqlutils.MakeSQLRunner(tenant1DB)
	tenant1ID := serverutils.TestTenantID()

	_, tenant2DB := serverutils.StartSharedProcessTenant(t, srv,
		base.TestSharedProcessTenantArgs{
			TenantName: "tenant-two",
			TenantID:   serverutils.TestTenantID2(),
		})
	tenant2SQL := sqlutils.MakeSQLRunner(tenant2DB)
	tenant2ID := serverutils.TestTenantID2()

	// Generate work on each tenant with distinct app names.
	tenant1SQL.Exec(t, `SET application_name = 'tenant_one_app'`)
	tenant1SQL.Exec(t, `CREATE TABLE t1 (id INT PRIMARY KEY, data STRING)`)
	tenant1SQL.Exec(t,
		`INSERT INTO t1 SELECT i, repeat('a', 100) FROM generate_series(1, 5000) AS g(i)`)

	tenant2SQL.Exec(t, `SET application_name = 'tenant_two_app'`)
	tenant2SQL.Exec(t, `CREATE TABLE t2 (id INT PRIMARY KEY, data STRING)`)
	tenant2SQL.Exec(t,
		`INSERT INTO t2 SELECT i, repeat('b', 100) FROM generate_series(1, 5000) AS g(i)`)

	systemSQL.Exec(t, `SET application_name = 'system_tenant_app'`)
	systemSQL.Exec(t, `CREATE TABLE sys_t (id INT PRIMARY KEY, data STRING)`)
	systemSQL.Exec(t,
		`INSERT INTO sys_t SELECT i, repeat('c', 100) FROM generate_series(1, 5000) AS g(i)`)

	// Wait for each tenant to have at least one ASH sample so that
	// subsequent assertions are not vacuously true.
	testutils.SucceedsSoon(t, func() error {
		tenant1SQL.Exec(t, `SELECT count(*) FROM t1 WHERE data LIKE '%a%'`)
		tenant2SQL.Exec(t, `SELECT count(*) FROM t2 WHERE data LIKE '%b%'`)
		systemSQL.Exec(t, `SELECT count(*) FROM sys_t WHERE data LIKE '%c%'`)

		for _, r := range []struct {
			runner *sqlutils.SQLRunner
			name   string
		}{
			{tenant1SQL, "tenant 1"},
			{tenant2SQL, "tenant 2"},
			{systemSQL, "system"},
		} {
			var count int
			r.runner.QueryRow(t,
				`SELECT count(*) FROM crdb_internal.node_active_session_history`,
			).Scan(&count)
			if count == 0 {
				return errors.Newf("no ASH samples for %s yet", r.name)
			}
		}
		return nil
	})

	t.Run("tenant isolation filtering", func(t *testing.T) {
		t1Rows := tenant1SQL.QueryStr(t, `
			SELECT DISTINCT tenant_id
			FROM crdb_internal.node_active_session_history`)
		require.NotEmpty(t, t1Rows,
			"tenant 1 should have ASH samples")
		for _, row := range t1Rows {
			require.Equal(t, fmt.Sprint(tenant1ID.ToUint64()), row[0],
				"tenant 1 should only see its own tenant_id")
		}

		t2Rows := tenant2SQL.QueryStr(t, `
			SELECT DISTINCT tenant_id
			FROM crdb_internal.node_active_session_history`)
		require.NotEmpty(t, t2Rows,
			"tenant 2 should have ASH samples")
		for _, row := range t2Rows {
			require.Equal(t, fmt.Sprint(tenant2ID.ToUint64()), row[0],
				"tenant 2 should only see its own tenant_id")
		}
	})

	t.Run("tenants cannot see others app_names", func(t *testing.T) {
		var crossCount int
		tenant1SQL.QueryRow(t, `
			SELECT count(*)
			FROM crdb_internal.node_active_session_history
			WHERE app_name = 'tenant_two_app'`,
		).Scan(&crossCount)
		require.Zero(t, crossCount,
			"tenant 1 should not see tenant 2's app_name in ASH")

		tenant2SQL.QueryRow(t, `
			SELECT count(*)
			FROM crdb_internal.node_active_session_history
			WHERE app_name = 'tenant_one_app'`,
		).Scan(&crossCount)
		require.Zero(t, crossCount,
			"tenant 2 should not see tenant 1's app_name in ASH")
	})
}

// TestASHVirtualTableAccessControl verifies that unprivileged users
// cannot read the ASH virtual tables and that VIEWACTIVITY or
// VIEWACTIVITYREDACTED grants access.
func TestASHVirtualTableAccessControl(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	adminDB := sqlutils.MakeSQLRunner(db)
	adminDB.Exec(t, `SET CLUSTER SETTING obs.ash.enabled = true`)

	adminDB.Exec(t, `CREATE USER testuser`)
	adminDB.Exec(t, `CREATE USER testuser2`)
	adminDB.Exec(t, `GRANT SYSTEM VIEWACTIVITY TO testuser2`)
	adminDB.Exec(t, `CREATE USER testuser3`)
	adminDB.Exec(t, `GRANT SYSTEM VIEWACTIVITYREDACTED TO testuser3`)

	for _, table := range []string{
		"crdb_internal.node_active_session_history",
		"crdb_internal.cluster_active_session_history",
	} {
		t.Run(table, func(t *testing.T) {
			t.Run("no privilege", func(t *testing.T) {
				noPrivDB := srv.ApplicationLayer().SQLConn(t, serverutils.User("testuser"))
				_, err := noPrivDB.Query(fmt.Sprintf(`SELECT count(*) FROM %s`, table))
				require.Error(t, err)
				require.Contains(t, err.Error(),
					"does not have VIEWACTIVITY or VIEWACTIVITYREDACTED privilege")
			})

			t.Run("VIEWACTIVITY", func(t *testing.T) {
				vaDB := srv.ApplicationLayer().SQLConn(t, serverutils.User("testuser2"))
				var count int
				err := vaDB.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %s`, table)).Scan(&count)
				require.NoError(t, err, "VIEWACTIVITY should allow reading %s", table)
			})

			t.Run("VIEWACTIVITYREDACTED", func(t *testing.T) {
				varDB := srv.ApplicationLayer().SQLConn(t, serverutils.User("testuser3"))
				var count int
				err := varDB.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %s`, table)).Scan(&count)
				require.NoError(t, err, "VIEWACTIVITYREDACTED should allow reading %s", table)
			})
		})
	}
}
