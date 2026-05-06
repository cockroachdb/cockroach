// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rgreconciler_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestSystemTenantEndToEnd verifies that on the system tenant a CREATE
// RESOURCE GROUP propagates from system.resource_groups to
// system.tenant_resource_groups via the singleton reconciliation job,
// and likewise a DROP propagates as a delete.
func TestSystemTenantEndToEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(s.SQLConn(t))
	sqlDB.Exec(t, "SET CLUSTER SETTING sql.experimental_resource_groups.enabled = true")

	var jobCount int
	sqlDB.QueryRow(t,
		`SELECT count(*) FROM system.jobs WHERE id = $1`,
		int64(jobs.ResourceGroupReconciliationJobID),
	).Scan(&jobCount)
	require.Equal(t, 1, jobCount)

	sqlDB.Exec(t, "CREATE RESOURCE GROUP rg_e2e WITH cpu_weight = 100, max_cpu = true")
	testutils.SucceedsSoon(t, func() error {
		var n int
		sqlDB.QueryRow(t,
			`SELECT count(*) FROM system.tenant_resource_groups WHERE name = 'rg_e2e' AND tenant_id = 1`,
		).Scan(&n)
		if n == 0 {
			return errors.New("rg_e2e row not yet observed in system.tenant_resource_groups")
		}
		return nil
	})

	sqlDB.Exec(t, "DROP RESOURCE GROUP rg_e2e")
	testutils.SucceedsSoon(t, func() error {
		var n int
		sqlDB.QueryRow(t,
			`SELECT count(*) FROM system.tenant_resource_groups WHERE name = 'rg_e2e'`,
		).Scan(&n)
		if n != 0 {
			return errors.Newf("rg_e2e still present (n=%d)", n)
		}
		return nil
	})
}

// TestAppTenantEndToEnd verifies the same propagation on an application
// tenant: the connector pusher round-trips a CREATE / DROP via the
// UpdateResourceGroups RPC to the host's system.tenant_resource_groups,
// scoped to the right tenant_id.
func TestAppTenantEndToEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	tenantID := serverutils.TestTenantID()
	tenant, err := s.TenantController().StartTenant(ctx, base.TestTenantArgs{
		TenantID:   tenantID,
		TenantName: "tenant",
	})
	require.NoError(t, err)

	hostDB := sqlutils.MakeSQLRunner(s.SQLConn(t))
	tenantDB := sqlutils.MakeSQLRunner(tenant.SQLConn(t))
	tenantDB.Exec(t, "SET CLUSTER SETTING sql.experimental_resource_groups.enabled = true")

	tenantDB.Exec(t, "CREATE RESOURCE GROUP rg_app WITH cpu_weight = 50, max_cpu = false")
	testutils.SucceedsSoon(t, func() error {
		var n int
		hostDB.QueryRow(t,
			`SELECT count(*) FROM system.tenant_resource_groups WHERE name = 'rg_app' AND tenant_id = $1`,
			tenantID.ToUint64(),
		).Scan(&n)
		if n == 0 {
			return errors.New("rg_app row not yet observed in host's system.tenant_resource_groups")
		}
		return nil
	})

	tenantDB.Exec(t, "DROP RESOURCE GROUP rg_app")
	testutils.SucceedsSoon(t, func() error {
		var n int
		hostDB.QueryRow(t,
			`SELECT count(*) FROM system.tenant_resource_groups WHERE name = 'rg_app' AND tenant_id = $1`,
			tenantID.ToUint64(),
		).Scan(&n)
		if n != 0 {
			return errors.Newf("rg_app still present (n=%d)", n)
		}
		return nil
	})
}
