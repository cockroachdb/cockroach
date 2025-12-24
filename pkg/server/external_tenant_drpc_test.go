// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestExternalTenantInheritsDRPCSetting verifies that external tenants created
// via StartTenant() inherit the DRPC cluster setting from their parent server.
func TestExternalTenantInheritsDRPCSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "drpc-enabled", func(t *testing.T, drpcEnabled bool) {
		ctx := context.Background()

		// Set DefaultDRPCOption based on test parameter
		opt := base.TestDRPCDisabled
		if drpcEnabled {
			opt = base.TestDRPCEnabled
		}

		s := serverutils.StartServerOnly(t, base.TestServerArgs{
			DefaultDRPCOption: opt,
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		})
		defer s.Stopper().Stop(ctx)

		// Verify the system tenant has the expected DRPC setting
		var systemTenantDRPC bool
		sqlutils.MakeSQLRunner(s.SQLConn(t)).QueryRow(t,
			"SHOW CLUSTER SETTING rpc.experimental_drpc.enabled").Scan(&systemTenantDRPC)
		require.Equal(t, drpcEnabled, systemTenantDRPC,
			"system tenant should have DRPC=%v", drpcEnabled)

		// Start an external tenant and verify it has the expected DRPC setting
		tenantArgs := base.TestTenantArgs{
			TenantID:   roachpb.MustMakeTenantID(2),
			TenantName: "test-tenant",
		}
		tenant, err := s.TenantController().StartTenant(ctx, tenantArgs)
		require.NoError(t, err)

		var tenantDRPC bool
		sqlutils.MakeSQLRunner(tenant.SQLConn(t)).QueryRow(t,
			"SHOW CLUSTER SETTING rpc.experimental_drpc.enabled").Scan(&tenantDRPC)

		require.Equal(t, drpcEnabled, tenantDRPC,
			"external tenant should inherit DRPC=%v from parent server", drpcEnabled)
	})
}
