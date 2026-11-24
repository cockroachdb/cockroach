// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestExternalTenantDRPCPropagation verifies that the DRPC setting from the
// parent server's DefaultDRPCOption is propagated to external tenants started
// explicitly via StartTenant(). DRPC is a test infrastructure concern that
// should apply uniformly to all tenants regardless of how they are started.
func TestExternalTenantDRPCPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "drpc-enabled", func(t *testing.T, drpcEnabled bool) {
		ctx := context.Background()

		opt := base.TestDRPCDisabled
		if drpcEnabled {
			opt = base.TestDRPCEnabled
		}

		s := serverutils.StartServerOnly(t, base.TestServerArgs{
			DefaultDRPCOption: opt,
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		})
		defer s.Stopper().Stop(ctx)

		// Start an external tenant explicitly without providing Settings.
		// The DRPC config should still be propagated from the parent.
		tenantArgs := base.TestTenantArgs{
			TenantID: serverutils.TestTenantID(),
		}
		tenant, err := s.TenantController().StartTenant(ctx, tenantArgs)
		require.NoError(t, err)

		require.Equal(t, drpcEnabled, tenant.RPCContext().UseDRPC,
			"external tenant should have UseDRPC=%v matching parent's DefaultDRPCOption", drpcEnabled)
	})
}
