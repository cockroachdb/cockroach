// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigtestcluster

import (
	"context"
	gosql "database/sql"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigreconciler"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsqltranslator"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Handle is a testing helper that lets users operate a multi-tenant test
// cluster while providing convenient, scoped access to each tenant's specific
// span config primitives. It's not safe for concurrent use.
type Handle struct {
	t       *testing.T
	tc      *testcluster.TestCluster
	ts      map[roachpb.TenantID]*Tenant
	scKnobs *spanconfig.TestingKnobs
}

// NewHandle returns a new Handle.
func NewHandle(
	t *testing.T, tc *testcluster.TestCluster, scKnobs *spanconfig.TestingKnobs,
) *Handle {
	return &Handle{
		t:       t,
		tc:      tc,
		ts:      make(map[roachpb.TenantID]*Tenant),
		scKnobs: scKnobs,
	}
}

// InitializeTenant initializes a tenant with the given ID, returning the
// relevant tenant state.
func (h *Handle) InitializeTenant(ctx context.Context, tenID roachpb.TenantID) *Tenant {
	testServer := h.tc.Server(0)
	tenantState := &Tenant{t: h.t}
	if tenID == roachpb.SystemTenantID {
		tenantState.TestTenantInterface = testServer
		tenantState.db = sqlutils.MakeSQLRunner(h.tc.ServerConn(0))
		tenantState.cleanup = func() {} // noop
	} else {
		serverGCJobKnobs := testServer.TestingKnobs().GCJob
		tenantGCJobKnobs := sql.GCJobTestingKnobs{SkipWaitingForMVCCGC: true}
		if serverGCJobKnobs != nil {
			tenantGCJobKnobs = *serverGCJobKnobs.(*sql.GCJobTestingKnobs)
			tenantGCJobKnobs.SkipWaitingForMVCCGC = true
		}
		tenantArgs := base.TestTenantArgs{
			TenantID: tenID,
			TestingKnobs: base.TestingKnobs{
				SpanConfig: h.scKnobs,
				GCJob:      &tenantGCJobKnobs,
			},
		}
		var err error
		tenantState.TestTenantInterface, err = testServer.StartTenant(ctx, tenantArgs)
		require.NoError(h.t, err)

		pgURL, cleanupPGUrl := sqlutils.PGUrl(h.t, tenantState.SQLAddr(), "Tenant", url.User(username.RootUser))
		tenantSQLDB, err := gosql.Open("postgres", pgURL.String())
		require.NoError(h.t, err)

		tenantState.db = sqlutils.MakeSQLRunner(tenantSQLDB)
		tenantState.cleanup = func() {
			require.NoError(h.t, tenantSQLDB.Close())
			cleanupPGUrl()
		}
	}

	var tenKnobs *spanconfig.TestingKnobs
	if scKnobs := tenantState.TestingKnobs().SpanConfig; scKnobs != nil {
		tenKnobs = scKnobs.(*spanconfig.TestingKnobs)
	}
	tenExecCfg := tenantState.ExecutorConfig().(sql.ExecutorConfig)
	tenKVAccessor := tenantState.SpanConfigKVAccessor().(spanconfig.KVAccessor)
	tenSQLTranslatorFactory := tenantState.SpanConfigSQLTranslatorFactory().(*spanconfigsqltranslator.Factory)
	tenSQLWatcher := tenantState.SpanConfigSQLWatcher().(spanconfig.SQLWatcher)
	tenSettings := tenantState.ClusterSettings()

	// TODO(irfansharif): We don't always care about these recordings -- should
	// it be optional?
	tenantState.recorder = spanconfigtestutils.NewKVAccessorRecorder(tenKVAccessor)
	tenantState.reconciler = spanconfigreconciler.New(
		tenSQLWatcher,
		tenSQLTranslatorFactory,
		tenantState.recorder,
		&tenExecCfg,
		tenExecCfg.Codec,
		tenID,
		tenSettings,
		tenKnobs,
	)

	h.ts[tenID] = tenantState
	return tenantState
}

// AllowSecondaryTenantToSetZoneConfigurations enables zone configuration
// support for the given tenant. Given the cluster setting involved is tenant
// read-only, the SQL statement is run as the system tenant.
func (h *Handle) AllowSecondaryTenantToSetZoneConfigurations(t *testing.T, tenID roachpb.TenantID) {
	_, found := h.LookupTenant(tenID)
	require.True(t, found)
	sqlDB := sqlutils.MakeSQLRunner(h.tc.ServerConn(0))
	sqlDB.Exec(
		t,
		"ALTER TENANT [$1] SET CLUSTER SETTING sql.zone_configs.allow_for_secondary_tenant.enabled = true",
		tenID.ToUint64(),
	)
}

// EnsureTenantCanSetZoneConfigurationsOrFatal ensures that the tenant observes
// a 'true' value for sql.zone_configs.allow_for_secondary_tenants.enabled. It
// fatals if this condition doesn't evaluate within SucceedsSoonDuration.
func (h *Handle) EnsureTenantCanSetZoneConfigurationsOrFatal(t *testing.T, tenant *Tenant) {
	testutils.SucceedsSoon(t, func() error {
		var val string
		tenant.QueryRow(
			"SHOW CLUSTER SETTING sql.zone_configs.allow_for_secondary_tenant.enabled",
		).Scan(&val)

		if val == "false" {
			return errors.New(
				"waiting for sql.zone_configs.allow_for_secondary_tenant.enabled to be updated",
			)
		}
		return nil
	})
}

// LookupTenant returns the relevant tenant state, if any.
func (h *Handle) LookupTenant(tenantID roachpb.TenantID) (_ *Tenant, found bool) {
	s, ok := h.ts[tenantID]
	return s, ok
}

// Tenants returns all available tenant states.
func (h *Handle) Tenants() []*Tenant {
	ts := make([]*Tenant, 0, len(h.ts))
	for _, tenantState := range h.ts {
		ts = append(ts, tenantState)
	}
	return ts
}

// Cleanup frees up internal resources.
func (h *Handle) Cleanup() {
	for _, tenantState := range h.ts {
		tenantState.cleanup()
	}
	h.ts = nil
}
