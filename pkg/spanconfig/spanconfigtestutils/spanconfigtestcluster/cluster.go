// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigtestcluster

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigreconciler"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsqltranslator"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
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
	sysDB   *sqlutils.SQLRunner
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
		sysDB:   sqlutils.MakeSQLRunner(tc.Server(0).SystemLayer().SQLConn(t)),
	}
}

// InitializeTenant initializes a tenant with the given ID, returning the
// relevant tenant state.
func (h *Handle) InitializeTenant(ctx context.Context, tenID roachpb.TenantID) *Tenant {
	testServer := h.tc.Server(0)
	tenantState := &Tenant{t: h.t}
	if tenID == roachpb.SystemTenantID {
		tenantState.ApplicationLayerInterface = testServer.SystemLayer()
		tenantState.db = h.sysDB
		tenantState.cleanup = func() {} // noop
	} else {
		serverGCJobKnobs := testServer.SystemLayer().TestingKnobs().GCJob
		// Copy the GC job knobs from the server to the tenant.
		tenantGCJobKnobs := sql.GCJobTestingKnobs{}
		if serverGCJobKnobs != nil {
			tenantGCJobKnobs = *serverGCJobKnobs.(*sql.GCJobTestingKnobs)
		}
		tenantArgs := base.TestTenantArgs{
			TenantID: tenID,
			TestingKnobs: base.TestingKnobs{
				SpanConfig: h.scKnobs,
				GCJob:      &tenantGCJobKnobs,
			},
		}
		var err error
		tenantState.ApplicationLayerInterface, err = testServer.TenantController().StartTenant(ctx, tenantArgs)
		require.NoError(h.t, err)

		tenantSQLDB := tenantState.SQLConn(h.t)
		tenantSQLDB.SetMaxOpenConns(1)

		tenantState.db = sqlutils.MakeSQLRunner(tenantSQLDB)
		tenantState.cleanup = func() {}
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
