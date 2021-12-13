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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigreconciler"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// Handle is a testing helper that lets users operate a multi-tenant test
// cluster while providing convenient, scoped access to each tenant's specific
// span config primitives. It's not safe for concurrent use.
type Handle struct {
	t  *testing.T
	tc *testcluster.TestCluster
	ts map[roachpb.TenantID]*Tenant
}

// NewHandle returns a new Handle.
func NewHandle(t *testing.T, tc *testcluster.TestCluster) *Handle {
	return &Handle{
		t:  t,
		tc: tc,
		ts: make(map[roachpb.TenantID]*Tenant),
	}
}

// InitializeTenant initializes a tenant with the given ID, returning the
// relevant tenant state.
func (h *Handle) InitializeTenant(ctx context.Context, tenID roachpb.TenantID) *Tenant {
	// Prevent a logging assertion that the server ID is initialized multiple
	// times.
	log.TestingClearServerIdentifiers()

	testServer := h.tc.Server(0)
	tenantState := &Tenant{t: h.t}
	if tenID == roachpb.SystemTenantID {
		tenantState.TestTenantInterface = testServer
		tenantState.db = sqlutils.MakeSQLRunner(h.tc.ServerConn(0))
		tenantState.cleanup = func() {} // noop
	} else {
		tenantArgs := base.TestTenantArgs{TenantID: tenID}
		var err error
		tenantState.TestTenantInterface, err = testServer.StartTenant(ctx, tenantArgs)
		require.NoError(h.t, err)

		pgURL, cleanupPGUrl := sqlutils.PGUrl(h.t, tenantState.SQLAddr(), "Tenant", url.User(security.RootUser))
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
	tenSQLTranslator := tenantState.SpanConfigSQLTranslator().(spanconfig.SQLTranslator)
	tenSQLWatcher := tenantState.SpanConfigSQLWatcher().(spanconfig.SQLWatcher)

	// TODO(irfansharif): We don't always care about these recordings -- should
	// it be optional?
	tenantState.recorder = spanconfigtestutils.NewKVAccessorRecorder(tenKVAccessor)
	tenantState.reconciler = spanconfigreconciler.New(
		tenSQLWatcher,
		tenSQLTranslator,
		tenantState.recorder,
		&tenExecCfg,
		tenExecCfg.Codec,
		tenID,
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
