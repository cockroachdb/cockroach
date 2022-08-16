// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import (
	"context"
	gosql "database/sql"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
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

// SetSQLDBForUser sets the tenants' SQL runner to a PGURL connection using the
// passed in `user`. The method returns a function that resets the tenants' SQL
// runner to a PGURL connection for the root user.
func (h *Handle) SetSQLDBForUser(tenantID roachpb.TenantID, user string) func() {
	tenantState, ok := h.ts[tenantID]
	if !ok {
		h.t.Fatalf("tenant ID %d has not been initialized", tenantID)
	}

	resetToRootUser := func() {
		tenantState.curDB = tenantState.userToDB[username.RootUserName().Normalized()]
	}

	if runner, ok := tenantState.userToDB[user]; ok {
		tenantState.curDB = runner
		return resetToRootUser
	}

	pgURL, cleanup := sqlutils.PGUrl(h.t, h.tc.Server(0).ServingSQLAddr(),
		"TestBackupRestoreDataDriven", url.User(user))
	userSQLDB, err := gosql.Open("postgres", pgURL.String())
	require.NoError(h.t, err)
	tenantState.curDB = sqlutils.MakeSQLRunner(userSQLDB)
	tenantState.userToDB[user] = tenantState.curDB
	tenantState.cleanupFns = append(tenantState.cleanupFns, func() {
		require.NoError(h.t, userSQLDB.Close())
		cleanup()
	})

	return resetToRootUser
}

// InitializeTenant initializes a tenant with the given ID, returning the
// relevant tenant state.
func (h *Handle) InitializeTenant(ctx context.Context, tenID roachpb.TenantID) {
	testServer := h.tc.Server(0)
	tenantState := &Tenant{t: h.t, userToDB: make(map[string]*sqlutils.SQLRunner)}
	if tenID == roachpb.SystemTenantID {
		tenantState.TestTenantInterface = testServer
		pgURL, cleanupPGUrl := sqlutils.PGUrl(h.t, tenantState.SQLAddr(), "System",
			url.User(username.RootUser))
		userSQLDB, err := gosql.Open("postgres", pgURL.String())
		require.NoError(h.t, err)
		tenantState.curDB = sqlutils.MakeSQLRunner(userSQLDB)
		tenantState.userToDB[username.RootUserName().Normalized()] = tenantState.curDB
		tenantState.cleanupFns = append(tenantState.cleanupFns, func() {
			require.NoError(h.t, userSQLDB.Close())
			cleanupPGUrl()
		})
	} else {
		tenantArgs := base.TestTenantArgs{
			TenantID: tenID,
		}
		var err error
		tenantState.TestTenantInterface, err = testServer.StartTenant(ctx, tenantArgs)
		require.NoError(h.t, err)

		pgURL, cleanupPGUrl := sqlutils.PGUrl(h.t, tenantState.SQLAddr(), "Tenant", url.User(username.RootUser))
		tenantSQLDB, err := gosql.Open("postgres", pgURL.String())
		require.NoError(h.t, err)

		tenantState.curDB = sqlutils.MakeSQLRunner(tenantSQLDB)
		tenantState.userToDB[username.RootUserName().Normalized()] = tenantState.curDB
		tenantState.cleanupFns = append(tenantState.cleanupFns, func() {
			require.NoError(h.t, tenantSQLDB.Close())
			cleanupPGUrl()
		})
	}

	h.ts[tenID] = tenantState
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
		for _, cleanup := range tenantState.cleanupFns {
			cleanup()
		}
	}
	h.ts = nil
}
