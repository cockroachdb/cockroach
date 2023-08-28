// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerController(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	sc := s.TenantController().ServerController().(*serverController)
	ts := s.SystemLayer().(*testServer)

	d, err := sc.getServer(ctx, "system")
	require.NoError(t, err)
	if d.(*systemServerWrapper).server != ts.topLevelServer {
		t.Fatal("expected wrapped system server")
	}

	d, err = sc.getServer(ctx, "somename")
	require.Nil(t, d)
	require.Error(t, err, `no tenant found with name "somename"`)

	_, err = db.Exec("CREATE TENANT hello; ALTER TENANT hello START SERVICE SHARED")
	require.NoError(t, err)

	_, err = sc.getServer(ctx, "hello")
	// TODO(knz): We're not really expecting an error here.
	// The actual error seen will exist as long as in-memory
	// servers use the standard KV connector.
	//
	// To make this error go away, we need either to place
	// this test in a separate CCL package, or to make these servers
	// use a new non-CCL connector.
	//
	// However, none of this is necessary to test the
	// controller itself: it's sufficient to see that the
	// tenant constructor was called.
	require.Error(t, err, "tenant connector requires a CCL binary")
	// TODO(knz): test something about d.
}

func TestSQLErrorUponInvalidTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	db, err := s.SystemLayer().SQLConnE("cluster:nonexistent")
	// Expect no error yet: the connection is opened lazily; an
	// error here means the parameters were incorrect.
	require.NoError(t, err)

	err = db.Ping()
	require.NotNil(t, err)
	require.Regexp(t, `service unavailable for target tenant \(nonexistent\)`, err.Error())
}

func TestSharedProcessServerInheritsTempStorageLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const specialSize = 123123123

	// Start a server with a custom temp storage limit.
	ctx := context.Background()
	st := cluster.MakeClusterSettings()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Settings:          st,
		TempStorageConfig: base.DefaultTestTempStorageConfigWithSize(st, specialSize),
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	// Start a shared process tenant server.
	ts, _, err := s.StartSharedProcessTenant(ctx, base.TestSharedProcessTenantArgs{
		TenantName: "hello",
	})
	require.NoError(t, err)

	tss := ts.(*testTenant)
	require.Equal(t, int64(specialSize), tss.SQLCfg.TempStorageConfig.Mon.Limit())
}

// TestServerSQLConn checks that the SQLConn() method on the
// SystemLayer() of TestServerInterface works even when non-specific
// SQL connection requests are redirected to a secondary tenant.
func TestServerSQLConn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)
	systemTenant := s.SystemLayer()

	// Start some secondary tenant servers.
	secondaryTenantExtNoName, err := s.StartTenant(ctx, base.TestTenantArgs{
		TenantID: roachpb.MustMakeTenantID(2),
	})
	require.NoError(t, err)

	secondaryTenantExtNamed, err := s.StartTenant(ctx, base.TestTenantArgs{
		TenantName: "hello",
		TenantID:   roachpb.MustMakeTenantID(10),
	})
	require.NoError(t, err)

	secondaryTenantSh, _, err := s.StartSharedProcessTenant(ctx, base.TestSharedProcessTenantArgs{
		TenantName: "world",
	})
	require.NoError(t, err)
	multitenant.DefaultTenantSelect.Override(ctx, &systemTenant.ClusterSettings().SV, "world")

	for _, tc := range []struct {
		testName     string
		tbName       string
		sqlInterface serverutils.ApplicationLayerInterface
	}{
		{"system", "foo", systemTenant},
		{"secondary-external-noname", "bar", secondaryTenantExtNoName},
		{"secondary-external-named", "baz", secondaryTenantExtNamed},
		{"secondary-shared", "qux", secondaryTenantSh},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			_, err = tc.sqlInterface.InternalExecutor().(isql.Executor).
				Exec(ctx, "create-table", nil, "CREATE TABLE defaultdb."+tc.tbName+" (i INT)")
			require.NoError(t, err)

			conn := tc.sqlInterface.SQLConn(t, "defaultdb")
			var unused int
			assert.NoError(t, conn.QueryRowContext(ctx, "SELECT count(*) FROM "+tc.tbName).Scan(&unused))
		})
	}
}
