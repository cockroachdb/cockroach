// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverccl

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSharedProcessTenantNoSpanLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := serverutils.StartCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		}})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	_, err := db.Exec("CREATE TENANT hello")
	require.NoError(t, err)

	_, err = db.Exec("ALTER TENANT hello START SERVICE SHARED")
	require.NoError(t, err)

	_, err = db.Exec("SET CLUSTER SETTING spanconfig.virtual_cluster.max_spans = 1000")
	require.NoError(t, err)

	var tenantDB *gosql.DB
	testutils.SucceedsSoon(t, func() error {
		var err error
		tenantDB, err = tc.Server(0).SystemLayer().SQLConnE(serverutils.DBName("cluster:hello"))
		if err != nil {
			return err
		}

		if err := tenantDB.Ping(); err != nil {
			return err
		}
		return nil
	})
	defer tenantDB.Close()

	_, err = tenantDB.Exec("SELECT crdb_internal.generate_test_objects('foo', 1001)")
	require.NoError(t, err)
}
