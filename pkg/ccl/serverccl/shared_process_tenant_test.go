// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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

	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DisableDefaultTestTenant: true,
		}})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	_, err := db.Exec("CREATE TENANT hello; ALTER TENANT hello START SERVICE SHARED")
	require.NoError(t, err)

	_, err = db.Exec("ALTER TENANT ALL SET CLUSTER SETTING spanconfig.tenant_limit = 1000")
	require.NoError(t, err)

	sqlAddr := tc.Server(0).ServingSQLAddr()
	var tenantDB *gosql.DB
	testutils.SucceedsSoon(t, func() error {
		var err error
		tenantDB, err = serverutils.OpenDBConnE(sqlAddr, "cluster:hello", false, tc.Stopper())
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
