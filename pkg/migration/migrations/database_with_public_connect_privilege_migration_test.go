// Copyright  The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package migrations_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestDatabaseWithPrivilegeConnectPrivilegeMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.DatabaseWithPublicConnectPrivilegeMigration - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	// Create database to simulate a pre 22.1.
	tdb.Exec(
		t,
		`
CREATE USER testuser;
CREATE DATABASE db_without_public_connect_priv;
CREATE DATABASE db_with_public_connect_priv;
REVOKE CONNECT ON DATABASE db_without_public_connect_priv FROM public
`)

	// Upgrade and check the privilege is there.
	tdb.Exec(
		t,
		`SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.DatabaseWithPublicConnectPrivilegeMigration).String(),
	)

	var rowCount int
	tdb.QueryRow(t, `
SELECT COUNT(1)
FROM [SHOW GRANTS ON DATABASE db_with_public_connect_priv]
WHERE grantee = 'public' AND privilege_type = 'CONNECT'
`).Scan(&rowCount)
	require.Equal(t, 1, rowCount)
	tdb.QueryRow(t, `
SELECT COUNT(1)
FROM [SHOW GRANTS ON DATABASE db_without_public_connect_priv]
WHERE grantee = 'public' AND privilege_type = 'CONNECT'
`).Scan(&rowCount)
	require.Equal(t, 1, rowCount)
}
