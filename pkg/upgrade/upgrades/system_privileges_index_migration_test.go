// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades_test

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

func TestSystemPrivilegesIndexMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BootstrapVersionKeyOverride:    clusterversion.V22_2,
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.V23_1AlterSystemPrivilegesAddIndexOnPathAndUsername - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()
	tdb := sqlutils.MakeSQLRunner(db)

	// Run migration.
	_, err := tc.Conns[0].ExecContext(
		ctx,
		`SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_1AlterSystemPrivilegesAddIndexOnPathAndUsername).String(),
	)
	require.NoError(t, err)

	expectedSchema := `CREATE TABLE public.privileges (
	username STRING NOT NULL,
	path STRING NOT NULL,
	privileges STRING[] NOT NULL,
	grant_options STRING[] NOT NULL,
	user_id OID NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (username ASC, path ASC),
	UNIQUE INDEX privileges_path_user_id_key (path ASC, user_id ASC) STORING (privileges, grant_options),
	UNIQUE INDEX privileges_path_username_key (path ASC, username ASC) STORING (privileges, grant_options)
)`
	r := tdb.QueryRow(t, "SELECT create_statement FROM [SHOW CREATE TABLE system.privileges]")
	var actualSchema string
	r.Scan(&actualSchema)
	require.Equal(t, expectedSchema, actualSchema)
}
