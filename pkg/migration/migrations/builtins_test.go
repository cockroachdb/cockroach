// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/migration/migrations"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestIsAtLeastVersionBuiltin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.Start21_2),
				},
			},
		},
	}

	var (
		ctx   = context.Background()
		tc    = testcluster.StartTestCluster(t, 1, clusterArgs)
		conn  = tc.ServerConn(0)
		sqlDB = sqlutils.MakeSQLRunner(conn)
	)
	defer tc.Stopper().Stop(ctx)

	// Sanity check that an error is returned for an invalid argument.
	sqlDB.ExpectErr(t, ".*invalid version.*", "SELECT crdb_internal.is_at_least_version('foo')")

	// Check that the builtin returns false when comparing against 21.2 version
	// because we are still on 21.1.
	row := sqlDB.QueryRow(t, "SELECT crdb_internal.is_at_least_version('21.2')")
	var actual string
	row.Scan(&actual)
	require.Equal(t, "false", actual)

	// Run the migration.
	migrations.Migrate(
		t,
		conn,
		clusterversion.V21_2,
		nil,   /* done */
		false, /* expectError */
	)

	// It should now return true.
	row = sqlDB.QueryRow(t, "SELECT crdb_internal.is_at_least_version('21.2')")
	row.Scan(&actual)
	require.Equal(t, "true", actual)
}
