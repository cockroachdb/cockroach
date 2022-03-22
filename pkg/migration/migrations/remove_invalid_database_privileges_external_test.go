// Copyright 2022 The Cockroach Authors.
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
	"encoding/hex"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestConvertIncompatibleDatabasePrivilegesToDefaultPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.RemoveIncompatibleDatabasePrivileges - 1),
				},
			},
		},
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, clusterArgs)
	defer tc.Stopper().Stop(ctx)
	sqlDB := tc.ServerConn(0)
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	/*
			The hex for the descriptor to inject was created by running the following
			commands in a 21.1 binary.

			CREATE DATABASE test;
			CREATE USER testuser;
			CREATE USER testuser2;
			GRANT SELECT, UPDATE, DELETE, INSERT ON DATABASE test TO testuser;
			GRANT SELECT, CREATE, UPDATE, DELETE, INSERT ON DATABASE test TO testuser2;

		   SELECT encode(descriptor, 'hex') AS descriptor
		     FROM system.descriptor
		    WHERE id
		          IN (
		               SELECT id
		                 FROM system.namespace
		                WHERE "parentID"
		                      = (
		                           SELECT id
		                             FROM system.namespace
		                            WHERE "parentID" = 0 AND name = 'db'
		                       )
		                   OR "parentID" = 0 AND name = 'test'
		           );
	*/

	tdb.Exec(t, "CREATE USER testuser")
	tdb.Exec(t, "CREATE USER testuser2")
	const databaseDescriptorToInject = "124e0a0474657374103b1a3c0a090a0561646d696e10020a080a04726f6f7410020a0d0a08746573747573657210e0030a0e0a0974657374757365723210e403120464656d6f18012200280740004a00"

	encoded, err := hex.DecodeString(databaseDescriptorToInject)
	require.NoError(t, err)

	var desc descpb.Descriptor
	require.NoError(t, protoutil.Unmarshal(encoded, &desc))

	testuser := security.MakeSQLUsernameFromPreNormalizedString("testuser")
	testuser2 := security.MakeSQLUsernameFromPreNormalizedString("testuser2")
	_, dbDesc, _, _ := descpb.FromDescriptorWithMVCCTimestamp(&desc, hlc.Timestamp{WallTime: 1})
	privilegesForTestuser := dbDesc.Privileges.FindOrCreateUser(testuser)
	privilegesForTestuser2 := dbDesc.Privileges.FindOrCreateUser(testuser2)

	// Verify that testuser has the incompatible privileges on the database.
	// We manually check the privileges instead of using CheckPrivilege to ensure
	// RunPostDeserializationChanges are not called.
	require.Equal(t, privilegesForTestuser.Privileges&privilege.SELECT.Mask(), privilege.SELECT.Mask())
	require.Equal(t, privilegesForTestuser.Privileges&privilege.INSERT.Mask(), privilege.INSERT.Mask())
	require.Equal(t, privilegesForTestuser.Privileges&privilege.DELETE.Mask(), privilege.DELETE.Mask())
	require.Equal(t, privilegesForTestuser.Privileges&privilege.UPDATE.Mask(), privilege.UPDATE.Mask())

	require.Equal(t, privilegesForTestuser2.Privileges&privilege.SELECT.Mask(), privilege.SELECT.Mask())
	require.Equal(t, privilegesForTestuser2.Privileges&privilege.INSERT.Mask(), privilege.INSERT.Mask())
	require.Equal(t, privilegesForTestuser2.Privileges&privilege.DELETE.Mask(), privilege.DELETE.Mask())
	require.Equal(t, privilegesForTestuser2.Privileges&privilege.UPDATE.Mask(), privilege.UPDATE.Mask())
	require.Equal(t, privilegesForTestuser2.Privileges&privilege.CREATE.Mask(), privilege.CREATE.Mask())

	require.NoError(t, sqlutils.InjectDescriptors(
		ctx, sqlDB, []*descpb.Descriptor{&desc}, true, /* force */
	))

	// Migrate to the new cluster version.
	tdb.Exec(t, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.RemoveIncompatibleDatabasePrivileges).String())

	tdb.CheckQueryResultsRetry(t, "SHOW CLUSTER SETTING version",
		[][]string{{clusterversion.ByKey(clusterversion.RemoveIncompatibleDatabasePrivileges).String()}})

	tdb.CheckQueryResults(t, "SHOW GRANTS ON DATABASE test", [][]string{
		{"test", "admin", "ALL", "true"},
		{"test", "root", "ALL", "true"},
		{"test", "testuser2", "CREATE", "false"},
	})

	tdb.Exec(t, "USE test")

	// Check that the incompatible privileges have turned into default privileges.
	tdb.CheckQueryResults(t, "SHOW DEFAULT PRIVILEGES FOR ALL ROLES",
		[][]string{
			{"NULL", "true", "tables", "testuser", "DELETE"},
			{"NULL", "true", "tables", "testuser", "INSERT"},
			{"NULL", "true", "tables", "testuser", "SELECT"},
			{"NULL", "true", "tables", "testuser", "UPDATE"},
			{"NULL", "true", "tables", "testuser2", "DELETE"},
			{"NULL", "true", "tables", "testuser2", "INSERT"},
			{"NULL", "true", "tables", "testuser2", "SELECT"},
			{"NULL", "true", "tables", "testuser2", "UPDATE"},
			{"NULL", "true", "types", "public", "USAGE"},
		})
}
