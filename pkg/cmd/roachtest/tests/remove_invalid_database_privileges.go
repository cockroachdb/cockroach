// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/stretchr/testify/require"
)

func registerRemoveInvalidDatabasePrivileges(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "remove-invalid-database-privileges",
		Owner:   registry.OwnerSQLFoundations,
		Cluster: r.MakeClusterSpec(3),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runRemoveInvalidDatabasePrivileges(ctx, t, c, *t.BuildVersion())
		},
	})
}

func runRemoveInvalidDatabasePrivileges(
	ctx context.Context, t test.Test, c cluster.Cluster, buildVersion version.Version,
) {
	// This is meant to test a migration from 21.2 to 22.1.
	if buildVersion.Major() != 22 || buildVersion.Minor() != 1 {
		t.L().PrintfCtx(ctx, "skipping test because build version is %s", buildVersion)
		return
	}
	const mainVersion = ""
	// We kick off our test on version 21.1.12 since 21.1 was the last version
	// we supported granting invalid database privileges.
	const version21_1_12 = "21.1.12"
	const version21_2_4 = "21.2.4"
	u := newVersionUpgradeTest(c,
		uploadAndStart(c.All(), version21_1_12),
		waitForUpgradeStep(c.All()),
		preventAutoUpgradeStep(1),
		preventAutoUpgradeStep(2),
		preventAutoUpgradeStep(3),

		// No nodes have been upgraded; Grant incompatible database privileges
		// to testuser before upgrading.
		execSQL("CREATE USER testuser;", "", 1),
		execSQL("CREATE DATABASE test;", "", 1),
		execSQL("GRANT CREATE, SELECT, INSERT, UPDATE, DELETE ON DATABASE test TO testuser;", "", 1),

		showGrantsOnDatabase("system", [][]string{
			{"system", "admin", "GRANT"},
			{"system", "admin", "SELECT"},
			{"system", "root", "GRANT"},
			{"system", "root", "SELECT"},
		}),
		showGrantsOnDatabase("test", [][]string{
			{"test", "admin", "ALL"},
			{"test", "root", "ALL"},
			{"test", "testuser", "CREATE"},
			{"test", "testuser", "DELETE"},
			{"test", "testuser", "INSERT"},
			{"test", "testuser", "SELECT"},
			{"test", "testuser", "UPDATE"},
		}),

		binaryUpgradeStep(c.Node(1), version21_2_4),
		allowAutoUpgradeStep(1),
		binaryUpgradeStep(c.Node(2), version21_2_4),
		allowAutoUpgradeStep(2),
		binaryUpgradeStep(c.Node(3), version21_2_4),
		allowAutoUpgradeStep(3),

		waitForUpgradeStep(c.All()),

		binaryUpgradeStep(c.Node(1), mainVersion),
		allowAutoUpgradeStep(1),
		binaryUpgradeStep(c.Node(2), mainVersion),
		allowAutoUpgradeStep(2),
		binaryUpgradeStep(c.Node(3), mainVersion),
		allowAutoUpgradeStep(3),

		waitForUpgradeStep(c.All()),

		showGrantsOnDatabase("system", [][]string{
			{"system", "admin", "CONNECT"},
			{"system", "root", "CONNECT"},
		}),
		showGrantsOnDatabase("test", [][]string{
			{"test", "admin", "ALL"},
			{"test", "root", "ALL"},
			{"test", "testuser", "CREATE"},
		}),

		showDefaultPrivileges("test", [][]string{
			{"tables", "testuser", "SELECT"},
			{"tables", "testuser", "INSERT"},
			{"tables", "testuser", "DELETE"},
			{"tables", "testuser", "UPDATE"},
			{"types", "public", "USAGE"},
		}),

		showDefaultPrivileges("system", [][]string{
			{"types", "public", "USAGE"},
		}),
	)
	u.run(ctx, t)
}

func showGrantsOnDatabase(dbName string, expectedPrivileges [][]string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnE(ctx, t.L(), loadNode)
		require.NoError(t, err)
		rows, err := conn.Query(
			fmt.Sprintf("SELECT database_name, grantee, privilege_type FROM [SHOW GRANTS ON DATABASE %s]",
				dbName))
		require.NoError(t, err)
		var name, grantee, privilegeType string
		i := 0
		for rows.Next() {
			privilegeRow := expectedPrivileges[i]
			err = rows.Scan(&name, &grantee, &privilegeType)
			require.NoError(t, err)

			require.Equal(t, privilegeRow[0], name)
			require.Equal(t, privilegeRow[1], grantee)
			require.Equal(t, privilegeRow[2], privilegeType)
			i++
		}

		if i != len(expectedPrivileges) {
			t.Errorf("expected %d rows, found %d rows", len(expectedPrivileges), i)
		}
	}
}

func showDefaultPrivileges(dbName string, expectedDefaultPrivileges [][]string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnE(ctx, t.L(), loadNode)
		require.NoError(t, err)
		_, err = conn.Exec(fmt.Sprintf("USE %s", dbName))
		require.NoError(t, err)
		rows, err := conn.Query("SELECT object_type, grantee, privilege_type FROM [SHOW DEFAULT PRIVILEGES FOR ALL ROLES]")
		require.NoError(t, err)

		var objectType, grantee, privilegeType string
		i := 0
		for rows.Next() {
			defaultPrivilegeRow := expectedDefaultPrivileges[i]
			err = rows.Scan(&objectType, &grantee, &privilegeType)
			require.NoError(t, err)
			require.Equal(t, defaultPrivilegeRow[0], objectType)
			require.Equal(t, defaultPrivilegeRow[1], grantee)
			require.Equal(t, defaultPrivilegeRow[2], privilegeType)

			i++
		}

		if i != len(expectedDefaultPrivileges) {
			t.Errorf("expected %d rows, found %d rows", len(expectedDefaultPrivileges), i)
		}
	}
}
