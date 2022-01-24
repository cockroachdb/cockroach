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
		Owner:   registry.OwnerSQLExperience,
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

		// No nodes have been upgraded; any user can grant or revoke a privilege that they hold (grant options
		// are ignored).
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
	)
	u.run(ctx, t)
}

func showGrantsOnDatabase(dbName string, expectedPrivileges [][]string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnE(ctx, t.L(), loadNode)
		require.NoError(t, err)
		rows, err := conn.Query(fmt.Sprintf("SHOW GRANTS ON DATABASE %s", dbName))

		var name, grantee, privilegeType string
		i := 0
		for rows.Next() {
			privilegeRow := expectedPrivileges[i]
			rows.Scan(&name, &grantee, &privilegeType)

			require.Equal(t, privilegeRow[0], name)
			require.Equal(t, privilegeRow[1], grantee)
			require.Equal(t, privilegeRow[2], privilegeType)
			i += 1
		}

		if i != len(expectedPrivileges) {
			t.Errorf("expected %d rows, found %d rows", len(expectedPrivileges), i)
		}
	}
}
