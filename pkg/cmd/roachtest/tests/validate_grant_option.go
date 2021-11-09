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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/stretchr/testify/require"
)

func registerValidateGrantOption(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "sql-experience/validate-grant-option",
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(3),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runRegisterValidateGrantOption(ctx, t, c, *t.BuildVersion())
		},
	})
}

// execSQL executes the SQL statement as "root".
func execSQL(sqlStatement string, errorExpected bool, node int) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnE(ctx, node)
		require.NoError(t, err)
		_, err = conn.Exec(sqlStatement)
		if !errorExpected {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
}

// execSQLAsUser executes the SQL statement as the specified user.
func execSQLAsUser(sqlStatement string, user string, errorExpected bool, node int) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnEAsUser(ctx, node, user)
		require.NoError(t, err)
		_, err = conn.Exec(sqlStatement)
		if !errorExpected {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
}

func runRegisterValidateGrantOption(
	ctx context.Context, t test.Test, c cluster.Cluster, buildVersion version.Version,
) {
	const mainVersion = ""
	predecessorVersion, err := PredecessorVersion(buildVersion)
	if err != nil {
		t.Fatal(err)
	}

	u := newVersionUpgradeTest(c,
		uploadAndStart(c.All(), predecessorVersion),
		waitForUpgradeStep(c.All()),
		preventAutoUpgradeStep(1),
		preventAutoUpgradeStep(2),
		preventAutoUpgradeStep(3),

		// No nodes have been upgraded; any user can grant or revoke a privilege that they hold (grant options
		// are ignored).
		execSQL("CREATE USER foo;", false, 1),
		execSQL("CREATE USER foo2;", false, 2),
		execSQL("CREATE USER foo3;", false, 2),
		execSQL("CREATE USER foo4;", false, 2),
		execSQL("CREATE USER target;", false, 1),

		execSQL("CREATE DATABASE d;", false, 2),
		execSQL("CREATE SCHEMA s;", false, 3),
		execSQL("CREATE TABLE t1();", false, 2),
		execSQL("CREATE TYPE ty AS ENUM();", false, 3),

		execSQL("GRANT ALL PRIVILEGES ON DATABASE d TO foo;", false, 1),
		execSQL("GRANT ALL PRIVILEGES ON SCHEMA s TO foo2;", false, 3),
		execSQL("GRANT ALL PRIVILEGES ON TABLE t1 TO foo3;", false, 1),
		execSQL("GRANT ALL PRIVILEGES ON TYPE ty TO foo4;", false, 1),

		execSQLAsUser("SELECT * FROM t1;", "foo3", false, 2),
		execSQLAsUser("GRANT CREATE ON DATABASE d TO target;", "foo", false, 1),
		execSQLAsUser("GRANT USAGE ON SCHEMA s TO target;", "foo2", false, 3),
		execSQLAsUser("GRANT SELECT ON TABLE t1 TO target;", "foo3", false, 1),
		execSQLAsUser("GRANT USAGE ON TYPE ty TO target;", "foo4", false, 1),

		// Node 1 is upgraded; because nodes 2 and 3 are on the previous version, any user can still grant
		// or revoke a privilege that they hold (grant options are ignored).
		binaryUpgradeStep(c.Node(1), mainVersion),
		allowAutoUpgradeStep(1),
		execSQLAsUser("GRANT CREATE ON DATABASE d TO target;", "foo", false, 1),
		execSQLAsUser("GRANT USAGE ON SCHEMA s TO target;", "foo2", false, 3),
		execSQLAsUser("GRANT INSERT ON TABLE t1 TO target;", "foo3", false, 2),
		execSQLAsUser("GRANT GRANT ON TYPE ty TO target;", "foo4", false, 2),

		// Node 2 is upgraded; because node 3 is on the previous version, any user can still grant or revoke
		// a privilege that they hold (grant options are ignored).
		binaryUpgradeStep(c.Node(2), mainVersion),
		allowAutoUpgradeStep(2),
		execSQLAsUser("GRANT ALL PRIVILEGES ON DATABASE d TO target;", "foo", false, 3),
		execSQLAsUser("GRANT ALL PRIVILEGES ON SCHEMA s TO target;", "foo2", false, 2),
		execSQLAsUser("GRANT ALL PRIVILEGES ON TABLE t1 TO target;", "foo3", false, 1),
		execSQLAsUser("GRANT ALL PRIVILEGES ON TYPE ty TO target;", "foo4", false, 1),
		execSQLAsUser("GRANT GRANT ON DATABASE d TO foo2;", "foo", false, 1),
		execSQLAsUser("GRANT GRANT ON SCHEMA s TO foo3;", "foo2", false, 1),
		execSQLAsUser("GRANT GRANT, SELECT ON TABLE t1 TO foo4;", "foo3", false, 2),

		// Node 3 is upgraded; because all the nodes have been upgraded, the migration will begin running after
		// allowAutoUpgradeStep(3). The roachtest will ensure that the migration will be complete before moving
		// beyond waitForUpgradeStep(c.All()).
		binaryUpgradeStep(c.Node(3), mainVersion),
		allowAutoUpgradeStep(3),
		waitForUpgradeStep(c.All()),

		// All the nodes have been upgraded and the migration has finished; existing users will have had their grant
		// option bits set equal to their privilege bits if they hold "GRANT" or "ALL" privileges. Grant options will
		// now be enforced (an error will be returned if a user tries to grant a privilege they do not hold grant
		// options for, which corresponds to "true" in the errorExpected field).
		execSQLAsUser("GRANT DELETE ON TABLE t1 TO foo2;", "foo3", false, 3),
		execSQLAsUser("GRANT ALL PRIVILEGES ON SCHEMA s TO foo3;", "target", false, 2),
		execSQLAsUser("GRANT CREATE ON DATABASE d TO foo3;", "foo2", true, 2),
		execSQLAsUser("GRANT USAGE ON SCHEMA s TO foo;", "foo3", true, 2),
		execSQLAsUser("GRANT INSERT ON TABLE t1 TO foo;", "foo4", true, 1),
		execSQLAsUser("GRANT SELECT ON TABLE t1 TO foo;", "foo4", false, 1),

		execSQL("CREATE USER foo5;", false, 2),
		execSQL("CREATE TABLE t2();", false, 3),
		execSQL("GRANT ALL PRIVILEGES ON TABLE t2 TO foo5;", false, 1),
		execSQLAsUser("GRANT DELETE ON TABLE t2 TO foo2;", "foo5", true, 2),
	)
	u.run(ctx, t)
}
