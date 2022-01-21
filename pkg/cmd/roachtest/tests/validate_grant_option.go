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
		Name:    "validate-grant-option",
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(3),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runRegisterValidateGrantOption(ctx, t, c, *t.BuildVersion())
		},
	})
}

// execSQL executes the SQL statement as "root".
func execSQL(sqlStatement string, expectedErrText string, node int) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnE(ctx, t.L(), node)
		require.NoError(t, err)
		t.L().PrintfCtx(ctx, "user root on node %d executing: %s", node, sqlStatement)
		_, err = conn.Exec(sqlStatement)
		if len(expectedErrText) == 0 {
			require.NoError(t, err)
		} else {
			require.EqualError(t, err, expectedErrText)
		}
	}
}

// execSQLAsUser executes the SQL statement as the specified user.
func execSQLAsUser(sqlStatement string, user string, expectedErrText string, node int) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnEAsUser(ctx, t.L(), node, user)
		require.NoError(t, err)
		t.L().PrintfCtx(ctx, "user %s on node %d executing: %s", user, node, sqlStatement)
		_, err = conn.Exec(sqlStatement)
		if len(expectedErrText) == 0 {
			require.NoError(t, err)
		} else {
			require.EqualError(t, err, expectedErrText)
		}
	}
}

func runRegisterValidateGrantOption(
	ctx context.Context, t test.Test, c cluster.Cluster, buildVersion version.Version,
) {
	// This is meant to test a migration from 21.2 to 22.1.
	if buildVersion.Major() != 22 || buildVersion.Minor() != 1 {
		t.L().PrintfCtx(ctx, "skipping test because build version is %s", buildVersion)
		return
	}
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
		execSQL("CREATE USER foo;", "", 1),
		execSQL("CREATE USER foo2;", "", 2),
		execSQL("CREATE USER foo3;", "", 2),
		execSQL("CREATE USER foo4;", "", 2),
		execSQL("CREATE USER foo5;", "", 2),
		execSQL("CREATE USER target;", "", 1),

		execSQL("CREATE DATABASE d;", "", 2),
		execSQL("CREATE SCHEMA s;", "", 3),
		execSQL("CREATE TABLE t1();", "", 2),
		execSQL("CREATE TYPE ty AS ENUM();", "", 3),

		execSQL("GRANT ALL PRIVILEGES ON DATABASE d TO foo;", "", 1),
		execSQL("GRANT ALL PRIVILEGES ON SCHEMA s TO foo2;", "", 3),
		execSQL("GRANT ALL PRIVILEGES ON TABLE t1 TO foo3;", "", 1),
		execSQL("GRANT ALL PRIVILEGES ON TYPE ty TO foo4;", "", 1),

		execSQLAsUser("SELECT * FROM t1;", "foo3", "", 2),
		execSQLAsUser("GRANT CREATE ON DATABASE d TO target;", "foo", "", 1),
		execSQLAsUser("GRANT USAGE ON SCHEMA s TO target;", "foo2", "", 3),
		execSQLAsUser("GRANT SELECT ON TABLE t1 TO target;", "foo3", "", 1),
		execSQLAsUser("GRANT USAGE ON TYPE ty TO target;", "foo4", "", 1),

		// Node 1 is upgraded, and nodes 2 and 3 are on the previous version.
		binaryUpgradeStep(c.Node(1), mainVersion),
		allowAutoUpgradeStep(1),
		execSQLAsUser("GRANT CREATE ON DATABASE d TO target;", "foo", "", 1),
		execSQLAsUser("GRANT CREATE ON DATABASE defaultdb TO foo;", "root", "", 1),
		execSQLAsUser("CREATE TABLE t2();", "foo", "", 1),                       // t2 created on a new node.
		execSQLAsUser("CREATE TABLE t3();", "foo", "", 2),                       // t3 created on an old node.
		execSQLAsUser("GRANT CREATE ON TABLE t2 TO foo2;", "foo", "", 1),        // foo2 does not get GRANT or grant option on t2.
		execSQLAsUser("GRANT GRANT, CREATE ON TABLE t3 TO foo2;", "foo", "", 2), // foo2 gets GRANT and grant option on t3
		execSQLAsUser("GRANT CREATE ON TABLE t2 TO foo3;", "foo", "", 1),        // foo3 does not get GRANT or grant option on t2.
		execSQLAsUser("GRANT GRANT, CREATE ON TABLE t3 TO foo3;", "foo", "", 2), // foo3 gets GRANT, but not grant option on t3

		execSQLAsUser("GRANT CREATE ON TABLE t2 TO foo4 WITH GRANT OPTION;", "foo", "pq: version 21.2-22 must be finalized to use grant options", 1),
		execSQLAsUser("GRANT CREATE ON TABLE t3 TO foo4 WITH GRANT OPTION;", "foo", "pq: at or near \"with\": syntax error", 2),

		execSQLAsUser("GRANT CREATE ON TABLE t2 TO foo3;", "foo2",
			"pq: user foo2 does not have GRANT privilege on relation t2", 1),
		execSQLAsUser("GRANT CREATE ON TABLE t2 TO foo3;", "foo2",
			"pq: user foo2 does not have GRANT privilege on relation t2", 2), // Same error from node 2.
		execSQLAsUser("GRANT CREATE ON TABLE t3 TO foo3;", "foo2", "", 1),
		execSQLAsUser("GRANT CREATE ON TABLE t3 TO foo3;", "foo2", "", 2),
		execSQLAsUser("GRANT CREATE ON TABLE t2 TO foo4;", "foo3",
			"pq: user foo3 does not have GRANT privilege on relation t2", 1),
		execSQLAsUser("GRANT CREATE ON TABLE t2 TO foo4;", "foo3",
			"pq: user foo3 does not have GRANT privilege on relation t2", 2), // Same error from node 2.
		execSQLAsUser("GRANT CREATE ON TABLE t3 TO foo4;", "foo3", "", 1),
		execSQLAsUser("GRANT CREATE ON TABLE t3 TO foo4;", "foo3", "", 2),

		execSQLAsUser("GRANT USAGE ON SCHEMA s TO target;", "foo2", "", 3),
		execSQLAsUser("GRANT INSERT ON TABLE t1 TO target;", "foo3", "", 2),
		execSQLAsUser("GRANT GRANT ON TYPE ty TO target;", "foo4", "", 2),

		// Node 2 is upgraded.
		binaryUpgradeStep(c.Node(2), mainVersion),
		allowAutoUpgradeStep(2),
		execSQLAsUser("GRANT ALL PRIVILEGES ON DATABASE d TO target;", "foo", "", 3),
		execSQLAsUser("GRANT ALL PRIVILEGES ON SCHEMA s TO target;", "foo2", "", 2),
		execSQLAsUser("GRANT ALL PRIVILEGES ON TABLE t1 TO target;", "foo3", "", 1),
		execSQLAsUser("GRANT ALL PRIVILEGES ON TYPE ty TO target;", "foo4", "", 1),
		execSQLAsUser("GRANT GRANT ON DATABASE d TO foo2;", "foo", "", 1),
		execSQLAsUser("GRANT GRANT ON SCHEMA s TO foo3;", "foo2", "", 1),
		execSQLAsUser("GRANT GRANT, SELECT ON TABLE t1 TO foo4;", "foo3", "", 2),
		execSQLAsUser("GRANT DELETE ON TABLE t1 TO foo4;", "foo3", "", 2),

		// Node 3 is upgraded; because all the nodes have been upgraded, the migration will begin running after
		// allowAutoUpgradeStep(3). The roachtest will ensure that the migration will be complete before moving
		// beyond waitForUpgradeStep(c.All()).
		binaryUpgradeStep(c.Node(3), mainVersion),
		allowAutoUpgradeStep(3),
		waitForUpgradeStep(c.All()),

		// All the nodes have been upgraded and the migration has finished; existing users will have had their grant
		// option bits set equal to their privilege bits if they hold "GRANT" or "ALL" privileges. Grant options will
		// now be enforced (an error will be returned if a user tries to grant a privilege they do not hold grant
		// options for, which corresponds to a nonempty string in the errorExpected field).
		execSQLAsUser("GRANT DELETE ON TABLE t1 TO foo2;", "foo3", "", 3),
		execSQLAsUser("GRANT ALL PRIVILEGES ON SCHEMA s TO foo3;", "target", "", 2),
		execSQLAsUser("GRANT CREATE ON DATABASE d TO foo3;", "foo2", "pq: user foo2 does not have CREATE privilege on database d", 2),
		execSQLAsUser("GRANT USAGE ON SCHEMA s TO foo;", "foo3", "", 2),
		execSQLAsUser("GRANT INSERT ON TABLE t1 TO foo;", "foo4", "pq: user foo4 does not have INSERT privilege on relation t1", 1),
		execSQLAsUser("GRANT SELECT ON TABLE t1 TO foo;", "foo4", "", 1),
		execSQLAsUser("GRANT DELETE ON TABLE t1 TO target;", "foo2", "pq: user foo2 missing WITH GRANT OPTION privilege on DELETE", 1),
		execSQLAsUser("GRANT DELETE ON TABLE t1 TO target;", "foo4", "", 1),
		execSQLAsUser("GRANT SELECT ON TABLE t1 TO target;", "foo4", "", 1),

		execSQLAsUser("GRANT SELECT ON TABLE t2 TO foo4 WITH GRANT OPTION;", "foo", "", 1),
		execSQLAsUser("GRANT SELECT ON TABLE t3 TO foo4 WITH GRANT OPTION;", "foo", "", 2),

		execSQLAsUser("GRANT CREATE ON TABLE t2 TO foo3;", "foo2", "pq: user foo2 missing WITH GRANT OPTION privilege on CREATE", 1),
		execSQLAsUser("GRANT CREATE ON TABLE t3 TO target;", "foo2", "", 2),
		execSQLAsUser("GRANT CREATE ON TABLE t2 TO foo4;", "foo3", "pq: user foo3 missing WITH GRANT OPTION privilege on CREATE", 3),
		execSQLAsUser("GRANT CREATE ON TABLE t3 TO target;", "foo3", "", 3),
	)
	u.run(ctx, t)
}
