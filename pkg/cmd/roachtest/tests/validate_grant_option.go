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

// execSQLUser changes the current user from "root" to the one that is input and executes the
// SQL statement as that user.
func execSQLUser(sqlStatement string, user string, errorExpected bool, node int) versionStep {
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

		execSQLUser("SELECT * FROM t1;", "foo3", false, 2),
		execSQLUser("GRANT CREATE ON DATABASE d TO target;", "foo", false, 1),
		execSQLUser("GRANT USAGE ON SCHEMA s TO target;", "foo2", false, 3),
		execSQLUser("GRANT SELECT ON TABLE t1 TO target;", "foo3", false, 1),
		execSQLUser("GRANT USAGE ON TYPE ty TO target;", "foo4", false, 1),

		binaryUpgradeStep(c.Node(1), mainVersion),
		allowAutoUpgradeStep(1),
		execSQLUser("GRANT CREATE ON DATABASE d TO target;", "foo", false, 1),
		execSQLUser("GRANT USAGE ON SCHEMA s TO target;", "foo2", false, 3),
		execSQLUser("GRANT INSERT ON TABLE t1 TO target;", "foo3", false, 2),
		execSQLUser("GRANT GRANT ON TYPE ty TO target;", "foo4", false, 2),

		binaryUpgradeStep(c.Node(2), mainVersion),
		allowAutoUpgradeStep(2),
		execSQLUser("GRANT ALL PRIVILEGES ON DATABASE d TO target;", "foo", false, 3),
		execSQLUser("GRANT ALL PRIVILEGES ON SCHEMA s TO target;", "foo2", false, 2),
		execSQLUser("GRANT ALL PRIVILEGES ON TABLE t1 TO target;", "foo3", false, 1),
		execSQLUser("GRANT ALL PRIVILEGES ON TYPE ty TO target;", "foo4", false, 1),
		execSQLUser("GRANT GRANT ON DATABASE d TO foo2;", "foo", false, 1),
		execSQLUser("GRANT GRANT ON SCHEMA s TO foo3;", "foo2", false, 1),
		execSQLUser("GRANT GRANT, SELECT ON TABLE t1 TO foo4;", "foo3", false, 2),

		binaryUpgradeStep(c.Node(3), mainVersion),
		allowAutoUpgradeStep(3),
		waitForUpgradeStep(c.All()), // ALL NODES ARE UPGRADED, THE MIGRATION HAS FINISHED
		execSQLUser("GRANT DELETE ON TABLE t1 TO foo2;", "foo3", false, 3),
		execSQLUser("GRANT ALL PRIVILEGES ON SCHEMA s TO foo3;", "target", false, 2),
		execSQLUser("GRANT CREATE ON DATABASE d TO foo3;", "foo2", true, 2),
		execSQLUser("GRANT USAGE ON SCHEMA s TO foo;", "foo3", true, 2),
		execSQLUser("GRANT INSERT ON TABLE t1 TO foo;", "foo4", true, 1),
		execSQLUser("GRANT SELECT ON TABLE t1 TO foo;", "foo4", false, 1),

		execSQL("CREATE USER foo5;", false, 2),
		execSQL("CREATE TABLE t2();", false, 3),
		execSQLUser("GRANT ALL PRIVILEGES ON TABLE t2 TO foo5;", "root", false, 1),
		execSQLUser("GRANT DELETE ON TABLE t2 TO foo2;", "foo5", true, 2),
	)
	u.run(ctx, t)
}
