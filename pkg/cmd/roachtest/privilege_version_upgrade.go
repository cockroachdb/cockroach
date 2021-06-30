// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/stretchr/testify/require"
)

// this test ensures that privileges stay consistent after version upgrades.
func registerPrivilegeVersionUpgrade(r *testRegistry) {
	r.Add(TestSpec{
		Name:       "versionupgrade/privileges",
		Owner:      OwnerSQLExperience,
		MinVersion: "v20.1.0",
		Cluster:    r.makeClusterSpec(1),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runPrivilegeVersionUpgrade(ctx, t, c, *t.BuildVersion())
		},
	})
}

func createDatabasePrivilegesStep(loadNode int, privileges privilege.List) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnE(ctx, loadNode)
		require.NoError(t, err)

		_, err = conn.Exec("REVOKE ALL ON DATABASE test FROM testuser")
		require.NoError(t, err)
		for _, privilege := range privileges.SortedNames() {
			_, err = conn.Exec(fmt.Sprintf("GRANT %s ON DATABASE test TO testuser", privilege))
		}
		require.NoError(t, err)
	}
}

func checkDatabasePrivilegesStep(loadNode int, privileges privilege.List) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnE(ctx, loadNode)
		require.NoError(t, err)

		r := sqlutils.MakeSQLRunner(conn)
		expectedGrants := [][]string{
			{"test", "admin", "ALL"},
			{"test", "root", "ALL"},
		}

		for _, privilege := range privileges.SortedNames() {
			expectedGrants = append(expectedGrants, []string{"test", "testuser", privilege})
		}
		r.CheckQueryResults(t, `SHOW GRANTS ON DATABASE test`, expectedGrants)
	}
}

func createSchemaPrivilegesStep(loadNode int, privileges privilege.List) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnE(ctx, loadNode)
		require.NoError(t, err)
		_, err = conn.Exec("CREATE SCHEMA test.test_schema")
		require.NoError(t, err)

		_, err = conn.Exec("REVOKE ALL ON SCHEMA test.test_schema FROM testuser")
		require.NoError(t, err)
		for _, privilege := range privileges.SortedNames() {
			_, err = conn.Exec(fmt.Sprintf("GRANT %s ON SCHEMA test.test_schema TO testuser", privilege))
			require.NoError(t, err)
		}
	}
}

func checkSchemaPrivilegesStep(loadNode int, privileges privilege.List) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnE(ctx, loadNode)
		require.NoError(t, err)

		r := sqlutils.MakeSQLRunner(conn)
		expectedGrants := [][]string{
			{"test", "test_schema", "admin", "ALL"},
			{"test", "test_schema", "root", "ALL"},
		}

		for _, privilege := range privileges.SortedNames() {
			expectedGrants = append(expectedGrants, []string{"test", "test_schema", "testuser", privilege})
		}
		r.CheckQueryResults(t, `SHOW GRANTS ON SCHEMA test.test_schema`, expectedGrants)
	}
}

func createTypePrivilegesStep(loadNode int, privileges privilege.List) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnE(ctx, loadNode)
		require.NoError(t, err)
		_, err = conn.Exec("CREATE TYPE test.test_type AS ENUM()")
		require.NoError(t, err)

		_, err = conn.Exec("REVOKE ALL ON TYPE test.test_type FROM testuser")
		require.NoError(t, err)
		for _, privilege := range privileges.SortedNames() {
			_, err = conn.Exec(fmt.Sprintf("GRANT %s ON TYPE test.test_type TO testuser", privilege))
			require.NoError(t, err)
		}
	}
}

func checkTypePrivilegesStep(loadNode int, privileges privilege.List) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnE(ctx, loadNode)
		require.NoError(t, err)

		r := sqlutils.MakeSQLRunner(conn)
		expectedGrants := [][]string{
			{"test", "public", "test_type", "admin", "ALL"},
			{"test", "public", "test_type", "public", "USAGE"},
			{"test", "public", "test_type", "root", "ALL"},
		}

		for _, privilege := range privileges.SortedNames() {
			expectedGrants = append(expectedGrants, []string{"test", "public", "test_type", "testuser", privilege})
		}
		_, err = conn.Exec("USE test")
		require.NoError(t, err)
		r.CheckQueryResults(t, `SHOW GRANTS ON TYPE test.test_type`, expectedGrants)
	}
}

func createTablePrivilegesStep(loadNode int, privileges privilege.List) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnE(ctx, loadNode)
		require.NoError(t, err)
		_, err = conn.Exec("CREATE TABLE test.test_table()")
		require.NoError(t, err)

		_, err = conn.Exec("REVOKE ALL ON test.test_table FROM testuser")
		require.NoError(t, err)
		for _, privilege := range privileges.SortedNames() {
			_, err = conn.Exec(fmt.Sprintf("GRANT %s ON test.test_table TO testuser", privilege))
			require.NoError(t, err)
		}
	}
}

func checkTablePrivilegesStep(loadNode int, privileges privilege.List) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnE(ctx, loadNode)
		require.NoError(t, err)

		r := sqlutils.MakeSQLRunner(conn)

		expectedGrants := [][]string{
			{"test", "public", "test_table", "admin", "ALL"},
			{"test", "public", "test_table", "root", "ALL"},
		}

		for _, privilege := range privileges.SortedNames() {
			expectedGrants = append(expectedGrants, []string{"test", "public", "test_table", "testuser", privilege})
		}
		r.CheckQueryResults(t, `SHOW GRANTS ON test.test_table`, expectedGrants)
	}
}

func resetStep() versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		err := u.c.WipeE(ctx, t.L())
		require.NoError(t, err)
		err = u.c.RunL(ctx, t.L(), u.c.All(), "rm -rf "+perfArtifactsDir)
		require.NoError(t, err)
		err = u.c.RunE(ctx, u.c.All(), "rm -rf {store-dir}")
		require.NoError(t, err)
	}
}

func createUserStep(loadNode int) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		conn, err := u.c.ConnE(ctx, loadNode)
		require.NoError(t, err)
		_, err = conn.Exec("CREATE USER testuser")
		require.NoError(t, err)
	}
}

func runPrivilegeVersionUpgrade(
	ctx context.Context, t test.Test, c cluster.Cluster, buildVersion version.Version,
) {
	predecessorVersion, err := PredecessorVersion(buildVersion)
	if err != nil {
		t.Fatal(err)
	}

	// An empty string will lead to the cockroach binary specified by flag
	// `cockroach` to be used.
	const mainVersion = ""
	upgradeTest := func(
		dbPrivs, schemaPrivs, tablePrivs, typePrivs privilege.List) *versionUpgradeTest {
		steps := []versionStep{
			resetStep(),
			uploadAndStartFromCheckpointFixture(c.All(), predecessorVersion),
			waitForUpgradeStep(c.All()),

			// NB: at this point, cluster and binary version equal predecessorVersion,
			// and auto-upgrades are on.
			preventAutoUpgradeStep(1),
		}

		steps = append(
			steps,
			createUserStep(1),
			createDatabasePrivilegesStep(1, dbPrivs),
			checkDatabasePrivilegesStep(1, dbPrivs),
			createSchemaPrivilegesStep(1, schemaPrivs),
			checkSchemaPrivilegesStep(1, schemaPrivs),
			createTablePrivilegesStep(1, tablePrivs),
			checkTablePrivilegesStep(1, tablePrivs),
			createTypePrivilegesStep(1, typePrivs),
			checkTypePrivilegesStep(1, typePrivs),
		)
		steps = append(
			steps,
			// Roll nodes forward and finalize upgrade.
			binaryUpgradeStep(c.Node(4), mainVersion),
			binaryUpgradeStep(c.Node(3), mainVersion),
			binaryUpgradeStep(c.Node(1), mainVersion),
			binaryUpgradeStep(c.Node(2), mainVersion),

			allowAutoUpgradeStep(1),
			waitForUpgradeStep(c.All()),

			checkDatabasePrivilegesStep(1, dbPrivs),
			checkSchemaPrivilegesStep(1, schemaPrivs),
			checkTablePrivilegesStep(1, tablePrivs),
			checkTypePrivilegesStep(1, typePrivs),
		)

		return newVersionUpgradeTest(c,
			steps...,
		)
	}

	allPrivs := privilege.List{privilege.ALL}
	upgradeTest(allPrivs, allPrivs, allPrivs, allPrivs).run(ctx, t)

	// Split privileges into two sets so they aren't folded into "ALL".
	dbPrivsSetOne, dbPrivsSetTwo := splitPrivilegeListHelper(privilege.GetValidPrivilegesForObject(privilege.Database))
	schemaPrivsSetOne, schemaPrivsSetTwo := splitPrivilegeListHelper(privilege.GetValidPrivilegesForObject(privilege.Schema))
	tablePrivsSetOne, tablePrivsSetTwo := splitPrivilegeListHelper(privilege.GetValidPrivilegesForObject(privilege.Table))
	typePrivsSetOne, typePrivsSetTwo := splitPrivilegeListHelper(privilege.GetValidPrivilegesForObject(privilege.Type))

	upgradeTest(dbPrivsSetOne, schemaPrivsSetOne, tablePrivsSetOne, typePrivsSetOne).run(ctx, t)
	upgradeTest(dbPrivsSetTwo, schemaPrivsSetTwo, tablePrivsSetTwo, typePrivsSetTwo).run(ctx, t)
}

func splitPrivilegeListHelper(privileges privilege.List) (privilege.List, privilege.List) {
	var privilegeSetOne privilege.List
	var privilegeSetTwo privilege.List
	for i, p := range privileges {
		if p == privilege.ALL {
			continue
		}
		if i%2 == 0 {
			privilegeSetOne = append(privilegeSetOne, p)
		} else {
			privilegeSetTwo = append(privilegeSetTwo, p)
		}
	}

	return privilegeSetOne, privilegeSetTwo
}
