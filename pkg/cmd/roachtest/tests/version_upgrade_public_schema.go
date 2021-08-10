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

// this test ensures that privileges stay consistent after version upgrades.
func registerVersionUpgradePublicSchema(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "versionupgrade/publicschema",
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(3),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runVersionUpgradePublicSchema(ctx, t, c, *t.BuildVersion())
		},
	})
}

func runVersionUpgradePublicSchema(
	ctx context.Context, t test.Test, c cluster.Cluster, buildVersion version.Version,
) {
	predecessorVersion, err := PredecessorVersion(buildVersion)
	if err != nil {
		t.Fatal(err)
	}

	const currentVersion = ""

	steps := []versionStep{
		resetStep(),
		uploadAndStart(c.All(), predecessorVersion),
		waitForUpgradeStep(c.All()),

		// NB: at this point, cluster and binary version equal predecessorVersion,
		// and auto-upgrades are on.
		preventAutoUpgradeStep(1),
	}

	steps = append(
		steps,
		createDatabaseStep("test1"),
		checkDatabaseStep("test1"),
		// Roll nodes forward and finalize upgrade.
		binaryUpgradeStep(c.Node(3), currentVersion),
		binaryUpgradeStep(c.Node(1), currentVersion),
		binaryUpgradeStep(c.Node(2), currentVersion),

		checkDatabaseStep("test1"),
		createDatabaseStep("test2"),
		checkDatabaseStep("test2"),

		allowAutoUpgradeStep(1),
		waitForUpgradeStep(c.All()),

		//createTableInSchema("test1"),
		//tryDroppingOldPublicSchema("test1"),
	)

	newVersionUpgradeTest(c, steps...).run(ctx, t)
}

func createDatabaseStep(dbName string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		t.L().Printf("createDatabaseStep %s", dbName)
		conn, err := u.c.ConnE(ctx, loadNode)
		require.NoError(t, err)
		_, err = conn.Exec(fmt.Sprintf("CREATE DATABASE %s", dbName))
		require.NoError(t, err)
	}
}

func checkDatabaseStep(dbName string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		t.L().Printf("checkDatabaseStep %s", dbName)
		conn, err := u.c.ConnE(ctx, loadNode)
		require.NoError(t, err)
		rows, err := conn.Query(fmt.Sprintf("SHOW SCHEMAS FROM %s;", dbName))
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var schemaName, owner string
			rows.Scan(&schemaName, &owner)
			t.L().Printf("%s %s", schemaName, owner)
		}

		rows2, err := conn.Query("SELECT * FROM system.namespace;")
		require.NoError(t, err)
		defer rows2.Close()
		for rows2.Next() {
			var parentID, parentSchemaID, id int
			var name string
			rows2.Scan(&parentID, &parentSchemaID, &name, &id)
			t.L().Printf("%d %d %s %d", parentID, parentSchemaID, name, id)
		}
	}
}

func createTableInSchema(dbName string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		t.L().Printf("createTableInSchema")
		conn, err := u.c.ConnE(ctx, loadNode)
		require.NoError(t, err)
		_, err = conn.Exec(fmt.Sprintf("CREATE TABLE %s.public.t();", dbName))
		require.NoError(t, err)
	}
}

func tryDroppingOldPublicSchema(dbName string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		t.L().Printf("tryDroppingOldPublicSchema")
		conn, err := u.c.ConnE(ctx, loadNode)
		require.NoError(t, err)
		_, err = conn.Exec(fmt.Sprintf("DROP SCHEMA %s.public CASCADE;", dbName))
		require.NoError(t, err)
	}
}
