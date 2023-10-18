// Copyright 2018 The Cockroach Authors.
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
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

var flowableReleaseTagRegex = regexp.MustCompile(`^flowable-(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)

// This test runs Flowable test suite against a single cockroach node.

func registerFlowable(r registry.Registry) {
	runFlowable := func(
		ctx context.Context,
		t test.Test,
		c cluster.Cluster,
	) {
		if c.IsLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())

		t.Status("creating database used by tests")
		db, err := c.ConnE(ctx, t.L(), node[0])
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		if _, err := db.ExecContext(
			ctx, `CREATE DATABASE flowable;`,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(
			ctx, `CREATE USER flowable;`,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(
			ctx, `GRANT admin TO flowable;`,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning flowable and installing prerequisites")
		latestTag, err := repeatGetLatestTag(
			ctx, t, "flowable", "flowable-engine", flowableReleaseTagRegex,
		)
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("Latest Flowable release is %s.", latestTag)

		if err := repeatRunE(
			ctx, t, c, node, "update apt-get", `sudo apt-get -qq update`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"install dependencies",
			`sudo apt-get -qq install openjdk-17-jre-headless openjdk-17-jdk-headless`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "remove old Flowable", `rm -rf /mnt/data1/flowable-engine`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t,
			c,
			"https://github.com/flowable/flowable-engine.git",
			"/mnt/data1/flowable-engine",
			latestTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("building Flowable")
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"building Flowable",
			`cd /mnt/data1/flowable-engine/ && ./mvnw clean install -DskipTests`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"configuring tests for cockroachdb",
			fmt.Sprintf(`mkdir -p $HOME/.flowable/jdbc/ && \
echo "%s" > $HOME/.flowable/jdbc/build.flowable6.cockroachdb.properties && \
cd /mnt/data1/flowable-engine/ && \
echo '%s' > flowable_crdb.patch && \
git apply --ignore-whitespace flowable_crdb.patch && \
grep "force-commit" . -lr | xargs sed -i 's/-- force-commit//g'`,
				flowableParams, flowablePatch,
			),
		); err != nil {
			t.Fatal(err)
		}

		if err := c.RunE(ctx, node,
			`cd /mnt/data1/flowable-engine/ && \
./mvnw clean test -Dtest=Flowable6Test#testLongServiceTaskLoop -Ddatabase=cockroachdb`,
		); err != nil {
			t.Fatal(err)
		}

		// Java 17 poses a problem for some other roachtests that use java.
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"uninstall java 17",
			`sudo apt-get purge -qq openjdk-17-jre-headless openjdk-17-jdk-headless`,
		); err != nil {
			t.Fatal(err)
		}
	}

	r.Add(registry.TestSpec{
		Name:             "flowable",
		Owner:            registry.OwnerSQLFoundations,
		Cluster:          r.MakeClusterSpec(1),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runFlowable(ctx, t, c)
		},
	})
}

const flowableParams = `
jdbc.url=jdbc:postgresql://127.0.0.1:26257/flowable?sslmode=disable
jdbc.driver=org.postgresql.Driver
jdbc.username=flowable
jdbc.password
`

// This patch will not be needed once https://github.com/flowable/flowable-engine/pull/3752
// is merged.
const flowablePatch = `
diff --git a/modules/flowable-engine-common/src/main/java/org/flowable/common/engine/impl/persistence/entity/TableDataManagerImpl.java b/modules/flowable-engine-common/src/main/java/org/flowable/common/engine/impl/persistence/entity/TableDataManagerImpl.java
index 38a332b785..620ed787bf 100644
--- a/modules/flowable-engine-common/src/main/java/org/flowable/common/engine/impl/persistence/entity/TableDataManagerImpl.java
+++ b/modules/flowable-engine-common/src/main/java/org/flowable/common/engine/impl/persistence/entity/TableDataManagerImpl.java
@@ -101,6 +101,12 @@ public class TableDataManagerImpl implements TableDataManager {
         List<String> tableNames = new ArrayList<>();
         try (ResultSet tables = databaseMetaData.getTables(catalog, schema, tableNameFilter, DbSqlSession.JDBC_METADATA_TABLE_TYPES)) {
             while (tables.next()) {
+                if ("cockroachdb".equals(getDbSqlSession().getDbSqlSessionFactory().getDatabaseType())) {
+                    String schemaName = tables.getString("TABLE_SCHEM");
+                    if ("crdb_internal".equals(schemaName)) {
+                        continue;
+                    }
+                }
                 String tableName = tables.getString("TABLE_NAME");
                 tableName = tableName.toUpperCase(Locale.ROOT);
                 tableNames.add(tableName);
`
