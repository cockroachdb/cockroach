// Copyright 2022 The Cockroach Authors.
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
	"runtime"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/stretchr/testify/require"
)

// testSetupResetStep setups the testing state (e.g. create a few databases and tables)
// and always use declarative schema changer on all nodes.
// Queries run in this function should be idempotent.
func testSetupResetStep(c cluster.Cluster) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		db := c.Conn(ctx, t.L(), 1)
		setUpQuery := `
CREATE DATABASE IF NOT EXISTS testdb;
CREATE SCHEMA IF NOT EXISTS testdb.testsc;
CREATE TABLE IF NOT EXISTS testdb.testsc.t (i INT PRIMARY KEY, j INT NOT NULL, INDEX idx (j), CONSTRAINT check_j CHECK (j > 0));
INSERT INTO testdb.testsc.t VALUES (1, 1);
CREATE TABLE IF NOT EXISTS testdb.testsc.t2 (i INT NOT NULL, j INT NOT NULL);
INSERT INTO testdb.testsc.t2 VALUES (1, 1);
CREATE TYPE IF NOT EXISTS testdb.testsc.typ AS ENUM ('a', 'b');
CREATE SEQUENCE IF NOT EXISTS testdb.testsc.s;
CREATE VIEW IF NOT EXISTS testdb.testsc.v AS (SELECT i*2 FROM testdb.testsc.t);
`
		_, err := db.ExecContext(ctx, setUpQuery)
		require.NoError(t, err)

		// Set all nodes to always use declarative schema changer
		// so that we don't fall back to legacy schema changer implicitly.
		// Being explicit can help catch bugs that will otherwise be
		// buried by the fallback.
		for _, node := range c.All() {
			db = c.Conn(ctx, t.L(), node)
			_, err = db.ExecContext(ctx, "SET use_declarative_schema_changer = unsafe_always")
			require.NoError(t, err)
		}
	}
}

// setShortGCTTLInSystemZoneConfig sets gc.ttlseconds in the default zone config
// to be 1 second.
func setShortGCTTLInSystemZoneConfig(c cluster.Cluster) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		db := c.Conn(ctx, t.L(), 1)
		_, err := db.ExecContext(ctx, "ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds = 1;")
		require.NoError(t, err)
	}
}

// planAndRunSchemaChange runs a schema change stmt from a particular node.
func planAndRunSchemaChange(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	node option.NodeListOption,
	schemaChangeStmt string,
) {
	gatewayDB := c.Conn(ctx, t.L(), node[0])
	defer gatewayDB.Close()
	t.Status("Running: ", schemaChangeStmt)
	_, err := gatewayDB.ExecContext(ctx, schemaChangeStmt)
	require.NoError(t, err)
}

// testSchemaChangesInMixedVersionV222AndV231 tests all stmts supported in V22_2.
// Stmts here is based on set up in testSetupResetStep.
func testSchemaChangesInMixedVersionV222AndV231(
	c cluster.Cluster, nodeIDs option.NodeListOption,
) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		planAndRunSchemaChange(ctx, t, c, nodeIDs.RandNode(), `COMMENT ON DATABASE testdb IS 'this is a database comment'`)
		planAndRunSchemaChange(ctx, t, c, nodeIDs.RandNode(), `COMMENT ON SCHEMA testdb.testsc IS 'this is a schema comment'`)
		planAndRunSchemaChange(ctx, t, c, nodeIDs.RandNode(), `COMMENT ON TABLE testdb.testsc.t IS 'this is a table comment'`)
		planAndRunSchemaChange(ctx, t, c, nodeIDs.RandNode(), `COMMENT ON COLUMN testdb.testsc.t.i IS 'this is a column comment'`)
		planAndRunSchemaChange(ctx, t, c, nodeIDs.RandNode(), `COMMENT ON INDEX testdb.testsc.t@idx IS 'this is a index comment'`)
		planAndRunSchemaChange(ctx, t, c, nodeIDs.RandNode(), `COMMENT ON CONSTRAINT check_j ON testdb.testsc.t IS 'this is a constraint comment'`)
		planAndRunSchemaChange(ctx, t, c, nodeIDs.RandNode(), `ALTER TABLE testdb.testsc.t ADD COLUMN k INT DEFAULT 35`)
		planAndRunSchemaChange(ctx, t, c, nodeIDs.RandNode(), `ALTER TABLE testdb.testsc.t DROP COLUMN k`)
		planAndRunSchemaChange(ctx, t, c, nodeIDs.RandNode(), `ALTER TABLE testdb.testsc.t2 ADD PRIMARY KEY (i)`)
		planAndRunSchemaChange(ctx, t, c, nodeIDs.RandNode(), `ALTER TABLE testdb.testsc.t2 ALTER PRIMARY KEY USING COLUMNS (j)`)
		planAndRunSchemaChange(ctx, t, c, nodeIDs.RandNode(), `DROP SEQUENCE testdb.testsc.s`)
		planAndRunSchemaChange(ctx, t, c, nodeIDs.RandNode(), `DROP TYPE testdb.testsc.typ`)
		planAndRunSchemaChange(ctx, t, c, nodeIDs.RandNode(), `DROP VIEW testdb.testsc.v`)
		planAndRunSchemaChange(ctx, t, c, nodeIDs.RandNode(), `DROP TABLE testdb.testsc.t`)
		planAndRunSchemaChange(ctx, t, c, nodeIDs.RandNode(), `DROP TABLE testdb.testsc.t2`)
		planAndRunSchemaChange(ctx, t, c, nodeIDs.RandNode(), `DROP SCHEMA testdb.testsc`)
		planAndRunSchemaChange(ctx, t, c, nodeIDs.RandNode(), `DROP DATABASE testdb CASCADE`)
	}
}

func setShortJobIntervalsStep(node int) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, node)
		runQuery := func(query string, args ...interface{}) error {
			_, err := db.ExecContext(ctx, query, args...)
			return err
		}

		if err := setShortJobIntervalsCommon(runQuery); err != nil {
			t.Fatal(err)
		}
	}
}

func registerDeclarativeSchemaChangerJobCompatibilityInMixedVersion(r registry.Registry) {
	// declarative_schema_changer/job-compatibility-mixed-version tests that,
	// in a mixed version cluster, jobs created by the declarative schema changer
	// are both backward and forward compatible. That is, declarative schema
	// changer job created by nodes running newer (resp. older) binary versions
	// can be adopted and finished by nodes running older (resp. newer) binary versions.
	// This test requires us to come back and change the to-be-tests stmts to be those
	// supported in the "previous" major release.
	r.Add(registry.TestSpec{
		Name:    "declarative_schema_changer/job-compatibility-mixed-version-V222-V231",
		Owner:   registry.OwnerSQLFoundations,
		Cluster: r.MakeClusterSpec(4),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.IsLocal() && runtime.GOARCH == "arm64" {
				t.Skip("Skip under ARM64. See https://github.com/cockroachdb/cockroach/issues/89268")
			}
			predV, err := version.PredecessorVersion(*t.BuildVersion())
			require.NoError(t, err)

			allNodes := c.All()
			upgradedNodes := c.Nodes(1, 2)
			oldNodes := c.Nodes(3, 4)

			u := newVersionUpgradeTest(c,
				// System setup.
				uploadAndStartFromCheckpointFixture(allNodes, predV),
				waitForUpgradeStep(allNodes),
				preventAutoUpgradeStep(1),
				setShortJobIntervalsStep(1),
				setShortGCTTLInSystemZoneConfig(c),

				// Upgrade some nodes.
				binaryUpgradeStep(upgradedNodes, clusterupgrade.MainVersion),

				// Job backward compatibility test:
				//   - upgraded nodes: plan schema change and create schema changer jobs
				//   - older nodes: adopt and execute schema changer jobs
				testSetupResetStep(c),
				disableJobAdoptionStep(c, upgradedNodes),
				testSchemaChangesInMixedVersionV222AndV231(c, upgradedNodes),
				enableJobAdoptionStep(c, upgradedNodes),

				// Job forward compatibility test:
				//   - older nodes: plan schema change and create schema changer jobs
				//   - upgraded nodes: adopt and execute schema changer jobs
				testSetupResetStep(c),
				disableJobAdoptionStep(c, oldNodes),
				testSchemaChangesInMixedVersionV222AndV231(c, oldNodes),
				enableJobAdoptionStep(c, oldNodes),
			)
			u.run(ctx, t)
		},
	})
}
