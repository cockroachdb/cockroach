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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/stretchr/testify/require"
)

// testSetupResetStep setups the testing state (e.g. create a few databases and tables)
// and always use declarative schema changer on all nodes.
func testSetupResetStep(c cluster.Cluster) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		db := c.Conn(ctx, t.L(), 1, "")
		setUpQuery := `
CREATE DATABASE testdb; 
CREATE SCHEMA testdb.testsc; 
CREATE TABLE testdb.testsc.t (i INT PRIMARY KEY);
CREATE TYPE testdb.testsc.typ AS ENUM ('a', 'b');
CREATE SEQUENCE testdb.testsc.s;
CREATE VIEW testdb.testsc.v AS (SELECT i*2 FROM testdb.testsc.t);
`
		_, err := db.ExecContext(ctx, setUpQuery)
		require.NoError(t, err)

		// Set all nodes to always use declarative schema changer
		// so that we don't fall back to legacy schema changer implicitly.
		// Being explicit can help catch bugs that will otherwise be
		// buried by the fallback.
		for _, node := range c.All() {
			db = c.Conn(ctx, t.L(), node, "")
			_, err = db.ExecContext(ctx, "SET use_declarative_schema_changer = unsafe_always")
			require.NoError(t, err)
		}
	}
}

// setShortGCTTLInSystemZoneConfig sets gc.ttlseconds in the default zone config
// to be 1 second.
func setShortGCTTLInSystemZoneConfig(c cluster.Cluster) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		db := c.Conn(ctx, t.L(), 1, "")
		_, err := db.ExecContext(ctx, "ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds = 1;")
		require.NoError(t, err)
	}
}

func planAndRunSchemaChange(
	c cluster.Cluster, nodeToPlanSchemaChange option.NodeListOption, schemaChangeStmt string,
) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		gatewayDB := c.Conn(ctx, t.L(), nodeToPlanSchemaChange[0], "")
		defer gatewayDB.Close()
		t.Status("Running: ", schemaChangeStmt)
		_, err := gatewayDB.ExecContext(ctx, schemaChangeStmt)
		require.NoError(t, err)
	}
}

func registerDeclarativeSchemaChangerJobCompatibilityInMixedVersion(r registry.Registry) {
	// declarative_schema_changer/job-compatibility-mixed-version tests that,
	// in a mixed version cluster, jobs created by the declarative schema changer
	// are both backward and forward compatible. That is, declarative schema
	// changer job created by nodes running newer (resp. older) binary versions
	// be adopted and finished by nodes running older (resp. newer) binary versions.
	// This test requires us to come back and change the to-be-tests stmts to be those
	// supported in the "previous" major release.
	r.Add(registry.TestSpec{
		Name:    "declarative_schema_changer/job-compatibility-mixed-version",
		Owner:   registry.OwnerSQLSchema,
		Cluster: r.MakeClusterSpec(4),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			// Skip this roachtest until master is on 23.1
			skip.WithIssue(t, 89345)

			if c.IsLocal() && runtime.GOARCH == "arm64" {
				t.Skip("Skip under ARM64. See https://github.com/cockroachdb/cockroach/issues/89268")
			}
			// An empty string means that the cockroach binary specified by flag
			// `cockroach` will be used.
			const mainVersion = ""
			allNodes := c.All()
			upgradedNodes := c.Nodes(1, 2)
			oldNodes := c.Nodes(3, 4)
			predV, err := PredecessorVersion(*t.BuildVersion())
			require.NoError(t, err)

			u := newVersionUpgradeTest(c,
				// System setup.
				uploadAndStartFromCheckpointFixture(allNodes, predV),
				waitForUpgradeStep(allNodes),
				preventAutoUpgradeStep(1),
				setShortJobIntervalsStep(1),
				setShortGCTTLInSystemZoneConfig(c),

				// Upgrade some nodes.
				binaryUpgradeStep(upgradedNodes, mainVersion),

				// Job backward compatibility test:
				//   - upgraded nodes: plan schema change and create schema changer jobs
				//   - older nodes: adopt and execute schema changer jobs
				testSetupResetStep(c),

				// Halt job execution on upgraded nodes.
				disableJobAdoptionStep(c, upgradedNodes),

				// Run schema change stmts, chosen from those supported in `predV` version.
				planAndRunSchemaChange(c, upgradedNodes.RandNode(), `DROP SEQUENCE testdb.testsc.s`),
				planAndRunSchemaChange(c, upgradedNodes.RandNode(), `DROP TYPE testdb.testsc.typ`),
				planAndRunSchemaChange(c, upgradedNodes.RandNode(), `DROP VIEW testdb.testsc.v`),
				planAndRunSchemaChange(c, upgradedNodes.RandNode(), `DROP TABLE testdb.testsc.t`),
				planAndRunSchemaChange(c, upgradedNodes.RandNode(), `DROP SCHEMA testdb.testsc`),
				planAndRunSchemaChange(c, upgradedNodes.RandNode(), `DROP DATABASE testdb CASCADE`),
				enableJobAdoptionStep(c, upgradedNodes),

				// Job forward compatibility test:
				//   - older nodes: plan schema change and create schema changer jobs
				//   - upgraded nodes: adopt and execute schema changer jobs
				testSetupResetStep(c),

				// Halt job execution on old nodes.
				disableJobAdoptionStep(c, oldNodes),

				// Run schema change stmts, chosen from those supported in `predV` version.
				planAndRunSchemaChange(c, oldNodes.RandNode(), `DROP SEQUENCE testdb.testsc.s`),
				planAndRunSchemaChange(c, oldNodes.RandNode(), `DROP TYPE testdb.testsc.typ`),
				planAndRunSchemaChange(c, oldNodes.RandNode(), `DROP VIEW testdb.testsc.v`),
				planAndRunSchemaChange(c, oldNodes.RandNode(), `DROP TABLE testdb.testsc.t`),
				planAndRunSchemaChange(c, oldNodes.RandNode(), `DROP SCHEMA testdb.testsc`),
				planAndRunSchemaChange(c, oldNodes.RandNode(), `DROP DATABASE testdb CASCADE`),
				enableJobAdoptionStep(c, oldNodes),
			)
			u.run(ctx, t)
		},
	})
}
