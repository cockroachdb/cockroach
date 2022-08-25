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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/stretchr/testify/require"
)

func schemaChangeSetUpStep(c cluster.Cluster, node int) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		db := c.Conn(ctx, t.L(), node)
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
	}
}

func planAndRunSchemaChange(
	c cluster.Cluster, nodeToPlanSchemaChange option.NodeListOption, schemaChangeStmt string,
) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		gatewayDB := c.Conn(ctx, t.L(), nodeToPlanSchemaChange[0])
		defer gatewayDB.Close()
		t.Status("Running: ", schemaChangeStmt)
		_, err := gatewayDB.ExecContext(ctx, schemaChangeStmt)
		require.NoError(t, err)
	}
}

func registerDeclarativeSchemaChangerBackwardCompatibilityMixedVersions(r registry.Registry) {
	// declarative_schema_changer/job-backward-compatibility-mixed-version tests that,
	// in a mixed version cluster, declarative schema changer in nodes running
	// newer binary versions creates jobs that can be adopted and finished by
	// nodes running older binary versions.
	// Specifically, in a mixed version cluster of v22.1 and v22.2, we let nodes running
	// on v22.1 to plan a schema change for DDL stmts supported in both v22.1 and v22.2,
	// and let nodes running on v22.2 to adopt and finish those jobs.
	r.Add(registry.TestSpec{
		Name:    "declarative_schema_changer/job-backward-compatibility-mixed-version",
		Owner:   registry.OwnerSQLSchema,
		Cluster: r.MakeClusterSpec(4),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			// An empty string means that the cockroach binary specified by flag
			// `cockroach` will be used.
			const mainVersion = ""
			roachNodes := c.All()
			upgradedNodes := c.Nodes(1, 2)
			//oldNodes := c.Nodes(3, 4)
			predV, err := PredecessorVersion(*t.BuildVersion())
			require.NoError(t, err)
			//c.Put(ctx, t.DeprecatedWorkload(), "./workload")

			u := newVersionUpgradeTest(c,
				uploadAndStartFromCheckpointFixture(roachNodes, predV),
				waitForUpgradeStep(roachNodes),
				preventAutoUpgradeStep(1),
				setShortJobIntervalsStep(1),
				schemaChangeSetUpStep(c, 1),

				// Upgrade some nodes.
				binaryUpgradeStep(upgradedNodes, mainVersion),

				// plan schema change    -> upgraded node
				// execute schema change -> old node
				//
				// Halt job execution on upgraded nodes.
				disableJobAdoptionStep(c, upgradedNodes),

				// Run a schema change from a new node so that it is planned on the new node
				// but the job is adopted on an old node.
				planAndRunSchemaChange(c, upgradedNodes.RandNode(), `DROP SEQUENCE testdb.testsc.s`),
				planAndRunSchemaChange(c, upgradedNodes.RandNode(), `DROP TYPE testdb.testsc.typ`),
				planAndRunSchemaChange(c, upgradedNodes.RandNode(), `DROP VIEW testdb.testsc.v`),
				planAndRunSchemaChange(c, upgradedNodes.RandNode(), `DROP TABLE testdb.testsc.t`),
				planAndRunSchemaChange(c, upgradedNodes.RandNode(), `DROP SCHEMA testdb.testsc`),
				planAndRunSchemaChange(c, upgradedNodes.RandNode(), `DROP DATABASE testdb CASCADE`),

				enableJobAdoptionStep(c, upgradedNodes),
			)
			u.run(ctx, t)
		},
	})
}
