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
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

func registerDeclarativeSchemaChangerJobCompatibilityInMixedVersion(r registry.Registry) {
	// declarative_schema_changer/job-compatibility-mixed-version tests that,
	// in a mixed version cluster, jobs created by the declarative schema changer
	// are both backward and forward compatible. That is, declarative schema
	// changer job created by nodes running newer (resp. older) binary versions
	// can be adopted and finished by nodes running older (resp. newer) binary versions.
	// This test requires us to come back and change the stmts in executeSupportedDDLs to be those
	// supported in the "previous" major release.
	r.Add(registry.TestSpec{
		Name:             "declarative_schema_changer/job-compatibility-mixed-version-V222-V231",
		Owner:            registry.OwnerSQLFoundations,
		Cluster:          r.MakeClusterSpec(4),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runDeclarativeSchemaChangerJobCompatibilityInMixedVersion(ctx, t, c)
		},
	})
}

// setShortJobIntervalsStep sets the jobs.registry.interval.cancel and .adopt cluster setting to
// be 1 second.
func setShortJobIntervalsStep(
	ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper,
) error {
	runQuery := func(query string, args ...interface{}) error {
		return h.Exec(r, query, args...)
	}

	return setShortJobIntervalsCommon(runQuery)
}

// setShortGCTTLInSystemZoneConfig sets gc.ttlseconds in the default zone config
// to be 1 second.
func setShortGCTTLInSystemZoneConfig(
	ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper,
) error {
	return h.Exec(r, "ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds = 1;")
}

// executeSupportedDDLs tests all stmts supported in V22_2.
// Stmts here is based on set up in testSetupResetStep.
func executeSupportedDDLs(
	ctx context.Context,
	c cluster.Cluster,
	t test.Test,
	helper *mixedversion.Helper,
	r *rand.Rand,
	testingUpgradedNodes bool,
) error {
	nodes := c.All().RandNode()
	// We are not always guaranteed to be in a mixed-version binary state.
	// If we are, update the set of nodes; otherwise, we will choose a random
	// node.
	if helper.Context.MixedBinary() {
		if testingUpgradedNodes {
			// In this case, we test that older nodes are able to adopt desc. jobs from newer nodes.
			nodes = helper.Context.NodesInNextVersion() // N.B. this is the set of upgradedNodes.
		} else {
			// In this case, we test that newer nodes are able to adopt desc. jobs from older nodes.
			nodes = helper.Context.NodesInPreviousVersion() // N.B. this is the set of oldNodes.
		}
	}
	testUtils, err := newCommonTestUtils(ctx, t, c, helper.Context.CockroachNodes)
	if err != nil {
		return err
	}
	// Disable job adoption for all nodes of the set we are testing [upgradedNodes, oldNodes] so that the
	// other respective set can adopt the job (ex. for upgradedNodes, we want the oldNodes to be able to adopt
	// these jobs).
	if err := testUtils.disableJobAdoption(ctx, t.L(), nodes); err != nil {
		return err
	}

	// DDLs supported since V22_2.
	v222DDLs := []string{
		`COMMENT ON DATABASE testdb IS 'this is a database comment'`,
		`COMMENT ON SCHEMA testdb.testsc IS 'this is a schema comment'`,
		`COMMENT ON TABLE testdb.testsc.t IS 'this is a table comment'`,
		`COMMENT ON COLUMN testdb.testsc.t.i IS 'this is a column comment'`,
		`COMMENT ON INDEX testdb.testsc.t@idx IS 'this is a index comment'`,
		`COMMENT ON CONSTRAINT check_j ON testdb.testsc.t IS 'this is a constraint comment'`,
		`ALTER TABLE testdb.testsc.t ADD COLUMN k INT DEFAULT 35`,
		`ALTER TABLE testdb.testsc.t DROP COLUMN k`,
		`ALTER TABLE testdb.testsc.t2 ADD PRIMARY KEY (i)`,
		`ALTER TABLE testdb.testsc.t2 ALTER PRIMARY KEY USING COLUMNS (j)`,
	}

	// Used to clean up our CREATE-d elements after we are done with them.
	cleanup := []string{
		// Supported since V22_2.
		`DROP SEQUENCE testdb.testsc.s`,
		`DROP TYPE testdb.testsc.typ`,
		`DROP VIEW testdb.testsc.v`,
		`DROP TABLE testdb.testsc.t`,
		`DROP TABLE testdb.testsc.t2`,
		`DROP SCHEMA testdb.testsc`,
		`DROP DATABASE testdb CASCADE`,
	}

	ddls := append(v222DDLs, cleanup...)

	for _, ddl := range ddls {
		if err := helper.ExecWithGateway(r, nodes, ddl); err != nil {
			return err
		}
	}
	return testUtils.enableJobAdoption(ctx, t.L(), nodes)
}

func runDeclarativeSchemaChangerJobCompatibilityInMixedVersion(
	ctx context.Context, t test.Test, c cluster.Cluster,
) {
	mvt := mixedversion.NewTest(
		ctx, t, t.L(), c, c.All(), mixedversion.NumUpgrades(1),
	)

	// Set up the testing state (e.g. create a few databases and tables) and always use declarative schema
	// changer on all nodes. Queries run in this function should be idempotent.
	testSetupResetStep := func(ctx context.Context, l *logger.Logger, r *rand.Rand, helper *mixedversion.Helper) error {

		// Ensure that the declarative schema changer is off so that we do not get failures related to unimplemented
		// statements in the dsc.
		for _, node := range c.All() {
			if err := helper.ExecWithGateway(r, option.NodeListOption{node}, "SET use_declarative_schema_changer = off"); err != nil {
				return err
			}
		}

		setUpQuery := `
		CREATE DATABASE IF NOT EXISTS testdb;
		CREATE SCHEMA IF NOT EXISTS testdb.testsc;
		CREATE TABLE IF NOT EXISTS testdb.testsc.t (i INT PRIMARY KEY, j INT NOT NULL, INDEX idx (j), CONSTRAINT check_j CHECK (j > 0));
		INSERT INTO testdb.testsc.t VALUES (1, 1);
		CREATE TABLE IF NOT EXISTS testdb.testsc.t2 (i INT NOT NULL, j INT NOT NULL, k STRING NOT NULL);
		INSERT INTO testdb.testsc.t2 VALUES (1, 1, 'foo');
		CREATE TYPE IF NOT EXISTS testdb.testsc.typ AS ENUM ('a', 'b');
		CREATE SEQUENCE IF NOT EXISTS testdb.testsc.s;
		CREATE VIEW IF NOT EXISTS testdb.testsc.v AS (SELECT i*2 FROM testdb.testsc.t);
`

		if err := helper.Exec(r, setUpQuery); err != nil {
			return err
		}

		// Set all nodes to always use declarative schema changer
		// so that we don't fall back to legacy schema changer implicitly.
		// Being explicit can help catch bugs that will otherwise be
		// buried by the fallback.
		for _, node := range c.All() {
			if err := helper.ExecWithGateway(r, option.NodeListOption{node}, "SET use_declarative_schema_changer = unsafe_always"); err != nil {
				return err
			}
		}
		return nil
	}

	// We want declarative schema change jobs to be adopted quickly after creation
	mvt.OnStartup("set short job interval", setShortJobIntervalsStep)
	mvt.OnStartup("set short gcttl", setShortGCTTLInSystemZoneConfig)
	mvt.OnStartup("set up testing state", testSetupResetStep)

	mvt.InMixedVersion("run test step", func(ctx context.Context, l *logger.Logger, r *rand.Rand, helper *mixedversion.Helper) error {
		// Job backward compatibility test:
		//   - upgraded nodes: plan schema change and create schema changer jobs
		//   - older nodes: adopt and execute schema changer jobs
		// Testing that declarative schema change jobs created by nodes running newer binary version can be adopted and
		// finished by nodes running older binary versions.
		if err := executeSupportedDDLs(ctx, c, t, helper, r, true /* testingUpgradedNodes */); err != nil {
			return err
		}
		// Recreate testing state.
		if err := testSetupResetStep(ctx, l, r, helper); err != nil {
			return err
		}
		// Job forward compatibility test:
		//   - older nodes: plan schema change and create schema changer jobs
		//   - upgraded nodes: adopt and execute schema changer jobs
		// Testing that declarative schema change jobs created by nodes running older binary version can be adopted and
		// finished by nodes running newer binary versions.
		return executeSupportedDDLs(ctx, c, t, helper, r, false /* testingUpgradedNodes */)
	})
	mvt.Run()
}
