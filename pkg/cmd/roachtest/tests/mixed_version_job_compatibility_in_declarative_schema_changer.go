// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

func registerDeclarativeSchemaChangerJobCompatibilityInMixedVersion(r registry.Registry) {
	// declarative_schema_changer/job-compatibility-mixed-version tests that,
	// in a mixed version cluster, jobs created by the declarative schema changer
	// are both backward and forward compatible. That is, declarative schema
	// changer job created by nodes running newer (resp. older) binary versions
	// can be adopted and finished by nodes running older (resp. newer) binary versions.
	// DDLs in executeSupportedDDLs are version-gated so that only DDLs supported
	// by the active cluster version are executed.
	r.Add(registry.TestSpec{
		Name:    "declarative_schema_changer/job-compatibility-mixed-version",
		Owner:   registry.OwnerSQLFoundations,
		Cluster: r.MakeClusterSpec(4),
		// Disabled on IBM because s390x is only built on master and mixed-version
		// is impossible to test as of 05/2025.
		CompatibleClouds: registry.AllClouds.NoAWS().NoIBM(),
		Suites:           registry.Suites(registry.MixedVersion, registry.Nightly),
		Monitor:          true,
		Randomized:       true,
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
	// Ensure the system database has a longer TTL interval, which is needed to avoid
	// flakes on system database queries for upgrades.
	if err := h.Exec(r, "ALTER DATABASE system CONFIGURE ZONE USING gc.ttlseconds=60;"); err != nil {
		return err
	}
	return h.Exec(r, "ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds = 1;")
}

// executeSupportedDDLs tests DDL statements supported by the declarative
// schema changer, gated by the active cluster version. setDBOnAllConns
// is used to temporarily switch the session database on all connections
// to avoid "cross-db references not supported" errors for DDLs that
// reference functions or other objects with fully-qualified names.
func executeSupportedDDLs(
	ctx context.Context,
	c cluster.Cluster,
	t test.Test,
	helper *mixedversion.Helper,
	r *rand.Rand,
	testingUpgradedNodes bool,
	setDBOnAllConns func(helper *mixedversion.Helper, r *rand.Rand, db string) error,
) error {
	nodes := c.All().SeededRandNode(r)
	// We are not always guaranteed to be in a mixed-version binary state.
	// If we are, update the set of nodes; otherwise, we will choose a random
	// node.
	if helper.Context().MixedBinary() {
		if testingUpgradedNodes {
			// In this case, we test that older nodes are able to adopt desc. jobs from newer nodes.
			nodes = helper.Context().NodesInNextVersion() // N.B. this is the set of upgradedNodes.
		} else {
			// In this case, we test that newer nodes are able to adopt desc. jobs from older nodes.
			nodes = helper.Context().NodesInPreviousVersion() // N.B. this is the set of oldNodes.
		}
	}
	connectFunc := func(node int) (*gosql.DB, error) { return helper.Connect(node), nil }
	// NOTE: we intentionally don't call `testutils.CloseConnections()`
	// here because these connnections are managed by the mixedversion
	// framework, which already closes them at the end of the test.
	testUtils, err := newCommonTestUtils(
		ctx, t, c, connectFunc,
		helper.DefaultService().Descriptor.Nodes, helper.DefaultService().Descriptor.Nodes,
	)
	if err != nil {
		return err
	}
	// Disable job adoption for all nodes of the set we are testing [upgradedNodes, oldNodes] so that the
	// other respective set can adopt the job (ex. for upgradedNodes, we want the oldNodes to be able to adopt
	// these jobs).
	if err := testUtils.disableJobAdoption(ctx, t.L(), nodes); err != nil {
		return err
	}

	// DDLs supported since at least V25_3 (no version gate in DSC).
	v253DDLs := []string{
		`ALTER TABLE testdb.testsc.t2 ALTER COLUMN i DROP NOT NULL`,
		`DROP TRIGGER trg ON testdb.testsc.t`,
		`CREATE TRIGGER trg BEFORE INSERT ON testdb.testsc.t FOR EACH ROW EXECUTE FUNCTION testdb.testsc.trg_fn()`,
	}

	// DDLs supported in V25_4.
	v254DDLs := []string{
		`TRUNCATE testdb.testsc.t3`,
		`ALTER TABLE testdb.testsc.t2 RENAME TO t2_renamed`,
		`ALTER TABLE testdb.testsc.t2_renamed RENAME TO t2`,
		`ALTER TABLE testdb.testsc.t2 ALTER COLUMN j SET ON UPDATE 1`,
		`ALTER TABLE testdb.testsc.t2 RENAME COLUMN k TO k_renamed`,
		`ALTER TABLE testdb.testsc.t2 RENAME COLUMN k_renamed TO k`,
	}

	// DDLs supported in V26_1.
	v261DDLs := []string{
		`ALTER TABLE testdb.testsc.t2 ALTER COLUMN j SET NOT VISIBLE`,
		`ALTER TABLE testdb.testsc.t2 ALTER COLUMN j SET VISIBLE`,
		`ALTER TABLE testdb.testsc.t2 ALTER COLUMN l SET MAXVALUE 40 RESTART WITH 20 SET CACHE 5 SET INCREMENT BY 2`,
		`ALTER TABLE testdb.testsc.t RENAME CONSTRAINT check_j TO check_j_renamed`,
		`ALTER TABLE testdb.testsc.t RENAME CONSTRAINT check_j_renamed TO check_j`,
		`ALTER TABLE testdb.testsc.t SET (exclude_data_from_backup = true)`,
		`ALTER TABLE testdb.testsc.t RESET (exclude_data_from_backup)`,
		`ALTER TABLE testdb.testsc.t2 ALTER COLUMN l SET GENERATED BY DEFAULT`,
		`ALTER TABLE testdb.testsc.t2 ALTER COLUMN l SET GENERATED ALWAYS`,
	}

	// DDLs supported in V26_2.
	v262DDLs := []string{
		`ALTER SEQUENCE testdb.testsc.s MAXVALUE 100`,
		`ALTER TABLE testdb.testsc.t DISABLE TRIGGER trg`,
		`ALTER TABLE testdb.testsc.t ENABLE TRIGGER trg`,
		`ALTER TABLE testdb.testsc.t DISABLE TRIGGER ALL`,
		`ALTER TABLE testdb.testsc.t ENABLE TRIGGER ALL`,
		`ALTER TABLE testdb.testsc.t DISABLE TRIGGER USER`,
		`ALTER TABLE testdb.testsc.t ENABLE TRIGGER USER`,
		`CREATE OR REPLACE TRIGGER trg BEFORE UPDATE ON testdb.testsc.t FOR EACH ROW EXECUTE FUNCTION testdb.testsc.trg_fn()`,
		`CREATE OR REPLACE FUNCTION testdb.testsc.trg_fn() RETURNS TRIGGER LANGUAGE PLpgSQL AS $$ BEGIN RETURN NEW; END; $$`,
	}

	// Used to clean up our CREATE-d elements after we are done with them.
	// The trigger and function must be dropped while connected to testdb
	// to avoid "cross-database function references not allowed" errors.
	cleanupFromTestDB := []string{
		`DROP TRIGGER IF EXISTS trg ON testdb.testsc.t`,
		`DROP FUNCTION IF EXISTS testdb.testsc.trg_fn`,
	}
	cleanupFromDefaultDB := []string{
		`DROP INDEX testdb.testsc.t@idx`,
		`DROP SEQUENCE testdb.testsc.s`,
		`DROP TYPE testdb.testsc.typ`,
		`DROP VIEW testdb.testsc.v`,
		`DROP TABLE testdb.testsc.t3`,
		`DROP TABLE testdb.testsc.t2`,
		`DROP TABLE testdb.testsc.t`,
		`DROP SCHEMA testdb.testsc`,
		`DROP DATABASE testdb CASCADE`,
		`DROP OWNED BY foo`,
	}

	clusterVersion, err := helper.ClusterVersion(r)
	if err != nil {
		return err
	}

	// Switch to testdb before DDL execution. Some DDLs (CREATE TRIGGER,
	// CREATE FUNCTION) reference testdb.testsc objects and are rejected
	// with "cross-db references not supported" if the session's current
	// database differs from the catalog name in the reference.
	if err := setDBOnAllConns(helper, r, "testdb"); err != nil {
		return err
	}

	for _, ddl := range v253DDLs {
		if err := helper.ExecWithGateway(r, nodes, ddl); err != nil {
			return err
		}
	}
	if clusterVersion.AtLeast(clusterversion.V25_4.Version()) {
		for _, ddl := range v254DDLs {
			if err := helper.ExecWithGateway(r, nodes, ddl); err != nil {
				return err
			}
		}
	}
	if clusterVersion.AtLeast(clusterversion.V26_1.Version()) {
		for _, ddl := range v261DDLs {
			if err := helper.ExecWithGateway(r, nodes, ddl); err != nil {
				return err
			}
		}
	}
	if clusterVersion.AtLeast(clusterversion.V26_2.Version()) {
		for _, ddl := range v262DDLs {
			if err := helper.ExecWithGateway(r, nodes, ddl); err != nil {
				return err
			}
		}
	}

	// Drop trigger and function while still connected to testdb to
	// avoid "cross-database function references not allowed" errors.
	for _, ddl := range cleanupFromTestDB {
		if err := helper.ExecWithGateway(r, nodes, ddl); err != nil {
			return err
		}
	}

	// Switch back to defaultdb before dropping testdb itself.
	if err := setDBOnAllConns(helper, r, "defaultdb"); err != nil {
		return err
	}

	for _, ddl := range cleanupFromDefaultDB {
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
		ctx, t, t.L(), c, c.All(),
		// Disable version skipping and limit the test to only one upgrade as the workload is only
		// compatible with the branch it was built from and the major version before that.
		mixedversion.NumUpgrades(1),
		mixedversion.DisableSkipVersionUpgrades,
	)

	// Set up the testing state (e.g. create a few databases and tables) and always use declarative schema
	// changer on all nodes. Queries run in this function should be idempotent.
	// retryOpts defines the retry options for transient errors (e.g. lease
	// transfers) that can occur during setup and DDL execution.
	retryOpts := retry.Options{MaxBackoff: 5 * time.Second, MaxRetries: 5}

	// setDBOnAllConns temporarily sets the current database on all node
	// connections. This is needed because CREATE [OR REPLACE] FUNCTION
	// with a fully-qualified name that differs from the session's current
	// database is rejected with "cross-db references not supported".
	setDBOnAllConns := func(helper *mixedversion.Helper, r *rand.Rand, db string) error {
		for _, node := range c.All() {
			if err := helper.ExecWithRetry(r, option.NodeListOption{node}, retryOpts, "USE "+db); err != nil {
				return err
			}
		}
		return nil
	}

	testSetupResetStep := func(ctx context.Context, l *logger.Logger, r *rand.Rand, helper *mixedversion.Helper) error {

		// Ensure that the declarative schema changer is off so that we do not get failures related to unimplemented
		// statements in the dsc.
		// To make sure the session variables are applied correctly, we limit each
		// connection pool to have at most 1 connection.
		for _, node := range c.All() {
			db := helper.Connect(node)
			db.SetMaxOpenConns(1)
			if err := helper.ExecWithRetry(r, option.NodeListOption{node}, retryOpts, "SET use_declarative_schema_changer = off"); err != nil {
				return err
			}
		}

		// These setup queries run with DSC off (set above).
		setUpQueries := []string{
			"CREATE DATABASE IF NOT EXISTS testdb;",
			"CREATE SCHEMA IF NOT EXISTS testdb.testsc;",
			"CREATE TABLE IF NOT EXISTS testdb.testsc.t (i INT PRIMARY KEY, j INT NOT NULL, INDEX idx (j), CONSTRAINT check_j CHECK (j > 0));",
			"INSERT INTO testdb.testsc.t VALUES (1, 1);",
			"CREATE TABLE IF NOT EXISTS testdb.testsc.t2 (i INT NOT NULL, j INT NOT NULL, k STRING NOT NULL, l INT GENERATED ALWAYS AS IDENTITY);",
			"INSERT INTO testdb.testsc.t2 VALUES (2, 3, 'foo');",
			"CREATE TABLE IF NOT EXISTS testdb.testsc.t3 (i INT NOT NULL, j INT NOT NULL, k STRING NOT NULL);",
			"INSERT INTO testdb.testsc.t3 VALUES (3, 3, 'bar');",
			"CREATE TYPE IF NOT EXISTS testdb.testsc.typ AS ENUM ('a', 'b');",
			"CREATE SEQUENCE IF NOT EXISTS testdb.testsc.s;",
			"CREATE VIEW IF NOT EXISTS testdb.testsc.v AS (SELECT i*2 FROM testdb.testsc.t);",
		}
		// Execute queries one at a time with an implicit txn and retry logic,
		// since transient errors (e.g. lease transfers from the
		// kv.expiration_leases_only.enabled mutator) can cause failures.
		for _, setupQuery := range setUpQueries {
			if err := helper.ExecWithRetry(r, c.All(), retryOpts, setupQuery); err != nil {
				return err
			}
		}

		// Create the trigger function with DSC off. The DSC only added
		// support for CREATE OR REPLACE FUNCTION in v26.2, so on older
		// versions this must go through the legacy schema changer.
		// Switch to testdb first to avoid cross-database reference errors.
		if err := setDBOnAllConns(helper, r, "testdb"); err != nil {
			return err
		}
		if err := helper.ExecWithRetry(r, c.All(), retryOpts,
			`CREATE OR REPLACE FUNCTION testdb.testsc.trg_fn() RETURNS TRIGGER LANGUAGE PLpgSQL AS $$ BEGIN RETURN NEW; END; $$;`,
		); err != nil {
			return err
		}

		// Set all nodes to always use declarative schema changer
		// so that we don't fall back to legacy schema changer implicitly.
		// Being explicit can help catch bugs that will otherwise be
		// buried by the fallback.
		for _, node := range c.All() {
			if err := helper.ExecWithRetry(r, option.NodeListOption{node}, retryOpts, "SET use_declarative_schema_changer = unsafe_always"); err != nil {
				return err
			}
		}

		// DROP TRIGGER and CREATE TRIGGER require the DSC (the legacy
		// schema changer does not support them). Run these still
		// connected to testdb, then switch back to defaultdb.
		triggerQueries := []string{
			"DROP TRIGGER IF EXISTS trg ON testdb.testsc.t;",
			"CREATE TRIGGER trg BEFORE INSERT ON testdb.testsc.t FOR EACH ROW EXECUTE FUNCTION testdb.testsc.trg_fn();",
		}
		for _, setupQuery := range triggerQueries {
			if err := helper.ExecWithRetry(r, c.All(), retryOpts, setupQuery); err != nil {
				return err
			}
		}
		if err := setDBOnAllConns(helper, r, "defaultdb"); err != nil {
			return err
		}
		return nil
	}

	testStep := func(ctx context.Context, l *logger.Logger, r *rand.Rand, helper *mixedversion.Helper) error {
		if err := testSetupResetStep(ctx, l, r, helper); err != nil {
			return err
		}
		// Job backward compatibility test:
		//   - upgraded nodes: plan schema change and create schema changer jobs
		//   - older nodes: adopt and execute schema changer jobs
		// Testing that declarative schema change jobs created by nodes running newer binary version can be adopted and
		// finished by nodes running older binary versions.
		if err := executeSupportedDDLs(ctx, c, t, helper, r, true /* testingUpgradedNodes */, setDBOnAllConns); err != nil {
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
		return executeSupportedDDLs(ctx, c, t, helper, r, false /* testingUpgradedNodes */, setDBOnAllConns)
	}

	// We want declarative schema change jobs to be adopted quickly after creation
	mvt.OnStartup("set short job interval", setShortJobIntervalsStep)
	mvt.OnStartup("set short gcttl", setShortGCTTLInSystemZoneConfig)

	mvt.InMixedVersion("test step in mixed version", testStep)
	mvt.AfterUpgradeFinalized("test step after upgrade has finalized", testStep)

	mvt.Run()
}
