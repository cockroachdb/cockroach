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
	gosql "database/sql"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/release"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
)

type versionFeatureTest struct {
	name      string
	statement string
}

// Feature tests that are invoked in mixed-version state during the
// upgrade test.  A gotcha is that these feature tests are also
// invoked when the cluster is in the middle of upgrading -- i.e. a
// state where the cluster version has already been bumped, but not
// all nodes are aware). This should be considered a feature of this
// test, and feature tests that flake because of it need to be fixed.
var versionUpgradeTestFeatures = []versionFeatureTest{
	// NB: the next four tests are ancient and supported since v2.0.
	{
		name: "ObjectAccess",
		statement: `
-- We should be able to successfully select from objects created in ancient
-- versions of CRDB using their FQNs. Prevents bugs such as #43141, where
-- databases created before a migration were inaccessible after the
-- migration.
--
-- NB: the data has been baked into the fixtures. Originally created via:
--   create database persistent_db
--   create table persistent_db.persistent_table(a int)"))
-- on CRDB v1.0
select * from persistent_db.persistent_table;
show tables from persistent_db;
`,
	},
	{
		name: "JSONB",
		statement: `
CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE test.t (j JSONB);
DROP TABLE test.t;
`,
	},
	{
		name: "Sequences",
		statement: `
CREATE DATABASE IF NOT EXISTS test;
CREATE SEQUENCE test.test_sequence;
DROP SEQUENCE test.test_sequence;
`,
	},
	{
		name: "Computed Columns",
		statement: `
CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE test.t (x INT AS (3) STORED);
DROP TABLE test.t;
`,
	},
	{
		name: "Split and Merge Ranges",
		statement: `
CREATE DATABASE IF NOT EXISTS splitmerge;
CREATE TABLE splitmerge.t (k INT PRIMARY KEY);
ALTER TABLE splitmerge.t SPLIT AT VALUES (1), (2), (3);
ALTER TABLE splitmerge.t UNSPLIT AT VALUES (1), (2), (3);
DROP TABLE splitmerge.t;
`,
	},
}

func runVersionUpgrade(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.All())
	mvt := mixedversion.NewTest(ctx, t, t.L(), c, c.All(), mixedversion.AlwaysUseFixtures)
	mvt.OnStartup(
		"setup schema changer workload",
		func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
			node := h.RandomNode(rng, c.All())
			l.Printf("executing workload init on node %d", node)
			return c.RunE(ctx, c.Node(node), fmt.Sprintf("./workload init schemachange {pgurl%s}", c.All()))
		})
	mvt.InMixedVersion(
		"run backup",
		func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
			// Verify that backups can be created in various configurations. This is
			// important to test because changes in system tables might cause backups to
			// fail in mixed-version clusters.
			dest := fmt.Sprintf("nodelocal://1/%d", timeutil.Now().UnixNano())
			return h.Exec(rng, `BACKUP TO $1`, dest)
		})
	mvt.InMixedVersion(
		"test features",
		func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
			for _, featureTest := range versionUpgradeTestFeatures {
				l.Printf("running feature test %q", featureTest.name)
				if err := h.Exec(rng, featureTest.statement); err != nil {
					l.Printf("%q: ERROR (%s)", featureTest.name, err)
					return err
				}
				l.Printf("%q: OK", featureTest.name)
			}

			return nil
		},
	)
	mvt.InMixedVersion(
		"test schema change step",
		func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
			tc := h.Context()
			// We currently only stage the `workload` binary built off the
			// SHA being tested; therefore, we skip testing the schemachange
			// workload if this is not an upgrade or downgrade involving the
			// current cockroach binary.
			// TODO(renato): stage different workload binaries for the
			// releases being used in the test and use the appropriate
			// binary in this step.
			if tc.FromVersion != clusterupgrade.MainVersion && tc.ToVersion != clusterupgrade.MainVersion {
				l.Printf("skipping this step -- only supported when current version is involved")
				return nil
			}

			l.Printf("running schema workload step")
			runCmd := roachtestutil.NewCommand("./workload run schemachange").Flag("verbose", 1).Flag("max-ops", 10).Flag("concurrency", 2).Arg("{pgurl:1-%d}", len(c.All()))
			randomNode := h.RandomNode(rng, c.All())
			return c.RunE(ctx, option.NodeListOption{randomNode}, runCmd.String())
		},
	)

	mvt.Run()
}

func (u *versionUpgradeTest) run(ctx context.Context, t test.Test) {
	defer func() {
		for _, db := range u.conns {
			_ = db.Close()
		}
	}()

	for i, step := range u.steps {
		if step != nil {
			t.Status(fmt.Sprintf("versionUpgradeTest: starting step %d", i+1))
			step(ctx, t, u)
		}
	}
}

type versionUpgradeTest struct {
	goOS  string
	c     cluster.Cluster
	steps []versionStep

	// Cache conns because opening one takes hundreds of ms, and we do it quite
	// a lot.
	conns []*gosql.DB
}

func newVersionUpgradeTest(c cluster.Cluster, steps ...versionStep) *versionUpgradeTest {
	return &versionUpgradeTest{
		goOS:  ifLocal(c, runtime.GOOS, "linux"),
		c:     c,
		steps: steps,
	}
}

// Return a cached conn to the given node. Don't call .Close(), the test harness
// will do it.
func (u *versionUpgradeTest) conn(ctx context.Context, t test.Test, i int) *gosql.DB {
	if u.conns == nil {
		for _, i := range u.c.All() {
			u.conns = append(u.conns, u.c.Conn(ctx, t.L(), i))
		}
	}
	db := u.conns[i-1]
	// Run a trivial query to shake out errors that can occur when the server has
	// restarted in the meantime.
	_ = db.PingContext(ctx)
	return db
}

// uploadVersion is a thin wrapper around
// `clusterupgrade.UploadVersion` that calls t.Fatal if that call
// returns an error
func uploadVersion(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	nodes option.NodeListOption,
	newVersion string,
) string {
	path, err := clusterupgrade.UploadVersion(ctx, t, t.L(), c, nodes, newVersion)
	if err != nil {
		t.Fatal(err)
	}

	return path
}

// upgradeNodes is a thin wrapper around
// `clusterupgrade.RestartNodesWithNewBinary` that calls t.Fatal if
// that call returns an errror.
func upgradeNodes(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	nodes option.NodeListOption,
	startOpts option.StartOpts,
	newVersion string,
) {
	if err := clusterupgrade.RestartNodesWithNewBinary(
		ctx, t, t.L(), c, nodes, startOpts, newVersion,
	); err != nil {
		t.Fatal(err)
	}
}

func (u *versionUpgradeTest) binaryVersion(
	ctx context.Context, t test.Test, i int,
) roachpb.Version {
	db := u.conn(ctx, t, i)
	v, err := clusterupgrade.BinaryVersion(db)
	if err != nil {
		t.Fatal(err)
	}

	return v
}

// versionStep is an isolated version migration on a running cluster.
type versionStep func(ctx context.Context, t test.Test, u *versionUpgradeTest)

func uploadAndStartFromCheckpointFixture(nodes option.NodeListOption, v string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		if err := clusterupgrade.InstallFixtures(ctx, t.L(), u.c, nodes, v); err != nil {
			t.Fatal(err)
		}
		binary := uploadVersion(ctx, t, u.c, nodes, v)
		startOpts := option.DefaultStartOpts()
		// NB: can't start sequentially since cluster already bootstrapped.
		startOpts.RoachprodOpts.Sequential = false
		if err := clusterupgrade.StartWithSettings(
			ctx, t.L(), u.c, nodes, startOpts, install.BinaryOption(binary),
		); err != nil {
			t.Fatal(err)
		}
	}
}

func uploadAndStart(nodes option.NodeListOption, v string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		binary := uploadVersion(ctx, t, u.c, nodes, v)
		startOpts := option.DefaultStartOpts()
		// NB: can't start sequentially since cluster already bootstrapped.
		startOpts.RoachprodOpts.Sequential = false
		if err := clusterupgrade.StartWithSettings(
			ctx, t.L(), u.c, nodes, startOpts, install.BinaryOption(binary),
		); err != nil {
			t.Fatal(err)
		}
	}
}

// binaryUpgradeStep rolling-restarts the given nodes into the new binary
// version. Note that this does *not* wait for the cluster version to upgrade.
// Use a waitForUpgradeStep() for that.
func binaryUpgradeStep(nodes option.NodeListOption, newVersion string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		if err := clusterupgrade.RestartNodesWithNewBinary(
			ctx, t, t.L(), u.c, nodes, option.DefaultStartOpts(), newVersion,
		); err != nil {
			t.Fatal(err)
		}
	}
}

func preventAutoUpgradeStep(node int) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, node)
		_, err := db.ExecContext(ctx, `SET CLUSTER SETTING cluster.preserve_downgrade_option = $1`, u.binaryVersion(ctx, t, node).String())
		if err != nil {
			t.Fatal(err)
		}
	}
}

func allowAutoUpgradeStep(node int) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, node)
		_, err := db.ExecContext(ctx, `RESET CLUSTER SETTING cluster.preserve_downgrade_option`)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// NB: this is intentionally kept separate from binaryUpgradeStep because we run
// feature tests between the steps, and we want to expose them (at least
// heuristically) to the real-world situation in which some nodes have already
// learned of a cluster version bump (from Gossip) where others haven't. This
// situation tends to exhibit unexpected behavior.
func waitForUpgradeStep(nodes option.NodeListOption) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		dbFunc := func(node int) *gosql.DB { return u.conn(ctx, t, node) }
		if err := clusterupgrade.WaitForClusterUpgrade(
			ctx, t.L(), nodes, dbFunc, clusterupgrade.DefaultUpgradeTimeout,
		); err != nil {
			t.Fatal(err)
		}
	}
}

// makeVersionFixtureAndFatal creates fixtures from which we can test
// mixed-version clusters (i.e. version X mixing with X-1). The fixtures date
// back all the way to v1.0; when development begins on version X, we make a
// fixture for version X-1 by running a starting the version X-2 cluster from
// the X-2 fixtures, upgrading it to version X-1, and copy the resulting store
// directories to the log directories (which are part of the artifacts). The
// test will then fail on purpose when it's done with instructions on where to
// move the files.
func makeVersionFixtureAndFatal(
	ctx context.Context, t test.Test, c cluster.Cluster, makeFixtureVersion string,
) {
	predecessorVersion, err := release.LatestPredecessor(version.MustParse(makeFixtureVersion))
	if err != nil {
		t.Fatal(err)
	}

	t.L().Printf("making fixture for %s (starting at %s)", makeFixtureVersion, predecessorVersion)
	fixtureVersion := makeFixtureVersion[1:] // drop the leading v

	newVersionUpgradeTest(c,
		// Start the cluster from a fixture. That fixture's cluster version may
		// be at the predecessor version (though in practice it's fully up to
		// date, if it was created via the checkpointer above), so add a
		// waitForUpgradeStep to make sure we're upgraded all the way before
		// moving on.
		//
		// See the comment on createCheckpoints for details on fixtures.
		uploadAndStartFromCheckpointFixture(c.All(), predecessorVersion),
		waitForUpgradeStep(c.All()),

		// NB: at this point, cluster and binary version equal predecessorVersion,
		// and auto-upgrades are on.

		binaryUpgradeStep(c.All(), fixtureVersion),
		waitForUpgradeStep(c.All()),

		func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
			// If we're taking checkpoints, momentarily stop the cluster (we
			// need to do that to get the checkpoints to reflect a
			// consistent cluster state). The binary at this point will be
			// the new one, but the cluster version was not explicitly
			// bumped, though auto-update may have taken place already.
			// For example, if newVersion is 2.1, the cluster version in
			// the store directories may be 2.0 on some stores and 2.1 on
			// the others (though if any are on 2.1, then that's what's
			// stored in system.settings).
			// This means that when we restart from that version, we're
			// going to want to use the binary mentioned in the checkpoint,
			// or at least one compatible with the *predecessor* of the
			// checkpoint version. For example, for checkpoint-2.1, the
			// cluster version might be 2.0, so we can only use the 2.0 or
			// 2.1 binary, but not the 19.1 binary (as 19.1 and 2.0 are not
			// compatible).
			name := clusterupgrade.CheckpointName(u.binaryVersion(ctx, t, 1).String())
			u.c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.All())

			binaryPath := clusterupgrade.BinaryPathForVersion(t, fixtureVersion)
			c.Run(ctx, c.All(), binaryPath, "debug", "pebble", "db", "checkpoint",
				"{store-dir}", "{store-dir}/"+name)
			// The `cluster-bootstrapped` marker can already be found within
			// store-dir, but the rocksdb checkpoint step above does not pick it
			// up as it isn't recognized by RocksDB. We copy the marker
			// manually, it's necessary for roachprod created clusters. See
			// #54761.
			c.Run(ctx, c.Node(1), "cp", "{store-dir}/cluster-bootstrapped", "{store-dir}/"+name)
			// Similar to the above - newer versions require the min version file to open a store.
			c.Run(ctx, c.All(), "cp", fmt.Sprintf("{store-dir}/%s", storage.MinVersionFilename), "{store-dir}/"+name)
			c.Run(ctx, c.All(), "tar", "-C", "{store-dir}/"+name, "-czf", "{log-dir}/"+name+".tgz", ".")
			t.Fatalf(`successfully created checkpoints; failing test on purpose.

Invoke the following to move the archives to the right place and commit the
result:

for i in 1 2 3 4; do
  mkdir -p pkg/cmd/roachtest/fixtures/${i} && \
  mv artifacts/generate-fixtures/run_1/logs/${i}.unredacted/checkpoint-*.tgz \
     pkg/cmd/roachtest/fixtures/${i}/
done
`)
		}).run(ctx, t)
}

// importTPCCStep runs a TPCC import import on the first crdbNode (monitoring them all for
// crashes during the import). If oldV is nil, this runs the import using the specified
// version (for example "19.2.1", as provided by LatestPredecessor()) using the location
// used by c.Stage(). An empty oldV uses the main cockroach binary.
func importTPCCStep(
	oldV string, headroomWarehouses int, crdbNodes option.NodeListOption,
) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		// We need to use the predecessor binary to load into the
		// predecessor cluster to avoid random breakage. For example, you
		// can't use 21.1 to import into 20.2 due to some flag changes.
		//
		// TODO(tbg): also import a large dataset (for example 2TB bank)
		// that will provide cold data that may need to be migrated.
		var cmd string
		if oldV == "" {
			cmd = tpccImportCmd(headroomWarehouses)
		} else {
			cmd = tpccImportCmdWithCockroachBinary(filepath.Join("v"+oldV, "cockroach"), headroomWarehouses, "--checks=false")
		}
		// Use a monitor so that we fail cleanly if the cluster crashes
		// during import.
		m := u.c.NewMonitor(ctx, crdbNodes)
		m.Go(func(ctx context.Context) error {
			return u.c.RunE(ctx, u.c.Node(crdbNodes[0]), cmd)
		})
		m.Wait()
	}
}

func importLargeBankStep(oldV string, rows int, crdbNodes option.NodeListOption) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		// Use the predecessor binary to load into the predecessor
		// cluster to avoid random breakage due to flag changes, etc.
		binary := "./cockroach"
		if oldV != "" {
			binary = filepath.Join("v"+oldV, "cockroach")
		}

		// Use a monitor so that we fail cleanly if the cluster crashes
		// during import.
		m := u.c.NewMonitor(ctx, crdbNodes)
		m.Go(func(ctx context.Context) error {
			return u.c.RunE(ctx, u.c.Node(crdbNodes[0]), binary, "workload", "fixtures", "import", "bank",
				"--payload-bytes=10240", "--rows="+fmt.Sprint(rows), "--seed=4", "--db=bigbank")
		})
		m.Wait()
	}
}

func sleepStep(d time.Duration) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		time.Sleep(d)
	}
}
