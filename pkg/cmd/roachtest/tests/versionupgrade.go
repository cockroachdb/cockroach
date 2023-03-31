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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/stretchr/testify/require"
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
	if c.IsLocal() && runtime.GOARCH == "arm64" {
		t.Skip("Skip under ARM64. See https://github.com/cockroachdb/cockroach/issues/89268")
	}
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.All())
	mvt := mixedversion.NewTest(ctx, t, t.L(), c, c.All())
	mvt.OnStartup("setup schema changer workload", func(ctx context.Context, l *logger.Logger, r *rand.Rand, helper *mixedversion.Helper) error {
		// Execute the workload init.
		return c.RunE(ctx, c.All(), "./workload init schemachange")
	})
	mvt.InMixedVersion("run backup", func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
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
			if c.IsLocal() {
				l.Printf("skipping step in bors builds while failures are handled -- #99115")
				return nil
			}
			l.Printf("running schema workload step")
			runCmd := roachtestutil.NewCommand("./workload run schemachange").Flag("verbose", 1).Flag("max-ops", 10).Flag("concurrency", 2).Arg("{pgurl:1-%d}", len(c.All()))
			randomNode := h.RandomNode(rng, c.All())
			return c.RunE(ctx, option.NodeListOption{randomNode}, runCmd.String())
		},
	)
	mvt.AfterUpgradeFinalized(
		"check if GC TTL is pinned",
		func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
			// TODO(irfansharif): This can be removed when the predecessor version
			// in this test is v23.1, where the default is 4h. This test was only to
			// make sure that existing clusters that upgrade to 23.1 retained their
			// existing GC TTL.
			l.Printf("checking if GC TTL is pinned to 24h")
			var ttlSeconds int
			query := `
	SELECT
		(crdb_internal.pb_to_json('cockroach.config.zonepb.ZoneConfig', raw_config_protobuf)->'gc'->'ttlSeconds')::INT
	FROM crdb_internal.zones
	WHERE target = 'RANGE default'
	LIMIT 1
`
			if err := h.QueryRow(rng, query).Scan(&ttlSeconds); err != nil {
				return fmt.Errorf("error querying GC TTL: %w", err)
			}
			expectedTTL := 24 * 60 * 60 // NB: 24h is what's used in the fixture
			if ttlSeconds != expectedTTL {
				return fmt.Errorf("unexpected GC TTL: actual (%d) != expected (%d)", ttlSeconds, expectedTTL)
			}
			return nil
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
		clusterupgrade.StartWithBinary(ctx, t.L(), u.c, nodes, binary, startOpts)
	}
}

func uploadAndStart(nodes option.NodeListOption, v string) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		binary := uploadVersion(ctx, t, u.c, nodes, v)
		startOpts := option.DefaultStartOpts()
		// NB: can't start sequentially since cluster already bootstrapped.
		startOpts.RoachprodOpts.Sequential = false
		clusterupgrade.StartWithBinary(ctx, t.L(), u.c, nodes, binary, startOpts)
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
		if err := clusterupgrade.WaitForClusterUpgrade(ctx, t.L(), nodes, dbFunc); err != nil {
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
	var useLocalBinary bool
	if makeFixtureVersion == "" {
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(1))
		require.NoError(t, c.Conn(ctx, t.L(), 1).QueryRowContext(
			ctx,
			`select regexp_extract(value, '^v([0-9]+\.[0-9]+\.[0-9]+)') from crdb_internal.node_build_info where field = 'Version';`,
		).Scan(&makeFixtureVersion))
		c.Wipe(ctx, c.Node(1))
		useLocalBinary = true
	}

	predecessorVersion, err := version.PredecessorVersion(*version.MustParse("v" + makeFixtureVersion))
	if err != nil {
		t.Fatal(err)
	}

	t.L().Printf("making fixture for %s (starting at %s)", makeFixtureVersion, predecessorVersion)

	if useLocalBinary {
		// Make steps below use the main cockroach binary (in particular, don't try
		// to download the released version for makeFixtureVersion which may not yet
		// exist)
		makeFixtureVersion = ""
	}

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

		binaryUpgradeStep(c.All(), makeFixtureVersion),
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

			binaryPath := clusterupgrade.BinaryPathFromVersion(makeFixtureVersion)
			c.Run(ctx, c.All(), binaryPath, "debug", "pebble", "db", "checkpoint",
				"{store-dir}", "{store-dir}/"+name)
			// The `cluster-bootstrapped` marker can already be found within
			// store-dir, but the rocksdb checkpoint step above does not pick it
			// up as it isn't recognized by RocksDB. We copy the marker
			// manually, it's necessary for roachprod created clusters. See
			// #54761.
			c.Run(ctx, c.Node(1), "cp", "{store-dir}/cluster-bootstrapped", "{store-dir}/"+name)
			// Similar to the above - newer versions require the min version file to open a store.
			c.Run(ctx, c.Node(1), "cp", fmt.Sprintf("{store-dir}/%s", storage.MinVersionFilename), "{store-dir}/"+name)
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
// version (for example "19.2.1", as provided by PredecessorVersion()) using the location
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
