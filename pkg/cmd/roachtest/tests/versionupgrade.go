// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/release"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
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
	testCtx := ctx
	opts := []mixedversion.CustomOption{
		mixedversion.AlwaysUseFixtures,
		mixedversion.AlwaysUseLatestPredecessors,
	}
	if c.IsLocal() {
		localTimeout := 30 * time.Minute
		var cancel context.CancelFunc
		testCtx, cancel = context.WithTimeout(ctx, localTimeout)
		defer cancel()
		opts = append(
			opts,
			mixedversion.NumUpgrades(1),
		)
	}

	mvt := mixedversion.NewTest(testCtx, t, t.L(), c, c.All(), opts...)

	mvt.InMixedVersion(
		"maybe run backup",
		func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
			// Separate process deployments do not have node local storage.
			if h.DeploymentMode() != mixedversion.SeparateProcessDeployment {
				// Verify that backups can be created in various configurations. This is
				// important to test because changes in system tables might cause backups to
				// fail in mixed-version clusters.
				dest := fmt.Sprintf("nodelocal://1/%d", timeutil.Now().UnixNano())
				return h.Exec(rng, `BACKUP INTO $1`, dest)
			} else {
				// Skip the backup step in separate-process deployments, since nodelocal
				// is not supported in pods.
				return nil
			}
		})
	mvt.InMixedVersion(
		"test features",
		func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
			for _, featureTest := range versionUpgradeTestFeatures {
				l.Printf("running feature test %q", featureTest.name)
				// These features rely on the fixtures used in this test,
				// which write data on the system interface.
				if err := h.System.Exec(rng, featureTest.statement); err != nil {
					l.Printf("%q: ERROR (%s)", featureTest.name, err)
					return err
				}
				l.Printf("%q: OK", featureTest.name)
			}

			return nil
		},
	)

	mvt.Run()
}

// uploadCockroach is a thin wrapper around
// `clusterupgrade.UploadCockroach` that calls t.Fatal if that call
// returns an error.
func uploadCockroach(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	nodes option.NodeListOption,
	newVersion *clusterupgrade.Version,
) string {
	path, err := clusterupgrade.UploadCockroach(ctx, t, t.L(), c, nodes, newVersion)
	if err != nil {
		t.Fatal(err)
	}

	return path
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
	// Manage connections to nodes and make sure to close any open
	// connections at the end of the test.
	conns := make(map[int]*gosql.DB)
	dbFunc := func(node int) *gosql.DB {
		if _, ok := conns[node]; !ok {
			conns[node] = c.Conn(ctx, t.L(), node)
		}

		return conns[node]
	}

	defer func() {
		for _, db := range conns {
			db.Close()
		}
	}()

	predecessorVersionStr, err := release.LatestPredecessor(version.MustParse(makeFixtureVersion))
	if err != nil {
		t.Fatal(err)
	}
	predecessorVersion := clusterupgrade.MustParseVersion(predecessorVersionStr)

	t.L().Printf("making fixture for %s (starting at %s)", makeFixtureVersion, predecessorVersion)
	fixtureVersion := clusterupgrade.MustParseVersion(makeFixtureVersion)

	t.L().Printf("installing fixtures")
	if err := clusterupgrade.InstallFixtures(ctx, t.L(), c, c.All(), predecessorVersion); err != nil {
		t.Fatalf("installing fixtures: %v", err)
	}

	t.L().Printf("uploading cockroach version %s", predecessorVersion)
	binary, err := clusterupgrade.UploadCockroach(ctx, t, t.L(), c, c.All(), predecessorVersion)
	if err != nil {
		t.Fatalf("uploading cockroach: %v", err)
	}

	t.L().Printf("starting cockroach process")
	if err := clusterupgrade.StartWithSettings(
		ctx, t.L(), c, c.All(), option.DefaultStartOpts(), install.BinaryOption(binary),
	); err != nil {
		t.Fatalf("starting cockroach: %v", err)
	}

	t.L().Printf("waiting for stable cluster version")
	if err := clusterupgrade.WaitForClusterUpgrade(
		ctx, t.L(), c.All(), dbFunc, clusterupgrade.DefaultUpgradeTimeout,
	); err != nil {
		t.Fatalf("waiting for cluster to reach version %s: %v", predecessorVersion, err)
	}

	t.L().Printf("restarting cluster to version %s", fixtureVersion)
	if err := clusterupgrade.RestartNodesWithNewBinary(
		ctx, t, t.L(), c, c.All(), option.NewStartOpts(option.NoBackupSchedule), fixtureVersion,
	); err != nil {
		t.Fatalf("restarting cluster to binary version %s: %v", fixtureVersion, err)
	}

	t.L().Printf("waiting for upgrade to %s to finalize", fixtureVersion)
	if err := clusterupgrade.WaitForClusterUpgrade(
		ctx, t.L(), c.All(), dbFunc, clusterupgrade.DefaultUpgradeTimeout,
	); err != nil {
		t.Fatalf("waiting for upgrade to %s to finalize: %v", fixtureVersion, err)
	}

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
	binaryVersion, err := clusterupgrade.BinaryVersion(ctx, dbFunc(1))
	if err != nil {
		t.Fatalf("fetching binary version on n1: %v", err)
	}

	name := clusterupgrade.CheckpointName(binaryVersion.String())
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.All())

	binaryPath := clusterupgrade.CockroachPathForVersion(t, fixtureVersion)
	c.Run(ctx, option.WithNodes(c.All()), binaryPath, "debug", "pebble", "db", "checkpoint",
		"{store-dir}", "{store-dir}/"+name)
	// The `cluster-bootstrapped` marker can already be found within
	// store-dir, but the rocksdb checkpoint step above does not pick it
	// up as it isn't recognized by RocksDB. We copy the marker
	// manually, it's necessary for roachprod created clusters. See
	// #54761.
	c.Run(ctx, option.WithNodes(c.Node(1)), "cp", "{store-dir}/cluster-bootstrapped", "{store-dir}/"+name)
	// Similar to the above - newer versions require the min version file to open a store.
	c.Run(ctx, option.WithNodes(c.All()), "cp", fmt.Sprintf("{store-dir}/%s", storage.MinVersionFilename), "{store-dir}/"+name)
	c.Run(ctx, option.WithNodes(c.All()), "tar", "-C", "{store-dir}/"+name, "-czf", "{log-dir}/"+name+".tgz", ".")
	t.Fatalf(`successfully created checkpoints; failing test on purpose.

Invoke the following to move the archives to the right place and commit the
result:

for i in 1 2 3 4; do
  mkdir -p pkg/cmd/roachtest/fixtures/${i} && \
  mv artifacts/generate-fixtures/run_1/logs/${i}.unredacted/checkpoint-*.tgz \
     pkg/cmd/roachtest/fixtures/${i}/
done
`)
}

// This is a regression test for a race detailed in
// https://github.com/cockroachdb/cockroach/issues/138342, where it became
// possible for an HTTP request to cause a fatal error if the sql server
// did not initialize the cluster version in time.
func registerHTTPRestart(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "http-register-routes/mixed-version",
		Owner:            registry.OwnerObservability,
		Cluster:          r.MakeClusterSpec(4),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.MixedVersion, registry.Nightly),
		Randomized:       true,
		Run:              runHTTPRestart,
		Timeout:          1 * time.Hour,
	})
}

func runHTTPRestart(ctx context.Context, t test.Test, c cluster.Cluster) {
	mvt := mixedversion.NewTest(ctx, t, t.L(), c,
		c.CRDBNodes(),
		mixedversion.AlwaysUseLatestPredecessors,
	)

	// Any http request requiring auth will do.
	httpReq := tspb.TimeSeriesQueryRequest{
		StartNanos: timeutil.Now().UnixNano() - 10*time.Second.Nanoseconds(),
		EndNanos:   timeutil.Now().UnixNano(),
		// Ask for 10s intervals.
		SampleNanos: (10 * time.Second).Nanoseconds(),
		Queries: []tspb.Query{{
			Name:             "cr.node.sql.service.latency-p90",
			SourceAggregator: tspb.TimeSeriesQueryAggregator_MAX.Enum(),
		}},
	}

	httpCall := func(ctx context.Context, node int, l *logger.Logger, useSystemTenant bool) {
		logEvery := roachtestutil.Every(1 * time.Second)
		var clientOpts []func(opts *roachtestutil.RoachtestHTTPOptions)
		var urlOpts []option.OptionFunc
		if useSystemTenant {
			clientOpts = append(clientOpts, roachtestutil.VirtualCluster(install.SystemInterfaceName))
			urlOpts = append(urlOpts, option.VirtualClusterName(install.SystemInterfaceName))
		}
		client := roachtestutil.DefaultHTTPClient(c, l, clientOpts...)
		adminUrls, err := c.ExternalAdminUIAddr(ctx, l, c.Node(node), urlOpts...)
		if err != nil {
			t.Fatal(err)
		}
		url := "https://" + adminUrls[0] + "/ts/query"
		l.Printf("Sending requests to %s", url)

		var response tspb.TimeSeriesQueryResponse
		// Eventually we should see a successful request.
		reqSuccess := false
		for {
			select {
			case <-ctx.Done():
				if !reqSuccess {
					t.Fatalf("n%d: No successful http requests made.", node)
				}
				return
			default:
			}
			if err := client.PostProtobuf(ctx, url, &httpReq, &response); err != nil {
				if logEvery.ShouldLog() {
					l.Printf("n%d: Error posting protobuf: %s", node, err)
				}
				continue
			}
			reqSuccess = true
		}
	}

	for _, n := range c.CRDBNodes() {
		mvt.BackgroundFunc("HTTP requests to system tenant", func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
			httpCall(ctx, n, l, true /* useSystemTenant */)
			return nil
		})
		mvt.BackgroundFunc("HTTP requests to secondary tenant", func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
			if h.DeploymentMode() == mixedversion.SystemOnlyDeployment {
				return nil
			}
			httpCall(ctx, n, l, false /* useSystemTenant */)
			return nil
		})
	}
	mvt.Run()
}
