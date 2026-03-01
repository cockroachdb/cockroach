// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"embed"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	e2e_tests "github.com/cockroachdb/cockroach/pkg/ui/workspaces/e2e-tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const cypressFilePath = "/tmp/dbconsole-cypress"
const testArtifactPath = "/tmp/dbconsole-cypress/artifacts"

var seedQueries = []string{
	`CREATE USER IF NOT EXISTS cypress PASSWORD 'tests'`,
	`GRANT admin TO cypress`,
}

// dbConsoleCypressTest provides functionality for installing dependencies and
// running db-console cypress tests directly on the workload node. It provides
// functions for installing Node.js and Cypress dependencies, seeding a test
// cluster with data, and running cypress tests against every node in the
// provided test cluster.
type dbConsoleCypressTest struct {
	t test.Test
	// testCluster is the roachtest cluster that the configured cypress tests
	// will run against. The test cluster is responsible for running these tests.
	testCluster cluster.Cluster
	// cypressFiles contains all the files that are necessary to run db-console's
	// cypress tests. These files contain both the files necessary to configure
	// cypress and the tests that will be run by cypress.
	cypressFiles []embed.FS
	// cypressWorkingDir contains the location that cypressFiles will be written
	// to on the workloadNode.
	cypressWorkingDir string
	// artifactPath contains the location that test artifacts, including cypress
	// screenshots upon failures, will be written to.
	artifactPath string
	// spec is the specified tests to run via cypress. If no value is set, all
	// tests are run.
	spec string
	// seedQueries contains all the queries to seed the cluster with before the
	// tests run.
	seedQueries []string
}

func newDbConsoleCypressTest(
	t test.Test, c cluster.Cluster, spec string, seedQueries []string,
) dbConsoleCypressTest {
	return dbConsoleCypressTest{
		t:                 t,
		testCluster:       c,
		cypressFiles:      e2e_tests.CypressEmbeds,
		artifactPath:      testArtifactPath,
		cypressWorkingDir: cypressFilePath,
		spec:              spec,
		seedQueries:       seedQueries,
	}
}

// SetupTest installs Cypress dependencies and seeds the cluster with the data
// necessary for the tests to succeed.
func (d *dbConsoleCypressTest) SetupTest(ctx context.Context, conn *gosql.DB) {
	d.installDeps(ctx)
	d.seedCluster(ctx, conn)
}

// RunTest runs the cypress tests against the provided targetNode's db-console.
// Test failures will produce artifacts in the roachtest's artifacts directory
// to help with failure investigations.
func (d *dbConsoleCypressTest) RunTest(ctx context.Context, targetNode int, l *logger.Logger) {
	var specStr string
	if d.spec != "" {
		specStr = fmt.Sprintf(`--spec "%s"`, d.spec)
	}
	rtCluster := d.testCluster
	workloadNode := rtCluster.WorkloadNode()
	adminUIAddrs, err := rtCluster.ExternalAdminUIAddr(ctx, d.t.L(), rtCluster.Node(targetNode))
	require.NoError(d.t, err)
	url := fmt.Sprintf("https://%s", adminUIAddrs[0])
	require.NoError(d.t, rtCluster.RunE(ctx, option.WithNodes(workloadNode), "mkdir", "-p", d.artifactPath))
	cypressRun := fmt.Sprintf(
		`cd %s && NO_COLOR=1 npx cypress run --e2e --config baseUrl=%s,screenshotsFolder=%s,videosFolder=%s %s`,
		d.cypressWorkingDir, url, d.artifactPath, d.artifactPath, specStr)
	// If the cypress run fails, get the test failure artifacts and write them
	// to roachtest's artifact directory.
	if err = rtCluster.RunE(ctx, option.WithNodes(workloadNode), cypressRun); !assert.NoError(d.t, err) {
		testArtifactsDir := d.t.ArtifactsDir()
		if mkDirErr := os.MkdirAll(testArtifactsDir, 0777); mkDirErr != nil {
			d.t.Fatal(mkDirErr)
		}
		require.NoError(d.t, rtCluster.Get(context.Background(), d.t.L(), d.artifactPath, testArtifactsDir, workloadNode))
		d.t.Fatal(err)
	}
}

// seedCluster seeds the cluster with dbConsoleCypressTest.seedQueries. This
// will set up the test cluster with all the data necessary to successfully
// run db-console cypress tests.
func (d *dbConsoleCypressTest) seedCluster(ctx context.Context, db *gosql.DB) {
	for _, cmd := range seedQueries {
		if _, err := db.ExecContext(ctx, cmd); err != nil {
			d.t.Fatal(err)
		}
	}
}

// installDeps installs all dependencies needed to run Cypress tests directly
// on the workload node: Node.js 22, pnpm, Cypress system libraries, and
// project npm dependencies.
func (d *dbConsoleCypressTest) installDeps(ctx context.Context) {
	workloadNode := d.testCluster.WorkloadNode()
	rtCluster := d.testCluster
	t := d.t
	require.NoError(t, rtCluster.RunE(ctx, option.WithNodes(workloadNode), "mkdir", "-p", d.cypressWorkingDir))

	d.writeCypressFilesToWorkloadNode(ctx)

	t.Status("installing node 22")
	require.NoError(t, installNode22(ctx, t, rtCluster, workloadNode, nodeOpts{withPnpm: true}))

	t.Status("installing cypress system dependencies")
	require.NoError(t, rtCluster.RunE(ctx, option.WithNodes(workloadNode),
		`sudo apt-get update && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y `+
			`libgtk2.0-0 libgtk-3-0 libgbm-dev libnotify-dev libnss3 libxss1 libasound2 libxtst6 xauth xvfb`))

	t.Status("installing npm dependencies")
	testutils.SucceedsSoon(t, func() error {
		return rtCluster.RunE(ctx, option.WithNodes(workloadNode),
			fmt.Sprintf("cd %s && pnpm install", d.cypressWorkingDir))
	})
}

// writeCypressFilesToWorkloadNode writes the embedded
// dbConsoleCypressTest.cypressFiles to the cluster's workloadNode.
func (d *dbConsoleCypressTest) writeCypressFilesToWorkloadNode(ctx context.Context) {
	joinPath := d.cypressWorkingDir
	workloadNode := d.testCluster.WorkloadNode()
	rtCluster := d.testCluster
	for _, embedFs := range d.cypressFiles {
		require.NoError(d.t, fs.WalkDir(embedFs, ".", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			relPath, err := filepath.Rel(".", path)
			if err != nil {
				return err
			}

			join := filepath.Join(joinPath, relPath)
			if d.IsDir() {
				return rtCluster.RunE(ctx, option.WithNodes(workloadNode), "mkdir", "-p", join)
			}

			data, err := embedFs.ReadFile(path)
			if err != nil {
				return err
			}
			return rtCluster.PutString(ctx, string(data), join, os.ModePerm, workloadNode)
		}))
	}
}

func registerDbConsoleCypress(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "db-console/mixed-version-cypress",
		Owner:   registry.OwnerObservability,
		Cluster: r.MakeClusterSpec(5, spec.WorkloadNode()),
		// Disabled on IBM because s390x is only built on master and mixed-version
		// is impossible to test as of 05/2025.
		CompatibleClouds: registry.AllClouds.NoIBM(),
		Suites:           registry.Suites(registry.MixedVersion, registry.Nightly),
		Monitor:          true,
		Randomized:       false,
		Run:              runDbConsoleCypressMixedVersions,
		Timeout:          2 * time.Hour,
	})
	r.Add(registry.TestSpec{
		Name:    "db-console/cypress",
		Owner:   registry.OwnerObservability,
		Cluster: r.MakeClusterSpec(4, spec.WorkloadNode()),
		// Disabled on IBM because of some nodejs dependencies that are not
		// available on s390x.
		CompatibleClouds: registry.AllClouds.NoIBM(),
		Suites:           registry.Suites(registry.Nightly),
		Randomized:       false,
		Run:              runDbConsoleCypress,
		Timeout:          1 * time.Hour,
	})
	r.Add(registry.TestSpec{
		Name:    "db-console/cypress-pages",
		Owner:   registry.OwnerObservability,
		Cluster: r.MakeClusterSpec(4, spec.WorkloadNode()),
		// Disabled on IBM because of some nodejs dependencies that are not
		// available on s390x.
		CompatibleClouds: registry.AllClouds.NoIBM(),
		Suites:           registry.Suites(registry.Nightly),
		Randomized:       false,
		Run:              runDbConsoleCypressPages,
		Timeout:          30 * time.Minute,
	})
}

// runDbConsoleCypress runs cypress health-check tests against the db-console
// for each node in the cluster.
func runDbConsoleCypress(ctx context.Context, t test.Test, c cluster.Cluster) {
	if c.IsLocal() {
		t.Fatal("cannot be run in local mode")
	}

	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.CRDBNodes())

	cypressTest := newDbConsoleCypressTest(t, c, "cypress/e2e/health-check/*.ts", seedQueries)
	db, err := c.ConnE(ctx, t.L(), cypressTest.testCluster.CRDBNodes()[0])
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cypressTest.SetupTest(ctx, db)
	for _, targetNode := range c.CRDBNodes() {
		cypressTest.RunTest(ctx, targetNode, t.L())
	}
}

// runDbConsoleCypressPages runs cypress page tests against the db-console
// for each node in the cluster without requiring authentication.
func runDbConsoleCypressPages(ctx context.Context, t test.Test, c cluster.Cluster) {
	if c.IsLocal() {
		t.Fatal("cannot be run in local mode")
	}

	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.CRDBNodes())

	cypressTest := newDbConsoleCypressTest(t, c, "cypress/e2e/pages/*.ts", seedQueries)
	db, err := c.ConnE(ctx, t.L(), cypressTest.testCluster.CRDBNodes()[0])
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.ExecContext(ctx, `SET CLUSTER SETTING sql.stats.flush.interval = '10s';`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx, `CREATE TABLE test_table(id integer PRIMARY KEY, t TEXT);`)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, &gosql.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	query := `INSERT INTO test_table(id, t) SELECT i, sha512(random()::text) FROM ` +
		`generate_series(0, 1000) AS t(i);`
	_, err = tx.ExecContext(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	cypressTest.SetupTest(ctx, db)
	for _, targetNode := range c.CRDBNodes() {
		cypressTest.RunTest(ctx, targetNode, t.L())
	}
}

// runDbConsoleCypressMixedVersions runs cypress health-check test against the
// db-console for each node in the mixed version cluster.
func runDbConsoleCypressMixedVersions(ctx context.Context, t test.Test, c cluster.Cluster) {
	if c.IsLocal() {
		t.Fatal("cannot be run in local mode")
	}
	mvt := mixedversion.NewTest(ctx, t, t.L(), c, c.CRDBNodes())
	cypressTest := newDbConsoleCypressTest(t, c, "cypress/e2e/health-check/*.ts", seedQueries)
	init := func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		_, db := h.RandomDB(r)
		cypressTest.SetupTest(ctx, db)
		return nil
	}
	inMixedVersion := func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		for _, targetNode := range c.CRDBNodes() {
			cypressTest.RunTest(ctx, targetNode, l)
		}
		return nil
	}

	mvt.OnStartup("Setup Cypress tests", init)
	mvt.InMixedVersion("Run cypress tests", inMixedVersion)
	mvt.Run()
}
