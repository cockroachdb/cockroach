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
	"path"
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
	e2e_tests "github.com/cockroachdb/cockroach/pkg/ui/workspaces/e2e-tests"
	"github.com/stretchr/testify/require"
)

//go:embed db-console/Dockerfile
var dockerFile string

const cypressFilePath = "/tmp/dbconsole-cypress"
const testArtifactPath = "/tmp/dbconsole-cypress/artifacts"
const imageName = "cypress-roach-test"

var seedQueries = []string{
	`CREATE USER IF NOT EXISTS cypress PASSWORD 'tests'`,
	`GRANT admin TO cypress`,
}

type dbConsoleCypressTest struct {
	t test.Test
	c cluster.Cluster
	// cypressFiles contains all the files that will be written to the cluster's workloadNode that are necessary to
	// run db-console's cypress tests. These files contain both the files necessary to configure cypress and the
	// tests that will be run by cypress
	cypressFiles []embed.FS
	// cypressPath contains the location that cypressFiles will be written to on the workloadNode
	cypressPath string
	// imageName is the name that will be given to the docker image built and run as part of these tests
	imageName string
	// dockerFile is the Dockerfile contents that will be written to the cluster's workloadNode to be used for docker
	// build and docker run
	dockerFile string
	// artifactPath contains the location that test artifacts, including cypress screenshots upon failures, will be
	// written to
	artifactPath string
	// spec is the specified tests to run via cypress. If no value is set, all tests are run
	spec string
	// seedQueries contains all the queries to seed the cluster with before the tests runs
	seedQueries []string
}

func newDbConsoleCypressTest(t test.Test, c cluster.Cluster, spec string) dbConsoleCypressTest {
	return dbConsoleCypressTest{
		t:            t,
		c:            c,
		cypressFiles: e2e_tests.CypressEmbeds,
		imageName:    imageName,
		dockerFile:   dockerFile,
		artifactPath: testArtifactPath,
		cypressPath:  cypressFilePath,
		spec:         spec,
		seedQueries:  seedQueries,
	}
}

func registerDbConsole(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "db-console/mixed-version-cypress",
		Owner:            registry.OwnerObservability,
		Cluster:          r.MakeClusterSpec(5, spec.WorkloadNode()),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
		Randomized:       false,
		Run:              runDbConsoleCypressMixedVersions,
		Timeout:          1 * time.Hour,
	})
	r.Add(registry.TestSpec{
		Name:             "db-console/cypress",
		Owner:            registry.OwnerObservability,
		Cluster:          r.MakeClusterSpec(4, spec.WorkloadNode()),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
		Randomized:       false,
		Run:              runDbConsoleCypress,
		Timeout:          1 * time.Hour,
	})
}

// runDbConsoleCypress runs cypress health-check tests against the db-console for each node in the cluster
func runDbConsoleCypress(ctx context.Context, t test.Test, c cluster.Cluster) {
	if c.IsLocal() {
		t.Fatal("cannot be run in local mode")
	}

	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())

	cypressTest := newDbConsoleCypressTest(t, c, "cypress/e2e/health-check/*.ts")
	db, err := c.ConnE(ctx, t.L(), cypressTest.c.WorkloadNode()[0])
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cypressTest.buildDockerImage(ctx)
	cypressTest.seedCluster(ctx, db)
	for _, targetNode := range c.CRDBNodes() {
		cypressTest.runTests(ctx, targetNode)
	}
}

// runDbConsoleCypressMixedVersions runs cypress health-check test against the db-console for each node in the
// mixed version cluster.
func runDbConsoleCypressMixedVersions(ctx context.Context, t test.Test, c cluster.Cluster) {
	if c.IsLocal() {
		t.Fatal("cannot be run in local mode")
	}
	mvt := mixedversion.NewTest(ctx, t, t.L(), c, c.CRDBNodes())
	cypressTest := newDbConsoleCypressTest(t, c, "cypress/e2e/health-check/*.ts")
	init := func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		cypressTest.buildDockerImage(ctx)
		_, db := h.RandomDB(r)
		cypressTest.seedCluster(ctx, db)
		return nil
	}
	inMixedVersion := func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		for _, targetNode := range c.CRDBNodes() {
			cypressTest.runTests(ctx, targetNode)
		}
		return nil
	}

	mvt.OnStartup("Setup Cypress tests", init)
	mvt.InMixedVersion("Run cypress tests", inMixedVersion)
	mvt.Run()
}

// seedCluster seeds the cluster with dbConsoleCypressTest.seedQueries. This should be invoked before runTests
// is called.
func (d *dbConsoleCypressTest) seedCluster(ctx context.Context, db *gosql.DB) {
	for _, cmd := range seedQueries {
		if _, err := db.ExecContext(ctx, cmd); err != nil {
			d.t.Fatal(err)
		}
	}
}

// buildDockerImage builds the Docker image which will run db-console cypress tests
func (d *dbConsoleCypressTest) buildDockerImage(ctx context.Context) {
	workloadNode := d.c.WorkloadNode()
	rtCluster := d.c
	t := d.t
	require.NoError(t, rtCluster.RunE(ctx, option.WithNodes(workloadNode), "mkdir", "-p", d.cypressPath))

	// create docker file
	require.NoError(t, rtCluster.PutString(ctx, dockerFile, path.Join(d.cypressPath, "Dockerfile"), os.ModePerm, workloadNode))

	// write cypress files to the node
	d.writeToNode(ctx)
	// Install Docker
	t.Status("installing docker")
	require.NoError(t, rtCluster.Install(ctx, t.L(), workloadNode, "docker"), "failed to install docker")
	// Build docker image
	require.NoError(t, rtCluster.RunE(ctx, option.WithNodes(workloadNode),
		fmt.Sprintf("docker build -t %s %s", d.imageName, d.cypressPath)))
}

// runTests runs the cypress tests against the provided targetNode's db-console.
func (d *dbConsoleCypressTest) runTests(ctx context.Context, targetNode int) {
	var specStr string
	if d.spec != "" {
		specStr = fmt.Sprintf(`--spec "%s"`, d.spec)
	}
	rtCluster := d.c
	workloadNode := rtCluster.WorkloadNode()
	adminUIAddrs, err := rtCluster.ExternalAdminUIAddr(ctx, d.t.L(), rtCluster.Node(targetNode))
	require.NoError(d.t, err)
	url := fmt.Sprintf("https://%s", adminUIAddrs[0])
	dockerRun := fmt.Sprintf(
		`docker run -v %s:/e2e/artifacts %s --config baseUrl=%s,screenshotsFolder=/e2e/artifacts,videosFolder=/e2e/artifacts %s`,
		d.artifactPath, d.imageName, url, specStr)
	if err = rtCluster.RunE(ctx, option.WithNodes(workloadNode), dockerRun); err != nil {
		testArtifactsDir := d.t.ArtifactsDir()
		if mkDirErr := os.MkdirAll(testArtifactsDir, 0777); mkDirErr != nil {
			d.t.Fatal(mkDirErr)
		}
		require.NoError(d.t, rtCluster.RunE(ctx, option.WithNodes(workloadNode), "mkdir", "-p", d.artifactPath))
		require.NoError(d.t, rtCluster.Get(ctx, d.t.L(), d.artifactPath, testArtifactsDir, workloadNode))
		require.NoError(d.t, err)
	}
}

// writeToNode writes the dbConsoleCypressTest.cypressFiles to the cluster's workloadNode
func (d *dbConsoleCypressTest) writeToNode(ctx context.Context) {
	joinPath := d.cypressPath
	node := d.c.WorkloadNode()
	rtCluster := d.c
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
				return rtCluster.RunE(ctx, option.WithNodes(node), "mkdir", "-p", join)
			}

			data, err := embedFs.ReadFile(path)
			if err != nil {
				return err
			}
			return rtCluster.PutString(ctx, string(data), join, os.ModePerm, node)
		}))
	}
}
