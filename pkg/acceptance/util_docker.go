// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package acceptance

import (
	"context"
	"crypto/rand"
	"fmt"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/containerd/containerd/platforms"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
)

func defaultContainerConfig() container.Config {
	return container.Config{
		Image: acceptanceImage,
		Env: []string{
			fmt.Sprintf("PGUSER=%s", username.RootUser),
			fmt.Sprintf("PGPORT=%s", base.DefaultPort),
			"PGSSLCERT=/certs/client.root.crt",
			"PGSSLKEY=/certs/client.root.key",
		},
		Entrypoint: []string{"autouseradd", "-u", "roach", "-C", "/home/roach", "--"},
	}
}

// testDockerFail ensures the specified docker cmd fails.
func testDockerFail(ctx context.Context, t *testing.T, name string, cmd []string) {
	containerConfig := defaultContainerConfig()
	containerConfig.Cmd = cmd
	if err := testDockerSingleNode(ctx, t, name, containerConfig); err == nil {
		t.Error("expected failure")
	}
}

// testDockerSuccess ensures the specified docker cmd succeeds.
func testDockerSuccess(ctx context.Context, t *testing.T, name string, cmd []string) {
	containerConfig := defaultContainerConfig()
	containerConfig.Cmd = cmd
	if err := testDockerSingleNode(ctx, t, name, containerConfig); err != nil {
		t.Error(err)
	}
}

const (
	// Iterating against a locally built version of the docker image can be done
	// by changing acceptanceImage to the hash of the container.
	acceptanceImage = "us-east1-docker.pkg.dev/crl-ci-images/cockroach/acceptance:20221005-223354"
)

func testDocker(
	ctx context.Context, t *testing.T, num int, name string, containerConfig container.Config,
) error {
	maybeSkipTest(t)
	var err error
	RunDocker(t, func(t *testing.T) {
		var pwd string
		pwd, err = os.Getwd()
		if err != nil {
			return
		}
		testdataDir := filepath.Join(pwd, "testdata")
		if bazel.BuiltWithBazel() {
			testdataDir, err = os.MkdirTemp("", "")
			if err != nil {
				t.Fatal(err)
			}
			// Copy runfiles symlink content to a temporary directory to avoid broken symlinks in docker.
			err = copyRunfiles("testdata", testdataDir)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = os.RemoveAll(testdataDir)
			}()
		}
		hostConfig := container.HostConfig{
			NetworkMode: "host",
			Binds:       []string{testdataDir + ":/mnt/data"},
		}
		if bazel.BuiltWithBazel() {
			interactivetestsDir, err := os.MkdirTemp("", "")
			if err != nil {
				t.Fatal(err)
			}
			// Copy runfiles symlink content to a temporary directory to avoid broken symlinks in docker.
			err = copyRunfiles("../cli/interactive_tests", interactivetestsDir)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = os.RemoveAll(interactivetestsDir)
			}()
			hostConfig.Binds = append(hostConfig.Binds, interactivetestsDir+":/mnt/interactive_tests")
		}

		// Add a randomID to the container name to avoid overlap between tests running on
		// different shards.
		var nBig *big.Int
		nBig, err = rand.Int(rand.Reader, big.NewInt(10000000))
		if err != nil {
			t.Fatal(err)
		}
		n := nBig.Int64()
		name = name + "-" + strconv.Itoa(int(n))

		// Prepare the docker cluster.
		// We need to do this "under" the directory preparation above so as
		// to prevent the test from crashing because the directory gets
		// deleted before the container shutdown assertions get a chance to run.
		cfg := cluster.TestConfig{
			Name:     name,
			Duration: *flagDuration,
		}
		for i := 0; i < num; i++ {
			cfg.Nodes = append(cfg.Nodes, cluster.NodeConfig{Stores: []cluster.StoreConfig{{}}})
		}
		l := StartCluster(ctx, t, cfg).(*cluster.DockerCluster)

		var preserveLogs bool
		defer func() {
			// Check the final health of the cluster nodes and
			// stop the cluster after that.
			l.AssertAndStop(ctx, t)

			// Note: we must be careful to clean up the volumes *after*
			// the cluster has been shut down (in the `AssertAndStop` call).
			// Otherwise, the directory removal will cause the cluster nodes
			// to crash and report abnormal termination, even when the test
			// succeeds otherwise.
			log.Infof(ctx, "cleaning up docker volume")
			l.Cleanup(ctx, preserveLogs)
		}()

		if len(l.Nodes) > 0 {
			containerConfig.Env = append(containerConfig.Env, "PGHOST="+l.Hostname(0))
		}

		log.Infof(ctx, "starting one-shot container")
		err = l.OneShot(
			ctx, acceptanceImage, types.ImagePullOptions{}, containerConfig, hostConfig,
			platforms.DefaultSpec(), "docker-"+name,
		)
		log.Infof(ctx, "one-shot container terminated: %v", err)
		preserveLogs = err != nil
	})
	return err
}

// Bazel uses symlinks in the runfiles directory. If a directory with symlinks is mounted inside a docker container,
// the symlinks point to not existing destination.
// This function copies the content of the symlinks to another directory,
// so the files can be used inside a docker container. The caller function is responsible for cleaning up.
// This function doesn't copy the original file permissions and uses 755 for directories and files.
func copyRunfiles(source, destination string) error {
	return filepath.WalkDir(source, func(path string, dirEntry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relPath := strings.Replace(path, source, "", 1)
		if relPath == "" {
			return nil
		}
		if dirEntry.IsDir() {
			return os.Mkdir(filepath.Join(destination, relPath), 0755)
		}
		data, err := os.ReadFile(filepath.Join(source, relPath))
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(destination, relPath), data, 0755)
	})
}

func testDockerSingleNode(
	ctx context.Context, t *testing.T, name string, containerConfig container.Config,
) error {
	return testDocker(ctx, t, 1, name, containerConfig)
}

func testDockerOneShot(
	ctx context.Context, t *testing.T, name string, containerConfig container.Config,
) error {
	return testDocker(ctx, t, 0, name, containerConfig)
}

var cmdBase = []string{
	"/usr/bin/env",
	"COCKROACH_SKIP_UPDATE_CHECK=1",
	"COCKROACH_CRASH_REPORTS=",
	// Disable metamorphic testing for acceptance tests, since they are
	// end-to-end tests and metamorphic constants can make them too slow.
	"COCKROACH_INTERNAL_DISABLE_METAMORPHIC_TESTING=true",
	"/bin/bash",
	"-c",
}

func runTestDockerCLI(t *testing.T, testNameSuffix, testFilePath string) {
	containerConfig := defaultContainerConfig()
	containerConfig.Cmd = []string{"stat", cluster.CockroachBinaryInContainer}
	containerConfig.Env = []string{
		"CI=1", // Disables the initial color query by the termenv library.
		fmt.Sprintf("PGUSER=%s", username.RootUser),
		fmt.Sprintf("COCKROACH_DEV_LICENSE=%s", envutil.EnvOrDefaultString("COCKROACH_DEV_LICENSE", "")),
	}
	ctx := context.Background()
	if err := testDockerOneShot(ctx, t, "cli_test_"+testNameSuffix, containerConfig); err != nil {
		skip.IgnoreLintf(t, "TODO(dt): No binary in one-shot container, see #6086: %s", err)
	}

	containerPath := "/go/src/github.com/cockroachdb/cockroach/cli/interactive_tests"
	if bazel.BuiltWithBazel() {
		containerPath = "/mnt/interactive_tests"
	}
	testFile := filepath.Base(testFilePath)
	testPath := filepath.Join(containerPath, testFile)
	t.Run(testFile, func(t *testing.T) {
		log.Infof(ctx, "-- starting tests from: %s", testFile)

		// Symlink the logs directory to /logs, which is visible outside of the
		// container and preserved if the test fails. (They don't write to /logs
		// directly because they are often run manually outside of Docker, where
		// /logs is unlikely to exist.)
		cmd := "ln -s /logs logs"

		// We run the expect command using 'bash -c "(expect ...)"'.
		//
		// We cannot run "expect" directly, nor "bash -c 'expect ...'",
		// because both cause Expect to become the PID 1 process inside
		// the container. On Unix, orphan processes need to be wait()ed
		// upon by the PID 1 process when they terminate, lest they
		// remain forever in the zombie state. Unfortunately, Expect
		// does not contain code to do this. Bash does.
		cmd += "; (expect -d -f " + testPath + " " + cluster.CockroachBinaryInContainer + ")"
		containerConfig.Cmd = append(cmdBase, cmd)

		if err := testDockerOneShot(ctx, t, "cli_test_"+testNameSuffix, containerConfig); err != nil {
			t.Error(err)
		}
	})
}
