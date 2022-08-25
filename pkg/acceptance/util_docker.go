// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package acceptance

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/security/username"
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
	acceptanceImage = "docker.io/cockroachdb/acceptance:20200303-091324"
)

func testDocker(
	ctx context.Context, t *testing.T, num int, name string, containerConfig container.Config,
) error {
	var err error
	RunDocker(t, func(t *testing.T) {
		cfg := cluster.TestConfig{
			Name:     name,
			Duration: *flagDuration,
		}
		for i := 0; i < num; i++ {
			cfg.Nodes = append(cfg.Nodes, cluster.NodeConfig{Stores: []cluster.StoreConfig{{}}})
		}
		l := StartCluster(ctx, t, cfg).(*cluster.DockerCluster)
		defer l.AssertAndStop(ctx, t)

		if len(l.Nodes) > 0 {
			containerConfig.Env = append(containerConfig.Env, "PGHOST="+l.Hostname(0))
		}
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
		err = l.OneShot(
			ctx, acceptanceImage, types.ImagePullOptions{}, containerConfig, hostConfig,
			platforms.DefaultSpec(), "docker-"+name,
		)
		preserveLogs := err != nil
		l.Cleanup(ctx, preserveLogs)
	})
	return err
}

// Bazel uses symlinks in the runfiles directory. If a directory with symlinks is mounted inside a docker container,
// the symlinks point to not existing destination.
// This function copies the content of the symlinks to another directory,
// so the files can be used inside a docker container. The caller function is responsible for cleaning up.
// This function doesn't copy the original file permissions and uses 755 for directories and files.
func copyRunfiles(source, destination string) error {
	return filepath.WalkDir(source, func(path string, dirEntry os.DirEntry, walkErr error) error {
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
