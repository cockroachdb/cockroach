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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/containerd/containerd/platforms"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
)

func defaultContainerConfig() container.Config {
	return container.Config{
		Image: acceptanceImage,
		Env: []string{
			fmt.Sprintf("PGUSER=%s", security.RootUser),
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
		hostConfig := container.HostConfig{
			NetworkMode: "host",
			Binds:       []string{filepath.Join(pwd, "testdata") + ":/mnt/data"},
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
