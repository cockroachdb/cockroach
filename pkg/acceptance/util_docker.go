// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package acceptance

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
)

func defaultContainerConfig() container.Config {
	return container.Config{
		Image: postgresTestImage,
		Env: []string{
			fmt.Sprintf("PGPORT=%s", base.DefaultPort),
			"PGSSLCERT=/certs/node.crt",
			"PGSSLKEY=/certs/node.key",
		},
	}
}

type dockerTestConfig struct {
	name string
	// cmd is the command that which be run within the docker container. Only
	// needs to be set if a containerConfig has not already been produced.
	cmd []string
	// binds is a list of directories from the `pkg/acceptance` directory to mount
	// onto `/` for access within the docker container. They will have the same
	// name within the docker container as outside of it (see java_test.go which
	// mounts the java_test directory).
	binds []string
}

// testDockerFail ensures the specified docker cmd fails.
func testDockerFail(ctx context.Context, t *testing.T, dt dockerTestConfig) {
	containerConfig := defaultContainerConfig()
	containerConfig.Cmd = dt.cmd
	if err := testDockerSingleNode(ctx, t, dt, containerConfig); err == nil {
		t.Error("expected failure")
	}
}

// testDockerSuccess ensures the specified docker cmd succeeds.
func testDockerSuccess(ctx context.Context, t *testing.T, dt dockerTestConfig) {
	containerConfig := defaultContainerConfig()
	containerConfig.Cmd = dt.cmd
	if err := testDockerSingleNode(ctx, t, dt, containerConfig); err != nil {
		t.Error(err)
	}
}

const (
	// Iterating against a locally built version of the docker image can be done
	// by changing postgresTestImage to the hash of the container.
	postgresTestImage = "docker.io/cockroachdb/postgres-test:20171011-1414"
)

func testDocker(
	ctx context.Context, t *testing.T, num int, dt dockerTestConfig, containerConfig container.Config,
) error {
	var err error
	RunDocker(t, func(t *testing.T) {
		cfg := cluster.TestConfig{
			Name:     dt.name,
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
		var binds []string
		for _, b := range dt.binds {
			binds = append(binds, filepath.Join(pwd, b)+":/"+b)
		}
		hostConfig := container.HostConfig{
			NetworkMode: "host",
			Binds:       binds,
		}
		err = l.OneShot(
			ctx, postgresTestImage, types.ImagePullOptions{}, containerConfig, hostConfig, "docker-"+dt.name,
		)
		if err == nil {
			// Clean up the log files if the run was successful.
			l.Cleanup(ctx)
		}
	})
	return err
}

func testDockerSingleNode(
	ctx context.Context, t *testing.T, dt dockerTestConfig, containerConfig container.Config,
) error {
	return testDocker(ctx, t, 1, dt, containerConfig)
}

func testDockerOneShot(
	ctx context.Context, t *testing.T, dt dockerTestConfig, containerConfig container.Config,
) error {
	return testDocker(ctx, t, 0, dt, containerConfig)
}
