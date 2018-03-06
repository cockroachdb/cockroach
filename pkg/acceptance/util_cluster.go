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
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/acceptance/localcluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/binfetcher"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

const (
	localTest  = "runMode=local"
	dockerTest = "runMode=docker"
)

// RunLocal runs the given acceptance test using a bare cluster.
func RunLocal(t *testing.T, testee func(t *testing.T)) {
	t.Run(localTest, testee)
}

// RunDocker runs the given acceptance test using a Docker cluster.
func RunDocker(t *testing.T, testee func(t *testing.T)) {
	t.Run(dockerTest, testee)
}

// turns someTest#123 into someTest when invoked with ReplicaAllLiteralString.
// This is useful because the go test harness automatically disambiguates
// subtests in that way when they are invoked multiple times with the same name,
// and we sometimes call RunDocker multiple times in tests.
var reStripTestEnumeration = regexp.MustCompile(`#\d+$`)

// runTestWithCluster runs the passed in test against the configuration
// specified by the flags. If any options are specified, they may mutate the
// test config before it runs.
func runTestWithCluster(
	t *testing.T,
	testFunc func(context.Context, *testing.T, cluster.Cluster, cluster.TestConfig),
	options ...func(*cluster.TestConfig),
) {
	cfg := readConfigFromFlags()
	ctx := context.Background()

	for _, opt := range options {
		opt(&cfg)
	}

	cluster := StartCluster(ctx, t, cfg)
	log.Infof(ctx, "cluster started successfully")
	defer cluster.AssertAndStop(ctx, t)
	testFunc(ctx, t, cluster, cfg)
}

// StartCluster starts a cluster from the relevant flags. All test clusters
// should be created through this command since it sets up the logging in a
// unified way.
func StartCluster(ctx context.Context, t *testing.T, cfg cluster.TestConfig) (c cluster.Cluster) {
	var completed bool
	defer func() {
		if !completed && c != nil {
			c.AssertAndStop(ctx, t)
		}
	}()

	parts := strings.Split(t.Name(), "/")
	if len(parts) < 2 {
		t.Fatal("must invoke RunLocal or RunDocker")
	}

	var runMode string
	for _, part := range parts[1:] {
		part = reStripTestEnumeration.ReplaceAllLiteralString(part, "")
		switch part {
		case localTest:
			fallthrough
		case dockerTest:
			if runMode != "" {
				t.Fatalf("test has more than one run mode: %s and %s", runMode, part)
			}
			runMode = part
		}
	}

	switch runMode {
	case localTest:
		pwd, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		dataDir, err := ioutil.TempDir(pwd, ".localcluster")
		if err != nil {
			t.Fatal(err)
		}

		logDir := *flagLogDir
		if logDir != "" {
			logDir = filepath.Join(logDir, filepath.Clean(t.Name()))
		}

		perNodeCfg := map[int]localcluster.NodeConfig{}
		for i := 0; i < len(cfg.Nodes); i++ {
			// TODO(tschottdorf): handle Nodes[i].Stores properly.
			if cfg.Nodes[i].Version != "" {
				var nCfg localcluster.NodeConfig
				nCfg.Binary = GetBinary(ctx, t, cfg.Nodes[i].Version)
				perNodeCfg[i] = nCfg
			}
		}
		clusterCfg := localcluster.ClusterConfig{
			Ephemeral:  true,
			DataDir:    dataDir,
			LogDir:     logDir,
			NumNodes:   len(cfg.Nodes),
			PerNodeCfg: perNodeCfg,
		}

		l := localcluster.New(clusterCfg)

		l.Start(ctx)
		c = &localcluster.LocalCluster{Cluster: l}

	case dockerTest:
		logDir := *flagLogDir
		if logDir != "" {
			logDir = filepath.Join(logDir, filepath.Clean(t.Name()))
		}
		l := cluster.CreateDocker(ctx, cfg, logDir, stopper)
		l.Start(ctx)
		c = l

	default:
		t.Fatalf("unable to run in mode %q, use either RunLocal or RunDocker", runMode)
	}

	// Don't wait for replication unless requested (usually it is).
	if !cfg.NoWait && cfg.InitMode != cluster.INIT_NONE {
		wantedReplicas := 3
		if numNodes := c.NumNodes(); numNodes < wantedReplicas {
			wantedReplicas = numNodes
		}

		// Looks silly, but we actually start zero-node clusters in the
		// reference tests.
		if wantedReplicas > 0 {
			log.Infof(ctx, "waiting for first range to have %d replicas", wantedReplicas)

			testutils.SucceedsSoon(t, func() error {
				select {
				case <-stopper.ShouldStop():
					t.Fatal("interrupted")
				case <-time.After(time.Second):
				}

				// Always talk to node 0 because it's guaranteed to exist.
				db, err := c.NewDB(ctx, 0)
				if err != nil {
					t.Fatal(err)
				}
				rows, err := db.Query(`SELECT ARRAY_LENGTH(replicas, 1) FROM crdb_internal.ranges LIMIT 1`)
				if err != nil {
					// Versions <= 1.1 do not contain the crdb_internal table, which is what's used
					// to determine whether a cluster has up-replicated. This is relevant for the
					// version upgrade acceptance test. Just skip the replication check for this case.
					if testutils.IsError(err, "(table|relation) \"crdb_internal.ranges\" does not exist") {
						return nil
					}
					t.Fatal(err)
				}
				defer rows.Close()
				var foundReplicas int
				if rows.Next() {
					if err = rows.Scan(&foundReplicas); err != nil {
						t.Fatalf("unable to scan for length of replicas array: %s", err)
					}
					if log.V(1) {
						log.Infof(ctx, "found %d replicas", foundReplicas)
					}
				} else {
					return errors.Errorf("no ranges listed")
				}

				if foundReplicas < wantedReplicas {
					return errors.Errorf("expected %d replicas, only found %d", wantedReplicas, foundReplicas)
				}
				return nil
			})
		}

		// Ensure that all nodes are serving SQL by making sure a simple
		// read-only query succeeds.
		for i := 0; i < c.NumNodes(); i++ {
			testutils.SucceedsSoon(t, func() error {
				db, err := c.NewDB(ctx, i)
				if err != nil {
					return err
				}
				if _, err := db.Exec("SHOW DATABASES"); err != nil {
					return err
				}
				return nil
			})
		}
	}

	completed = true
	return c
}

// GetBinary retrieves a binary for the specified version and returns it.
func GetBinary(ctx context.Context, t *testing.T, version string) string {
	t.Helper()
	bin, err := binfetcher.Download(ctx, binfetcher.Options{
		Binary:  "cockroach",
		Dir:     ".localcluster_cache",
		Version: version,
	})
	if err != nil {
		t.Fatalf("unable to set up binary for v%s: %s", version, err)
	}
	return bin
}
