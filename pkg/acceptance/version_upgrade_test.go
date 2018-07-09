// Copyright 2016 The Cockroach Authors.
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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/acceptance/localcluster"
	clusterversion "github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestVersionUpgrade(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	cfg := ReadConfigFromFlags()
	RunLocal(t, func(t *testing.T) {
		testVersionUpgrade(ctx, t, cfg)
	})
}

// versionStep is an isolated version migration on a running cluster.
type versionStep interface {
	name() string
	run(ctx context.Context, t *testing.T, c cluster.Cluster)
}

// binaryVersionUpgrade performs a rolling upgrade of all nodes in the cluster
// up to its binary version.
type binaryVersionUpgrade struct {
	newVersion string
	// If not empty when constructed, these nodes' data dir will be wiped before starting.
	wipeNodes []int
}

// sourceVersion corresponds to the source binary version.
const sourceVersion = "source"

func (bv binaryVersionUpgrade) name() string { return fmt.Sprintf("binary=%s", bv.newVersion) }

func (bv binaryVersionUpgrade) run(ctx context.Context, t *testing.T, c cluster.Cluster) {
	var newBin string
	if bv.newVersion == sourceVersion {
		newBin = localcluster.SourceBinary()
	} else {
		newBin = GetBinary(ctx, t, bv.newVersion)
	}
	lc := c.(*localcluster.LocalCluster)

	var wipeData bool
	if len(bv.wipeNodes) == 0 {
		bv.wipeNodes = make([]int, c.NumNodes())
		for i := 0; i < c.NumNodes(); i++ {
			bv.wipeNodes[i] = i
		}
	} else {
		wipeData = true
	}

	// Restart nodes in a random order; otherwise it'd be node 0 running the
	// migrations and it probably also has all the leases.
	firstToRestart := rand.Intn(len(bv.wipeNodes))
	for _, j := range bv.wipeNodes {
		nodeIdx := (j + firstToRestart) % len(bv.wipeNodes)
		if wipeData {
			// Nodes to upgrade is provided, wipe the old data and start them fresh.
			if err := lc.RemoveNodeData(nodeIdx); err != nil {
				t.Fatal(err)
			}
			log.Infof(ctx, "wiping node %d's data directory", nodeIdx)
		}
		log.Infof(ctx, "upgrading node at index %d's binary to %s", nodeIdx, bv.newVersion)
		lc.ReplaceBinary(nodeIdx, newBin)
		if err := lc.Restart(ctx, nodeIdx); err != nil {
			t.Fatal(err)
		}

		bv.checkNode(ctx, t, c, nodeIdx)

		// TODO(nvanbenschoten): add upgrade qualification step. What should we
		// test? We could run logictests. We could add custom logic here. Maybe
		// this should all be pushed to nightly migration tests instead.
		time.Sleep(1 * time.Second)
	}
}

func (bv binaryVersionUpgrade) checkAll(ctx context.Context, t *testing.T, c cluster.Cluster) {
	for i := 0; i < c.NumNodes(); i++ {
		bv.checkNode(ctx, t, c, i)
	}
}

func (bv binaryVersionUpgrade) checkNode(
	ctx context.Context, t *testing.T, c cluster.Cluster, nodeIdx int,
) {
	db := makePGClient(t, c.PGUrl(ctx, nodeIdx))
	defer db.Close()

	// 'Version' for 1.1, 'Tag' in 1.0.x.
	var version string
	if err := db.QueryRow(
		`SELECT value FROM crdb_internal.node_build_info where field IN ('Version' , 'Tag')`,
	).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != bv.newVersion && bv.newVersion != sourceVersion {
		t.Fatalf("created node at v%s, but it is %s", bv.newVersion, version)
	}
}

type feature struct {
	name              string
	minAllowedVersion string
	query             string
}

func testFeature(ctx context.Context, t *testing.T, c cluster.Cluster, f feature, cv string) {
	db := makePGClient(t, c.PGUrl(ctx, 0 /* nodeId */))
	defer db.Close()

	minAllowedVersion, err := roachpb.ParseVersion(f.minAllowedVersion)
	if err != nil {
		t.Fatal(err)
	}

	actualVersion, err := roachpb.ParseVersion(cv)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(f.query)
	if actualVersion.Less(minAllowedVersion) {
		if err == nil {
			t.Fatalf("expected %s to fail on cluster version %s", f.name, cv)
		}
	} else {
		if err != nil {
			t.Fatalf("expected %s to succeed on cluster version %s, got %s", f.name, cv, err)
		}
	}
}

// clusterVersionUpgrade performs a cluster version upgrade to its version.
// It waits until all nodes have seen the upgraded cluster version.
type clusterVersionUpgrade struct {
	newVersion string
	manual     bool
}

func (cv clusterVersionUpgrade) name() string { return fmt.Sprintf("cluster=%s", cv.newVersion) }

func (cv clusterVersionUpgrade) run(ctx context.Context, t *testing.T, c cluster.Cluster) {
	// hasShowSettingBug is true when we're working around
	// https://github.com/cockroachdb/cockroach/issues/22796.
	//
	// The problem there is that `SHOW CLUSTER SETTING version` does not take into
	// account the gossiped value of that setting but reads it straight from the
	// KV store. This means that even though a node may report a certain version,
	// it may not actually have processed it yet, which leads to illegal upgrades
	// in this test. When this flag is set to true, we query
	// `crdb_internal.cluster_settings` instead, which *does* take everything from
	// Gossip.
	v, err := roachpb.ParseVersion(cv.newVersion)
	if err != nil {
		t.Fatal(err)
	}
	hasShowSettingBug := v.Less(roachpb.Version{Major: 1, Minor: 1, Unstable: 1})

	if cv.manual {
		func() {
			db := makePGClient(t, c.PGUrl(ctx, rand.Intn(c.NumNodes())))
			defer db.Close()

			log.Infof(ctx, "upgrading cluster version to %s", cv.newVersion)
			if _, err := db.Exec(fmt.Sprintf(`SET CLUSTER SETTING version = '%s'`, cv.newVersion)); err != nil {
				t.Fatal(err)
			}
		}()
	}

	if hasShowSettingBug {
		log.Infof(ctx, "using workaround for upgrade to %s", cv.newVersion)
	}

	for i := 0; i < c.NumNodes(); i++ {
		testutils.SucceedsSoon(t, func() error {
			db := makePGClient(t, c.PGUrl(ctx, i))
			defer db.Close()

			var version string
			if !hasShowSettingBug {
				if err := db.QueryRow("SHOW CLUSTER SETTING version").Scan(&version); err != nil {
					t.Fatalf("%d: %s", i, err)
				}
			} else {
				// This uses the receiving node's Gossip and as such allows us to verify that all of the
				// nodes have gotten wind of the version bump.
				if err := db.QueryRow(
					`SELECT current_value FROM crdb_internal.cluster_settings WHERE name = 'version'`,
				).Scan(&version); err != nil {
					t.Fatalf("%d: %s", i, err)
				}
			}
			if version != cv.newVersion {
				return errors.Errorf("%d: expected version %s, got %s", i, cv.newVersion, version)
			}
			return nil
		})
	}

	log.Infof(ctx, "cluster is now at version %s", cv.newVersion)

	// TODO(nvanbenschoten): add upgrade qualification step.
	time.Sleep(1 * time.Second)
}

func min(i, j int) int {
	if i < j {
		return i
	}
	return j
}

func testVersionUpgrade(ctx context.Context, t *testing.T, cfg cluster.TestConfig) {
	steps := []versionStep{
		binaryVersionUpgrade{newVersion: "v1.0.6"},
		// v1.1.0 is the first binary version that knows about cluster versions,
		// but thinks it can only support up to 1.0-3.
		binaryVersionUpgrade{newVersion: "v1.1.0"},
		clusterVersionUpgrade{newVersion: "1.0", manual: true},
		clusterVersionUpgrade{newVersion: "1.0-3", manual: true},

		binaryVersionUpgrade{newVersion: "v1.1.1"},
		clusterVersionUpgrade{newVersion: "1.1", manual: true},

		// Wipe out the data dir for the last nodes to start it fresh.
		binaryVersionUpgrade{newVersion: "v2.0.0", wipeNodes: []int{min(3, len(cfg.Nodes)) - 1}},

		binaryVersionUpgrade{newVersion: "v2.0.0"},
		clusterVersionUpgrade{newVersion: "1.1-6", manual: true},
		clusterVersionUpgrade{newVersion: "2.0", manual: true},

		binaryVersionUpgrade{newVersion: sourceVersion},
		clusterVersionUpgrade{newVersion: clusterversion.BinaryServerVersion.String()},
	}

	features := []feature{
		{
			name:              "JSONB",
			minAllowedVersion: "2.0-0",
			query: `
			CREATE DATABASE IF NOT EXISTS test;
			CREATE TABLE test.t (j JSONB);
			DROP TABLE test.t;`,
		}, {
			name:              "Sequences",
			minAllowedVersion: "2.0-0",
			query: `
			CREATE DATABASE IF NOT EXISTS test;
			CREATE SEQUENCE test.test_sequence;
			DROP SEQUENCE test.test_sequence;`,
		}, {
			name:              "Computed Columns",
			minAllowedVersion: "2.0-0",
			query: `
			CREATE DATABASE IF NOT EXISTS test;
			CREATE TABLE test.t (x INT AS (3) STORED);
			DROP TABLE test.t;`,
		},
	}

	if len(cfg.Nodes) > 3 {
		cfg.Nodes = cfg.Nodes[:3]
	}
	startingBinVersion := steps[0].(binaryVersionUpgrade)
	steps = steps[1:]
	for i := range cfg.Nodes {
		cfg.Nodes[i].Version = startingBinVersion.newVersion
	}

	c := StartCluster(ctx, t, cfg)
	defer c.AssertAndStop(ctx, t)

	// Create a bunch of tables, over the batch size on which some migrations
	// operate. It generally seems like a good idea to have a bunch of tables in
	// the cluster, and we had a bug about migrations on large numbers of tables:
	// #22370.
	db := makePGClient(t, c.PGUrl(ctx, 0 /* nodeId */))
	defer db.Close()
	if _, err := db.Exec(fmt.Sprintf("create database lotsatables")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		_, err := db.Exec(fmt.Sprintf("create table lotsatables.t%d (x int primary key)", i))
		if err != nil {
			t.Fatal(err)
		}
	}

	startingBinVersion.checkAll(ctx, t, c)
	for _, step := range steps {
		t.Log(step.name())
		step.run(ctx, t, c)
		if s, ok := step.(clusterVersionUpgrade); ok {
			for _, feature := range features {
				testFeature(ctx, t, c, feature, s.newVersion)
			}
		}
	}
}
