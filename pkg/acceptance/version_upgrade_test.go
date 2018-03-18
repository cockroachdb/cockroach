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
	cfg := readConfigFromFlags()
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
type binaryVersionUpgrade string

// sourceVersion corresponds to the source binary version.
const sourceVersion = "source"

func (bv binaryVersionUpgrade) name() string { return fmt.Sprintf("binary=%s", bv) }

func (bv binaryVersionUpgrade) run(ctx context.Context, t *testing.T, c cluster.Cluster) {
	t.Helper()

	var newBin string
	if string(bv) == sourceVersion {
		newBin = localcluster.SourceBinary()
	} else {
		newBin = GetBinary(ctx, t, string(bv))
	}
	lc := c.(*localcluster.LocalCluster)
	for i := 0; i < c.NumNodes(); i++ {
		log.Infof(ctx, "upgrading node %d's binary to %s", i, string(bv))
		lc.ReplaceBinary(i, newBin)
		if err := lc.Restart(ctx, i); err != nil {
			t.Fatal(err)
		}

		bv.checkNode(ctx, t, c, i)

		// TODO(nvanbenschoten): add upgrade qualification step. What should we
		// test? We could run logictests. We could add custom logic here. Maybe
		// this should all be pushed to nightly migration tests instead.
		time.Sleep(1 * time.Second)
	}
}

func (bv binaryVersionUpgrade) checkAll(ctx context.Context, t *testing.T, c cluster.Cluster) {
	t.Helper()
	for i := 0; i < c.NumNodes(); i++ {
		bv.checkNode(ctx, t, c, i)
	}
}

func (bv binaryVersionUpgrade) checkNode(
	ctx context.Context, t *testing.T, c cluster.Cluster, nodeIdx int,
) {
	t.Helper()
	db := makePGClient(t, c.PGUrl(ctx, nodeIdx))
	defer db.Close()

	// 'Version' for 1.1, 'Tag' in 1.0.x.
	var version string
	if err := db.QueryRow(
		`SELECT value FROM crdb_internal.node_build_info where field IN ('Version' , 'Tag')`,
	).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != string(bv) && string(bv) != sourceVersion {
		t.Fatalf("created node at v%s, but it is %s", string(bv), version)
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
type clusterVersionUpgrade string

func (cv clusterVersionUpgrade) name() string { return fmt.Sprintf("cluster=%s", cv) }

func (cv clusterVersionUpgrade) run(ctx context.Context, t *testing.T, c cluster.Cluster) {
	t.Helper()

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
	v, err := roachpb.ParseVersion(string(cv))
	if err != nil {
		t.Fatal(err)
	}
	hasShowSettingBug := v.Less(roachpb.Version{Major: 1, Minor: 1, Unstable: 1})

	func() {
		db := makePGClient(t, c.PGUrl(ctx, rand.Intn(c.NumNodes())))
		defer db.Close()

		log.Infof(ctx, "upgrading cluster version to %s", cv)
		if _, err := db.Exec(fmt.Sprintf(`SET CLUSTER SETTING version = '%s'`, cv)); err != nil {
			t.Fatal(err)
		}
	}()

	if hasShowSettingBug {
		log.Infof(ctx, "using workaround for upgrade to %s", cv)
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
			if version != string(cv) {
				return errors.Errorf("%d: expected version %s, got %s", i, cv, version)
			}
			return nil
		})
	}

	log.Infof(ctx, "cluster is now at version %s", cv)

	// TODO(nvanbenschoten): add upgrade qualification step.
	time.Sleep(1 * time.Second)
}

func testVersionUpgrade(ctx context.Context, t *testing.T, cfg cluster.TestConfig) {
	steps := []versionStep{
		binaryVersionUpgrade("v1.0.6"),
		// v1.1.0 is the first binary version that knows about cluster versions,
		// but thinks it can only support up to 1.0-3.
		binaryVersionUpgrade("v1.1.0"),
		clusterVersionUpgrade("1.0"),
		clusterVersionUpgrade("1.0-3"),
		binaryVersionUpgrade("v1.1.1"),
		clusterVersionUpgrade("1.1"),
		binaryVersionUpgrade(sourceVersion),
		clusterVersionUpgrade("1.1-6"),
		clusterVersionUpgrade("2.0"),
		clusterVersionUpgrade(clusterversion.BinaryServerVersion.String()),
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
		cfg.Nodes[i].Version = string(startingBinVersion)
	}

	c := StartCluster(ctx, t, cfg)
	defer c.AssertAndStop(ctx, t)

	startingBinVersion.checkAll(ctx, t, c)
	for _, step := range steps {
		t.Log(step.name())
		step.run(ctx, t, c)
		if s, ok := step.(clusterVersionUpgrade); ok {
			for _, feature := range features {
				testFeature(ctx, t, c, feature, string(s))
			}
		}
	}
}
