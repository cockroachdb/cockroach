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

// clusterVersionUpgrade performs a cluster version upgrade to its version.
// It waits until all nodes have seen the upgraded cluster version.
type clusterVersionUpgrade string

func (cv clusterVersionUpgrade) name() string { return fmt.Sprintf("cluster=%s", cv) }

func (cv clusterVersionUpgrade) run(ctx context.Context, t *testing.T, c cluster.Cluster) {
	t.Helper()

	func() {
		db := makePGClient(t, c.PGUrl(ctx, rand.Intn(c.NumNodes())))
		defer db.Close()

		log.Infof(ctx, "upgrading cluster version to %s", string(cv))
		if _, err := db.Exec(fmt.Sprintf(`SET CLUSTER SETTING version = '%s'`, string(cv))); err != nil {
			t.Fatal(err)
		}
	}()

	for i := 0; i < c.NumNodes(); i++ {
		testutils.SucceedsSoon(t, func() error {
			db := makePGClient(t, c.PGUrl(ctx, i))
			defer db.Close()

			var version string
			if err := db.QueryRow("SHOW CLUSTER SETTING version").Scan(&version); err != nil {
				t.Fatalf("%d: %s", i, err)
			}
			if version != string(cv) {
				return errors.Errorf("%d: expected version %s, got %s", i, string(cv), version)
			}
			return nil
		})
	}

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
		clusterVersionUpgrade(clusterversion.BinaryServerVersion.String()),
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
	}
}
