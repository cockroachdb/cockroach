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
	"testing"

	"golang.org/x/net/context"

	gosql "database/sql"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/acceptance/localcluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestMixedVersion(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	cfg := readConfigFromFlags()
	RunLocal(t, func(t *testing.T) {
		testMixedVersionHarness(ctx, t, cfg)
	})
}

func testMixedVersionHarness(ctx context.Context, t *testing.T, cfg cluster.TestConfig) {
	for i := range cfg.Nodes {
		// Leave the field blank for all but the first node so that the use the
		// version we're testing in this run.
		if i == 0 {
			cfg.Nodes[i].Version = "v1.0.5"
		}
	}

	c := StartCluster(ctx, t, cfg)
	defer c.AssertAndStop(ctx, t)

	// Verify that the nodes are *really* at the versions configured. This
	// tests the CI harness.
	for i := 0; i < c.NumNodes(); i++ {
		db, err := gosql.Open("postgres", c.PGUrl(ctx, i))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		expVersion := cfg.Nodes[i].Version
		var version string
		// 'Version' for 1.1, 'Tag' in 1.0.x.
		if err := db.QueryRow(
			`SELECT value FROM crdb_internal.node_build_info where field IN ('Version' , 'Tag')`,
		).Scan(&version); err != nil {
			t.Fatal(err)
		}
		// Strip leading `v` and compare (if expVersion = "", we'll accept anything).
		if version != expVersion && expVersion != "" {
			t.Fatalf("created node at v%s, but it is %s", expVersion, version)
		}

		// Similarly, should see the bootstrap version (the version of the
		// first node) from the settings. The first node itself is skipped;
		// it doesn't know about this setting.

		if i == 0 {
			continue
		}

		if err := db.QueryRow("SHOW CLUSTER SETTING version").Scan(&version); err != nil {
			t.Fatal(err)
		}
		const exp = "1.0" // no leading `v` here
		if version != exp {
			t.Fatalf("%d: node running at %s, not %s", i, version, exp)
		}
	}

	for i := 0; i < c.NumNodes(); i++ {
		if err := c.Kill(ctx, i); err != nil {
			t.Fatal(err)
		}
	}

	lc := c.(*localcluster.LocalCluster)

	var chs []<-chan error
	// Restart the nodes asynchronously and in a way that doesn't start the
	// first node (at v1.0.5 first) (this is due to technical limitations in
	// v1.0.5 and the test harness). See (*LocalCluster).StartAsync() for
	// details.
	for i := c.NumNodes() - 1; i >= 0; i-- {
		chs = append(chs, lc.RestartAsync(ctx, i))
	}

	for _, ch := range chs {
		if err := <-ch; err != nil {
			t.Fatal(err)
		}
	}

	db, err := gosql.Open("postgres", c.PGUrl(ctx, 1))
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM system.settings WHERE name = 'version';").Scan(&count); err != nil {
		t.Fatal(err)
	}

	// Since there are nodes at >1.0 in the cluster, a migration that populates
	// the version setting should have run.
	//
	// NB: we could do this check before the restart as well. The new nodes
	// won't declare startup complete until migrations have run. But putting
	// it here also checks that the cluster still "works" after the restart.
	if count < 1 {
		t.Fatal("initial cluster version was not migrated in")
	}
}
