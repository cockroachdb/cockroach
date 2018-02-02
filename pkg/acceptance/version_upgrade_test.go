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
	"testing"

	gosql "database/sql"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/acceptance/localcluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
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

func testVersionUpgrade(ctx context.Context, t *testing.T, cfg cluster.TestConfig) {

	// Version 1.0.5 does not contain
	// https://github.com/cockroachdb/cockroach/pull/19493 and the test will be
	// flaky.
	//
	// TODO(tschottdorf): The first version containing that PR will (likely) be
	// included in 1.1.2. Check and remove once that's passed and this test has
	// v1.0.5 bumped to a higher version that includes #19493.
	if len(cfg.Nodes) > 3 {
		cfg.Nodes = cfg.Nodes[:3]
	}

	for i := range cfg.Nodes {
		// Leave the field blank for all but the first node so that they use the
		// version we're testing in this run.
		if i == 0 {
			cfg.Nodes[i].Version = "v1.0.5"
		}
	}

	c := StartCluster(ctx, t, cfg)
	defer c.AssertAndStop(ctx, t)

	// Verify that the nodes are *really* at the versions configured. This
	// tests the CI harness.
	log.Info(ctx, "verifying that configured versions are actual versions")
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
	log.Info(ctx, "upgrading the first node's binary to match the other nodes (i.e. the testing binary)")
	lc.Nodes[0].Cfg.ExtraArgs[0] = lc.Nodes[1].Cfg.ExtraArgs[0]

	var chs []<-chan error
	log.Info(ctx, "restarting the nodes asynchronously")
	for i := 0; i < c.NumNodes(); i++ {
		chs = append(chs, lc.RestartAsync(ctx, i))
	}

	for _, ch := range chs {
		if err := <-ch; err != nil {
			t.Fatal(err)
		}
	}

	func() {
		db := makePGClient(t, c.PGUrl(ctx, 0))
		defer db.Close()

		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM system.public.settings WHERE name = 'version';").Scan(&count); err != nil {
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
	}()

	bumps := []string{"1.0", "1.1", "1.1-2"}

	for i, bump := range bumps {
		func() {
			db := makePGClient(t, c.PGUrl(ctx, i%c.NumNodes()))
			defer db.Close()
			if _, err := db.Exec(fmt.Sprintf(`SET CLUSTER SETTING version = '%s'`, bump)); err != nil {
				t.Fatal(err)
			}
		}()
	}

	for i := 0; i < c.NumNodes(); i++ {
		testutils.SucceedsSoon(t, func() error {
			db := makePGClient(t, c.PGUrl(ctx, i))
			defer db.Close()

			var version string
			if err := db.QueryRow("SHOW CLUSTER SETTING version").Scan(&version); err != nil {
				t.Fatalf("%d: %s", i, err)
			}
			if exp := bumps[len(bumps)-1]; version != exp {
				return errors.Errorf("%d: expected version %s, got %s", i, exp, version)
			}
			return nil
		})
	}
}
