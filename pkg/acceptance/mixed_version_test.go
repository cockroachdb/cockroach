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
	"testing"

	gosql "database/sql"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
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

func testMixedVersionHarness(
	ctx context.Context, t *testing.T, cfg cluster.TestConfig,
) {
	for i := range cfg.Nodes {
		if i == 0 {
			cfg.Nodes[i].Version = "v1.0.5"
		} else {
			// Leave the field blank: this uses the version we're supposed to
			// test in this run.
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
}
