// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"os"
	"runtime"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

func registerFixtures(r registry.Registry) {
	// Run this test to create a new fixture for the version upgrade test. This
	// is necessary after every release. For example, the day `master` becomes
	// the 20.2 release, this test will fail because it is missing a fixture for
	// 20.1; run the test (on 20.1). Check it in (instructions will be logged
	// below) and off we go.
	//
	// The version to create/update the fixture for must be released
	// (i.e.  can download it from the homepage). For example, to make a
	// "v20.2" fixture, you will need a binary that has "v20.2" in the
	// output of `./cockroach version`, and this process will end up
	// creating fixtures that have "v20.2" in them. This would be part
	// of tagging the master branch as v21.1 in the process of going
	// through the major release for v20.2. The version is passed in as
	// FIXTURE_VERSION environment variable. The contents of this
	// environment variable must match a released cockroach binary.
	//
	// Please note that you do *NOT* need to update the fixtures in a patch
	// release. This only happens as part of preparing the master branch for the
	// next release. The release team runbooks, at time of writing, reflect
	// this.
	//
	// Example invocation:
	//   FIXTURE_VERSION=v20.2.0-beta.1 roachtest --local run generate-fixtures \
	//     --debug --cockroach ./cockroach --suite fixtures
	runFixtures := func(
		ctx context.Context,
		t test.Test,
		c cluster.Cluster,
	) {
		if c.IsLocal() && runtime.GOARCH == "arm64" {
			t.Skip("Skip under ARM64. See https://github.com/cockroachdb/cockroach/issues/89268")
		}
		fixtureVersion := os.Getenv("FIXTURE_VERSION")
		if fixtureVersion == "" {
			t.Fatal("FIXTURE_VERSION must be set")
		}
		makeVersionFixtureAndFatal(ctx, t, c, fixtureVersion)
	}

	r.Add(registry.TestSpec{
		Name:             "generate-fixtures",
		Timeout:          30 * time.Minute,
		CompatibleClouds: registry.Clouds(spec.GCE, spec.Local),
		Suites:           registry.Suites(registry.Fixtures),
		Owner:            registry.OwnerTestEng,
		Cluster:          r.MakeClusterSpec(4),
		Run:              runFixtures,
	})
}
