// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

var gormReleaseTag = regexp.MustCompile(`^v(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)

// WARNING: DO NOT MODIFY the name of the below constant/variable without approval from the docs team.
// This is used by docs automation to produce a list of supported versions for ORM's.
var gormSupportedTag = "v1.24.1"

func registerGORM(r registry.Registry) {
	runGORM := func(ctx context.Context, t test.Test, c cluster.Cluster) {
		if c.IsLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		c.Start(ctx, t.L(), option.NewStartOpts(sqlClientsInMemoryDB), install.MakeClusterSettings(), c.All())
		version, err := fetchCockroachVersion(ctx, t.L(), c, node[0])
		if err != nil {
			t.Fatal(err)
		}
		if err := alterZoneConfigAndClusterSettings(ctx, t, version, c, node[0]); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning gorm and installing prerequisites")
		latestTag, err := repeatGetLatestTag(
			ctx, t, "go-gorm", "gorm", gormReleaseTag)
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("Latest gorm release is %s.", latestTag)
		t.L().Printf("Supported gorm release is %s.", gormSupportedTag)

		installGolang(ctx, t, c, node)

		const (
			gormRepo     = "github.com/go-gorm/gorm"
			gormPath     = goPath + "/src/" + gormRepo
			gormTestPath = gormPath + "/tests/"
			resultsDir   = "~/logs/report/gorm"
			resultsPath  = resultsDir + "/report.xml"
		)

		// Remove any old gorm installations
		if err := repeatRunE(
			ctx, t, c, node, "remove old gorm", fmt.Sprintf("rm -rf %s", gormPath),
		); err != nil {
			t.Fatal(err)
		}

		// Install go-junit-report to convert test results to .xml format we know
		// how to work with.
		if err := repeatRunE(
			ctx, t, c, node, "install go-junit-report", fmt.Sprintf("GOPATH=%s go install github.com/jstemmer/go-junit-report@latest", goPath),
		); err != nil {
			t.Fatal(err)
		}
		// It's safer to clean up dependencies this way than it is to give the cluster
		// wipe root access.
		defer func() {
			c.Run(ctx, option.WithNodes(c.All()), "go clean -modcache")
		}()

		if err := repeatGitCloneE(
			ctx,
			t,
			c,
			fmt.Sprintf("https://%s.git", gormRepo),
			gormPath,
			gormSupportedTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		if err := c.RunE(ctx, option.WithNodes(node), fmt.Sprintf("mkdir -p %s", resultsDir)); err != nil {
			t.Fatal(err)
		}

		blocklistName, expectedFailures := "gormBlocklist", gormBlocklist
		ignorelistName, ignoredFailures := "gormIgnorelist", gormIgnorelist
		t.L().Printf("Running cockroach version %s, using blocklist %s, using ignorelist %s", version, blocklistName, ignorelistName)

		err = c.RunE(ctx, option.WithNodes(node), `./cockroach sql -e "CREATE DATABASE gorm" --url={pgurl:1}`)
		require.NoError(t, err)

		t.Status("downloading go dependencies for tests")
		err = c.RunE(
			ctx,
			option.WithNodes(node),
			fmt.Sprintf(`cd %s && go mod tidy && go mod download`, gormTestPath),
		)
		require.NoError(t, err)

		t.Status("running gorm test suite and collecting results")

		// Ignore the error as there will be failing tests.
		// TODO(rafi): migrate_test.go is removed here since it relies on
		// multi-dimensional arrays, which aren't supported, and leads to a panic in
		// the test runner.
		err = c.RunE(
			ctx,
			option.WithNodes(node),
			fmt.Sprintf(`cd %s && rm migrate_test.go &&
				GORM_DIALECT="postgres" GORM_DSN="user=%s password=%s dbname=gorm host=localhost port={pgport:1} sslmode=require"
				go test -v ./... 2>&1 | %s/bin/go-junit-report > %s`,
				gormTestPath, install.DefaultUser, install.DefaultPassword, goPath, resultsPath),
		)
		if err != nil {
			t.L().Printf("error whilst running tests (may be expected): %#v", err)
		}

		parseAndSummarizeJavaORMTestsResults(
			ctx, t, c, node, "gorm" /* ormName */, []byte(resultsPath),
			blocklistName, expectedFailures, ignoredFailures, version, latestTag,
		)
	}

	r.Add(registry.TestSpec{
		Name:             "gorm",
		Owner:            registry.OwnerSQLFoundations,
		Cluster:          r.MakeClusterSpec(1),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly, registry.ORM),
		Run:              runGORM,
	})
}
