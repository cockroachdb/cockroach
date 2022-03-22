// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
var gormSupportedTag = "v1.23.1"

func registerGORM(r registry.Registry) {
	runGORM := func(ctx context.Context, t test.Test, c cluster.Cluster) {
		if c.IsLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())
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
			ctx, t, c, node, "install go-junit-report", fmt.Sprintf("GOPATH=%s go get -u github.com/jstemmer/go-junit-report", goPath),
		); err != nil {
			t.Fatal(err)
		}

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

		if err := c.RunE(ctx, node, fmt.Sprintf("mkdir -p %s", resultsDir)); err != nil {
			t.Fatal(err)
		}

		blocklistName, expectedFailures, ignorelistName, ignoredFailures := gormBlocklists.getLists(version)
		if expectedFailures == nil {
			t.Fatalf("No gorm blocklist defined for cockroach version %s", version)
		}
		t.L().Printf("Running cockroach version %s, using blocklist %s, using ignorelist %s", version, blocklistName, ignorelistName)

		err = c.RunE(ctx, node, `./cockroach sql -e "CREATE DATABASE gorm" --insecure`)
		require.NoError(t, err)

		t.Status("downloading go dependencies for tests")
		err = c.RunE(
			ctx,
			node,
			fmt.Sprintf(`cd %s && go get -u -t ./... && go mod download && go mod tidy `, gormTestPath),
		)
		require.NoError(t, err)

		t.Status("running gorm test suite and collecting results")

		// Ignore the error as there will be failing tests.
		err = c.RunE(
			ctx,
			node,
			fmt.Sprintf(`cd %s && GORM_DIALECT="postgres" GORM_DSN="user=root password= dbname=gorm host=localhost port=26257 sslmode=disable"
				go test -v ./... 2>&1 | %s/bin/go-junit-report > %s`,
				gormTestPath, goPath, resultsPath),
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
		Name:    "gorm",
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(1),
		Tags:    []string{`default`, `orm`},
		Run:     runGORM,
	})
}
