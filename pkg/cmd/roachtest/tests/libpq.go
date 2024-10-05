// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

var libPQReleaseTagRegex = regexp.MustCompile(`^v(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)

// WARNING: DO NOT MODIFY the name of the below constant/variable without approval from the docs team.
// This is used by docs automation to produce a list of supported versions for ORM's.
var libPQSupportedTag = "v1.10.5"

func registerLibPQ(r registry.Registry) {
	runLibPQ := func(ctx context.Context, t test.Test, c cluster.Cluster) {
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

		t.Status("cloning lib/pq and installing prerequisites")
		latestTag, err := repeatGetLatestTag(
			ctx, t, "lib", "pq", libPQReleaseTagRegex)
		require.NoError(t, err)
		t.L().Printf("Latest lib/pq release is %s.", latestTag)
		t.L().Printf("Supported lib/pq release is %s.", libPQSupportedTag)

		installGolang(ctx, t, c, node)

		const (
			libPQRepo   = "github.com/lib/pq"
			libPQPath   = goPath + "/src/" + libPQRepo
			resultsDir  = "~/logs/report/libpq-results"
			resultsPath = resultsDir + "/report.xml"
		)

		// Remove any old lib/pq installations
		err = repeatRunE(
			ctx, t, c, node, "remove old lib/pq", fmt.Sprintf("rm -rf %s", libPQPath),
		)
		require.NoError(t, err)

		// Install go-junit-report to convert test results to .xml format we know
		// how to work with.
		err = repeatRunE(ctx, t, c, node, "install go-junit-report",
			fmt.Sprintf("GOPATH=%s go install github.com/jstemmer/go-junit-report@latest", goPath),
		)
		require.NoError(t, err)
		// It's safer to clean up dependencies this way than it is to give the cluster
		// wipe root access.
		defer func() {
			c.Run(ctx, option.WithNodes(c.All()), "go clean -modcache")
		}()

		err = repeatGitCloneE(
			ctx,
			t,
			c,
			fmt.Sprintf("https://%s.git", libPQRepo),
			libPQPath,
			libPQSupportedTag,
			node,
		)
		require.NoError(t, err)
		if err := c.RunE(ctx, option.WithNodes(node), fmt.Sprintf("mkdir -p %s", resultsDir)); err != nil {
			t.Fatal(err)
		}

		t.L().Printf("Running cockroach version %s, using blocklist %s, using ignorelist %s", version, "libPQBlocklist", "libPQIgnorelist")

		t.Status("running lib/pq test suite and collecting results")

		// List all the tests that start with Test or Example.
		testListRegex := "^(Test|Example)"
		result, err := c.RunWithDetailsSingleNode(
			ctx, t.L(),
			option.WithNodes(node),
			fmt.Sprintf(
				`cd %s && PGPORT={pgport:1} PGUSER=%s PGPASSWORD=%s PGSSLMODE=require PGDATABASE=postgres go test -list "%s"`,
				libPQPath, install.DefaultUser, install.DefaultPassword, testListRegex),
		)
		require.NoError(t, err)

		// Convert the output of go test -list into an list.
		tests := strings.Fields(result.Stdout)
		var allowedTests []string
		testListR, err := regexp.Compile(testListRegex)
		require.NoError(t, err)

		for _, testName := range tests {
			// Ignore tests that do not match the test regex pattern.
			if !testListR.MatchString(testName) {
				continue
			}
			// If the test is part of ignoreList, do not run the test.
			if _, ok := libPQIgnorelist[testName]; !ok {
				allowedTests = append(allowedTests, testName)
			}
		}

		allowedTestsRegExp := fmt.Sprintf(`"^(%s)$"`, strings.Join(allowedTests, "|"))

		// Ignore the error as there will be failing tests.
		_ = c.RunE(
			ctx,
			option.WithNodes(node),
			fmt.Sprintf("cd %s && PGPORT={pgport:1} PGUSER=%s PGPASSWORD=%s PGSSLMODE=require PGDATABASE=postgres go test -run %s -v 2>&1 | %s/bin/go-junit-report > %s",
				libPQPath, install.DefaultUser, install.DefaultPassword, allowedTestsRegExp, goPath, resultsPath),
		)

		parseAndSummarizeJavaORMTestsResults(
			ctx, t, c, node, "lib/pq" /* ormName */, []byte(resultsPath),
			"libPQBlocklist", libPQBlocklist, libPQIgnorelist, version, libPQSupportedTag,
		)
	}

	r.Add(registry.TestSpec{
		Name:             "lib/pq",
		Owner:            registry.OwnerSQLFoundations,
		Cluster:          r.MakeClusterSpec(1),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly, registry.Driver),
		Run:              runLibPQ,
	})
}
