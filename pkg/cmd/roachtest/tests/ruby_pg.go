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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

var rubyPGTestFailureRegex = regexp.MustCompile(`^rspec ./.*# .*`)
var rubyPGVersion = "v1.2.3"

// This test runs Ruby PG's full test suite against a single cockroach node.
func registerRubyPG(r registry.Registry) {
	runRubyPGTest := func(
		ctx context.Context,
		t test.Test,
		c cluster.Cluster,
	) {
		if c.IsLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
		if err := c.PutLibraries(ctx, "./lib"); err != nil {
			t.Fatal(err)
		}
		c.Start(ctx, c.All())

		version, err := fetchCockroachVersion(ctx, c, node[0], nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, version, c, node[0], nil); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning rails and installing prerequisites")

		t.L().Printf("Supported ruby-pg version is %s.", rubyPGVersion)

		if err := repeatRunE(
			ctx, t, c, node, "update apt-get", `sudo apt-get -qq update`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"install dependencies",
			`sudo apt-get -qq install ruby-full ruby-dev rubygems build-essential zlib1g-dev libpq-dev libsqlite3-dev`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"install ruby 2.7",
			`mkdir -p ruby-install && \
        curl -fsSL https://github.com/postmodern/ruby-install/archive/v0.6.1.tar.gz | tar --strip-components=1 -C ruby-install -xz && \
        sudo make -C ruby-install install && \
        sudo ruby-install --system ruby 2.7.1 && \
        sudo gem update --system`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "remove old ruby-pg", `sudo rm -rf /mnt/data1/ruby-pg`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t,
			c,
			"https://github.com/ged/ruby-pg.git",
			"/mnt/data1/ruby-pg",
			rubyPGVersion,
			node,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("installing bundler")
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"installing bundler",
			`cd /mnt/data1/ruby-pg/ && sudo gem install bundler:2.1.4`,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("installing gems")
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"installing gems",
			`cd /mnt/data1/ruby-pg/ && sudo bundle install`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "remove old ruby-pg helpers.rb", `sudo rm /mnt/data1/ruby-pg/spec/helpers.rb`,
		); err != nil {
			t.Fatal(err)
		}

		// Write the cockroach config into the test suite to use.
		rubyPGHelpersFile := "./pkg/cmd/roachtest/tests/ruby_pg_helpers.rb"
		err = c.PutE(ctx, t.L(), rubyPGHelpersFile, "/mnt/data1/ruby-pg/spec/helpers.rb", c.All())
		require.NoError(t, err)

		t.Status("running ruby-pg test suite")
		// Note that this is expected to return an error, since the test suite
		// will fail. And it is safe to swallow it here.
		rawResults, _ := c.RunWithBuffer(ctx, t.L(), node,
			`cd /mnt/data1/ruby-pg/ && sudo rake`,
		)

		t.L().Printf("Test Results:\n%s", rawResults)

		// Find all the failed and errored tests.
		results := newORMTestsResults()
		blocklistName, expectedFailures, _, _ := rubyPGBlocklist.getLists(version)
		if expectedFailures == nil {
			t.Fatalf("No ruby-pg blocklist defined for cockroach version %s", version)
		}

		scanner := bufio.NewScanner(bytes.NewReader(rawResults))
		for scanner.Scan() {
			match := rubyPGTestFailureRegex.FindStringSubmatch(scanner.Text())
			if match == nil {
				continue
			}
			if len(match) != 1 {
				log.Fatalf(ctx, "expected one match for test name, found: %d", len(match))
			}

			// Take the first test name.
			test := match[0]

			// This regex is used to get the name of the test.
			// The test name follows the file name and a hashtag.
			// ie. test.rb:99 # TEST NAME.
			strs := regexp.MustCompile("^rspec .*.rb.*([0-9]|]) # ").Split(test, -1)
			if len(strs) != 2 {
				log.Fatalf(ctx, "expected test output line to be split into two strings")
			}
			test = strs[1]

			issue, expectedFailure := expectedFailures[test]
			switch {
			case expectedFailure:
				results.results[test] = fmt.Sprintf("--- FAIL: %s - %s (expected)",
					test, maybeAddGithubLink(issue),
				)
				results.failExpectedCount++
				results.currentFailures = append(results.currentFailures, test)
			case !expectedFailure:
				results.results[test] = fmt.Sprintf("--- PASS: %s - %s (unexpected)",
					test, maybeAddGithubLink(issue),
				)
			}
			results.runTests[test] = struct{}{}
		}

		results.summarizeAll(t, "ruby-pg", blocklistName, expectedFailures, version, rubyPGVersion)
	}

	r.Add(registry.TestSpec{
		Name:    "ruby-pg",
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(1),
		Tags:    []string{`default`, `orm`},
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runRubyPGTest(ctx, t, c)
		},
	})
}
