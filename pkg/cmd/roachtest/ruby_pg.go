// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var rubyPGTestFailureRegex = regexp.MustCompile(`^rspec ./.*# .*`)
var supportedRailsVersionRubyPG = "6.1"
var rubyPGVersion = "v1.2.3"

// This test runs Ruby PG's full test suite against a single cockroach node.
func registerRubyPG(r *testRegistry) {
	runRubyPGTest := func(
		ctx context.Context,
		t *test,
		c *cluster,
	) {
		if c.isLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		c.Put(ctx, cockroach, "./cockroach", c.All())
		if err := c.PutLibraries(ctx, "./lib"); err != nil {
			t.Fatal(err)
		}
		c.Start(ctx, t, c.All())

		version, err := fetchCockroachVersion(ctx, c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, version, c, node[0]); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning rails and installing prerequisites")

		c.l.Printf("Supported rails release is %s.", supportedRailsVersionRubyPG)
		c.l.Printf("Supported ruby-pg version is %s.", rubyPGVersion)

		if err := repeatRunE(
			ctx, c, node, "update apt-get", `sudo apt-get -qq update`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			c,
			node,
			"install dependencies",
			`sudo apt-get -qq install ruby-full ruby-dev rubygems build-essential zlib1g-dev libpq-dev libsqlite3-dev`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
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
			ctx, c, node, "remove old ruby-pg", `sudo rm -rf /mnt/data1/ruby-pg`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t.l,
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
			c,
			node,
			"installing gems",
			fmt.Sprintf(
				`cd /mnt/data1/ruby-pg/ && `+
					`RAILS_VERSION=%s sudo bundle install`, supportedRailsVersionRubyPG),
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "remove old ruby-pg helpers.rb", `sudo rm /mnt/data1/ruby-pg/spec/helpers.rb`,
		); err != nil {
			t.Fatal(err)
		}

		// Write the cockroach config into the test suite to use.
		if err := repeatRunE(
			ctx, c, node, "configuring tests to use cockroach",
			"curl https://gist.githubusercontent.com/RichardJCai/60fa14ca95f19595e9974946cea99c07/raw/f4dc86a8c44eca4a9814e8ff793e862d3068f386/gistfile1.txt  --output /mnt/data1/ruby-pg/spec/helpers.rb",
		); err != nil {
			t.Fatal(err)
		}

		//_ = c.RunE(ctx, node, `cd /mnt/data1/ruby-pg/ && sudo rake`)

		t.Status("running ruby-pg test suite")
		// Note that this is expected to return an error, since the test suite
		// will fail. And it is safe to swallow it here.
		rawResults, _ := c.RunWithBuffer(ctx, t.l, node,
			`cd /mnt/data1/ruby-pg/ && `+
				`cd /mnt/data1/ruby-pg/ && sudo rake`,
		)

		c.l.Printf("Test Results:\n%s", rawResults)

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
				log.Warningf(ctx, "expected one match for test name, found: %d", len(match))
				for _, m := range match {
					log.Warningf(ctx, "%s\n", m)
				}
			}

			// Take the first test name.
			test := match[0]

			// This regex is used to get the name of the test.
			// The test name follows the file name and a hashtag.
			// ie. test.rb:99 # TEST NAME.
			strs := regexp.MustCompile("^rspec .*.rb.*([0-9]|]) # ").Split(test, -1)
			if len(strs) != 2 {
				log.Warningf(ctx, "length not 2")
				log.Warningf(ctx, test)
				continue
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

	r.Add(testSpec{
		MinVersion: "v20.1.0",
		Name:       "ruby-pg",
		Owner:      OwnerSQLExperience,
		Cluster:    makeClusterSpec(1),
		Tags:       []string{`default`, `orm`},
		Run: func(ctx context.Context, t *test, c *cluster) {
			runRubyPGTest(ctx, t, c)
		},
	})
}
