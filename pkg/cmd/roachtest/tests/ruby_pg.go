// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

var rubyPGTestFailureRegex = regexp.MustCompile(`^rspec ./.*# .*`)
var testFailureFilenameRegexp = regexp.MustCompile("^rspec .*.rb.*([0-9]|]) # ")
var testSummaryRegexp = regexp.MustCompile("^([0-9]+) examples, [0-9]+ failures")

// WARNING: DO NOT MODIFY the name of the below constant/variable without approval from the docs team.
// This is used by docs automation to produce a list of supported versions for ORM's.
var rubyPGVersion = "v1.4.6"

// Embed the helper file, so we don't need to know where it is
// relative to the roachtest runner, just relative to this test.
// This way we can still find it if roachtest changes paths.
//
//go:embed ruby_pg_helpers.rb
var rubyPGHelpersFile string

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
		startOpts := option.NewStartOpts(sqlClientsInMemoryDB)
		startOpts.RoachprodOpts.SQLPort = config.DefaultSQLPort
		// TODO(darrylwong): ruby-pg is currently being updated to run on Ubuntu 22.04.
		// Once complete, fix up ruby_pg_helpers to accept a tls connection.
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(install.SecureOption(false)), c.All())

		version, err := fetchCockroachVersion(ctx, t.L(), c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, t, version, c, node[0]); err != nil {
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
			"install ruby 3.1.2",
			`mkdir -p ruby-install && \
        curl -fsSL https://github.com/postmodern/ruby-install/archive/v0.8.3.tar.gz | tar --strip-components=1 -C ruby-install -xz && \
        sudo rm -rf /usr/local/bin/* && \
        sudo make -C ruby-install install && \
        sudo ruby-install --system ruby 3.1.2 && \
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

		for original, replacement := range map[string]string{
			"CREATE FUNCTION errfunc()":    "DROP FUNCTION IF EXISTS errfunc(); CREATE FUNCTION errfunc()",
			"CREATE TEMP TABLE copytable":  "DROP TABLE IF EXISTS copytable; CREATE TEMP TABLE copytable",
			"CREATE TEMP TABLE copytable2": "DROP TABLE IF EXISTS copytable2; CREATE TEMP TABLE copytable2",
			"CREATE TABLE fmodtest":        "DROP TABLE IF EXISTS fmodtest; CREATE TABLE fmodtest",
			"CREATE TABLE ftablecoltest":   "DROP TABLE IF EXISTS ftablecoltest; CREATE TABLE ftablecoltest",
			"CREATE TABLE ftabletest":      "DROP TABLE IF EXISTS ftabletest; CREATE TABLE ftabletest",
			"CREATE TABLE students":        "DROP TABLE IF EXISTS students; CREATE TABLE students",
		} {
			if err := repeatRunE(
				ctx, t, c, node, "patch test to workaround flaky cleanup logic",
				fmt.Sprintf(`find /mnt/data1/ruby-pg/spec/pg/ -name "*_spec.rb" | xargs sed -i -e "s/%s/%s/g"`, original, replacement),
			); err != nil {
				t.Fatal(err)
			}
		}

		t.Status("installing bundler")
		if err := repeatRunE(
			ctx,
			t,
			c,
			node,
			"installing bundler",
			`cd /mnt/data1/ruby-pg/ && sudo gem install bundler:2.4.9`,
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
		err = c.PutString(ctx, rubyPGHelpersFile, "/mnt/data1/ruby-pg/spec/helpers.rb", 0755, c.All())
		require.NoError(t, err)

		t.Status("running ruby-pg test suite")
		// Note that this is expected to return an error, since the test suite
		// will fail. And it is safe to swallow it here.
		result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(node),
			`cd /mnt/data1/ruby-pg/ && bundle exec rake compile test`,
		)

		// Fatal for a roachprod or transient error. A roachprod error is when result.Err==nil.
		// Proceed for any other (command) errors
		if err != nil && (result.Err == nil || rperrors.IsTransient(err)) {
			t.Fatal(err)
		}

		rawResults := []byte(result.Stdout + result.Stderr)
		t.L().Printf("Test Results:\n%s", rawResults)

		// Find all the failed and errored tests.
		results := newORMTestsResults()

		scanner := bufio.NewScanner(bytes.NewReader(rawResults))
		totalTests := int64(0)
		for scanner.Scan() {
			line := scanner.Text()
			testSummaryMatch := testSummaryRegexp.FindStringSubmatch(line)
			if testSummaryMatch != nil {
				totalTests, err = strconv.ParseInt(testSummaryMatch[1], 10, 64)
				require.NoError(t, err)
				continue
			}

			match := rubyPGTestFailureRegex.FindStringSubmatch(line)
			if match == nil {
				continue
			}
			if len(match) != 1 {
				t.Fatalf("expected one match for test name, found: %d", len(match))
			}

			// Take the first test name.
			test := match[0]

			// This regex is used to get the name of the test.
			// The test name follows the file name and a hashtag.
			// ie. test.rb:99 # TEST NAME.
			strs := testFailureFilenameRegexp.Split(test, -1)
			if len(strs) != 2 {
				t.Fatalf("expected test output line to be split into two strings")
			}
			test = strs[1]

			issue, expectedFailure := rubyPGBlocklist[test]
			ignoredReason, expectedIgnored := rubyPGIgnorelist[test]
			switch {
			case expectedIgnored:
				results.results[test] = fmt.Sprintf("--- SKIP: %s due to %s (expected)", test, ignoredReason)
				results.ignoredCount++
			case expectedFailure:
				results.results[test] = fmt.Sprintf("--- FAIL: %s - %s (expected)",
					test, maybeAddGithubLink(issue),
				)
				results.failExpectedCount++
				results.currentFailures = append(results.currentFailures, test)
			case !expectedFailure:
				results.results[test] = fmt.Sprintf("--- FAIL: %s - %s (unexpected)",
					test, maybeAddGithubLink(issue),
				)
				results.failUnexpectedCount++
				results.currentFailures = append(results.currentFailures, test)
			}
			results.runTests[test] = struct{}{}
		}

		if totalTests == 0 {
			t.Fatalf("failed to find total number of tests run")
		}
		totalPasses := int(totalTests) - (results.failUnexpectedCount + results.failExpectedCount)
		results.passUnexpectedCount = len(rubyPGBlocklist) - results.failExpectedCount
		results.passExpectedCount = totalPasses - results.passUnexpectedCount

		const blocklistName = "rubyPGBlocklist"
		results.summarizeAll(t, "ruby-pg", blocklistName, rubyPGBlocklist, version, rubyPGVersion)
	}

	r.Add(registry.TestSpec{
		Name:             "ruby-pg",
		Timeout:          1 * time.Hour,
		Owner:            registry.OwnerSQLFoundations,
		Cluster:          r.MakeClusterSpec(1),
		Leases:           registry.MetamorphicLeases,
		NativeLibs:       registry.LibGEOS,
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly, registry.Driver),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runRubyPGTest(ctx, t, c)
		},
	})
}
