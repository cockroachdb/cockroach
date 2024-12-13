// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

var activerecordResultRegex = regexp.MustCompile(`^(?P<test>[^\s]+#[^\s]+) = (?P<timing>\d+\.\d+ s) = (?P<result>.)$`)
var railsReleaseTagRegex = regexp.MustCompile(`^v(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)\.?(?P<subpoint>\d*)$`)

// WARNING: DO NOT MODIFY the name of the below constant/variable without approval from the docs team.
// This is used by docs automation to produce a list of supported versions for ORM's.
var supportedRailsVersion = "7.2.1"
var activerecordAdapterVersion = "v7.2.0"

// This test runs activerecord's full test suite against a single cockroach node.

func registerActiveRecord(r registry.Registry) {
	runActiveRecord := func(
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
		// Activerecord uses root user with ssl disabled.
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(install.SecureOption(false)), c.All())

		version, err := fetchCockroachVersion(ctx, t.L(), c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, t, version, c, node[0]); err != nil {
			t.Fatal(err)
		}

		t.Status("creating database used by tests")
		db, err := c.ConnE(ctx, t.L(), node[0])
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		if _, err := db.ExecContext(
			ctx, `CREATE DATABASE activerecord_unittest;`,
		); err != nil {
			t.Fatal(err)
		}

		if _, err := db.ExecContext(
			ctx, `CREATE DATABASE activerecord_unittest2;`,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("cloning rails and installing prerequisites")
		// Report the latest tag, but do not use it. The newest versions produces output that breaks our xml parser,
		// and we want to pin to the working version for now.
		latestTag, err := repeatGetLatestTag(
			ctx, t, "rails", "rails", railsReleaseTagRegex,
		)
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("Latest rails release is %s.", latestTag)
		t.L().Printf("Supported rails release is %s.", supportedRailsVersion)
		t.L().Printf("Supported adapter version is %s.", activerecordAdapterVersion)

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
			"install ruby 3.3",
			`mkdir -p ruby-install && \
        curl -fsSL https://github.com/postmodern/ruby-install/archive/v0.9.1.tar.gz | tar --strip-components=1 -C ruby-install -xz && \
        sudo make -C ruby-install install && \
        sudo ruby-install --system ruby 3.3.6 && \
        sudo gem update --system`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, t, c, node, "remove old activerecord adapter", `rm -rf /mnt/data1/activerecord-cockroachdb-adapter`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t,
			c,
			"https://github.com/cockroachdb/activerecord-cockroachdb-adapter.git",
			"/mnt/data1/activerecord-cockroachdb-adapter",
			activerecordAdapterVersion,
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
			`cd /mnt/data1/activerecord-cockroachdb-adapter/ && sudo gem install bundler:2.5.23`,
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
			fmt.Sprintf(
				`cd /mnt/data1/activerecord-cockroachdb-adapter/ && `+
					`sudo RAILS_VERSION=%s bundle install`, supportedRailsVersion),
		); err != nil {
			t.Fatal(err)
		}

		blocklistName, ignorelistName := "activeRecordBlocklist", "activeRecordIgnoreList"
		status := fmt.Sprintf("Running cockroach version %s, using blocklist %s, using ignorelist %s",
			version, blocklistName, ignorelistName)
		t.L().Printf("%s", status)

		t.Status("running activerecord test suite")

		result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(node),
			fmt.Sprintf(
				`cd /mnt/data1/activerecord-cockroachdb-adapter/ && `+
					`sudo RAILS_VERSION=%s RUBYOPT="-W0" TESTOPTS="-v" bundle exec rake test`, supportedRailsVersion),
		)

		// Fatal for a roachprod or transient error. A roachprod error is when result.Err==nil.
		// Proceed for any other (command) errors
		if err != nil && (result.Err == nil || rperrors.IsTransient(err)) {
			t.Fatal(err)
		}

		// Result error contains stdout, stderr, and any error content returned by exec package.
		rawResults := []byte(result.Stdout + result.Stderr)
		t.L().Printf("Test Results:\n%s", rawResults)

		// Find all the failed and errored tests.
		results := newORMTestsResults()

		scanner := bufio.NewScanner(bytes.NewReader(rawResults))
		for scanner.Scan() {
			match := activerecordResultRegex.FindStringSubmatch(scanner.Text())
			if match == nil {
				continue
			}
			test, result := match[1], match[3]
			pass := result == "."
			skipped := result == "S"
			results.allTests = append(results.allTests, test)

			ignoredIssue, expectedIgnored := activeRecordIgnoreList[test]
			issue, expectedFailure := activeRecordBlocklist[test]
			switch {
			case expectedIgnored:
				results.results[test] = fmt.Sprintf("--- SKIP: %s due to %s (expected)", test, ignoredIssue)
				results.ignoredCount++
			case skipped && expectedFailure:
				results.results[test] = fmt.Sprintf("--- SKIP: %s (unexpected)", test)
				results.unexpectedSkipCount++
			case skipped:
				results.results[test] = fmt.Sprintf("--- SKIP: %s (expected)", test)
				results.skipCount++
			case pass && !expectedFailure:
				results.results[test] = fmt.Sprintf("--- PASS: %s (expected)", test)
				results.passExpectedCount++
			case pass && expectedFailure:
				results.results[test] = fmt.Sprintf("--- PASS: %s - %s (unexpected)",
					test, maybeAddGithubLink(issue),
				)
				results.passUnexpectedCount++
			case !pass && expectedFailure:
				results.results[test] = fmt.Sprintf("--- FAIL: %s - %s (expected)",
					test, maybeAddGithubLink(issue),
				)
				results.failExpectedCount++
				results.currentFailures = append(results.currentFailures, test)
			case !pass && !expectedFailure:
				results.results[test] = fmt.Sprintf("--- FAIL: %s (unexpected)", test)
				results.failUnexpectedCount++
				results.currentFailures = append(results.currentFailures, test)
			}
			results.runTests[test] = struct{}{}
		}

		results.summarizeAll(
			t, "activerecord" /* ormName */, blocklistName, activeRecordBlocklist, version, supportedRailsVersion,
		)
	}

	r.Add(registry.TestSpec{
		Name:             "activerecord",
		Owner:            registry.OwnerSQLFoundations,
		Timeout:          5 * time.Hour,
		Cluster:          r.MakeClusterSpec(1),
		NativeLibs:       registry.LibGEOS,
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly, registry.ORM),
		Run:              runActiveRecord,
		Leases:           registry.MetamorphicLeases,
	})
}
