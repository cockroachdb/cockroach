// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// teamcity-trigger launches a variety of nightly build jobs on TeamCity using
// its REST API. It is intended to be run from a meta-build on a schedule
// trigger.
//
// One might think that TeamCity would support scheduling the same build to run
// multiple times with different parameters, but alas. The feature request has
// been open for ten years: https://youtrack.jetbrains.com/issue/TW-6439
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/abourget/teamcity"
	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/cmd/cmdutil"
	"github.com/kisielk/gotool"
)

func main() {
	if len(os.Args) != 1 {
		fmt.Fprintf(os.Stderr, "usage: %s\n", os.Args[0])
		os.Exit(1)
	}

	branch := cmdutil.RequireEnv("TC_BUILD_BRANCH")
	serverURL := cmdutil.RequireEnv("TC_SERVER_URL")
	username := cmdutil.RequireEnv("TC_API_USER")
	password := cmdutil.RequireEnv("TC_API_PASSWORD")

	tcClient := teamcity.New(serverURL, username, password)
	runTC(func(buildID string, opts map[string]string) {
		build, err := tcClient.QueueBuild(buildID, branch, opts)
		if err != nil {
			log.Fatalf("failed to create teamcity build (buildID=%s, branch=%s, opts=%+v): %s",
				build, branch, opts, err)
		}
		log.Printf("created teamcity build (buildID=%s, branch=%s, opts=%+v): %s",
			buildID, branch, opts, build)
	})
}

func getBaseImportPath() string {
	if bazel.BuiltWithBazel() {
		return "./"
	}
	return "github.com/cockroachdb/cockroach/pkg/"
}

func runTC(queueBuild func(string, map[string]string)) {
	buildID := "Cockroach_Nightlies_Stress"
	if bazel.BuiltWithBazel() {
		buildID = "Cockroach_Nightlies_StressBazel"
	}
	baseImportPath := getBaseImportPath()
	importPaths := gotool.ImportPaths([]string{baseImportPath + "..."})

	// Queue stress builds. One per configuration per package.
	for _, importPath := range importPaths {
		// By default, run each package for up to 100 iterations.
		maxRuns := 100

		// By default, run each package for up to 1h.
		maxTime := 1 * time.Hour

		// By default, fail the stress run on the first test failure.
		maxFails := 1

		// By default, a single test times out after 40 minutes.
		// NOTE: This is used only for the (now deprecated) non-Bazel
		// stress job. Bazel test timeouts are handled at the test
		// target level (i.e. in BUILD.bazel).
		testTimeout := 40 * time.Minute

		// The stress program by default runs as many instances in parallel as there
		// are CPUs. Each instance itself can run tests in parallel. The amount of
		// parallelism needs to be reduced, or we can run into OOM issues,
		// especially for race builds and/or logic tests (see
		// https://github.com/cockroachdb/cockroach/pull/10966).
		//
		// We limit both the stress program parallelism and the go test parallelism
		// to 4 for non-race builds and 2 for race builds. For logic tests, we
		// halve these values.
		parallelism := 4

		opts := map[string]string{
			"env.PKG": importPath,
		}

		// Conditionally override settings.
		switch importPath {
		case baseImportPath + "kv/kvnemesis":
			// Disable -maxruns for kvnemesis. Run for the full 1h.
			maxRuns = 0
			if bazel.BuiltWithBazel() {
				opts["env.EXTRA_BAZEL_FLAGS"] = "--test_env COCKROACH_KVNEMESIS_STEPS=1000"
			} else {
				opts["env.COCKROACH_KVNEMESIS_STEPS"] = "1000"
			}
		case baseImportPath + "sql/logictest", baseImportPath + "kv/kvserver":
			// Stress heavy with reduced parallelism (to avoid overloading the
			// machine, see https://github.com/cockroachdb/cockroach/pull/10966).
			parallelism /= 2
			// Increase test timeout to compensate.
			testTimeout = 2 * time.Hour
			maxTime = 3 * time.Hour
		}

		if bazel.BuiltWithBazel() {
			// NB: This is what will eventually be passed to Bazel as the --test_timeout.
			// `stress` will run for maxTime, so we give it an extra minute to clean up.
			opts["env.TESTTIMEOUTSECS"] = fmt.Sprintf("%.0f", (maxTime + time.Minute).Seconds())
		} else {
			opts["env.TESTTIMEOUT"] = testTimeout.String()
		}

		// Run non-race build.
		if bazel.BuiltWithBazel() {
			bazelFlags, ok := opts["env.EXTRA_BAZEL_FLAGS"]
			if ok {
				opts["env.EXTRA_BAZEL_FLAGS"] = fmt.Sprintf("%s --test_sharding_strategy=disabled --jobs %d", bazelFlags, parallelism)
			} else {
				opts["env.EXTRA_BAZEL_FLAGS"] = fmt.Sprintf("--test_sharding_strategy=disabled --jobs %d", parallelism)
			}
		} else {
			opts["env.GOFLAGS"] = fmt.Sprintf("-parallel=%d", parallelism)
		}
		opts["env.STRESSFLAGS"] = fmt.Sprintf("-maxruns %d -maxtime %s -maxfails %d -p %d",
			maxRuns, maxTime, maxFails, parallelism)
		queueBuild(buildID, opts)

		// Run non-race build with deadlock detection.
		opts["env.TAGS"] = "deadlock"
		queueBuild(buildID, opts)
		delete(opts, "env.TAGS")

		// Run race build. With run with -p 1 to avoid overloading the machine.
		noParallelism := 1
		if bazel.BuiltWithBazel() {
			extraBazelFlags := opts["env.EXTRA_BAZEL_FLAGS"]
			// NB: Normally we'd just use `--config race`, but that implies a
			// `--test_timeout` that overrides the one we manually specify.
			opts["env.EXTRA_BAZEL_FLAGS"] = fmt.Sprintf("%s --@io_bazel_rules_go//go/config:race --test_env=GORACE=halt_on_error=1", extraBazelFlags)
		} else {
			opts["env.GOFLAGS"] = fmt.Sprintf("-race -parallel=%d", parallelism)
		}
		opts["env.STRESSFLAGS"] = fmt.Sprintf("-maxruns %d -maxtime %s -maxfails %d -p %d",
			maxRuns, maxTime, maxFails, noParallelism)
		queueBuild(buildID, opts)
	}
}
