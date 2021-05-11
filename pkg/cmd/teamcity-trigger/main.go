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

const baseImportPath = "github.com/cockroachdb/cockroach/pkg/"

var importPaths = gotool.ImportPaths([]string{baseImportPath + "..."})

func runTC(queueBuild func(string, map[string]string)) {
	// Queue stress builds. One per configuration per package.
	for _, importPath := range importPaths {
		// By default, run each package for up to 100 iterations.
		maxRuns := 100

		// By default, run each package for up to 1h.
		maxTime := 1 * time.Hour

		// By default, fail the stress run on the first test failure.
		maxFails := 1

		// By default, a single test times out after 40 minutes.
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
			opts["env.COCKROACH_KVNEMESIS_STEPS"] = "10000"
		case baseImportPath + "sql/logictest":
			// Stress logic tests with reduced parallelism (to avoid overloading the
			// machine, see https://github.com/cockroachdb/cockroach/pull/10966).
			parallelism /= 2
			// Increase logic test timeout.
			testTimeout = 2 * time.Hour
			maxTime = 3 * time.Hour
		}

		opts["env.TESTTIMEOUT"] = testTimeout.String()

		// Run non-race build.
		opts["env.GOFLAGS"] = fmt.Sprintf("-parallel=%d", parallelism)
		opts["env.STRESSFLAGS"] = fmt.Sprintf("-maxruns %d -maxtime %s -maxfails %d -p %d",
			maxRuns, maxTime, maxFails, parallelism)
		queueBuild("Cockroach_Nightlies_Stress", opts)

		// Run race build. With run with -p 1 to avoid overloading the machine.
		noParallelism := 1
		opts["env.GOFLAGS"] = fmt.Sprintf("-race -parallel=%d", parallelism)
		opts["env.STRESSFLAGS"] = fmt.Sprintf("-maxruns %d -maxtime %s -maxfails %d -p %d",
			maxRuns, maxTime, maxFails, noParallelism)
		queueBuild("Cockroach_Nightlies_Stress", opts)
	}
}
