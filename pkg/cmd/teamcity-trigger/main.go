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
	"os/exec"
	"strings"
	"time"

	"github.com/abourget/teamcity"
	"github.com/cockroachdb/cockroach/pkg/cmd/cmdutil"
)

type timeoutSpec struct {
	// Set `recursive` to also apply this timeout to all subpackages. Remember that timeouts should
	// be just over what is needed for the tests to pass to be alerted of any future performance regressions
	// so take care when setting this option and only set it if you really need what it does.
	recursive bool
	// maxTime will be used for the following:
	// 1) Passed as a stress flag.
	// 2) Bazel timeout will be set to `maxTime` + 1 minute to give `stress` an extra minute to clean up.
	// 3) Go test process timeout will be set to `maxTime` + 55 seconds to let it timeout before Bazel kills it and allow
	// us to get stacktraces.
	maxTime time.Duration
}

const (
	buildID = "Cockroach_Nightlies_StressBazel"
)

var (
	customTimeouts = map[string]timeoutSpec{
		"//pkg/sql/logictest": {
			maxTime: 3 * time.Hour,
		},
		"//pkg/kv/kvserver": {
			maxTime: 3 * time.Hour,
		},
		"//pkg/ccl/backupccl": {
			maxTime: 2 * time.Hour,
		},
		"//pkg/ccl/logictestccl/tests/3node-tenant": {
			maxTime: 2 * time.Hour,
		},
	}
)

func getMaxTime(testTarget string) time.Duration {
	pathWithoutTarget := strings.Split(testTarget, ":")[0]
	// Case 1: importPath is explicitly covered by customTimeouts.
	if spec, ok := customTimeouts[pathWithoutTarget]; ok {
		return spec.maxTime
	}
	// Case 2: importPath is implicitly covered by customTimeouts.
	for prefixPath, spec := range customTimeouts {
		if spec.recursive && strings.HasPrefix(pathWithoutTarget, prefixPath) {
			return spec.maxTime
		}
	}
	// Case 3: default timeout is 1 hour.
	return 1 * time.Hour
}

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
	count := 0
	runTC(func(buildID string, opts map[string]string) {
		count++
		build, err := tcClient.QueueBuild(buildID, branch, opts)
		if err != nil {
			log.Fatalf("failed to create teamcity build (buildID=%s, branch=%s, opts=%+v): %s",
				build, branch, opts, err)
		}
		log.Printf("created teamcity build (buildID=%s, branch=%s, opts=%+v): %s",
			buildID, branch, opts, build)
	})
	if count == 0 {
		log.Fatal("no builds were created")
	}
}

func runTC(queueBuild func(string, map[string]string)) {
	// queueBuildThenWait sets a limit on the rate at which we trigger TC stress
	// build config to avoid overloading TC [see DEVINF-834].
	currentTriggerCount := 0
	queueBuildThenWait := func(buildID string, opts map[string]string) {
		if currentTriggerCount == 20 {
			currentTriggerCount = 0
			time.Sleep(time.Minute * 10)
		}
		currentTriggerCount += 1
		queueBuild(buildID, opts)
	}
	targets, err := exec.Command("bazel", "query", "kind(go_test, //pkg/...)", "--output=label").Output()
	if err != nil {
		log.Fatal(err)
	}
	// Queue stress builds. One per configuration per test target.
	for _, testTarget := range strings.Split(string(targets), "\n") {
		testTarget = strings.TrimSpace(testTarget)
		if testTarget == "" {
			continue
		}
		// By default, run each package for up to 100 iterations.
		maxRuns := 100
		maxTime := getMaxTime(testTarget)
		// By default, fail the stress run on the first test failure.
		maxFails := 1

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
			"env.TARGET": testTarget,
		}

		// Conditionally override settings.
		if testTarget == "//pkg/kv/kvnemesis:kvnemesis_test" {
			// Disable -maxruns for kvnemesis. Run for the full 1h.
			maxRuns = 0
			opts["env.EXTRA_BAZEL_FLAGS"] = "--test_env COCKROACH_KVNEMESIS_STEPS=1000"
		}

		if testTarget == "//pkg/sql/logictest:logictest_test" || testTarget == "//pkg/kv/kvserver:kvserver_test" {
			// Stress heavy with reduced parallelism (to avoid overloading the
			// machine, see https://github.com/cockroachdb/cockroach/pull/10966).
			parallelism /= 2
		}

		// NB: This is what will eventually be passed to Bazel as the --test_timeout.
		// `stress` will run for maxTime, so we give it an extra minute to clean up.
		opts["env.TESTTIMEOUTSECS"] = fmt.Sprintf("%.0f", (maxTime + time.Minute).Seconds())

		// Run non-race build.
		bazelFlags, ok := opts["env.EXTRA_BAZEL_FLAGS"]
		if ok {
			opts["env.EXTRA_BAZEL_FLAGS"] = fmt.Sprintf("%s --test_sharding_strategy=disabled --jobs %d", bazelFlags, parallelism)
		} else {
			opts["env.EXTRA_BAZEL_FLAGS"] = fmt.Sprintf("--test_sharding_strategy=disabled --jobs %d", parallelism)
		}

		opts["env.STRESSFLAGS"] = fmt.Sprintf("-maxruns %d -maxtime %s -maxfails %d -p %d",
			maxRuns, maxTime, maxFails, parallelism)
		queueBuildThenWait(buildID, opts)

		// Run non-race build with deadlock detection.
		opts["env.TAGS"] = "deadlock"
		queueBuildThenWait(buildID, opts)
		delete(opts, "env.TAGS")

		// Run race build. With run with -p 1 to avoid overloading the machine.
		noParallelism := 1
		extraBazelFlags := opts["env.EXTRA_BAZEL_FLAGS"]
		// NB: Normally we'd just use `--config race`, but that implies a
		// `--test_timeout` that overrides the one we manually specify.
		opts["env.EXTRA_BAZEL_FLAGS"] = fmt.Sprintf("%s --@io_bazel_rules_go//go/config:race --test_env=GORACE=halt_on_error=1", extraBazelFlags)
		opts["env.STRESSFLAGS"] = fmt.Sprintf("-maxruns %d -maxtime %s -maxfails %d -p %d",
			maxRuns, maxTime, maxFails, noParallelism)
		opts["env.TAGS"] = "race"
		queueBuildThenWait(buildID, opts)
		delete(opts, "env.TAGS")
	}
}
