// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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

func runTC(queueBuild func(string, map[string]string)) {
	importPaths := gotool.ImportPaths([]string{"github.com/cockroachdb/cockroach/pkg/..."})

	// Queue stress builds. One per configuration per package.
	for _, opts := range []map[string]string{
		{}, // uninstrumented
		// The race detector is CPU intensive, so we want to run less processes in
		// parallel. (Stress, by default, will run one process per CPU.)
		//
		// TODO(benesch): avoid assuming that TeamCity agents have eight CPUs.
		{"env.GOFLAGS": "-race", "env.STRESSFLAGS": "-p 4"},
	} {
		for _, importPath := range importPaths {
			opts["env.PKG"] = importPath
			queueBuild("Cockroach_Nightlies_Stress", opts)
		}
	}
}
