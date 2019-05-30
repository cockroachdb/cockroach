// Copyright 2018 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"context"
	"net/http"
	"os/exec"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
)

func runBuildInfo(ctx context.Context, t *test, c *cluster) {
	c.Put(ctx, cockroach, "./cockroach")
	c.Start(ctx, t)

	var details serverpb.DetailsResponse
	url := `http://` + c.ExternalAdminUIAddr(ctx, c.Node(1))[0] + `/_status/details/local`
	err := httputil.GetJSON(http.Client{}, url, &details)
	if err != nil {
		t.Fatal(err)
	}

	bi := details.BuildInfo
	testData := map[string]string{
		"go_version": bi.GoVersion,
		"tag":        bi.Tag,
		"time":       bi.Time,
		"revision":   bi.Revision,
	}
	for key, val := range testData {
		if val == "" {
			t.Fatalf("build info not set for \"%s\"", key)
		}
	}
}

// runBuildAnalyze performs static analysis on the built binary to
// ensure it's built as expected.
func runBuildAnalyze(ctx context.Context, t *test, c *cluster) {

	if c.isLocal() {
		// This test is linux-specific and needs to be able to install apt
		// packages, so only run it on dedicated remote VMs.
		t.spec.Skip = "local execution not supported"
		return
	}

	c.Put(ctx, cockroach, "./cockroach")

	// 1. Check for executable stack.
	//
	// Executable stack memory is a security risk (not a vulnerability
	// in itself, but makes it easier to exploit other vulnerabilities).
	// Whether or not the stack is executable is a property of the built
	// executable, subject to some subtle heuristics. This test ensures
	// that we're not hitting anything that causes our stacks to become
	// executable.
	//
	// References:
	// https://www.airs.com/blog/archives/518
	// https://wiki.ubuntu.com/SecurityTeam/Roadmap/ExecutableStacks
	// https://github.com/cockroachdb/cockroach/issues/37885

	// There are several ways to do this analysis: `readelf -lW`,
	// `scanelf -qe`, and `execstack -q`. `readelf` is part of binutils,
	// so it's relatively ubiquitous, but we don't have it in the
	// roachtest environment. Since we don't have anything preinstalled
	// we can use, choose `scanelf` for being the simplest to use (empty
	// output indicates everything's fine, non-empty means something
	// bad).
	c.Run(ctx, c.Node(1), "sudo apt-get update")
	c.Run(ctx, c.Node(1), "sudo apt-get -qqy install pax-utils")

	cmd := exec.CommandContext(ctx, roachprod, "run", c.makeNodes(c.Node(1)), "scanelf -qe cockroach")
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("scanelf failed: %s", err)
	}
	if len(output) > 0 {
		t.Fatalf("scanelf returned non-empty output (executable stack): %s", string(output))
	}
}
