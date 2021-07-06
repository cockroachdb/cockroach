// Copyright 2018 The Cockroach Authors.
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
	"context"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
)

// RunBuildInfo is a test that sanity checks the build info.
func RunBuildInfo(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx)

	var details serverpb.DetailsResponse
	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, c.Node(1))
	if err != nil {
		t.Fatal(err)
	}
	url := `http://` + adminUIAddrs[0] + `/_status/details/local`
	err = httputil.GetJSON(http.Client{}, url, &details)
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

// RunBuildAnalyze performs static analysis on the built binary to
// ensure it's built as expected.
func RunBuildAnalyze(ctx context.Context, t test.Test, c cluster.Cluster) {

	if c.IsLocal() {
		// This test is linux-specific and needs to be able to install apt
		// packages, so only run it on dedicated remote VMs.
		t.Skip("local execution not supported")
	}

	c.Put(ctx, t.Cockroach(), "./cockroach")

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

	output, err := c.RunWithBuffer(ctx, t.L(), c.Node(1), "scanelf -qe cockroach")
	if err != nil {
		t.Fatalf("scanelf failed: %s", err)
	}
	if len(output) > 0 {
		t.Fatalf("scanelf returned non-empty output (executable stack): %s", string(output))
	}
}
