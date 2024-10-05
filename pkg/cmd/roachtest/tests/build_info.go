// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
)

// RunBuildInfo is a test that sanity checks the build info.
func RunBuildInfo(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())

	var details serverpb.DetailsResponse
	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.Node(1))
	if err != nil {
		t.Fatal(err)
	}
	url := `https://` + adminUIAddrs[0] + `/_status/details/local`
	client := roachtestutil.DefaultHTTPClient(c, t.L())
	err = client.GetJSON(ctx, url, &details)
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
	c.Run(ctx, option.WithNodes(c.Node(1)), "sudo apt-get update")
	c.Run(ctx, option.WithNodes(c.Node(1)), "sudo apt-get -qqy install pax-utils")

	result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.Node(1)), "scanelf -qe cockroach")
	if err != nil {
		t.Fatalf("scanelf failed: %s", err)
	}
	output := strings.TrimSpace(result.Stdout)
	if len(output) > 0 {
		t.Fatalf("scanelf returned non-empty output (executable stack): %s", output)
	}
}
