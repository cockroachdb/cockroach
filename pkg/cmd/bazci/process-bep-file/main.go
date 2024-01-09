// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build bazel
// +build bazel

// process-bep-file is a binary to parse test results from a "build event
// protocol" binary file, as constructed by
// `bazel ... --build_event_binary_file`. We use this data to report issues to
// GitHub.

package main

import (
	"context"
	"encoding/xml"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/build/engflow"
	bazelutil "github.com/cockroachdb/cockroach/pkg/build/util"
	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost"
	"github.com/cockroachdb/cockroach/pkg/cmd/internal/issues"
)

var (
	branch          = flag.String("branch", "", "currently checked out git branch")
	eventStreamFile = flag.String("eventsfile", "", "eventstream file produced by bazel build --build_event_binary_file")

	invocationId  = flag.String("invocation", "", "UUID of the invocation")
	serverUrl     = flag.String("serverurl", "https://tanzanite.cluster.engflow.com/", "URL of the EngFlow cluster")
	tlsClientCert = flag.String("cert", "", "TLS client certificate for accessing EngFlow, probably a .crt file")
	tlsClientKey  = flag.String("key", "", "TLS client key for accessing EngFlow")

	extraParams = flag.String("extra", "", "comma-separated list of keys to mark as 'true' in the produced GitHub issue")

	githubApiToken = os.Getenv("GITHUB_API_TOKEN")
)

func getSha() (string, error) {
	cmd := exec.Command("git", "rev-parse", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func postOptions(res *engflow.TestResultWithXml, sha string) *issues.Options {
	return &issues.Options{
		Token:  githubApiToken,
		Org:    "cockroachdb",
		Repo:   "cockroach",
		SHA:    sha,
		Branch: *branch,
		EngFlowOptions: &issues.EngFlowOptions{
			Attempt:      int(res.Attempt),
			InvocationID: *invocationId,
			Label:        res.Label,
			Run:          int(res.Run),
			Shard:        int(res.Shard),
			ServerURL:    *serverUrl,
		},
		GetBinaryVersion: build.BinaryVersion,
	}

}

func failurePoster(res *engflow.TestResultWithXml, sha string) githubpost.FailurePoster {
	postOpts := postOptions(res, sha)
	formatter := func(ctx context.Context, failure githubpost.Failure) (issues.IssueFormatter, issues.PostRequest) {
		fmter, req := githubpost.DefaultFormatter(ctx, failure)
		// We don't want an artifacts link: there are none on EngFlow.
		req.Artifacts = ""
		if req.ExtraParams == nil {
			req.ExtraParams = make(map[string]string)
		}
		if res.Run != 0 {
			req.ExtraParams["run"] = fmt.Sprintf("%d", res.Run)
		}
		if res.Shard != 0 {
			req.ExtraParams["shard"] = fmt.Sprintf("%d", res.Shard)
		}
		if res.Attempt != 0 {
			req.ExtraParams["attempt"] = fmt.Sprintf("%d", res.Attempt)
		}
		if *extraParams != "" {
			for _, key := range strings.Split(*extraParams, ",") {
				req.ExtraParams[key] = "true"
			}
		}
		return fmter, req
	}
	return func(ctx context.Context, failure githubpost.Failure) error {
		fmter, req := formatter(ctx, failure)
		return issues.Post(ctx, log.Default(), fmter, req, postOpts)
	}
}

func process() error {
	ctx := context.Background()
	sha, err := getSha()
	if err != nil {
		return err
	}

	eventStreamF, err := os.Open(*eventStreamFile)
	if err != nil {
		return err
	}
	defer func() { _ = eventStreamF.Close() }()
	invocation, err := engflow.LoadTestResults(eventStreamF, *tlsClientCert, *tlsClientKey)
	if err != nil {
		return err
	}

	if githubApiToken == "" {
		fmt.Printf("no GITHUB_API_TOKEN; skipping reporting to GitHub")
		return nil
	}

	fullTestResults := invocation.TestResults

	for _, results := range fullTestResults {
		// seenFailedTests lists all the failed top-level (parent) tests
		// that we have seen in this test package. If a test.xml doesn't
		// introduce any new failed tests, we don't file a GitHub issue.
		seenFailedTests := make(map[string]struct{})
		for _, res := range results {
			var seenNew bool
			if res.Err != nil {
				fmt.Printf("got error downloading test XML for result %+v; got error %+v", res, res.Err)
				continue
			}
			var testXml bazelutil.TestSuites
			if err := xml.Unmarshal([]byte(res.TestXml), &testXml); err != nil {
				fmt.Printf("could not parse test.xml: got error %+v", err)
				continue
			}
			for _, suite := range testXml.Suites {
				for _, testCase := range suite.TestCases {
					if testCase.Failure == nil && testCase.Error == nil {
						// Nothing to report.
						continue
					}
					testName := testCase.Name
					split := strings.SplitN(testName, "/", 2)
					if len(split) == 2 {
						// We want the parent test.
						testName = split[0]
					}
					if _, ok := seenFailedTests[testName]; !ok {
						seenFailedTests[testName] = struct{}{}
						seenNew = true
					}
				}
			}
			if seenNew {
				if err := githubpost.PostFromTestXMLWithFailurePoster(
					ctx, failurePoster(res, sha), testXml); err != nil {
					fmt.Printf("could not post to GitHub: got error %+v", err)
				}
			}
		}
	}

	return nil
}

func main() {
	flag.Parse()
	if *branch == "" {
		fmt.Println("must provide -branch")
		os.Exit(1)
	}
	if *eventStreamFile == "" {
		fmt.Println("must provide -eventsfile")
		os.Exit(1)
	}
	if *invocationId == "" {
		fmt.Println("must provide -invocation")
		os.Exit(1)
	}
	if *serverUrl == "" {
		fmt.Println("must provide -serverurl")
		os.Exit(1)
	}
	if *tlsClientCert == "" {
		fmt.Println("must provide -cert")
		os.Exit(1)
	}
	if *tlsClientKey == "" {
		fmt.Println("must provide -key")
		os.Exit(1)
	}
	if err := process(); err != nil {
		fmt.Printf("ERROR: %+v", err)
		os.Exit(1)
	}
}
