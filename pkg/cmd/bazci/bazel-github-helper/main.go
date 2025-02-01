// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build bazel

// bazel-github-helper is a binary to parse test results from a "build event
// protocol" binary file, as constructed by
// `bazel ... --build_event_binary_file`. We use this data to construct a JSON
// test report. We also optionally populate `GITHUB_STEP_SUMMARY` and can post
// GitHub issues if the `GITHUB_ACTIONS_BRANCH` variable is set to `master`
// or a release branch.

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build/engflow"
	bazelutil "github.com/cockroachdb/cockroach/pkg/build/util"
	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost"
)

var (
	eventStreamFile = flag.String("eventsfile", "", "eventstream file produced by bazel build --build_event_binary_file")
	serverName      = flag.String("servername", "mesolite", "name of the EngFlow cluster (mesolite, tanzanite)")
	tlsClientCert   = flag.String("cert", "", "TLS client certificate for accessing EngFlow, probably a .crt file")
	tlsClientKey    = flag.String("key", "", "TLS client key for accessing EngFlow")
	jsonOutFile     = flag.String("jsonout", "", "output JSON file")

	summaryFile = os.Getenv("GITHUB_STEP_SUMMARY")
)

type failedTestResult struct {
	label, name         string
	run, shard, attempt int32
	status              string
}

func process() error {
	eventStreamF, err := os.Open(*eventStreamFile)
	if err != nil {
		return err
	}
	defer func() { _ = eventStreamF.Close() }()
	invocation, err := engflow.LoadInvocationInfo(eventStreamF, *tlsClientCert, *tlsClientKey)
	if err != nil {
		return err
	}

	jsonReport, errs := engflow.ConstructJSONReport(invocation, *serverName)
	for _, err := range errs {
		fmt.Fprintf(os.Stderr, "ERROR: %+v\n", err)
	}

	if *jsonOutFile != "" {
		jsonData, err := json.Marshal(jsonReport)
		if err == nil {
			logError("write json report", os.WriteFile(*jsonOutFile, jsonData, 0644))
		} else {
			logError("marshal json", err)
		}
	} else {
		fmt.Fprintln(os.Stderr, "-jsonoutfile not passed, skipping constructing JSON test report")
	}

	if summaryFile != "" {
		summaryF, err := os.OpenFile(summaryFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err == nil {
			defer func() { _ = summaryF.Close() }()
			logError("dump summary", dumpSummary(summaryF, invocation))
		} else {
			logError("open summary file", err)
		}
	} else {
		fmt.Fprintln(os.Stderr, "GITHUB_STEP_SUMMARY not passed, skipping populating GitHub actions test report")
	}

	branch := os.Getenv("GITHUB_ACTIONS_BRANCH")
	job := os.Getenv("GITHUB_JOB")
	githubToken := os.Getenv("GITHUB_TOKEN")
	sha, err := getSha()
	if err != nil {
		logError("get checked-out SHA", err)
	}
	ctx := context.Background()
	// Only post issues if this is master or a release branch and the job is
	// not EXPERIMENTAL.
	if (branch == "master" || strings.HasPrefix(branch, "release-")) &&
		!strings.HasPrefix(job, "EXPERIMENTAL_") &&
		githubToken != "" {
		for _, testResults := range invocation.TestResults {
			for _, testResult := range testResults {
				if testResult.Err != nil {
					// We already logged this error.
					continue
				}
				var testXml bazelutil.TestSuites
				if err := xml.Unmarshal([]byte(testResult.TestXml), &testXml); err != nil {
					fmt.Fprintf(os.Stderr, "Could not parse test.xml: got error %+v\n", err)
					continue
				}

				if err := githubpost.PostFromTestXMLWithFailurePoster(
					ctx, engflow.FailurePoster(testResult, engflow.FailurePosterOptions{
						Sha:            sha,
						InvocationId:   invocation.InvocationId,
						ServerName:     "mesolite",
						GithubApiToken: githubToken,
					}), testXml); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to process %+v with the following error: %+v", testXml, err)
					continue
				}
			}
		}
	} else {
		fmt.Fprintf(os.Stderr, "Not reporting issues for branch %s and job %s", branch, job)
	}

	return nil
}

func dumpSummary(f *os.File, invocation *engflow.InvocationInfo) error {
	var title string
	if !invocation.Finished {
		title = "# Build did not succeed"
	} else if invocation.ExitCode == 0 {
		title = "# Build Succeeded"
	} else {
		title = fmt.Sprintf("# Build Failed (code: %s)", invocation.ExitCodeName)
	}
	_, err := f.WriteString(fmt.Sprintf("%s\n", title))
	if err != nil {
		return err
	}
	_, err = f.WriteString(fmt.Sprintf("### [EngFlow link](https://%s.cluster.engflow.com/invocations/default/%s)\n", *serverName, invocation.InvocationId))
	if err != nil {
		return err
	}

	var failedTests []failedTestResult
	for _, testResults := range invocation.TestResults {
		for _, testResult := range testResults {
			if testResult.Err != nil {
				// We already logged this error.
				continue
			}
			var testXml bazelutil.TestSuites
			if err := xml.Unmarshal([]byte(testResult.TestXml), &testXml); err != nil {
				fmt.Fprintf(os.Stderr, "Could not parse test.xml: got error %+v\n", err)
				continue
			}
			for _, suite := range testXml.Suites {
				for _, testCase := range suite.TestCases {
					outResult := failedTestResult{
						label:   testResult.Label,
						name:    testCase.Name,
						run:     testResult.Run,
						shard:   testResult.Shard,
						attempt: testResult.Attempt,
					}
					if testCase.Error != nil {
						outResult.status = "ERROR"
					} else if testCase.Failure != nil {
						outResult.status = "FAILURE"
					}
					if outResult.status != "" {
						failedTests = append(failedTests, outResult)
					}
				}
			}
		}
	}

	sort.Slice(failedTests, func(i, j int) bool {
		t1 := failedTests[i]
		t2 := failedTests[j]
		if t1.label < t2.label {
			return true
		} else if t1.label == t2.label {
			return t1.name < t2.name
		} else {
			return false
		}
	})

	if len(failedTests) != 0 {
		_, err := f.WriteString(`| Label | TestName | Status | Link |
| --- | --- | --- | --- |
`)
		if err != nil {
			return err
		}
		for _, failedTest := range failedTests {
			base64Target := base64.StdEncoding.EncodeToString([]byte(failedTest.label))
			_, err := f.WriteString(fmt.Sprintf("| `%s` | `%s` | `%s` | [Link](https://%s.cluster.engflow.com/invocations/default/%s?testReportRun=%d&testReportShard=%d&testReportAttempt=%d#targets-%s) |\n", failedTest.label, failedTest.name, failedTest.status, *serverName, invocation.InvocationId, failedTest.run, failedTest.shard, failedTest.attempt, base64Target))
			if err != nil {
				return err
			}
		}
	}

	if len(invocation.FailedBuildActions) > 0 {
		for _, actions := range invocation.FailedBuildActions {
			for _, action := range actions {
				_, err := f.WriteString(fmt.Sprintf("### Build of `%s` failed (exit code %d)\n", action.Label, action.ExitCode))
				if err != nil {
					return err
				}
				if action.FailureDetail != "" {
					_, err := f.WriteString(fmt.Sprintf("```\n%s\n```\n", action.FailureDetail))
					if err != nil {
						return err
					}
				}
				for _, err := range action.Errs {
					fmt.Fprintf(os.Stderr, "could not download action log: got error %+v\n", err)
				}
				if action.Stdout != "" {
					_, err := f.WriteString(fmt.Sprintf("Stdout:\n```\n%s\n```\n", action.Stdout))
					if err != nil {
						return err
					}
				}
				if action.Stderr != "" {
					_, err := f.WriteString(fmt.Sprintf("Stdout:\n```\n%s\n```\n", action.Stderr))
					if err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func getSha() (string, error) {
	cmd := exec.Command("git", "rev-parse", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func logError(ctx string, err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "encountered error when attempting to %s: %+v", ctx, err)
}

func main() {
	flag.Parse()
	if *eventStreamFile == "" {
		fmt.Fprintln(os.Stderr, "must provide -eventsfile")
		os.Exit(1)
	}
	if *serverName == "" {
		fmt.Fprintln(os.Stderr, "must provide -servername")
		os.Exit(1)
	}
	if *tlsClientCert == "" {
		fmt.Fprintln(os.Stderr, "must provide -cert")
		os.Exit(1)
	}
	if *tlsClientKey == "" {
		fmt.Fprintln(os.Stderr, "must provide -key")
		os.Exit(1)
	}
	if err := process(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %+v", err)
		os.Exit(1)
	}
}
