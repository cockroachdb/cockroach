// Copyright 2024 The Cockroach Authors.
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

// extract-test-results is a binary to parse test results from a "build event
// protocol" binary file, as constructed by
// `bazel ... --build_event_binary_file`. We use this data to construct a JSON
// test report.

package main

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/build/engflow"
	bazelutil "github.com/cockroachdb/cockroach/pkg/build/util"
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
	invocation, err := engflow.LoadTestResults(eventStreamF, *tlsClientCert, *tlsClientKey)
	if err != nil {
		return err
	}

	jsonReport, errs := engflow.ConstructJSONReport(invocation, *serverName)
	for _, err := range errs {
		fmt.Fprintf(os.Stderr, "ERROR: %+v\n", err)
	}

	if *jsonOutFile != "" {
		jsonData, err := json.Marshal(jsonReport)
		if err != nil {
			return err
		}
		err = os.WriteFile(*jsonOutFile, jsonData, 0644)
		if err != nil {
			return err
		}
	} else {
		fmt.Fprintln(os.Stderr, "-jsonoutfile not passed, skipping constructing JSON test report")
	}

	if summaryFile != "" {
		summaryF, err := os.OpenFile(summaryFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer func() { _ = summaryF.Close() }()
		err = dumpSummary(summaryF, invocation)
		if err != nil {
			return err
		}
	} else {
		fmt.Fprintln(os.Stderr, "GITHUB_STEP_SUMMARY not passed, skipping populating GitHub actions test report")
	}

	return nil
}

func dumpSummary(f *os.File, invocation *engflow.InvocationInfo) error {
	var title string
	if invocation.ExitCode == 0 {
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
				fmt.Fprintf(os.Stderr, "Could not parse test.xml: got error %+v", err)
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

	if len(failedTests) != 0 {
		_, err := f.WriteString(`| Label | TestName | Status | Link |
| --- | --- | --- | --- |
`)
		if err != nil {
			return err
		}
		for _, failedTest := range failedTests {
			base64Target := base64.StdEncoding.EncodeToString([]byte(failedTest.label))
			_, err := f.WriteString(fmt.Sprintf("| `%s` | `%s` | `%s` | [Link](https://%s.cluster.engflow.com/invocations/default/%s?testReportRun=%d&testReportShard=%d&testReportAttempt=%d#targets-%s) |", failedTest.label, failedTest.name, failedTest.status, *serverName, invocation.InvocationId, failedTest.run, failedTest.shard, failedTest.attempt, base64Target))
			if err != nil {
				return err
			}
		}
	}

	return nil
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
