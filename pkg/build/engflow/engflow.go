// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engflow

import (
	"crypto/tls"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	bes "github.com/cockroachdb/cockroach/pkg/build/bazel/bes"
	bazelutil "github.com/cockroachdb/cockroach/pkg/build/util"
	//lint:ignore SA1019 grandfathered
	gproto "github.com/golang/protobuf/proto"
	"golang.org/x/net/http2"
)

type testResultWithMetadata struct {
	run, shard, attempt int32
	testResult          *bes.TestResult
}

type TestResultWithXml struct {
	Label               string
	Run, Shard, Attempt int32
	TestResult          *bes.TestResult
	TestXml             string
	Err                 error
}

type InvocationInfo struct {
	InvocationId      string
	StartedTimeMillis int64
	FinishTimeMillis  int64
	TestResults       map[string][]*TestResultWithXml
}

type JsonReport struct {
	Server         string                      `json:"server"`
	InvocationId   string                      `json:"invocation_id"`
	StartedAt      string                      `json:"started_at"`
	FinishedAt     string                      `json:"finished_at"`
	ResultsByLabel map[string][]JsonTestResult `json:"results_by_label"`
}

type JsonTestResult struct {
	TestName       string `json:"test_name"`
	Status         string `json:"status"` // One of: "SUCCESS", "FAILURE", "ERROR", "SKIPPED"
	DurationMillis int64  `json:"duration_millis"`
}

func getHttpClient(certFile, keyFile string) (*http.Client, error) {
	cer, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	config := &tls.Config{
		Certificates: []tls.Certificate{cer},
	}
	transport := &http2.Transport{
		TLSClientConfig: config,
	}
	httpClient := &http.Client{
		Transport: transport,
	}
	return httpClient, nil
}

func downloadTestXml(client *http.Client, uri string) (string, error) {
	url := strings.ReplaceAll(uri, "bytestream://", "https://")
	url = strings.ReplaceAll(url, "/blobs/", "/api/v0/blob/")
	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	xml, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(xml), nil
}

func fetchTestXml(
	httpClient *http.Client,
	label string,
	testResult testResultWithMetadata,
	ch chan *TestResultWithXml,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	for _, output := range testResult.testResult.TestActionOutput {
		if output.Name == "test.xml" {
			xml, err := downloadTestXml(httpClient, output.GetUri())
			ch <- &TestResultWithXml{
				Label:      label,
				Run:        testResult.run,
				Shard:      testResult.shard,
				Attempt:    testResult.attempt,
				TestXml:    xml,
				Err:        err,
				TestResult: testResult.testResult,
			}
		}
	}
}

// LoadTestResults parses the test results out of the given event stream file, returning a
// map of test results keyed by label.
// Note the TestResultWithXml struct contains an Err field. This is the error
// (if any) from fetching the test.xml for this test run. This must be checked
// *in addition to* the err return value from the function.
func LoadTestResults(
	eventStreamFile io.Reader, certFile string, keyFile string,
) (*InvocationInfo, error) {
	httpClient, err := getHttpClient(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	content, err := io.ReadAll(eventStreamFile)
	if err != nil {
		return nil, err
	}
	buf := gproto.NewBuffer(content)
	ret := &InvocationInfo{
		TestResults: make(map[string][]*TestResultWithXml),
	}
	testResults := make(map[string][]testResultWithMetadata)

	for {
		var event bes.BuildEvent
		err := buf.DecodeMessage(&event)
		if err != nil {
			// This is probably OK: just no more stuff left in the buffer.
			break
		}
		switch id := event.Id.Id.(type) {
		case *bes.BuildEventId_Started:
			started := event.GetStarted()
			ret.InvocationId = started.Uuid
			ret.StartedTimeMillis = started.StartTimeMillis
		case *bes.BuildEventId_TestResult:
			res := testResultWithMetadata{
				run:        id.TestResult.Run,
				shard:      id.TestResult.Shard,
				attempt:    id.TestResult.Attempt,
				testResult: event.GetTestResult(),
			}
			testResults[id.TestResult.Label] = append(testResults[id.TestResult.Label], res)
		case *bes.BuildEventId_BuildFinished:
			finished := event.GetFinished()
			ret.FinishTimeMillis = finished.FinishTimeMillis
		}

	}

	unread := buf.Unread()
	if len(unread) != 0 {
		return nil, fmt.Errorf("didn't read entire file: %d bytes remaining", len(unread))
	}

	// Download test xml's.
	ch := make(chan *TestResultWithXml)
	var wg sync.WaitGroup
	for label, results := range testResults {
		for _, result := range results {
			wg.Add(1)
			go fetchTestXml(httpClient, label, result, ch, &wg)
		}
	}
	go func(wg *sync.WaitGroup) {
		wg.Wait()
		close(ch)
	}(&wg)

	// Collect test xml's.
	for result := range ch {
		ret.TestResults[result.Label] = append(ret.TestResults[result.Label], result)
	}

	for _, slice := range ret.TestResults {
		slices.SortFunc(slice, func(a, b *TestResultWithXml) int {
			// First Shard, then Run, then Attempt.
			if a.Run < b.Run {
				return -1
			} else if a.Run > b.Run {
				return 1
			} else if a.Shard < b.Shard {
				return -1
			} else if a.Shard > b.Shard {
				return 1
			} else if a.Attempt < b.Attempt {
				return -1
			} else if a.Attempt > b.Attempt {
				return 1
			}
			return 0
		})
	}

	return ret, nil
}

func timeMillisToString(t int64) string {
	return time.UnixMilli(t).Format(time.RFC3339)
}

func stringToMillis(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	return int64(f * 1000.0), nil
}

// ConstructJSONReport transforms an InvocationInfo into a JsonReport. It can be
// serialized with json.Marshal. In addition to the JSON report, we also return
// a list of errors. Usually this list should be empty unless a problem
// occurred fetching test.xml in LoadTestResults earlier, or if the test.xml's
// cannot be parsed somehow. However, we always make a best effort to return
// a complete and functional JsonReport. So for example if one test.xml cannot
// be parsed or was not fetched, then the report will be missing those test
// results, but everything else will be present. If errs is empty, then the
// report is complete.
func ConstructJSONReport(invocation *InvocationInfo, serverName string) (JsonReport, []error) {
	ret := JsonReport{
		Server:         serverName,
		InvocationId:   invocation.InvocationId,
		StartedAt:      timeMillisToString(invocation.StartedTimeMillis),
		FinishedAt:     timeMillisToString(invocation.FinishTimeMillis),
		ResultsByLabel: make(map[string][]JsonTestResult),
	}
	var errs []error

	for label, results := range invocation.TestResults {
		var slice []JsonTestResult
		for _, res := range results {
			if res.Err != nil {
				errs = append(errs, fmt.Errorf("couldn't fetch test.xml for test %+v (%w)", res.TestResult, res.Err))
				continue
			}
			var testXml bazelutil.TestSuites
			if err := xml.Unmarshal([]byte(res.TestXml), &testXml); err != nil {
				errs = append(errs, fmt.Errorf("could not parse test.xml for test %+v (%w)", res.TestResult, err))
				continue
			}
			for _, suite := range testXml.Suites {
				for _, testCase := range suite.TestCases {
					var outputResult JsonTestResult
					outputResult.TestName = testCase.Name
					durationMillis, err := stringToMillis(testCase.Time)
					if err != nil {
						errs = append(errs, fmt.Errorf("could not parse time from %s for test %+v (%w)", testCase.Time, res.TestResult, err))
						// The duration will be 0 for the report which is fine.
					}
					outputResult.DurationMillis = durationMillis
					if testCase.Error != nil {
						outputResult.Status = "ERROR"
					} else if testCase.Failure != nil {
						outputResult.Status = "FAILURE"
					} else if testCase.Skipped != nil {
						outputResult.Status = "SKIPPED"
					} else {
						outputResult.Status = "SUCCESS"
					}
					slice = append(slice, outputResult)
				}
			}

		}
		ret.ResultsByLabel[label] = slice
	}

	return ret, errs
}
