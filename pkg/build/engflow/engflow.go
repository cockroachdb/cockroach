// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package engflow

import (
	"context"
	"crypto/tls"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	bes "github.com/cockroachdb/cockroach/pkg/build/bazel/bes"
	bazelutil "github.com/cockroachdb/cockroach/pkg/build/util"
	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost"
	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost/issues"
	//lint:ignore SA1019 grandfathered
	gproto "github.com/golang/protobuf/proto"
	"golang.org/x/net/http2"
)

type testResultWithMetadata struct {
	run, shard, attempt int32
	testResult          *bes.TestResult
}

type buildActionWithDownloadUris struct {
	exitCode             int32
	stdoutUri, stderrUri string
	failureDetail        string
}

type TestResultWithXml struct {
	Label               string
	Run, Shard, Attempt int32
	TestResult          *bes.TestResult
	TestXml             string
	Err                 error
}

type BuildAction struct {
	Label         string
	ExitCode      int32
	Stdout        string
	Stderr        string
	Errs          []error
	FailureDetail string
}

type InvocationInfo struct {
	InvocationId       string
	StartedTimeMillis  int64
	FinishTimeMillis   int64
	Finished           bool
	ExitCode           int32
	ExitCodeName       string
	TestResults        map[string][]*TestResultWithXml
	FailedBuildActions map[string][]*BuildAction
}

type JsonReport struct {
	Server         string                      `json:"server"`
	InvocationId   string                      `json:"invocation_id"`
	StartedAt      string                      `json:"started_at"`
	FinishedAt     string                      `json:"finished_at"`
	ExitCode       int32                       `json:"exit_code"`
	ExitCodeName   string                      `json:"exit_code_name"`
	ResultsByLabel map[string][]JsonTestResult `json:"results_by_label"`

	GitHubRunId      int64  `json:"github_run_id,omitempty"`
	GitHubJob        string `json:"github_job,omitempty"`
	GitHubRunAttempt int64  `json:"github_run_attempt,omitempty"`

	TeamCityBuildTypeId string `json:"teamcity_buildtype_id,omitempty"`
	TeamCityBuildId     int64  `json:"teamcity_build_id,omitempty"`

	Branch string `json:"branch,omitempty"`
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

func downloadFile(client *http.Client, uri string) (string, error) {
	url := strings.ReplaceAll(uri, "bytestream://", "https://")
	url = strings.ReplaceAll(url, "/blobs/", "/api/v0/blob/")
	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	contents, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(contents), nil
}

func fetchTestXmlForTest(
	httpClient *http.Client,
	label string,
	testResult *testResultWithMetadata,
	ch chan *TestResultWithXml,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	out := TestResultWithXml{
		Label:      label,
		Run:        testResult.run,
		Shard:      testResult.shard,
		Attempt:    testResult.attempt,
		TestResult: testResult.testResult,
	}
	if testResult.testResult == nil {
		ch <- &out
		return
	}
	for _, output := range testResult.testResult.TestActionOutput {
		if output.Name == "test.xml" {
			xml, err := downloadFile(httpClient, output.GetUri())
			out.TestXml = xml
			out.Err = err
			ch <- &out
			break
		}
	}
}

func fetchStdoutStderrForBuildAction(
	httpClient *http.Client,
	label string,
	action *buildActionWithDownloadUris,
	ch chan *BuildAction,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	var err error
	var stdout, stderr string
	errs := make([]error, 0, 2)
	if action.stdoutUri != "" {
		stdout, err = downloadFile(httpClient, action.stdoutUri)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if action.stderrUri != "" {
		stderr, err = downloadFile(httpClient, action.stderrUri)
		if err != nil {
			errs = append(errs, err)
		}
	}
	ch <- &BuildAction{
		Label:         label,
		ExitCode:      action.exitCode,
		FailureDetail: action.failureDetail,
		Stdout:        stdout,
		Stderr:        stderr,
		Errs:          errs,
	}
}

// LoadInvocationInfo parses the relevant information including test results
// out of the given event stream file, returning an InvocationInfo object.
// Note the TestResultWithXml sub-struct contains an Err field. This is the
// error (if any) from fetching the test.xml for this test run. This must be
// checked *in addition to* the err return value from the function.
func LoadInvocationInfo(
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
	ret := &InvocationInfo{}
	testResults := make(map[string][]*testResultWithMetadata)
	failedActions := make(map[string][]*buildActionWithDownloadUris)

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
		case *bes.BuildEventId_ActionCompleted:
			action := event.GetAction()
			outAction := buildActionWithDownloadUris{
				exitCode: action.ExitCode,
			}
			if action.Stdout != nil {
				outAction.stdoutUri = action.Stdout.GetUri()
			}
			if action.Stderr != nil {
				outAction.stderrUri = action.Stderr.GetUri()
			}
			if action.FailureDetail != nil {
				outAction.failureDetail = action.FailureDetail.Message
			}
			failedActions[id.ActionCompleted.Label] = append(failedActions[id.ActionCompleted.Label], &outAction)
		case *bes.BuildEventId_TestResult:
			res := testResultWithMetadata{
				run:        id.TestResult.Run,
				shard:      id.TestResult.Shard,
				attempt:    id.TestResult.Attempt,
				testResult: event.GetTestResult(),
			}
			testResults[id.TestResult.Label] = append(testResults[id.TestResult.Label], &res)
		case *bes.BuildEventId_BuildFinished:
			finished := event.GetFinished()
			ret.FinishTimeMillis = finished.FinishTimeMillis
			ret.Finished = true
			exitCode := finished.ExitCode
			ret.ExitCode = exitCode.Code
			ret.ExitCodeName = exitCode.Name
		}
	}

	unread := buf.Unread()
	if len(unread) != 0 {
		fmt.Fprintf(os.Stderr, "did not read entire BES file: %d bytes remaining (was the bazel invocation interrupted?)", len(unread))
		// We should be fine to proceed though we may be missing data.
	}

	// Download test xml's and build action output.
	testCh := make(chan *TestResultWithXml)
	actionCh := make(chan *BuildAction)
	var testWg, actionWg sync.WaitGroup
	for label, results := range testResults {
		for _, result := range results {
			testWg.Add(1)
			go fetchTestXmlForTest(httpClient, label, result, testCh, &testWg)
		}
	}
	for label, actions := range failedActions {
		for _, action := range actions {
			actionWg.Add(1)
			go fetchStdoutStderrForBuildAction(httpClient, label, action, actionCh, &actionWg)
		}
	}
	go func(wg *sync.WaitGroup) {
		wg.Wait()
		close(testCh)
	}(&testWg)
	go func(wg *sync.WaitGroup) {
		wg.Wait()
		close(actionCh)
	}(&actionWg)

	var finalWg sync.WaitGroup

	var finalTestResults map[string][]*TestResultWithXml
	finalWg.Add(1)
	// Collect test xml's.
	go func(wg *sync.WaitGroup) {
		res := make(map[string][]*TestResultWithXml)
		for result := range testCh {
			res[result.Label] = append(res[result.Label], result)
		}
		finalTestResults = res
		wg.Done()
	}(&finalWg)
	var finalBuildActions map[string][]*BuildAction
	finalWg.Add(1)
	go func(wg *sync.WaitGroup) {
		res := make(map[string][]*BuildAction)
		for action := range actionCh {
			res[action.Label] = append(res[action.Label], action)
		}
		finalBuildActions = res
		wg.Done()
	}(&finalWg)

	finalWg.Wait()
	ret.TestResults = finalTestResults
	ret.FailedBuildActions = finalBuildActions

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
//
// In addition to the passed-in arguments, we also consult the following
// environment variables for supplemental data: GITHUB_RUN_ID, GITHUB_JOB,
// GITHUB_RUN_ATTEMPT, TC_BUILDTYPE_ID, TC_BUILD_ID, GITHUB_ACTIONS_BRANCH and
// TC_BUILD_BRANCH. These are environment variables that either GitHub sets for us (see
// https://docs.github.com/en/actions/learn-github-actions/variables#default-environment-variables),
// we set in TeamCity, or we set as part of the GitHub workflow definition (see
// github-actions-essential-ci.yml).
func ConstructJSONReport(invocation *InvocationInfo, serverName string) (JsonReport, []error) {
	ret := JsonReport{
		Server:              serverName,
		InvocationId:        invocation.InvocationId,
		StartedAt:           timeMillisToString(invocation.StartedTimeMillis),
		FinishedAt:          timeMillisToString(invocation.FinishTimeMillis),
		ExitCode:            invocation.ExitCode,
		ExitCodeName:        invocation.ExitCodeName,
		ResultsByLabel:      make(map[string][]JsonTestResult),
		GitHubRunId:         tryParseInt(os.Getenv("GITHUB_RUN_ID")),
		GitHubJob:           os.Getenv("GITHUB_JOB"),
		GitHubRunAttempt:    tryParseInt(os.Getenv("GITHUB_RUN_ATTEMPT")),
		TeamCityBuildTypeId: os.Getenv("TC_BUILDTYPE_ID"),
		TeamCityBuildId:     tryParseInt(os.Getenv("TC_BUILD_ID")),
		Branch:              getBranch(),
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

func postOptions(res *TestResultWithXml, opts FailurePosterOptions) *issues.Options {
	return &issues.Options{
		Token:  opts.GithubApiToken,
		Org:    "cockroachdb",
		Repo:   "cockroach",
		SHA:    opts.Sha,
		Branch: getBranch(),
		EngFlowOptions: &issues.EngFlowOptions{
			Attempt:      int(res.Attempt),
			InvocationID: opts.InvocationId,
			Label:        res.Label,
			Run:          int(res.Run),
			Shard:        int(res.Shard),
			ServerURL:    fmt.Sprintf("https://%s.cluster.engflow.com/", opts.ServerName),
		},
		GetBinaryVersion: build.BinaryVersion,
	}
}

type FailurePosterOptions struct {
	Sha, InvocationId, ServerName, GithubApiToken string
	ExtraParams                                   []string
}

// FailurePoster returns a githubpost.FailurePoster that's appropriate for use
// with githubpost.PostFromTestXMLWithFailurePoster.
func FailurePoster(res *TestResultWithXml, opts FailurePosterOptions) githubpost.FailurePoster {
	postOpts := postOptions(res, opts)
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
		if len(opts.ExtraParams) > 0 {
			for _, key := range opts.ExtraParams {
				req.ExtraParams[key] = "true"
			}
		}
		return fmter, req
	}
	return func(ctx context.Context, failure githubpost.Failure) error {
		fmter, req := formatter(ctx, failure)
		_, err := issues.Post(ctx, log.Default(), fmter, req, postOpts)
		return err
	}
}

func tryParseInt(s string) int64 {
	if s == "" {
		return 0
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return i
}

func getBranch() string {
	b := os.Getenv("GITHUB_ACTIONS_BRANCH")
	if b != "" {
		return b
	}
	return strings.TrimPrefix(os.Getenv("TC_BUILD_BRANCH"), "refs/heads/")
}
