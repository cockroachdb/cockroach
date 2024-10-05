// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build bazel
// +build bazel

// process-bep-file is a binary to parse test results from a "build event
// protocol" binary file, as constructed by
// `bazel ... --build_event_binary_file`.

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/build"
	bes "github.com/cockroachdb/cockroach/pkg/build/bazel/bes"
	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost"
	"github.com/cockroachdb/cockroach/pkg/cmd/internal/issues"
	//lint:ignore SA1019
	gproto "github.com/golang/protobuf/proto"
	"golang.org/x/net/http2"
)

type testResultWithMetadata struct {
	run, shard, attempt int32
	testResult          *bes.TestResult
}

type testResultWithXml struct {
	label               string
	run, shard, attempt int32
	testResult          *bes.TestResult
	testXml             string
	err                 error
}

var (
	branch          = flag.String("branch", "", "currently checked out git branch")
	eventStreamFile = flag.String("eventsfile", "", "eventstream file produced by bazel build --build_event_binary_file")

	invocationId  = flag.String("invocation", "", "UUID of the invocation")
	serverUrl     = flag.String("serverurl", "https://tanzanite.cluster.engflow.com/", "URL of the EngFlow cluster")
	tlsClientCert = flag.String("cert", "", "TLS client certificate for accessing EngFlow, probably a .crt file")
	tlsClientKey  = flag.String("key", "", "TLS client key for accessing EngFlow")
)

func getSha() (string, error) {
	cmd := exec.Command("git", "rev-parse", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func postOptions(sha string) *issues.Options {
	return &issues.Options{
		Token:  os.Getenv("GITHUB_API_TOKEN"),
		Org:    "cockroachdb",
		Repo:   "cockroach",
		SHA:    sha,
		Branch: *branch,
		EngFlowOptions: &issues.EngFlowOptions{
			InvocationID: *invocationId,
			ServerURL:    *serverUrl,
		},
		GetBinaryVersion: build.BinaryVersion,
	}

}

func failurePoster(res *testResultWithXml, opts *issues.Options) githubpost.FailurePoster {
	formatter := func(ctx context.Context, failure githubpost.Failure) (issues.IssueFormatter, issues.PostRequest) {
		fmter, req := githubpost.DefaultFormatter(ctx, failure)
		// We don't want an artifacts link: there are none on EngFlow.
		req.Artifacts = ""
		if req.ExtraParams == nil {
			req.ExtraParams = make(map[string]string)
		}
		if res.run != 0 {
			req.ExtraParams["run"] = fmt.Sprintf("%d", res.run)
		}
		if res.shard != 0 {
			req.ExtraParams["shard"] = fmt.Sprintf("%d", res.shard)
		}
		if res.attempt != 0 {
			req.ExtraParams["attempt"] = fmt.Sprintf("%d", res.attempt)
		}
		return fmter, req
	}
	return func(ctx context.Context, failure githubpost.Failure) error {
		fmter, req := formatter(ctx, failure)
		_, err := issues.Post(ctx, log.Default(), fmter, req, opts)
		return err
	}
}

func getHttpClient() (*http.Client, error) {
	cer, err := tls.LoadX509KeyPair(*tlsClientCert, *tlsClientKey)
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
	ch chan *testResultWithXml,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	for _, output := range testResult.testResult.TestActionOutput {
		if output.Name == "test.xml" {
			xml, err := downloadTestXml(httpClient, output.GetUri())
			ch <- &testResultWithXml{
				label:      label,
				run:        testResult.run,
				shard:      testResult.shard,
				attempt:    testResult.attempt,
				testXml:    xml,
				err:        err,
				testResult: testResult.testResult,
			}
		}
	}
}

func process() error {
	ctx := context.Background()
	sha, err := getSha()
	if err != nil {
		return err
	}
	postOpts := postOptions(sha)
	httpClient, err := getHttpClient()
	if err != nil {
		return err
	}
	content, err := os.ReadFile(*eventStreamFile)
	if err != nil {
		return err
	}
	buf := gproto.NewBuffer(content)
	testResults := make(map[string][]testResultWithMetadata)
	fullTestResults := make(map[string][]*testResultWithXml)
	for {
		var event bes.BuildEvent
		err := buf.DecodeMessage(&event)
		if err != nil {
			// This is probably OK: just no more stuff left in the buffer.
			break
		}
		switch id := event.Id.Id.(type) {
		case *bes.BuildEventId_TestResult:
			res := testResultWithMetadata{
				run:        id.TestResult.Run,
				shard:      id.TestResult.Shard,
				attempt:    id.TestResult.Attempt,
				testResult: event.GetTestResult(),
			}
			testResults[id.TestResult.Label] = append(testResults[id.TestResult.Label], res)
		}
	}
	unread := buf.Unread()
	if len(unread) != 0 {
		return fmt.Errorf("didn't read entire file: %d bytes remaining", len(unread))
	}

	// Download test xml's.
	ch := make(chan *testResultWithXml)
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
		fullTestResults[result.label] = append(fullTestResults[result.label], result)
		if result.err != nil {
			fmt.Printf("got error downloading test XML for result %+v; got error %+v", result, result.err)
		}
	}

	if os.Getenv("GITHUB_API_TOKEN") == "" {
		fmt.Printf("no GITHUB_API_TOKEN; skipping reporting to GitHub")
		return nil
	}

	for _, results := range fullTestResults {
		for _, res := range results {
			if res.testXml == "" {
				// We already logged the download failure above.
				continue
			}
			testXml := strings.NewReader(res.testXml)
			if err := githubpost.PostFromTestXMLWithFailurePoster(
				ctx, failurePoster(res, postOpts), testXml); err != nil {
				fmt.Printf("could not post to GitHub: got error %+v", err)
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
