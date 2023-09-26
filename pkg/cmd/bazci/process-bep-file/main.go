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
// `bazel ... --build_event_binary_file`.

package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"

	bes "github.com/cockroachdb/cockroach/pkg/build/bazel/bes"
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
	eventStreamFile = flag.String("eventsfile", "", "eventstream file produced by bazel build --build_event_binary_file")
	tlsClientCert   = flag.String("cert", "", "TLS client certificate for accessing EngFlow, probably a .crt file")
	tlsClientKey    = flag.String("key", "", "TLS client key for accessing EngFlow")
)

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
	ch chan testResultWithXml,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	for _, output := range testResult.testResult.TestActionOutput {
		if output.Name == "test.xml" {
			xml, err := downloadTestXml(httpClient, output.GetUri())
			ch <- testResultWithXml{
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
	fullTestResults := make(map[string][]testResultWithXml)
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
	ch := make(chan testResultWithXml)
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

	// TODO: handle results

	return nil
}

func main() {
	flag.Parse()
	if *eventStreamFile == "" {
		fmt.Println("must provide -eventsfile")
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
