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
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/build/engflow"
)

var (
	eventStreamFile = flag.String("eventsfile", "", "eventstream file produced by bazel build --build_event_binary_file")
	serverName      = flag.String("servername", "mesolite", "name of the EngFlow cluster (mesolite, tanzanite)")
	tlsClientCert   = flag.String("cert", "", "TLS client certificate for accessing EngFlow, probably a .crt file")
	tlsClientKey    = flag.String("key", "", "TLS client key for accessing EngFlow")
)

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
		fmt.Fprintf(os.Stderr, "ERROR: %+v", err)
	}

	jsonOut, err := json.Marshal(jsonReport)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", jsonOut)

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
