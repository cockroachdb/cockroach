// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest-selector/sfselector"
)

const (
	defaultForPastDays = 30
	defaultFirstRunOn  = 20
	defaultLastRunOn   = 7
)

func main() {
	ctx := context.Background()

	forPastDays := flag.Int("for-past-days", defaultForPastDays,
		"number of days data to consider for test selection")
	firstRunOn := flag.Int("first-run-on", defaultFirstRunOn,
		"number of days to consider for the first time the test is run")
	lastRunOn := flag.Int("last-run-on", defaultLastRunOn,
		"number of days to consider for the last time the test is run")
	selectFromSuccessPct := flag.Int("select-from-success-pct", 0,
		"percentage of tests to be selected for running from the successful test list sorted by number of runs")
	cloud := flag.String("cloud", "gce",
		"the cloud where the tests were run")
	suite := flag.String("suite", "nightly",
		"the test suite for which the selection is done")
	dryRun := flag.Bool("dry-run", true,
		"instead of uploading the csv content to GCS, dump it in stdout")
	flag.Parse()
	flag.VisitAll(func(f *flag.Flag) {
		if f.Value.String() != "" {
			fmt.Printf("Flag %s is set to %s\n", f.Name, f.Value.String())
		}
	})
	if err := sfselector.SelectTests(ctx, &sfselector.SelectTestsReq{
		ForPastDays:          *forPastDays,
		FirstRunOn:           *firstRunOn,
		LastRunOn:            *lastRunOn,
		SelectFromSuccessPct: *selectFromSuccessPct,
		Cloud:                *cloud,
		Suite:                *suite,
		DryRun:               *dryRun,
	}); err != nil {
		fmt.Printf("Failed to select tests: %v\n", err)
	}
}
