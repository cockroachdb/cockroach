// Copyright 2018 The Cockroach Authors.
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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func registerScrubIndexOnlyTPCC(r *testRegistry) {
	// numScrubRuns is set assuming a single SCRUB run (index only) takes ~1 min
	r.Add(makeScrubTPCCTest(5, 100, 30*time.Minute, "index-only", 20))
}

func registerScrubAllChecksTPCC(r *testRegistry) {
	// numScrubRuns is set assuming a single SCRUB run (all checks) takes ~2 min
	r.Add(makeScrubTPCCTest(5, 100, 30*time.Minute, "all-checks", 10))
}

func makeScrubTPCCTest(
	numNodes, warehouses int, length time.Duration, optionName string, numScrubRuns int,
) testSpec {
	var stmtOptions string
	// SCRUB checks are run at -1m to avoid contention with TPCC traffic.
	// By the time the SCRUB queries start, the tables will have been loaded for
	// some time (since it takes some time to run the TPCC consistency checks),
	// so using a timestamp in the past is fine.
	switch optionName {
	case "index-only":
		stmtOptions = `AS OF SYSTEM TIME '-1m' WITH OPTIONS INDEX ALL`
	case "all-checks":
		stmtOptions = `AS OF SYSTEM TIME '-1m'`
	default:
		panic(fmt.Sprintf("Not a valid option: %s", optionName))
	}

	return testSpec{
		Name:    fmt.Sprintf("scrub/%s/tpcc/w=%d", optionName, warehouses),
		Cluster: makeClusterSpec(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses: warehouses,
				Extra:      "--wait=false --tolerate-errors",
				During: func(ctx context.Context) error {
					if !c.isLocal() {
						// Wait until tpcc has been running for a few minutes to start SCRUB checks
						sleepInterval := time.Minute * 10
						maxSleep := length / 2
						if sleepInterval > maxSleep {
							sleepInterval = maxSleep
						}
						time.Sleep(sleepInterval)
					}

					conn := c.Conn(ctx, 1)
					defer conn.Close()

					c.l.Printf("Starting %d SCRUB checks", numScrubRuns)
					for i := 0; i < numScrubRuns; i++ {
						c.l.Printf("Running SCRUB check %d\n", i+1)
						before := timeutil.Now()
						err := sqlutils.RunScrubWithOptions(conn, "tpcc", "order", stmtOptions)
						c.l.Printf("SCRUB check %d took %v\n", i+1, timeutil.Since(before))

						if err != nil {
							t.Fatal(err)
						}
					}
					return nil
				},
				Duration: length,
			})
		},
		MinVersion: "v19.1.0",
	}
}
