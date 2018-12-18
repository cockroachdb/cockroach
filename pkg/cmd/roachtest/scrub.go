// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func registerScrubIndexOnlyTPCC(r *registry) {
	r.Add(makeScrubTPCCTest(5, 1000, time.Hour*2, "index-only", 3))
}

func registerScrubAllChecksTPCC(r *registry) {
	r.Add(makeScrubTPCCTest(5, 1000, time.Hour*2, "all-checks", 2))
}

func makeScrubTPCCTest(
	numNodes, warehouses int, length time.Duration, optionName string, numScrubRuns int,
) testSpec {
	var stmtOptions string
	// SCRUB checks are run at -5s to avoid contention with TPCC traffic.
	// By the time the SCRUB queries start, the tables will have been loaded for
	// some time (since it takes some time to run the TPCC consistency checks),
	// so using a timestamp in the past is fine.
	switch optionName {
	case "index-only":
		stmtOptions = `AS OF SYSTEM TIME '-5s' WITH OPTIONS INDEX ALL`
	case "all-checks":
		stmtOptions = `AS OF SYSTEM TIME '-5s'`
	default:
		panic(fmt.Sprintf("Not a valid option: %s", optionName))
	}

	return testSpec{
		Name:  fmt.Sprintf("scrub/%s/tpcc-%d", optionName, warehouses),
		Nodes: nodes(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses: warehouses,
				Extra:      "--wait=false --tolerate-errors",
				During: func(ctx context.Context) error {
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
	}
}
