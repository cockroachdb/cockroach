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
)

var tpccTables = [...]string{
	"customer",
	"district",
	"history",
	"item",
	"new_order",
	"order",
	"order_line",
	"stock",
	"warehouse",
}

func registerScrubIndexOnlyTPCC(r *registry) {
	// TODO (lucy): increase numScrubRuns once we improve SCRUB performance
	r.Add(makeScrubTPCCTest(5, 1000, time.Hour*3, true, 2))
}

func registerScrubAllChecksTPCC(r *registry) {
	// TODO (lucy): increase numScrubRuns once we improve SCRUB performance
	r.Add(makeScrubTPCCTest(5, 1000, time.Hour*3, false, 1))
}

func makeScrubTPCCTest(
	numNodes, warehouses int, length time.Duration, indexOnly bool, numScrubRuns int,
) testSpec {
	var optionName string
	if indexOnly {
		optionName = "index-only"
	} else {
		optionName = "all-checks"
	}

	stmts := make([]string, numScrubRuns*len(tpccTables))
	for i := 0; i < len(stmts); i += len(tpccTables) {
		for j, t := range tpccTables {
			if indexOnly {
				stmts[i+j] = fmt.Sprintf(`EXPERIMENTAL SCRUB TABLE tpcc.%s AS OF SYSTEM TIME '-0s' WITH OPTIONS INDEX ALL`, t)
			} else {
				stmts[i+j] = fmt.Sprintf(`EXPERIMENTAL SCRUB TABLE tpcc.%s AS OF SYSTEM TIME '-0s'`, t)
			}
		}
	}

	return testSpec{
		Name:  fmt.Sprintf("scrub/%s/tpcc-%d", optionName, warehouses),
		Nodes: nodes(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses: warehouses,
				Extra:      "--wait=false --tolerate-errors",
				During: func(ctx context.Context) error {
					return runAndLogStmts(ctx, t, c, "scrub", stmts)
				},
				Duration: length,
			})
		},
	}
}
