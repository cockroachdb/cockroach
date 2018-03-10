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

	"github.com/cockroachdb/cockroach/pkg/util/retry"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

func init() {
	// This test generates a large Bank table all within a single range. It does
	// so by setting the max range size to a huge number before populating the
	// table. It then drops the range size back down to normal and watches as
	// the large range splits apart.
	runSplit := func(t *test, rows, nodes int) {
		// payload is the size of the payload column for each row in the Bank
		// table.
		const payload = 100
		// rowOverheadEstimate is an estimate of the overhead of a single
		// row in the Bank table, not including the size of the payload
		// itself. This overhead includes the size of the other two columns
		// in the table along with the size of each row's associated KV key.
		const rowOverheadEstimate = 160
		const rowEstimate = rowOverheadEstimate + payload

		ctx := context.Background()
		c := newCluster(ctx, t, nodes)
		defer c.Destroy(ctx)

		c.Put(ctx, cockroach, "./cockroach", c.All())
		c.Put(ctx, workload, "./workload", c.All())
		c.Start(ctx, c.All())

		m := newMonitor(ctx, c, c.All())
		m.Go(func(ctx context.Context) error {
			t.Status("increasing range_max_bytes")
			setRangeMaxBytes := func(maxBytes int) {
				c.Run(ctx, 1, fmt.Sprintf(
					"echo 'range_max_bytes: %d' | ./cockroach zone set .default --insecure -f -",
					maxBytes))
			}
			// Set the range size to double what we expect the size of the
			// bank table to be. This should result in the table fitting
			// inside a single range.
			setRangeMaxBytes(2 * rows * rowEstimate)

			t.Status("populating bank table")
			c.Run(ctx, 1, fmt.Sprintf("./workload init bank --rows=%d --ranges=1 {pgurl:1}", rows))

			t.Status("checking for single range")
			rangeCount := func() int {
				db := c.Conn(ctx, 1)
				defer db.Close()

				var count int
				const q = "SELECT COUNT(*) FROM [SHOW TESTING_RANGES FROM TABLE bank.bank]"
				if err := db.QueryRow(q).Scan(&count); err != nil {
					t.Fatalf("failed to get range count: %v", err)
				}
				return count
			}
			if rc := rangeCount(); rc != 1 {
				return errors.Errorf("bank table split over multiple ranges")
			}

			t.Status("decreasing range_max_bytes")
			rangeSize := 64 << 20 // 64MB
			setRangeMaxBytes(rangeSize)

			expRC := (rows * rowEstimate) / rangeSize
			expSplits := expRC - 1
			t.Status(fmt.Sprintf("waiting for %d splits", expSplits))
			waitDuration := time.Duration(expSplits) * time.Second // 1 second per split
			return retry.ForDuration(waitDuration, func() error {
				if rc := rangeCount(); rc > expRC {
					return errors.Errorf("bank table split over %d ranges, expected at least %d",
						rc, expRC)
				}
				return nil
			})
		})
		m.Wait()
	}

	rows := int(1E7)
	nodes := 9

	tests.Add(fmt.Sprintf("splits/bank/rows=%d,nodes=%d", rows, nodes),
		func(t *test) {
			runSplit(t, rows, nodes)
		})
}
