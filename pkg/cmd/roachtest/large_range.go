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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/dustin/go-humanize"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

func init() {
	const size = 10 << 30 // 10 GB
	// TODO(nvanbenschoten): Snapshots currently hold the entirety of a range in
	// memory on the receiving side. This is dangerous when we grow a range to
	// such large sizes because it means that a snapshot could trigger an OOM.
	// Because of this, we stick to 3 nodes to avoid rebalancing-related
	// snapshots. Once #16954 is addressed, we can increase this count so that
	// splitting the single large range also triggers rebalancing.
	const numNodes = 3

	tests.Add(testSpec{
		Name:  fmt.Sprintf("largerange/splits/size=%s,nodes=%d", bytesStr(size), numNodes),
		Nodes: nodes(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runLargeRangeSplits(ctx, t, c, size)
		},
	})
}

func bytesStr(size uint64) string {
	return strings.Replace(humanize.IBytes(size), " ", "", -1)
}

// This test generates a large Bank table all within a single range. It does
// so by setting the max range size to a huge number before populating the
// table. It then drops the range size back down to normal and watches as
// the large range splits apart.
func runLargeRangeSplits(ctx context.Context, t *test, c *cluster, size int) {
	// payload is the size of the payload column for each row in the Bank
	// table.
	const payload = 100
	// rowOverheadEstimate is an estimate of the overhead of a single
	// row in the Bank table, not including the size of the payload
	// itself. This overhead includes the size of the other two columns
	// in the table along with the size of each row's associated KV key.
	const rowOverheadEstimate = 160
	const rowEstimate = rowOverheadEstimate + payload
	// rows is the number of rows we'll need to insert into the bank table
	// to produce a range of roughly the right size.
	rows := size / rowEstimate

	c.Put(ctx, cockroach, "./cockroach", c.All())
	c.Put(ctx, workload, "./workload", c.All())
	c.Start(ctx, c.All())

	m := newMonitor(ctx, c, c.All())
	m.Go(func(ctx context.Context) error {
		db := c.Conn(ctx, 1)
		defer db.Close()

		t.Status("increasing range_max_bytes")
		setRangeMaxBytes := func(maxBytes int) {
			stmtZone := fmt.Sprintf(`ALTER RANGE default EXPERIMENTAL CONFIGURE ZONE '
range_max_bytes: %d
'`, maxBytes)
			if _, err := db.Exec(stmtZone); err != nil {
				t.Fatalf("failed to set range_max_bytes: %v", err)
			}
		}
		// Set the range size to double what we expect the size of the
		// bank table to be. This should result in the table fitting
		// inside a single range.
		setRangeMaxBytes(2 * size)

		t.Status("populating bank table")
		// NB: workload init does not wait for upreplication after creating the
		// schema but before populating it. This is ok because upreplication
		// occurs much faster than we can actually create a large range.
		c.Run(ctx, c.Node(1), fmt.Sprintf("./workload init bank "+
			"--rows=%d --payload-bytes=%d --ranges=1 {pgurl:1-%d}", rows, payload, c.nodes))

		t.Status("checking for single range")
		rangeCount := func() int {
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

		expRC := size / rangeSize
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
