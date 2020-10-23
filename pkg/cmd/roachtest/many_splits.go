// Copyright 2020 The Cockroach Authors.
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
)

// runManySplits attempts to create 2000 tiny ranges on a 4-node cluster using
// left-to-right splits and check the cluster is still live afterwards.
func runManySplits(ctx context.Context, t *test, c *cluster) {
	args := startArgs("--env=COCKROACH_SCAN_MAX_IDLE_TIME=5ms")
	c.Put(ctx, cockroach, "./cockroach")
	c.Start(ctx, t, args)

	db := c.Conn(ctx, 1)
	defer db.Close()

	// Wait for upreplication then create many ranges.
	waitForFullReplication(t, db)

	m := newMonitor(ctx, c, c.All())
	m.Go(func(ctx context.Context) error {
		const numRanges = 2000
		t.l.Printf("creating %d ranges...", numRanges)
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`
			CREATE TABLE t(x, PRIMARY KEY(x)) AS TABLE generate_series(1,%[1]d);
            ALTER TABLE t SPLIT AT TABLE generate_series(1,%[1]d);
		`, numRanges)); err != nil {
			return err
		}
		return nil
	})
	m.Wait()
}
