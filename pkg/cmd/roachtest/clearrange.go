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

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func registerClearRange(r *registry) {
	for _, checks := range []bool{true, false} {
		r.Add(testSpec{
			Name:       fmt.Sprintf(`clearrange/checks=%t`, checks),
			Timeout:    90 * time.Minute,
			MinVersion: `v2.1.0`,
			Nodes:      nodes(10),
			Stable:     true, // DO NOT COPY to new tests
			Run: func(ctx context.Context, t *test, c *cluster) {
				runClearRange(ctx, t, c, checks)
			},
		})
	}
}

func runClearRange(ctx context.Context, t *test, c *cluster, aggressiveChecks bool) {
	// Created via:
	// roachtest --cockroach cockroach-v2.0-8 store-gen --stores=10 bank \
	//           --payload-bytes=10240 --ranges=0 --rows=65104166
	if err := c.RunE(ctx, c.Node(1), "test -d /mnt/data1/.zfs/snapshot/pristine"); err != nil {
		// Use ZFS so the initial store dumps can be instantly rolled back to
		// their pristine state. Useful for iterating quickly on the test.
		c.Reformat(ctx, c.All(), "zfs")

		t.Status(`downloading store dumps`)
		fixtureURL := `gs://cockroach-fixtures/workload/bank/version=1.0.0,payload-bytes=10240,ranges=0,rows=65104166,seed=4`
		location := storeDirURL(fixtureURL, c.nodes, "2.0-8")

		// Download this store dump, which measures around 2TB (across all nodes).
		if err := downloadStoreDumps(ctx, c, location, c.nodes); err != nil {
			t.Fatal(err)
		}
		c.Run(ctx, c.All(), "test -e /sbin/zfs && sudo zfs snapshot data1@pristine")
	} else {
		t.Status(`restoring store dumps`)
		c.Run(ctx, c.All(), "sudo zfs rollback data1@pristine")
	}

	c.Put(ctx, cockroach, "./cockroach")
	if aggressiveChecks {
		// Run with an env var that runs a synchronous consistency check after each rebalance and merge.
		// This slows down merges, so it might hide some races.
		c.Start(ctx, t, startArgs(
			"--env=COCKROACH_CONSISTENCY_AGGRESSIVE=true",
			"--env=COCKROACH_FATAL_ON_STATS_MISMATCH=true",
		))
	} else {
		c.Start(ctx, t)
	}

	// Also restore a much smaller table. We'll use it to run queries against
	// the cluster after having dropped the large table above, verifying that
	// the  cluster still works.
	t.Status(`restoring tiny table`)
	defer t.WorkerStatus()

	c.Run(ctx, c.Node(1), `./cockroach sql --insecure -e "CREATE DATABASE tinybank"`)
	c.Run(ctx, c.Node(1), `./cockroach sql --insecure -e "
				RESTORE csv.bank FROM
        'gs://cockroach-fixtures/workload/bank/version=1.0.0,payload-bytes=100,ranges=10,rows=800,seed=1/bank'
				WITH into_db = 'tinybank'"`)

	t.Status()

	// Set up a convenience function that we can call to learn the number of
	// ranges for the bank.bank table (even after it's been dropped).
	numBankRanges := func() func() int {
		conn := c.Conn(ctx, 1)
		defer conn.Close()

		var startHex string
		// NB: set this to false to save yourself some time during development. Selecting
		// from crdb_internal.ranges is very slow because it contacts all of the leaseholders.
		// You may actually want to run a version of cockroach that doesn't do that because
		// it'll still slow you down every time the method returned below is called.
		if true {
			if err := conn.QueryRow(
				`SELECT to_hex(start_key) FROM crdb_internal.ranges WHERE database_name = 'bank' AND table_name = 'bank' ORDER BY start_key ASC LIMIT 1`,
			).Scan(&startHex); err != nil {
				t.Fatal(err)
			}
		} else {
			startHex = "bd" // extremely likely to be the right thing (b'\275').
		}
		return func() int {
			conn := c.Conn(ctx, 1)
			defer conn.Close()
			var n int
			if err := conn.QueryRow(
				`SELECT count(*) FROM crdb_internal.ranges WHERE substr(to_hex(start_key), 1, length($1::string)) = $1`, startHex,
			).Scan(&n); err != nil {
				t.Fatal(err)
			}
			return n
		}
	}()

	m := newMonitor(ctx, c)
	m.Go(func(ctx context.Context) error {
		conn := c.Conn(ctx, 1)
		defer conn.Close()

		if _, err := conn.ExecContext(ctx, `SET CLUSTER SETTING kv.range_merge.queue_enabled = true`); err != nil {
			return err
		}

		// Merge as fast as possible to put maximum stress on the system.
		if _, err := conn.ExecContext(ctx, `SET CLUSTER SETTING kv.range_merge.queue_interval = '0s'`); err != nil {
			return err
		}

		t.WorkerStatus("dropping table")
		defer t.WorkerStatus()

		// Set a low TTL so that the ClearRange-based cleanup mechanism can kick in earlier.
		// This could also be done after dropping the table.
		if _, err := conn.ExecContext(ctx, `ALTER TABLE bank.bank CONFIGURE ZONE USING gc.ttlseconds = 30`); err != nil {
			return err
		}

		t.WorkerStatus("computing number of ranges")
		initialBankRanges := numBankRanges()

		t.WorkerStatus("dropping bank table")
		if _, err := conn.ExecContext(ctx, `DROP TABLE bank.bank`); err != nil {
			return err
		}

		// Spend some time reading data with a timeout to make sure the
		// DROP above didn't brick the cluster. At the time of writing,
		// clearing all of the table data takes ~6min, so we want to run
		// for at least a multiple of that duration.
		const minDuration = 45 * time.Minute
		deadline := timeutil.Now().Add(minDuration)
		curBankRanges := numBankRanges()
		t.WorkerStatus("waiting for ~", curBankRanges, " merges to complete (and for at least ", minDuration, " to pass)")
		for timeutil.Now().Before(deadline) || curBankRanges > 1 {
			after := time.After(5 * time.Minute)
			curBankRanges = numBankRanges() // this call takes minutes, unfortunately
			t.WorkerProgress(1 - float64(curBankRanges)/float64(initialBankRanges))

			var count int
			// NB: context cancellation in QueryRowContext does not work as expected.
			// See #25435.
			if _, err := conn.ExecContext(ctx, `SET statement_timeout = '5s'`); err != nil {
				return err
			}
			// If we can't aggregate over 80kb in 5s, the database is far from usable.
			if err := conn.QueryRowContext(ctx, `SELECT count(*) FROM tinybank.bank`).Scan(&count); err != nil {
				return err
			}

			select {
			case <-after:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		// TODO(tschottdorf): verify that disk space usage drops below to <some small amount>, but that
		// may not actually happen (see https://github.com/cockroachdb/cockroach/issues/29290).
		return nil
	})
	m.Wait()
}
