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
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func registerClearRange(r *registry) {
	r.Add(testSpec{
		Name:       `clearrange`,
		MinVersion: `v2.1.0`,
		Nodes:      nodes(10),
		Stable:     false,
		Run: func(ctx context.Context, t *test, c *cluster) {
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
			c.Start(ctx)

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

			m := newMonitor(ctx, c)
			m.Go(func(ctx context.Context) error {
				conn := c.Conn(ctx, 1)
				defer conn.Close()

				if _, err := conn.ExecContext(ctx, `SET CLUSTER SETTING kv.range_merge.queue_enabled = true`); err != nil {
					return err
				}

				t.WorkerStatus("dropping table")
				defer t.WorkerStatus()

				// Set a low TTL so that the ClearRange-based cleanup mechanism can kick in earlier.
				// This could also be done after dropping the table.
				if _, err := conn.ExecContext(ctx, `ALTER TABLE bank.bank EXPERIMENTAL CONFIGURE ZONE 'gc: {ttlseconds: 30}'`); err != nil {
					return err
				}

				if _, err := conn.ExecContext(ctx, `DROP TABLE bank.bank`); err != nil {
					return err
				}

				// Spend a few minutes reading data with a timeout to make sure the
				// DROP above didn't brick the cluster. At the time of writing,
				// clearing all of the table data takes ~6min. We run for 2.5x that
				// time to verify that nothing has gone wonky on the cluster.
				//
				// Don't lower this number, or the test may pass erroneously.
				const minutes = 45
				t.WorkerStatus("repeatedly running count(*) on small table")
				for i := 0; i < minutes; i++ {
					after := time.After(time.Minute)
					var count int
					// NB: context cancellation in QueryRowContext does not work as expected.
					// See #25435.
					if _, err := conn.ExecContext(ctx, `SET statement_timeout = '10s'`); err != nil {
						return err
					}
					// If we can't aggregate over 80kb in 10s, the database is far from usable.
					start := timeutil.Now()
					if err := conn.QueryRowContext(ctx, `SELECT count(*) FROM tinybank.bank`).Scan(&count); err != nil {
						return err
					}
					c.l.Printf("read %d rows in %0.1fs\n", count, timeutil.Since(start).Seconds())
					t.WorkerProgress(float64(i+1) / float64(minutes))
					select {
					case <-after:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				// TODO(benesch): verify that every last range in the table has been
				// merged away. For now, just exercising the merge code is a good start.
				return nil
			})
			m.Wait()
		},
	})
}
