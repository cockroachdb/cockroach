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

	"github.com/pkg/errors"
)

func registerClearRange(r *registry) {
	r.Add(testSpec{
		Name:  `clearrange`,
		Nodes: nodes(10),
		// At the time of writing, #24029 is still open and this test does indeed
		// thoroughly brick the cluster.
		Stable: false,
		Run: func(ctx context.Context, t *test, c *cluster) {
			t.Status(`downloading store dumps`)
			// Created via:
			// roachtest --cockroach cockroach-v2.0.1 store-gen --stores=10 bank \
			//           --payload-bytes=10240 --ranges=0 --rows=65104166
			location := `gs://cockroach-fixtures/workload/bank/` + `version=1.0.0,payload-bytes=10240,ranges=0,rows=65104166,seed=1`
			if err := downloadStoreDumps(ctx, c, location, "2.0", c.nodes); err != nil {
				t.Fatal(err)
			}

			c.Put(ctx, cockroach, "./cockroach")
			c.Start(ctx)

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

				t.WorkerStatus("dropping table")
				defer t.WorkerStatus()

				if _, err := conn.ExecContext(ctx, `SET CLUSTER SETTING server.consistency_check.interval = '0s'`); err != nil {
					return err
				}

				if _, err := conn.ExecContext(ctx, `ALTER TABLE bank.bank EXPERIMENTAL CONFIGURE ZONE 'gc: {ttlseconds: 30}'`); err != nil {
					return err
				}

				if _, err := conn.ExecContext(ctx, `DROP TABLE bank.bank`); err != nil {
					return err
				}

				// Spend a few minutes reading data to make sure the DROP above didn't brick the cluster.
				// Don't lower this number, or the test may pass erroneously.
				//
				// TODO(tschottdorf): could use kv workload with a min rate instead.
				const minutes = 20
				t.WorkerStatus("repeatedly running COUNT(*) on small table")
				for i := 0; i < minutes; i++ {
					after := time.After(time.Minute)
					var count int
					if err := func() error {
						// If we can't aggregate over 80kb in 10s, the database is far from usable.
						ctx10s, cancel := context.WithTimeout(ctx, 10*time.Second)
						defer cancel()
						const q = `SELECT COUNT(*) FROM tinybank.bank`
						if err := conn.QueryRowContext(ctx10s, q).Scan(&count); err != nil {
							return errors.Wrap(err, q)
						}
						c.l.printf("read %d rows\n", count)
						// See https://github.com/cockroachdb/cockroach/issues/25435#issuecomment-391325495.
						// The query above will basically never fail on its own at the time of writing.
						return errors.Wrap(ctx.Err(), q)
					}(); err != nil {
						return err
					}
					t.WorkerProgress(float64(i+1) / float64(minutes))
					select {
					case <-after:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			})
			m.Wait()
		},
	})
}
