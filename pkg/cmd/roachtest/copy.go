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

	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

func init() {
	// This test imports a fully-populated Bank table. It then creates an empty
	// Bank schema. Finally, it performs a series of `INSERT ... FROM SELECT ...`
	// statements to copy all data from the first table into the second table.
	runCopy := func(t *test, rows, nodes int) {
		const payload = 100

		ctx := context.Background()
		c := newCluster(ctx, t, nodes)
		defer c.Destroy(ctx)

		c.Put(ctx, cockroach, "./cockroach", c.All())
		c.Put(ctx, workload, "./workload", c.All())
		c.Start(ctx, c.All())

		m := newMonitor(ctx, c, c.All())
		m.Go(func(ctx context.Context) error {
			run := func(stmt string) {
				t.Status(stmt)
				c.Run(ctx, 1, `./cockroach sql --insecure -e "`+stmt+`"`)
			}

			t.Status("importing Bank fixture")
			c.Run(ctx, 1, fmt.Sprintf(
				"./workload fixtures load bank --rows=%d --payload-bytes=%d {pgurl:1}",
				rows, payload))
			run("ALTER TABLE bank.bank RENAME TO bank.bank_orig")

			t.Status("create copy of Bank schema")
			c.Run(ctx, 1, "./workload init bank --rows=0 --ranges=1 {pgurl:1}")

			db := c.Conn(ctx, 1)
			defer db.Close()
			rangeCount := func() int {
				var count int
				const q = "SELECT COUNT(*) FROM [SHOW TESTING_RANGES FROM TABLE bank.bank]"
				if err := db.QueryRow(q).Scan(&count); err != nil {
					t.Fatalf("failed to get range count: %v", err)
				}
				return count
			}
			if rc := rangeCount(); rc != 1 {
				return errors.Errorf("empty bank table split over multiple ranges")
			}

			// Copy batches of rows from bank_orig to bank. Each batch needs to
			// be under kv.raft.command.max_size=64MB or we'll hit a "command is
			// too large" error. We play it safe and chose batches whose rows
			// add up to about a quarter of this limit.
			rowsPerInsert := (16 << 20 /* 16MB */) / payload
			for lastID := -1; lastID+1 < rows; {
				var stat string
				if lastID > 0 {
					stat = fmt.Sprintf(": %f%% complete", 100*float64(lastID)/float64(rows))
				}
				t.Status("copying from bank_orig to bank" + stat)
				q := fmt.Sprintf(`
					SELECT id FROM [
						INSERT INTO bank.bank
						SELECT * FROM bank.bank_orig
						WHERE id > %d
						ORDER BY id ASC
						LIMIT %d
						RETURNING ID
					]
					ORDER BY id DESC
					LIMIT 1`,
					lastID, rowsPerInsert)
				if err := db.QueryRow(q).Scan(&lastID); err != nil {
					t.Fatalf("failed to copy rows: %v", err)
				}
			}

			// TODO(nvanbenschoten): should we assert that the number of ranges
			// in the new table is above some threshold?
			c.l.printf("range count after copy = %d\n", rangeCount())
			return nil
		})
		m.Wait()
	}

	rows := 10000000
	nodes := 9

	tests.Add(
		fmt.Sprintf("copy/bank/rows=%d,nodes=%d", rows, nodes),
		func(t *test) {
			if local {
				rows = 1000000
				nodes = 4
				fmt.Printf("running with nodes=%d in local mode\n", nodes)
			}
			runCopy(t, rows, nodes)
		})
}
