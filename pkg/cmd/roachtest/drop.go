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

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	_ "github.com/lib/pq"
	"strconv"
	"strings"
	"time"
)

func init() {
	// TODO(tschottdorf): rearrange all tests so that their synopses are available
	// via godoc and (some variation on) `roachtest run <testname> --help`.

	// This test imports a TPCC dataset and then issues a manual deletion followed
	// by a truncation for the `stock` table (which contains warehouses*100k
	// rows). Next, it issues a `DROP` for the whole database, and sets the GC TTL
	// to one second.
	runDrop := func(ctx context.Context, t *test, c *cluster, warehouses, nodes int) {
		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Put(ctx, workload, "./workload", c.Range(1, nodes))
		c.Start(ctx, c.Range(1, nodes), startArgs("-e", "COCKROACH_MEMPROF_INTERVAL=15s"))

		db := c.Conn(ctx, 1)
		defer db.Close()

		m := newMonitor(ctx, c, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			run := func(stmt string) {
				t.Status(stmt)
				_, err := db.ExecContext(ctx, stmt)
				if err != nil {
					t.Fatal(err)
				}
			}

			run(`SET CLUSTER SETTING trace.debug.enable = true`)

			t.Status("importing TPCC fixture")
			c.Run(ctx, 1, fmt.Sprintf(
				"./workload fixtures load tpcc --warehouses=%d --db tpcc {pgurl:1}", warehouses))

			// Drop a constraint that would get in the way of deleting from tpcc.stock.
			const stmtDropConstraint = "ALTER TABLE tpcc.order_line DROP CONSTRAINT fk_ol_supply_w_id_ref_stock"
			run(stmtDropConstraint)

			var minWarehouse int
			if err := db.QueryRow("select min(s_w_id) from tpcc.stock").Scan(&minWarehouse); err != nil {
				t.Fatalf("failed to get range count: %v", err)
			}

			var maxWarehouse int
			if err := db.QueryRow("select max(s_w_id) from tpcc.stock").Scan(&maxWarehouse); err != nil {
				t.Fatalf("failed to get range count: %v", err)
			}

			var rows int
			if err := db.QueryRow("select count(*) from tpcc.stock").Scan(&rows); err != nil {
				t.Fatalf("failed to get row count: %v", err)
			}

			deleted := int64(0)

			for i := minWarehouse; i <= maxWarehouse; i++ {
				t.Progress(float64(deleted) / float64(rows))
				res, err := db.ExecContext(ctx,
					"DELETE FROM tpcc.stock WHERE s_w_id = $1", i)
				if err != nil {
					return err
				}

				rowsDeleted, err := res.RowsAffected()
				if err != nil {
					return err
				}

				deleted += rowsDeleted
			}

			const stmtTruncate = "TRUNCATE TABLE tpcc.stock"
			run(stmtTruncate)

			const stmtDrop = "DROP DATABASE tpcc"
			run(stmtDrop)
			// The data has already been deleted, but changing the default zone config
			// should take effect retroactively.
			const stmtZone = `ALTER RANGE default EXPERIMENTAL CONFIGURE ZONE '
gc:
  ttlseconds: 1
'`
			run(stmtZone)

			// We're waiting a maximum of 5 minutes to makes sure that the drop operations clear the disk.
			for i := 0; i < 5; i++ {
				allNodesSpaceCleared := true
				for j := 1; j <= nodes; j++ {
					out, err := c.RunWithBuffer(ctx, c.l, j, fmt.Sprintf("du -sc {store-dir:%d}", j))
					if err != nil {
						return err
					}

					str := string(out)

					// We need this check because sometimes the first line of the roachprod output is a warning
					// about adding an ip to a list of known hosts.
					if strings.Contains(str, "Warning") {
						str = strings.Split(str, "\n")[1]
					}
					result := strings.Split(str, "/")

					size, err := strconv.Atoi(strings.TrimSpace(result[0]))
					if err != nil {
						return err
					}

					c.l.printf("Node %d space used: %s\n", j, humanizeutil.IBytes(int64(size)))

					// Return if the size of the directory is less than 100mb
					if size > 10000000 {
						allNodesSpaceCleared = allNodesSpaceCleared && false
					}
				}

				if allNodesSpaceCleared {
					break
				}
				time.Sleep(time.Minute)
			}

			return nil
		})
		m.Wait()
	}

	warehouses := 100
	numNodes := 9

	tests.Add(testSpec{
		Name:  fmt.Sprintf("drop/tpcc/w=%d,nodes=%d", warehouses, numNodes),
		Nodes: nodes(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			// NB: this is likely not going to work out in `-local` mode. Edit the
			// numbers during iteration.
			if local {
				numNodes = 4
				warehouses = 1
				fmt.Printf("running with w=%d,nodes=%d in local mode\n", warehouses, numNodes)
			}
			runDrop(ctx, t, c, warehouses, numNodes)
		},
	})
}
