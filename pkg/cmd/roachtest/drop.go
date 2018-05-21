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

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	_ "github.com/lib/pq"
)

func registerDrop(r *registry) {
	// TODO(tschottdorf): rearrange all tests so that their synopses are available
	// via godoc and (some variation on) `roachtest run <testname> --help`.

	// This test imports a TPCC dataset and then issues a manual deletion followed
	// by a truncation for the `stock` table (which contains warehouses*100k
	// rows). Next, it issues a `DROP` for the whole database, and sets the GC TTL
	// to one second.
	runDrop := func(ctx context.Context, t *test, c *cluster, warehouses, nodes int, initDiskSpace int) {
		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Put(ctx, workload, "./workload", c.Range(1, nodes))
		c.Start(ctx, c.Range(1, nodes), startArgs("-e", "COCKROACH_MEMPROF_INTERVAL=15s"))

		m := newMonitor(ctx, c, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			t.WorkerStatus("importing TPCC fixture")
			c.Run(ctx, c.Node(1), fmt.Sprintf(
				"./workload fixtures load tpcc --warehouses=%d --db tpcc {pgurl:1}", warehouses))

			// Don't open the DB connection until after the data has been imported.
			// Otherwise the ALTER TABLE query below might fail to find the
			// tpcc.order_line table that we just imported (!) due to what seems to
			// be a problem with table descriptor leases (#24374).
			db := c.Conn(ctx, 1)
			defer db.Close()

			run := func(stmt string, args ...interface{}) {
				t.WorkerStatus(stmt)
				_, err := db.ExecContext(ctx, stmt, args...)
				if err != nil {
					t.Fatal(err)
				}
			}

			run(`SET CLUSTER SETTING trace.debug.enable = true`)

			// Drop a constraint that would get in the way of deleting from tpcc.stock.
			const stmtDropConstraint = "ALTER TABLE tpcc.order_line DROP CONSTRAINT fk_ol_supply_w_id_ref_stock"
			run(stmtDropConstraint)

			var rows, minWarehouse, maxWarehouse int
			if err := db.QueryRow("select count(*), min(s_w_id), max(s_w_id) from tpcc.stock").Scan(&rows,
				&minWarehouse, &maxWarehouse); err != nil {
				t.Fatalf("failed to get range count: %v", err)
			}

			for j := 1; j <= nodes; j++ {
				size, err := getDiskUsageInByte(ctx, c, j)
				if err != nil {
					return err
				}

				c.l.printf("Node %d space used: %s\n", j, humanizeutil.IBytes(int64(size)))

				// Return if the size of the directory is less than 100mb
				if size < initDiskSpace {
					t.Fatalf("Node %d space used: %s less than %s", j, humanizeutil.IBytes(int64(size)),
						humanizeutil.IBytes(int64(initDiskSpace)))
				}
			}

			for i := minWarehouse; i <= maxWarehouse; i++ {
				t.Progress(float64(i) / float64(maxWarehouse))
				run("DELETE FROM tpcc.stock WHERE s_w_id = $1", i)
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

			var allNodesSpaceCleared bool
			var sizeReport string
			maxSizeBytes := 100 * 1024 * 1024
			// We're waiting a maximum of 10 minutes to makes sure that the drop operations clear the disk.
			for i := 0; i < 10; i++ {
				sizeReport = ""
				allNodesSpaceCleared = true
				for j := 1; j <= nodes; j++ {
					size, err := getDiskUsageInByte(ctx, c, j)
					if err != nil {
						return err
					}

					nodeSpaceUsed := fmt.Sprintf("Node %d space after deletion used: %s\n", j, humanizeutil.IBytes(int64(size)))
					c.l.printf(nodeSpaceUsed)

					// Return if the size of the directory is less than 100mb
					if size > maxSizeBytes {
						allNodesSpaceCleared = false
						sizeReport += nodeSpaceUsed
					}
				}

				if allNodesSpaceCleared {
					break
				}
				time.Sleep(time.Minute)
			}

			if !allNodesSpaceCleared {
				sizeReport += fmt.Sprintf("disk space usage has not dropped below %s on all nodes.",
					humanizeutil.IBytes(int64(maxSizeBytes)))
				t.Fatalf(sizeReport)
			}

			return nil
		})
		m.Wait()
	}

	warehouses := 100
	numNodes := 9

	// 1GB
	initDiskSpace := int(1E9)

	r.Add(testSpec{
		Name:  fmt.Sprintf("drop/tpcc/w=%d,nodes=%d", warehouses, numNodes),
		Nodes: nodes(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			// NB: this is likely not going to work out in `-local` mode. Edit the
			// numbers during iteration.
			if local {
				numNodes = 4
				warehouses = 1

				// 100 MB
				initDiskSpace = 1E8
				fmt.Printf("running with w=%d,nodes=%d in local mode\n", warehouses, numNodes)
			}
			runDrop(ctx, t, c, warehouses, numNodes, initDiskSpace)
		},
	})
}
