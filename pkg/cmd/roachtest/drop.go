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
)

func init() {
	// TODO(tschottdorf): rearrange all tests so that their synopses are available
	// via godoc and (some variation on) `roachtest run <testname> --help`.

	// This test imports a TPCC dataset and then issues a manual deletion followed
	// by a truncation for the `stock` table (which contains warehouses*100k
	// rows). Next, it issues a `DROP` for the whole database, and sets the GC TTL
	// to one second.
	runDrop := func(ctx context.Context, t *test, c *cluster, warehouses int) {
		c.Put(ctx, cockroach, "./cockroach", c.All())
		c.Put(ctx, workload, "./workload", c.All())
		c.Start(ctx, c.All(), startArgs("-e", "COCKROACH_MEMPROF_INTERVAL=15s"))

		m := newMonitor(ctx, c, c.All())
		m.Go(func(ctx context.Context) error {
			run := func(stmt string) {
				t.Status(stmt)
				c.Run(ctx, 1, `./cockroach sql --insecure -e "`+stmt+`"`)
			}

			run(`SET CLUSTER SETTING trace.debug.enable = true`)

			t.Status("importing TPCC fixture")
			c.Run(ctx, 1, fmt.Sprintf(
				"./workload fixtures load tpcc --warehouses=%d --db tpcc {pgurl:1}", warehouses))

			const stmtDelete = "DELETE FROM tpcc.stock"
			run(stmtDelete)

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

			// TODO(tschottdorf): assert that the disk usage drops to "near nothing".
			return nil
		})
		m.Wait()
	}

	warehouses := 100

	tests.Add(testSpec{
		Name:  fmt.Sprintf("drop/tpcc/w=%d,nodes=9", warehouses),
		Nodes: nodes(9),
		Run: func(ctx context.Context, t *test, c *cluster) {
			// NB: this is likely not going to work out in `-local` mode. Edit the
			// numbers during iteration.
			if local {
				warehouses = 1
				fmt.Printf("running with w=%d,nodes=%d in local mode\n", warehouses, c.nodes)
			}
			runDrop(ctx, t, c, warehouses)
		},
	})
}
