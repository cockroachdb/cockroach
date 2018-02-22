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
	runDrop := func(t *test, warehouses, nodes int) {
		ctx := context.Background()
		c := newCluster(ctx, t, nodes)
		defer c.Destroy(ctx)

		c.Put(ctx, cockroach, "./cockroach", c.All())
		c.Put(ctx, workload, "./workload", c.All())
		c.Start(ctx, c.All())

		t.Status("importing TPCC fixture")
		m := newMonitor(ctx, c, c.All())
		m.Go(func(ctx context.Context) error {
			cmd := fmt.Sprintf(
				"./workload fixtures load tpcc --warehouses=%d --into-db tpcc {pgurl:1}", warehouses)
			c.Run(ctx, 1, cmd)

			run := func(stmt string) {
				t.Status(stmt)
				c.Run(ctx, 1, `./cockroach sql --insecure -e "`+stmt+`"`)
			}

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
	nodes := 9

	tests.Add(
		fmt.Sprintf("drop/tpcc/w=%d,nodes=%d", warehouses, nodes),
		func(t *test) {
			// NB: this is likely not going to work out in `-local` mode. Edit the
			// numbers during iteration.
			if local {
				nodes = 4
				warehouses = 1
				fmt.Printf("running with w=%d,nodes=%d in local mode\n", warehouses, nodes)
			}
			runDrop(t, warehouses, nodes)
		})
}
