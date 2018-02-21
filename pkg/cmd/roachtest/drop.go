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
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

func init() {
	// TODO(tschottdorf): rearrange all tests so that their synopses are available
	// via godoc and (some variation on) `roachtest run <testname> --help`.

	// This test imports a TPCC dataset and then issues a manual deletion for the
	// `stock` table (which contains warehouses*100k rows). Next, it issues a
	// `DROP` for the whole database, and sets the GC TTL to one second.
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

			// TODO(tschottdorf): this is awkward: pgurl interpolation sits inside
			// `roachprod`, but we just want to connect to a node and run a SQL command.
			// Need to put an `Expand` method on `*cluster` as well that in turn calls
			// into one exposed on `roachprod` and returns the result. In the meantime,
			// hack around it and just run this on the first node making assumptions
			// about the URL. As a consequence, this only works with `-local` right now.
			db, err := sql.Open("postgres", "postgresql://root@localhost:26257/tpcc?sslmode=disable")
			if err != nil {
				return err
			}
			defer db.Close()

			const stmtDelete = "DELETE FROM tpcc.stock"
			t.Status(stmtDelete)
			if _, err := db.ExecContext(ctx, stmtDelete); err != nil {
				return err
			}

			const stmtDrop = "DROP DATABASE tpcc"
			if _, err := db.ExecContext(ctx, stmtDrop); err != nil {
				return err
			}

			// The data has already been deleted, but changing the default zone config
			// should take effect retroactively.
			const stmtZone = `ALTER RANGE default EXPERIMENTAL CONFIGURE ZONE '
gc:
  ttlseconds: 1
'`
			t.Status(stmtZone)
			if _, err := db.ExecContext(ctx, stmtZone); err != nil {
				return err
			}

			// TODO(tschottdorf): assert that the disk usage drops to "near nothing".
			return nil
		})
		m.Wait()
	}

	tests.Add("drop/tpcc/w=100/nodes=9", func(t *test) {
		// NB: this is likely not going to work out in `-local` mode. Edit the
		// numbers during iteration.
		runDrop(t, 1, 3) // TODO: w=100, nodes=9
	})
}
