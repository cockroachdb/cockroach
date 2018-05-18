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
	"bytes"
	"context"
	"fmt"
	"strings"

	"golang.org/x/sync/errgroup"
)

var stores = 3

// registerStoreGen registers a store generation "test" that powers the
// 'roachtest store-gen' subcommand.
func registerStoreGen(r *registry, args []string) {
	workloadArgs := strings.Join(args, " ")

	r.Add(testSpec{
		Name:  "store-gen",
		Nodes: nodes(stores),
		Run: func(ctx context.Context, t *test, c *cluster) {
			c.Put(ctx, cockroach, "./cockroach")
			c.Put(ctx, workload, "./workload")
			c.Start(ctx)

			{
				m := newMonitor(ctx, c)
				m.Go(func(ctx context.Context) error {
					t.WorkerStatus("restoring fixture")
					defer t.WorkerStatus()

					c.Run(ctx, c.Node(1), fmt.Sprintf("./workload fixtures load %s", workloadArgs))
					return nil
				})
				m.Wait()
			}

			urlBase, err := c.RunWithBuffer(ctx, c.l, c.Node(1),
				fmt.Sprintf(`./workload fixtures url %s`, workloadArgs))
			if err != nil {
				t.Fatal(err)
			}

			db := c.Conn(ctx, 1)
			defer db.Close()
			var binVersion string
			err = db.QueryRow("SELECT crdb_internal.node_executable_version()").Scan(&binVersion)
			if err != nil {
				t.Fatal(err)
			}

			// Stop all the nodes before downloading the store directory, or you end
			// up with invalid fixtures.
			c.Stop(ctx, c.All())

			fixturesURL := fmt.Sprintf("%s/stores=%d,bin-version=%s",
				bytes.TrimSpace(urlBase), stores, binVersion)

			// gsutil rm fails if no objects exist, even with -f, so check if any
			// old store directories exist before attempting to remove them.
			err = c.RunE(ctx, c.Node(1), fmt.Sprintf("gsutil -m -q ls %s &> /dev/null", fixturesURL))
			if err == nil {
				t.Status("deleting old store dumps")
				c.Run(ctx, c.Node(1), fmt.Sprintf("gsutil -m -q rm -r %s", fixturesURL))
			}

			t.Status("uploading store dumps")
			var g errgroup.Group
			for store := 1; store <= stores; store++ {
				store := store
				g.Go(func() error {
					c.Run(ctx, c.Node(store), fmt.Sprintf("gsutil -m -q cp -r {store-dir}/* %s/%d/",
						fixturesURL, store))
					return nil
				})
			}
			if err := g.Wait(); err != nil {
				t.Fatal(err)
			}
		},
	})
}
