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

func storeDirURL(fixtureURL string, stores int, binVersion string) string {
	return fmt.Sprintf("%s/stores=%d,bin-version=%s", fixtureURL, stores, binVersion)
}

func storeDumpExists(ctx context.Context, c *cluster, storeDirPath string) (bool, error) {
	output, err := c.RunWithBuffer(ctx, c.l, c.Node(1),
		fmt.Sprintf("gsutil -m -q ls %s", storeDirPath))
	if err == nil {
		return true, nil
	}
	if strings.Contains(string(output), "matched no objects") {
		return false, nil
	}
	return false, err
}

func downloadStoreDumps(ctx context.Context, c *cluster, storeDirPath string, nodeCount int) error {
	var g errgroup.Group
	for node := 1; node <= nodeCount; node++ {
		node := node
		g.Go(func() error {
			path := fmt.Sprintf("%s/%d/*", storeDirPath, node)
			return c.RunE(ctx, c.Node(node), `mkdir -p {store-dir} && gsutil -m cp -r `+path+` {store-dir}`)
		})
	}
	return g.Wait()
}

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
			c.Start(ctx, t, startArgs("--sequential"))

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

			fixtureURL := string(bytes.TrimSpace(urlBase))
			storeDirsPath := storeDirURL(fixtureURL, stores, binVersion)

			// Stop all the nodes before downloading the store directory, or you end
			// up with invalid fixtures.
			c.Stop(ctx, c.All())

			// gsutil rm fails if no objects exist, even with -f, so check if any
			// old store directories exist before attempting to remove them.
			if exists, err := storeDumpExists(ctx, c, storeDirsPath); err != nil {
				t.Fatal(err)
			} else if exists {
				t.Status("deleting old store dumps")
				c.Run(ctx, c.Node(1), fmt.Sprintf("gsutil -m -q rm -r %s", storeDirsPath))
			}

			t.Status("uploading store dumps")
			var g errgroup.Group
			for store := 1; store <= stores; store++ {
				store := store
				g.Go(func() error {
					c.Run(ctx, c.Node(store), fmt.Sprintf("gsutil -m -q cp -r {store-dir}/* %s/%d/",
						storeDirsPath, store))
					return nil
				})
			}
			if err := g.Wait(); err != nil {
				t.Fatal(err)
			}
		},
	})
}
