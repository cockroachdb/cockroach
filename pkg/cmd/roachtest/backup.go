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

	"golang.org/x/sync/errgroup"
)

func downloadStoreDumps(
	ctx context.Context, c *cluster, location string, binVersion string, nodeCount int,
) error {
	var g errgroup.Group
	for node := 1; node <= nodeCount; node++ {
		node := node
		g.Go(func() error {
			path := location + fmt.Sprintf(`/stores=%d,bin-version=%s/%d/*`, nodeCount, binVersion, node)
			return c.RunE(ctx, c.Node(node), `mkdir -p {store-dir} && gsutil -m -q cp -r `+path+` {store-dir}`)
		})
	}
	return g.Wait()
}

func registerBackup(r *registry) {
	r.Add(testSpec{
		Name:   `backup2TB`,
		Nodes:  nodes(10),
		Stable: true, // DO NOT COPY to new tests
		Run: func(ctx context.Context, t *test, c *cluster) {
			nodes := c.nodes

			t.Status(`downloading store dumps`)
			// Created via:
			// roachtest --cockroach cockroach-v2.0.1 store-gen --stores=10 bank \
			//           --payload-bytes=10240 --ranges=0 --rows=65104166
			location := `gs://cockroach-fixtures/workload/bank/version=1.0.0,payload-bytes=10240,ranges=0,rows=65104166,seed=1`
			if err := downloadStoreDumps(ctx, c, location, "2.0", nodes); err != nil {
				t.Fatal(err)
			}

			c.Put(ctx, cockroach, "./cockroach")
			c.Start(ctx)
			m := newMonitor(ctx, c)
			m.Go(func(ctx context.Context) error {
				t.Status(`running backup`)
				c.Run(ctx, c.Node(1), `./cockroach sql --insecure -e "
				BACKUP bank.bank TO 'gs://cockroachdb-backup-testing/`+c.name+`'"`)
				return nil
			})
			m.Wait()
		},
	})
}
