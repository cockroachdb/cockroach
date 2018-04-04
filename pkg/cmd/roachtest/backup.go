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
	"sync"
)

func init() {
	tests.Add(testSpec{
		Name:  `backup2TB`,
		Nodes: nodes(10),
		Run: func(ctx context.Context, t *test, c *cluster) {
			nodes := c.nodes

			t.Status(`downloading store dumps`)
			var wg sync.WaitGroup
			for node := 1; node <= nodes; node++ {
				node := c.Node(node)
				wg.Add(1)
				go func() {
					defer wg.Done()
					c.Run(ctx, node, `mkdir -p /mnt/data1/cockroach`)
					path := fmt.Sprintf(`gs://cockroach-fixtures/workload/bank/`+
						`version=1.0.0,payload-bytes=10240,ranges=0,rows=65104166,seed=1/`+
						`stores=%d/%d/*`, nodes, node)
					c.Run(ctx, node, `gsutil -m -q cp -r `+path+` /mnt/data1/cockroach`)
				}()
			}
			wg.Wait()

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
