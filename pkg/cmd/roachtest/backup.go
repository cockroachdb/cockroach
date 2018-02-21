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
	tests.Add(`backup2TB`, func(t *test) {
		const nodes = 10

		ctx := context.Background()
		c := newCluster(ctx, t, nodes)
		defer c.Destroy(ctx)

		c.status(`downloading store dumps`)
		var wg sync.WaitGroup
		for node := 1; node <= nodes; node++ {
			node := node
			wg.Add(1)
			go func() {
				defer wg.Done()
				c.Run(ctx, node, `mkdir -p /mnt/data1/cockroach`)
				path := fmt.Sprintf(`gs://cockroach-fixtures/workload/bank/`+
					`version=1.0.0,payload-bytes=10240,ranges=0,rows=65104166,seed=1/`+
					`stores=%d/%d.tgz`, nodes, node-1)
				c.Run(ctx, node, `gsutil cat `+path+` | tar xvzf - -C /mnt/data1/cockroach/`)
			}()
		}
		wg.Wait()

		c.Put(ctx, cockroach, "./cockroach")
		c.Start(ctx)
		m := newMonitor(ctx, c)
		m.Go(func(ctx context.Context) error {
			c.status(`running 2tb backup`)
			c.Run(ctx, 1, `./cockroach sql --insecure -e "
				BACKUP workload.bank TO 'gs://cockroachdb-backup-testing/`+c.name+`'"`)
			return nil
		})
		m.Wait()
	})
}
