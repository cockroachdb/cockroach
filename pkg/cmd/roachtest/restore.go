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
)

func init() {
	tests.Add(testSpec{
		Name:  `restore2TB`,
		Nodes: nodes(10),
		Run: func(ctx context.Context, t *test, c *cluster) {
			c.Put(ctx, cockroach, "./cockroach")
			c.Start(ctx)
			m := newMonitor(ctx, c)
			m.Go(func(ctx context.Context) error {
				t.Status(`running restore`)
				c.Run(ctx, c.Node(1), `./cockroach sql --insecure -e "CREATE DATABASE restore2tb"`)
				// TODO(dan): It'd be nice if we could keep track over time of how
				// long this next line took.
				c.Run(ctx, c.Node(1), `./cockroach sql --insecure -e "
				RESTORE csv.bank FROM
				'gs://cockroach-fixtures/workload/bank/version=1.0.0,payload-bytes=10240,ranges=0,rows=65104166,seed=1/bank'
				WITH into_db = 'restore2tb'"`)
				return nil
			})
			m.Wait()
		},
	})
}
