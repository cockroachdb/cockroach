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

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func registerCreateIndexTPCC(r *registry) {
	warehouses := 1000
	numNodes := 5
	r.Add(testSpec{
		Name:    fmt.Sprintf("createindex/tpcc/warehouses=%d/nodes=%d", warehouses, numNodes),
		Nodes:   nodes(numNodes),
		Timeout: 3 * time.Hour,
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses: warehouses,
				Extra:      "--wait=false --tolerate-errors",
				During: func(ctx context.Context) error {
					conn := c.Conn(ctx, 1)
					start := timeutil.Now()
					if _, err := conn.Exec(`
					CREATE UNIQUE INDEX foo ON tpcc.order (o_entry_d, o_w_id, o_d_id, o_carrier_id, o_id);
				`); err != nil {
						t.Fatal(err)
					}
					c.l.Printf("CREATE INDEX took %s", timeutil.Since(start))
					return nil
				},
				Duration: 2 * time.Hour,
			})
		},
	})
}
