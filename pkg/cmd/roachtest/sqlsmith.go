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

	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// This test runs SQLsmith in search of new panics.

func registerSQLsmith(r *registry) {
	runSQLsmith := func(
		ctx context.Context,
		t *test,
		c *cluster,
	) {
		if c.isLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		c.Put(ctx, cockroach, "./cockroach", c.All())
		c.Put(ctx, workload, "./workload", c.All())
		c.Start(ctx, t, c.All())

		t.Status("cloning SQLsmith and installing prerequisites")
		opts := retry.Options{
			InitialBackoff: 10 * time.Second,
			Multiplier:     2,
			MaxBackoff:     5 * time.Minute,
		}
		for attempt, r := 0, retry.StartWithCtx(ctx, opts); r.Next(); {
			if ctx.Err() != nil {
				return
			}
			if c.t.Failed() {
				return
			}
			attempt++

			c.l.Printf("attempt %d - update dependencies", attempt)
			if err := c.RunE(ctx, node, `sudo apt-get -q update`); err != nil {
				continue
			}
			if err := c.RunE(
				ctx, node,
				`sudo apt-get -qy install build-essential autoconf autoconf-archive libpqxx-dev libboost-regex-dev libsqlite3-dev`,
			); err != nil {
				continue
			}

			c.l.Printf("attempt %d - cloning SQLsmith", attempt)
			if err := c.RunE(ctx, node, `rm -rf /mnt/data1/sqlsmith`); err != nil {
				continue
			}
			if err := c.GitCloneE(
				ctx,
				"https://github.com/cockroachdb/sqlsmith.git",
				"/mnt/data1/sqlsmith",
				"cockroach",
				node,
			); err != nil {
				continue
			}

			c.l.Printf("attempt %d - building SQLsmith", attempt)
			if err := c.RunE(ctx, node, `cd /mnt/data1/sqlsmith/ && autoreconf -i`); err != nil {
				continue
			}
			if err := c.RunE(ctx, node, `cd /mnt/data1/sqlsmith/ && ./configure`); err != nil {
				continue
			}
			if err := c.RunE(ctx, node, `cd /mnt/data1/sqlsmith/ && make`); err != nil {
				continue
			}

			break
		}

		m := newMonitor(ctx, c, c.All())
		m.Go(func(ctx context.Context) error {
			t.Status("running SQLsmith against TPCC schema")
			c.Run(ctx, node, fmt.Sprintf(`./workload fixtures load tpcc --warehouses=%d {pgurl:1}`, 10))
			c.Run(ctx, node,
				`cd /mnt/data1/sqlsmith/ && ./sqlsmith --target="postgres://root@localhost:26257/tpcc?sslmode=disable" --max-queries=1000 --verbose > --dump-all-queries`,
			)
			return nil
		})
		m.Wait()
	}

	r.Add(testSpec{
		Name:       "sqlsmith",
		Nodes:      nodes(1),
		Stable:     false,
		MinVersion: "2.2.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			runSQLsmith(ctx, t, c)
		},
		Skip:    "https://github.com/cockroachdb/cockroach/issues/32270",
		Timeout: 20 * time.Minute,
	})
}
