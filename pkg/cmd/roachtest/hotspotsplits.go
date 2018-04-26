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
	"io/ioutil"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func registerHotSpotSplits(r *registry) {
	// This test sets up a cluster and runs kv on it with high concurrency and a large block size
	// to force a large range. We then make sure that the largest range isn't larger than a threshold and
	// that backpressure is working correctly.
	runHotSpot := func(ctx context.Context, t *test, c *cluster, nodes int, duration time.Duration, concurrency int) {
		c.Put(ctx, workload, "./workload", c.Node(nodes))
		c.Put(ctx, cockroach, "./cockroach", c.All())
		c.Start(ctx, c.All())

		c.Run(ctx, c.Node(nodes), `./workload init kv --drop`)

		var m *errgroup.Group // see comment in version.go
		m, ctx = errgroup.WithContext(ctx)

		m.Go(func() error {
			c.l.printf("starting load generator\n")

			quietL, err := newLogger("run kv", strconv.Itoa(0), "workload"+strconv.Itoa(0), ioutil.Discard, os.Stderr)
			if err != nil {
				return err
			}
			const blockSize = 1 << 20 // 1 MB
			return c.RunL(ctx, quietL, c.Node(nodes), fmt.Sprintf(
				"./workload run kv --read-percent=0 --splits=0 --concurrency=%d --min-block-bytes=%d --max-block-bytes=%d --duration %s {pgurl:1-3}",
				concurrency, blockSize, blockSize+1, duration.String()))
		})

		m.Go(func() error {
			t.Status(fmt.Sprintf("starting checks for range sizes"))
			const sizeLimit = 3 * (1 << 26) // 192 MB

			db := c.Conn(ctx, 1)
			defer db.Close()

			run := func(stmt string) {
				t.Status(stmt)
				_, err := db.ExecContext(ctx, stmt)
				if err != nil {
					t.Fatal(err)
				}
			}

			run(`SET CLUSTER SETTING trace.debug.enable = true`)

			var size = float64(0)
			for tBegin := timeutil.Now(); timeutil.Since(tBegin) <= duration; {
				if err := db.QueryRow(
					`select max(bytes_per_replica->'PMax') from crdb_internal.kv_store_status;`,
				).Scan(&size); err != nil {
					return err
				}
				if size > sizeLimit {
					return errors.Errorf("range size %s exceeded %s", humanizeutil.IBytes(int64(size)),
						humanizeutil.IBytes(int64(sizeLimit)))
				}

				t.Status(fmt.Sprintf("max range size %s", humanizeutil.IBytes(int64(size))))

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second):
				}
			}

			return nil
		})
		if err := m.Wait(); err != nil {
			t.Fatal(err)
		}
	}

	minutes := 10 * time.Minute
	numNodes := 4
	concurrency := 100

	r.Add(testSpec{
		SkippedBecause: "https://github.com/cockroachdb/cockroach/issues/25036",
		Name:           fmt.Sprintf("hotspotsplits/nodes=%d", numNodes),
		Nodes:          nodes(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			if local {
				concurrency = 50
				fmt.Printf("lowering concurrency to %d in local testing\n", concurrency)
			}
			runHotSpot(ctx, t, c, numNodes, minutes, concurrency)
		},
	})
}
