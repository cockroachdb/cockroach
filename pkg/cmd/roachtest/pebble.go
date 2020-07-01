// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"os"
	"time"
)

func registerPebble(r *testRegistry) {
	pebble := os.Getenv("PEBBLE_BIN")
	if pebble == "" {
		pebble = "./pebble.linux"
	}

	run := func(ctx context.Context, t *test, c *cluster, size int) {
		c.Put(ctx, pebble, "./pebble")

		const initialKeys = 10_000_000
		const cache = 4 << 30             // 4 GB
		const duration = 10 * time.Second // TODO(peter): make this 10m
		const data = "$(dirname {store-dir})/data.tar"

		// Generate the initial DB state. This is somewhat time consuming for
		// larger value sizes, so we do this once and reuse the same DB state on
		// all of the workloads.
		initCmd := fmt.Sprintf(
			"./pebble bench ycsb {store-dir}"+
				" --wipe "+
				" --workload=read=100"+
				" --concurrency=256"+
				" --values=%d"+
				" --initial-keys=%d"+
				" --cache=%d"+
				" --num-ops=1",
			size, initialKeys, cache)
		c.Run(ctx, c.All(), initCmd)
		c.Run(ctx, c.All(), "rm -f "+data+"; tar cvPf "+data+" {store-dir}")

		for _, workload := range []string{"A", "B", "C", "D", "E"} {
			keys := "zipf"
			switch workload {
			case "D":
				keys = "uniform"
			}

			cmd := fmt.Sprintf(
				"rm -fr {store-dir} &&"+
					" tar xPf "+data+" &&"+
					" ./pebble bench ycsb {store-dir}"+
					" --workload=%s"+
					" --concurrency=256"+
					" --values=%d"+
					" --keys=%s"+
					" --initial-keys=0"+
					" --prepopulated-keys=%d"+
					" --cache=%d"+
					" --duration=%s",
				workload, size, keys, initialKeys, cache, duration)
			c.Run(ctx, c.All(), cmd)
		}
	}

	for _, size := range []int{64, 1024, 4096} {
		size := size
		r.Add(testSpec{
			Name:                          fmt.Sprintf("pebble/ycsb/size=%d", size),
			Owner:                         OwnerStorage,
			Timeout:                       2 * time.Hour,
			MinVersion:                    "v20.1.0",
			Cluster:                       makeClusterSpec(1, cpu(16)),
			Tags:                          []string{"pebble"},
			DisableReplicaDivergenceCheck: true,
			DisableDeadNodeCheck:          true,
			Run: func(ctx context.Context, t *test, c *cluster) {
				run(ctx, t, c, size)
			},
		})
	}
}
