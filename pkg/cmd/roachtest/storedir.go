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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/ccl/workloadccl"
	"github.com/cockroachdb/cockroach/pkg/workload"
)

const (
	gcsURLScheme  = `gs`
	gcsTestBucket = `cockroachdb-backup-testing`
)

func fixtureConfig() workloadccl.FixtureConfig {
	return workloadccl.FixtureConfig{
		GCSBucket: `cockroach-fixtures`,
		GCSPrefix: `workload`,
	}
}

// TODO(dan): There should be tooling for making these snapshots (and it should
// be much less roundabout). In the meantime, manually run something like the
// following:
//
// roachprod create <cluster> -n <nodes>
// roachprod put <cluster> <linux build of cockroach> cockroach
// roachprod put <cluster> <linux build of workload> workload
// roachprod start <cluster>
// for i in {1..<nodes>}; do roachprod ssh <cluster>:$i -- './workload csv-server --port=8081 &> logs/workload-csv-server.log < /dev/null &'; done
// roachprod ssh <cluster>:1 -- ./workload fixtures make <workload and flags> --csv-server=http://localhost:8081
// roachprod ssh <cluster>:1 -- ./workload fixtures load <workload and flags>
// for i in {1..<nodes>}; do roachprod ssh <cluster>:$i -- 'tar czf - -C /mnt/data1/cockroach . | gsutil cp - `./workload fixtures store-dir <workload and flags> $i <nodes>`'
// roachprod destroy <cluster>

// RestoreStoreDirSnapshots completely overwrites the store directories of every
// node in the cluster with a previously taken .tgz snapshot stored in GCS.
func (c *cluster) RestoreStoreDirSnapshots(ctx context.Context, gen workload.Generator) {
	if c.isLocal() {
		c.t.Fatal(`TODO(dan): support store dir snapshots on local clusters`)
	}

	var wg sync.WaitGroup
	for node := 1; node <= c.nodes; node++ {
		node := node
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Assumes cluster configuration of 1 store per node.
			storeIdx, numStores := node-1, c.nodes
			path := workloadccl.StoreDir(fixtureConfig(), gen, storeIdx, numStores)

			c.Stop(ctx, c.Node(node))
			// TODO(dan): Get this path from roachprod instead of hardcoding it.
			c.Run(ctx, node, `rm -rf /mnt/data1/cockroach/*`)
			c.Run(ctx, node, `mkdir -p /mnt/data1/cockroach`)
			cmd := `gsutil cat ` + path + ` | tar xvzf - -C /mnt/data1/cockroach/`
			c.Run(ctx, node, cmd)
		}()
	}
	wg.Wait()
}
