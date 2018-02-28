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
	"runtime"

	"github.com/cockroachdb/cockroach/pkg/util/binfetcher"
	_ "github.com/lib/pq"
)

func init() {
	runVersion := func(t *test, version string, nodes int) {
		ctx := context.Background()
		c := newCluster(ctx, t, nodes)
		defer c.Destroy(ctx)

		os := ifLocal(runtime.GOOS, "linux")

		b, err := binfetcher.Download(ctx, binfetcher.Options{
			Binary:  "cockroach",
			Version: version,
			GOOS:    os,
			GOARCH:  "amd64",
		})
		if err != nil {
			t.Fatal(err)
		}

		c.Put(ctx, workload, "./workload", c.All())
		// NB: this math doesn't work for a general value of `nodes`.
		c.Put(ctx, b, "./cockroach", c.Range(1, nodes/2))
		c.Put(ctx, cockroach, "./cockroach", c.Range(nodes/2+1, nodes))
		// It happens in a completely different place, but the startup order makes
		// this cluster run at `version`. Pinky promise. (We should plumb `PGUrl`
		// and then connect and check).
		c.Start(ctx, c.All())

		m := newMonitor(ctx, c, c.All())
		m.Go(func(ctx context.Context) error {
			t.Status("tpcc")
			duration := " --duration=" + ifLocal("10s", "10m")
			cmd := fmt.Sprintf(
				"./workload run tpcc --init --warehouses=1"+duration+" {pgurl:1-%d}",
				nodes)
			c.Run(ctx, 1, cmd)
			return nil
		})
		m.Wait()
	}

	const nodes = 3
	const version = "v1.1.5"
	tests.Add(
		fmt.Sprintf("version/mixedWith=%s/nodes=%d", version, nodes),
		func(t *test) {
			runVersion(t, version, nodes)
		})
}
