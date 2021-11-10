// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

func registerPebble(r registry.Registry) {
	pebble := os.Getenv("PEBBLE_BIN")
	if pebble == "" {
		pebble = "./pebble.linux"
	}

	run := func(ctx context.Context, t test.Test, c cluster.Cluster, size int, dur int64) {
		c.Put(ctx, pebble, "./pebble")

		const initialKeys = 10_000_000
		const cache = 4 << 30 // 4 GB
		const dataDir = "$(dirname {store-dir})"
		const dataTar = dataDir + "/data.tar"
		const benchDir = dataDir + "/bench"

		var duration = time.Duration(dur) * time.Minute

		runCmd := func(cmd string) {
			t.L().PrintfCtx(ctx, "> %s", cmd)
			err := c.RunL(ctx, t.L(), c.All(), cmd)
			t.L().Printf("> result: %+v", err)
			if err := ctx.Err(); err != nil {
				t.L().Printf("(note: incoming context was canceled: %s", err)
			}
			if err != nil {
				t.Fatal(err)
			}
		}

		// Generate the initial DB state. This is somewhat time consuming for
		// larger value sizes, so we do this once and reuse the same DB state on
		// all of the workloads.
		runCmd(fmt.Sprintf(
			"(./pebble bench ycsb %s"+
				" --wipe "+
				" --workload=read=100"+
				" --concurrency=1"+
				" --values=%d"+
				" --initial-keys=%d"+
				" --cache=%d"+
				" --num-ops=1 && "+
				"rm -f %s && tar cvPf %s %s) > init.log 2>&1",
			benchDir, size, initialKeys, cache, dataTar, dataTar, benchDir))

		for _, workload := range []string{"A", "B", "C", "D", "E", "F"} {
			keys := "zipf"
			switch workload {
			case "D":
				keys = "uniform"
			}

			runCmd(fmt.Sprintf(
				"rm -fr %s && tar xPf %s &&"+
					" ./pebble bench ycsb %s"+
					" --workload=%s"+
					" --concurrency=256"+
					" --values=%d"+
					" --keys=%s"+
					" --initial-keys=0"+
					" --prepopulated-keys=%d"+
					" --cache=%d"+
					" --duration=%s > ycsb.log 2>&1",
				benchDir, dataTar, benchDir, workload, size, keys, initialKeys, cache, duration))

			runCmd(fmt.Sprintf("tar cvPf profiles_%s.tar *.prof", workload))

			dest := filepath.Join(t.ArtifactsDir(), fmt.Sprintf("ycsb_%s.log", workload))
			if err := c.Get(ctx, t.L(), "ycsb.log", dest, c.All()); err != nil {
				t.Fatal(err)
			}

			profilesName := fmt.Sprintf("profiles_%s.tar", workload)
			dest = filepath.Join(t.ArtifactsDir(), profilesName)
			if err := c.Get(ctx, t.L(), profilesName, dest, c.All()); err != nil {
				t.Fatal(err)
			}

			runCmd("rm -fr *.prof")
		}
	}

	// Generate roachtests that run for both 10 minutes and 90 minutes. The former
	// is useful for local testing, while the latter is used to populate the
	// Pebble Nightly benchmarks.
	for _, dur := range []int64{10, 90} {
		for _, size := range []int{64, 1024} {
			size := size
			// Alter the tag name based on the duration of the test. This tag is used
			// as a selector in the nightly benchmarks script (see
			// build/teamcity-nightly-pebble.sh).
			var tag string
			if dur == 90 {
				tag = "pebble_nightly"
			} else {
				tag = "pebble"
			}
			r.Add(registry.TestSpec{
				// NOTE: This test name should not change, as it is relied upon by the
				// Pebble mkbench command, which creates a well-known directory
				// structure that is relied-upon by the javascript on the Pebble
				// Benchmarks webpage.
				Name:    fmt.Sprintf("pebble/ycsb/size=%d", size),
				Owner:   registry.OwnerStorage,
				Timeout: 12 * time.Hour,
				Cluster: r.MakeClusterSpec(5, spec.CPU(16)),
				Tags:    []string{tag},
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					run(ctx, t, c, size, dur)
				},
			})
		}
	}
}
