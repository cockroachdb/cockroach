// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

func registerPebbleYCSB(r registry.Registry) {
	pebble := os.Getenv("PEBBLE_BIN")
	if pebble == "" {
		pebble = "./pebble.linux"
	}

	for _, dur := range []int64{10, 90} {
		for _, size := range []int{64, 1024} {
			size := size

			// NOTE: This test name should not change for benchmark data that gets
			// persisted to S3, as it is relied-upon by the Pebble mkbench command,
			// which creates a well-known directory structure that is in-turn
			// relied-upon by the javascript on the Pebble Benchmarks webpage.
			name := fmt.Sprintf("pebble/ycsb/size=%d", size)
			suites := registry.Suites(registry.PebbleNightlyYCSB)

			// For the shorter benchmark runs, we append a suffix to the name to avoid
			// a name collision. This is safe to do as these tests are not executed as
			// part of the nightly benchmark runs.
			if dur != 90 {
				suites = registry.Suites(registry.Pebble)
				name += fmt.Sprintf("/duration=%d", dur)
			}

			d := dur
			r.Add(registry.TestSpec{
				Name:             name,
				Owner:            registry.OwnerStorage,
				Benchmark:        true,
				Timeout:          12 * time.Hour,
				Cluster:          r.MakeClusterSpec(5, spec.CPU(16)),
				CompatibleClouds: registry.AllClouds,
				Suites:           suites,
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runPebbleYCSB(ctx, t, c, size, pebble, d, nil, true /* artifacts */)
				},
			})
		}
	}

	// Add the race build.
	r.Add(registry.TestSpec{
		Name:             "pebble/ycsb/A/race/duration=30",
		Owner:            registry.OwnerStorage,
		Benchmark:        true,
		Timeout:          12 * time.Hour,
		Cluster:          r.MakeClusterSpec(5, spec.CPU(16)),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.PebbleNightlyYCSBRace),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runPebbleYCSB(ctx, t, c, 64, pebble, 30, []string{"A"}, false /* artifacts */)
		},
	})
}

// runPebbleYCSB runs the Pebble YCSB benchmarks.
func runPebbleYCSB(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	size int,
	bin string,
	dur int64,
	workloads []string,
	collectArtifacts bool,
) {
	c.Put(ctx, bin, "./pebble")
	if workloads == nil {
		workloads = []string{"A", "B", "C", "D", "E", "F"}
	}

	const initialKeys = 10_000_000
	const cache = 4 << 30 // 4 GB
	const dataDir = "$(dirname {store-dir})"
	const dataTar = dataDir + "/data.tar"
	const benchDir = dataDir + "/bench"

	var duration = time.Duration(dur) * time.Minute

	// Generate the initial DB state. This is somewhat time consuming for
	// larger value sizes, so we do this once and reuse the same DB state on
	// all of the workloads.
	runPebbleCmd(ctx, t, c, fmt.Sprintf(
		"./pebble bench ycsb %s"+
			" --wipe "+
			" --workload=read=100"+
			" --concurrency=1"+
			" --values=%d"+
			" --initial-keys=%d"+
			" --cache=%d"+
			" --num-ops=1 && "+
			"rm -f %s && tar cvPf %s %s",
		benchDir, size, initialKeys, cache, dataTar, dataTar, benchDir))

	for _, workload := range workloads {
		keys := "zipf"
		switch workload {
		case "D":
			keys = "uniform"
		}

		runPebbleCmd(ctx, t, c, fmt.Sprintf(
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

		if !collectArtifacts {
			return
		}

		runPebbleCmd(ctx, t, c, fmt.Sprintf("tar cvPf profiles_%s.tar *.prof", workload))

		dest := filepath.Join(t.ArtifactsDir(), fmt.Sprintf("ycsb_%s.log", workload))
		if err := c.Get(ctx, t.L(), "ycsb.log", dest, c.All()); err != nil {
			t.Fatal(err)
		}

		profilesName := fmt.Sprintf("profiles_%s.tar", workload)
		dest = filepath.Join(t.ArtifactsDir(), profilesName)
		if err := c.Get(ctx, t.L(), profilesName, dest, c.All()); err != nil {
			t.Fatal(err)
		}

		runPebbleCmd(ctx, t, c, "rm -fr *.prof")
	}
}

// runPebbleCmd runs the given command on all worker nodes in the test cluster.
func runPebbleCmd(ctx context.Context, t test.Test, c cluster.Cluster, cmd string) {
	t.L().PrintfCtx(ctx, "> %s", cmd)
	err := c.RunE(ctx, option.WithNodes(c.All()), cmd)
	t.L().Printf("> result: %+v", err)
	if err := ctx.Err(); err != nil {
		t.L().Printf("(note: incoming context was canceled: %s", err)
	}
	if err != nil {
		t.Fatal(err)
	}
}
