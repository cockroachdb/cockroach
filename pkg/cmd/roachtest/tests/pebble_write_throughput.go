// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

func registerPebbleWriteThroughput(r registry.Registry) {
	pebble := os.Getenv("PEBBLE_BIN")
	if pebble == "" {
		pebble = "./pebble.linux"
	}

	// Register the Pebble write benchmark. We only run the 1024 variant for now.
	size := 1024
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("pebble/write/size=%d", size),
		Owner:            registry.OwnerStorage,
		Benchmark:        true,
		Timeout:          10 * time.Hour,
		Cluster:          r.MakeClusterSpec(5, spec.CPU(16), spec.SSD(16), spec.RAID0(true)),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.PebbleNightlyWrite),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runPebbleWriteBenchmark(ctx, t, c, size, pebble)
		},
	})
}

// runPebbleWriteBenchmark runs the Pebble write benchmark.
func runPebbleWriteBenchmark(
	ctx context.Context, t test.Test, c cluster.Cluster, size int, bin string,
) {
	c.Put(ctx, bin, "./pebble")

	const (
		duration    = 8 * time.Hour
		dataDir     = "$(dirname {store-dir})"
		benchDir    = dataDir + "/bench"
		concurrency = 1024
		rateStart   = 30_000
	)

	runPebbleCmd(ctx, t, c, fmt.Sprintf(
		" ./pebble bench write %s"+
			" --wipe"+
			" --concurrency=%d"+
			" --duration=%s"+
			" --values=%d"+
			" --rate-start=%d"+
			" --debug > write.log 2>&1",
		benchDir, concurrency, duration, size, rateStart,
	))

	runPebbleCmd(ctx, t, c, "tar cvPf profiles.tar *.prof")

	dest := filepath.Join(t.ArtifactsDir(), "write.log")
	if err := c.Get(ctx, t.L(), "write.log", dest, c.All()); err != nil {
		t.Fatal(err)
	}

	profilesName := "profiles.tar"
	dest = filepath.Join(t.ArtifactsDir(), profilesName)
	if err := c.Get(ctx, t.L(), profilesName, dest, c.All()); err != nil {
		t.Fatal(err)
	}

	runPebbleCmd(ctx, t, c, "rm -fr *.prof")
}
