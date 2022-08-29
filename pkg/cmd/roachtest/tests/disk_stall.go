// Copyright 2018 The Cockroach Authors.
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
	"math/rand"
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// registerDiskStalledDetection registers the disk stall test.
func registerDiskStalledDetection(r registry.Registry) {
	for _, affectsLogDir := range []bool{false, true} {
		for _, affectsDataDir := range []bool{false, true} {
			// Grab copies of the args because we'll pass them into a closure.
			// Everyone's favorite bug to write in Go.
			affectsLogDir := affectsLogDir
			affectsDataDir := affectsDataDir
			r.Add(registry.TestSpec{
				Name: fmt.Sprintf(
					"disk-stalled/log=%t,data=%t",
					affectsLogDir, affectsDataDir,
				),
				Owner:   registry.OwnerStorage,
				Cluster: r.MakeClusterSpec(1),
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runDiskStalledDetection(ctx, t, c, affectsLogDir, affectsDataDir)
				},
			})
		}
	}
}

func runDiskStalledDetection(
	ctx context.Context, t test.Test, c cluster.Cluster, affectsLogDir bool, affectsDataDir bool,
) {
	if c.IsLocal() && runtime.GOOS != "linux" {
		t.Fatalf("must run on linux os, found %s", runtime.GOOS)
	}

	n := c.Node(1)

	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Run(ctx, n, "sudo umount -f {store-dir}/faulty || true")
	c.Run(ctx, n, "mkdir -p {store-dir}/{real,faulty} || true")
	// Make sure the actual logs are downloaded as artifacts.
	c.Run(ctx, n, "rm -f logs && ln -s {store-dir}/real/logs logs || true")

	t.Status("setting up charybdefs")

	if err := c.Install(ctx, t.L(), n, "charybdefs"); err != nil {
		t.Fatal(err)
	}
	c.Run(ctx, n, "sudo charybdefs {store-dir}/faulty -oallow_other,modules=subdir,subdir={store-dir}/real")
	c.Run(ctx, n, "sudo mkdir -p {store-dir}/real/logs")
	c.Run(ctx, n, "sudo chmod -R 777 {store-dir}/{real,faulty}")

	errCh := make(chan install.RunResultDetails)

	// NB: charybdefs' delay nemesis introduces 50ms per syscall. It would
	// be nicer to introduce a longer delay, but this works.
	tooShortSync := 40 * time.Millisecond

	maxLogSync := time.Hour
	logDir := "real/logs"
	if affectsLogDir {
		logDir = "faulty/logs"
		maxLogSync = tooShortSync
	}
	maxDataSync := time.Hour
	dataDir := "real"
	if affectsDataDir {
		maxDataSync = tooShortSync
		dataDir = "faulty"
	}

	tStarted := timeutil.Now()
	dur := 10 * time.Minute
	if !affectsDataDir && !affectsLogDir {
		dur = 30 * time.Second
	}

	go func() {
		t.WorkerStatus("running server")
		result, err := c.RunWithDetailsSingleNode(ctx, t.L(), n,
			fmt.Sprintf("timeout --signal 9 %ds env "+
				"COCKROACH_ENGINE_MAX_SYNC_DURATION_DEFAULT=%s "+
				"COCKROACH_LOG_MAX_SYNC_DURATION=%s "+
				"COCKROACH_AUTO_BALLAST=false "+
				"./cockroach start-single-node --insecure --store {store-dir}/%s --log '{sinks: {stderr: {filter: INFO}}, file-defaults: {dir: \"{store-dir}/%s\"}}'",
				int(dur.Seconds()), maxDataSync, maxLogSync, dataDir, logDir,
			),
		)
		if err != nil {
			result.Err = err
		}
		errCh <- result
	}()

	time.Sleep(time.Duration(rand.Intn(5)) * time.Second)

	t.Status("blocking storage")
	c.Run(ctx, n, "charybdefs-nemesis --delay")

	result := <-errCh
	if result.Err == nil {
		t.Fatalf("expected an error: %s", result.Stdout)
	}

	// This test can also run in sanity check mode to make sure it doesn't fail
	// due to the aggressive env vars above.
	expectMsg := affectsDataDir || affectsLogDir

	if expectMsg != strings.Contains(result.Stderr, "disk stall detected") {
		t.Fatalf("unexpected output: %v", result.Err)
	} else if elapsed := timeutil.Since(tStarted); !expectMsg && elapsed < dur {
		t.Fatalf("no disk stall injected, but process terminated too early after %s (expected >= %s)", elapsed, dur)
	}

	c.Run(ctx, n, "charybdefs-nemesis --clear")
	c.Run(ctx, n, "sudo umount {store-dir}/faulty")
}
