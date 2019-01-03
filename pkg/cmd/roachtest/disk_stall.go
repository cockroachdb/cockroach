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
// permissions and limitations under the License.

package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func registerDiskStalledDetection(r *registry) {
	for _, affectsLogDir := range []bool{false, true} {
		for _, affectsDataDir := range []bool{false, true} {
			// Grab copies of the args because we'll pass them into a closure.
			// Everyone's favorite bug to write in Go.
			affectsLogDir := affectsLogDir
			affectsDataDir := affectsDataDir
			r.Add(testSpec{
				Name: fmt.Sprintf(
					"disk-stalled/log=%t,data=%t",
					affectsLogDir, affectsDataDir,
				),
				MinVersion: `v2.2.0`,
				Nodes:      nodes(1),
				Run: func(ctx context.Context, t *test, c *cluster) {
					runDiskStalledDetection(ctx, t, c, affectsLogDir, affectsDataDir)
				},
			})
		}
	}
}

func runDiskStalledDetection(
	ctx context.Context, t *test, c *cluster, affectsLogDir bool, affectsDataDir bool,
) {
	n := c.Node(1)
	tmpDir, err := ioutil.TempDir("", "stalled")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	c.Put(ctx, cockroach, "./cockroach")
	c.Run(ctx, n, "sudo umount -f {store-dir}/faulty || true")
	c.Run(ctx, n, "mkdir -p {store-dir}/{real,faulty} || true")
	// Make sure the actual logs are downloaded as artifacts.
	c.Run(ctx, n, "rm -f logs/real && ln -s {store-dir}/real/logs logs/real || true")

	t.Status("setting up charybdefs")

	if err := execCmd(ctx, t.l, roachprod, "install", c.makeNodes(n), "charybdefs"); err != nil {
		t.Fatal(err)
	}
	c.Run(ctx, n, "sudo charybdefs {store-dir}/faulty -oallow_other,modules=subdir,subdir={store-dir}/real && chmod 777 {store-dir}/{real,faulty}")
	l, err := t.l.ChildLogger("cockroach")
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		err error
		out string
	}
	errCh := make(chan result)

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
		out, err := c.RunWithBuffer(ctx, l, n,
			fmt.Sprintf("timeout --signal 9 %ds env COCKROACH_ENGINE_MAX_SYNC_DURATION=%s COCKROACH_LOG_MAX_SYNC_DURATION=%s "+
				"./cockroach start --insecure --logtostderr=INFO --store {store-dir}/%s --log-dir {store-dir}/%s",
				int(dur.Seconds()), maxDataSync, maxLogSync, dataDir, logDir,
			),
		)
		errCh <- result{err, string(out)}
	}()

	time.Sleep(time.Duration(rand.Intn(5)) * time.Second)

	t.Status("blocking storage")
	c.Run(ctx, n, "charybdefs-nemesis --delay")

	res := <-errCh
	if res.err == nil {
		t.Fatalf("expected an error: %s", res.out)
	}

	// This test can also run in sanity check mode to make sure it doesn't fail
	// due to the aggressive env vars above.
	expectMsg := affectsDataDir || affectsLogDir

	if expectMsg != strings.Contains(res.out, "disk stall detected") {
		t.Fatalf("unexpected output: %v %s", res.err, res.out)
	} else if elapsed := timeutil.Since(tStarted); !expectMsg && elapsed < dur {
		t.Fatalf("no disk stall injected, but process terminated too early after %s (expected >= %s)", elapsed, dur)
	}

	c.Run(ctx, n, "charybdefs-nemesis --clear")
	c.Run(ctx, n, "sudo umount {store-dir}/faulty")
}
