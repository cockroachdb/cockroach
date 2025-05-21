// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package tpcc is a benchmark suite to exercise the transactions of tpcc
// in a benchmarking setting.
//
// We love to optimize our benchmarks, but some of them are too simplistic.
// Hopefully optimizing these queries with a tight iteration cycle will better
// reflect real world workloads.
package tpcc

import (
	"context"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	_ "github.com/cockroachdb/cockroach/pkg/workload/tpcc"
)

// BenchmarkTPCC runs a benchmark of different mixes of TPCC queries against
// a single warehouse. It runs the client side of the workload in a subprocess
// to filter out the client overhead.
//
// Without any special flags, the benchmark will load the data for the single
// warehouse once in a temp dir and will then copy the store directory for
// that warehouse to an in-memory store for each sub-benchmark. The --store-dir
// flag can be used to specify the path to such a store dir to enable faster
// iteration re-running the test. The --generate-store-dir flag, when combined
// with --store-dir will generate a new store directory at the specified path.
// The combination of the two flags is how you bootstrap such a path initially.
//
// For example, generate the store directory with:
//
//	./dev bench pkg/bench/tpcc -f BenchmarkTPCC --test-args '--generate-store-dir --store-dir=/tmp/benchtpcc'
//
// Reuse the store directory with:
//
//	./dev bench pkg/bench/tpcc -f BenchmarkTPCC --test-args '--store-dir=/tmp/benchtpcc'
//
// TODO(ajwerner): Consider moving this all to CCL to leverage the import data
// loader.
func BenchmarkTPCC(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	// Generate TPCC data if necessary.
	_, storeDir, cleanup := maybeGenerateStoreDir(b)
	defer cleanup()

	for _, impl := range []string{"literal", "optimized"} {
		b.Run(impl, func(b *testing.B) {
			var baseFlag string
			if impl == "literal" {
				baseFlag = workloadFlag("literal-implementation", "true")
			}
			for _, mixFlag := range []string{
				workloadFlag("mix", "newOrder=1"),
				workloadFlag("mix", "payment=1"),
				workloadFlag("mix", "orderStatus=1"),
				workloadFlag("mix", "delivery=1"),
				workloadFlag("mix", "stockLevel=1"),
				workloadFlag("mix", "newOrder=10,payment=10,orderStatus=1,delivery=1,stockLevel=1"),
			} {
				b.Run(mixFlag, func(b *testing.B) {
					run(b, storeDir, []string{baseFlag, mixFlag})
				})
			}
		})

	}
}

func workloadFlag(name, value string) string {
	return name + "=" + value
}

func run(b *testing.B, storeDir string, workloadFlags []string) {
	pgURL, closeServer := startCockroach(b, storeDir)
	defer closeServer()
	c, output := startClient(b, pgURL, workloadFlags)

	var s synchronizer
	s.init(c.Process.Pid)

	// Reset the timer when the client starts running queries.
	if timedOut := s.waitWithTimeout(); timedOut {
		b.Fatalf("waiting on client timed-out:\n%s", output.String())
	}
	b.ResetTimer()
	s.notify(b)

	// Stop the timer when the client stops running queries.
	s.wait()
	b.StopTimer()

	if err := c.Wait(); err != nil {
		b.Fatalf("client failed: %s\n%s\n%s", err, output.String(), output.String())
	}
}

func startCockroach(b testing.TB, storeDir string) (pgURL string, closeServer func()) {
	// Clone the store dir.
	td, engCleanup := testutils.TempDir(b)
	c, output := cloneEngine.
		withEnv(srcEngineEnvVar, storeDir).
		withEnv(dstEngineEnvVar, td).
		exec()
	if err := c.Run(); err != nil {
		b.Fatalf("failed to clone engine: %s\n%s", err, output.String())
	}

	// Start the server.
	s := serverutils.StartServerOnly(b, base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{{Path: td}},
	})
	logstore.DisableSyncRaftLog.Override(context.Background(), &s.SystemLayer().ClusterSettings().SV, true)

	// Generate a PG URL.
	u, urlCleanup, err := pgurlutils.PGUrlE(
		s.AdvSQLAddr(), b.TempDir(), url.User("root"),
	)
	if err != nil {
		b.Fatalf("failed to create pgurl: %s", err)
	}
	u.Path = databaseName

	return u.String(), func() {
		engCleanup()
		s.Stopper().Stop(context.Background())
		urlCleanup()
	}
}

func startClient(
	b *testing.B, pgURL string, workloadFlags []string,
) (c *exec.Cmd, output *synchronizedBuffer) {
	c, output = runClient.
		withEnv(nEnvVar, b.N).
		withEnv(pgurlEnvVar, pgURL).
		exec(workloadFlags...)
	if err := c.Start(); err != nil {
		b.Fatalf("failed to start client: %s\n%s", err, output.String())
	}
	return c, output
}

type synchronizer struct {
	pid    int // the PID of the process to coordinate with
	waitCh chan os.Signal
}

func (c *synchronizer) init(pid int) {
	c.pid = pid
	c.waitCh = make(chan os.Signal, 1)
	signal.Notify(c.waitCh, syscall.SIGUSR1)
}

func (c synchronizer) notify(t testing.TB) {
	if err := syscall.Kill(c.pid, syscall.SIGUSR1); err != nil {
		t.Fatalf("failed to notify process %d: %s", c.pid, err)
	}
}

func (c synchronizer) wait() {
	<-c.waitCh
}

func (c synchronizer) waitWithTimeout() (timedOut bool) {
	select {
	case <-c.waitCh:
		return false
	case <-time.After(5 * time.Second):
		return true
	}
}
