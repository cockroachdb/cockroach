// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tpcc

import (
	"bytes"
	"context"
	"flag"
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
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	_ "github.com/cockroachdb/cockroach/pkg/workload/tpcc"
)

const storePath = "store"

var rewrite = flag.Bool("rewrite", false,
	"regenerate store files containing the TPC-C schema and data",
)

// BenchmarkTPCC runs TPC-C transactions against a single warehouse. It runs the
// client side of the workload in a subprocess so that the client overhead is
// not included in CPU and heap profiles.
//
// The benchmark will load the data for the single warehouse from the
// testdata/store directory. This directory needs to be populated by running the
// benchmark once with the --rewrite flag. When running each sub-benchmark, the
// store directory will be copied to an in-memory store. This enabled faster
// iteration when re-running the benchmark.
//
// For example, first generate the store directory with:
//
//	./dev bench pkg/bench/tpcc -f 'BenchmarkTPCC/literal/mix=newOrder=1$' --rewrite
//
// Future benchmark runs will reuse the store directory:
//
//	./dev bench pkg/bench/tpcc -f BenchmarkTPCC
//
// To regenerate the store direct, simply run the benchmark with the --rewrite
// flag again.
func BenchmarkTPCC(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	// Generate TPCC data if requested.
	if *rewrite {
		storeDir := datapathutils.TestDataPath(b, storePath)
		if err := os.RemoveAll(storeDir); err != nil {
			b.Fatalf("failed to clear store dir: %s", err)
		}
		cmd, stdout := generateStoreDir.exec()
		if err := cmd.Run(); err != nil {
			b.Fatalf("failed to generate store dir: %s\n%s", err, stdout.String())
		}
	}

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
					run(b, []string{baseFlag, mixFlag})
				})
			}
		})

	}
}

func workloadFlag(name, value string) string {
	return name + "=" + value
}

func run(b *testing.B, workloadFlags []string) {
	pgURL, closeServer := startCockroach(b)
	defer closeServer()
	cmd, stdout := startClient(b, pgURL, workloadFlags)

	var s synchronizer
	s.init(cmd.Process.Pid)

	// Reset the timer when the client starts running queries.
	if timedOut := s.waitWithTimeout(); timedOut {
		b.Fatalf("waiting on client timed-out:\n%s", stdout.String())
	}
	b.ResetTimer()
	s.notify(b)

	// Stop the timer when the client stops running queries.
	s.wait()
	b.StopTimer()
	s.notify(b)

	if err := cmd.Wait(); err != nil {
		b.Fatalf("client failed: %s\n%s\n%s", err, stdout.String(), stdout.String())
	}
}

func startCockroach(b testing.TB) (pgURL string, closeServer func()) {
	ctx := context.Background()

	// Clone the store dir.
	td, engCleanup := testutils.TempDir(b)
	cmd, stdout := cloneEngine.withEnv(dstEngineEnvVar, td).exec()
	if err := cmd.Run(); err != nil {
		b.Fatalf("failed to clone engine: %s\n%s", err, stdout.String())
	}

	// Start the server.
	s, _, _ := serverutils.StartServer(b, base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{{Path: td}},
	})
	logstore.DisableSyncRaftLog.Override(ctx, &s.SystemLayer().ClusterSettings().SV, true)

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
		s.Stopper().Stop(ctx)
		urlCleanup()
	}
}

func startClient(
	b *testing.B, pgURL string, workloadFlags []string,
) (_ *exec.Cmd, stdout *bytes.Buffer) {
	cmd, stdout := runClient.
		withEnv(nEnvVar, b.N).
		withEnv(pgurlEnvVar, pgURL).
		exec(workloadFlags...)
	if err := cmd.Start(); err != nil {
		b.Fatalf("failed to start client: %s\n%s", err, stdout.String())
	}
	return cmd, stdout
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
