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
	"os/signal"
	"syscall"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	_ "github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/stretchr/testify/require"
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

	_, engPath, cleanup := maybeGenerateStoreDir(b)
	defer cleanup()
	baseOptions := [...]option{
		serverArgs(func(
			b testing.TB,
		) (_ base.TestServerArgs, cleanup func()) {
			td, cleanup := testutils.TempDir(b)
			cmd, output := cloneEngine.
				withEnv(srcEngineEnvVar, engPath).
				withEnv(dstEngineEnvVar, td).
				exec()
			if err := cmd.Run(); err != nil {
				b.Fatalf("failed to clone engine: %s\n%s", err, output.String())
			}
			return base.TestServerArgs{
				StoreSpecs: []base.StoreSpec{{Path: td}},
			}, cleanup
		}),
		setupServer(func(tb testing.TB, s serverutils.TestServerInterface) {
			logstore.DisableSyncRaftLog.Override(context.Background(), &s.SystemLayer().ClusterSettings().SV, true)
		}),
	}

	for _, impl := range []string{"literal", "optimized"} {
		b.Run(impl, func(b *testing.B) {
			implOpts := baseOptions[:]
			if impl == "literal" {
				implOpts = append(implOpts, workloadFlag("literal-implementation", "true"))
			}
			for _, opts := range []options{
				{workloadFlag("mix", "newOrder=1")},
				{workloadFlag("mix", "payment=1")},
				{workloadFlag("mix", "orderStatus=1")},
				{workloadFlag("mix", "delivery=1")},
				{workloadFlag("mix", "stockLevel=1")},
				{workloadFlag("mix", "newOrder=10,payment=10,orderStatus=1,delivery=1,stockLevel=1")},
			} {
				b.Run(opts.String(), func(b *testing.B) {
					newBenchmark(append(opts, implOpts...)).run(b)
				})
			}
		})

	}
}

type benchmark struct {
	benchmarkConfig
	pgURL   string
	closers []func()
}

func newBenchmark(opts ...options) *benchmark {
	var bm benchmark
	for _, opt := range opts {
		opt.apply(&bm.benchmarkConfig)
	}
	return &bm
}

func (bm *benchmark) run(b *testing.B) {
	defer bm.close()

	bm.startCockroach(b)
	pid, wait := bm.startClient(b)

	var s synchronizer
	s.init(pid)

	// Reset the timer when the client starts running queries.
	s.wait()
	b.ResetTimer()
	s.notify(b)

	// Stop the timer when the client stops running queries.
	s.wait()
	b.StopTimer()

	require.NoError(b, wait())
}

func (bm *benchmark) close() {
	for i := len(bm.closers) - 1; i >= 0; i-- {
		bm.closers[i]()
	}
}

func (bm *benchmark) startCockroach(b testing.TB) {
	args, cleanup := bm.argsGenerator(b)
	bm.closers = append(bm.closers, cleanup)

	s, db, _ := serverutils.StartServer(b, args)
	bm.closers = append(bm.closers, func() {
		s.Stopper().Stop(context.Background())
	})

	for _, fn := range bm.setupServer {
		fn(b, s)
	}
	for _, stmt := range bm.setupStmts {
		sqlutils.MakeSQLRunner(db).Exec(b, stmt)
	}

	pgURL, cleanup, err := pgurlutils.PGUrlE(
		s.AdvSQLAddr(), b.TempDir(), url.User("root"),
	)
	require.NoError(b, err)
	pgURL.Path = databaseName
	bm.pgURL = pgURL.String()
	bm.closers = append(bm.closers, cleanup)
}

func (bm *benchmark) startClient(b *testing.B) (pid int, wait func() error) {
	cmd, output := runClient.
		withEnv(nEnvVar, b.N).
		withEnv(pgurlEnvVar, bm.pgURL).
		exec(bm.workloadFlags...)
	if err := cmd.Start(); err != nil {
		b.Fatalf("failed to start client: %s\n%s", err, output.String())
	}
	bm.closers = append(bm.closers,
		func() { _ = cmd.Process.Kill(); _ = cmd.Wait() },
	)
	return cmd.Process.Pid, cmd.Wait
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

func (c synchronizer) notifyAndWait(t testing.TB) {
	c.notify(t)
	c.wait()
}
