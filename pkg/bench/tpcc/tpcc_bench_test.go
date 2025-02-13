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
	"io"
	"net"
	"net/http"
	"net/url"
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
// TODO(ajwerner): Consider moving this all to CCL to leverage the import data
// loader.
func BenchmarkTPCC(b *testing.B) {
	defer leaktest.AfterTest(b)()

	_, engPath, cleanup := maybeGenerateStoreDir(b)
	defer cleanup()
	baseOptions := options{
		serverArgs(func(
			b testing.TB,
		) (_ base.TestServerArgs, cleanup func()) {
			td, cleanup := testutils.TempDir(b)
			require.NoError(b, cloneEngine.exec(cmdEnv{
				{srcEngineEnvVar, engPath},
				{dstEngineEnvVar, td},
			}).Run())
			return base.TestServerArgs{
				StoreSpecs: []base.StoreSpec{{Path: td}},
			}, cleanup
		}),
		setupServer(func(tb testing.TB, s serverutils.TestServerInterface) {
			logstore.DisableSyncRaftLog.Override(context.Background(), &s.SystemLayer().ClusterSettings().SV, true)
		}),
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
			defer log.Scope(b).Close(b)
			newBenchmark(append(opts, baseOptions...)).run(b)
		})
	}
}

type benchmark struct {
	benchmarkConfig

	pgURL    string
	eventURL string

	closers []func()
	closed  chan struct{}
	eventCh chan event
}

func newBenchmark(opts ...options) *benchmark {
	bm := benchmark{
		eventCh: make(chan event),
		closed:  make(chan struct{}),
	}
	for _, opt := range opts {
		opt.apply(&bm.benchmarkConfig)
	}
	bm.closers = append(bm.closers, func() { close(bm.closed) })
	return &bm
}

func (bm *benchmark) run(b *testing.B) {
	defer bm.close()
	bm.startCockroach(b)
	bm.startEventServer(b)
	wait := bm.startClient(b)
	{
		ev := bm.getEvent(b, runStartEvent)
		b.Log("running benchmark")
		b.ResetTimer()
		close(ev)
	}
	{
		doneEv := bm.getEvent(b, runDoneEvent)
		b.StopTimer()
		close(doneEv)
	}
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

func (bm *benchmark) startEventServer(b testing.TB) {
	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(b, err)
	bm.closers = append(bm.closers, func() { _ = l.Close() })
	bm.eventURL = "http://" + l.Addr().String()
	go func() { _ = http.Serve(l, bm) }()
}

func (bm *benchmark) startClient(b *testing.B) (wait func() error) {
	cmd := runClient.exec(cmdEnv{
		{nEnvVar, b.N},
		{pgurlEnvVar, bm.pgURL},
		{eventEnvVar, bm.eventURL},
	}, bm.workloadFlags...)
	require.NoError(b, cmd.Start())
	bm.closers = append(bm.closers,
		func() { _ = cmd.Process.Kill(); _ = cmd.Wait() },
	)
	return cmd.Wait
}

// event is passed to the benchmark's eventCh when an HTTP request
// comes in on the event server. The request blocks until the channel
// is closed.
type event struct {
	name string
	done chan struct{}
}

func (bm *benchmark) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		bm.addEvent(err.Error())
		w.WriteHeader(500)
		return
	}
	<-bm.addEvent(string(data))
}

func (bm *benchmark) addEvent(name string) <-chan struct{} {
	ev := event{name: name, done: make(chan struct{})}
	select {
	case bm.eventCh <- ev:
	case <-bm.closed:
		close(ev.done)
	}
	return ev.done
}

func (bm *benchmark) getEvent(b *testing.B, evName string) chan<- struct{} {
	ev := <-bm.eventCh
	require.Equal(b, evName, ev.name)
	return ev.done
}
