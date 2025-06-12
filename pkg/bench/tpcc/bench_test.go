// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tpcc

import (
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/bench/benchprof"
	"github.com/cockroachdb/cockroach/pkg/ccl/workloadccl"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	_ "github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
)

const (
	dbName = "tpcc"
	nodes  = 3
)

// BenchmarkTPCC runs TPC-C transactions against a single warehouse. Both the
// optimized and literal implementations of our TPC-C workload are benchmarked.
// There are benchmarks for running each transaction individually, as well as a
// "default" mix that runs the standard mix of TPC-C transactions.
//
// If CPU or heap profiles are requested with the "-test.cpuprofile" or
// "-test.memprofile" flags, the benchmark will "hijack" the standard profiles
// and create new profiles that omit CPU samples and allocations made during the
// setup phase of the benchmark.
//
// The profile names will include the prefix of the profile flags and the
// benchmark names. For example, the profiles "foo_tpcc_literal_new_order.cpu"
// and "foo_tpcc_literal_new_order.mem" will be created when running the
// benchmark:
//
//	./dev bench pkg/bench/tpcc -f BenchmarkTPCC/literal/new_order \
//	  --test-args '-test.cpuprofile=foo.cpu -test.memprofile=foo.mem'
//
// NB: The "foo.cpu" file will not be created because we must stop the global
// CPU profiler in order to collect profiles that omit setup samples. The
// "foo.mem" file will created and include all allocations made during the
// entire duration of the benchmark.
func BenchmarkTPCC(b *testing.B) {
	defer log.Scope(b).Close(b)

	for _, impl := range []struct{ name, flag string }{
		{"literal", "--literal-implementation=true"},
		{"optimized", "--literal-implementation=false"},
	} {
		b.Run(impl.name, func(b *testing.B) {
			for _, mix := range []struct{ name, flag string }{
				{"new_order", "--mix=newOrder=1"},
				{"payment", "--mix=payment=1"},
				{"order_status", "--mix=orderStatus=1"},
				{"delivery", "--mix=delivery=1"},
				{"stock_level", "--mix=stockLevel=1"},
				{"default", "--mix=newOrder=10,payment=10,orderStatus=1,delivery=1,stockLevel=1"},
			} {
				var tc serverutils.TestClusterInterface
				var pgURLs [nodes]string
				b.Run(mix.name, func(b *testing.B) {
					ctx := context.Background()
					// TODO(mgartner): This is a hack to avoid repeatedly
					// setting up the cluster for a single benchmark run. Go's
					// benchmarking tooling will run a benchmark with b.N=1
					// first, and ramp up b.N until the benchmark hits a time
					// threshold. This means that the setup code will run
					// multiple times for a single benchmark result. To avoid
					// the high latency this would incur, we only run the setup
					// code when on the first execution of each iteration of the
					// benchmark, when b.N=1. If the benchmark is run with
					// --count greater than 1, then b.N will be reset to 1 for
					// each iteration, and a new cluster will be created,
					// ensuring benchmark results across interations remain
					// independent. This won't be necessary in Go 1.24+ when
					// b.Loop can be used instead.
					if b.N == 1 && tc != nil {
						tc.Stopper().Stop(ctx)
						tc = nil
					}
					// Setup the cluster.
					if tc == nil {
						tc, pgURLs = startCluster(b, ctx)
					}
					run(b, ctx, pgURLs, []string{impl.flag, mix.flag})
				})
				if tc != nil {
					tc.Stopper().Stop(context.Background())
				}
			}
		})
	}
}

// run executes the TPC-C workload with the specified workload flags.
func run(b *testing.B, ctx context.Context, pgURLs [nodes]string, workloadFlags []string) {
	defer benchprof.StartAllProfiles(b).Stop(b)

	flags := append([]string{
		"--wait=0",
		"--workers=1",
		"--db=" + dbName,
	}, workloadFlags...)
	gen := tpccGenerator(b, flags)

	reg := histogram.NewRegistry(time.Minute, "tpcc")
	ql, err := gen.Ops(ctx, pgURLs[:], reg)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ql.WorkerFns[0](ctx); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// startCluster starts a cluster and initializes the workload data.
func startCluster(
	b *testing.B, ctx context.Context,
) (_ serverutils.TestClusterInterface, pgURLs [nodes]string) {
	st := cluster.MakeTestingClusterSettings()

	// NOTE: disabling background work makes the benchmark more predictable, but
	// also moderately less realistic.
	ts.TimeseriesStorageEnabled.Override(ctx, &st.SV, false)
	stats.AutomaticStatisticsClusterMode.Override(ctx, &st.SV, false)

	const cacheSize = 2 * 1024 * 1024 * 1024 // 2GB
	serverArgs := make(map[int]base.TestServerArgs, nodes)
	for i := 0; i < nodes; i++ {
		serverArgs[i] = base.TestServerArgs{
			Settings:  st,
			CacheSize: cacheSize,
			Knobs: base.TestingKnobs{
				DialerKnobs: nodedialer.DialerTestingKnobs{
					// Disable local client optimization.
					TestingNoLocalClientOptimization: true,
				},
			},
			SQLMemoryPoolSize: 1 << 30, // 1 GiB
		}
	}

	// Start the cluster.
	tc := serverutils.StartCluster(b, nodes, base.TestClusterArgs{
		ServerArgsPerNode: serverArgs,
		ParallelStart:     true,
	})

	// Generate PG URLs.
	for node := 0; node < nodes; node++ {
		pgURL, cleanupURL := tc.ApplicationLayer(0).PGUrl(b, serverutils.DBName(dbName))
		pgURLs[node] = pgURL.String()
		tc.Stopper().AddCloser(stop.CloserFn(cleanupURL))
	}

	// Create the database.
	r := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	r.Exec(b, "CREATE DATABASE "+dbName)
	r.Exec(b, "USE "+dbName)

	// Load the TPC-C workload data.
	gen := tpccGenerator(b, []string{"--db=" + dbName})
	var loader workloadccl.ImportDataLoader
	sqlDB, err := gosql.Open(`cockroach`, strings.Join(pgURLs[:], " "))
	if err != nil {
		b.Fatal(err)
	}
	if _, err := workloadsql.Setup(ctx, sqlDB, gen, loader); err != nil {
		b.Fatal(err)
	}

	// Collect stats on each table.
	for _, table := range gen.Tables() {
		r.Exec(b, fmt.Sprintf("ANALYZE %q", table.Name))
	}

	return tc, pgURLs
}

type generator interface {
	workload.Flagser
	workload.Hookser
	workload.Generator
	workload.Opser
	SetOutput(io.Writer)
}

func tpccGenerator(b *testing.B, flags []string) generator {
	tpcc, err := workload.Get("tpcc")
	if err != nil {
		b.Fatal(err)
	}
	gen := tpcc.New().(generator)
	gen.SetOutput(io.Discard)
	if err := gen.Flags().Parse(flags); err != nil {
		b.Fatal(err)
	}
	if err := gen.Hooks().Validate(); err != nil {
		b.Fatal(err)
	}
	return gen
}
