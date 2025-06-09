// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tpcc

import (
	"bytes"
	"context"
	"flag"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	runtimepprof "runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sniffarg"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	_ "github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/google/pprof/profile"
)

const (
	dbName = "tpcc"
	nodes  = 3
)

var profileFlag = flag.String("profile", "",
	"if set, CPU and heap profiles are collected with the given file name prefix")

// BenchmarkTPCC runs TPC-C transactions against a single warehouse. Both the
// optimized and literal implementations of our TPC-C workload are benchmarked.
// There are benchmarks for running each transaction individually, as well as a
// "default" mix that runs the standard mix of TPC-C transactions.
//
// CPU and heap profiles can be collected by passing the "-profile" flag with a
// prefix for the profile file names. The profile names will include the
// benchmark names, e.g., "foo_tpcc_literal_new_order.cpu" and
// "foo_tpcc_literal_new_order.mem" will be created when running the benchmark:
//
//	./dev bench pkg/bench/tpcc -f BenchmarkTPCC/literal/new_order \
//	  --test-args '-test.benchtime=30s -profile=foo'
//
// These profiles will omit CPU samples and allocations made during the setup
// phase of the benchmark, unlike profiles collected with the "-test.cpuprofile"
// and "-test.memprofile" flags.
func BenchmarkTPCC(b *testing.B) {
	defer log.Scope(b).Close(b)

	// Setup the cluster once for all benchmarks.
	ctx := context.Background()
	tc, pgURL := startCluster(b, ctx)
	defer tc.Stopper().Stop(ctx)

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
				b.Run(mix.name, func(b *testing.B) {
					run(b, ctx, pgURL, []string{impl.flag, mix.flag})
				})
			}
		})
	}
}

// run executes the TPC-C workload with the specified workload flags.
func run(b *testing.B, ctx context.Context, pgURL string, workloadFlags []string) {
	defer startCPUProfile(b).Stop(b)
	defer startAllocsProfile(b).Stop(b)

	flags := append([]string{
		"--wait=0",
		"--workers=1",
		"--db=" + dbName,
	}, workloadFlags...)
	gen := tpccGenerator(b, flags)

	reg := histogram.NewRegistry(time.Minute, "tpcc")
	ql, err := gen.Ops(ctx, []string{pgURL}, reg)
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
) (_ serverutils.TestClusterInterface, pgURL string) {
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
		}
	}

	// Start the cluster.
	tc := serverutils.StartCluster(b, nodes, base.TestClusterArgs{
		ServerArgsPerNode: serverArgs,
		ParallelStart:     true,
	})

	// Generate a PG URL.
	u, cleanupURL := tc.ApplicationLayer(0).PGUrl(b, serverutils.DBName(dbName))
	tc.Stopper().AddCloser(stop.CloserFn(cleanupURL))

	// Create the database.
	r := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	r.Exec(b, "CREATE DATABASE "+dbName)
	r.Exec(b, "USE "+dbName)

	// Load the TPC-C workload data.
	gen := tpccGenerator(b, []string{"--db=" + dbName})
	var loader workloadsql.InsertsDataLoader
	if _, err := workloadsql.Setup(ctx, tc.ServerConn(0), gen, loader); err != nil {
		b.Fatal(err)
	}

	return tc, u.String()
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

type doneFn func(testing.TB)

func (f doneFn) Stop(b testing.TB) {
	f(b)
}

func startCPUProfile(b testing.TB) doneFn {
	prefix := *profileFlag
	if prefix == "" {
		return func(b testing.TB) {}
	}

	fileName := profileFileName(b, prefix, "cpu")
	f, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		b.Fatal(err)
	}

	if err := runtimepprof.StartCPUProfile(f); err != nil {
		b.Fatal(err)
	}

	return func(b testing.TB) {
		runtimepprof.StopCPUProfile()
		if err := f.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func startAllocsProfile(b testing.TB) doneFn {
	prefix := *profileFlag
	if prefix == "" {
		return func(b testing.TB) {}
	}

	fileName := profileFileName(b, prefix, "mem")
	diffAllocs := diffProfile(b, func() []byte {
		p := runtimepprof.Lookup("allocs")
		var buf bytes.Buffer

		runtime.GC()
		if err := p.WriteTo(&buf, 0); err != nil {
			b.Fatal(err)
		}

		return buf.Bytes()
	})

	return func(b testing.TB) {
		if sl := diffAllocs(b); len(sl) > 0 {
			if err := os.WriteFile(fileName, sl, 0644); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func diffProfile(b testing.TB, take func() []byte) func(testing.TB) []byte {
	// The below is essentially cribbed from pprof.go in net/http/pprof.

	baseBytes := take()
	if baseBytes == nil {
		return func(tb testing.TB) []byte { return nil }
	}
	pBase, err := profile.ParseData(baseBytes)
	if err != nil {
		b.Fatal(err)
	}

	return func(b testing.TB) []byte {
		pNew, err := profile.ParseData(take())
		if err != nil {
			b.Fatal(err)
		}
		pBase.Scale(-1)
		pMerged, err := profile.Merge([]*profile.Profile{pBase, pNew})
		if err != nil {
			b.Fatal(err)
		}
		pMerged.TimeNanos = pNew.TimeNanos
		pMerged.DurationNanos = pNew.TimeNanos - pBase.TimeNanos

		buf := bytes.Buffer{}
		if err := pMerged.Write(&buf); err != nil {
			b.Fatal(err)
		}
		return buf.Bytes()
	}
}

func profileFileName(b testing.TB, prefix, suffix string) string {
	var outputDir string
	if err := sniffarg.DoEnv("test.outputdir", &outputDir); err != nil {
		b.Fatal(err)
	}

	saniRE := regexp.MustCompile(`\W+`)
	testName := strings.TrimPrefix(b.Name(), "Benchmark")
	testName = strings.ToLower(testName)
	testName = saniRE.ReplaceAllString(testName, "_")

	fileName := prefix + "_" + testName + "." + suffix
	if outputDir != "" {
		fileName = filepath.Join(outputDir, fileName)
	}
	return fileName
}
