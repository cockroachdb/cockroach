// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tpcc

import (
	"context"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
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

// BenchmarkTPCC runs TPC-C transactions against a single warehouse.
func BenchmarkTPCC(b *testing.B) {
	defer log.Scope(b).Close(b)

	// Setup the cluster once for all benchmarks.
	c, pgURL := startCluster(b)
	defer c.Stopper().Stop(context.Background())

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
					run(b, pgURL, []string{impl.flag, mix.flag})
				})
			}
		})

	}
}

// run executes the TPC-C workload with the specified workload flags.
func run(b *testing.B, pgURL string, workloadFlags []string) {
	ctx := context.Background()

	flags := append([]string{
		"--wait=0",
		"--workers=1",
		"--db=" + dbName,
	}, workloadFlags...)
	gen := tpccGenerator(b, flags)

	// Temporarily redirect stdout to /dev/null to suppress the workload's
	// output during the benchmark.
	restoreStdout := redirectStdoutToDevNull(b)
	defer restoreStdout()

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
func startCluster(b *testing.B) (_ serverutils.TestClusterInterface, pgURL string) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	// NOTE: disabling background work makes the benchmark more predictable, but
	// also moderately less realistic.
	ts.TimeseriesStorageEnabled.Override(context.Background(), &st.SV, false)
	stats.AutomaticStatisticsClusterMode.Override(context.Background(), &st.SV, false)

	const cacheSize = 2 * 1024 * 1024 * 1024 // 2GB
	serverArgs := make(map[int]base.TestServerArgs, nodes)
	for i := 0; i < nodes; i++ {
		serverArgs[i] = base.TestServerArgs{
			Settings:  st,
			CacheSize: cacheSize,
		}
	}

	// Start the cluster.
	c := serverutils.StartCluster(b, nodes, base.TestClusterArgs{
		ServerArgsPerNode: serverArgs,
		ParallelStart:     true,
	})

	// Generate a PG URL.
	u, urlCleanup, err := pgurlutils.PGUrlE(
		c.Server(0).AdvSQLAddr(), b.TempDir(), url.User("root"),
	)
	if err != nil {
		b.Fatalf("failed to create pgurl: %s", err)
	}
	u.Path = dbName
	c.Stopper().AddCloser(stop.CloserFn(urlCleanup))

	// Create the database.
	r := sqlutils.MakeSQLRunner(c.ServerConn(0))
	r.Exec(b, "CREATE DATABASE "+dbName)
	r.Exec(b, "USE "+dbName)

	// Load the TPC-C workload data.
	gen := tpccGenerator(b, []string{"--db=" + dbName})
	var loader workloadsql.InsertsDataLoader
	if _, err := workloadsql.Setup(ctx, c.ServerConn(0), gen, loader); err != nil {
		b.Fatal(err)
	}

	return c, u.String()
}

type generator interface {
	workload.Flagser
	workload.Hookser
	workload.Generator
	workload.Opser
}

func tpccGenerator(b *testing.B, flags []string) generator {
	tpcc, err := workload.Get("tpcc")
	if err != nil {
		b.Fatal(err)
	}
	gen := tpcc.New().(generator)
	if err := gen.Flags().Parse(flags); err != nil {
		b.Fatal(err)
	}
	if err := gen.Hooks().Validate(); err != nil {
		b.Fatal(err)
	}
	return gen
}

// redirectStdoutToDevNull redirects stdout to /dev/null.
func redirectStdoutToDevNull(b *testing.B) (restoreStdout func()) {
	old := os.Stdout
	var err error
	if os.Stdout, err = os.Open(os.DevNull); err != nil {
		b.Fatal(err)
	}
	return func() {
		_ = os.Stdout.Close()
		os.Stdout = old
	}
}
