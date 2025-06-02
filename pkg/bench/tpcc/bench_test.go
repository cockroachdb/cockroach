// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tpcc

import (
	"context"
	"net/url"
	"os/exec"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/workload"
	_ "github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
)

const (
	dbName = "tpcc"
	nodes  = 3
)

// BenchmarkTPCC runs TPC-C transactions against a single warehouse. It runs the
// client side of the workload in a subprocess so that the client overhead is
// not included in CPU and heap profiles.
func BenchmarkTPCC(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	// Setup the c once for all benchmarks.
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

func run(b *testing.B, pgURL string, workloadFlags []string) {
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

// startCluster starts a cluster and initializes the workload data.
func startCluster(b testing.TB) (_ serverutils.TestClusterInterface, pgURL string) {
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

	r := sqlutils.MakeSQLRunner(c.ServerConn(0))
	r.Exec(b, "CREATE DATABASE "+dbName)
	r.Exec(b, "USE "+dbName)
	tpcc, err := workload.Get("tpcc")
	if err != nil {
		b.Fatal(err)
	}
	gen := tpcc.New().(interface {
		workload.Flagser
		workload.Hookser
		workload.Generator
	})
	if err := gen.Flags().Parse([]string{"--db=" + dbName}); err != nil {
		b.Fatal(err)
	}
	if err := gen.Hooks().Validate(); err != nil {
		b.Fatal(err)
	}

	var l workloadsql.InsertsDataLoader
	if _, err := workloadsql.Setup(ctx, c.ServerConn(0), gen, l); err != nil {
		b.Fatal(err)
	}

	return c, u.String()
}

func startClient(
	b *testing.B, pgURL string, workloadFlags []string,
) (c *exec.Cmd, output *synchronizedBuffer) {
	c, output = runClient.
		withEnv(nEnvVar, b.N).
		withEnv(pgURLEnvVar, pgURL).
		exec(workloadFlags...)
	if err := c.Start(); err != nil {
		b.Fatalf("failed to start client: %s\n%s", err, output.String())
	}
	return c, output
}
