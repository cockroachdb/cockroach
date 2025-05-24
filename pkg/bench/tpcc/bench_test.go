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
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testfixtures"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	_ "github.com/cockroachdb/cockroach/pkg/workload/tpcc"
)

// BenchmarkTPCC runs TPC-C transactions against a single warehouse. It runs the
// client side of the workload in a subprocess so that the client overhead is
// not included in CPU and heap profiles.
//
// The benchmark will generate the schema and table data for a single warehouse,
// using a reusable store directory. In future runs the cockroach server will
// clone and use the store directory, rather than regenerating the schema and
// data. This enables faster iteration when re-running the benchmark.
func BenchmarkTPCC(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	// Reuse or generate TPCC data.
	storeName := "bench_tpcc_store_" + storage.MinimumSupportedFormatVersion.String()
	storeDir := testfixtures.ReuseOrGenerate(b, storeName, func(dir string) {
		c, output := generateStoreDir.withEnv(storeDirEnvVar, dir).exec()
		if err := c.Run(); err != nil {
			b.Fatalf("failed to generate store dir: %s\n%s", err, output.String())
		}
	})

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
					run(b, storeDir, []string{impl.flag, mix.flag})
				})
			}
		})

	}
}

func run(b *testing.B, storeDir string, workloadFlags []string) {
	server, pgURL := startCockroach(b, storeDir)
	defer server.Stopper().Stop(context.Background())
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

func startCockroach(
	b testing.TB, storeDir string,
) (server serverutils.TestServerInterface, pgURL string) {
	// Clone the store dir.
	td := b.TempDir()
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

	// Generate a PG URL.
	u, urlCleanup, err := pgurlutils.PGUrlE(
		s.AdvSQLAddr(), b.TempDir(), url.User("root"),
	)
	if err != nil {
		b.Fatalf("failed to create pgurl: %s", err)
	}
	u.Path = databaseName
	s.Stopper().AddCloser(stop.CloserFn(urlCleanup))

	return s, u.String()
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
