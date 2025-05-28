// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tpcc

import (
	"context"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/jackc/pgx/v5"
)

// This file contains "internal tests" that are run by BenchmarkTPCC in a
// subprocess. They are not real tests at all, and they are skipped if the
// COCKROACH_INTERNAL_TEST environment variable is not set. These tests are run
// in a subprocess so that profiles collected while running the benchmark do not
// include the overhead of the client code.

// databaseName is the name of the database used by this test.
const databaseName = "tpcc"

// Environment variables used to communicate configuration from the benchmark
// to the client subprocess.
const (
	allowInternalTestEnvVar = "COCKROACH_INTERNAL_TEST"
	pgURLEnvVar             = "COCKROACH_PGURL"
	nEnvVar                 = "COCKROACH_N"
	srcEngineEnvVar         = "COCKROACH_SRC_ENGINE"
	dstEngineEnvVar         = "COCKROACH_DST_ENGINE"
)

var (
	benchmarkN        = envutil.EnvOrDefaultInt(nEnvVar, -1)
	allowInternalTest = envutil.EnvOrDefaultBool(allowInternalTestEnvVar, false)

	runClient = makeCmd("TestInternalRunClient", TestInternalRunClient)
)

func TestInternalRunClient(t *testing.T) {
	if !allowInternalTest {
		skip.IgnoreLint(t)
	}

	if benchmarkN <= 0 {
		t.Fatal(nEnvVar + " env var must be positive")
	}
	ctx := context.Background()

	pgURL, ok := envutil.EnvString(pgURLEnvVar, 0)
	if !ok {
		t.Fatal(pgURLEnvVar + " must be set")
	}
	ql := makeQueryLoad(t, pgURL)
	defer func() { _ = ql.Close(ctx) }()

	conn, err := pgx.Connect(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()

	// Verify the TPC-C database exists.
	if _, err := conn.Exec(ctx, "USE "+databaseName); err != nil {
		t.Fatal(databaseName + " database does not exist")
	}

	// Send a signal to the parent process and wait for an ack before
	// running queries.
	var s synchronizer
	s.init(os.Getppid())
	s.notify(t)
	if timedOut := s.waitWithTimeout(); timedOut {
		t.Fatalf("waiting on parent process timed-out")
	}

	for i := 0; i < benchmarkN; i++ {
		if err := ql.WorkerFns[0](ctx); err != nil {
			t.Fatal(err)
		}
	}

	// Notify the parent process that the benchmark has completed.
	s.notify(t)
}

func makeQueryLoad(t *testing.T, pgURL string) workload.QueryLoad {
	tpcc, err := workload.Get("tpcc")
	if err != nil {
		t.Fatal(err)
	}
	gen := tpcc.New()
	wl := gen.(interface {
		workload.Flagser
		workload.Hookser
		workload.Opser
	})
	ctx := context.Background()

	flags := append([]string{
		"--wait=0",
		"--workers=1",
		"--db=" + databaseName,
	}, flag.CommandLine.Args()...)
	if err := wl.Flags().Parse(flags); err != nil {
		t.Fatal(err)
	}

	if err := wl.Hooks().Validate(); err != nil {
		t.Fatal(err)
	}

	reg := histogram.NewRegistry(time.Minute, "tpcc")
	ql, err := wl.Ops(ctx, []string{pgURL}, reg)
	if err != nil {
		t.Fatal(err)
	}
	return ql
}
