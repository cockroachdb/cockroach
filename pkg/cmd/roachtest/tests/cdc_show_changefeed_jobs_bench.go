// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type cdcBenchConfig struct {
	tableCount             int
	nodeCount              int
	ranges                 int
	roachtestTimeout       time.Duration
	changefeedCount        int
	resolvedInterval       string
	minCheckpointFrequency string
	iterations             int
	phaseDuration          time.Duration
	sampleEvery            time.Duration
}

func registerCdcShowChangefeedJobsBench(r registry.Registry) {
	cfg := cdcBenchConfig{
		tableCount:             100,
		nodeCount:              4,
		ranges:                 25,
		roachtestTimeout:       time.Minute * 25,
		changefeedCount:        25,
		resolvedInterval:       "3s",
		minCheckpointFrequency: "30s",
		iterations:             5,
		phaseDuration:          time.Minute,
		sampleEvery:            10 * time.Second,
	}
	cfJobsSpec := r.MakeClusterSpec(cfg.nodeCount)
	r.Add(registry.TestSpec{
		Name:              "cdc/show-changefeed-jobs-bench",
		Owner:             registry.OwnerCDC,
		Cluster:           cfJobsSpec,
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		CompatibleClouds:  registry.AllClouds,
		Suites:            registry.ManualOnly,
		Timeout:           cfg.roachtestTimeout,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runCdcShowChangefeedJobsBench(ctx, t, c, cfg)
		},
	})
}

// runCdcShowChangefeedJobsBench stresses the jobs table to the point of increasing
// the latency of SHOW CHANGEFEED JOBS. The purpose is not to catch regression
// but to test potential improvements for storing/querying changefeed job info
// for this query.
func runCdcShowChangefeedJobsBench(
	ctx context.Context, t test.Test, c cluster.Cluster, cfg cdcBenchConfig,
) {
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()

	sqlDB := sqlutils.MakeSQLRunner(conn)
	sqlDB.Exec(t, "CREATE DATABASE d")
	sqlDB.Exec(t, "SET DATABASE=d")

	// Because this roachtest spins up many changefeed jobs really quickly,
	// run the adopt interval which by default only runs every 30s and adopts 10
	// jobs at a time.
	sqlDB.Exec(t, "SET CLUSTER SETTING jobs.registry.interval.adopt='5s'")

	rng, seed := randutil.NewLockedPseudoRand()
	t.L().Printf("Rand seed: %d", seed)

	group := t.NewErrorGroup()

	workloadCtx, workloadCancel := context.WithCancel(ctx)

	sqlDB.Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled=false")
	sqlDB.Exec(t, "SET CLUSTER SETTING changefeed.span_checkpoint.interval = '1s'")
	sqlDB.Exec(t, "SET CLUSTER SETTING changefeed.frontier_checkpoint_frequency = '1s'")
	sqlDB.Exec(t, "SET CLUSTER SETTING changefeed.frontier_highwater_lag_checkpoint_threshold = '100ms'")

	initCmd := fmt.Sprintf(
		"./cockroach workload init bank --tables=%d --ranges=%d --rows=%d {pgurl%s}",
		cfg.tableCount, cfg.ranges, cfg.ranges, c.Node(1))
	if err := c.RunE(ctx, option.WithNodes(c.Node(1)), initCmd); err != nil {
		t.Fatalf("failed to initialize bank tables: %v", err)
	}

	group.Go(func(ctx context.Context, _ *logger.Logger) error {
		// Keep workload running only as long as needed to cover the measurement
		// window (2 phases per iteration), with a small buffer.
		runDur := time.Duration(cfg.iterations)*2*cfg.phaseDuration + 15*time.Second
		runCmd := fmt.Sprintf(
			"./cockroach workload run bank --db=bank --concurrency=%d --tolerate-errors --duration=%s {pgurl:1-%d}",
			1, runDur, cfg.nodeCount,
		)
		if err := c.RunE(workloadCtx, option.WithNodes(c.Node(1)), runCmd); err != nil {
			if !errors.Is(err, context.Canceled) {
				return err
			}
		}
		return nil
	})

	group.Go(func(ctx context.Context, _ *logger.Logger) error {
		createChangefeeds(ctx, t, c, rng, cfg)
		return nil
	})

	group.Go(func(ctx context.Context, _ *logger.Logger) error {
		conn := c.Conn(ctx, t.L(), 1)
		defer conn.Close()

		// runAndTime runs a query and returns the duration it took to execute.
		runAndTime := func(q string) (time.Duration, error) {
			start := timeutil.Now()
			_, err := conn.ExecContext(ctx, q)
			return timeutil.Since(start), err
		}

		// runPhase runs a query for a specified duration and samples the results
		// at a specified interval.
		runPhase := func(name, query string, phaseDur, sampleEvery time.Duration) (samples int, avg time.Duration, err error) {
			deadline := timeutil.Now().Add(phaseDur)
			ticker := time.NewTicker(sampleEvery)
			defer ticker.Stop()
			var n int
			var sum time.Duration
			for timeutil.Now().Before(deadline) {
				select {
				case <-ctx.Done():
					return 0, 0, ctx.Err()
				case <-ticker.C:
					d, e := runAndTime(query)
					if e != nil {
						t.L().Printf("%s err: %v", name, e)
						continue
					}
					n++
					sum += d
				}
			}
			if n == 0 {
				return 0, 0, nil
			}
			return n, sum / time.Duration(n), nil
		}

		showJobsQ := "SELECT * FROM [SHOW JOBS] WHERE job_type = 'CHANGEFEED'"
		showCFQ := "SELECT * FROM [SHOW CHANGEFEED JOBS]"

		// Run the queries for the specified number of iterations, logging the results.
		for i := 1; i <= cfg.iterations; i++ {
			n, avg, err := runPhase("SHOW JOBS", showJobsQ, cfg.phaseDuration, cfg.sampleEvery)
			if err != nil {
				return err
			}
			t.L().Printf("[iter %d] SHOW JOBS samples=%d avg=%s", i, n, avg)

			n, avg, err = runPhase("SHOW CHANGEFEED JOBS", showCFQ, cfg.phaseDuration, cfg.sampleEvery)
			if err != nil {
				return err
			}
			t.L().Printf("[iter %d] SHOW CHANGEFEED JOBS samples=%d avg=%s", i, n, avg)
		}
		workloadCancel()
		return nil
	})

	require.NoError(t, group.WaitE())
}

// createChangefeeds creates a specified number of changefeed jobs, each
// targeting all tables in the bank database.
func createChangefeeds(
	ctx context.Context, t test.Test, c cluster.Cluster, rng *rand.Rand, cfg cdcBenchConfig,
) {
	sqlDBs := make([]*sqlutils.SQLRunner, cfg.nodeCount)
	for i := 0; i < cfg.nodeCount; i++ {
		conn := c.Conn(ctx, t.L(), i+1)
		sqlDBs[i] = sqlutils.MakeSQLRunner(conn)
		defer conn.Close() //nolint:deferloop
	}

	sqlDBs[0].Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true;`)

	tables := make([]string, cfg.tableCount)
	for i := 0; i < cfg.tableCount; i++ {
		tables[i] = fmt.Sprintf("bank.bank_%d", i)
	}
	allTables := strings.Join(tables, ", ")
	for i := 0; i < cfg.changefeedCount; i++ {
		stmt := fmt.Sprintf(
			"CREATE CHANGEFEED FOR %s INTO 'null://' "+
				"WITH updated, initial_scan='no', resolved='%s', min_checkpoint_frequency='%s'",
			allTables, cfg.resolvedInterval, cfg.minCheckpointFrequency,
		)
		sqlDBs[0].Exec(t, stmt)
	}
}
