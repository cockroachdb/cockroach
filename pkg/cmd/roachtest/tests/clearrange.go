// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func registerClearRange(r registry.Registry) {
	for _, checks := range []bool{true, false} {
		checks := checks
		r.Add(registry.TestSpec{
			Name:  fmt.Sprintf(`clearrange/checks=%t`, checks),
			Owner: registry.OwnerStorage,
			// 5h for import, 90 for the test. The import should take closer
			// to <3:30h but it varies.
			Timeout:          5*time.Hour + 90*time.Minute,
			Cluster:          r.MakeClusterSpec(10, spec.CPU(16)),
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           registry.MetamorphicLeases,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runClearRange(ctx, t, c, checks)
			},
		})
	}
}

func runClearRange(ctx context.Context, t test.Test, c cluster.Cluster, aggressiveChecks bool) {
	t.Status("restoring fixture")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
	m := c.NewDeprecatedMonitor(ctx)
	m.Go(func(ctx context.Context) error {
		// NB: on a 10 node cluster, this should take well below 3h.
		tBegin := timeutil.Now()
		c.Run(ctx, option.WithNodes(c.Node(1)), "./cockroach", "workload", "fixtures", "import", "bank",
			"--payload-bytes=10240", "--ranges=10", "--rows=65104166", "--seed=4", "--db=bigbank", "{pgurl:1}")
		t.L().Printf("import took %.2fs", timeutil.Since(tBegin).Seconds())
		return nil
	})
	m.Wait()
	c.Stop(ctx, t.L(), option.DefaultStopOpts())
	t.Status()

	settings := install.MakeClusterSettings()
	if aggressiveChecks {
		// Run with an env var that runs a synchronous consistency check after each rebalance and merge.
		// This slows down merges, so it might hide some races.
		//
		// NB: the below invocation was found to actually make it to the server at the time of writing.
		settings.Env = append(settings.Env, "COCKROACH_CONSISTENCY_AGGRESSIVE=true")
	}

	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings)
	m = c.NewDeprecatedMonitor(ctx)

	// Also restore a much smaller table. We'll use it to run queries against
	// the cluster after having dropped the large table above, verifying that
	// the  cluster still works.
	t.Status(`restoring tiny table`)
	defer t.WorkerStatus()

	// Use a 120s connect timeout to work around the fact that the server will
	// declare itself ready before it's actually 100% ready. See:
	// https://github.com/cockroachdb/cockroach/issues/34897#issuecomment-465089057
	c.Run(ctx, option.WithNodes(c.Node(1)), `COCKROACH_CONNECT_TIMEOUT=120 ./cockroach sql --url={pgurl:1} -e "DROP DATABASE IF EXISTS tinybank"`)
	c.Run(ctx, option.WithNodes(c.Node(1)), "./cockroach", "workload", "fixtures", "import", "bank", "--db=tinybank",
		"--payload-bytes=100", "--ranges=10", "--rows=800", "--seed=1", "{pgurl:1}")

	t.Status()

	bigBankSpan, err := getKeyspanForTable(ctx, t, c, 1, "bigbank.bank")
	require.NoError(t, err)
	t.L().Printf("bigbank DB ID: %s (%x - %x)", bigBankSpan, bigBankSpan.Key, bigBankSpan.EndKey)
	getBigBankStats := func() spanStats {
		stats := getSpanStats(ctx, t, c, 1, bigBankSpan)
		t.L().Printf("bigbank: %d ranges, %s disk, %s live, %s total",
			stats.rangeCount,
			humanizeutil.IBytes(stats.approximateDiskBytes),
			humanizeutil.IBytes(stats.liveBytes),
			humanizeutil.IBytes(stats.totalBytes))
		return stats
	}

	m.Go(func(ctx context.Context) error {
		c.Run(ctx, option.WithNodes(c.Node(1)), `./cockroach workload init kv {pgurl:1}`)
		c.Run(ctx, option.WithNodes(c.All()), fmt.Sprintf(`./cockroach workload run kv --concurrency=32 --duration=1h --tolerate-errors {pgurl%s}`, c.All()))
		return nil
	})
	m.Go(func(ctx context.Context) error {
		conn := c.Conn(ctx, t.L(), 1)
		defer conn.Close()

		if _, err := conn.ExecContext(ctx, `SET CLUSTER SETTING kv.range_merge.queue_enabled = true`); err != nil {
			return err
		}

		// Merge as fast as possible to put maximum stress on the system.
		if _, err := conn.ExecContext(ctx, `SET CLUSTER SETTING kv.range_merge.queue_interval = '0s'`); err != nil {
			return err
		}

		// Collect the stats before dropping the table. getBigBanksStats will
		// print them out.
		_ = getBigBankStats()

		t.WorkerStatus("dropping table")
		defer t.WorkerStatus()

		// Set a low TTL so that the ClearRange-based cleanup mechanism can kick in earlier.
		// This could also be done after dropping the table.
		if _, err := conn.ExecContext(ctx, `ALTER TABLE bigbank.bank CONFIGURE ZONE USING gc.ttlseconds = 1200`); err != nil {
			return err
		}

		t.WorkerStatus("computing span stats")
		preDropBankStats := getBigBankStats()

		t.WorkerStatus("dropping bank table")
		if _, err := conn.ExecContext(ctx, `DROP TABLE bigbank.bank`); err != nil {
			return err
		}

		curBankStats := getBigBankStats()
		progressFn := func() float64 {
			// Compute progress as a float [0, 1.0].
			//
			// We compute the progress in terms of the number of ranges and the
			// amount of disk space, relative to the stats we computed
			// immediately after dropping the table. That is:
			//
			//    1 - (current / initial)
			//
			// The range count progress subtracts 1 from each count to account
			// for the expectation that one range will always remain.
			//
			// We compute the overall progress as the minimum of the two metrics'
			// progress.
			mergeProgress := 1 - (float64(curBankStats.rangeCount-1) /
				float64(preDropBankStats.rangeCount-1))
			diskProgress := 1 - (float64(curBankStats.approximateDiskBytes) /
				float64(preDropBankStats.approximateDiskBytes))
			return max(0, min(mergeProgress, diskProgress))
		}
		// Terminate when progress is 0.975 or greater. That is, we've reclaimed
		// 97.5% of the disk space and merged 97.5% of the ranges.
		for progress := progressFn(); progress < 0.975; progress = progressFn() {
			t.WorkerProgress(progress)

			var count int
			// NB: context cancellation in QueryRowContext does not work as expected.
			// See #25435.
			if _, err := conn.ExecContext(ctx, `SET statement_timeout = '5s'`); err != nil {
				return err
			}
			// If we can't aggregate over 80kb in 5s, the database is far from usable.
			if err := conn.QueryRowContext(ctx, `SELECT count(*) FROM tinybank.bank`).Scan(&count); err != nil {
				return err
			}

			t.WorkerStatus("progress ", progress, " (", curBankStats.rangeCount, " ranges, ",
				humanizeutil.IBytes(curBankStats.approximateDiskBytes), " disk usage)")
			select {
			case <-time.After(time.Minute):
			case <-ctx.Done():
				return ctx.Err()
			}
			curBankStats = getBigBankStats()
		}
		t.WorkerStatus("reclamation condition met")
		return nil
	})
	m.Wait()
}

func getKeyspanForTable(
	ctx context.Context, t test.Test, c cluster.Cluster, n int, tbl string,
) (roachpb.Span, error) {
	conn := c.Conn(ctx, t.L(), n)
	defer conn.Close()
	var startKey, endKey roachpb.Key
	err := conn.QueryRow(`SELECT `+
		`crdb_internal.table_span($1::regclass::oid::int)[1] AS start_key, `+
		`crdb_internal.table_span($1::regclass::oid::int)[2] AS end_key`,
		tbl).Scan(&startKey, &endKey)
	return roachpb.Span{
		Key:    startKey,
		EndKey: endKey,
	}, err
}

type spanStats struct {
	rangeCount           int
	approximateDiskBytes int64
	liveBytes            int64
	totalBytes           int64
}

func getSpanStats(
	ctx context.Context, t test.Test, c cluster.Cluster, n int, span roachpb.Span,
) spanStats {
	conn := c.Conn(ctx, t.L(), n)
	defer conn.Close()

	var stats spanStats
	err := conn.QueryRow(
		`SELECT `+
			`(stats->'range_count')::int AS range_count, `+
			`(stats->'approximate_disk_bytes')::int AS approximate_disk_bytes, `+
			`(stats->'approximate_total_stats'->'live_bytes')::int AS live_bytes, `+
			`(stats->'approximate_total_stats'->'key_bytes')::int + (stats->'approximate_total_stats'->'val_bytes')::int AS total_bytes `+
			`FROM crdb_internal.tenant_span_stats(ARRAY(SELECT($1::bytes, $2::bytes)))`,
		span.Key, span.EndKey).
		Scan(
			&stats.rangeCount,
			&stats.approximateDiskBytes,
			&stats.liveBytes,
			&stats.totalBytes,
		)
	require.NoError(t, err)
	return stats
}
