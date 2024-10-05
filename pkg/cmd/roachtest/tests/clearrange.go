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
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	// Using a separate clearrange test on zfs instead of randomly
	// using the same test, cause the Timeout might be different,
	// and may need to be tweaked.
	r.Add(registry.TestSpec{
		Name:  `clearrange/zfs/checks=true`,
		Owner: registry.OwnerStorage,
		// 5h for import, 120 for the test. The import should take closer
		// to <3:30h but it varies.
		Timeout:           5*time.Hour + 120*time.Minute,
		Cluster:           r.MakeClusterSpec(10, spec.CPU(16), spec.SetFileSystem(spec.Zfs)),
		CompatibleClouds:  registry.OnlyGCE,
		Suites:            registry.Suites(registry.Nightly),
		EncryptionSupport: registry.EncryptionMetamorphic,
		Leases:            registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runClearRange(ctx, t, c, true /* checks */)
		},
	})
}

func runClearRange(ctx context.Context, t test.Test, c cluster.Cluster, aggressiveChecks bool) {
	t.Status("restoring fixture")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
	m := c.NewMonitor(ctx)
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
	m = c.NewMonitor(ctx)

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

	// Set up a convenience function that we can call to learn the number of
	// ranges for the bigbank.bank table (even after it's been dropped).
	numBankRanges := func() func() int {
		conn := c.Conn(ctx, t.L(), 1)
		defer conn.Close()

		var startHex string
		if err := conn.QueryRow(
			`SELECT to_hex(raw_start_key)
FROM [SHOW RANGES FROM TABLE bigbank.bank WITH KEYS]
ORDER BY raw_start_key ASC LIMIT 1`,
		).Scan(&startHex); err != nil {
			t.Fatal(err)
		}
		return func() int {
			conn := c.Conn(ctx, t.L(), 1)
			defer conn.Close()
			var n int
			if err := conn.QueryRow(
				`SELECT count(*) FROM crdb_internal.ranges_no_leases WHERE substr(to_hex(start_key), 1, length($1::string)) = $1`, startHex,
			).Scan(&n); err != nil {
				t.Fatal(err)
			}
			return n
		}
	}()

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

		t.WorkerStatus("dropping table")
		defer t.WorkerStatus()

		// Set a low TTL so that the ClearRange-based cleanup mechanism can kick in earlier.
		// This could also be done after dropping the table.
		if _, err := conn.ExecContext(ctx, `ALTER TABLE bigbank.bank CONFIGURE ZONE USING gc.ttlseconds = 1200`); err != nil {
			return err
		}

		t.WorkerStatus("computing number of ranges")
		initialBankRanges := numBankRanges()

		t.WorkerStatus("dropping bank table")
		if _, err := conn.ExecContext(ctx, `DROP TABLE bigbank.bank`); err != nil {
			return err
		}

		// Spend some time reading data with a timeout to make sure the
		// DROP above didn't brick the cluster. At the time of writing,
		// clearing all of the table data takes ~6min, so we want to run
		// for at least a multiple of that duration.
		const minDuration = 45 * time.Minute
		deadline := timeutil.Now().Add(minDuration)
		curBankRanges := numBankRanges()
		t.WorkerStatus("waiting for ~", curBankRanges, " merges to complete (and for at least ", minDuration, " to pass)")
		for timeutil.Now().Before(deadline) || curBankRanges > 1 {
			after := time.After(5 * time.Minute)
			curBankRanges = numBankRanges() // this call takes minutes, unfortunately
			t.WorkerProgress(1 - float64(curBankRanges)/float64(initialBankRanges))

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

			t.WorkerStatus("waiting for ~", curBankRanges, " merges to complete (and for at least ", timeutil.Until(deadline), " to pass)")
			select {
			case <-after:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		// TODO(tschottdorf): verify that disk space usage drops below to <some small amount>, but that
		// may not actually happen (see https://github.com/cockroachdb/cockroach/issues/29290).
		return nil
	})
	m.Wait()
}
