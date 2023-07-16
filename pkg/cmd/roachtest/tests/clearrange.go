// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
)

func registerClearRange(r registry.Registry) {
	for _, checks := range []bool{true, false} {
		for _, rangeTombstones := range []bool{true, false} {
			checks := checks
			rangeTombstones := rangeTombstones

			clusterSpec := r.MakeClusterSpec(
				10, /* nodeCount */
				spec.CPU(16),
				spec.Zones("us-east1-b"),
				spec.VolumeSize(500),
				spec.Cloud(spec.GCE),
			)
			clusterSpec.InstanceType = "n2-standard-16"
			clusterSpec.GCEMinCPUPlatform = "Intel Ice Lake"
			clusterSpec.GCEVolumeType = "pd-ssd"

			r.Add(registry.TestSpec{
				Name:  fmt.Sprintf(`clearrange/checks=%t/rangeTs=%t`, checks, rangeTombstones),
				Owner: registry.OwnerStorage,
				// 5h for import, 90m for the test. The import should take
				// closer to <3:30h but it varies.
				Timeout:        5*time.Hour + 90*time.Minute,
				Cluster:        clusterSpec,
				Leases:         registry.MetamorphicLeases,
				SnapshotPrefix: "clearrange-bank-65m",
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runClearRange(ctx, t, c, checks, rangeTombstones)
				},
			})

			// Using a separate clearrange test on zfs instead of randomly
			// using the same test, cause the Timeout might be different,
			// and may need to be tweaked.
			r.Add(registry.TestSpec{
				Name:  fmt.Sprintf(`clearrange/zfs/checks=%t/rangeTs=%t`, checks, rangeTombstones),
				Skip:  "Consistently failing. See #68716 context.",
				Owner: registry.OwnerStorage,
				// 5h for import, 120 for the test. The import should take closer
				// to <3:30h but it varies.
				Timeout:           5*time.Hour + 120*time.Minute,
				Cluster:           r.MakeClusterSpec(10, spec.CPU(16), spec.SetFileSystem(spec.Zfs)),
				EncryptionSupport: registry.EncryptionMetamorphic,
				Leases:            registry.MetamorphicLeases,
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runClearRange(ctx, t, c, checks, rangeTombstones)
				},
			})
		}
	}
}

func runClearRange(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	aggressiveChecks bool,
	useRangeTombstones bool,
) {
	snapshots, err := c.ListSnapshots(ctx, vm.VolumeSnapshotListOpts{
		NamePrefix: t.SnapshotPrefix(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) == 0 {
		t.L().Printf("no existing snapshots found for %s (%s), doing pre-work",
			t.Name(), t.SnapshotPrefix())

		pred, err := version.PredecessorVersion(*t.BuildVersion())
		if err != nil {
			t.Fatal(err)
		}

		path, err := clusterupgrade.UploadVersion(ctx, t, t.L(), c, c.All(), pred)
		if err != nil {
			t.Fatal(err)
		}

		// Copy over the binary to ./cockroach and run it from there. This test
		// captures disk snapshots, which are fingerprinted using the binary
		// version found in this path. The reason it can't just poke at the
		// running CRDB process is because when grabbing snapshots, CRDB is not
		// running.
		c.Run(ctx, c.All(), fmt.Sprintf("cp %s ./cockroach", path))

		t.Status("restoring fixture")
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())

		// NB: on a 10 node cluster, this should take well below 3h.
		tBegin := timeutil.Now()
		c.Run(ctx, c.Node(1), "./cockroach", "workload", "fixtures", "import", "bank",
			"--payload-bytes=10240", "--ranges=10", "--rows=65104166", "--seed=4", "--db=bigbank")
		t.L().Printf("import took %.2fs", timeutil.Since(tBegin).Seconds())

		// Stop all nodes before capturing cluster snapshots.
		c.Stop(ctx, t.L(), option.DefaultStopOpts())

		// Create the aforementioned snapshots.
		snapshots, err = c.CreateSnapshot(ctx, t.SnapshotPrefix())
		if err != nil {
			t.Fatal(err)
		}
		t.L().Printf("created %d new snapshot(s) with prefix %q, using this state",
			len(snapshots), t.SnapshotPrefix())
	} else {
		t.L().Printf("using %d pre-existing snapshot(s) with prefix %q",
			len(snapshots), t.SnapshotPrefix())

		if err := c.ApplySnapshots(ctx, snapshots); err != nil {
			t.Fatal(err)
		}
	}

	c.Put(ctx, t.Cockroach(), "./cockroach")
	t.Status()

	settings := install.MakeClusterSettings()
	if aggressiveChecks {
		// Run with an env var that runs a synchronous consistency check after each rebalance and merge.
		// This slows down merges, so it might hide some races.
		//
		// NB: the below invocation was found to actually make it to the server at the time of writing.
		settings.Env = append(settings.Env, []string{"COCKROACH_CONSISTENCY_AGGRESSIVE=true", "COCKROACH_ENFORCE_CONSISTENT_STATS=true"}...)
	}

	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings)

	{
		conn := c.Conn(ctx, t.L(), 1)
		if _, err := conn.ExecContext(ctx,
			`SET CLUSTER SETTING storage.mvcc.range_tombstones.enabled = $1`,
			useRangeTombstones); err != nil {
			t.Fatal(err)
		}
		conn.Close()
	}

	// Also restore a much smaller table. We'll use it to run queries against
	// the cluster after having dropped the large table above, verifying that
	// the  cluster still works.
	t.Status(`restoring tiny table`)
	defer t.WorkerStatus()

	// Use a 120s connect timeout to work around the fact that the server will
	// declare itself ready before it's actually 100% ready. See:
	// https://github.com/cockroachdb/cockroach/issues/34897#issuecomment-465089057
	c.Run(ctx, c.Node(1), `COCKROACH_CONNECT_TIMEOUT=120 ./cockroach sql --insecure -e "DROP DATABASE IF EXISTS tinybank"`)
	c.Run(ctx, c.Node(1), "./cockroach", "workload", "fixtures", "import", "bank", "--db=tinybank",
		"--payload-bytes=100", "--ranges=10", "--rows=800", "--seed=1")

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

	m := c.NewMonitor(ctx)
	m.Go(func(ctx context.Context) error {
		c.Run(ctx, c.Node(1), `./cockroach workload init kv`)
		c.Run(ctx, c.All(), `./cockroach workload run kv --concurrency=32 --duration=1h`)
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

		// Set a low TTL so that the ClearRange-based cleanup mechanism can kick
		// in earlier. This could also be done after dropping the table.
		if _, err := conn.ExecContext(ctx, `ALTER TABLE bigbank.bank CONFIGURE ZONE USING gc.ttlseconds = 600`); err != nil {
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
