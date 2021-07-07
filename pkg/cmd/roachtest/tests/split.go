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
	gosql "database/sql"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	humanize "github.com/dustin/go-humanize"
)

type splitParams struct {
	maxSize       int           // The maximum size a range is allowed to be.
	concurrency   int           // Number of concurrent workers.
	readPercent   int           // % of queries that are read queries.
	spanPercent   int           // % of queries that query all the rows.
	qpsThreshold  int           // QPS Threshold for load based splitting.
	minimumRanges int           // Minimum number of ranges expected at the end.
	maximumRanges int           // Maximum number of ranges expected at the end.
	sequential    bool          // Sequential distribution.
	waitDuration  time.Duration // Duration the workload should run for.
}

func registerLoadSplits(r registry.Registry) {
	const numNodes = 3

	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("splits/load/uniform/nodes=%d", numNodes),
		Owner:   registry.OwnerKV,
		Cluster: r.MakeClusterSpec(numNodes),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			// This number was determined experimentally. Often, but not always,
			// more splits will happen.
			expSplits := 10
			runLoadSplits(ctx, t, c, splitParams{
				maxSize:       10 << 30,      // 10 GB
				concurrency:   64,            // 64 concurrent workers
				readPercent:   95,            // 95% reads
				qpsThreshold:  100,           // 100 queries per second
				minimumRanges: expSplits + 1, // Expected Splits + 1
				maximumRanges: math.MaxInt32, // We're only checking for minimum.
				// The calculation of the wait duration is as follows:
				//
				// Each split requires at least `split.RecordDurationThreshold` seconds to record
				// keys in a range. So in the kv default distribution, if we make the assumption
				// that all load will be uniform across the splits AND that the QPS threshold is
				// still exceeded for all the splits as the number of splits we're targeting is
				// "low" - we expect that for `expSplits` splits, it will require:
				//
				// Minimum Duration For a Split * log2(expSplits) seconds
				//
				// We also add an extra expSplits second(s) for the overhead of creating each one.
				// If the number of expected splits is increased, this calculation will hold
				// for uniform distribution as long as the QPS threshold is continually exceeded
				// even with the expected number of splits. This puts a bound on how high the
				// `expSplits` value can go.
				// Add 1s for each split for the overhead of the splitting process.
				// waitDuration: time.Duration(int64(math.Ceil(math.Ceil(math.Log2(float64(expSplits)))*
				// 	float64((split.RecordDurationThreshold/time.Second))))+int64(expSplits)) * time.Second,
				//
				// NB: the above has proven flaky. Just use a fixed duration
				// that we think should be good enough. For example, for five
				// expected splits we get ~35s, for ten ~50s, and for 20 ~1m10s.
				// These are all pretty short, so any random abnormality will mess
				// things up.
				waitDuration: 10 * time.Minute,
			})
		},
	})
	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("splits/load/sequential/nodes=%d", numNodes),
		Owner:   registry.OwnerKV,
		Cluster: r.MakeClusterSpec(numNodes),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLoadSplits(ctx, t, c, splitParams{
				maxSize:       10 << 30, // 10 GB
				concurrency:   64,       // 64 concurrent workers
				readPercent:   0,        // 0% reads
				qpsThreshold:  100,      // 100 queries per second
				minimumRanges: 1,        // We expect no splits so require only 1 range.
				// We expect no splits so require only 1 range. However, in practice we
				// sometimes see a split or two early in, presumably when the sampling
				// gets lucky.
				maximumRanges: 3,
				sequential:    true,
				waitDuration:  60 * time.Second,
			})
		},
	})
	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("splits/load/spanning/nodes=%d", numNodes),
		Owner:   registry.OwnerKV,
		Cluster: r.MakeClusterSpec(numNodes),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLoadSplits(ctx, t, c, splitParams{
				maxSize:       10 << 30, // 10 GB
				concurrency:   64,       // 64 concurrent workers
				readPercent:   0,        // 0% reads
				spanPercent:   95,       // 95% spanning queries
				qpsThreshold:  100,      // 100 queries per second
				minimumRanges: 1,        // We expect no splits so require only 1 range.
				maximumRanges: 1,        // We expect no splits so require only 1 range.
				waitDuration:  60 * time.Second,
			})
		},
	})
}

// runLoadSplits tests behavior of load based splitting under
// conditions defined by the params. It checks whether certain number of
// splits occur in different workload scenarios.
func runLoadSplits(ctx context.Context, t test.Test, c cluster.Cluster, params splitParams) {
	c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(1))
	c.Start(ctx, c.All())

	m := c.NewMonitor(ctx, c.All())
	m.Go(func(ctx context.Context) error {
		db := c.Conn(ctx, 1)
		defer db.Close()

		t.Status("disable load based splitting")
		if err := disableLoadBasedSplitting(ctx, db); err != nil {
			return err
		}

		t.Status("increasing range_max_bytes")
		minBytes := 16 << 20 // 16 MB
		setRangeMaxBytes := func(maxBytes int) {
			stmtZone := fmt.Sprintf(
				"ALTER RANGE default CONFIGURE ZONE USING range_max_bytes = %d, range_min_bytes = %d",
				maxBytes, minBytes)
			if _, err := db.Exec(stmtZone); err != nil {
				t.Fatalf("failed to set range_max_bytes: %v", err)
			}
		}
		// Set the range size to a huge size so we don't get splits that occur
		// as a result of size thresholds. The kv table will thus be in a single
		// range unless split by load.
		setRangeMaxBytes(params.maxSize)

		t.Status("running uniform kv workload")
		c.Run(ctx, c.Node(1), fmt.Sprintf("./workload init kv {pgurl:1-%d}", c.Spec().NodeCount))

		t.Status("checking initial range count")
		rangeCount := func() int {
			var ranges int
			const q = "SELECT count(*) FROM [SHOW RANGES FROM TABLE kv.kv]"
			if err := db.QueryRow(q).Scan(&ranges); err != nil {
				// TODO(rafi): Remove experimental_ranges query once we stop testing
				// 19.1 or earlier.
				if strings.Contains(err.Error(), "syntax error at or near \"ranges\"") {
					err = db.QueryRow("SELECT count(*) FROM [SHOW EXPERIMENTAL_RANGES FROM TABLE kv.kv]").Scan(&ranges)
				}
				if err != nil {
					t.Fatalf("failed to get range count: %v", err)
				}
			}
			return ranges
		}
		if rc := rangeCount(); rc != 1 {
			return errors.Errorf("kv.kv table split over multiple ranges.")
		}

		// Set the QPS threshold for load based splitting before turning it on.
		if _, err := db.ExecContext(ctx, fmt.Sprintf("SET CLUSTER SETTING kv.range_split.load_qps_threshold = %d",
			params.qpsThreshold)); err != nil {
			return err
		}
		t.Status("enable load based splitting")
		if _, err := db.ExecContext(ctx, `SET CLUSTER SETTING kv.range_split.by_load_enabled = true`); err != nil {
			return err
		}
		var extraFlags string
		if params.sequential {
			extraFlags += "--sequential"
		}
		c.Run(ctx, c.Node(1), fmt.Sprintf("./workload run kv "+
			"--init --concurrency=%d --read-percent=%d --span-percent=%d %s {pgurl:1-%d} --duration='%s'",
			params.concurrency, params.readPercent, params.spanPercent, extraFlags, c.Spec().NodeCount,
			params.waitDuration.String()))

		t.Status("waiting for splits")
		if rc := rangeCount(); rc < params.minimumRanges || rc > params.maximumRanges {
			return errors.Errorf("kv.kv has %d ranges, expected between %d and %d splits",
				rc, params.minimumRanges, params.maximumRanges)
		}
		return nil
	})
	m.Wait()
}

func registerLargeRange(r registry.Registry) {
	const size = 32 << 30 // 32 GB
	const numNodes = 6
	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("splits/largerange/size=%s,nodes=%d", bytesStr(size), numNodes),
		Owner:   registry.OwnerKV,
		Cluster: r.MakeClusterSpec(numNodes),
		Timeout: 5 * time.Hour,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLargeRangeSplits(ctx, t, c, size)
		},
	})
}

func bytesStr(size uint64) string {
	return strings.Replace(humanize.IBytes(size), " ", "", -1)
}

// This test generates a large Bank table all within a single range. It does
// so by setting the max range size to a huge number before populating the
// table. It then drops the range size back down to normal and watches as
// the large range splits apart.
func runLargeRangeSplits(ctx context.Context, t test.Test, c cluster.Cluster, size int) {
	// payload is the size of the payload column for each row in the Bank
	// table.
	const payload = 100
	// rowOverheadEstimate is an estimate of the overhead of a single
	// row in the Bank table, not including the size of the payload
	// itself. This overhead includes the size of the other two columns
	// in the table along with the size of each row's associated KV key.
	const rowOverheadEstimate = 160
	const rowEstimate = rowOverheadEstimate + payload
	// rows is the number of rows we'll need to insert into the bank table
	// to produce a range of roughly the right size.
	rows := size / rowEstimate

	c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.All())
	c.Start(ctx, c.All())

	m := c.NewMonitor(ctx, c.All())
	m.Go(func(ctx context.Context) error {
		db := c.Conn(ctx, 1)
		defer db.Close()

		// We don't want load based splitting from splitting the range before
		// it's ready to be split.
		t.Status("disable load based splitting")
		if err := disableLoadBasedSplitting(ctx, db); err != nil {
			return err
		}

		t.Status("increasing snapshot_rebalance rate")
		if _, err := db.ExecContext(ctx, `SET CLUSTER SETTING kv.snapshot_rebalance.max_rate='512MiB'`); err != nil {
			return err
		}

		t.Status("increasing snapshot_recovery rate")
		if _, err := db.ExecContext(ctx, `SET CLUSTER SETTING kv.snapshot_recovery.max_rate='512MiB'`); err != nil {
			return err
		}

		t.Status("increasing range_max_bytes")
		minBytes := 16 << 20 // 16 MB
		setRangeMaxBytes := func(maxBytes int) {
			stmtZone := fmt.Sprintf(
				"ALTER RANGE default CONFIGURE ZONE USING range_max_bytes = %d, range_min_bytes = %d",
				maxBytes, minBytes)
			if _, err := db.Exec(stmtZone); err != nil {
				t.Fatalf("failed to set range_max_bytes: %v", err)
			}
		}
		// Set the range size to double what we expect the size of the
		// bank table to be. This should result in the table fitting
		// inside a single range.
		setRangeMaxBytes(2 * size)

		t.Status("populating bank table")
		// NB: workload init does not wait for upreplication after creating the
		// schema but before populating it. This is ok because upreplication
		// occurs much faster than we can actually create a large range.
		c.Run(ctx, c.Node(1), fmt.Sprintf("./workload init bank "+
			"--rows=%d --payload-bytes=%d --ranges=1 {pgurl:1-%d}", rows, payload, c.Spec().NodeCount))

		t.Status("checking for single range")
		rangeCount := func() int {
			var ranges int
			const q = "SELECT count(*) FROM [SHOW RANGES FROM TABLE bank.bank]"
			if err := db.QueryRow(q).Scan(&ranges); err != nil {
				t.Fatalf("failed to get range count: %v", err)
			}
			t.L().Printf("%d ranges in bank table", ranges)
			return ranges
		}
		if rc := rangeCount(); rc != 1 {
			return errors.Errorf("bank table split over multiple ranges")
		}

		t.Status("decreasing range_max_bytes")
		rangeSize := 64 << 20 // 64 MB
		setRangeMaxBytes(rangeSize)

		expRC := size/rangeSize - 3 // -3 to tolerate a small inaccuracy in rowEstimate
		expSplits := expRC - 1
		t.Status(fmt.Sprintf("waiting for %d splits", expSplits))

		// 1 second per split + a grace period.
		waitDuration := time.Duration(expSplits)*time.Second + 100*time.Second
		if err := retry.ForDuration(waitDuration, func() error {
			if rc := rangeCount(); rc < expRC {
				return errors.Errorf("bank table split over %d ranges, expected at least %d",
					rc, expRC)
			}
			return nil
		}); err != nil {
			return err
		}

		t.Status("waiting for rebalancing")
		return retry.ForDuration(1*time.Hour, func() error {
			// Wait for the store with the smallest number of ranges to contain
			// at least 80% as many ranges of the store with the largest number
			// of ranges.
			const q = `
			WITH ranges AS (
				SELECT replicas FROM crdb_internal.ranges_no_leases
			), store_ids AS (
				SELECT unnest(replicas) AS store_id FROM ranges
			), store_id_count AS (
				SELECT store_id, count(1) AS num_replicas FROM store_ids GROUP BY store_id
			)
			SELECT min(num_replicas), max(num_replicas) FROM store_id_count;
			`
			var minRangeCount, maxRangeCount int
			if err := db.QueryRow(q).Scan(&minRangeCount, &maxRangeCount); err != nil {
				t.Fatalf("failed to get per-store range count: %v", err)
			}
			t.L().Printf("min_range_count=%d, max_range_count=%d", minRangeCount, maxRangeCount)
			if float64(minRangeCount) < 0.8*float64(maxRangeCount) {
				return errors.Errorf("rebalancing incomplete: min_range_count=%d, max_range_count=%d",
					minRangeCount, minRangeCount)
			}
			return nil
		})
	})
	m.Wait()
}

func disableLoadBasedSplitting(ctx context.Context, db *gosql.DB) error {
	_, err := db.ExecContext(ctx, `SET CLUSTER SETTING kv.range_split.by_load_enabled = false`)
	if err != nil {
		// If the cluster setting doesn't exist, the cluster version is < 2.2.0 and
		// so Load based Splitting doesn't apply anyway and the error should be ignored.
		if !strings.Contains(err.Error(), "unknown cluster setting") {
			return err
		}
	}
	return nil
}
