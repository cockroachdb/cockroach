// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	humanize "github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

type splitLoad interface {
	// init initializes the split workload.
	init(context.Context, test.Test, cluster.Cluster) error
	// run starts the split workload.
	run(context.Context, test.Test, cluster.Cluster) error
	// rangeCount returns the range count for the split workload ranges.
	rangeCount(*gosql.DB) (int, error)
}

type kvSplitLoad struct {
	// concurrency is the number of concurrent workers.
	concurrency int
	// readPercent is the % of queries that are read queries.
	readPercent int
	// spanPercent is the % of queries that query all the rows.
	spanPercent int
	// sequential indicates the kv workload will use a sequential distribution.
	sequential bool
	// blockSize controls the size of writes to the kv table.
	blockSize int
	// waitDuration is the duration the workload should run for.
	waitDuration time.Duration
}

func (ksl kvSplitLoad) init(ctx context.Context, t test.Test, c cluster.Cluster) error {
	t.Status("running uniform kv workload")
	return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), fmt.Sprintf("./cockroach workload init kv {pgurl%s}", c.CRDBNodes()))
}

func (ksl kvSplitLoad) rangeCount(db *gosql.DB) (int, error) {
	return rangeCountFrom("kv.kv", db)
}

func (ksl kvSplitLoad) run(ctx context.Context, t test.Test, c cluster.Cluster) error {
	var extraFlags string
	if ksl.sequential {
		extraFlags += "--sequential "
	}
	if ksl.blockSize != 0 {
		extraFlags += fmt.Sprintf("--min-block-bytes=%d --max-block-bytes=%d ",
			ksl.blockSize, ksl.blockSize)
	}
	return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), fmt.Sprintf("./cockroach workload run kv "+
		"--init --concurrency=%d --read-percent=%d --span-percent=%d %s {pgurl%s} --duration='%s'",
		ksl.concurrency, ksl.readPercent, ksl.spanPercent, extraFlags, c.CRDBNodes(),
		ksl.waitDuration.String()))
}

type ycsbSplitLoad struct {
	// workload is the YCSB workload letter e.g. a, b, ..., f.
	workload string
	// concurrency is the number of concurrent workers.
	concurrency int
	// hashed determines whether the inserted keys are hashed.
	hashed bool
	// insertCount is the number of records to pre-load into the user table.
	insertCount int
	// waitDuration is the duration the workload should run for.
	waitDuration time.Duration
}

func (ysl ycsbSplitLoad) init(ctx context.Context, t test.Test, c cluster.Cluster) error {
	t.Status("running ycsb workload ", ysl.workload)
	extraArgs := ""
	if ysl.hashed {
		extraArgs += "--insert-hash"
	}

	return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), fmt.Sprintf(
		"./cockroach workload init ycsb --insert-count=%d --workload=%s %s {pgurl%s}",
		ysl.insertCount, ysl.workload, extraArgs, c.CRDBNodes()))
}

func (ysl ycsbSplitLoad) rangeCount(db *gosql.DB) (int, error) {
	return rangeCountFrom("ycsb.usertable", db)
}

func (ysl ycsbSplitLoad) run(ctx context.Context, t test.Test, c cluster.Cluster) error {
	extraArgs := ""
	if ysl.hashed {
		extraArgs += "--insert-hash"
	}

	return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), fmt.Sprintf(
		"./cockroach workload run ycsb --record-count=%d --workload=%s --concurrency=%d "+
			"--duration='%s' %s {pgurl%s}",
		ysl.insertCount, ysl.workload, ysl.concurrency,
		ysl.waitDuration.String(), extraArgs, c.CRDBNodes()))
}

func rangeCountFrom(from string, db *gosql.DB) (int, error) {
	var ranges int
	q := fmt.Sprintf("SELECT count(*) FROM [SHOW RANGES FROM TABLE %s]",
		from)
	if err := db.QueryRow(q).Scan(&ranges); err != nil {
		return 0, err
	}
	return ranges, nil
}

type splitParams struct {
	load              splitLoad
	maxSize           int           // The maximum size a range is allowed to be.
	qpsThreshold      int           // QPS Threshold for load based splitting.
	cpuThreshold      time.Duration // CPU Threshold for load based splitting.
	minimumRanges     int           // Minimum number of ranges expected at the end.
	maximumRanges     int           // Maximum number of ranges expected at the end.
	initialRangeCount int           // Initial range count expected after intiailization.
}

func registerLoadSplits(r registry.Registry) {
	// Use the 4th node as the workload runner.
	const numNodes = 4
	const numRoachNodes = 3
	cSpec := r.MakeClusterSpec(numNodes, spec.WorkloadNode())

	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("splits/load/uniform/nodes=%d", numRoachNodes),
		Owner:            registry.OwnerKV,
		Cluster:          cSpec,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			// This number was determined experimentally. Often, but not always,
			// more splits will happen.
			expSplits := 10
			runLoadSplits(ctx, t, c, splitParams{
				maxSize:       10 << 30,      // 10 GB
				qpsThreshold:  100,           // 100 queries per second
				minimumRanges: expSplits + 1, // Expected Splits + 1
				maximumRanges: math.MaxInt32, // We're only checking for minimum.

				load: kvSplitLoad{
					concurrency: 64, // 64 concurrent workers
					readPercent: 95, // 95% reads
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
				}})
		},
	})
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("splits/load/uniform/nodes=%d/obj=cpu", numRoachNodes),
		Owner:            registry.OwnerKV,
		Cluster:          cSpec,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLoadSplits(ctx, t, c, splitParams{
				maxSize:      10 << 30,               // 10 GB
				cpuThreshold: 100 * time.Millisecond, // 1/10th of a CPU per second.
				// There should be at least 13 splits, in practice there are on average
				// 20.
				minimumRanges: 14,
				maximumRanges: 25,
				load: kvSplitLoad{
					concurrency:  64, // 64 concurrent workers
					readPercent:  95, // 95% reads
					waitDuration: 10 * time.Minute,
				}})
		},
	})
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("splits/load/sequential/nodes=%d", numRoachNodes),
		Owner:            registry.OwnerKV,
		Cluster:          cSpec,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLoadSplits(ctx, t, c, splitParams{
				maxSize:      10 << 30, // 10 GB
				qpsThreshold: 100,      // 100 queries per second
				// We expect no splits so require only 1 range. However, in practice we
				// sometimes see a split or two early in, presumably when the sampling
				// gets lucky or due to workload concurrency, where later (>sequence)
				// requests are serviced quicker than concurrent earlier requests, see
				// reservoir sampling examples in #118457.
				minimumRanges: 1,
				maximumRanges: 4,
				load: kvSplitLoad{
					concurrency:  64, // 64 concurrent workers
					readPercent:  0,  // 0% reads
					sequential:   true,
					waitDuration: 60 * time.Second,
				}})
		},
	})
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("splits/load/sequential/nodes=%d/obj=cpu", numRoachNodes),
		Owner:            registry.OwnerKV,
		Cluster:          cSpec,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLoadSplits(ctx, t, c, splitParams{
				maxSize:      10 << 30,               // 10 GB
				cpuThreshold: 100 * time.Millisecond, // 1/10th of a CPU per second.
				// We expect no splits so require only 1 range, however in practice a
				// split may slip in. The reason we don't expect splits for a
				// sequential pattern is that existing split samples should only have
				// the right counter incremented as new requests come in to the right.
				// Any sample we keep should always be the right most request we have
				// seen so far.
				minimumRanges: 1,
				maximumRanges: 5,
				load: kvSplitLoad{
					concurrency:  64,
					readPercent:  0,
					sequential:   true,
					waitDuration: 60 * time.Second,
				}})
		},
	})
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("splits/load/spanning/nodes=%d", numRoachNodes),
		Owner:            registry.OwnerKV,
		Cluster:          cSpec,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLoadSplits(ctx, t, c, splitParams{
				maxSize:       10 << 30, // 10 GB
				qpsThreshold:  100,      // 100 queries per second
				minimumRanges: 1,        // We expect no splits so require only 1 range.
				maximumRanges: 1,        // We expect no splits so require only 1 range.
				load: kvSplitLoad{
					concurrency:  64, // 64 concurrent workers
					readPercent:  0,  // 0% reads
					spanPercent:  95, // 95% spanning queries
					waitDuration: 60 * time.Second,
				}})
		},
	})
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("splits/load/spanning/nodes=%d/obj=cpu", numRoachNodes),
		Owner:            registry.OwnerKV,
		Cluster:          cSpec,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLoadSplits(ctx, t, c, splitParams{
				maxSize:      10 << 30,               // 10 GB
				cpuThreshold: 100 * time.Millisecond, // 1/10th of a CPU per second.
				// We expect between 1-5 splits over 10 minutes, however we can get
				// unlucky with split selection due to randomness and end up splitting
				// more times if the initial load based splits are suboptimal. There
				// isn't the same requirement for containment as QPS, instead we want
				// the CPU to be distributed over the ranges. i.e. splitting a range
				// based on QPS when there are only scans amplifies the orignal QPS,
				// effectively doubling it. Whereas for CPU, the resulting lhs and rhs
				// post split should still add up to approx the original range's CPU -
				// when ignoring fixed overhead.
				minimumRanges: 1,
				maximumRanges: 15,
				load: kvSplitLoad{
					concurrency:  64, // 64 concurrent workers
					readPercent:  0,  // 0% reads
					spanPercent:  95, // 95% spanning queries
					waitDuration: 10 * time.Minute,
				}})
		},
	})
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("splits/load/ycsb/a/nodes=%d/obj=cpu", numRoachNodes),
		Owner:            registry.OwnerKV,
		Cluster:          cSpec,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLoadSplits(ctx, t, c, splitParams{
				maxSize:      10 << 30,               // 10 GB
				cpuThreshold: 100 * time.Millisecond, // 1/10th of a CPU per second.
				// YCSB/A has a zipfian distribution with 50% inserts and 50% updates.
				// The number of splits should be between 18-38 after 10 minutes with
				// 100ms threshold on 8vCPU machines.
				minimumRanges:     18,
				maximumRanges:     40,
				initialRangeCount: 2,
				load: ycsbSplitLoad{
					workload:     "a",
					concurrency:  64,
					insertCount:  1e4, // 100k
					waitDuration: 10 * time.Minute,
				}})
		},
	})
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("splits/load/ycsb/b/nodes=%d/obj=cpu", numRoachNodes),
		Owner:            registry.OwnerKV,
		Cluster:          cSpec,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLoadSplits(ctx, t, c, splitParams{
				maxSize: 10 << 30, // 10 GB
				// YCSB/B has a zipfian distribution with 95% reads and 5% updates.
				// The number of splits should be similar to YCSB/A.
				cpuThreshold:      100 * time.Millisecond, // 1/10th of a CPU per second.
				minimumRanges:     15,
				maximumRanges:     35,
				initialRangeCount: 2,
				load: ycsbSplitLoad{
					workload:     "b",
					concurrency:  64,
					insertCount:  1e4, // 100k
					waitDuration: 10 * time.Minute,
				}})
		},
	})
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("splits/load/ycsb/d/nodes=%d/obj=cpu", numRoachNodes),
		Owner:            registry.OwnerKV,
		Cluster:          cSpec,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLoadSplits(ctx, t, c, splitParams{
				maxSize:      10 << 30,               // 10 GB
				cpuThreshold: 100 * time.Millisecond, // 1/10th of a CPU per second.
				// YCSB/D has a latest distribution i.e. moving hotkey. The inserts are
				// hashed - this will lead to many hotspots over the keyspace that
				// move. Expect a few less splits than A and B.
				minimumRanges:     15,
				maximumRanges:     30,
				initialRangeCount: 2,
				load: ycsbSplitLoad{
					workload:     "d",
					concurrency:  64,
					insertCount:  1e4, // 100k
					waitDuration: 10 * time.Minute,
				}})
		},
	})
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("splits/load/ycsb/e/nodes=%d/obj=cpu", numRoachNodes),
		Owner:            registry.OwnerKV,
		Cluster:          cSpec,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLoadSplits(ctx, t, c, splitParams{
				maxSize:      10 << 30,               // 10 GB
				cpuThreshold: 100 * time.Millisecond, // 1/10th of a CPU per second.
				// YCSB/E has a zipfian distribution with 95% scans (limit 1k) and 5%
				// inserts.
				minimumRanges:     5,
				maximumRanges:     30,
				initialRangeCount: 2,
				load: ycsbSplitLoad{
					workload:     "e",
					concurrency:  64,
					insertCount:  1e4, // 100k
					waitDuration: 10 * time.Minute,
				}})
		},
	})
}

// runLoadSplits tests behavior of load based splitting under
// conditions defined by the params. It checks whether certain number of
// splits occur in different workload scenarios.
func runLoadSplits(ctx context.Context, t test.Test, c cluster.Cluster, params splitParams) {
	startOpts := option.NewStartOpts(option.NoBackupSchedule)
	startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
		"--vmodule=split_queue=2,store_rebalancer=2,allocator=2,replicate_queue=2,"+
			"decider=3,replica_split_load=1",
	)
	// This test sets a larger range size than allowed by the default settings.
	settings := install.MakeClusterSettings()
	if params.maxSize > 8<<30 {
		settings.Env = append(settings.Env, fmt.Sprintf("COCKROACH_MAX_RANGE_MAX_BYTES=%d", params.maxSize))
	}
	c.Start(ctx, t.L(), startOpts, settings, c.CRDBNodes())

	m := c.NewMonitor(ctx, c.CRDBNodes())
	m.Go(func(ctx context.Context) error {
		db := c.Conn(ctx, t.L(), 1)
		defer db.Close()

		t.Status("disable load based splitting")
		if err := disableLoadBasedSplitting(ctx, db); err != nil {
			return err
		}

		// Set the objective to QPS or CPU and update the load split threshold
		// appropriately.
		if params.qpsThreshold > 0 {
			t.Status("setting split objective to QPS with threshold ", params.qpsThreshold)
			if err := setLoadBasedRebalancingObjective(ctx, db, "qps"); err != nil {
				return err
			}
			if _, err := db.ExecContext(ctx, fmt.Sprintf("SET CLUSTER SETTING kv.range_split.load_qps_threshold = %d",
				params.qpsThreshold)); err != nil {
				return err
			}
		} else if params.cpuThreshold > 0 {
			t.Status("setting split objective to CPU with threshold ", params.cpuThreshold)
			if err := setLoadBasedRebalancingObjective(ctx, db, "cpu"); err != nil {
				return err
			}
			if _, err := db.ExecContext(ctx, fmt.Sprintf("SET CLUSTER SETTING kv.range_split.load_cpu_threshold = '%s'",
				params.cpuThreshold)); err != nil {
				return err
			}
		} else {
			t.Fatal("no CPU or QPS split threshold set")
		}

		// The default for backpressureRangeHardCap is 8 GiB.
		if params.maxSize > 8<<30 {
			t.Status("allowing ranges up to ", params.maxSize*2, " bytes")
			_, err := db.ExecContext(ctx, fmt.Sprintf("SET CLUSTER SETTING kv.range.range_size_hard_cap = '%d'", params.maxSize*2))
			require.NoError(t, err)
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
		// Set the range size to a huge size so we don't get splits that occur as a
		// result of size thresholds. The workload table will thus be in a single
		// range unless split by load.
		setRangeMaxBytes(params.maxSize)

		require.NoError(t,
			roachtestutil.WaitForReplication(ctx, t.L(), db, 3 /* repicationFactor */, roachtestutil.ExactlyReplicationFactor))

		// Init the split workload.
		if err := params.load.init(ctx, t, c); err != nil {
			t.Fatal(err)
		}

		t.Status("checking initial range count")
		expectedInitialRangeCount := params.initialRangeCount
		if expectedInitialRangeCount == 0 {
			expectedInitialRangeCount = 1
		}
		if rc, _ := params.load.rangeCount(db); rc != expectedInitialRangeCount {
			return errors.Errorf("%d initial ranges, expected %d initial ranges",
				rc, expectedInitialRangeCount)
		}

		t.Status("enable load based splitting")
		if _, err := db.ExecContext(ctx, `SET CLUSTER SETTING kv.range_split.by_load_enabled = true`); err != nil {
			return err
		}

		t.Status("running workload and waiting for splits")
		if err := params.load.run(ctx, t, c); err != nil {
			return err
		}
		t.Status("workload finished, checking splits")
		if rc, err := params.load.rangeCount(db); err != nil {
			t.Fatal(err)
		} else if rc < params.minimumRanges || rc > params.maximumRanges {
			// We start with expectedInitialRangeCount ranges in the workload table,
			// asserted on above. The number of splits is ranges - initial.
			return errors.Errorf("%d splits, expected between %d and %d splits (ranges %d initial %d)",
				rc-expectedInitialRangeCount,
				params.minimumRanges-expectedInitialRangeCount,
				params.maximumRanges-expectedInitialRangeCount,
				rc,
				expectedInitialRangeCount,
			)
		} else {
			t.L().Printf("%d splits occurred", rc-expectedInitialRangeCount)
		}
		return nil
	})
	m.Wait()
}

func registerLargeRange(r registry.Registry) {
	const size = 32 << 30 // 32 GB
	const numNodes = 6
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("splits/largerange/size=%s,nodes=%d", bytesStr(size), numNodes),
		Owner:            registry.OwnerKV,
		Cluster:          r.MakeClusterSpec(numNodes),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Timeout:          5 * time.Hour,
		// Never run with runtime assertions as this makes this test take
		// too long to complete.
		CockroachBinary: registry.StandardCockroach,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLargeRangeSplits(ctx, t, c, size)
		},
	})
}

func bytesStr(size uint64) string {
	return strings.Replace(humanize.IBytes(size), " ", "", -1)
}

func setRangeMaxBytes(t test.Test, db *gosql.DB, minBytes, maxBytes int) {
	stmtZone := fmt.Sprintf(
		"ALTER RANGE default CONFIGURE ZONE USING range_max_bytes = %d, range_min_bytes = %d",
		maxBytes, minBytes)
	_, err := db.Exec(stmtZone)
	require.NoError(t, err)
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
	const minBytes = 16 << 20 // 16 MB

	// Set the range max size to a multiple of what we expect the size of the
	// bank table to be. This should result in the table fitting inside a single
	// range.
	rangeMaxSize := 10 * size

	numNodes := c.Spec().NodeCount
	// This test sets a larger range size than allowed by the default settings.
	settings := install.MakeClusterSettings()
	settings.Env = append(settings.Env, fmt.Sprintf("COCKROACH_MAX_RANGE_MAX_BYTES=%d", rangeMaxSize))
	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.Node(1))

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	rangeCount := func(t test.Test) (int, string) {
		const q = "SHOW RANGES FROM TABLE bank.bank"
		m, err := sqlutils.RowsToStrMatrix(sqlutils.MakeSQLRunner(db).Query(t, q))
		if err != nil {
			t.Fatal(err)
		}
		return len(m), sqlutils.MatrixToStr(m)
	}

	retryOpts := func() (retry.Options, chan struct{}) {
		// Use non-spammy retry options. We're using them mostly for heavy lifting
		// so waiting up to a minute between reports is totally fine.
		ch := make(chan struct{})
		return retry.Options{
			InitialBackoff:      10 * time.Second,
			MaxBackoff:          time.Minute,
			Multiplier:          2.0,
			RandomizationFactor: 1.0,
			Closer:              ch,
		}, ch
	}

	// Phase 1: start single node, disable splits, make large range.
	t.Status(fmt.Sprintf("creating large bank table range (%d rows at ~%s each)", rows, humanizeutil.IBytes(rowEstimate)))
	{
		m := c.NewMonitor(ctx, c.Node(1))
		m.Go(func(ctx context.Context) error {

			// We don't want load based splitting from splitting the range before
			// it's ready to be split.
			if err := disableLoadBasedSplitting(ctx, db); err != nil {
				return err
			}

			// This effectively disables the hard cap.
			if _, err := db.ExecContext(ctx, fmt.Sprintf("SET CLUSTER SETTING kv.range.range_size_hard_cap = '%d'", rangeMaxSize*2)); err != nil {
				return err
			}
			if _, err := db.ExecContext(ctx, `SET CLUSTER SETTING kv.snapshot_rebalance.max_rate='512MiB'`); err != nil {
				return err
			}
			// This test splits an exceptionally large range. Disable MVCC stats
			// re-computation to ensure the splits happen in a timely manner.
			if _, err := db.ExecContext(ctx, `SET CLUSTER SETTING kv.split.mvcc_stats_recomputation.enabled = 'false'`); err != nil {
				return err
			}
			setRangeMaxBytes(t, db, minBytes, rangeMaxSize)

			// NB: would probably be faster to use --data-loader=IMPORT here, but IMPORT
			// will disregard our preference to keep things in a single range.
			c.Run(ctx, option.WithNodes(c.Node(1)), fmt.Sprintf("./cockroach workload init bank "+
				"--rows=%d --payload-bytes=%d --data-loader INSERT --ranges=1 {pgurl:1}", rows, payload))

			if rc, s := rangeCount(t); rc != 1 {
				return errors.Errorf("bank table split over multiple ranges:\n%s", s)
			}
			return nil
		})
		m.Wait()
	}

	// Phase 2: add other nodes, wait for full replication of bank table.
	t.Status("waiting for full replication")
	{
		c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.Range(2, numNodes))
		m := c.NewMonitor(ctx, c.All())
		// NB: we do a round-about thing of making sure that there's at least one
		// range that has 3 replicas (rather than waiting that there are no ranges
		// with less than three replicas) because the `bank` table doesn't show
		// up until it has been split off.
		const query = `
select concat('r', range_id::string) as range, voting_replicas
from [ SHOW RANGES FROM DATABASE bank ] where cardinality(voting_replicas) >= $1;`
		tBegin := timeutil.Now()
		m.Go(func(ctx context.Context) error {
			opts, ch := retryOpts()
			defer time.AfterFunc(time.Hour, func() { close(ch) }).Stop()

			return opts.Do(ctx, func(ctx context.Context) error {
				m, err := sqlutils.RowsToStrMatrix(sqlutils.MakeSQLRunner(db).Query(t, query, 3))
				if err != nil {
					return err
				}
				t.L().Printf("waiting for range with >= 3 replicas:\n%s", sqlutils.MatrixToStr(m))
				if len(m) == 0 {
					return errors.New("not replicated yet")
				}
				return nil
			})
		})
		m.Wait()

		mt, err := sqlutils.RowsToStrMatrix(sqlutils.MakeSQLRunner(db).Query(t, query, 0 /* list all */))
		require.NoError(t, err)
		t.L().Printf("bank table replicated after %s:\n%s", timeutil.Since(tBegin), sqlutils.MatrixToStr(mt))
	}

	// Phase 3: drop the max range size and observe splits as well as rebalancing.
	rangeSize := 64 << 20       // 64 MB
	expRC := size/rangeSize - 3 // -3 to tolerate a small inaccuracy in rowEstimate
	expSplits := expRC - 1
	t.Status(fmt.Sprintf("waiting for %d splits and rebalancing", expSplits))
	{
		m := c.NewMonitor(ctx, c.All())
		m.Go(func(ctx context.Context) error {
			setRangeMaxBytes(t, db, minBytes, rangeSize)
			// Phase 3a: wait for splits.
			{
				// 1 second per split + a grace period. There really shouldn't be much of a delay
				// in the splits since the range is already upreplicated and no more snapshots
				// should be required, especially seeing how there isn't even traffic on the table.
				waitDuration := time.Duration(expSplits)*time.Second + 100*time.Second

				opts, timeoutCh := retryOpts()
				defer time.AfterFunc(waitDuration, func() { close(timeoutCh) }).Stop()
				if err := opts.Do(ctx, func(ctx context.Context) error {
					if rc, _ := rangeCount(t); rc < expRC {
						// NB: intentionally not printing the rows, it's a lot.
						err := errors.Errorf("bank table split over %d ranges, expected at least %d", rc, expRC)
						t.L().Printf("%v", err)
						return err
					}
					return nil
				}); err != nil {
					return err
				}
				t.L().Printf("splits complete")
			}

			// Wait up to an hour for rebalancing. This should be more than enough, moving
			// 32GiB around isn't too onerous.
			opts, timeoutCh := retryOpts()
			defer time.AfterFunc(time.Hour, func() { close(timeoutCh) }).Stop()

			return opts.Do(ctx, func(ctx context.Context) error {
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
					return err
				}
				if float64(minRangeCount) < 0.8*float64(maxRangeCount) {
					err := errors.Errorf("rebalancing incomplete: min_range_count=%d, max_range_count=%d",
						minRangeCount, maxRangeCount)
					t.L().Printf("%v", err)
					return err
				}
				t.L().Printf("rebalancing complete: min_range_count=%d, max_range_count=%d", minRangeCount, maxRangeCount)
				return nil
			})
		})
		m.Wait()
	}
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

func setLoadBasedRebalancingObjective(ctx context.Context, db *gosql.DB, obj string) error {
	_, err := db.ExecContext(
		ctx,
		fmt.Sprintf(`SET CLUSTER SETTING kv.allocator.load_based_rebalancing.objective = '%s'`, obj),
	)
	return err

}
