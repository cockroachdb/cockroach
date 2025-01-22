// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	humanize "github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

type cdcBenchScanType string
type cdcBenchServer string

const (
	// cdcBenchInitialScan runs an initial scan across a table, i.e. it scans and
	// emits all rows in the table.
	cdcBenchInitialScan cdcBenchScanType = "initial"

	// cdcBenchCatchupScan runs a catchup scan across a table where all the data
	// is eligible for emission, i.e. it creates a changefeed with a cursor below
	// the data ingestion timestamp and emits all rows in the table.
	cdcBenchCatchupScan cdcBenchScanType = "catchup"

	// cdcBenchColdCatchupScan runs a catchup scan across a table, where none of
	// the data is eligible, i.e. it creates a changefeed with a cursor above the
	// data ingestion timestamp. This is the common case in production clusters,
	// where tables are large and the relative amount of changes is low. This
	// won't emit any rows, but it still needs to scan the entire table to look
	// for data above the cursor, and relies on Pebble's block property filters to
	// do so efficiently. Ideally, this wouldn't take any time at all, but in
	// practice it can.
	cdcBenchColdCatchupScan cdcBenchScanType = "catchup-cold"

	cdcBenchNoServer cdcBenchServer = ""
	// The legacy processor was removed in 25.1+. In such
	// timeseries, "processor" refers to the now defunct legacy
	// processor.
	cdcBenchSchedulerServer cdcBenchServer = "scheduler" // new scheduler
)

var (
	cdcBenchScanTypes = []cdcBenchScanType{
		cdcBenchInitialScan, cdcBenchCatchupScan, cdcBenchColdCatchupScan}
	cdcBenchServers = []cdcBenchServer{cdcBenchSchedulerServer}
)

func registerCDCBench(r registry.Registry) {

	// Initial/catchup scan benchmarks.
	for _, scanType := range cdcBenchScanTypes {
		for _, ranges := range []int64{100, 100000} {
			const (
				nodes  = 5 // excluding coordinator/workload node
				cpus   = 16
				rows   = 1_000_000_000 // 19 GB
				format = "json"
			)
			r.Add(registry.TestSpec{
				Name: fmt.Sprintf(
					"cdc/scan/%s/nodes=%d/cpu=%d/rows=%s/ranges=%s/protocol=mux/format=%s/sink=null",
					scanType, nodes, cpus, formatSI(rows), formatSI(ranges), format),
				Owner:            registry.OwnerCDC,
				Benchmark:        true,
				Cluster:          r.MakeClusterSpec(nodes+1, spec.CPU(cpus)),
				CompatibleClouds: registry.AllExceptAWS,
				Suites:           registry.Suites(registry.Weekly),
				Timeout:          4 * time.Hour, // Allow for the initial import and catchup scans with 100k ranges.
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runCDCBenchScan(ctx, t, c, scanType, rows, ranges, format)
				},
			})
		}
	}

	// Workload impact benchmarks.
	// TODO(#135952): Reenable readPercent 100. This benchmark historically tested
	// kv100, but it was disabled because cdc bench can be flaky and kv100 does
	// not provide enough value for the noise.
	for _, readPercent := range []int{0} {
		for _, ranges := range []int64{100, 100000} {
			const (
				nodes  = 5 // excluding coordinator and workload nodes
				cpus   = 16
				format = "json"
			)

			// Control run that only runs the workload, with no changefeed.
			r.Add(registry.TestSpec{
				Name: fmt.Sprintf(
					"cdc/workload/kv%d/nodes=%d/cpu=%d/ranges=%s/control",
					readPercent, nodes, cpus, formatSI(ranges)),
				Owner:            registry.OwnerCDC,
				Benchmark:        true,
				Cluster:          r.MakeClusterSpec(nodes+2, spec.CPU(cpus)),
				CompatibleClouds: registry.AllExceptAWS,
				Suites:           registry.Suites(registry.Weekly),
				Timeout:          time.Hour,
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runCDCBenchWorkload(ctx, t, c, ranges, readPercent, "", "", nullSink)
				},
			})

			// Workloads with a concurrent changefeed running.
			for _, server := range cdcBenchServers {
				r.Add(registry.TestSpec{
					Name: fmt.Sprintf(
						"cdc/workload/kv%d/nodes=%d/cpu=%d/ranges=%s/server=%s/protocol=mux/format=%s/sink=null",
						readPercent, nodes, cpus, formatSI(ranges), server, format),
					Owner:            registry.OwnerCDC,
					Benchmark:        true,
					Cluster:          r.MakeClusterSpec(nodes+2, spec.CPU(cpus)),
					CompatibleClouds: registry.AllExceptAWS,
					Suites:           registry.Suites(registry.Weekly),
					Timeout:          time.Hour,
					Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
						runCDCBenchWorkload(ctx, t, c, ranges, readPercent, server, format, nullSink)
					},
				})

				r.Add(registry.TestSpec{
					Name: fmt.Sprintf(
						"cdc/workload/kv%d/nodes=%d/cpu=%d/ranges=%s/server=%s/protocol=mux/format=%s/sink=kafka",
						readPercent, nodes, cpus, formatSI(ranges), server, format),
					Owner:            registry.OwnerCDC,
					Benchmark:        true,
					Cluster:          r.MakeClusterSpec(nodes+3, spec.CPU(cpus)),
					CompatibleClouds: registry.AllExceptAWS,
					Suites:           registry.Suites(registry.Weekly),
					Timeout:          time.Hour,
					Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
						runCDCBenchWorkload(ctx, t, c, ranges, readPercent, server, format, kafkaSink)
					},
				})
			}
		}
	}
}

func formatSI(num int64) string {
	numSI, suffix := humanize.ComputeSI(float64(num))
	return fmt.Sprintf("%d%s", int64(numSI), suffix)
}

// makeCDCBenchOptions creates common cluster options for CDC benchmarks.
func makeCDCBenchOptions(c cluster.Cluster) (option.StartOpts, install.ClusterSettings) {
	opts := option.DefaultStartOpts()
	settings := install.MakeClusterSettings()
	settings.ClusterSettings["kv.rangefeed.enabled"] = "true"

	// Checkpoint frequently.  Some of the larger benchmarks might overload the
	// cluster.  Producing frequent span-level checkpoints helps with recovery.
	settings.ClusterSettings["changefeed.span_checkpoint.interval"] = "60s"
	settings.ClusterSettings["changefeed.span_checkpoint.lag_threshold"] = "30s"

	// Bump up the number of allowed catchup scans.  Doing catchup for 100k ranges with default
	// configuration (8 client side, 16 per store) takes a while (~1500-2000 ranges per min minutes).
	settings.ClusterSettings["kv.rangefeed.concurrent_catchup_iterators"] = "16"

	// Give changefeed more memory and slow down rangefeed checkpoints.
	// When running large catchup scan benchmarks (100k ranges), as the benchmark
	// nears completion, more and more ranges generate checkpoint events.  When
	// the rate of checkpoints high (default used to be 200ms), the changefeed
	// begins to block on memory acquisition since the fan in factor (~20k
	// ranges/node) greatly exceeds processing loop speed (1 goroutine).
	// The current pipeline looks like this:
	//    rangefeed ->
	//       1 goroutine physicalKVFeed (acquire Memory) ->
	//       1 goroutine copyFromSourceToDestination (filter events) ->
	//       1 goroutine changeAggregator.Next ->
	//       N goroutines rest of the pipeline (encode and emit)
	// The memory for the checkpoint events (even ones after end_time) must be allocated
	// first; then these events are thrown away (many inefficiencies here -- but
	// it's the only thing we can do w/out having to add "end time" support to the rangefeed library).
	// The rate of incoming events greatly exceeds the rate with which we consume these events
	// (and release allocations), resulting in significant drop in completed ranges throughput.
	// Current default is 3s, but if needed increase this time out:
	//    settings.ClusterSettings["kv.rangefeed.closed_timestamp_refresh_interval"] = "5s"
	settings.ClusterSettings["changefeed.memory.per_changefeed_limit"] = "4G"

	// Scheduled backups may interfere with performance, disable them.
	opts.RoachprodOpts.ScheduleBackups = false

	// Prom helpers assume AdminUIPort is at 26258
	roachtestutil.SetDefaultAdminUIPort(c, &opts.RoachprodOpts)

	// Backpressure writers when rangefeed clients can't keep up. This gives more
	// reliable results, since we can otherwise randomly hit timeouts and incur
	// catchup scans.
	settings.Env = append(settings.Env, "COCKROACH_RANGEFEED_SEND_TIMEOUT=0")

	// If this benchmark experiences periodic changefeed restarts due to rpc errors
	// (grpc context canceled), consider increase network timeout.
	// Under significant load (due to rangefeed), timeout could easily be triggered
	// due to elevated goroutine scheduling latency.
	// Current default is 4s which should be sufficient.
	// settings.Env = append(settings.Env, "COCKROACH_NETWORK_TIMEOUT=6s")

	return opts, settings
}

// runCDCBenchScan benchmarks throughput for a changefeed initial or catchup
// scan as rows scanned per second.
//
// It sets up a cluster with N-1 data nodes, and a separate changefeed
// coordinator node. The latter is also used as the workload runner, since we
// don't start the coordinator until the data has been imported.
func runCDCBenchScan(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	scanType cdcBenchScanType,
	numRows, numRanges int64,
	format string,
) {
	const sink = "null://"
	var (
		numNodes = c.Spec().NodeCount
		nData    = c.Range(1, numNodes-1)
		nCoord   = c.Node(numNodes)
	)

	// Start data nodes first to place data on them. We'll start the changefeed
	// coordinator later, since we don't want any data on it.
	opts, settings := makeCDCBenchOptions(c)

	c.Start(ctx, t.L(), opts, settings, nData)
	m := c.NewMonitor(ctx, nData.Merge(nCoord))

	conn := c.Conn(ctx, t.L(), nData[0])
	defer conn.Close()

	// Prohibit ranges on the changefeed coordinator.
	t.L().Printf("configuring zones")
	for _, target := range getAllZoneTargets(ctx, t, conn) {
		_, err := conn.ExecContext(ctx, fmt.Sprintf(
			`ALTER %s CONFIGURE ZONE USING num_replicas=3, constraints='[-node%d]'`, target, nCoord[0]))
		require.NoError(t, err)
	}

	// Wait for system ranges to upreplicate.
	require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), conn))

	// Create and split the workload table. We don't import data here, because it
	// imports before splitting, which takes a very long time.
	//
	// NB: don't scatter -- the ranges end up fairly well-distributed anyway, and
	// the scatter can often fail with 100k ranges.
	t.L().Printf("creating table with %s ranges", humanize.Comma(numRanges))
	c.Run(ctx, option.WithNodes(nCoord), fmt.Sprintf(
		`./cockroach workload init kv --splits %d {pgurl:%d}`, numRanges, nData[0]))
	require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), conn))

	time.Sleep(1 * time.Second)
	cursor := timeutil.Now() // before data is ingested

	// Ingest data. init allows us to import into the existing table. However,
	// catchup scans can't operate across an import, so use inserts in that case.
	loader := "import"
	if scanType == cdcBenchCatchupScan {
		loader = "insert"
	}
	t.L().Printf("ingesting %s rows using %s", humanize.Comma(numRows), loader)
	c.Run(ctx, option.WithNodes(nCoord), fmt.Sprintf(
		`./cockroach workload init kv --insert-count %d --data-loader %s {pgurl:%d}`,
		numRows, loader, nData[0]))

	// Now that the ranges are placed, start the changefeed coordinator.
	t.L().Printf("starting coordinator node")
	c.Start(ctx, t.L(), opts, settings, nCoord)

	conn = c.Conn(ctx, t.L(), nCoord[0])
	defer conn.Close()

	if scanType == cdcBenchColdCatchupScan {
		cursor = timeutil.Now() // after data is ingested
	}

	// Start the scan on the changefeed coordinator. We set an explicit end time
	// in the near future, and compute throughput based on the job's start and
	// finish time.
	t.L().Printf("running changefeed %s scan", scanType)
	with := fmt.Sprintf(`format = '%s', end_time = '%s'`,
		format, timeutil.Now().Add(30*time.Second).Format(time.RFC3339))
	switch scanType {
	case cdcBenchInitialScan:
		with += ", initial_scan = 'yes'"
	case cdcBenchCatchupScan, cdcBenchColdCatchupScan:
		with += fmt.Sprintf(", cursor = '%s'", cursor.Format(time.RFC3339))
	default:
		t.Fatalf("unknown scan type %q", scanType)
	}

	// Lock schema so that changefeed schema feed runs under fast path.
	_, err := conn.ExecContext(ctx, "ALTER TABLE kv.kv  SET (schema_locked = true);")
	require.NoError(t, err)

	var jobID int
	require.NoError(t, conn.QueryRowContext(ctx,
		fmt.Sprintf(`CREATE CHANGEFEED FOR kv.kv INTO '%s' WITH %s`, sink, with)).
		Scan(&jobID))

	// Wait for the changefeed to complete, and compute throughput.
	m.Go(func(ctx context.Context) error {
		t.L().Printf("waiting for changefeed to finish")
		info, err := waitForChangefeed(ctx, conn, jobID, t.L(), func(info changefeedInfo) (bool, error) {
			switch jobs.State(info.status) {
			case jobs.StateSucceeded:
				return true, nil
			case jobs.StatePending, jobs.StateRunning:
				return false, nil
			default:
				return false, errors.Errorf("unexpected changefeed status %q", info.status)
			}
		})
		if err != nil {
			return err
		}

		duration := info.GetFinishedTime().Sub(info.startedTime)
		rate := int64(float64(numRows) / duration.Seconds())
		t.L().Printf("changefeed completed in %s (scanned %s rows per second)",
			duration.Truncate(time.Second), humanize.Comma(rate))

		// Record scan rate to stats file.
		return writeCDCBenchStats(ctx, t, c, nCoord, "scan-rate", rate)
	})

	m.Wait()
}

// runCDCBenchWorkload runs a KV workload on top of a changefeed, measuring the
// workload throughput and latency. Rangefeeds are configured to backpressure
// writers, which yields reliable results for the full write+emission cost.
// The workload results (throughput and latency) can be compared to separate
// control runs that only run the workload without changefeeds and rangefeeds.
//
// It sets up a cluster with N-2 data nodes, and a separate changefeed
// coordinator node and workload runner.
func runCDCBenchWorkload(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	numRanges int64,
	readPercent int,
	server cdcBenchServer,
	format string,
	sinkType sinkType,
) {
	var (
		sinkURI   string
		numNodes  = c.Spec().NodeCount
		nData     = c.Range(1, numNodes-2)
		nCoord    = c.Node(numNodes - 1)
		nWorkload = c.Node(numNodes)

		workloadSeed = randutil.NewPseudoSeed()
		concurrency  = len(nData) * 64
		duration     = 20 * time.Minute
		insertCount  = int64(0)
		cdcEnabled   = format != ""
	)

	switch sinkType {
	case kafkaSink:
		nData = c.Range(1, numNodes-3)
		nCoord = c.Node(numNodes - 1)
		nWorkload = c.Node(numNodes - 2)
		nKafka := c.Node(numNodes)

		kafka, cleanup := setupKafka(ctx, t, c, nKafka)
		defer cleanup()
		sinkURI = kafka.sinkURL(ctx)
	case nullSink, "":
		sinkURI = "null://"
	default:
		t.Fatalf("unsupported sink type %q", sinkType)
	}

	if readPercent == 100 {
		insertCount = 1_000_000 // ingest some data to read
	}
	// Either of these will disable changefeeds. Make sure they're all disabled.
	if server == "" || format == "" {
		require.Empty(t, server)
		require.Empty(t, format)
		cdcEnabled = false
	}

	// Start data nodes first to place data on them. We'll start the changefeed
	// coordinator later, since we don't want any data on it.
	opts, settings := makeCDCBenchOptions(c)
	settings.ClusterSettings["kv.rangefeed.enabled"] = strconv.FormatBool(cdcEnabled)
	settings.ClusterSettings["server.child_metrics.enabled"] = "true"

	switch server {
	case cdcBenchSchedulerServer:
		settings.ClusterSettings["kv.rangefeed.scheduler.enabled"] = "true"
	case cdcBenchNoServer:
	default:
		t.Fatalf("unknown server type %q", server)
	}

	c.Start(ctx, t.L(), opts, settings, nData)
	m := c.NewMonitor(ctx, nData.Merge(nCoord))

	conn := c.Conn(ctx, t.L(), nData[0])
	defer conn.Close()

	// Prohibit ranges on the changefeed coordinator.
	t.L().Printf("configuring zones")
	for _, target := range getAllZoneTargets(ctx, t, conn) {
		_, err := conn.ExecContext(ctx, fmt.Sprintf(
			`ALTER %s CONFIGURE ZONE USING num_replicas=3, constraints='[-node%d]'`, target, nCoord[0]))
		require.NoError(t, err)
	}

	// Wait for system ranges to upreplicate.
	require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), conn))

	// Create and split the workload table.
	//
	// NB: don't scatter -- the ranges end up fairly well-distributed anyway, and
	// the scatter can often fail with 100k ranges.
	t.L().Printf("creating table with %s ranges", humanize.Comma(numRanges))
	c.Run(ctx, option.WithNodes(nWorkload), fmt.Sprintf(
		`./cockroach workload init kv --splits %d {pgurl:%d}`, numRanges, nData[0]))
	require.NoError(t, roachtestutil.WaitFor3XReplication(ctx, t.L(), conn))

	// For read-only workloads, ingest some data. init --insert-count does not use
	// the standard key generator that the read workload uses, so we have to write
	// them with a separate write workload first, see:
	// https://github.com/cockroachdb/cockroach/issues/107874
	if insertCount > 0 {
		const batchSize = 1000
		batches := (insertCount-1)/batchSize + 1 // ceiling division
		t.L().Printf("ingesting %s rows", humanize.Comma(insertCount))
		c.Run(ctx, option.WithNodes(nWorkload), fmt.Sprintf(
			`./cockroach workload run kv --seed %d --read-percent 0 --batch %d --max-ops %d {pgurl:%d}`,
			workloadSeed, batchSize, batches, nData[0]))
	}

	// Now that the ranges are placed, start the changefeed coordinator.
	t.L().Printf("starting coordinator node")
	c.Start(ctx, t.L(), opts, settings, nCoord)

	conn = c.Conn(ctx, t.L(), nCoord[0])
	defer conn.Close()

	// Start the changefeed if enabled. We disable the initial scan, since we
	// don't care about the historical data.
	var jobID int
	var done atomic.Value // time.Time
	if cdcEnabled {
		t.L().Printf("starting changefeed")

		// Lock schema so that changefeed schema feed runs under fast path.
		_, err := conn.ExecContext(ctx, "ALTER TABLE kv.kv  SET (schema_locked = true);")
		require.NoError(t, err)

		require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
			`CREATE CHANGEFEED FOR kv.kv INTO '%s' WITH format = '%s', initial_scan = 'no'`,
			sinkURI, format)).
			Scan(&jobID))

		// Monitor the changefeed for failures. When the workload finishes, it will
		// store the completion timestamp in done, and we'll wait for the
		// changefeed's watermark to reach it.
		//
		// The watermark and lag isn't recorded by the benchmark, but we make sure
		// all data is eventually emitted. It is also helpful for inspection, and we
		// may want to track or assert on it later. Initially, this asserted that
		// the changefeed wasn't lagging by more than 1-2 minutes, but with 100k
		// ranges it was found to sometimes lag by over 8 minutes.
		m.Go(func(ctx context.Context) error {
			info, err := waitForChangefeed(ctx, conn, jobID, t.L(), func(info changefeedInfo) (bool, error) {
				switch jobs.State(info.status) {
				case jobs.StatePending, jobs.StateRunning:
					doneValue := done.Load()
					return doneValue != nil && info.GetHighWater().After(doneValue.(time.Time)), nil
				default:
					return false, errors.Errorf("unexpected changefeed status %s", info.status)
				}
			})
			if err != nil {
				return err
			}
			t.L().Printf("changefeed watermark is %s", info.GetHighWater().Format(time.RFC3339))
			return nil
		})

		// Wait for a stable changefeed before starting the workload, by waiting for
		// the watermark to reach the current time.
		now := timeutil.Now()
		t.L().Printf("waiting for changefeed watermark to reach current time (%s)",
			now.Format(time.RFC3339))
		info, err := waitForChangefeed(ctx, conn, jobID, t.L(), func(info changefeedInfo) (bool, error) {
			switch jobs.State(info.status) {
			case jobs.StatePending, jobs.StateRunning:
				return info.GetHighWater().After(now), nil
			default:
				return false, errors.Errorf("unexpected changefeed status %s", info.status)
			}
		})
		require.NoError(t, err)
		t.L().Printf("changefeed watermark is %s", info.GetHighWater().Format(time.RFC3339))

	} else {
		t.L().Printf("control run, not starting changefeed")
	}

	// Run the workload and record stats. Make sure to use the same seed, so we
	// read any rows we wrote above.
	m.Go(func(ctx context.Context) error {
		// If there's more than 10,000 replicas per node they may struggle to
		// maintain RPC connections or liveness, which occasionally fails client
		// write requests with ambiguous errors. We tolerate errors in this case
		// until we optimize rangefeeds.
		//
		// TODO(erikgrinaker): remove this when benchmarks are stable.
		var extra string
		if readPercent < 100 && (numRanges/int64(len(nData))) >= 10000 {
			extra += ` --tolerate-errors`
		}
		t.L().Printf("running workload")
		labels := map[string]string{
			"duration":     duration.String(),
			"concurrency":  fmt.Sprintf("%d", concurrency),
			"read_percent": fmt.Sprintf("%d", readPercent),
			"insert_count": fmt.Sprintf("%d", insertCount),
		}

		err := c.RunE(ctx, option.WithNodes(nWorkload), fmt.Sprintf(
			`./cockroach workload run kv --seed %d %s `+
				`--concurrency %d --duration %s --write-seq R%d --read-percent %d %s {pgurl:%d-%d}`,
			workloadSeed, roachtestutil.GetWorkloadHistogramArgs(t, c, labels), concurrency, duration, insertCount, readPercent, extra,
			nData[0], nData[len(nData)-1]))
		if err != nil {
			return err
		}
		t.L().Printf("workload completed")

		// When the workload completes, signal the completion time to the changefeed
		// monitor via done, which will wait for it to fully catch up.
		if cdcEnabled {
			now := timeutil.Now()
			done.Store(now)
			info, err := getChangefeedInfo(conn, jobID)
			if err != nil {
				return err
			}
			t.L().Printf("waiting for changefeed watermark to reach %s (lagging by %s)",
				now.Format(time.RFC3339), now.Sub(info.GetHighWater()).Truncate(time.Second))
		}
		return nil
	})

	m.Wait()
}

// getAllZoneTargets returns all zone targets (e.g. "RANGE default", "DATABASE
// system", etc).
func getAllZoneTargets(ctx context.Context, t test.Test, conn *gosql.DB) []string {
	rows, err := conn.QueryContext(ctx, `SELECT target FROM [SHOW ALL ZONE CONFIGURATIONS]`)
	require.NoError(t, err)
	var targets []string
	for rows.Next() {
		var target string
		require.NoError(t, rows.Scan(&target))
		targets = append(targets, target)
	}
	require.NoError(t, rows.Err())
	return targets
}

// waitForChangefeed waits until the changefeed satisfies the given closure.
func waitForChangefeed(
	ctx context.Context,
	conn *gosql.DB,
	jobID int,
	logger *logger.Logger,
	f func(changefeedInfo) (bool, error),
) (changefeedInfo, error) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	const maxLoadJobAttempts = 5
	for loadJobAttempt := 0; ; loadJobAttempt++ {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return changefeedInfo{}, ctx.Err()
		}

		info, err := getChangefeedInfo(conn, jobID)
		if err != nil {
			logger.Errorf("error getting changefeed info: %v (attempt %d)", err, loadJobAttempt+1)
			if loadJobAttempt > 5 {
				return changefeedInfo{}, errors.Wrapf(err, "failed %d attempts to get changefeed info", maxLoadJobAttempts)
			}
			continue
		} else if info.GetError() != "" {
			return changefeedInfo{}, errors.Errorf("changefeed error: %s", info.GetError())
		}
		if ok, err := f(*info); err != nil {
			return changefeedInfo{}, err
		} else if ok {
			return *info, nil
		}
		loadJobAttempt = 0
	}
}

// writeCDCBenchStats writes a single perf metric into stats file on the
// given node, for graphing in roachperf.
func writeCDCBenchStats(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	node option.NodeListOption,
	metric string,
	value int64,
) error {
	// The easiest way to record a precise metric for roachperf is to cast it as a
	// duration in seconds in the histogram's upper bound.
	valueS := time.Duration(value) * time.Second

	exporter := roachtestutil.CreateWorkloadHistogramExporter(t, c)
	reg := histogram.NewRegistryWithExporter(valueS, histogram.MockWorkloadName, exporter)

	bytesBuf := bytes.NewBuffer([]byte{})
	writer := io.Writer(bytesBuf)

	exporter.Init(&writer)
	defer roachtestutil.CloseExporter(ctx, exporter, t, c, bytesBuf, node, "")

	var err error
	reg.GetHandle().Get(metric).Record(valueS)
	reg.Tick(func(tick histogram.Tick) {
		err = tick.Exporter.SnapshotAndWrite(tick.Hist, tick.Now, tick.Elapsed, &tick.Name)
	})
	if err != nil {
		return err
	}

	return nil
}
