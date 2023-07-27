// Copyright 2023 The Cockroach Authors.
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
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	humanize "github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

type cdcBenchScanType string

const (
	// cdcBenchInitialScan runs an initial scan across a table, i.e. it scans and
	// emits all rows then stops the changefeed.
	cdcBenchInitialScan cdcBenchScanType = "initial"

	// cdcBenchCatchupScan runs a catchup scan across a table, where all the data
	// is eligible for emission, i.e. it creates a changefeed with a cursor below
	// the data ingestion timestamp.
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
)

func registerCDCBench(r registry.Registry) {
	// Initial/catchup scan benchmarks.
	scanTypes := []cdcBenchScanType{cdcBenchInitialScan, cdcBenchCatchupScan, cdcBenchColdCatchupScan}
	for _, scanType := range scanTypes {
		for _, ranges := range []int64{100, 100000} {
			for _, mux := range []bool{false, true} {
				ranges, scanType := ranges, scanType // pin loop variables
				const (
					nodes  = 5 // excluding coordinator/workload node
					cpus   = 16
					rows   = 1_000_000_000 // 19 GB
					format = "json"
				)
				r.Add(registry.TestSpec{
					Name: fmt.Sprintf(
						"cdc/scan/%s/nodes=%d/cpu=%d/rows=%s/ranges=%d/mux=%t/format=%s/sink=null",
						scanType, nodes, cpus, formatSI(rows), ranges, mux, format),
					Owner:           registry.OwnerCDC,
					Benchmark:       true,
					Cluster:         r.MakeClusterSpec(nodes+1, spec.CPU(cpus)),
					RequiresLicense: true,
					Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
						runCDCBenchScan(ctx, t, c, scanType, rows, ranges, mux, format)
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
func makeCDCBenchOptions() (option.StartOpts, install.ClusterSettings) {
	opts := option.DefaultStartOpts()
	settings := install.MakeClusterSettings()
	settings.ClusterSettings["kv.rangefeed.enabled"] = "true"

	// Scheduled backups may interfere with performance, disable them.
	opts.RoachprodOpts.ScheduleBackups = false

	// Backpressure writers when rangefeed clients can't keep up. This gives more
	// reliable results, since we can otherwise randomly hit timeouts and incur
	// catchup scans.
	settings.Env = append(settings.Env, "COCKROACH_RANGEFEED_SEND_TIMEOUT=0")

	return opts, settings
}

// runCDCBenchScan benchmarks throughput for a changefeed initial or catchup
// scan (new/old data) as rows per second.
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
	mux bool,
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
	opts, settings := makeCDCBenchOptions()
	settings.ClusterSettings["changefeed.mux_rangefeed.enabled"] = strconv.FormatBool(mux)
	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, t.L(), opts, settings, nData)
	m := c.NewMonitor(ctx, nData.Merge(nCoord))

	conn := c.Conn(ctx, t.L(), nData[0])
	defer conn.Close()

	// Prohibit ranges on the changefeed coordinator.
	t.L().Printf("configuring zones")
	for _, target := range getAllZoneTargets(ctx, t, conn) {
		_, err := conn.ExecContext(ctx, fmt.Sprintf(
			`ALTER %s CONFIGURE ZONE USING num_replicas = 3, constraints = '[-node%d]'`,
			target, nCoord[0]))
		require.NoError(t, err)
	}

	// Wait for system ranges to upreplicate.
	require.NoError(t, WaitFor3XReplication(ctx, t, conn))

	// Create and split the workload table. We don't import data here, because it
	// imports before splitting, which takes a very long time.
	//
	// NB: don't scatter -- the ranges end up fairly well-distributed anyway, and
	// the scatter can often fail with 100k ranges.
	t.L().Printf("creating table with %s ranges", humanize.Comma(numRanges))
	c.Run(ctx, nCoord, fmt.Sprintf(
		`./cockroach workload init kv --splits %d {pgurl:%d}`, numRanges, nData[0]))
	require.NoError(t, WaitFor3XReplication(ctx, t, conn))

	cursor := timeutil.Now()

	// Ingest data. init allows us to import into the existing table. However,
	// catchup scans can't operate across an import, so use inserts in that case.
	loader := "import"
	if scanType == cdcBenchCatchupScan {
		loader = "insert"
	}
	t.L().Printf("ingesting %s rows using %s", humanize.Comma(numRows), loader)
	c.Run(ctx, nCoord, fmt.Sprintf(
		`./cockroach workload init kv --insert-count %d --data-loader %s {pgurl:%d}`,
		numRows, loader, nData[0]))

	// Now that the ranges are placed, start the changefeed coordinator.
	t.L().Printf("starting coordinator node")
	c.Start(ctx, t.L(), opts, settings, nCoord)

	conn = c.Conn(ctx, t.L(), nCoord[0])
	defer conn.Close()

	if scanType == cdcBenchColdCatchupScan {
		cursor = timeutil.Now() // after data was written
	}

	// Start the initial scan on the changefeed coordinator. We set an explicit
	// end time in the near future, and compute throughput based on the job's
	// start and finish time.
	t.L().Printf("running changefeed %s scan", scanType)
	with := fmt.Sprintf(`format = '%s', end_time = '%s', min_checkpoint_frequency = '10s'`,
		format, timeutil.Now().Add(5*time.Second).Format(time.RFC3339))
	switch scanType {
	case cdcBenchInitialScan:
		with += ", initial_scan"
	case cdcBenchCatchupScan, cdcBenchColdCatchupScan:
		with += fmt.Sprintf(", cursor = '%s'", cursor.Format(time.RFC3339))
	default:
		t.Fatalf("unknown scan type %q", scanType)
	}
	var jobID int
	err := conn.QueryRowContext(ctx,
		fmt.Sprintf(`CREATE CHANGEFEED FOR kv.kv INTO '%s' WITH %s`, sink, with)).
		Scan(&jobID)
	require.NoError(t, err)

	// Wait for the job to complete, and compute throughput.
	m.Go(func(ctx context.Context) error {
		t.L().Printf("waiting for changefeed to finish")
		info, err := waitForChangefeedFinish(ctx, conn, jobID, jobs.StatusSucceeded)
		if err != nil {
			return err
		}

		duration := info.finishedTime.Sub(info.startedTime)
		rate := int64(float64(numRows) / duration.Seconds())
		t.L().Printf("changefeed completed in %s (%s rows per second)",
			duration.Truncate(time.Second), humanize.Comma(rate))

		return writeCDCBenchStats(ctx, t, c, nCoord, "scan-rate", rate)
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

// waitForChangefeedFinish waits until the given changefeed succeeds, and then
// returns its info. Any status other than running/succeeded results in an
// error.
func waitForChangefeedFinish(
	ctx context.Context, conn *gosql.DB, jobID int, status jobs.Status,
) (changefeedInfo, error) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return changefeedInfo{}, ctx.Err()
		}

		info, err := getChangefeedInfo(conn, jobID)
		if err != nil {
			return changefeedInfo{}, err
		} else if info.errMsg != "" {
			return changefeedInfo{}, errors.Errorf("changefeed error: %s", info.errMsg)
		}
		switch jobs.Status(info.status) {
		case jobs.StatusSucceeded:
			return *info, nil
		case jobs.StatusRunning:
		default:
			return changefeedInfo{}, errors.Errorf("unexpected changefeed status %q", info.status)
		}
	}
}

// writeCDCBenchStats writes a single perf metric into stats.json on the
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
	reg := histogram.NewRegistry(valueS, histogram.MockWorkloadName)
	bytesBuf := bytes.NewBuffer([]byte{})
	jsonEnc := json.NewEncoder(bytesBuf)

	var err error
	reg.GetHandle().Get(metric).Record(valueS)
	reg.Tick(func(tick histogram.Tick) {
		err = jsonEnc.Encode(tick.Snapshot())
	})
	if err != nil {
		return err
	}

	// Upload the perf artifacts to the given node.
	path := filepath.Join(t.PerfArtifactsDir(), "stats.json")
	if err := c.RunE(ctx, node, "mkdir -p "+filepath.Dir(path)); err != nil {
		return err
	}
	if err := c.PutString(ctx, bytesBuf.String(), path, 0755, node); err != nil {
		return err
	}
	return nil
}
