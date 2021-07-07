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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func registerQueue(r registry.Registry) {
	// One node runs the workload generator, all other nodes host CockroachDB.
	const numNodes = 2
	r.Add(registry.TestSpec{
		Skip:    "https://github.com/cockroachdb/cockroach/issues/17229",
		Name:    fmt.Sprintf("queue/nodes=%d", numNodes-1),
		Owner:   registry.OwnerKV,
		Cluster: r.MakeClusterSpec(numNodes),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runQueue(ctx, t, c)
		},
	})
}

func runQueue(ctx context.Context, t test.Test, c cluster.Cluster) {
	dbNodeCount := c.Spec().NodeCount - 1
	workloadNode := c.Spec().NodeCount

	// Distribute programs to the correct nodes and start CockroachDB.
	c.Put(ctx, t.Cockroach(), "./cockroach", c.Range(1, dbNodeCount))
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(workloadNode))
	c.Start(ctx, c.Range(1, dbNodeCount))

	runQueueWorkload := func(duration time.Duration, initTables bool) {
		m := c.NewMonitor(ctx, c.Range(1, dbNodeCount))
		m.Go(func(ctx context.Context) error {
			concurrency := ifLocal(c, "", " --concurrency="+fmt.Sprint(dbNodeCount*64))
			duration := fmt.Sprintf(" --duration=%s", duration.String())
			batch := " --batch 100"
			init := ""
			if initTables {
				init = " --init"
			}
			cmd := fmt.Sprintf(
				"./workload run queue --histograms="+t.PerfArtifactsDir()+"/stats.json"+
					init+
					concurrency+
					duration+
					batch+
					" {pgurl:1-%d}",
				dbNodeCount,
			)
			c.Run(ctx, c.Node(workloadNode), cmd)
			return nil
		})
		m.Wait()
	}

	// getQueueScanTime samples the time to run a statement that scans the queue
	// table.
	getQueueScanTime := func() time.Duration {
		db := c.Conn(ctx, 1)
		sampleCount := 5
		samples := make([]time.Duration, sampleCount)
		for i := 0; i < sampleCount; i++ {
			startTime := timeutil.Now()
			var queueCount int
			row := db.QueryRow("SELECT count(*) FROM queue.queue WHERE ts < 1000")
			if err := row.Scan(&queueCount); err != nil {
				t.Fatalf("error running delete statement on queue: %s", err)
			}
			endTime := timeutil.Now()
			samples[i] = endTime.Sub(startTime)
		}
		var sum time.Duration
		for _, sample := range samples {
			sum += sample
		}
		return sum / time.Duration(sampleCount)
	}

	// Run an initial short workload to populate the queue table and get a baseline
	// performance for the queue scan time.
	t.Status("running initial workload")
	runQueueWorkload(10*time.Second, true)
	scanTimeBefore := getQueueScanTime()

	// Set TTL on table queue.queue to 0, so that rows are deleted immediately
	db := c.Conn(ctx, 1)
	_, err := db.ExecContext(ctx, `ALTER TABLE queue.queue CONFIGURE ZONE USING gc.ttlseconds = 30`)
	if err != nil && strings.Contains(err.Error(), "syntax error") {
		// Pre-2.1 was EXPERIMENTAL.
		// TODO(knz): Remove this in 2.2.
		_, err = db.ExecContext(ctx, `ALTER TABLE queue.queue EXPERIMENTAL CONFIGURE ZONE 'gc: {ttlseconds: 30}'`)
	}
	if err != nil {
		t.Fatalf("error setting zone config TTL: %s", err)
	}
	// Truncate table to avoid duplicate key constraints.
	if _, err := db.Exec("DELETE FROM queue.queue"); err != nil {
		t.Fatalf("error deleting rows after initial insertion: %s", err)
	}

	t.Status("running primary workload")
	runQueueWorkload(10*time.Minute, false)

	// Sanity Check: ensure that the queue has actually been deleting rows. There
	// may be some entries left over from the end of the workflow, but the number
	// should not exceed the computed maxRows.

	row := db.QueryRow("SELECT count(*) FROM queue.queue")
	var queueCount int
	if err := row.Scan(&queueCount); err != nil {
		t.Fatalf("error selecting queueCount from queue: %s", err)
	}
	maxRows := 100
	if c.IsLocal() {
		maxRows *= dbNodeCount * 64
	}
	if queueCount > maxRows {
		t.Fatalf("resulting table had %d entries, expected %d or fewer", queueCount, maxRows)
	}

	// Sample the scan time after the primary workload. We expect this to be
	// similar to the baseline time; if time needed has increased by a factor
	// of five or more, we consider the test to have failed.
	scanTimeAfter := getQueueScanTime()
	fmt.Printf("scan time before load: %s, scan time after: %s", scanTimeBefore, scanTimeAfter)
	fmt.Printf("scan time increase: %f (%f/%f)", float64(scanTimeAfter)/float64(scanTimeBefore), float64(scanTimeAfter), float64(scanTimeBefore))
	if scanTimeAfter > scanTimeBefore*30 {
		t.Fatalf(
			"scan time increased by factor of %f after queue workload",
			float64(scanTimeAfter)/float64(scanTimeBefore),
		)
	}
}
