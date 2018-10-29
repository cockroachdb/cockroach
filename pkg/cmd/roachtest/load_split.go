// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/storage/split"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

func registerUniformLoadSplits(r *registry) {
	const numNodes = 3

	r.Add(testSpec{
		Name:       fmt.Sprintf("range_split/by_load/uniform/nodes=%d", numNodes),
		MinVersion: `v2.1-2`,
		Nodes:      nodes(numNodes),
		Stable:     true, // DO NOT COPY to new tests
		Run: func(ctx context.Context, t *test, c *cluster) {
			runUniformLoadSplits(ctx, t, c)
		},
	})
}

func runUniformLoadSplits(ctx context.Context, t *test, c *cluster) {
	const maxSize = 10 << 30 // 10 GB
	const concurrency = 64   // 64 concurrent workers
	const readPercent = 95   // 95% reads
	const qpsThreshold = 100 // 100 queries per second

	c.Put(ctx, cockroach, "./cockroach", c.All())
	c.Put(ctx, workload, "./workload", c.Node(1))
	c.Start(ctx, t, c.All())

	m := newMonitor(ctx, c, c.All())
	m.Go(func(ctx context.Context) error {
		db := c.Conn(ctx, 1)
		defer db.Close()

		t.Status("disable load based splitting")
		if _, err := db.ExecContext(ctx, `SET CLUSTER SETTING kv.range_split.by_load_enabled = false`); err != nil {
			return err
		}

		t.Status("increasing range_max_bytes")
		setRangeMaxBytes := func(maxBytes int) {
			stmtZone := fmt.Sprintf("ALTER RANGE default CONFIGURE ZONE USING range_max_bytes = %d", maxBytes)
			if _, err := db.Exec(stmtZone); err != nil {
				t.Fatalf("failed to set range_max_bytes: %v", err)
			}
		}
		// Set the range size to a huge size so we don't get splits that occur
		// as a result of size thresholds. The kv table will thus be in a single
		// range unless split by load.
		setRangeMaxBytes(maxSize)

		t.Status("running uniform kv workload")
		c.Run(ctx, c.Node(1), fmt.Sprintf("./workload init kv {pgurl:1-%d}", c.nodes))

		t.Status("checking initial range count")
		rangeCount := func() int {
			var count int
			const q = "SELECT count(*) FROM [SHOW EXPERIMENTAL_RANGES FROM TABLE kv.kv]"
			if err := db.QueryRow(q).Scan(&count); err != nil {
				t.Fatalf("failed to get range count: %v", err)
			}
			return count
		}
		if rc := rangeCount(); rc != 1 {
			return errors.Errorf("kv.kv table split over multiple ranges.")
		}

		// Set the QPS threshold for load based splitting before turning it on.
		if _, err := db.ExecContext(ctx, fmt.Sprintf("SET CLUSTER SETTING kv.range_split.load_qps_threshold = %d",
			qpsThreshold)); err != nil {
			return err
		}
		t.Status("enable load based splitting")
		if _, err := db.ExecContext(ctx, `SET CLUSTER SETTING kv.range_split.by_load_enabled = true`); err != nil {
			return err
		}
		// After load based splitting is turned on, from experiments
		// it's clear than at least 20 splits will happen. We could
		// change this. Used 20 using pure intuition for a suitable split
		// count from LBS with this kind of workload.
		expSplits := 20

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
		// If the number of expected splits is increased, this calculation will hold
		// for uniform distribution as long as the QPS threshold is continually exceeded
		// even with the expected number of splits. This puts a bound on how high the
		// `expSplits` value can go.
		waitDuration := int64(math.Ceil(math.Ceil(math.Log2(float64(expSplits))) *
			float64((split.RecordDurationThreshold / time.Second))))
		c.Run(ctx, c.Node(1), fmt.Sprintf("./workload run kv "+
			"--init --concurrency=%d --read-percent=%d {pgurl:1-%d} --duration='%ds'", concurrency,
			readPercent, c.nodes, waitDuration+int64(expSplits) /* 1s per split for overhead */))

		t.Status(fmt.Sprintf("waiting for %d splits", expSplits))
		if rc := rangeCount(); rc < expSplits+1 {
			return errors.Errorf("kv.kv has %d ranges, expected at least %d",
				rc, expSplits+1)
		}
		return nil
	})
	m.Wait()
}
