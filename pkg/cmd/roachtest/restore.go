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
	"io/ioutil"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
)

// HealthChecker runs a regular check that verifies that a specified subset
// of (CockroachDB) nodes look "very healthy". That is, there are no stuck
// proposals, liveness problems, or whatever else might get added in the
// future.
type HealthChecker struct {
	c      *cluster
	nodes  nodeListOption
	doneCh chan struct{}
}

// NewHealthChecker returns a populated HealthChecker.
func NewHealthChecker(c *cluster, nodes nodeListOption) *HealthChecker {
	return &HealthChecker{
		c:      c,
		nodes:  nodes,
		doneCh: make(chan struct{}),
	}
}

// Done signals the HeatlthChecker's Runner to shut down.
func (hc *HealthChecker) Done() {
	close(hc.doneCh)
}

// Runner runs health checks on a one minute interval.
func (hc *HealthChecker) Runner(ctx context.Context) error {
	logger, err := hc.c.l.childLogger("health")
	if err != nil {
		return err
	}

	// It's no fun to try to pass the correct escaping through roachprod, so
	// don't even try.
	const script = `
#!/bin/bash
set -euo pipefail

! grep -RE 'have.been.waiting|slow.heartbeat|errRetryLiveness: result is ambiguous' $1
`

	tmp, err := ioutil.TempFile("", "roachtest")
	if err != nil {
		return err
	}
	fname := tmp.Name()
	_ = tmp.Close()
	if err := ioutil.WriteFile(fname, []byte(script), 0755); err != nil {
		return err
	}
	const runCheck = "./healthcheck"
	hc.c.Put(ctx, fname, runCheck, hc.nodes)

	if _, err := hc.c.RunWithBuffer(ctx, logger, hc.nodes, "chmod", "+x", runCheck); err != nil {
		return err
	}

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-hc.doneCh:
			return nil
		case <-ticker.C:
		}
		// TODO(tschottdorf): this isn't pretty and we'd really want some programmatic way to get
		// this information less prone to rot. While the gossip_alerts table might become that
		// eventually, it isn't yet.
		_, err := hc.c.RunWithBuffer(ctx, logger, hc.nodes, runCheck, "{log-dir}")
		if err != nil {
			return err
		}
	}
}

// DiskUsageLogger regularly logs the disk spaced used by the nodes in the cluster.
type DiskUsageLogger struct {
	c      *cluster
	doneCh chan struct{}
}

// NewDiskusageLogger populates a DiskUsageLogger.
func NewDiskUsageLogger(c *cluster) *DiskUsageLogger {
	return &DiskUsageLogger{
		c:      c,
		doneCh: make(chan struct{}),
	}
}

// Done instructs the Runner to terminate.
func (dul *DiskUsageLogger) Done() {
	close(dul.doneCh)
}

// Runner runs in a loop until Done() is called and prints the cluster-wide per
// node disk usage in descending order.
func (dul *DiskUsageLogger) Runner(ctx context.Context) error {
	logger, err := dul.c.l.childLogger("diskusage")
	if err != nil {
		return err
	}
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-dul.doneCh:
			return nil
		case <-ticker.C:
		}

		var bytesUsed []int
		for i := 1; i <= dul.c.nodes; i++ {
			cur, err := getDiskUsageInByte(ctx, dul.c, logger, i)
			if err != nil {
				return errors.Wrapf(err, "node %d", i)
			}
			bytesUsed = append(bytesUsed, cur)
		}
		sort.Slice(bytesUsed, func(i, j int) bool { return bytesUsed[i] > bytesUsed[j] }) // descending

		var s []string
		for i := 0; i < len(bytesUsed); i++ {
			s = append(s, fmt.Sprintf("n%d: %s", i+1, humanizeutil.IBytes(int64(bytesUsed[i]))))
		}

		logger.printf("%s\n", strings.Join(s, ", "))
	}
}

func registerRestore(r *registry) {
	for _, nodeCount := range []int{10, 32} {
		r.Add(testSpec{
			Name:   fmt.Sprintf("restore2TB/nodes=%d", nodeCount),
			Nodes:  nodes(nodeCount),
			Stable: true, // DO NOT COPY to new tests
			Run: func(ctx context.Context, t *test, c *cluster) {
				c.Put(ctx, cockroach, "./cockroach")
				c.Start(ctx)
				m := newMonitor(ctx, c)

				// Run the disk usage logger in the monitor to guarantee its
				// having terminated when the test ends.
				dul := NewDiskUsageLogger(c)
				m.Go(dul.Runner)
				hc := NewHealthChecker(c, c.All())
				m.Go(hc.Runner)

				m.Go(func(ctx context.Context) error {
					defer dul.Done()
					defer hc.Done()
					t.Status(`running restore`)
					c.Run(ctx, c.Node(1), `./cockroach sql --insecure -e "CREATE DATABASE restore2tb"`)
					// TODO(dan): It'd be nice if we could keep track over time of how
					// long this next line took.
					c.Run(ctx, c.Node(1), `./cockroach sql --insecure -e "
				RESTORE csv.bank FROM
				'gs://cockroach-fixtures/workload/bank/version=1.0.0,payload-bytes=10240,ranges=0,rows=65104166,seed=1/bank'
				WITH into_db = 'restore2tb'"`)
					return nil
				})
				m.Wait()
			},
		})
	}
}
