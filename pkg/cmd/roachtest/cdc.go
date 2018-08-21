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
	gosql "database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

func registerCDC(r *registry) {
	runCDC := func(ctx context.Context, t *test, c *cluster, warehouses int, initialScan bool) {
		maxInitialScanAllowed, maxLatencyAllowed := 3*time.Minute, time.Minute
		if initialScan {
			maxInitialScanAllowed = 30 * time.Minute
		}
		if warehouses >= 1000 {
			maxLatencyAllowed = 10 * time.Minute
		}

		crdbNodes := c.Range(1, c.nodes-1)
		workloadNode := c.Node(c.nodes)
		kafkaNode := c.Node(c.nodes)

		c.Put(ctx, cockroach, "./cockroach", crdbNodes)
		c.Put(ctx, workload, "./workload", workloadNode)
		c.Start(ctx, crdbNodes, startArgs(`--args=--vmodule=poller=2`))

		t.Status("loading initial data")
		c.Run(ctx, workloadNode, fmt.Sprintf(
			`./workload fixtures load tpcc --warehouses=%d --checks=false {pgurl:1}`, warehouses,
		))

		t.Status("installing kafka")
		c.Run(ctx, kafkaNode, `curl https://packages.confluent.io/archive/4.0/confluent-oss-4.0.0-2.11.tar.gz | tar -xzv`)
		c.Run(ctx, kafkaNode, `sudo apt-get update`)
		c.Run(ctx, kafkaNode, `yes | sudo apt-get install default-jre`)
		c.Run(ctx, kafkaNode, `mkdir -p /mnt/data1/confluent`)
		c.Run(ctx, kafkaNode, `CONFLUENT_CURRENT=/mnt/data1/confluent ./confluent-4.0.0/bin/confluent start`)

		db := c.Conn(ctx, 1)
		if _, err := db.Exec(`SET CLUSTER SETTING trace.debug.enable = true`); err != nil {
			c.t.Fatal(err)
		}
		defer func() {
			// Shut down any running feeds. Not necessary for the nightly, but nice
			// for development.
			if _, err := db.Exec(`CANCEL JOBS (
				SELECT job_id FROM [SHOW JOBS] WHERE status = 'running'
			)`); err != nil {
				c.t.Fatal(err)
			}
		}()

		doneCh := make(chan struct{}, 1)
		var initialScanLatency, maxSeenLatency time.Duration
		latencyDroppedBelowMaxAllowed := false

		t.Status("running workload")
		m := newMonitor(ctx, c, crdbNodes)
		m.Go(func(ctx context.Context) error {
			defer func() { doneCh <- struct{}{} }()
			c.Run(ctx, workloadNode, fmt.Sprintf(
				`./workload run tpcc --warehouses=%d {pgurl:1-%d} --duration=120m`,
				warehouses, c.nodes-1,
			))
			return nil
		})
		m.Go(func(ctx context.Context) error {
			l, err := c.l.childLogger(`changefeed`)
			if err != nil {
				return err
			}

			var cursor string
			if err := db.QueryRow(`SELECT cluster_logical_timestamp()`).Scan(&cursor); err != nil {
				c.t.Fatal(err)
			}
			c.l.printf("starting cursor at %s\n", cursor)

			var jobID int
			createStmt := `CREATE CHANGEFEED FOR
				tpcc.warehouse, tpcc.district, tpcc.customer, tpcc.history,
				tpcc.order, tpcc.new_order, tpcc.item, tpcc.stock,
				tpcc.order_line
			INTO $1 WITH resolved`
			extraArgs := []interface{}{`kafka://` + c.InternalIP(ctx, kafkaNode)[0] + `:9092`}
			if !initialScan {
				createStmt += `, cursor=$2`
				extraArgs = append(extraArgs, cursor)
			}
			if err = db.QueryRow(createStmt, extraArgs...).Scan(&jobID); err != nil {
				return err
			}
			t.Status("watching changefeed")

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-doneCh:
					return nil
				case <-time.After(time.Second):
				}

				var status string
				var hwRaw gosql.NullString
				if err := db.QueryRow(
					`SELECT status, high_water_timestamp FROM crdb_internal.jobs WHERE job_id = $1`,
					jobID,
				).Scan(&status, &hwRaw); err != nil {
					return err
				}
				if status != `running` {
					return errors.Errorf(`unexpected status: %s`, status)
				}
				if hwRaw.Valid {
					// Intentionally not using tree.DecimalToHLC to avoid the dep.
					hwWallTime, err := strconv.ParseInt(
						hwRaw.String[:strings.IndexRune(hwRaw.String, '.')], 10, 64)
					if err != nil {
						return errors.Wrapf(err, "parsing [%s]", hwRaw.String)
					}
					if initialScanLatency == 0 {
						initialScanLatency = timeutil.Since(timeutil.Unix(0, hwWallTime))
						l.printf("initial scan latency %s\n", initialScanLatency)
						t.Status("finished initial scan")
						continue
					}
					latency := timeutil.Since(timeutil.Unix(0, hwWallTime))
					if latency < maxLatencyAllowed {
						latencyDroppedBelowMaxAllowed = true
					}
					if !latencyDroppedBelowMaxAllowed {
						// Before we have RangeFeed, the polls just get
						// progressively smaller after the initial one. Start
						// tracking the max latency once we seen a latency
						// that's less than the max allowed. Verify at the end
						// of the test that this happens at some point.
						l.printf("end-to-end latency %s not yet below max allowed %s\n",
							latency, maxLatencyAllowed)
						continue
					}
					if latency > maxSeenLatency {
						maxSeenLatency = latency
					}
					l.printf("end-to-end latency %s max so far %s\n", latency, maxSeenLatency)
				}
			}
		})
		m.Wait()

		if initialScanLatency == 0 {
			t.Fatalf("initial scan did not complete")
		}
		if initialScanLatency > maxInitialScanAllowed {
			t.Fatalf("initial scan latency was more than allowed: %s vs %s",
				initialScanLatency, maxInitialScanAllowed)
		}
		if !latencyDroppedBelowMaxAllowed {
			t.Fatalf("latency never dropped below max allowed: %s", maxLatencyAllowed)
		}
		if maxSeenLatency > maxLatencyAllowed {
			t.Fatalf("max latency was more than allowed: %s vs %s",
				maxSeenLatency, maxLatencyAllowed)
		}
	}

	r.Add(testSpec{
		Name:       "cdc/w=1000/nodes=3/init=false",
		MinVersion: "2.1.0",
		Nodes:      nodes(4, cpu(16)),
		Stable:     false,
		Run: func(ctx context.Context, t *test, c *cluster) {
			runCDC(ctx, t, c, 1000, false /* initialScan */)
		},
	})
	r.Add(testSpec{
		Name:       "cdc/w=100/nodes=3/init=true",
		MinVersion: "2.1.0",
		Nodes:      nodes(4, cpu(16)),
		Stable:     false,
		Run: func(ctx context.Context, t *test, c *cluster) {
			runCDC(ctx, t, c, 100, true /* initialScan */)
		},
	})
}
