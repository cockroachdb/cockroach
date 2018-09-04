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

func installKafka(ctx context.Context, c *cluster, kafkaNode nodeListOption) {
	c.Run(ctx, kafkaNode, `curl https://packages.confluent.io/archive/4.0/confluent-oss-4.0.0-2.11.tar.gz | tar -xzv`)
	c.Run(ctx, kafkaNode, `sudo apt-get update`)
	c.Run(ctx, kafkaNode, `yes | sudo apt-get install default-jre`)
	c.Run(ctx, kafkaNode, `mkdir -p /mnt/data1/confluent`)

}

func startKafka(ctx context.Context, c *cluster, kafkaNode nodeListOption) {
	c.Run(ctx, kafkaNode, `CONFLUENT_CURRENT=/mnt/data1/confluent ./confluent-4.0.0/bin/confluent start`)
}

func stopKafka(ctx context.Context, c *cluster, kafkaNode nodeListOption) {
	c.Run(ctx, kafkaNode, `CONFLUENT_CURRENT=/mnt/data1/confluent ./confluent-4.0.0/bin/confluent stop`)
}

// stopFeeds cancels any running feeds on the cluster. Not necessary for the
// nightly, but nice for development.
func stopFeeds(db *gosql.DB, c *cluster) {
	if _, err := db.Exec(`CANCEL JOBS (
			SELECT job_id FROM [SHOW JOBS] WHERE status = 'running'
		)`); err != nil {
		c.t.Fatal(err)
	}
}

type tpccWorkload struct {
	workloadNodes  nodeListOption
	sqlNodes       nodeListOption
	warehouseCount int
}

func (tr *tpccWorkload) install(ctx context.Context, c *cluster) {
	c.Run(ctx, tr.workloadNodes, fmt.Sprintf(
		`./workload fixtures load tpcc --warehouses=%d --checks=false {pgurl%s}`,
		tr.warehouseCount,
		tr.sqlNodes.randNode(),
	))
}

func (tr *tpccWorkload) run(ctx context.Context, c *cluster) {
	c.Run(ctx, tr.workloadNodes, fmt.Sprintf(
		`./workload run tpcc --warehouses=%d {pgurl%s} --duration=30m`,
		tr.warehouseCount,
		tr.sqlNodes,
	))
}

type cdcTestArgs struct {
	warehouseCount int
	initialScan    bool
	kafkaChaos     bool
}

func cdcBasicTest(ctx context.Context, t *test, c *cluster, args cdcTestArgs) {
	maxInitialScanAllowed, maxLatencyAllowed := 3*time.Minute, time.Minute
	if args.initialScan {
		maxInitialScanAllowed = 30 * time.Minute
	}
	if args.warehouseCount >= 1000 {
		maxLatencyAllowed = 10 * time.Minute
	}

	crdbNodes := c.Range(1, c.nodes-1)
	workloadNode := c.Node(c.nodes)
	kafkaNode := c.Node(c.nodes)

	c.Put(ctx, cockroach, "./cockroach", crdbNodes)
	c.Put(ctx, workload, "./workload", workloadNode)
	c.Start(ctx, crdbNodes, startArgs(`--args=--vmodule=poller=2`))

	tpcc := tpccWorkload{
		sqlNodes:       crdbNodes,
		workloadNodes:  workloadNode,
		warehouseCount: args.warehouseCount,
	}

	t.Status("loading initial data")
	tpcc.install(ctx, c)
	t.Status("installing kafka")
	installKafka(ctx, c, kafkaNode)
	startKafka(ctx, c, kafkaNode)
	db := c.Conn(ctx, 1)
	defer stopFeeds(db, c)
	if _, err := db.Exec(`SET CLUSTER SETTING trace.debug.enable = true`); err != nil {
		c.t.Fatal(err)
	}

	tpccComplete := make(chan struct{}, 1)
	var initialScanLatency, maxSeenLatency time.Duration
	latencyDroppedBelowMaxAllowed := false

	t.Status("running workload")
	m := newMonitor(ctx, c, crdbNodes)
	m.Go(func(ctx context.Context) error {
		defer func() { close(tpccComplete) }()
		tpcc.run(ctx, c)
		return nil
	})
	m.Go(func(ctx context.Context) error {
		l, err := c.l.ChildLogger(`changefeed`)
		if err != nil {
			return err
		}

		var cursor string
		if err := db.QueryRow(`SELECT cluster_logical_timestamp()`).Scan(&cursor); err != nil {
			c.t.Fatal(err)
		}
		c.l.Printf("starting cursor at %s\n", cursor)

		var jobID int
		createStmt := `CREATE CHANGEFEED FOR
			tpcc.warehouse, tpcc.district, tpcc.customer, tpcc.history,
			tpcc.order, tpcc.new_order, tpcc.item, tpcc.stock,
			tpcc.order_line
		INTO $1 WITH resolved`
		extraArgs := []interface{}{`kafka://` + c.InternalIP(ctx, kafkaNode)[0] + `:9092`}
		if !args.initialScan {
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
			case <-tpccComplete:
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
					l.Printf("initial scan latency %s\n", initialScanLatency)
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
					l.Printf("end-to-end latency %s not yet below max allowed %s\n",
						latency, maxLatencyAllowed)
					continue
				}
				if latency > maxSeenLatency {
					maxSeenLatency = latency
				}
				l.Printf("end-to-end latency %s max so far %s\n", latency, maxSeenLatency)
			}
		}
	})
	if args.kafkaChaos {
		m.Go(func(ctx context.Context) error {
			period, downTime := 2*time.Minute, 20*time.Second
			t := time.NewTicker(period)
			for {
				select {
				case <-tpccComplete:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				case <-t.C:
				}

				stopKafka(ctx, c, kafkaNode)

				select {
				case <-tpccComplete:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(downTime):
				}

				startKafka(ctx, c, kafkaNode)
			}
		})
	}
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
	// Do not verify max-seen latency in chaos tests.
	if !args.kafkaChaos && maxSeenLatency > maxLatencyAllowed {
		t.Fatalf("max latency was more than allowed: %s vs %s",
			maxSeenLatency, maxLatencyAllowed)
	}
}

func registerCDC(r *registry) {
	r.Add(testSpec{
		Name:       "cdc/w=1000/nodes=3/init=false",
		MinVersion: "2.1.0",
		Nodes:      nodes(4, cpu(16)),
		Stable:     false,
		Run: func(ctx context.Context, t *test, c *cluster) {
			cdcBasicTest(ctx, t, c, cdcTestArgs{
				warehouseCount: 1000,
				initialScan:    false,
				kafkaChaos:     false,
			})
		},
	})
	r.Add(testSpec{
		Name:       "cdc/w=100/nodes=3/init=true",
		MinVersion: "2.1.0",
		Nodes:      nodes(4, cpu(16)),
		Stable:     false,
		Run: func(ctx context.Context, t *test, c *cluster) {
			cdcBasicTest(ctx, t, c, cdcTestArgs{
				warehouseCount: 100,
				initialScan:    true,
				kafkaChaos:     false,
			})
		},
	})
	r.Add(testSpec{
		Skip:       "https://github.com/cockroachdb/cockroach/issues/29196",
		Name:       "cdc/w=100/nodes=3/init=false/chaos=true",
		MinVersion: "2.1.0",
		Nodes:      nodes(4, cpu(16)),
		Stable:     false,
		Run: func(ctx context.Context, t *test, c *cluster) {
			cdcBasicTest(ctx, t, c, cdcTestArgs{
				warehouseCount: 100,
				initialScan:    false,
				kafkaChaos:     true,
			})
		},
	})
}
