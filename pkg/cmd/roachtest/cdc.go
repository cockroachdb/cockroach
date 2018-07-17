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
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func registerCDC(r *registry) {
	runCDC := func(ctx context.Context, t *test, c *cluster, warehouses int, initialScan bool) {
		crdbNodes := c.Range(1, c.nodes-1)
		workloadNode := c.Node(c.nodes)
		kafkaNode := c.Node(c.nodes)

		c.Put(ctx, cockroach, "./cockroach", crdbNodes)
		c.Put(ctx, workload, "./workload", workloadNode)
		// Force encryption off as we do not have a good way to detect whether it is enabled
		// for the `debug compact` call below.
		c.Start(ctx, crdbNodes, startArgsDontEncrypt)

		t.Status("loading initial data")
		c.Run(ctx, workloadNode, fmt.Sprintf(
			`./workload fixtures load tpcc --warehouses=%d {pgurl:1}`, warehouses,
		))

		// Run compactions on the data so TimeBoundIterator works. This can be
		// fixed. See #26870
		c.Stop(ctx, crdbNodes)
		c.Run(ctx, crdbNodes, `./cockroach debug compact /mnt/data1/cockroach/`)
		c.Start(ctx, crdbNodes, startArgsDontEncrypt)

		t.Status("installing kafka")
		c.Run(ctx, kafkaNode, `curl https://packages.confluent.io/archive/4.0/confluent-oss-4.0.0-2.11.tar.gz | tar -xzv`)
		c.Run(ctx, kafkaNode, `sudo apt-get update`)
		c.Run(ctx, kafkaNode, `yes | sudo apt-get install default-jre`)
		c.Run(ctx, kafkaNode, `mkdir /mnt/data1/confluent`)
		c.Run(ctx, kafkaNode, `CONFLUENT_CURRENT=/mnt/data1/confluent ./confluent-4.0.0/bin/confluent start`)

		db := c.Conn(ctx, 1)
		if _, err := db.Exec(`SET CLUSTER SETTING trace.debug.enable = true`); err != nil {
			c.t.Fatal(err)
		}
		defer func() {
			// Shut down any running feeds. Not neceesary for the nightly, but nice
			// for development.
			if _, err := db.Exec(`CANCEL JOBS (
				SELECT job_id FROM [SHOW JOBS] WHERE status = 'running'
			)`); err != nil {
				c.t.Fatal(err)
			}
		}()

		doneCh := make(chan struct{}, 1)
		var initialScanLatency, maxLatency time.Duration

		t.Status("running workload")
		m := newMonitor(ctx, c, crdbNodes)
		m.Go(func(ctx context.Context) error {
			defer func() { doneCh <- struct{}{} }()
			c.Run(ctx, workloadNode, fmt.Sprintf(
				`./workload run tpcc --warehouses=%d {pgurl:1-%d} --duration=10m`,
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
			createStmt := `CREATE CHANGEFEED FOR DATABASE tpcc INTO $1 WITH timestamps`
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

				// Until we have changefeed monitoring, query the job progress
				// proto directly.
				var progressBytes []byte
				if err := db.QueryRow(
					`SELECT progress FROM system.jobs WHERE id = $1`, jobID,
				).Scan(&progressBytes); err != nil {
					return err
				}
				var progress jobspb.Progress
				if err := protoutil.Unmarshal(progressBytes, &progress); err != nil {
					return err
				}
				if nanos := progress.GetChangefeed().Highwater.WallTime; nanos > 0 {
					if initialScan && initialScanLatency == 0 {
						initialScanLatency = timeutil.Since(timeutil.Unix(0, nanos))
						if maxAllowed := 3 * time.Minute; initialScanLatency > maxAllowed {
							t.Fatalf("initial scan latency was more than allowed: %s vs %s",
								initialScanLatency, maxAllowed)
						}
						l.printf("initial scan latency %s\n", initialScanLatency)
						t.Status("finished initial scan")
						continue
					}
					latency := timeutil.Since(timeutil.Unix(0, nanos))
					if latency > maxLatency {
						maxLatency = latency
					}
					l.printf("end-to-end latency %s max so far %s\n", latency, maxLatency)
				}
			}
		})
		m.Wait()

		if maxAllowed := time.Minute; maxLatency > maxAllowed {
			t.Fatalf("max latency was more than allowed: %s vs %s", maxLatency, maxAllowed)
		}
	}

	r.Add(testSpec{
		Name:   "cdc/w=100/nodes=3/init=false",
		Nodes:  nodes(4, cpu(16)),
		Stable: false,
		Run: func(ctx context.Context, t *test, c *cluster) {
			runCDC(ctx, t, c, 100, false /* initialScan */)
		},
	})
	r.Add(testSpec{
		Name:   "cdc/w=10/nodes=3/init=true",
		Nodes:  nodes(4, cpu(16)),
		Stable: false,
		Run: func(ctx context.Context, t *test, c *cluster) {
			runCDC(ctx, t, c, 10, true /* initialScan */)
		},
	})
}
