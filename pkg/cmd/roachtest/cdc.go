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
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

type workloadType string

const (
	tpccWorkloadType   workloadType = "tpcc"
	ledgerWorkloadType workloadType = "ledger"
)

type cdcTestArgs struct {
	workloadType       workloadType
	tpccWarehouseCount int
	workloadDuration   string
	initialScan        bool
	kafkaChaos         bool

	targetInitialScanLatency time.Duration
	targetSteadyLatency      time.Duration
}

func cdcBasicTest(ctx context.Context, t *test, c *cluster, args cdcTestArgs) {
	c.RemountNoBarrier(ctx)

	crdbNodes := c.Range(1, c.nodes-1)
	workloadNode := c.Node(c.nodes)
	kafkaNode := c.Node(c.nodes)
	c.Put(ctx, cockroach, "./cockroach", crdbNodes)
	c.Put(ctx, workload, "./workload", workloadNode)
	c.Start(ctx, t, crdbNodes, startArgs(`--args=--vmodule=changefeed=2,poller=2`))

	db := c.Conn(ctx, 1)
	if _, err := db.Exec(`SET CLUSTER SETTING trace.debug.enable = true`); err != nil {
		c.t.Fatal(err)
	}
	defer func() {
		if err := stopFeeds(db, c); err != nil {
			t.Fatal(err)
		}
	}()

	t.Status("installing kafka")
	kafka := kafkaManager{
		c:     c,
		nodes: kafkaNode,
	}
	kafka.install(ctx)
	kafka.start(ctx)

	m := newMonitor(ctx, c, crdbNodes)
	workloadCompleteCh := make(chan struct{}, 1)

	if args.workloadType == tpccWorkloadType {
		t.Status("installing TPCC")
		tpcc := tpccWorkload{
			sqlNodes:           crdbNodes,
			workloadNodes:      workloadNode,
			tpccWarehouseCount: args.tpccWarehouseCount,
		}
		tpcc.install(ctx, c)

		t.Status("initiating workload")
		m.Go(func(ctx context.Context) error {
			defer func() { close(workloadCompleteCh) }()
			tpcc.run(ctx, c, args.workloadDuration)
			return nil
		})
	} else {
		t.Status("installing Ledger Workload")
		lw := ledgerWorkload{
			sqlNodes:      crdbNodes,
			workloadNodes: workloadNode,
		}
		lw.install(ctx, c)

		t.Status("initiating workload")
		m.Go(func(ctx context.Context) error {
			defer func() { close(workloadCompleteCh) }()
			lw.run(ctx, c, args.workloadDuration)
			return nil
		})
	}

	verifierLogger, err := c.l.ChildLogger("verifier")
	if err != nil {
		t.Fatal(err)
	}
	defer verifierLogger.close()
	verifier := latencyVerifier{
		targetInitialScanLatency: args.targetInitialScanLatency,
		targetSteadyLatency:      args.targetSteadyLatency,
		logger:                   verifierLogger,
	}
	m.Go(func(ctx context.Context) error {
		l, err := t.l.ChildLogger(`changefeed`)
		if err != nil {
			return err
		}

		var targets string
		if args.workloadType == tpccWorkloadType {
			targets = `tpcc.warehouse, tpcc.district, tpcc.customer, tpcc.history,
			tpcc.order, tpcc.new_order, tpcc.item, tpcc.stock,
			tpcc.order_line`
		} else {
			targets = `ledger.customer, ledger.transaction, ledger.entry, ledger.session`
		}
		jobID, err := createChangefeed(db, targets, kafka.sinkURL(ctx), args.initialScan)
		if err != nil {
			return err
		}

		info, err := getChangefeedInfo(db, jobID)
		if err != nil {
			return err
		}
		verifier.statementTime = info.statementTime
		l.Printf("started changefeed at (%d) %s\n", verifier.statementTime.UnixNano(), verifier.statementTime)
		t.Status("watching changefeed")
		return verifier.pollLatency(ctx, db, jobID, time.Second, workloadCompleteCh)
	})

	if args.kafkaChaos {
		m.Go(func(ctx context.Context) error {
			period, downTime := 2*time.Minute, 20*time.Second
			return kafka.chaosLoop(ctx, period, downTime, workloadCompleteCh)
		})
	}
	m.Wait()

	verifier.assertValid(t)
}

func registerCDC(r *registry) {
	r.Add(testSpec{
		Name:       "cdc/w=1000/nodes=3/init=false",
		MinVersion: "2.1.0",
		Nodes:      nodes(4, cpu(16)),
		Stable:     false,
		Run: func(ctx context.Context, t *test, c *cluster) {
			cdcBasicTest(ctx, t, c, cdcTestArgs{
				workloadType:             tpccWorkloadType,
				tpccWarehouseCount:       1000,
				workloadDuration:         "120m",
				initialScan:              false,
				kafkaChaos:               false,
				targetInitialScanLatency: 3 * time.Minute,
				targetSteadyLatency:      10 * time.Minute,
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
				workloadType:             tpccWorkloadType,
				tpccWarehouseCount:       100,
				workloadDuration:         "30m",
				initialScan:              true,
				kafkaChaos:               false,
				targetInitialScanLatency: 30 * time.Minute,
				targetSteadyLatency:      time.Minute,
			})
		},
	})
	r.Add(testSpec{
		Name:       "cdc/w=100/nodes=3/init=false/chaos=true",
		MinVersion: "2.1.0",
		Nodes:      nodes(4, cpu(16)),
		Stable:     false,
		Run: func(ctx context.Context, t *test, c *cluster) {
			cdcBasicTest(ctx, t, c, cdcTestArgs{
				workloadType:             tpccWorkloadType,
				tpccWarehouseCount:       100,
				workloadDuration:         "30m",
				initialScan:              false,
				kafkaChaos:               true,
				targetInitialScanLatency: 3 * time.Minute,
				targetSteadyLatency:      5 * time.Minute,
			})
		},
	})
	// TODO(mrtracy): This workload was designed with a certain tx/s load in mind,
	// but the load level is not currently being requested or enforced. Add the
	// necessary code to reach this level (575 tx/s).
	r.Add(testSpec{
		Name:       "cdc/ledger/nodes=3/init=true",
		MinVersion: "2.1.0",
		// TODO(mrtracy): This workload is designed to be running on a 20CPU nodes,
		// but this cannot be allocated without some sort of configuration outside
		// of this test. Look into it.
		Nodes:  nodes(4, cpu(16)),
		Stable: false,
		Run: func(ctx context.Context, t *test, c *cluster) {
			cdcBasicTest(ctx, t, c, cdcTestArgs{
				workloadType:             ledgerWorkloadType,
				workloadDuration:         "30m",
				initialScan:              true,
				kafkaChaos:               false,
				targetInitialScanLatency: 30 * time.Minute,
				targetSteadyLatency:      time.Minute,
			})
		},
	})
}

type kafkaManager struct {
	c     *cluster
	nodes nodeListOption
}

func (k kafkaManager) install(ctx context.Context) {
	k.c.Run(ctx, k.nodes, `curl -s https://packages.confluent.io/archive/4.0/confluent-oss-4.0.0-2.11.tar.gz | tar -xz`)
	k.c.Run(ctx, k.nodes, `sudo apt-get -q update`)
	k.c.Run(ctx, k.nodes, `yes | sudo apt-get -q install default-jre`)
	k.c.Run(ctx, k.nodes, `mkdir -p /mnt/data1/confluent`)
}

func (k kafkaManager) start(ctx context.Context) {
	// This isn't necessary for the nightly tests, but it's nice for iteration.
	k.c.Run(ctx, k.nodes, `CONFLUENT_CURRENT=/mnt/data1/confluent ./confluent-4.0.0/bin/confluent destroy || true`)
	k.c.Run(ctx, k.nodes, `CONFLUENT_CURRENT=/mnt/data1/confluent ./confluent-4.0.0/bin/confluent start kafka`)
}

func (k kafkaManager) stop(ctx context.Context) {
	k.c.Run(ctx, k.nodes, `CONFLUENT_CURRENT=/mnt/data1/confluent ./confluent-4.0.0/bin/confluent stop`)
}

func (k kafkaManager) chaosLoop(
	ctx context.Context, period, downTime time.Duration, stopper chan struct{},
) error {
	t := time.NewTicker(period)
	for {
		select {
		case <-stopper:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}

		k.stop(ctx)

		select {
		case <-stopper:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(downTime):
		}

		k.start(ctx)
	}
}

func (k kafkaManager) sinkURL(ctx context.Context) string {
	return `kafka://` + k.c.InternalIP(ctx, k.nodes)[0] + `:9092`
}

type tpccWorkload struct {
	workloadNodes      nodeListOption
	sqlNodes           nodeListOption
	tpccWarehouseCount int
}

func (tw *tpccWorkload) install(ctx context.Context, c *cluster) {
	c.Run(ctx, tw.workloadNodes, fmt.Sprintf(
		`./workload fixtures load tpcc --warehouses=%d --checks=false {pgurl%s}`,
		tw.tpccWarehouseCount,
		tw.sqlNodes.randNode(),
	))
}

func (tw *tpccWorkload) run(ctx context.Context, c *cluster, workloadDuration string) {
	c.Run(ctx, tw.workloadNodes, fmt.Sprintf(
		`./workload run tpcc --warehouses=%d {pgurl%s} --duration=%s`,
		tw.tpccWarehouseCount, tw.sqlNodes, workloadDuration,
	))
}

type ledgerWorkload struct {
	workloadNodes nodeListOption
	sqlNodes      nodeListOption
}

func (lw *ledgerWorkload) install(ctx context.Context, c *cluster) {
	c.Run(ctx, lw.workloadNodes.randNode(), fmt.Sprintf(
		`./workload init ledger {pgurl%s}`,
		lw.sqlNodes.randNode(),
	))
}

func (lw *ledgerWorkload) run(ctx context.Context, c *cluster, workloadDuration string) {
	c.Run(ctx, lw.workloadNodes, fmt.Sprintf(
		`./workload run ledger --mix=balance=0,withdrawal=50,deposit=50,reversal=0 {pgurl%s} --duration=%s`,
		lw.sqlNodes,
		workloadDuration,
	))
}

type latencyVerifier struct {
	statementTime            time.Time
	targetSteadyLatency      time.Duration
	targetInitialScanLatency time.Duration
	logger                   *logger

	initialScanLatency   time.Duration
	maxSeenSteadyLatency time.Duration
	latencyBecameSteady  bool
}

func (lv *latencyVerifier) noteHighwater(highwaterTime time.Time) {
	if highwaterTime.Before(lv.statementTime) {
		return
	}
	if lv.initialScanLatency == 0 {
		lv.initialScanLatency = timeutil.Since(lv.statementTime)
		lv.logger.Printf("initial scan completed: latency %s\n", lv.initialScanLatency)
		return
	}

	latency := timeutil.Since(highwaterTime)
	if latency < lv.targetSteadyLatency/2 {
		lv.latencyBecameSteady = true
	}
	if !lv.latencyBecameSteady {
		// Before we have RangeFeed, the polls just get
		// progressively smaller after the initial one. Start
		// tracking the max latency once we seen a latency
		// that's less than the max allowed. Verify at the end
		// of the test that this happens at some point.
		lv.logger.Printf("end-to-end latency %s not yet below target steady latency %s\n",
			latency, lv.targetSteadyLatency)
		return
	}
	if latency > lv.maxSeenSteadyLatency {
		lv.maxSeenSteadyLatency = latency
	}
	lv.logger.Printf("end-to-end steady latency %s; max steady latency so far %s\n",
		latency, lv.maxSeenSteadyLatency)
}

func (lv *latencyVerifier) pollLatency(
	ctx context.Context, db *gosql.DB, jobID int, interval time.Duration, stopper chan struct{},
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-stopper:
			return nil
		case <-time.After(time.Second):
		}

		info, err := getChangefeedInfo(db, jobID)
		if err != nil {
			return err
		}
		if info.status != `running` {
			return errors.Errorf(`unexpected status: %s`, info.status)
		}
		lv.noteHighwater(info.highwaterTime)
	}
}

func (lv *latencyVerifier) assertValid(t *test) {
	if lv.initialScanLatency == 0 {
		t.Fatalf("initial scan did not complete")
	}
	if lv.initialScanLatency > lv.targetInitialScanLatency {
		t.Fatalf("initial scan latency was more than target: %s vs %s",
			lv.initialScanLatency, lv.targetInitialScanLatency)
	}
	if !lv.latencyBecameSteady {
		t.Fatalf("latency never dropped to acceptable steady level: %s", lv.targetSteadyLatency)
	}
	if lv.maxSeenSteadyLatency > lv.targetSteadyLatency {
		t.Fatalf("max latency was more than allowed: %s vs %s",
			lv.maxSeenSteadyLatency, lv.targetSteadyLatency)
	}
}

func createChangefeed(db *gosql.DB, targets, sinkURL string, initialScan bool) (int, error) {
	var jobID int
	createStmt := fmt.Sprintf(`CREATE CHANGEFEED FOR %s INTO $1 WITH resolved`, targets)
	extraArgs := []interface{}{sinkURL}
	if !initialScan {
		createStmt += `, cursor=$2`
		extraArgs = append(extraArgs, timeutil.Now().UnixNano())
	}
	if err := db.QueryRow(createStmt, extraArgs...).Scan(&jobID); err != nil {
		return 0, err
	}
	return jobID, nil
}

type changefeedInfo struct {
	status        string
	statementTime time.Time
	highwaterTime time.Time
}

func getChangefeedInfo(db *gosql.DB, jobID int) (changefeedInfo, error) {
	var status string
	var payloadBytes []byte
	var progressBytes []byte
	if err := db.QueryRow(
		`SELECT status, payload, progress FROM system.jobs WHERE id = $1`, jobID,
	).Scan(&status, &payloadBytes, &progressBytes); err != nil {
		return changefeedInfo{}, err
	}
	var payload jobspb.Payload
	if err := protoutil.Unmarshal(payloadBytes, &payload); err != nil {
		return changefeedInfo{}, err
	}
	var progress jobspb.Progress
	if err := protoutil.Unmarshal(progressBytes, &progress); err != nil {
		return changefeedInfo{}, err
	}
	var highwaterTime time.Time
	highwater := progress.GetHighWater()
	if highwater != nil {
		highwaterTime = highwater.GoTime()
	}
	return changefeedInfo{
		status:        status,
		statementTime: payload.GetChangefeed().StatementTime.GoTime(),
		highwaterTime: highwaterTime,
	}, nil
}

// stopFeeds cancels any running feeds on the cluster. Not necessary for the
// nightly, but nice for development.
func stopFeeds(db *gosql.DB, c *cluster) error {
	_, err := db.Exec(`CANCEL JOBS (
			SELECT job_id FROM [SHOW JOBS] WHERE status = 'running'
		)`)
	return err
}
