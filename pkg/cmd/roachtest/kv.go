// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func registerKV(r *testRegistry) {
	type kvOptions struct {
		nodes       int
		cpus        int
		readPercent int
		batchSize   int
		blockSize   int
		encryption  bool
		sequential  bool
	}
	runKV := func(ctx context.Context, t *test, c *cluster, opts kvOptions) {
		nodes := c.spec.NodeCount - 1
		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Put(ctx, workload, "./workload", c.Node(nodes+1))
		c.Start(ctx, t, c.Range(1, nodes), startArgs(fmt.Sprintf("--encrypt=%t", opts.encryption)))

		t.Status("running workload")
		m := newMonitor(ctx, c, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			concurrency := ifLocal("", " --concurrency="+fmt.Sprint(nodes*64))
			splits := " --splits=1000"
			duration := " --duration=" + ifLocal("10s", "10m")
			readPercent := fmt.Sprintf(" --read-percent=%d", opts.readPercent)
			histograms := " --histograms=" + perfArtifactsDir + "/stats.json"
			var batchSize string
			if opts.batchSize > 0 {
				batchSize = fmt.Sprintf(" --batch=%d", opts.batchSize)
			}

			var blockSize string
			if opts.blockSize > 0 {
				blockSize = fmt.Sprintf(" --min-block-bytes=%d --max-block-bytes=%d",
					opts.blockSize, opts.blockSize)
			}

			var sequential string
			if opts.sequential {
				splits = "" // no splits
				sequential = " --sequential"
			}
			cmd := fmt.Sprintf("./workload run kv --init"+
				histograms+concurrency+splits+duration+readPercent+batchSize+blockSize+sequential+
				" {pgurl:1-%d}", nodes)
			c.Run(ctx, c.Node(nodes+1), cmd)
			return nil
		})
		m.Wait()
	}

	for _, opts := range []kvOptions{
		// Standard configs.
		{nodes: 1, cpus: 8, readPercent: 0},
		{nodes: 1, cpus: 8, readPercent: 95},
		{nodes: 1, cpus: 32, readPercent: 0},
		{nodes: 1, cpus: 32, readPercent: 95},
		{nodes: 3, cpus: 8, readPercent: 0},
		{nodes: 3, cpus: 8, readPercent: 95},
		{nodes: 3, cpus: 32, readPercent: 0},
		{nodes: 3, cpus: 32, readPercent: 95},

		// Configs with large block sizes.
		{nodes: 3, cpus: 8, readPercent: 0, blockSize: 1 << 12 /* 4 KB */},
		{nodes: 3, cpus: 8, readPercent: 95, blockSize: 1 << 12 /* 4 KB */},
		{nodes: 3, cpus: 32, readPercent: 0, blockSize: 1 << 12 /* 4 KB */},
		{nodes: 3, cpus: 32, readPercent: 95, blockSize: 1 << 12 /* 4 KB */},
		{nodes: 3, cpus: 8, readPercent: 0, blockSize: 1 << 16 /* 64 KB */},
		{nodes: 3, cpus: 8, readPercent: 95, blockSize: 1 << 16 /* 64 KB */},
		{nodes: 3, cpus: 32, readPercent: 0, blockSize: 1 << 16 /* 64 KB */},
		{nodes: 3, cpus: 32, readPercent: 95, blockSize: 1 << 16 /* 64 KB */},

		// Configs with large batch sizes.
		{nodes: 3, cpus: 8, readPercent: 0, batchSize: 16},
		{nodes: 3, cpus: 8, readPercent: 95, batchSize: 16},

		// Configs with large nodes.
		{nodes: 3, cpus: 96, readPercent: 0},
		{nodes: 3, cpus: 96, readPercent: 95},
		{nodes: 4, cpus: 96, readPercent: 50, batchSize: 64},

		// Configs with encryption.
		{nodes: 1, cpus: 8, readPercent: 0, encryption: true},
		{nodes: 1, cpus: 8, readPercent: 95, encryption: true},
		{nodes: 3, cpus: 8, readPercent: 0, encryption: true},
		{nodes: 3, cpus: 8, readPercent: 95, encryption: true},

		// Configs with a sequential access pattern.
		{nodes: 3, cpus: 32, readPercent: 0, sequential: true},
		{nodes: 3, cpus: 32, readPercent: 95, sequential: true},
	} {
		opts := opts

		var nameParts []string
		nameParts = append(nameParts, fmt.Sprintf("kv%d", opts.readPercent))
		nameParts = append(nameParts, fmt.Sprintf("enc=%t", opts.encryption))
		nameParts = append(nameParts, fmt.Sprintf("nodes=%d", opts.nodes))
		if opts.cpus != 8 { // support legacy test name which didn't include cpu
			nameParts = append(nameParts, fmt.Sprintf("cpu=%d", opts.cpus))
		}
		if opts.batchSize != 0 { // support legacy test name which didn't include batch size
			nameParts = append(nameParts, fmt.Sprintf("batch=%d", opts.batchSize))
		}
		if opts.blockSize != 0 { // support legacy test name which didn't include block size
			nameParts = append(nameParts, fmt.Sprintf("size=%dkb", opts.blockSize>>10))
		}
		if opts.sequential {
			nameParts = append(nameParts, fmt.Sprintf("seq"))
		}

		minVersion := "v2.0.0"
		if opts.encryption {
			minVersion = "v2.1.0"
		}

		r.Add(testSpec{
			Name:       strings.Join(nameParts, "/"),
			MinVersion: minVersion,
			Cluster:    makeClusterSpec(opts.nodes+1, cpu(opts.cpus)),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runKV(ctx, t, c, opts)
			},
		})
	}
}

func registerKVContention(r *testRegistry) {
	const nodes = 4
	r.Add(testSpec{
		Name:       fmt.Sprintf("kv/contention/nodes=%d", nodes),
		MinVersion: "v19.2.0",
		Cluster:    makeClusterSpec(nodes + 1),
		Run: func(ctx context.Context, t *test, c *cluster) {
			c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
			c.Put(ctx, workload, "./workload", c.Node(nodes+1))

			// Start the cluster with an extremely high txn liveness threshold.
			// If requests ever get stuck on a transaction that was abandoned
			// then it will take 2m for them to get unstuck, at which point the
			// QPS threshold check in the test is likely to fail.
			args := startArgs("--env=COCKROACH_TXN_LIVENESS_HEARTBEAT_MULTIPLIER=120")
			c.Start(ctx, t, args, c.Range(1, nodes))

			// Enable request tracing, which is a good tool for understanding
			// how different transactions are interacting.
			c.Run(ctx, c.Node(1),
				`./cockroach sql --insecure -e "SET CLUSTER SETTING trace.debug.enable = true"`)

			t.Status("running workload")
			m := newMonitor(ctx, c, c.Range(1, nodes))
			m.Go(func(ctx context.Context) error {
				// Write to a small number of keys to generate a large amount of
				// contention. Use a relatively high amount of concurrency and
				// aim to average one concurrent write for each key in the keyspace.
				const cycleLength = 512
				const concurrency = 128
				const avgConcPerKey = 1
				const batchSize = avgConcPerKey * (cycleLength / concurrency)

				// Split the table so that each node can have a single leaseholder.
				splits := nodes

				// Run the workload for an hour. Add a secondary index to avoid
				// UPSERTs performing blind writes.
				const duration = 1 * time.Hour
				cmd := fmt.Sprintf("./workload run kv --init --secondary-index --duration=%s "+
					"--cycle-length=%d --concurrency=%d --batch=%d --splits=%d {pgurl:1-%d}",
					duration, cycleLength, concurrency, batchSize, splits, nodes)
				start := timeutil.Now()
				c.Run(ctx, c.Node(nodes+1), cmd)
				end := timeutil.Now()

				// Assert that the average throughput stayed above a certain
				// threshold. In this case, assert that max throughput only
				// dipped below 100 qps for 5% of the time.
				const minQPS = 100
				verifyTxnPerSecond(ctx, c, t, c.Node(1), start, end, minQPS, 0.05)
				return nil
			})
			m.Wait()
		},
	})
}

func registerKVQuiescenceDead(r *testRegistry) {
	r.Add(testSpec{
		Name:       "kv/quiescence/nodes=3",
		Cluster:    makeClusterSpec(4),
		MinVersion: "v2.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			nodes := c.spec.NodeCount - 1
			c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
			c.Put(ctx, workload, "./workload", c.Node(nodes+1))
			c.Start(ctx, t, c.Range(1, nodes))

			run := func(cmd string, lastDown bool) {
				n := nodes
				if lastDown {
					n--
				}
				m := newMonitor(ctx, c, c.Range(1, n))
				m.Go(func(ctx context.Context) error {
					t.WorkerStatus(cmd)
					defer t.WorkerStatus()
					return c.RunE(ctx, c.Node(nodes+1), cmd)
				})
				m.Wait()
			}

			db := c.Conn(ctx, 1)
			defer db.Close()

			waitForFullReplication(t, db)

			qps := func(f func()) float64 {

				numInserts := func() float64 {
					var v float64
					if err := db.QueryRowContext(
						ctx, `SELECT value FROM crdb_internal.node_metrics WHERE name = 'sql.insert.count'`,
					).Scan(&v); err != nil {
						t.Fatal(err)
					}
					return v
				}

				tBegin := timeutil.Now()
				before := numInserts()
				f()
				after := numInserts()
				return (after - before) / timeutil.Since(tBegin).Seconds()
			}

			const kv = "./workload run kv --duration=10m --read-percent=0"

			// Initialize the database with ~10k ranges so that the absence of
			// quiescence hits hard once a node goes down.
			run("./workload run kv --init --max-ops=1 --splits 10000 --concurrency 100 {pgurl:1}", false)
			run(kv+" --seed 0 {pgurl:1}", true) // warm-up
			// Measure qps with all nodes up (i.e. with quiescence).
			qpsAllUp := qps(func() {
				run(kv+" --seed 1 {pgurl:1}", true)
			})
			// Gracefully shut down third node (doesn't matter whether it's graceful or not).
			c.Run(ctx, c.Node(nodes), "./cockroach quit --insecure --host=:{pgport:3}")
			c.Stop(ctx, c.Node(nodes))
			// Measure qps with node down (i.e. without quiescence).
			qpsOneDown := qps(func() {
				// Use a different seed to make sure it's not just stepping into the
				// other earlier kv invocation's footsteps.
				run(kv+" --seed 2 {pgurl:1}", true)
			})

			if minFrac, actFrac := 0.8, qpsOneDown/qpsAllUp; actFrac < minFrac {
				t.Fatalf(
					"QPS dropped from %.2f to %.2f (factor of %.2f, min allowed %.2f)",
					qpsAllUp, qpsOneDown, actFrac, minFrac,
				)
			}
			t.l.Printf("QPS went from %.2f to %2.f with one node down\n", qpsAllUp, qpsOneDown)
			c.Start(ctx, t, c.Node(nodes)) // satisfy dead node detector
		},
	})
}

func registerKVGracefulDraining(r *testRegistry) {
	r.Add(testSpec{
		Skip:    "https://github.com/cockroachdb/cockroach/issues/33501",
		Name:    "kv/gracefuldraining/nodes=3",
		Cluster: makeClusterSpec(4),
		Run: func(ctx context.Context, t *test, c *cluster) {
			nodes := c.spec.NodeCount - 1
			c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
			c.Put(ctx, workload, "./workload", c.Node(nodes+1))
			c.Start(ctx, t, c.Range(1, nodes))

			db := c.Conn(ctx, 1)
			defer db.Close()

			waitForFullReplication(t, db)

			// Initialize the database with a lot of ranges so that there are
			// definitely a large number of leases on the node that we shut down
			// before it starts draining.
			splitCmd := "./workload run kv --init --max-ops=1 --splits 100 {pgurl:1}"
			c.Run(ctx, c.Node(nodes+1), splitCmd)

			m := newMonitor(ctx, c, c.Range(1, nodes))

			// Run kv for 5 minutes, during which we can gracefully kill nodes and
			// determine whether doing so affects the cluster-wide qps.
			const expectedQPS = 1000
			m.Go(func(ctx context.Context) error {
				cmd := fmt.Sprintf(
					"./workload run kv --duration=5m --read-percent=0 --tolerate-errors --max-rate=%d {pgurl:1-%d}",
					expectedQPS, nodes-1)
				t.WorkerStatus(cmd)
				defer t.WorkerStatus()
				return c.RunE(ctx, c.Node(nodes+1), cmd)
			})

			m.Go(func(ctx context.Context) error {
				// Gracefully shut down the third node, let the cluster run for a
				// while, then restart it. Then repeat for good measure.
				for i := 0; i < 2; i++ {
					select {
					case <-ctx.Done():
						return nil
					case <-time.After(1 * time.Minute):
					}
					c.Run(ctx, c.Node(nodes), "./cockroach quit --insecure --host=:{pgport:3}")
					c.Stop(ctx, c.Node(nodes))
					select {
					case <-ctx.Done():
						return nil
					case <-time.After(1 * time.Minute):
					}
					c.Start(ctx, t, c.Node(nodes))
				}
				return nil
			})

			// Let the test run for nearly the entire duration of the kv command.
			runDuration := 4*time.Minute + 30*time.Second
			time.Sleep(runDuration)

			// Check that the QPS has been at the expected max rate for the entire
			// test duration, even as one of the nodes was being stopped and started.
			adminURLs := c.ExternalAdminUIAddr(ctx, c.Node(1))
			url := "http://" + adminURLs[0] + "/ts/query"
			now := timeutil.Now()
			request := tspb.TimeSeriesQueryRequest{
				StartNanos: now.Add(-runDuration).UnixNano(),
				EndNanos:   now.UnixNano(),
				// Check the performance in each timeseries sample interval.
				SampleNanos: server.DefaultMetricsSampleInterval.Nanoseconds(),
				Queries: []tspb.Query{
					{
						Name:             "cr.node.sql.query.count",
						Downsampler:      tspb.TimeSeriesQueryAggregator_AVG.Enum(),
						SourceAggregator: tspb.TimeSeriesQueryAggregator_SUM.Enum(),
						Derivative:       tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE.Enum(),
					},
				},
			}
			var response tspb.TimeSeriesQueryResponse
			if err := httputil.PostJSON(http.Client{}, url, &request, &response); err != nil {
				t.Fatal(err)
			}
			if len(response.Results[0].Datapoints) <= 1 {
				t.Fatalf("not enough datapoints in timeseries query response: %+v", response)
			}
			datapoints := response.Results[0].Datapoints

			// Because we're specifying a --max-rate well less than what cockroach
			// should be capable of, draining one of the three nodes should have no
			// effect on performance at all, meaning that a fairly aggressive
			// threshold here should be ok.
			minQPS := expectedQPS * 0.9

			// Examine every data point except the first one, because at that time
			// splits may still have been happening or the cluster may still have
			// been initializing.
			for i := 1; i < len(datapoints); i++ {
				if qps := datapoints[i].Value; qps < minQPS {
					t.Fatalf(
						"QPS of %.2f at time %v is below minimum allowable QPS of %.2f; entire timeseries: %+v",
						qps, timeutil.Unix(0, datapoints[i].TimestampNanos), minQPS, datapoints)
				}
			}

			m.Wait()
		},
	})
}

func registerKVSplits(r *testRegistry) {
	for _, item := range []struct {
		quiesce bool
		splits  int
		timeout time.Duration
	}{
		// NB: with 500000 splits, this test sometimes fails since it's pushing
		// far past the number of replicas per node we support, at least if the
		// ranges start to unquiesce (which can set off a cascade due to resource
		// exhaustion).
		{true, 300000, 2 * time.Hour},
		// This version of the test prevents range quiescence to trigger the
		// badness described above more reliably for when we wish to improve
		// the performance. For now, just verify that 30k unquiesced ranges
		// is tenable.
		{false, 30000, 2 * time.Hour},
	} {
		item := item // for use in closure below
		r.Add(testSpec{
			Name:    fmt.Sprintf("kv/splits/nodes=3/quiesce=%t", item.quiesce),
			Timeout: item.timeout,
			Cluster: makeClusterSpec(4),
			Run: func(ctx context.Context, t *test, c *cluster) {
				nodes := c.spec.NodeCount - 1
				c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
				c.Put(ctx, workload, "./workload", c.Node(nodes+1))
				c.Start(ctx, t, c.Range(1, nodes),
					startArgs(
						// NB: this works. Don't change it or only one of the two vars may actually
						// make it to the server.
						"--env", "COCKROACH_MEMPROF_INTERVAL=1m COCKROACH_DISABLE_QUIESCENCE="+strconv.FormatBool(!item.quiesce),
						"--args=--cache=256MiB",
					))

				t.Status("running workload")
				m := newMonitor(ctx, c, c.Range(1, nodes))
				m.Go(func(ctx context.Context) error {
					concurrency := ifLocal("", " --concurrency="+fmt.Sprint(nodes*64))
					splits := " --splits=" + ifLocal("2000", fmt.Sprint(item.splits))
					cmd := fmt.Sprintf(
						"./workload run kv --init --max-ops=1"+
							concurrency+splits+
							" {pgurl:1-%d}",
						nodes)
					c.Run(ctx, c.Node(nodes+1), cmd)
					return nil
				})
				m.Wait()
			},
		})
	}
}

func registerKVScalability(r *testRegistry) {
	runScalability := func(ctx context.Context, t *test, c *cluster, percent int) {
		nodes := c.spec.NodeCount - 1

		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Put(ctx, workload, "./workload", c.Node(nodes+1))

		const maxPerNodeConcurrency = 64
		for i := nodes; i <= nodes*maxPerNodeConcurrency; i += nodes {
			c.Wipe(ctx, c.Range(1, nodes))
			c.Start(ctx, t, c.Range(1, nodes))

			t.Status("running workload")
			m := newMonitor(ctx, c, c.Range(1, nodes))
			m.Go(func(ctx context.Context) error {
				cmd := fmt.Sprintf("./workload run kv --init --read-percent=%d "+
					"--splits=1000 --duration=1m "+fmt.Sprintf("--concurrency=%d", i)+
					" {pgurl:1-%d}",
					percent, nodes)

				l, err := t.l.ChildLogger(fmt.Sprint(i))
				if err != nil {
					t.Fatal(err)
				}
				defer l.close()

				return c.RunL(ctx, l, c.Node(nodes+1), cmd)
			})
			m.Wait()
		}
	}

	// TODO(peter): work in progress adaption of `roachprod test kv{0,95}`.
	if false {
		for _, p := range []int{0, 95} {
			p := p
			r.Add(testSpec{
				Name:    fmt.Sprintf("kv%d/scale/nodes=6", p),
				Cluster: makeClusterSpec(7, cpu(8)),
				Run: func(ctx context.Context, t *test, c *cluster) {
					runScalability(ctx, t, c, p)
				},
			})
		}
	}
}

func registerKVRangeLookups(r *testRegistry) {
	type rangeLookupWorkloadType int
	const (
		splitWorkload rangeLookupWorkloadType = iota
		relocateWorkload
	)

	const (
		nodes = 8
		cpus  = 8
	)

	runRangeLookups := func(ctx context.Context, t *test, c *cluster, workers int, workloadType rangeLookupWorkloadType, maximumRangeLookupsPerSec float64) {
		nodes := c.spec.NodeCount - 1
		doneInit := make(chan struct{})
		doneWorkload := make(chan struct{})
		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Put(ctx, workload, "./workload", c.Node(nodes+1))
		c.Start(ctx, t, c.Range(1, nodes))

		t.Status("running workload")

		conns := make([]*gosql.DB, nodes)
		for i := 0; i < nodes; i++ {
			conns[i] = c.Conn(ctx, i+1)
		}
		defer func() {
			for i := 0; i < nodes; i++ {
				conns[i].Close()
			}
		}()
		waitForFullReplication(t, conns[0])

		m := newMonitor(ctx, c, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			defer close(doneWorkload)
			cmd := fmt.Sprintf("./workload init kv {pgurl:1-%d}", nodes)
			c.Run(ctx, c.Node(nodes+1), cmd)
			close(doneInit)
			concurrency := ifLocal("", " --concurrency="+fmt.Sprint(nodes*64))
			splits := " --splits=1000"
			duration := " --duration=" + ifLocal("10s", "10m")
			readPercent := " --read-percent=50"
			// We run kv with --tolerate-errors, since the relocate workload is
			// expected to create `result is ambiguous (removing replica)` errors.
			cmd = fmt.Sprintf("./workload run kv --tolerate-errors"+
				concurrency+splits+duration+readPercent+
				" {pgurl:1-%d}", nodes)
			start := timeutil.Now()
			c.Run(ctx, c.Node(nodes+1), cmd)
			end := timeutil.Now()
			verifyLookupsPerSec(ctx, c, t, c.Node(1), start, end, maximumRangeLookupsPerSec)
			return nil
		})

		<-doneInit
		for i := 0; i < workers; i++ {
			m.Go(func(ctx context.Context) error {
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-doneWorkload:
						return nil
					default:
					}

					conn := conns[c.Range(1, nodes).randNode()[0]-1]
					switch workloadType {
					case splitWorkload:
						_, err := conn.ExecContext(ctx, `
							ALTER TABLE
								kv.kv
							SPLIT AT
								VALUES (CAST(floor(random() * 9223372036854775808) AS INT))
							WITH EXPIRATION '1s'
						`)
						if err != nil && !pgerror.IsSQLRetryableError(err) {
							return err
						}
					case relocateWorkload:
						newReplicas := rand.Perm(nodes)[:3]
						_, err := conn.ExecContext(ctx, `
							ALTER TABLE
								kv.kv
							EXPERIMENTAL_RELOCATE
								SELECT ARRAY[$1, $2, $3], CAST(floor(random() * 9223372036854775808) AS INT)
						`, newReplicas[0]+1, newReplicas[1]+1, newReplicas[2]+1)
						if err != nil && !pgerror.IsSQLRetryableError(err) && !isExpectedRelocateError(err) {
							return err
						}
					default:
						panic("unexpected")
					}
				}
			})
		}
		m.Wait()
	}
	for _, item := range []struct {
		workers                   int
		workloadType              rangeLookupWorkloadType
		maximumRangeLookupsPerSec float64
	}{
		{2, splitWorkload, 10.0},
		// Relocates are expected to fail periodically when relocating random
		// ranges, so use more workers.
		{4, relocateWorkload, 50.0},
	} {
		// For use in closure.
		item := item
		var workloadName string
		switch item.workloadType {
		case splitWorkload:
			workloadName = "split"
		case relocateWorkload:
			workloadName = "relocate"
		default:
			panic("unexpected")
		}
		r.Add(testSpec{
			Name:    fmt.Sprintf("kv50/rangelookups/%s/nodes=%d", workloadName, nodes),
			Cluster: makeClusterSpec(nodes+1, cpu(cpus)),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runRangeLookups(ctx, t, c, item.workers, item.workloadType, item.maximumRangeLookupsPerSec)
			},
		})
	}
}
