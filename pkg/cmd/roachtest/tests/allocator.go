// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// The duration of no rebalancing actions taken before we assume the
// configuration is in a steady state and assess balance.
const allocatorStableSeconds = 120

func registerAllocator(r registry.Registry) {
	runAllocator := func(ctx context.Context, t test.Test, c cluster.Cluster, start int, maxStdDev float64) {
		// Put away one node to be the stats collector.
		nodes := len(c.CRDBNodes())

		// Don't start scheduled backups in this perf sensitive test that reports to roachperf
		startOpts := option.NewStartOpts(option.NoBackupSchedule)
		startOpts.RoachprodOpts.ExtraArgs = []string{"--vmodule=store_rebalancer=5,allocator=5,allocator_scorer=5,replicate_queue=5"}
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Range(1, start))
		db := c.Conn(ctx, t.L(), 1)
		defer db.Close()

		m := c.NewMonitor(ctx, c.Range(1, start))
		m.Go(func(ctx context.Context) error {
			t.Status("loading fixture")
			if err := c.RunE(
				ctx, option.WithNodes(c.Node(1)),
				"./cockroach", "workload", "fixtures", "import", "tpch", "--scale-factor", "10", "{pgurl:1}",
			); err != nil {
				t.Fatal(err)
			}
			return nil
		})
		m.Wait()

		// Setup the prometheus instance and client.
		cfg := (&prometheus.Config{}).
			WithCluster(c.CRDBNodes().InstallNodes()).
			WithPrometheusNode(c.WorkloadNode().InstallNodes()[0])

		err := c.StartGrafana(ctx, t.L(), cfg)
		require.NoError(t, err)

		cleanupFunc := func() {
			if err := c.StopGrafana(ctx, t.L(), t.ArtifactsDir()); err != nil {
				t.L().ErrorfCtx(ctx, "Error(s) shutting down prom/grafana %s", err)
			}
		}
		defer cleanupFunc()

		promClient, err := clusterstats.SetupCollectorPromClient(ctx, c, t.L(), cfg)
		require.NoError(t, err)

		// Setup the stats collector for the prometheus client.
		statCollector := clusterstats.NewStatsCollector(ctx, promClient)

		// Start the remaining nodes to kick off upreplication/rebalancing.
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Range(start+1, nodes))
		c.Run(ctx, option.WithNodes(c.Node(1)), "./cockroach workload init kv --drop {pgurl:1}")
		for node := 1; node <= nodes; node++ {
			t.Go(func(taskCtx context.Context, _ *logger.Logger) error {
				cmd := fmt.Sprintf("./cockroach workload run kv --tolerate-errors --min-block-bytes=8 --max-block-bytes=127 {pgurl%s}", c.Node(node))
				return c.RunE(taskCtx, option.WithNodes(c.Node(node)), cmd)
			}, task.Name(fmt.Sprintf(`kv-%d`, node)))
		}

		// Wait for 3x replication, we record the time taken to achieve this.
		var replicateTime time.Time
		startTime := timeutil.Now()
		m = c.NewMonitor(ctx, c.CRDBNodes())
		m.Go(func(ctx context.Context) error {
			err := roachtestutil.WaitFor3XReplication(ctx, t.L(), db)
			replicateTime = timeutil.Now()
			return err
		})
		m.Wait()

		// Wait for replica count balance, this occurs only following
		// up-replication finishing.
		m = c.NewMonitor(ctx, c.CRDBNodes())
		m.Go(func(ctx context.Context) error {
			t.Status("waiting for reblance")
			err := waitForRebalance(ctx, t.L(), db, maxStdDev, allocatorStableSeconds)
			if err != nil {
				return err
			}
			endTime := timeutil.Now()
			_, err = statCollector.Exporter().Export(
				ctx,
				c,
				t,
				false, /* dryRun */
				startTime, endTime,
				joinSummaryQueries(actionsSummary, requestBalanceSummary, resourceBalanceSummary, rebalanceCostSummary),
				// NB: We record the time taken to reach balance, from when
				// up-replication began, until the last rebalance action taken.
				// The up replication time, is the time taken to up-replicate
				// alone, not considering post up-replication rebalancing.
				func(stats map[string]clusterstats.StatSummary) (string, float64) {
					return "t-balance(s)", endTime.Sub(startTime).Seconds() - allocatorStableSeconds
				},
				func(stats map[string]clusterstats.StatSummary) (string, float64) {
					return "t-uprepl(s)", replicateTime.Sub(startTime).Seconds()
				},
			)
			return err
		})
		m.Wait()
	}

	r.Add(registry.TestSpec{
		Name:             `replicate/up/1to3`,
		Owner:            registry.OwnerKV,
		Benchmark:        true,
		Cluster:          r.MakeClusterSpec(4, spec.WorkloadNode()),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runAllocator(ctx, t, c, 1, 10.0)
		},
	})
	r.Add(registry.TestSpec{
		Name:             `replicate/rebalance/3to5`,
		Owner:            registry.OwnerKV,
		Benchmark:        true,
		Cluster:          r.MakeClusterSpec(6, spec.WorkloadNode()),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runAllocator(ctx, t, c, 3, 42.0)
		},
	})
	r.Add(registry.TestSpec{
		Name:      `replicate/wide`,
		Owner:     registry.OwnerKV,
		Benchmark: true,
		// Allow a longer running time to account for runs that use a
		// cockroach build with runtime assertions enabled.
		Timeout:          30 * time.Minute,
		Cluster:          r.MakeClusterSpec(9, spec.CPU(1), spec.WorkloadNode(), spec.WorkloadNodeCPU(1)),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Run:              runWideReplication,
	})
}

// printRebalanceStats prints the time it took for rebalancing to finish and the
// final standard deviation of replica counts across stores.
func printRebalanceStats(l *logger.Logger, db *gosql.DB) error {
	// TODO(cuongdo): Output these in a machine-friendly way and graph.

	// Output time it took to rebalance.
	{
		var rebalanceIntervalStr string
		if err := db.QueryRow(
			`SELECT (SELECT max(timestamp) FROM system.rangelog) - `+
				`(SELECT max(timestamp) FROM system.eventlog WHERE "eventType"=$1)`,
			`node_join`, /* sql.EventLogNodeJoin */
		).Scan(&rebalanceIntervalStr); err != nil {
			return err
		}
		l.Printf("cluster took %s to rebalance\n", rebalanceIntervalStr)
	}

	// Output # of range events that occurred. All other things being equal,
	// larger numbers are worse and potentially indicate thrashing.
	{
		var rangeEvents int64
		q := `SELECT count(*) from system.rangelog`
		if err := db.QueryRow(q).Scan(&rangeEvents); err != nil {
			return err
		}
		l.Printf("%d range events\n", rangeEvents)
	}

	// Output standard deviation of the replica counts for all stores.
	{
		var stdDev float64
		if err := db.QueryRow(
			`SELECT stddev(range_count) FROM crdb_internal.kv_store_status`,
		).Scan(&stdDev); err != nil {
			return err
		}
		l.Printf("stdDev(replica count) = %.2f\n", stdDev)
	}

	// Output the number of ranges on each store.
	{
		rows, err := db.Query(`SELECT store_id, range_count FROM crdb_internal.kv_store_status`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var storeID, rangeCount int64
			if err := rows.Scan(&storeID, &rangeCount); err != nil {
				return err
			}
			l.Printf("s%d has %d ranges\n", storeID, rangeCount)
		}
	}

	return nil
}

type replicationStats struct {
	SecondsSinceLastEvent int64
	EventType             string
	RangeID               int64
	StoreID               int64
	ReplicaCountStdDev    float64
}

func (s replicationStats) String() string {
	return fmt.Sprintf("last range event: %s for range %d/store %d (%ds ago)",
		s.EventType, s.RangeID, s.StoreID, s.SecondsSinceLastEvent)
}

// allocatorStats returns the duration of stability (i.e. no replication
// changes) and the standard deviation in replica counts. Only unrecoverable
// errors are returned.
func allocatorStats(db *gosql.DB) (s replicationStats, err error) {
	defer func() {
		if err != nil {
			s.ReplicaCountStdDev = math.MaxFloat64
		}
	}()

	// NB: These are the storage.RangeLogEventType enum, but it's intentionally
	// not used to avoid pulling in the dep.
	eventTypes := []interface{}{
		// NB: these come from storagepb.RangeLogEventType.
		`split`, `add_voter`, `remove_voter`,
	}

	q := `SELECT extract_duration(seconds FROM now()-timestamp), "rangeID", "storeID", "eventType"` +
		`FROM system.rangelog WHERE "eventType" IN ($1, $2, $3) ORDER BY timestamp DESC LIMIT 1`

	row := db.QueryRow(q, eventTypes...)
	if row == nil {
		// This should never happen, because the archived store we're starting with
		// will always have some range events.
		return replicationStats{}, errors.New("couldn't find any range events")
	}
	if err := row.Scan(&s.SecondsSinceLastEvent, &s.RangeID, &s.StoreID, &s.EventType); err != nil {
		return replicationStats{}, err
	}

	if err := db.QueryRow(
		`SELECT stddev(range_count) FROM crdb_internal.kv_store_status`,
	).Scan(&s.ReplicaCountStdDev); err != nil {
		return replicationStats{}, err
	}

	return s, nil
}

// waitForRebalance waits until there's been no recent range adds, removes, and
// splits. We wait until the cluster is balanced or until `stableSeconds`
// elapses, whichever comes first. Then, it prints stats about the rebalancing
// process. If the replica count appears unbalanced, an error is returned.
//
// This method is crude but necessary. If we were to wait until range counts
// were just about even, we'd miss potential post-rebalance thrashing.
func waitForRebalance(
	ctx context.Context, l *logger.Logger, db *gosql.DB, maxStdDev float64, stableSeconds int64,
) error {
	const statsInterval = 2 * time.Second

	var statsTimer timeutil.Timer
	defer statsTimer.Stop()
	statsTimer.Reset(statsInterval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-statsTimer.C:
			statsTimer.Read = true
			stats, err := allocatorStats(db)
			if err != nil {
				return err
			}

			l.Printf("%v\n", stats)
			if stableSeconds <= stats.SecondsSinceLastEvent {
				l.Printf("replica count stddev = %f, max allowed stddev = %f\n", stats.ReplicaCountStdDev, maxStdDev)
				if stats.ReplicaCountStdDev > maxStdDev {
					_ = printRebalanceStats(l, db)
					return errors.Errorf(
						"%ds elapsed without changes, but replica count standard "+
							"deviation is %.2f (>%.2f)", stats.SecondsSinceLastEvent,
						stats.ReplicaCountStdDev, maxStdDev)
				}
				return printRebalanceStats(l, db)
			}
			statsTimer.Reset(statsInterval)
		}
	}
}

func runWideReplication(ctx context.Context, t test.Test, c cluster.Cluster) {
	nodes := c.Spec().NodeCount
	if nodes != 9 {
		t.Fatalf("9-node cluster required")
	}

	startOpts := option.DefaultStartOpts()
	startOpts.RoachprodOpts.ExtraArgs = []string{"--vmodule=replicate_queue=6"}
	settings := install.MakeClusterSettings()
	settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=5ms")
	c.Start(ctx, t.L(), startOpts, settings, c.All())

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	zones := func() []string {
		rows, err := db.Query(`SELECT target FROM crdb_internal.zones`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var results []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatal(err)
			}
			results = append(results, name)
		}
		return results
	}

	run := func(stmt string) {
		t.L().Printf("%s\n", stmt)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}

	setReplication := func(width int) {
		// Change every zone to have the same number of replicas as the number of
		// nodes in the cluster.
		for _, zone := range zones() {
			run(fmt.Sprintf(`ALTER %s CONFIGURE ZONE USING num_replicas = %d`, zone, width))
		}
	}
	setReplication(nodes)

	countMisreplicated := func(width int) int {
		var count int
		if err := db.QueryRow(
			"SELECT count(*) FROM crdb_internal.ranges WHERE array_length(replicas,1) != $1",
			width,
		).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}

	waitForReplication := func(width int) {
		for count := -1; count != 0; time.Sleep(time.Second) {
			count = countMisreplicated(width)
			t.L().Printf("%d mis-replicated ranges\n", count)
		}
	}

	waitForReplication(nodes)

	numRanges := func() int {
		var count int
		if err := db.QueryRow(`SELECT count(*) FROM crdb_internal.ranges`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}()

	// Stop the cluster and restart 2/3 of the nodes.
	c.Stop(ctx, t.L(), option.DefaultStopOpts())
	tBeginDown := timeutil.Now()
	c.Start(ctx, t.L(), startOpts, settings, c.Range(1, 6))

	waitForUnderReplicated := func(count int) {
		for start := timeutil.Now(); ; time.Sleep(time.Second) {
			query := `
SELECT sum((metrics->>'ranges.unavailable')::DECIMAL)::INT AS ranges_unavailable,
       sum((metrics->>'ranges.underreplicated')::DECIMAL)::INT AS ranges_underreplicated
FROM crdb_internal.kv_store_status
`
			var unavailable, underReplicated int
			if err := db.QueryRow(query).Scan(&unavailable, &underReplicated); err != nil {
				t.Fatal(err)
			}
			t.L().Printf("%d unavailable, %d under-replicated ranges\n", unavailable, underReplicated)
			if unavailable != 0 {
				// A freshly started cluster might show unavailable ranges for a brief
				// period of time due to the way that metric is calculated. Only
				// complain about unavailable ranges if they persist for too long.
				if timeutil.Since(start) >= 30*time.Second {
					t.Fatalf("%d unavailable ranges", unavailable)
				}
				continue
			}
			if underReplicated >= count {
				break
			}
		}
	}

	waitForUnderReplicated(numRanges)
	if n := countMisreplicated(9); n != 0 {
		t.Fatalf("expected 0 mis-replicated ranges, but found %d", n)
	}

	decom := func(id int) {
		c.Run(ctx, option.WithNodes(c.Node(1)),
			fmt.Sprintf("./cockroach node decommission --certs-dir=%s --port={pgport%s} --wait=none %d", install.CockroachNodeCertsDir, c.Node(id), id))
	}

	// Decommission a node. The ranges should down-replicate to 7 replicas.
	decom(9)
	waitForReplication(7)

	// Set the replication width to 5. The replicas should down-replicate, though
	// this currently requires the time-until-store-dead threshold to pass
	// because the allocator cannot select a replica for removal that is on a
	// store for which it doesn't have a store descriptor.
	run(`SET CLUSTER SETTING server.time_until_store_dead = '90s'`)
	// Sleep until the node is dead so that when we actually wait for replication,
	// we can expect things to move swiftly.
	time.Sleep(90*time.Second - timeutil.Since(tBeginDown))

	setReplication(5)
	waitForReplication(5)

	// Restart the down nodes to prevent the dead node detector from complaining.
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Range(7, 9))
}
