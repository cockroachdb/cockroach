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
	"math"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func registerAllocator(r *testRegistry) {
	runAllocator := func(ctx context.Context, t *test, c *cluster, start int, maxStdDev float64) {
		const fixturePath = `gs://cockroach-fixtures/workload/tpch/scalefactor=10/backup`
		c.Put(ctx, cockroach, "./cockroach")
		c.Put(ctx, workload, "./workload")

		// Start the first `start` nodes and restore the fixture
		args := startArgs("--args=--vmodule=store_rebalancer=5,allocator=5,allocator_scorer=5,replicate_queue=5")
		c.Start(ctx, t, c.Range(1, start), args)
		db := c.Conn(ctx, 1)
		defer db.Close()

		m := newMonitor(ctx, c, c.Range(1, start))
		m.Go(func(ctx context.Context) error {
			t.Status("loading fixture")
			if _, err := db.Exec(`RESTORE DATABASE tpch FROM $1`, fixturePath); err != nil {
				t.Fatal(err)
			}
			return nil
		})
		m.Wait()

		// Start the remaining nodes to kick off upreplication/rebalancing.
		c.Start(ctx, t, c.Range(start+1, c.spec.NodeCount), args)

		c.Run(ctx, c.Node(1), `./workload init kv --drop`)
		for node := 1; node <= c.spec.NodeCount; node++ {
			node := node
			// TODO(dan): Ideally, the test would fail if this queryload failed,
			// but we can't put it in monitor as-is because the test deadlocks.
			go func() {
				const cmd = `./workload run kv --tolerate-errors --min-block-bytes=8 --max-block-bytes=127`
				l, err := t.l.ChildLogger(fmt.Sprintf(`kv-%d`, node))
				if err != nil {
					t.Fatal(err)
				}
				defer l.close()
				_ = execCmd(ctx, t.l, roachprod, "ssh", c.makeNodes(c.Node(node)), "--", cmd)
			}()
		}

		m = newMonitor(ctx, c, c.All())
		m.Go(func(ctx context.Context) error {
			t.Status("waiting for reblance")
			return waitForRebalance(ctx, t.l, db, maxStdDev)
		})
		m.Wait()
	}

	r.Add(testSpec{
		Name:    `replicate/up/1to3`,
		Owner:   OwnerKV,
		Cluster: makeClusterSpec(3),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runAllocator(ctx, t, c, 1, 10.0)
		},
	})
	r.Add(testSpec{
		Name:    `replicate/rebalance/3to5`,
		Owner:   OwnerKV,
		Cluster: makeClusterSpec(5),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runAllocator(ctx, t, c, 3, 42.0)
		},
	})
	r.Add(testSpec{
		Name:    `replicate/wide`,
		Owner:   OwnerKV,
		Timeout: 10 * time.Minute,
		Cluster: makeClusterSpec(9, cpu(1)),
		Run:     runWideReplication,
	})
}

// printRebalanceStats prints the time it took for rebalancing to finish and the
// final standard deviation of replica counts across stores.
func printRebalanceStats(l *logger, db *gosql.DB) error {
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
		`split`, `add`, `remove`,
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
// splits. We wait until the cluster is balanced or until `StableInterval`
// elapses, whichever comes first. Then, it prints stats about the rebalancing
// process. If the replica count appears unbalanced, an error is returned.
//
// This method is crude but necessary. If we were to wait until range counts
// were just about even, we'd miss potential post-rebalance thrashing.
func waitForRebalance(ctx context.Context, l *logger, db *gosql.DB, maxStdDev float64) error {
	// const statsInterval = 20 * time.Second
	const statsInterval = 2 * time.Second
	const stableSeconds = 3 * 60

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

func runWideReplication(ctx context.Context, t *test, c *cluster) {
	nodes := c.spec.NodeCount
	if nodes != 9 {
		t.Fatalf("9-node cluster required")
	}

	args := startArgs("--env=COCKROACH_SCAN_MAX_IDLE_TIME=5ms")
	c.Put(ctx, cockroach, "./cockroach")
	c.Start(ctx, t, c.All(), args)

	db := c.Conn(ctx, 1)
	defer db.Close()

	zones := func() []string {
		oldVersion := false
		rows, err := db.Query(`SELECT target FROM crdb_internal.zones`)
		// TODO(solon): Remove this block once we are no longer running roachtest
		// against version 19.1 and earlier.
		if err != nil && strings.Contains(err.Error(), `column "target" does not exist`) {
			oldVersion = true
			rows, err = db.Query(`SELECT zone_name FROM crdb_internal.zones`)
		}
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
			// TODO(solon): Remove this block once we are no longer running roachtest
			// against version 19.1 and earlier.
			if oldVersion {
				which := "RANGE"
				if name[0] == '.' {
					name = name[1:]
				} else if strings.Count(name, ".") == 0 {
					which = "DATABASE"
				} else {
					which = "TABLE"
				}
				name = fmt.Sprintf("%s %s", which, name)
			}
			results = append(results, name)
		}
		return results
	}

	run := func(stmt string) {
		t.l.Printf("%s\n", stmt)
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
			t.l.Printf("%d mis-replicated ranges\n", count)
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
	c.Stop(ctx)
	c.Start(ctx, t, c.Range(1, 6), args)

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
			t.l.Printf("%d unavailable, %d under-replicated ranges\n", unavailable, underReplicated)
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
		c.Run(ctx, c.Node(1),
			fmt.Sprintf("./cockroach node decommission --insecure --wait=none %d", id))
	}

	// Decommission a node. The ranges should down-replicate to 7 replicas.
	decom(9)
	waitForReplication(7)

	// Set the replication width to 5. The replicas should down-replicate, though
	// this currently requires the time-until-store-dead threshold to pass
	// because the allocator cannot select a replica for removal that is on a
	// store for which it doesn't have a store descriptor.
	run(`SET CLUSTER SETTING server.time_until_store_dead = '90s'`)
	setReplication(5)
	waitForReplication(5)

	// Restart the down nodes to prevent the dead node detector from complaining.
	c.Start(ctx, t, c.Range(7, 9))
}
