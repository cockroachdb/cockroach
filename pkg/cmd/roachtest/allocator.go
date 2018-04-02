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
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

func init() {
	runAllocator := func(ctx context.Context, t *test, c *cluster, start int, maxStdDev float64) {
		const fixturePath = `gs://cockroach-fixtures/workload/tpch/scalefactor=10/backup`
		c.Put(ctx, cockroach, "./cockroach")
		c.Put(ctx, workload, "./workload")

		// Start the first `start` nodes and restore the fixture
		args := startArgs("--args=--vmodule=allocator=5,allocator_scorer=5,replicate_queue=5")
		c.Start(ctx, c.Range(1, start), args)
		db := c.Conn(ctx, 1)
		defer db.Close()

		m := newMonitor(ctx, c, c.Range(1, start))
		m.Go(func(ctx context.Context) error {
			t.Status("loading fixture")
			if _, err := db.Exec(`RESTORE DATABASE workload FROM $1`, fixturePath); err != nil {
				t.Fatal(err)
			}
			return nil
		})
		m.Wait()

		// Start the remaining nodes to kick off upreplication/rebalancing.
		c.Start(ctx, c.Range(start+1, c.nodes), args)

		c.Run(ctx, c.Node(1), `./workload init kv --drop`)
		for node := 1; node <= c.nodes; node++ {
			node := node
			// TODO(dan): Ideally, the test would fail if this queryload failed,
			// but we can't put it in monitor as-is because the test deadlocks.
			go func() {
				const cmd = `./workload run kv --tolerate-errors --min-block-bytes=8 --max-block-bytes=128`
				l, err := c.l.childLogger(fmt.Sprintf(`kv-%d`, node))
				if err != nil {
					t.Fatal(err)
				}
				defer l.close()
				_ = execCmd(ctx, c.l, "roachprod", "ssh", c.makeNodes(c.Node(node)), "--", cmd)
			}()
		}

		m = newMonitor(ctx, c, c.All())
		m.Go(func(ctx context.Context) error {
			t.Status("waiting for reblance")
			return waitForRebalance(ctx, c.l, db, maxStdDev)
		})
		m.Wait()
	}

	tests.Add(testSpec{
		Name:  `upreplicate/1to3`,
		Nodes: nodes(3),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runAllocator(ctx, t, c, 1, 10.0)
		},
	})
	tests.Add(testSpec{
		Name:  `rebalance/3to5`,
		Nodes: nodes(5),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runAllocator(ctx, t, c, 3, 42.0)
		},
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
			`SELECT (SELECT MAX(timestamp) FROM system.rangelog) - `+
				`(SELECT MAX(timestamp) FROM system.eventlog WHERE "eventType"=$1)`,
			`node_join`, /* sql.EventLogNodeJoin */
		).Scan(&rebalanceIntervalStr); err != nil {
			return err
		}
		l.printf("cluster took %s to rebalance\n", rebalanceIntervalStr)
	}

	// Output # of range events that occurred. All other things being equal,
	// larger numbers are worse and potentially indicate thrashing.
	{
		var rangeEvents int64
		q := `SELECT COUNT(*) from system.rangelog`
		if err := db.QueryRow(q).Scan(&rangeEvents); err != nil {
			return err
		}
		l.printf("%d range events\n", rangeEvents)
	}

	// Output standard deviation of the replica counts for all stores.
	{
		var stdDev float64
		if err := db.QueryRow(
			`SELECT STDDEV(range_count) FROM crdb_internal.kv_store_status`,
		).Scan(&stdDev); err != nil {
			return err
		}
		l.printf("stdDev(replica count) = %.2f\n", stdDev)
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
			l.printf("s%d has %d ranges\n", storeID, rangeCount)
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
		`SELECT STDDEV(range_count) FROM crdb_internal.kv_store_status`,
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

			l.printf("%v\n", stats)
			if stableSeconds <= stats.SecondsSinceLastEvent {
				l.printf("replica count stddev = %f, max allowed stddev = %f\n", stats.ReplicaCountStdDev, maxStdDev)
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
