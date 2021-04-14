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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/tpch"
	"github.com/cockroachdb/errors"
)

// Motivation:
// Although there are unit tests that test query cancellation, they have been
// insufficient in detecting problems with canceling long-running, multi-node
// DistSQL queries. This is because, unlike local queries which only need to
// cancel the transaction's context, DistSQL queries must cancel flow contexts
// on each node involved in the query. Typical strategies for local execution
// testing involve using a builtin like generate_series to create artificially
// long-running queries, but these approaches don't create multi-node DistSQL
// queries; the only way to do so is by querying a large dataset split across
// multiple nodes. Due to the high cost of loading the pre-requisite data, these
// tests are best suited as nightlies.
//
// Once DistSQL queries provide more testing knobs, these tests can likely be
// replaced with unit tests.
func registerCancel(r *testRegistry) {
	runCancel := func(ctx context.Context, t *test, c *cluster, tpchQueriesToRun []int, useDistsql bool) {
		c.Put(ctx, cockroach, "./cockroach", c.All())
		c.Start(ctx, t, c.All())

		m := newMonitor(ctx, c, c.All())
		m.Go(func(ctx context.Context) error {
			t.Status("restoring TPCH dataset for Scale Factor 1")
			if err := loadTPCHDataset(ctx, t, c, 1 /* sf */, newMonitor(ctx, c), c.All()); err != nil {
				t.Fatal(err)
			}

			conn := c.Conn(ctx, 1)
			defer conn.Close()

			queryPrefix := "USE tpch; "
			if !useDistsql {
				queryPrefix += "SET distsql = off; "
			}

			t.Status("running queries to cancel")
			for _, queryNum := range tpchQueriesToRun {
				sem := make(chan struct{}, 1)
				go func(query string) {
					t.l.Printf("executing \"%s\"\n", query)
					sem <- struct{}{}
					_, err := conn.Exec(queryPrefix + query)
					if err == nil {
						close(sem)
						t.Fatal("query completed before it could be canceled")
					} else {
						fmt.Printf("query failed with error: %s\n", err)
						if !errors.Is(err, cancelchecker.QueryCanceledError) {
							t.Fatal("unexpected error")
						}
					}
					sem <- struct{}{}
				}(tpch.QueriesByNumber[queryNum])

				<-sem

				// The cancel query races with the execution of the query it's trying to
				// cancel, which may result in attempting to cancel the query before it
				// has started.  To be more confident that the query is executing, wait
				// a bit before attempting to cancel it.
				time.Sleep(100 * time.Millisecond)

				const cancelQuery = `CANCEL QUERIES
	SELECT query_id FROM [SHOW CLUSTER QUERIES] WHERE query not like '%SHOW CLUSTER QUERIES%'`
				c.Run(ctx, c.Node(1), `./cockroach sql --insecure -e "`+cancelQuery+`"`)
				cancelStartTime := timeutil.Now()

				select {
				case _, ok := <-sem:
					if !ok {
						t.Fatal("query could not be canceled")
					}
					timeToCancel := timeutil.Now().Sub(cancelStartTime)
					fmt.Printf("canceling q%d took %s\n", queryNum, timeToCancel)

				case <-time.After(5 * time.Second):
					t.Fatal("query took too long to respond to cancellation")
				}
			}

			return nil
		})
		m.Wait()
	}

	const numNodes = 3
	// Choose several longer running TPCH queries (each is taking at least 3s to
	// complete).
	tpchQueriesToRun := []int{7, 9, 20, 21}

	r.Add(testSpec{
		Name:    fmt.Sprintf("cancel/tpch/distsql/queries=%v,nodes=%d", tpchQueriesToRun, numNodes),
		Owner:   OwnerSQLQueries,
		Cluster: makeClusterSpec(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runCancel(ctx, t, c, tpchQueriesToRun, true /* useDistsql */)
		},
	})

	r.Add(testSpec{
		Name:    fmt.Sprintf("cancel/tpch/local/queries=%v,nodes=%d", tpchQueriesToRun, numNodes),
		Owner:   OwnerSQLQueries,
		Cluster: makeClusterSpec(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runCancel(ctx, t, c, tpchQueriesToRun, false /* useDistsql */)
		},
	})
}
