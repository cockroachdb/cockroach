// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
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
func registerCancel(r registry.Registry) {
	runCancel := func(ctx context.Context, t test.Test, c cluster.Cluster, tpchQueriesToRun []int, useDistsql bool) {
		c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
		c.Start(ctx, c.All())

		m := c.NewMonitor(ctx, c.All())
		m.Go(func(ctx context.Context) error {
			t.Status("restoring TPCH dataset for Scale Factor 1")
			if err := loadTPCHDataset(ctx, t, c, 1 /* sf */, c.NewMonitor(ctx), c.All()); err != nil {
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
				// sem is used to indicate that the query-runner goroutine has
				// been spawned up.
				sem := make(chan struct{})
				// Any error regarding the cancellation (or of its absence) will
				// be sent on errCh.
				errCh := make(chan error, 1)
				go func(query string) {
					defer close(errCh)
					t.L().Printf("executing q%d\n", queryNum)
					sem <- struct{}{}
					close(sem)
					_, err := conn.Exec(queryPrefix + query)
					if err == nil {
						errCh <- errors.New("query completed before it could be canceled")
					} else {
						fmt.Printf("query failed with error: %s\n", err)
						// Note that errors.Is() doesn't work here because
						// lib/pq wraps the query canceled error.
						if !strings.Contains(err.Error(), cancelchecker.QueryCanceledError.Error()) {
							errCh <- errors.Wrap(err, "unexpected error")
						}
					}
				}(tpch.QueriesByNumber[queryNum])

				// Wait for the query-runner goroutine to start.
				<-sem

				// The cancel query races with the execution of the query it's trying to
				// cancel, which may result in attempting to cancel the query before it
				// has started.  To be more confident that the query is executing, wait
				// a bit before attempting to cancel it.
				time.Sleep(250 * time.Millisecond)

				const cancelQuery = `CANCEL QUERIES
	SELECT query_id FROM [SHOW CLUSTER QUERIES] WHERE query not like '%SHOW CLUSTER QUERIES%'`
				c.Run(ctx, c.Node(1), `./cockroach sql --insecure -e "`+cancelQuery+`"`)
				cancelStartTime := timeutil.Now()

				select {
				case err, ok := <-errCh:
					if ok {
						t.Fatal(err)
					}
					// If errCh is closed, then the cancellation was successful.
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
	var queries string
	for i, q := range tpchQueriesToRun {
		if i > 0 {
			queries += ","
		}
		queries += fmt.Sprintf("%d", q)
	}

	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("cancel/tpch/distsql/queries=%s,nodes=%d", queries, numNodes),
		Owner:   registry.OwnerSQLQueries,
		Cluster: r.MakeClusterSpec(numNodes),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runCancel(ctx, t, c, tpchQueriesToRun, true /* useDistsql */)
		},
	})

	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("cancel/tpch/local/queries=%s,nodes=%d", queries, numNodes),
		Owner:   registry.OwnerSQLQueries,
		Cluster: r.MakeClusterSpec(numNodes),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runCancel(ctx, t, c, tpchQueriesToRun, false /* useDistsql */)
		},
	})
}
