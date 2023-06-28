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
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())

		m := c.NewMonitor(ctx, c.All())
		m.Go(func(ctx context.Context) error {
			conn := c.Conn(ctx, t.L(), 1)
			defer conn.Close()

			t.Status("restoring TPCH dataset for Scale Factor 1")
			if err := loadTPCHDataset(
				ctx, t, c, conn, 1 /* sf */, c.NewMonitor(ctx), c.All(), true, /* disableMergeQueue */
			); err != nil {
				t.Fatal(err)
			}

			queryPrefix := "USE tpch; "
			if !useDistsql {
				queryPrefix += "SET distsql = off; "
			}

			t.Status("running queries to cancel")
			successfullyCompletedErr := errors.New("query completed before it could be canceled")
			queryNotFoundRE, err := regexp.Compile("could not cancel query.*query ID.*not found")
			if err != nil {
				t.Fatal(err)
			}
			rng, _ := randutil.NewTestRand()
			for _, queryNum := range tpchQueriesToRun {
				const numRuns = 5
				var numCanceled int
				for run := 0; run < numRuns; run++ {
					// sem is used to indicate that the query-runner goroutine
					// has been spawned up.
					sem := make(chan struct{})
					// Any error regarding the cancellation (or of its absence)
					// will be sent on errCh.
					errCh := make(chan error, 1)
					go func(queryNum int) {
						runnerConn := c.Conn(ctx, t.L(), 1)
						defer runnerConn.Close()
						query := tpch.QueriesByNumber[queryNum]
						t.L().Printf("executing q%d\n", queryNum)
						close(sem)
						rows, err := runnerConn.Query(queryPrefix + "EXPLAIN ANALYZE (DISTSQL) " + query)
						if err == nil {
							err = successfullyCompletedErr
							for rows.Next() {
								var line string
								if err = rows.Scan(&line); err != nil {
									break
								}
								t.L().Printf("%s\n", line)
							}
						}
						errCh <- err
					}(queryNum)

					// Wait for the query-runner goroutine to start.
					<-sem

					// Continuously poll until we get the queryID that we want
					// to cancel. We expect it to show up within 10 seconds.
					var queryID string
					timeoutCh := time.After(10 * time.Second)
					for {
						// Sleep for some random duration up to 200ms. This
						// allows us to sometimes find the query when it's in
						// the planning stage while in most cases it's in the
						// execution stage already.
						time.Sleep(time.Duration(rng.Intn(200)+1) * time.Millisecond)
						rows, err := conn.Query(`SELECT query_id FROM [SHOW CLUSTER QUERIES] WHERE query NOT LIKE '%SHOW CLUSTER QUERIES%'`)
						if err != nil {
							t.Fatal(err)
						}
						if rows.Next() {
							if err = rows.Scan(&queryID); err != nil {
								t.Fatal(err)
							}
							break
						}
						if err = rows.Close(); err != nil {
							t.Fatal(err)
						}
						select {
						case <-timeoutCh:
							t.Fatal(errors.New("didn't see the query to cancel within 10 seconds"))
						default:
						}
					}

					_, err := conn.Exec(`CANCEL QUERY $1`, queryID)
					if err != nil {
						// We might have slept for too long so that the query
						// successfully finished and the query ID was deleted
						// from the query registry.
						if !queryNotFoundRE.MatchString(err.Error()) {
							t.Fatal(err)
						}
						t.Status(err)
						continue
					}
					cancelStartTime := timeutil.Now()

					select {
					case err := <-errCh:
						t.Status(err)
						if errors.Is(err, successfullyCompletedErr) {
							break
						} else if !strings.Contains(err.Error(), cancelchecker.QueryCanceledError.Error()) {
							// Note that errors.Is() doesn't work here because
							// lib/pq wraps the query canceled error.
							t.Fatal(errors.Wrap(err, "unexpected error"))
						}
						timeToCancel := timeutil.Since(cancelStartTime)
						t.Status(fmt.Sprintf("canceling q%d took %s\n", queryNum, timeToCancel))
						numCanceled++

					case <-time.After(5 * time.Second):
						t.Status("cancellation didn't occur within 5 seconds")
						select {
						case err = <-errCh:
							timeToCancel := timeutil.Since(cancelStartTime)
							t.Status(fmt.Sprintf("canceling q%d took %s\n", queryNum, timeToCancel))
							t.Fatal("query took too long to respond to cancellation")
						case <-time.After(time.Minute):
							t.Fatal("query didn't return after 1 minute either")
						}
					}
				}
				if minExpected := (numRuns + 1) / 2; numCanceled < minExpected {
					t.Fatalf(
						"expected at least %d successful cancellations out of %d runs, found only %d",
						minExpected, numRuns, numCanceled,
					)
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
		Leases:  registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runCancel(ctx, t, c, tpchQueriesToRun, true /* useDistsql */)
		},
	})

	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("cancel/tpch/local/queries=%s,nodes=%d", queries, numNodes),
		Owner:   registry.OwnerSQLQueries,
		Cluster: r.MakeClusterSpec(numNodes),
		Leases:  registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runCancel(ctx, t, c, tpchQueriesToRun, false /* useDistsql */)
		},
	})
}
