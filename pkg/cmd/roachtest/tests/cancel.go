// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
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
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())

		m := c.NewMonitor(ctx, c.All())
		m.Go(func(ctx context.Context) error {
			conn := c.Conn(ctx, t.L(), 1)
			defer conn.Close()

			t.Status("restoring TPCH dataset for Scale Factor 1")
			if err := loadTPCHDataset(
				ctx, t, c, conn, 1 /* sf */, c.NewMonitor(ctx), c.All(), false, /* disableMergeQueue */
			); err != nil {
				t.Fatal(err)
			}

			t.Status("running queries to cancel")
			rng, _ := randutil.NewTestRand()
			for _, queryNum := range tpchQueriesToRun {
				// Run each query 5 times to increase the test coverage.
				for run := 0; run < 5; run++ {
					// sem is used to indicate that the query-runner goroutine
					// has been spawned up and has done preliminary setup.
					sem := make(chan struct{})
					// An error will always be sent on errCh by the runner
					// (either query execution error or an error indicating the
					// absence of expected cancellation error).
					errCh := make(chan error, 1)
					t.Go(func(taskCtx context.Context, l *logger.Logger) error {
						runnerConn := c.Conn(taskCtx, l, 1)
						defer runnerConn.Close()
						setupQueries := []string{"USE tpch;"}
						if !useDistsql {
							setupQueries = append(setupQueries, "SET distsql = off;")
						}
						for _, setupQuery := range setupQueries {
							l.Printf("executing setup query %q", setupQuery)
							if _, err := runnerConn.Exec(setupQuery); err != nil {
								errCh <- err
								close(sem)
								// Errors are handled in the main goroutine.
								return nil //nolint:returnerrcheck
							}
						}
						query := tpch.QueriesByNumber[queryNum]
						l.Printf("executing q%d\n", queryNum)
						close(sem)
						_, err := runnerConn.Exec(query)
						if err == nil {
							err = errors.New("query completed before it could be canceled")
						}
						errCh <- err
						return nil
					}, task.Name("query-runner"))

					// Wait for the query-runner goroutine to start as well as
					// to execute setup queries.
					<-sem

					// Continuously poll until we get the queryID that we want
					// to cancel. We expect it to show up within 10 seconds.
					var queryID, query string
					timeoutCh := time.After(10 * time.Second)
					pollingStartTime := timeutil.Now()
					for {
						// Sleep for some random duration up to 500ms. This
						// allows us to sometimes find the query when it's in
						// the planning stage while in most cases it's in the
						// execution stage already.
						toSleep := time.Duration(rng.Intn(501)) * time.Millisecond
						t.Status(fmt.Sprintf("sleeping for %s", toSleep))
						time.Sleep(toSleep)
						rows, err := conn.Query(`SELECT query_id, query FROM [SHOW CLUSTER QUERIES] WHERE query NOT LIKE '%SHOW CLUSTER QUERIES%'`)
						if err != nil {
							t.Fatal(err)
						}
						if rows.Next() {
							if err = rows.Scan(&queryID, &query); err != nil {
								t.Fatal(err)
							}
							break
						}
						if err = rows.Close(); err != nil {
							t.Fatal(err)
						}
						select {
						case err = <-errCh:
							t.Fatalf("received an error from the runner goroutine before the query could be canceled: %v", err)
						case <-timeoutCh:
							t.Fatal(errors.New("didn't see the query to cancel within 10 seconds"))
						default:
						}
					}

					t.Status(fmt.Sprintf("canceling the query after waiting for %s", timeutil.Since(pollingStartTime)))
					_, err := conn.Exec(`CANCEL QUERY $1`, queryID)
					if err != nil {
						t.Status(fmt.Sprintf("%s: %q", queryID, query))
						t.Fatalf("encountered an error when canceling %q with queryID=%s: %v", query, queryID, err)
					}
					cancelStartTime := timeutil.Now()

					select {
					case err := <-errCh:
						t.Status(err)
						if !strings.Contains(err.Error(), cancelchecker.QueryCanceledError.Error()) {
							// Note that errors.Is() doesn't work here because
							// lib/pq wraps the query canceled error.
							t.Fatal(errors.Wrap(err, "unexpected error"))
						}
						timeToCancel := timeutil.Since(cancelStartTime)
						t.Status(fmt.Sprintf("canceling q%d took %s\n", queryNum, timeToCancel))

					case <-time.After(3 * time.Second):
						t.Fatal("query took too long to respond to cancellation")
					}
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
		// Uses gs://cockroach-fixtures-us-east1. See:
		// https://github.com/cockroachdb/cockroach/issues/105968
		CompatibleClouds: registry.Clouds(spec.GCE, spec.Local),
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runCancel(ctx, t, c, tpchQueriesToRun, true /* useDistsql */)
		},
	})

	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("cancel/tpch/local/queries=%s,nodes=%d", queries, numNodes),
		Owner:   registry.OwnerSQLQueries,
		Cluster: r.MakeClusterSpec(numNodes),
		// Uses gs://cockroach-fixtures-us-east1. See:
		// https://github.com/cockroachdb/cockroach/issues/105968
		CompatibleClouds: registry.Clouds(spec.GCE, spec.Local),
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runCancel(ctx, t, c, tpchQueriesToRun, false /* useDistsql */)
		},
	})
}
