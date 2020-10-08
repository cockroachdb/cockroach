// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	gosql "database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

type queryCacheTestHelper struct {
	srv  serverutils.TestServerInterface
	godb *gosql.DB

	conns   []*gosql.Conn
	runners []*sqlutils.SQLRunner

	hitsDelta, missesDelta int
}

func makeQueryCacheTestHelper(tb testing.TB, numConns int) *queryCacheTestHelper {
	h := &queryCacheTestHelper{}
	h.srv, h.godb, _ = serverutils.StartServer(tb, base.TestServerArgs{})

	h.conns = make([]*gosql.Conn, numConns)
	h.runners = make([]*sqlutils.SQLRunner, numConns)
	for i := range h.conns {
		var err error
		h.conns[i], err = h.godb.Conn(context.Background())
		if err != nil {
			tb.Fatal(err)
		}
		h.runners[i] = sqlutils.MakeSQLRunner(h.conns[i])
	}
	r0 := h.runners[0]
	r0.Exec(tb, "DROP DATABASE IF EXISTS db1")
	r0.Exec(tb, "DROP DATABASE IF EXISTS db2")
	r0.Exec(tb, "CREATE DATABASE db1")
	r0.Exec(tb, "CREATE TABLE db1.t (a INT, b INT)")
	r0.Exec(tb, "INSERT INTO db1.t VALUES (1, 1)")
	for _, r := range h.runners {
		r.Exec(tb, "SET DATABASE = db1")
	}
	r0.Exec(tb, "SET CLUSTER SETTING sql.query_cache.enabled = true")
	h.ResetStats()
	return h
}

func (h *queryCacheTestHelper) Stop() {
	h.srv.Stopper().Stop(context.Background())
}

func (h *queryCacheTestHelper) GetStats() (numHits, numMisses int) {
	return int(h.srv.MustGetSQLCounter(MetaSQLOptPlanCacheHits.Name)) - h.hitsDelta,
		int(h.srv.MustGetSQLCounter(MetaSQLOptPlanCacheMisses.Name)) - h.missesDelta
}

func (h *queryCacheTestHelper) ResetStats() {
	hits, misses := h.GetStats()
	h.hitsDelta += hits
	h.missesDelta += misses
}

func (h *queryCacheTestHelper) AssertStats(tb *testing.T, expHits, expMisses int) {
	tb.Helper()
	hits, misses := h.GetStats()
	assert.Equal(tb, expHits, hits, "hits")
	assert.Equal(tb, expMisses, misses, "misses")
}

// CheckStats is similar to AssertStats, but returns an error instead of
// failing the test if the actual stats don't match the expected stats.
func (h *queryCacheTestHelper) CheckStats(tb *testing.T, expHits, expMisses int) error {
	tb.Helper()
	hits, misses := h.GetStats()
	if expHits != hits {
		return errors.Errorf("expected %d hits but found %d", expHits, hits)
	}
	if expMisses != misses {
		return errors.Errorf("expected %d misses but found %d", expMisses, misses)
	}
	return nil
}

func TestQueryCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Grouping the parallel subtests into a non-parallel subtest allows the defer
	// call above to work as expected.
	t.Run("group", func(t *testing.T) {
		t.Run("simple", func(t *testing.T) {
			t.Parallel() // SAFE FOR TESTING
			const numConns = 4
			h := makeQueryCacheTestHelper(t, numConns)
			defer h.Stop()

			// Alternate between the connections.
			for i := 0; i < 5; i++ {
				for _, r := range h.runners {
					r.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1", "1"}})
				}
			}
			// We should have 1 miss and the rest hits.
			h.AssertStats(t, 5*numConns-1, 1)
		})

		t.Run("simple-prepare", func(t *testing.T) {
			t.Parallel() // SAFE FOR TESTING
			const numConns = 4
			h := makeQueryCacheTestHelper(t, numConns)
			defer h.Stop()

			// Alternate between the connections.
			for i := 0; i < 5; i++ {
				for _, r := range h.runners {
					r.Exec(t, fmt.Sprintf("PREPARE a%d AS SELECT * FROM t", i))
				}
			}
			// We should have 1 miss and the rest hits.
			h.AssertStats(t, 5*numConns-1, 1)

			for i := 0; i < 5; i++ {
				for _, r := range h.runners {
					r.CheckQueryResults(
						t,
						fmt.Sprintf("EXECUTE a%d", i),
						[][]string{{"1", "1"}},
					)
				}
			}
		})

		t.Run("simple-prepare-with-args", func(t *testing.T) {
			t.Parallel() // SAFE FOR TESTING
			const numConns = 4
			h := makeQueryCacheTestHelper(t, numConns)
			defer h.Stop()

			// Alternate between the connections.
			for i := 0; i < 5; i++ {
				for _, r := range h.runners {
					r.Exec(t, fmt.Sprintf("PREPARE a%d AS SELECT a + $1, b + $2 FROM t", i))
				}
			}
			// We should have 1 miss and the rest hits.
			h.AssertStats(t, 5*numConns-1, 1)

			for i := 0; i < 5; i++ {
				for _, r := range h.runners {
					r.CheckQueryResults(
						t,
						fmt.Sprintf("EXECUTE a%d (10, 100)", i),
						[][]string{{"11", "101"}},
					)
					r.CheckQueryResults(
						t,
						fmt.Sprintf("EXECUTE a%d (20, 200)", i),
						[][]string{{"21", "201"}},
					)
				}
			}
		})

		// Verify that using a relative timestamp literal interacts correctly with
		// the query cache (#48717).
		t.Run("relative-timestamp", func(t *testing.T) {
			t.Parallel() // SAFE FOR TESTING
			h := makeQueryCacheTestHelper(t, 1 /* numConns */)
			defer h.Stop()

			r := h.runners[0]
			res := r.QueryStr(t, "SELECT 'now'::TIMESTAMP")
			time.Sleep(time.Millisecond)
			res2 := r.QueryStr(t, "SELECT 'now'::TIMESTAMP")
			if reflect.DeepEqual(res, res2) {
				t.Error("expected different result")
			}
		})

		t.Run("parallel", func(t *testing.T) {
			t.Parallel() // SAFE FOR TESTING
			const numConns = 4
			h := makeQueryCacheTestHelper(t, numConns)
			defer h.Stop()

			var group errgroup.Group
			for connIdx := range h.conns {
				c := h.conns[connIdx]
				group.Go(func() error {
					for j := 0; j < 10; j++ {
						rows, err := c.QueryContext(context.Background(), "SELECT * FROM t")
						if err != nil {
							return err
						}
						res, err := sqlutils.RowsToStrMatrix(rows)
						if err != nil {
							return err
						}
						if !reflect.DeepEqual(res, [][]string{{"1", "1"}}) {
							return errors.Errorf("incorrect results %v", res)
						}
					}
					return nil
				})
			}
			if err := group.Wait(); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("parallel-prepare", func(t *testing.T) {
			t.Parallel() // SAFE FOR TESTING
			const numConns = 4
			h := makeQueryCacheTestHelper(t, numConns)
			defer h.Stop()

			var group errgroup.Group
			for connIdx := range h.conns {
				c := h.conns[connIdx]
				group.Go(func() error {
					ctx := context.Background()
					for j := 0; j < 10; j++ {
						// Query with a multi-use CTE (as a regression test for #44867). The
						// left join condition never passes so this is really equivalent to:
						//   SELECT a+$1,b+$2 FROM t
						query := fmt.Sprintf(`PREPARE a%d AS
WITH cte(x,y) AS (SELECT a+$1, b+$2 FROM t)
SELECT cte.x, cte.y FROM cte LEFT JOIN cte as cte2 on cte.y = cte2.x`, j)

						if _, err := c.ExecContext(ctx, query); err != nil {
							return err
						}
						rows, err := c.QueryContext(ctx, fmt.Sprintf("EXECUTE a%d (10, 100)", j))
						if err != nil {
							return err
						}
						res, err := sqlutils.RowsToStrMatrix(rows)
						if err != nil {
							return err
						}
						if !reflect.DeepEqual(res, [][]string{{"11", "101"}}) {
							return errors.Errorf("incorrect results %v", res)
						}
					}
					return nil
				})
			}
			if err := group.Wait(); err != nil {
				t.Fatal(err)
			}
		})

		// Test connections running the same statement but under different databases.
		t.Run("multidb", func(t *testing.T) {
			t.Parallel() // SAFE FOR TESTING
			const numConns = 4
			h := makeQueryCacheTestHelper(t, numConns)
			defer h.Stop()

			r0 := h.runners[0]
			r0.Exec(t, "CREATE DATABASE db2")
			r0.Exec(t, "CREATE TABLE db2.t (a INT)")
			r0.Exec(t, "INSERT INTO db2.t VALUES (2)")
			for i := range h.runners {
				if i%2 == 1 {
					h.runners[i].Exec(t, "SET DATABASE = db2")
				}
			}
			// Alternate between the connections.
			for i := 0; i < 5; i++ {
				for j, r := range h.runners {
					var res [][]string
					if j%2 == 0 {
						res = [][]string{{"1", "1"}}
					} else {
						res = [][]string{{"2"}}
					}
					r.CheckQueryResults(t, "SELECT * FROM t", res)
				}
			}
		})

		t.Run("multidb-prepare", func(t *testing.T) {
			t.Parallel() // SAFE FOR TESTING
			const numConns = 4
			h := makeQueryCacheTestHelper(t, numConns)
			defer h.Stop()

			r0 := h.runners[0]
			r0.Exec(t, "CREATE DATABASE db2")
			r0.Exec(t, "CREATE TABLE db2.t (a INT)")
			r0.Exec(t, "INSERT INTO db2.t VALUES (2)")
			for i := range h.runners {
				if i%2 == 1 {
					h.runners[i].Exec(t, "SET DATABASE = db2")
				}
			}
			// Alternate between the connections.
			for i := 0; i < 5; i++ {
				for j, r := range h.runners {
					r.Exec(t, fmt.Sprintf("PREPARE a%d AS SELECT a + $1 FROM t", i))
					var res [][]string
					if j%2 == 0 {
						res = [][]string{{"11"}}
					} else {
						res = [][]string{{"12"}}
					}
					r.CheckQueryResults(t, fmt.Sprintf("EXECUTE a%d (10)", i), res)
				}
			}
		})

		// Test that a schema change triggers cache invalidation.
		t.Run("schemachange", func(t *testing.T) {
			t.Parallel() // SAFE FOR TESTING
			h := makeQueryCacheTestHelper(t, 2 /* numConns */)
			defer h.Stop()
			r0, r1 := h.runners[0], h.runners[1]
			r0.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1", "1"}})
			h.AssertStats(t, 0 /* hits */, 1 /* misses */)
			r1.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1", "1"}})
			h.AssertStats(t, 1 /* hits */, 1 /* misses */)
			r0.Exec(t, "ALTER TABLE t ADD COLUMN c INT AS (a+b) STORED")
			h.AssertStats(t, 1 /* hits */, 1 /* misses */)
			r1.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1", "1", "2"}})
			h.AssertStats(t, 1 /* hits */, 2 /* misses */)
		})

		// Test that creating new statistics triggers cache invalidation.
		t.Run("statschange", func(t *testing.T) {
			t.Parallel() // SAFE FOR TESTING
			h := makeQueryCacheTestHelper(t, 2 /* numConns */)
			defer h.Stop()
			r0, r1 := h.runners[0], h.runners[1]
			r0.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1", "1"}})
			h.AssertStats(t, 0 /* hits */, 1 /* misses */)
			r1.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1", "1"}})
			h.AssertStats(t, 1 /* hits */, 1 /* misses */)
			r0.Exec(t, "CREATE STATISTICS s FROM t")
			h.AssertStats(t, 1 /* hits */, 1 /* misses */)
			hits := 1
			testutils.SucceedsSoon(t, func() error {
				// The stats cache is updated asynchronously, so we may get some hits
				// before we get a miss.
				r1.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1", "1"}})
				if err := h.CheckStats(t, hits, 2 /* misses */); err != nil {
					hits++
					return err
				}
				return nil
			})
		})

		// Test that a schema change triggers cache invalidation.
		t.Run("schemachange-prepare", func(t *testing.T) {
			t.Parallel() // SAFE FOR TESTING
			h := makeQueryCacheTestHelper(t, 2 /* numConns */)
			defer h.Stop()
			r0, r1 := h.runners[0], h.runners[1]
			r0.Exec(t, "PREPARE a AS SELECT * FROM t")
			r0.CheckQueryResults(t, "EXECUTE a", [][]string{{"1", "1"}})
			r0.CheckQueryResults(t, "EXECUTE a", [][]string{{"1", "1"}})
			r0.Exec(t, "ALTER TABLE t ADD COLUMN c INT AS (a+b) STORED")
			r1.Exec(t, "PREPARE b AS SELECT * FROM t")
			r1.CheckQueryResults(t, "EXECUTE b", [][]string{{"1", "1", "2"}})
		})

		// Test a schema change where the other connections are running the query in
		// parallel.
		t.Run("schemachange-parallel", func(t *testing.T) {
			t.Parallel() // SAFE FOR TESTING
			const numConns = 4

			h := makeQueryCacheTestHelper(t, numConns)
			defer h.Stop()
			var group errgroup.Group
			for connIdx := 1; connIdx < numConns; connIdx++ {
				c := h.conns[connIdx]
				connIdx := connIdx
				group.Go(func() error {
					sawChanged := false
					prepIdx := 0
					doQuery := func() error {
						// Some threads do prepare, others execute directly.
						var rows *gosql.Rows
						var err error
						ctx := context.Background()
						if connIdx%2 == 1 {
							rows, err = c.QueryContext(ctx, "SELECT * FROM t")
						} else {
							prepIdx++
							_, err = c.ExecContext(ctx, fmt.Sprintf("PREPARE a%d AS SELECT * FROM t", prepIdx))
							if err == nil {
								rows, err = c.QueryContext(ctx, fmt.Sprintf("EXECUTE a%d", prepIdx))
								if err != nil {
									// If the schema change happens in-between the PREPARE and
									// EXECUTE, we will get an error. Tolerate this error if we
									// haven't seen updated results already.
									if !sawChanged && testutils.IsError(err, "cached plan must not change result type") {
										t.Logf("thread %d hit race", connIdx)
										return nil
									}
								}
							}
						}
						if err != nil {
							return err
						}
						res, err := sqlutils.RowsToStrMatrix(rows)
						if err != nil {
							return err
						}
						if reflect.DeepEqual(res, [][]string{{"1", "1"}}) {
							if sawChanged {
								return errors.Errorf("Saw updated results, then older results")
							}
						} else if reflect.DeepEqual(res, [][]string{{"1", "1", "2"}}) {
							sawChanged = true
						} else {
							return errors.Errorf("incorrect results %v", res)
						}
						return nil
					}

					// Run the query until we see an updated result.
					for !sawChanged {
						if err := doQuery(); err != nil {
							return err
						}
					}
					t.Logf("thread %d saw changed results", connIdx)

					// Now run the query a bunch more times to make sure we keep reading the
					// updated version.
					for i := 0; i < 10; i++ {
						if err := doQuery(); err != nil {
							return err
						}
					}
					return nil
				})
			}
			r0 := h.runners[0]
			r0.Exec(t, "ALTER TABLE t ADD COLUMN c INT AS (a+b) STORED")
			if err := group.Wait(); err != nil {
				t.Fatal(err)
			}
		})

		// Verify the case where a PREPARE encounters a query cache entry that was
		// created by a direct execution (and hence has no PrepareMetadata).
		t.Run("exec-and-prepare", func(t *testing.T) {
			t.Parallel() // SAFE FOR TESTING
			h := makeQueryCacheTestHelper(t, 1 /* numConns */)
			defer h.Stop()

			r0 := h.runners[0]
			r0.Exec(t, "SELECT * FROM t") // Should miss the cache.
			h.AssertStats(t, 0 /* hits */, 1 /* misses */)

			r0.Exec(t, "SELECT * FROM t") // Should hit the cache.
			h.AssertStats(t, 1 /* hits */, 1 /* misses */)

			r0.Exec(t, "PREPARE x AS SELECT * FROM t") // Should miss the cache.
			h.AssertStats(t, 1 /* hits */, 2 /* misses */)

			r0.Exec(t, "PREPARE y AS SELECT * FROM t") // Should hit the cache.
			h.AssertStats(t, 2 /* hits */, 2 /* misses */)

			r0.CheckQueryResults(t, "EXECUTE x", [][]string{{"1", "1"}})
			r0.CheckQueryResults(t, "EXECUTE y", [][]string{{"1", "1"}})
		})

		// Verify the case where we PREPARE the same statement with different hints.
		t.Run("prepare-hints", func(t *testing.T) {
			t.Parallel() // SAFE FOR TESTING
			h := makeQueryCacheTestHelper(t, 1 /* numConns */)
			defer h.Stop()

			r0 := h.runners[0]
			r0.Exec(t, "PREPARE a1 AS SELECT pg_typeof(1 + $1)") // Should miss the cache.
			h.AssertStats(t, 0 /* hits */, 1 /* misses */)

			r0.Exec(t, "PREPARE a2 AS SELECT pg_typeof(1 + $1)") // Should hit the cache.
			h.AssertStats(t, 1 /* hits */, 1 /* misses */)

			r0.Exec(t, "PREPARE b1 (float) AS SELECT pg_typeof(1 + $1)") // Should miss the cache.
			h.AssertStats(t, 1 /* hits */, 2 /* misses */)

			r0.Exec(t, "PREPARE b2 (float) AS SELECT pg_typeof(1 + $1)") // Should hit the cache.
			h.AssertStats(t, 2 /* hits */, 2 /* misses */)

			r0.Exec(t, "PREPARE c1 (decimal) AS SELECT pg_typeof(1 + $1)") // Should miss the cache.
			h.AssertStats(t, 2 /* hits */, 3 /* misses */)

			r0.Exec(t, "PREPARE c2 (decimal) AS SELECT pg_typeof(1 + $1)") // Should hit the cache.
			h.AssertStats(t, 3 /* hits */, 3 /* misses */)

			r0.Exec(t, "PREPARE a3 AS SELECT pg_typeof(1 + $1)") // Should miss the cache.
			h.AssertStats(t, 3 /* hits */, 4 /* misses */)

			r0.Exec(t, "PREPARE b3 (float) AS SELECT pg_typeof(1 + $1)") // Should miss the cache.
			h.AssertStats(t, 3 /* hits */, 5 /* misses */)

			r0.Exec(t, "PREPARE c3 (decimal) AS SELECT pg_typeof(1 + $1)") // Should miss the cache.
			h.AssertStats(t, 3 /* hits */, 6 /* misses */)

			r0.CheckQueryResults(t, "EXECUTE a1 (1)", [][]string{{"bigint"}})
			r0.CheckQueryResults(t, "EXECUTE a2 (1)", [][]string{{"bigint"}})
			r0.CheckQueryResults(t, "EXECUTE a3 (1)", [][]string{{"bigint"}})

			r0.CheckQueryResults(t, "EXECUTE b1 (1)", [][]string{{"double precision"}})
			r0.CheckQueryResults(t, "EXECUTE b2 (1)", [][]string{{"double precision"}})
			r0.CheckQueryResults(t, "EXECUTE b3 (1)", [][]string{{"double precision"}})

			r0.CheckQueryResults(t, "EXECUTE c1 (1)", [][]string{{"numeric"}})
			r0.CheckQueryResults(t, "EXECUTE c2 (1)", [][]string{{"numeric"}})
			r0.CheckQueryResults(t, "EXECUTE c3 (1)", [][]string{{"numeric"}})
		})
	})
}

// BenchmarkQueryCache is a set of benchmarks that run queries against a server
// with the query cache on and off, with varying number of parallel clients and
// with workloads that are either cacheable or not.
//
// For microbenchmarks of the query cache data structures, see the sql/querycache
// package.
func BenchmarkQueryCache(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	workloads := []string{"small", "large"}
	methods := []string{"simple", "prepare-once", "prepare-each"}

	run := func(
		b *testing.B,
		numClients int,
		workloadIdx int,
		methodIdx int,
		cacheOn bool,
	) {
		h := makeQueryCacheTestHelper(b, numClients)
		defer h.Stop()
		r0 := h.runners[0]
		r0.Exec(b, "CREATE TABLE kv (k INT PRIMARY KEY, v INT)")

		r0.Exec(b, fmt.Sprintf("SET CLUSTER SETTING sql.query_cache.enabled = %t", cacheOn))
		var group errgroup.Group
		b.ResetTimer()
		for connIdx := 0; connIdx < numClients; connIdx++ {
			c := h.conns[connIdx]
			group.Go(func() error {
				rng, _ := randutil.NewPseudoRand()
				ctx := context.Background()
				// We use a small or large range of values depending on the
				// workload type.
				valRange := 0
				switch workloadIdx {
				case 0: // small
					valRange = 100
				case 1: // large
					valRange = 10000000
				}
				var stmt *gosql.Stmt
				if methodIdx == 1 {
					var err error
					stmt, err = c.PrepareContext(ctx, "SELECT v FROM kv WHERE k=$1")
					if err != nil {
						return err
					}
				}

				for i := 0; i < b.N/numClients; i++ {
					val := rng.Intn(valRange)
					var err error
					switch methodIdx {
					case 0: // simple
						query := fmt.Sprintf("SELECT v FROM kv WHERE k=%d", val)
						_, err = c.ExecContext(ctx, query)

					case 1: // prepare-once
						_, err = stmt.ExecContext(ctx, val)

					case 2: // prepare-every-time
						_, err = c.ExecContext(ctx, "SELECT v FROM kv WHERE k=$1", val)
					}
					if err != nil {
						return err
					}
				}
				return nil
			})
			if err := group.Wait(); err != nil {
				b.Fatal(err)
			}
		}
	}

	for workload, workloadName := range workloads {
		b.Run(workloadName, func(b *testing.B) {
			for _, clients := range []int{1, 4, 8} {
				b.Run(fmt.Sprintf("clients-%d", clients), func(b *testing.B) {
					for method, methodName := range methods {
						b.Run(methodName, func(b *testing.B) {
							for _, cache := range []bool{false, true} {
								name := "cache-off"
								if cache {
									name = "cache-on"
								}
								b.Run(name, func(b *testing.B) {
									run(b, clients, workload, method, cache)
								})
							}
						})
					}
				})
			}
		})
	}
}
