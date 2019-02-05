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
// permissions and limitations under the License.

package sql

import (
	"context"
	gosql "database/sql"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/pkg/errors"
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
	h.srv.Stopper().Stop(context.TODO())
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

func TestQueryCache(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("simple", func(t *testing.T) {
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

	t.Run("parallel", func(t *testing.T) {
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
		const numConns = 4
		h := makeQueryCacheTestHelper(t, numConns)
		defer h.Stop()

		var group errgroup.Group
		for connIdx := range h.conns {
			c := h.conns[connIdx]
			group.Go(func() error {
				ctx := context.Background()
				for j := 0; j < 10; j++ {
					if _, err := c.ExecContext(
						ctx, fmt.Sprintf("PREPARE a%d AS SELECT a + $1, b + $2 FROM t", j),
					); err != nil {
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
		h := makeQueryCacheTestHelper(t, 2 /* numConns */)
		defer h.Stop()
		r0, r1 := h.runners[0], h.runners[1]
		r0.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1", "1"}})
		h.AssertStats(t, 0 /* hits */, 1 /* misses */)
		r1.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1", "1"}})
		h.AssertStats(t, 1 /* hits */, 1 /* misses */)
		r0.Exec(t, "CREATE STATISTICS s FROM t")
		h.AssertStats(t, 1 /* hits */, 1 /* misses */)
		r1.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1", "1"}})
		h.AssertStats(t, 1 /* hits */, 2 /* misses */)
	})

	// Test that a schema change triggers cache invalidation.
	t.Run("schemachange-prepare", func(t *testing.T) {
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

		r0.CheckQueryResults(t, "EXECUTE a1 (1)", [][]string{{"int"}})
		r0.CheckQueryResults(t, "EXECUTE a2 (1)", [][]string{{"int"}})
		r0.CheckQueryResults(t, "EXECUTE a3 (1)", [][]string{{"int"}})

		r0.CheckQueryResults(t, "EXECUTE b1 (1)", [][]string{{"float"}})
		r0.CheckQueryResults(t, "EXECUTE b2 (1)", [][]string{{"float"}})
		r0.CheckQueryResults(t, "EXECUTE b3 (1)", [][]string{{"float"}})

		r0.CheckQueryResults(t, "EXECUTE c1 (1)", [][]string{{"decimal"}})
		r0.CheckQueryResults(t, "EXECUTE c2 (1)", [][]string{{"decimal"}})
		r0.CheckQueryResults(t, "EXECUTE c3 (1)", [][]string{{"decimal"}})
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

	for workload, workloadName := range []string{"GoodWorkload", "BadWorkload"} {
		b.Run(workloadName, func(b *testing.B) {
			for _, clients := range []int{1, 4, 8} {
				b.Run(fmt.Sprintf("Clients%d", clients), func(b *testing.B) {
					for _, cache := range []bool{true, false} {
						name := "CacheOff"
						if cache {
							name = "CacheOn"
						}
						b.Run(name, func(b *testing.B) {
							h := makeQueryCacheTestHelper(b, clients)
							defer h.Stop()
							r0 := h.runners[0]
							r0.Exec(b, "CREATE TABLE kv (k INT PRIMARY KEY, v INT)")

							r0.Exec(b, fmt.Sprintf("SET CLUSTER SETTING sql.query_cache.enabled = %t", cache))
							var group errgroup.Group
							b.ResetTimer()
							for connIdx := 0; connIdx < clients; connIdx++ {
								c := h.conns[connIdx]
								group.Go(func() error {
									rng, _ := randutil.NewPseudoRand()
									// We use a small or large range of values depending on the
									// workload type.
									valRange := 100
									if workload == 1 {
										valRange = 10000000
									}
									for i := 0; i < b.N/clients; i++ {
										query := fmt.Sprintf("SELECT v FROM kv WHERE k=%d", rng.Intn(valRange))
										if _, err := c.ExecContext(context.Background(), query); err != nil {
											return err
										}
									}
									return nil
								})
								if err := group.Wait(); err != nil {
									b.Fatal(err)
								}
							}
						})
					}
				})
			}
		})
	}
}
