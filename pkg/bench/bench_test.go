// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bench

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func runBenchmarkSelect1(b *testing.B, db *sqlutils.SQLRunner) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := db.Query(b, `SELECT 1`)
		rows.Close()
	}
	b.StopTimer()
}

func BenchmarkSelect1(b *testing.B) {
	defer log.Scope(b).Close(b)
	ForEachDB(b, runBenchmarkSelect1)
}

func runBenchmarkSelectWithTargetsAndFilter(
	b *testing.B, db *sqlutils.SQLRunner, targets, filter string, args ...interface{},
) {
	defer func() {
		db.Exec(b, `DROP TABLE IF EXISTS bench.select`)
	}()

	db.Exec(b, `CREATE TABLE bench.select (k INT PRIMARY KEY, a INT, b INT, c INT, d INT)`)

	var buf bytes.Buffer
	buf.WriteString(`INSERT INTO bench.select VALUES `)

	// We insert all combinations of values between 1 and num for columns a, b, c. The intention is
	// to benchmark the expression parsing and query setup so we don't want to have many rows to go
	// through.
	const num = 3
	row := 0
	for i := 1; i <= num; i++ {
		for j := 1; j <= num; j++ {
			for k := 1; k <= num; k++ {
				if row > 0 {
					buf.WriteString(", ")
				}
				row++
				fmt.Fprintf(&buf, "(%d, %d, %d, %d)", row, i, j, k)
			}
		}
	}
	db.Exec(b, buf.String())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := db.Query(b, fmt.Sprintf(`SELECT %s FROM bench.select WHERE %s`, targets, filter), args...)
		rows.Close()
	}
	b.StopTimer()
}

// BenchmarkSelect2 runs a SELECT query with non-trivial expressions. The main
// purpose is to detect major regressions in query expression processing.
func BenchmarkSelect2(b *testing.B) {
	defer log.Scope(b).Close(b)
	ForEachDB(b, func(b *testing.B, db *sqlutils.SQLRunner) {
		targets := `a, b, c, a+b, a+1, (a+2)*(b+3)*(c+4)`
		filter := `(a = 1) OR ((a = 2) and (b = c)) OR (a + b = 3) OR (2*a + 4*b = 4*c)`
		runBenchmarkSelectWithTargetsAndFilter(b, db, targets, filter)
	})
}

// BenchmarkSelect3 runs a SELECT query with non-trivial expressions. The main
// purpose is to quantify regressions in numeric type processing.
func BenchmarkSelect3(b *testing.B) {
	defer log.Scope(b).Close(b)
	ForEachDB(b, func(b *testing.B, db *sqlutils.SQLRunner) {
		targets := `a/b, b/c, c != 3.3 + $1, a = 2.0, c * 9.0`
		filter := `a > 1 AND b < 4.5`
		args := []interface{}{1.0}
		runBenchmarkSelectWithTargetsAndFilter(b, db, targets, filter, args...)
	})
}

func BenchmarkCount(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ForEachDB(b, func(b *testing.B, db *sqlutils.SQLRunner) {
		defer func() {
			db.Exec(b, `DROP TABLE IF EXISTS bench.count`)
		}()

		db.Exec(b, `CREATE TABLE bench.count (k INT PRIMARY KEY, v TEXT)`)

		var buf bytes.Buffer
		val := 0
		for i := 0; i < 100; i++ {
			buf.Reset()
			buf.WriteString(`INSERT INTO bench.count VALUES `)
			for j := 0; j < 1000; j++ {
				if j > 0 {
					buf.WriteString(", ")
				}
				fmt.Fprintf(&buf, "(%d, '%s')", val, strconv.Itoa(val))
				val++
			}
			db.Exec(b, buf.String())
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			db.Exec(b, "SELECT count(*) FROM bench.count")
		}
		b.StopTimer()
	})
}

func BenchmarkCountTwoCF(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ForEachDB(b, func(b *testing.B, db *sqlutils.SQLRunner) {
		defer func() {
			db.Exec(b, `DROP TABLE IF EXISTS bench.count`)
		}()

		db.Exec(b, `CREATE TABLE bench.count (k INT PRIMARY KEY, v TEXT, FAMILY (k), FAMILY (v))`)

		var buf bytes.Buffer
		val := 0
		for i := 0; i < 100; i++ {
			buf.Reset()
			buf.WriteString(`INSERT INTO bench.count VALUES `)
			for j := 0; j < 1000; j++ {
				if j > 0 {
					buf.WriteString(", ")
				}
				fmt.Fprintf(&buf, "(%d, '%s')", val, strconv.Itoa(val))
				val++
			}
			db.Exec(b, buf.String())
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			db.Exec(b, "SELECT count(*) FROM bench.count")
		}
		b.StopTimer()
	})
}

func BenchmarkSort(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ForEachDB(b, func(b *testing.B, db *sqlutils.SQLRunner) {
		defer func() {
			db.Exec(b, `DROP TABLE IF EXISTS bench.sort`)
		}()

		db.Exec(b, `CREATE TABLE bench.sort (k INT PRIMARY KEY, v INT)`)

		var buf bytes.Buffer
		val := 0
		for i := 0; i < 100; i++ {
			buf.Reset()
			buf.WriteString(`INSERT INTO bench.sort VALUES `)
			for j := 0; j < 1000; j++ {
				if j > 0 {
					buf.WriteString(", ")
				}
				fmt.Fprintf(&buf, "(%d, %d)", val, -val)
				val++
			}
			db.Exec(b, buf.String())
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			db.Exec(b, "SELECT * FROM bench.sort ORDER BY v")
		}
		b.StopTimer()
	})
}

// BenchmarkTableResolution benchmarks table name resolution
// for a variety of different naming schemes.
func BenchmarkTableResolution(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)

	for _, createTempTables := range []bool{false, true} {
		b.Run(fmt.Sprintf("temp_schema_exists:%t", createTempTables), func(b *testing.B) {
			benchmarkCockroach(b, func(b *testing.B, db *sqlutils.SQLRunner) {
				defer func() {
					db.Exec(b, `DROP TABLE IF EXISTS bench.tbl`)
					if createTempTables {
						db.Exec(b, `DROP TABLE IF EXISTS bench.pg_temp.temp_tbl`)
					}
				}()

				db.Exec(b, `
			USE bench;
			CREATE TABLE tbl (k INT PRIMARY KEY, v INT);
		`)

				type benchCase struct {
					desc    string
					tblName string
				}
				cases := []benchCase{
					{"table", "tbl"},
					{"database.table", "bench.tbl"},
					{"database.public.table", "bench.public.tbl"},
					{"public.table", "public.tbl"},
				}
				if createTempTables {
					db.Exec(b, `
						SET experimental_enable_temp_tables=true;
						CREATE TEMP TABLE temp_tbl (k INT PRIMARY KEY);
					`)
					cases = append(cases, []benchCase{
						{"temp_table", "temp_tbl"},
						{"database.pg_temp.table", "bench.pg_temp.temp_tbl"},
						{"pg_temp.table", "pg_temp.temp_tbl"},
					}...)
				}
				for _, c := range cases {
					b.Run(c.desc, func(b *testing.B) {
						query := "SELECT * FROM " + c.tblName
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							db.Exec(b, query)
						}
						b.StopTimer()
					})
				}
			})
		})
	}
}

// runBenchmarkInsert benchmarks inserting count rows into a table.
func runBenchmarkInsert(b *testing.B, db *sqlutils.SQLRunner, count int) {
	defer func() {
		db.Exec(b, `DROP TABLE IF EXISTS bench.insert`)
	}()

	db.Exec(b, `CREATE TABLE bench.insert (k INT PRIMARY KEY)`)

	b.ResetTimer()
	var buf bytes.Buffer
	val := 0
	for i := 0; i < b.N; i++ {
		buf.Reset()
		buf.WriteString(`INSERT INTO bench.insert VALUES `)
		for j := 0; j < count; j++ {
			if j > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "(%d)", val)
			val++
		}
		db.Exec(b, buf.String())
	}
	b.StopTimer()

}

// runBenchmarkInsertFK benchmarks inserting count rows into a table with a
// present foreign key into another table.
func runBenchmarkInsertFK(b *testing.B, db *sqlutils.SQLRunner, count int) {
	for _, nFks := range []int{1, 5, 10} {
		b.Run(fmt.Sprintf("nFks=%d", nFks), func(b *testing.B) {
			defer func() {
				dropStmt := "DROP TABLE IF EXISTS bench.insert"
				for i := 0; i < nFks; i++ {
					dropStmt += fmt.Sprintf(",bench.fk%d", i)
				}
				db.Exec(b, dropStmt)
			}()

			for i := 0; i < nFks; i++ {
				db.Exec(b, fmt.Sprintf(`CREATE TABLE bench.fk%d (k INT PRIMARY KEY)`, i))
				db.Exec(b, fmt.Sprintf(`INSERT INTO bench.fk%d VALUES(1), (2), (3)`, i))
			}

			createStmt := `CREATE TABLE bench.insert (k INT PRIMARY KEY`
			valuesStr := "(%d"
			for i := 0; i < nFks; i++ {
				createStmt += fmt.Sprintf(",fk%d INT, FOREIGN KEY(fk%d) REFERENCES bench.fk%d(k)", i, i, i)
				valuesStr += ",1"
			}
			createStmt += ")"
			valuesStr += ")"
			db.Exec(b, createStmt)

			b.ResetTimer()
			var buf bytes.Buffer
			val := 0
			for i := 0; i < b.N; i++ {
				buf.Reset()
				buf.WriteString(`INSERT INTO bench.insert VALUES `)
				for j := 0; j < count; j++ {
					if j > 0 {
						buf.WriteString(", ")
					}
					fmt.Fprintf(&buf, valuesStr, val)
					val++
				}
				db.Exec(b, buf.String())
			}
			b.StopTimer()

		})
	}
}

// runBenchmarkInsertSecondaryIndex benchmarks inserting count rows into a table with a
// secondary index.
func runBenchmarkInsertSecondaryIndex(b *testing.B, db *sqlutils.SQLRunner, count int) {
	defer func() {
		db.Exec(b, `DROP TABLE IF EXISTS bench.insert`)
	}()

	db.Exec(b, `CREATE TABLE bench.insert (k INT PRIMARY KEY, v INT,  INDEX(v))`)

	for i := 0; i < 10; i++ {
		db.Exec(b, `CREATE INDEX ON bench.insert (v)`)
	}

	b.ResetTimer()
	var buf bytes.Buffer
	val := 0
	for i := 0; i < b.N; i++ {
		buf.Reset()
		buf.WriteString(`INSERT INTO bench.insert VALUES `)
		for j := 0; j < count; j++ {
			if j > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "(%d, %d)", val, val)
			val++
		}
		db.Exec(b, buf.String())
	}
	b.StopTimer()
}

func BenchmarkSQL(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ForEachDB(b, func(b *testing.B, db *sqlutils.SQLRunner) {
		for _, runFn := range []func(*testing.B, *sqlutils.SQLRunner, int){
			runBenchmarkDelete,
			runBenchmarkInsert,
			runBenchmarkInsertDistinct,
			runBenchmarkInsertFK,
			runBenchmarkInsertSecondaryIndex,
			runBenchmarkInterleavedSelect,
			runBenchmarkInterleavedFK,
			runBenchmarkTrackChoices,
			runBenchmarkUpdate,
			runBenchmarkUpsert,
		} {
			fnName := runtime.FuncForPC(reflect.ValueOf(runFn).Pointer()).Name()
			fnName = strings.TrimPrefix(fnName, "github.com/cockroachdb/cockroach/pkg/bench.runBenchmark")
			b.Run(fnName, func(b *testing.B) {
				for _, count := range []int{1, 10, 100, 1000} {
					b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
						runFn(b, db, count)
					})
				}
			})
		}
	})
}

// BenchmarkSampling measures the overhead of sampled statements. It also
// reports the memory utilization.
func BenchmarkSampling(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)

	for _, dbFn := range []func(*testing.B, BenchmarkFn){
		benchmarkCockroach,
		benchmarkMultinodeCockroach,
	} {
		dbName := runtime.FuncForPC(reflect.ValueOf(dbFn).Pointer()).Name()
		dbName = strings.TrimPrefix(dbName, "github.com/cockroachdb/cockroach/pkg/bench.benchmark")

		b.Run(dbName, func(b *testing.B) {
			dbFn(b, func(b *testing.B, db *sqlutils.SQLRunner) {
				for _, sampleRate := range []string{"1.0", "0.0"} {
					db.Exec(b, fmt.Sprintf("SET CLUSTER SETTING sql.txn_stats.sample_rate = %s", sampleRate))
					b.Run(fmt.Sprintf("sample_rate=%s", sampleRate), func(b *testing.B) {
						for _, runFn := range []func(*testing.B, *sqlutils.SQLRunner, int){
							runBenchmarkScan1,
							runBenchmarkInsert,
						} {
							fnName := runtime.FuncForPC(reflect.ValueOf(runFn).Pointer()).Name()
							fnName = strings.TrimPrefix(fnName, "github.com/cockroachdb/cockroach/pkg/bench.runBenchmark")
							b.Run(fnName, func(b *testing.B) {
								b.ReportAllocs()

								runFn(b, db, 1 /* count */)
							})
						}
					})
				}
			})
		})
	}
}

func benchmarkCockroachWithRealSpans(b *testing.B, realSpans bool, f BenchmarkFn) {
	s, db, _ := serverutils.StartServer(
		b, base.TestServerArgs{
			UseDatabase: "bench",
			Knobs: base.TestingKnobs{
				SQLExecutor: &sql.ExecutorTestingKnobs{
					ForceRealTracingSpans: realSpans,
				},
			},
		})
	defer s.Stopper().Stop(context.Background())

	if _, err := db.Exec(`CREATE DATABASE bench`); err != nil {
		b.Fatal(err)
	}

	f(b, sqlutils.MakeSQLRunner(db))
}

func benchmarkMultinodeCockroachWithRealSpans(b *testing.B, realSpans bool, f BenchmarkFn) {
	tc := testcluster.StartTestCluster(b, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "bench",
				Knobs: base.TestingKnobs{
					SQLExecutor: &sql.ExecutorTestingKnobs{
						ForceRealTracingSpans: realSpans,
					},
				},
			},
		})
	if _, err := tc.Conns[0].Exec(`CREATE DATABASE bench`); err != nil {
		b.Fatal(err)
	}
	defer tc.Stopper().Stop(context.Background())

	f(b, sqlutils.MakeRoundRobinSQLRunner(tc.Conns[0], tc.Conns[1], tc.Conns[2]))
}

// BenchmarkTracing measures the overhead of tracing. It also reports the memory
// utilization.
func BenchmarkTracing(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)

	for _, dbFn := range []func(*testing.B, bool, BenchmarkFn){
		benchmarkCockroachWithRealSpans,
		benchmarkMultinodeCockroachWithRealSpans,
	} {
		dbName := runtime.FuncForPC(reflect.ValueOf(dbFn).Pointer()).Name()
		dbName = strings.TrimPrefix(dbName, "github.com/cockroachdb/cockroach/pkg/bench.benchmark")
		dbName = strings.TrimSuffix(dbName, "WithRealSpans")
		b.Run(dbName, func(b *testing.B) {
			for _, tracingEnabled := range []bool{false, true} {
				dbFn(b, tracingEnabled, func(b *testing.B, db *sqlutils.SQLRunner) {
					// Disable statement sampling to de-noise this benchmark.
					db.Exec(b, "SET CLUSTER SETTING sql.txn_stats.sample_rate = 0.0")
					b.Run(fmt.Sprintf("tracing=%s", fmt.Sprintf("%t", tracingEnabled)[:1]), func(b *testing.B) {
						for _, runFn := range []func(*testing.B, *sqlutils.SQLRunner, int){
							runBenchmarkScan1,
							runBenchmarkInsert,
						} {
							fnName := runtime.FuncForPC(reflect.ValueOf(runFn).Pointer()).Name()
							fnName = strings.TrimPrefix(fnName, "github.com/cockroachdb/cockroach/pkg/bench.runBenchmark")
							b.Run(fnName, func(b *testing.B) {
								b.ReportAllocs()

								runFn(b, db, 1 /* count */)
							})
						}
					})
				})
			}
		})
	}
}

// runBenchmarkUpdate benchmarks updating count random rows in a table.
func runBenchmarkUpdate(b *testing.B, db *sqlutils.SQLRunner, count int) {
	defer func() {
		db.Exec(b, `DROP TABLE IF EXISTS bench.update`)
	}()

	const rows = 10000
	db.Exec(b, `CREATE TABLE bench.update (k INT PRIMARY KEY, v INT)`)

	var buf bytes.Buffer
	buf.WriteString(`INSERT INTO bench.update VALUES `)
	for i := 0; i < rows; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "(%d, %d)", i, i)
	}
	db.Exec(b, buf.String())

	s := rand.New(rand.NewSource(5432))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		buf.WriteString(`UPDATE bench.update SET v = v + 1 WHERE k IN (`)
		for j := 0; j < count; j++ {
			if j > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, `%d`, s.Intn(rows))
		}
		buf.WriteString(`)`)
		db.Exec(b, buf.String())
	}
	b.StopTimer()
}

// runBenchmarkUpsert benchmarks upserting count rows in a table.
func runBenchmarkUpsert(b *testing.B, db *sqlutils.SQLRunner, count int) {
	defer func() {
		db.Exec(b, `DROP TABLE IF EXISTS bench.upsert`)
	}()

	db.Exec(b, `CREATE TABLE bench.upsert (k INT PRIMARY KEY, v INT)`)

	// Upsert in Cockroach doesn't let you conflict the same row twice in one
	// statement (fwiw, neither does Postgres), so build one statement that
	// inserts half the values requested by `count` followed by a statement that
	// updates each of the values just inserted. This also weighs the benchmark
	// 50/50 for inserts vs updates.
	var upsertBuf bytes.Buffer
	upsertBuf.WriteString(`UPSERT INTO bench.upsert VALUES `)
	for j := 0; j < count; j += 2 {
		if j > 0 {
			upsertBuf.WriteString(`, `)
		}
		fmt.Fprintf(&upsertBuf, "($1+%d, unique_rowid())", j)
	}

	b.ResetTimer()
	key := 0
	for i := 0; i < b.N; i++ {
		db.Exec(b, upsertBuf.String(), key)
		db.Exec(b, upsertBuf.String(), key)
		key += count
	}
	b.StopTimer()
}

// runBenchmarkDelete benchmarks deleting count rows from a table.
func runBenchmarkDelete(b *testing.B, db *sqlutils.SQLRunner, rows int) {
	defer func() {
		db.Exec(b, `DROP TABLE IF EXISTS bench.delete`)
	}()

	db.Exec(b, `CREATE TABLE bench.delete (k INT PRIMARY KEY, v1 INT, v2 INT, v3 INT)`)

	b.ResetTimer()
	var buf bytes.Buffer
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		buf.Reset()
		buf.WriteString(`INSERT INTO bench.delete VALUES `)
		for j := 0; j < rows; j++ {
			if j > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "(%d, %d, %d, %d)", j, j, j, j)
		}
		db.Exec(b, buf.String())
		b.StartTimer()

		buf.Reset()
		buf.WriteString(`DELETE FROM bench.delete WHERE k IN (`)
		for j := 0; j < rows; j++ {
			if j > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, `%d`, j)
		}
		buf.WriteString(`)`)
		db.Exec(b, buf.String())
	}
	b.StopTimer()
}

func runBenchmarkScan1(b *testing.B, db *sqlutils.SQLRunner, count int) {
	runBenchmarkScan(b, db, count, 1)
}

// runBenchmarkScan benchmarks scanning a table containing count rows.
func runBenchmarkScan(b *testing.B, db *sqlutils.SQLRunner, count int, limit int) {
	defer func() {
		db.Exec(b, `DROP TABLE IF EXISTS bench.scan`)
	}()

	db.Exec(b, `CREATE TABLE bench.scan (k INT PRIMARY KEY)`)

	var buf bytes.Buffer
	buf.WriteString(`INSERT INTO bench.scan VALUES `)
	for i := 0; i < count; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "(%d)", i)
	}
	db.Exec(b, buf.String())

	query := `SELECT * FROM bench.scan`
	if limit != 0 {
		query = fmt.Sprintf(`%s LIMIT %d`, query, limit)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := db.Query(b, query)
		n := 0
		for rows.Next() {
			n++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			b.Fatal(err)
		}
		expected := count
		if limit != 0 && limit < expected {
			expected = limit
		}
		if n != expected {
			b.Fatalf("unexpected result count: %d (expected %d)", n, expected)
		}
	}
	b.StopTimer()
}

func BenchmarkScan(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ForEachDB(b, func(b *testing.B, db *sqlutils.SQLRunner) {
		for _, count := range []int{1, 10, 100, 1000, 10000} {
			b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
				for _, limit := range []int{0, 1, 10, 100} {
					b.Run(fmt.Sprintf("limit=%d", limit), func(b *testing.B) {
						runBenchmarkScan(b, db, count, limit)
					})
				}
			})
		}
	})
}

// runBenchmarkScanFilter benchmarks scanning (w/filter) from a table containing count1 * count2 rows.
func runBenchmarkScanFilter(
	b *testing.B, db *sqlutils.SQLRunner, count1, count2 int, limit int, filter string,
) {
	defer func() {
		db.Exec(b, `DROP TABLE IF EXISTS bench.scan2`)
	}()

	db.Exec(b, `CREATE TABLE bench.scan2 (a INT, b INT, PRIMARY KEY (a, b))`)

	var buf bytes.Buffer
	buf.WriteString(`INSERT INTO bench.scan2 VALUES `)
	for i := 0; i < count1; i++ {
		for j := 0; j < count2; j++ {
			if i+j > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "(%d, %d)", i, j)
		}
	}
	db.Exec(b, buf.String())

	query := fmt.Sprintf(`SELECT * FROM bench.scan2 WHERE %s`, filter)
	if limit != 0 {
		query += fmt.Sprintf(` LIMIT %d`, limit)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := db.Query(b, query)
		n := 0
		for rows.Next() {
			n++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func BenchmarkScanFilter(b *testing.B) {
	defer log.Scope(b).Close(b)
	const count1 = 25
	const count2 = 400
	ForEachDB(b, func(b *testing.B, db *sqlutils.SQLRunner) {
		b.Run(fmt.Sprintf("count1=%d", count1), func(b *testing.B) {
			b.Run(fmt.Sprintf("count2=%d", count2), func(b *testing.B) {
				for _, limit := range []int{1, 10, 50} {
					b.Run(fmt.Sprintf("limit=%d", limit), func(b *testing.B) {
						runBenchmarkScanFilter(
							b, db, count1, count2, limit,
							`a IN (1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 20, 21, 23) AND b < 10*a`,
						)
					})
				}

			})
		})
	})
}

func runBenchmarkInterleavedSelect(b *testing.B, db *sqlutils.SQLRunner, count int) {
	defer func() {
		db.Exec(b, `DROP TABLE IF EXISTS bench.interleaved_select2`)
		db.Exec(b, `DROP TABLE IF EXISTS bench.interleaved_select1`)
	}()

	db.Exec(b, `CREATE TABLE bench.interleaved_select1 (a INT PRIMARY KEY, b INT)`)
	db.Exec(b, `CREATE TABLE bench.interleaved_select2 (c INT PRIMARY KEY, d INT) INTERLEAVE IN PARENT interleaved_select1 (c)`)

	const interleaveFreq = 4

	var buf1 bytes.Buffer
	var buf2 bytes.Buffer
	buf1.WriteString(`INSERT INTO bench.interleaved_select1 VALUES `)
	buf2.WriteString(`INSERT INTO bench.interleaved_select2 VALUES `)
	for i := 0; i < count; i++ {
		if i > 0 {
			buf1.WriteString(", ")
		}
		fmt.Fprintf(&buf1, "(%d, %d)", i, i)
		if i%interleaveFreq == 0 {
			if i > 0 {
				buf2.WriteString(", ")
			}
			fmt.Fprintf(&buf2, "(%d, %d)", i, i)
		}
	}
	db.Exec(b, buf1.String())
	db.Exec(b, buf2.String())

	query := `SELECT * FROM bench.interleaved_select1 is1 INNER JOIN bench.interleaved_select2 is2 on is1.a = is2.c`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := db.Query(b, query)
		n := 0
		for rows.Next() {
			n++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			b.Fatal(err)
		}
		expected := (count + interleaveFreq - 1) / interleaveFreq
		if n != expected {
			b.Fatalf("unexpected result count: %d (expected %d)", n, expected)
		}
	}
	b.StopTimer()
}

func runBenchmarkInterleavedFK(b *testing.B, db *sqlutils.SQLRunner, count int) {
	defer func() {
		db.Exec(b, `DROP TABLE IF EXISTS bench.parent CASCADE; DROP TABLE IF EXISTS bench.child CASCADE`)
	}()

	db.Exec(b, `
CREATE TABLE bench.parent (a INT PRIMARY KEY);
INSERT INTO bench.parent VALUES(0);
CREATE TABLE bench.child
  (a INT REFERENCES bench.parent(a),
   b INT, PRIMARY KEY(a, b))
INTERLEAVE IN PARENT bench.parent (a)
`)

	b.ResetTimer()
	var buf bytes.Buffer
	val := 0
	for i := 0; i < b.N; i++ {
		buf.Reset()
		buf.WriteString(`INSERT INTO bench.child VALUES `)
		for j := 0; j < count; j++ {
			if j > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "(0, %d)", val)
			val++
		}
		db.Exec(b, buf.String())
	}
}

// runBenchmarkOrderBy benchmarks scanning a table and sorting the results.
func runBenchmarkOrderBy(
	b *testing.B, db *sqlutils.SQLRunner, count int, limit int, distinct bool,
) {
	defer func() {
		db.Exec(b, `DROP TABLE IF EXISTS bench.sort`)
	}()

	db.Exec(b, `CREATE TABLE bench.sort (k INT PRIMARY KEY, v INT, w INT)`)

	var buf bytes.Buffer
	buf.WriteString(`INSERT INTO bench.sort VALUES `)
	for i := 0; i < count; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "(%d, %d, %d)", i, i%(count*4/limit), i%2)
	}
	db.Exec(b, buf.String())

	var dist string
	if distinct {
		dist = `DISTINCT `
	}
	query := fmt.Sprintf(`SELECT %sv, w, k FROM bench.sort`, dist)
	if limit != 0 {
		query = fmt.Sprintf(`%s ORDER BY v DESC, w ASC, k DESC LIMIT %d`, query, limit)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := db.Query(b, query)
		n := 0
		for rows.Next() {
			n++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			b.Fatal(err)
		}
		expected := count
		if limit != 0 {
			expected = limit
		}
		if n != expected {
			b.Fatalf("unexpected result count: %d (expected %d)", n, expected)
		}
	}
	b.StopTimer()
}

func BenchmarkOrderBy(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	const count = 100000
	const limit = 10
	ForEachDB(b, func(b *testing.B, db *sqlutils.SQLRunner) {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			b.Run(fmt.Sprintf("limit=%d", limit), func(b *testing.B) {
				for _, distinct := range []bool{false, true} {
					b.Run(fmt.Sprintf("distinct=%t", distinct), func(b *testing.B) {
						runBenchmarkOrderBy(b, db, count, limit, distinct)
					})
				}
			})
		})
	})
}

func runBenchmarkTrackChoices(b *testing.B, db *sqlutils.SQLRunner, batchSize int) {
	defer func() {
		db.Exec(b, `DROP TABLE IF EXISTS bench.track_choices`)
	}()

	const numOptions = 10000
	// The CREATE INDEX statements are separate in order to be compatible with
	// Postgres.
	const createStmt = `
CREATE TABLE bench.track_choices (
  user_id bigint NOT NULL DEFAULT 0,
  track_id bigint NOT NULL DEFAULT 0,
  created_at timestamp NOT NULL,
  PRIMARY KEY (user_id, track_id)
);
CREATE INDEX user_created_at ON bench.track_choices (user_id, created_at);
CREATE INDEX track_created_at ON bench.track_choices (track_id, created_at);
`
	db.Exec(b, createStmt)

	b.ResetTimer()
	var buf bytes.Buffer
	for i := 0; i < b.N; i += batchSize {
		buf.Reset()
		buf.WriteString(`INSERT INTO bench.track_choices VALUES `)
		count := b.N - i
		if count > batchSize {
			count = batchSize
		}
		for j := 0; j < count; j++ {
			if j > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "(%d, %d, now())", rand.Int63(), rand.Int63n(numOptions))
		}
		db.Exec(b, buf.String())
	}
	b.StopTimer()
}

// Benchmark inserting distinct rows in batches where the min and max rows in
// separate batches overlap. This stresses the spanlatch manager implementation
// and verifies that we're allowing parallel execution of commands where possible.
func runBenchmarkInsertDistinct(b *testing.B, db *sqlutils.SQLRunner, numUsers int) {
	defer func() {
		db.Exec(b, `DROP TABLE IF EXISTS bench.insert_distinct`)
	}()

	const schema = `
CREATE TABLE bench.insert_distinct (
  articleID INT,
  userID INT,
  uniqueID INT DEFAULT unique_rowid(),
  PRIMARY KEY (articleID, userID, uniqueID))
`
	db.Exec(b, schema)

	b.ResetTimer()

	errChan := make(chan error)

	var count int64
	for i := 0; i < numUsers; i++ {
		go func(i int) {
			errChan <- func() error {
				var buf bytes.Buffer

				rnd := rand.New(rand.NewSource(int64(i)))
				// Article IDs are chosen from a zipf distribution. These values select
				// articleIDs that are mostly <10000. The parameters were experimentally
				// determined, but somewhat arbitrary.
				zipf := rand.NewZipf(rnd, 2, 10000, 100000)

				for {
					n := atomic.AddInt64(&count, 1)
					if int(n) >= b.N {
						return nil
					}

					// Insert between [1,100] articles in a batch.
					numArticles := 1 + rnd.Intn(100)
					buf.Reset()
					buf.WriteString(`INSERT INTO bench.insert_distinct VALUES `)
					for j := 0; j < numArticles; j++ {
						if j > 0 {
							buf.WriteString(", ")
						}
						fmt.Fprintf(&buf, "(%d, %d)", zipf.Uint64(), n)
					}

					if _, err := db.DB.ExecContext(context.Background(), buf.String()); err != nil {
						return err
					}
				}
			}()
		}(i)
	}

	for i := 0; i < numUsers; i++ {
		if err := <-errChan; err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
}

const wideTableSchema = `CREATE TABLE bench.widetable (
    f1 INT, f2 INT, f3 INT, f4 INT, f5 INT, f6 INT, f7 INT, f8 INT, f9 INT, f10 INT,
    f11 TEXT, f12 TEXT, f13 TEXT, f14 TEXT, f15 TEXT, f16 TEXT, f17 TEXT, f18 TEXT, f19 TEXT,
	f20 TEXT,
    PRIMARY KEY (f1, f2, f3)
  )`

func insertIntoWideTable(
	b *testing.B,
	buf bytes.Buffer,
	i, count, bigColumnBytes int,
	s *rand.Rand,
	db *sqlutils.SQLRunner,
) {
	buf.WriteString(`INSERT INTO bench.widetable VALUES `)
	for j := 0; j < count; j++ {
		if j != 0 {
			if j%3 == 0 {
				buf.WriteString(`;`)
				db.Exec(b, buf.String())
				buf.Reset()
				buf.WriteString(`INSERT INTO bench.widetable VALUES `)
			} else {
				buf.WriteString(`,`)
			}
		}
		buf.WriteString(`(`)
		for k := 0; k < 20; k++ {
			if k != 0 {
				buf.WriteString(`,`)
			}
			if k < 10 {
				fmt.Fprintf(&buf, "%d", i*count+j)
			} else if k < 19 {
				fmt.Fprintf(&buf, "'%d'", i*count+j)
			} else {
				fmt.Fprintf(&buf, "'%x'", randutil.RandBytes(s, bigColumnBytes))
			}
		}
		buf.WriteString(`)`)
	}
	buf.WriteString(`;`)
	db.Exec(b, buf.String())
	buf.Reset()
}

// runBenchmarkWideTable measures performance on a table with a large number of
// columns (20), half of which are fixed size, half of which are variable sized
// and presumed small. 1 of the presumed small columns is actually large.
//
// This benchmark tracks the tradeoff in column family allocation at table
// creation. Fewer column families mean fewer kv entries, which is faster. But
// fewer column families mean updates are less targeted, which means large
// columns in a family may be copied unnecessarily when it's updated. Perfect
// knowledge of traffic patterns can result in much better heuristics, but we
// don't have that information at table creation.
func runBenchmarkWideTable(b *testing.B, db *sqlutils.SQLRunner, count int, bigColumnBytes int) {
	defer func() {
		db.Exec(b, `DROP TABLE IF EXISTS bench.widetable`)
	}()

	db.Exec(b, wideTableSchema)

	s := rand.New(rand.NewSource(5432))

	var buf bytes.Buffer

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()

		insertIntoWideTable(b, buf, i, count, bigColumnBytes, s, db)

		// These are all updates, but ON CONFLICT DO UPDATE is (much!) faster
		// because it can do blind writes.
		buf.WriteString(`INSERT INTO bench.widetable (f1, f2, f3, f4, f5, f6, f7, f8, f9, f10) VALUES `)
		for j := 0; j < count; j++ {
			if j != 0 {
				buf.WriteString(`,`)
			}
			fmt.Fprintf(&buf, `(%d,%d,%d,`, i*count+j, i*count+j, i*count+j)
			for k := 0; k < 7; k++ {
				if k != 0 {
					buf.WriteString(`,`)
				}
				fmt.Fprintf(&buf, "%d", s.Intn(j+1))
			}
			buf.WriteString(`)`)
		}
		buf.WriteString(`ON CONFLICT (f1,f2,f3) DO UPDATE SET f4=excluded.f4,f5=excluded.f5,f6=excluded.f6,f7=excluded.f7,f8=excluded.f8,f9=excluded.f9,f10=excluded.f10;`)

		buf.WriteString(`DELETE FROM bench.widetable WHERE f1 in (`)
		for j := 0; j < count; j++ {
			if j != 0 {
				buf.WriteString(`,`)
			}
			fmt.Fprintf(&buf, "%d", j)
		}
		buf.WriteString(`);`)

		db.Exec(b, buf.String())
	}
	b.StopTimer()
}

// BenchmarkVecSkipScan benchmarks the vectorized engine's performance
// when skipping unneeded key values in the decoding process.
func BenchmarkVecSkipScan(b *testing.B) {
	defer log.Scope(b).Close(b)
	benchmarkCockroach(b, func(b *testing.B, db *sqlutils.SQLRunner) {
		create := `
CREATE TABLE bench.scan(
	x INT, y INT, z INT, 
	a INT, w INT, v INT, 
	PRIMARY KEY (x, y, z, a, w, v)
)
`
		db.Exec(b, create)
		const count = 1000
		for i := 0; i < count; i++ {
			db.Exec(
				b,
				fmt.Sprintf(
					"INSERT INTO bench.scan VALUES (%d, %d, %d, %d, %d, %d)",
					i, i, i, i, i, i,
				),
			)
		}
		b.ResetTimer()
		b.Run("Bench scan with skip", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				db.Exec(b, `SET vectorize=on; SELECT y FROM bench.scan`)
			}
		})
	})
}

func BenchmarkWideTable(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	const count = 10
	ForEachDB(b, func(b *testing.B, db *sqlutils.SQLRunner) {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			for _, bigColumnBytes := range []int{10, 100, 1000, 10000, 100000, 1000000} {
				b.Run(fmt.Sprintf("bigColumnBytes=%d", bigColumnBytes), func(b *testing.B) {
					runBenchmarkWideTable(b, db, count, bigColumnBytes)
				})
			}
		})
	})
}

func BenchmarkWideTableIgnoreColumns(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ForEachDB(b, func(b *testing.B, db *sqlutils.SQLRunner) {
		db.Exec(b, wideTableSchema)
		var buf bytes.Buffer
		s := rand.New(rand.NewSource(5432))
		insertIntoWideTable(b, buf, 0, 10000, 10, s, db)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			db.Exec(b, "SELECT count(*) FROM bench.widetable WHERE f4 < 10")
		}
	})
}

// BenchmarkPlanning runs some queries on an empty table. The purpose is to
// benchmark (and get memory allocation statistics for) the planning process.
func BenchmarkPlanning(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ForEachDB(b, func(b *testing.B, db *sqlutils.SQLRunner) {
		db.Exec(b, `CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT, INDEX(b), UNIQUE INDEX(c))`)

		queries := []string{
			`SELECT * FROM abc`,
			`SELECT * FROM abc WHERE a > 5 ORDER BY a`,
			`SELECT * FROM abc WHERE b = 5`,
			`SELECT * FROM abc WHERE b = 5 ORDER BY a`,
			`SELECT * FROM abc WHERE c = 5`,
			`SELECT * FROM abc JOIN abc AS abc2 ON abc.a = abc2.a`,
		}
		for _, q := range queries {
			b.Run(q, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					db.Exec(b, q)
				}
			})
		}
	})
}

// BenchmarkIndexJoin measure an index-join with 1000 rows.
func BenchmarkIndexJoin(b *testing.B) {
	defer log.Scope(b).Close(b)
	ForEachDB(b, func(b *testing.B, db *sqlutils.SQLRunner) {
		// The table will have an extra column not contained in the index to force a
		// join with the PK.
		create := `
		 CREATE TABLE tidx (
				 k INT NOT NULL,
				 v INT NULL,
				 extra STRING NULL,
				 CONSTRAINT "primary" PRIMARY KEY (k ASC),
				 INDEX idx (v ASC),
				 FAMILY "primary" (k, v, extra)
		 )
		`
		// We'll insert 1000 rows with random values below 1000 in the index. We'll
		// then query the index with a query that retrieves all the data (but the
		// optimizer doesn't know that).
		insert := "insert into tidx(k,v) select generate_series(1,1000), (random()*1000)::int"

		db.Exec(b, create)
		db.Exec(b, insert)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			db.Exec(b, "select * from bench.tidx where v < 1000")
		}
	})
}

func BenchmarkSortJoinAggregation(b *testing.B) {
	defer log.Scope(b).Close(b)
	ForEachDB(b, func(b *testing.B, db *sqlutils.SQLRunner) {
		tables := []struct {
			create   string
			populate string
		}{
			{
				create: `
				 CREATE TABLE abc (
					 a INT PRIMARY KEY,
					 b INT,
					 c FLOAT
				 )`,
				populate: `
				 INSERT INTO abc SELECT generate_series(1,1000), (random()*1000)::int, random()::float`,
			},
			{
				create: `
				 CREATE TABLE xyz (
					 x INT PRIMARY KEY,
					 y INT,
					 z FLOAT
				 )`,
				populate: `
				 INSERT INTO xyz SELECT generate_series(1,1000), (random()*1000)::int, random()::float`,
			},
		}

		for _, table := range tables {
			db.Exec(b, table.create)
			db.Exec(b, table.populate)
		}

		query := `
			SELECT b, count(*) FROM
				(SELECT b, avg(c) FROM (SELECT b, c FROM abc ORDER BY b) GROUP BY b)
				JOIN
				(SELECT y, sum(z) FROM (SELECT y, z FROM xyz ORDER BY y) GROUP BY y)
				ON b = y
			GROUP BY b
			ORDER BY b`

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			db.Exec(b, query)
		}
	})
}

func BenchmarkNameResolution(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ForEachDB(b, func(b *testing.B, db *sqlutils.SQLRunner) {
		db.Exec(b, `CREATE TABLE namespace (k INT PRIMARY KEY, v INT)`)
		db.Exec(b, `INSERT INTO namespace VALUES(1, 2)`)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			db.Exec(b, "SELECT * FROM namespace")
		}
		b.StopTimer()
	})
}
