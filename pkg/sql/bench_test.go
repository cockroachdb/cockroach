// Copyright 2015 The Cockroach Authors.
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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql_test

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"golang.org/x/net/context"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

func benchmarkCockroach(b *testing.B, f func(b *testing.B, db *gosql.DB)) {
	defer tracing.Disable()()
	s, db, _ := serverutils.StartServer(
		b, base.TestServerArgs{UseDatabase: "bench"})
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS bench`); err != nil {
		b.Fatal(err)
	}

	f(b, db)
}

func benchmarkMultinodeCockroach(b *testing.B, f func(b *testing.B, db *gosql.DB)) {
	defer tracing.Disable()()
	tc := testcluster.StartTestCluster(b, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "bench",
			},
		})
	if _, err := tc.Conns[0].Exec(`CREATE DATABASE bench`); err != nil {
		b.Fatal(err)
	}
	defer tc.Stopper().Stop(context.TODO())

	f(b, tc.Conns[0])
}

func benchmarkPostgres(b *testing.B, f func(b *testing.B, db *gosql.DB)) {
	// Note: the following uses SSL. To run this, make sure your local
	// Postgres server has SSL enabled. To use Cockroach's checked-in
	// testing certificates for Postgres' SSL, first determine the
	// location of your Postgres server's configuration file:
	// ```
	// $ psql -h localhost -p 5432 -c 'SHOW config_file'
	//                config_file
	// -----------------------------------------
	//  /usr/local/var/postgres/postgresql.conf
	// (1 row)
	//```
	//
	// Now open this file and set the following values:
	// ```
	// $ grep ^ssl /usr/local/var/postgres/postgresql.conf
	// ssl = on # (change requires restart)
	// ssl_cert_file = '$GOPATH/src/github.com/cockroachdb/cockroach/pkg/security/securitytest/test_certs/node.crt' # (change requires restart)
	// ssl_key_file = '$GOPATH/src/github.com/cockroachdb/cockroach/pkg/security/securitytest/test_certs/node.key' # (change requires restart)
	// ssl_ca_file = '$GOPATH/src/github.com/cockroachdb/cockroach/pkg/security/securitytest/test_certs/ca.crt' # (change requires restart)
	// ```
	// Where `$GOPATH/src/github.com/cockroachdb/cockroach`
	// is replaced with your local Cockroach source directory.
	// Be sure to restart Postgres for this to take effect.

	const addr = "localhost:5432"
	if conn, err := net.Dial("tcp", addr); err != nil {
		b.Skipf("unable to connect to postgres server on %s: %s", addr, err)
	} else {
		conn.Close()
	}

	db, err := gosql.Open("postgres", "sslmode=require host=localhost port=5432")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE SCHEMA IF NOT EXISTS bench`); err != nil {
		b.Fatal(err)
	}

	f(b, db)
}

func benchmarkMySQL(b *testing.B, f func(b *testing.B, db *gosql.DB)) {
	const addr = "localhost:3306"
	if conn, err := net.Dial("tcp", addr); err != nil {
		b.Skipf("unable to connect to mysql server on %s: %s", addr, err)
	} else {
		conn.Close()
	}

	db, err := gosql.Open("mysql", "root@tcp(localhost:3306)/")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS bench`); err != nil {
		b.Fatal(err)
	}

	f(b, db)
}

func forEachDB(b *testing.B, fn func(*testing.B, *gosql.DB)) {
	for _, dbFn := range []func(*testing.B, func(*testing.B, *gosql.DB)){
		benchmarkCockroach,
		benchmarkMultinodeCockroach,
		benchmarkPostgres,
		benchmarkMySQL,
	} {
		dbName := runtime.FuncForPC(reflect.ValueOf(dbFn).Pointer()).Name()
		dbName = strings.TrimPrefix(dbName, "github.com/cockroachdb/cockroach/pkg/sql_test.benchmark")
		b.Run(dbName, func(b *testing.B) {
			dbFn(b, fn)
		})
	}
}

func runBenchmarkSelect1(b *testing.B, db *gosql.DB) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(`SELECT 1`)
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
	b.StopTimer()
}

func BenchmarkSelect1(b *testing.B) {
	forEachDB(b, runBenchmarkSelect1)
}

func runBenchmarkSelectWithTargetsAndFilter(
	b *testing.B, db *gosql.DB, targets, filter string, args ...interface{},
) {
	if _, err := db.Exec(`DROP TABLE IF EXISTS bench.select`); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE bench.select (k INT PRIMARY KEY, a INT, b INT, c INT, d INT)`); err != nil {
		b.Fatal(err)
	}

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
	if _, err := db.Exec(buf.String()); err != nil {
		b.Fatal(err)
	}

	defer func() {
		if _, err := db.Exec(`DROP TABLE bench.select`); err != nil {
			b.Fatal(err)
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(fmt.Sprintf(`SELECT %s FROM bench.select WHERE %s`, targets, filter), args...)
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
	b.StopTimer()
}

// runBenchmarkSelect2 runs a SELECT query with non-trivial expressions. The main purpose is to
// detect major regressions in query expression processing.
func runBenchmarkSelect2(b *testing.B, db *gosql.DB) {
	targets := `a, b, c, a+b, a+1, (a+2)*(b+3)*(c+4)`
	filter := `(a = 1) OR ((a = 2) and (b = c)) OR (a + b = 3) OR (2*a + 4*b = 4*c)`
	runBenchmarkSelectWithTargetsAndFilter(b, db, targets, filter)
}

func BenchmarkSelect2(b *testing.B) {
	forEachDB(b, runBenchmarkSelect2)
}

// runBenchmarkSelect3 runs a SELECT query with non-trivial expressions. The main purpose is to
// quantify regressions in numeric type processing.
func runBenchmarkSelect3(b *testing.B, db *gosql.DB) {
	targets := `a/b, b/c, c != 3.3 + $1, a = 2.0, c * 9.0`
	filter := `a > 1 AND b < 4.5`
	args := []interface{}{1.0}
	runBenchmarkSelectWithTargetsAndFilter(b, db, targets, filter, args...)
}

func BenchmarkSelect3(b *testing.B) {
	forEachDB(b, runBenchmarkSelect3)
}

// runBenchmarkInsert benchmarks inserting count rows into a table.
func runBenchmarkInsert(b *testing.B, db *gosql.DB, count int) {
	if _, err := db.Exec(`DROP TABLE IF EXISTS bench.insert`); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE bench.insert (k INT PRIMARY KEY)`); err != nil {
		b.Fatal(err)
	}
	defer func() {
		if _, err := db.Exec(`DROP TABLE bench.insert`); err != nil {
			b.Fatal(err)
		}
	}()

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
		if _, err := db.Exec(buf.String()); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

}

func BenchmarkSQL(b *testing.B) {
	for _, runFn := range []func(*testing.B, *gosql.DB, int){
		runBenchmarkDelete,
		runBenchmarkInsert,
		runBenchmarkInsertDistinct,
		runBenchmarkInterleavedSelect,
		runBenchmarkTrackChoices,
		runBenchmarkTrackChoices,
		runBenchmarkUpdate,
		runBenchmarkUpsert,
	} {
		fnName := runtime.FuncForPC(reflect.ValueOf(runFn).Pointer()).Name()
		fnName = strings.TrimPrefix(fnName, "github.com/cockroachdb/cockroach/pkg/sql_test.runBenchmark")
		for _, count := range []int{1, 10, 100, 1000} {
			b.Run(fmt.Sprintf("%s%d", fnName, count), func(b *testing.B) {
				forEachDB(b, func(b *testing.B, db *gosql.DB) {
					runFn(b, db, count)
				})
			})
		}
	}
}

// runBenchmarkUpdate benchmarks updating count random rows in a table.
func runBenchmarkUpdate(b *testing.B, db *gosql.DB, count int) {
	rows := 10000
	if _, err := db.Exec(`DROP TABLE IF EXISTS bench.update`); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE bench.update (k INT PRIMARY KEY, v INT)`); err != nil {
		b.Fatal(err)
	}

	var buf bytes.Buffer
	buf.WriteString(`INSERT INTO bench.update VALUES `)
	for i := 0; i < rows; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "(%d, %d)", i, i)
	}
	if _, err := db.Exec(buf.String()); err != nil {
		b.Fatal(err)
	}

	defer func() {
		if _, err := db.Exec(`DROP TABLE bench.update`); err != nil {
			b.Fatal(err)
		}
	}()

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
		if _, err := db.Exec(buf.String()); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// runBenchmarkUpsert benchmarks upserting count rows in a table.
func runBenchmarkUpsert(b *testing.B, db *gosql.DB, count int) {
	if _, err := db.Exec(`DROP TABLE IF EXISTS bench.upsert`); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE bench.upsert (k INT PRIMARY KEY, v INT)`); err != nil {
		b.Fatal(err)
	}
	defer func() {
		if _, err := db.Exec(`DROP TABLE bench.upsert`); err != nil {
			b.Fatal(err)
		}
	}()

	s := rand.New(rand.NewSource(5432))

	b.ResetTimer()
	// Upsert in Cockroach doesn't let you conflict the same row twice in one
	// statement (fwiw, neither does Postgres), so build one statement that
	// inserts half the values requested by `count` followed by a statement that
	// updates each of the values just inserted. This also weighs the benchmark
	// 50/50 for inserts vs updates.
	var insertBuf bytes.Buffer
	var updateBuf bytes.Buffer
	key := 0
	for i := 0; i < b.N; i++ {
		// TODO(dan): Once the long form is implemented, use it here so we can have
		// Postgres benchmarks.
		insertBuf.Reset()
		insertBuf.WriteString(`UPSERT INTO bench.upsert VALUES `)
		updateBuf.Reset()
		updateBuf.WriteString(`UPSERT INTO bench.upsert VALUES `)
		j := 0
		for ; j < count; j += 2 {
			if j > 0 {
				insertBuf.WriteString(`, `)
				updateBuf.WriteString(`, `)
			}
			fmt.Fprintf(&insertBuf, "(%d, %d)", key, s.Int())
			fmt.Fprintf(&updateBuf, "(%d, %d)", key, s.Int())
			key++
		}
		insertBuf.WriteString(`; `)
		if _, err := updateBuf.WriteTo(&insertBuf); err != nil {
			b.Fatal(err)
		}
		if _, err := db.Exec(insertBuf.String()); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// runBenchmarkDelete benchmarks deleting count rows from a table.
func runBenchmarkDelete(b *testing.B, db *gosql.DB, rows int) {
	if _, err := db.Exec(`DROP TABLE IF EXISTS bench.delete`); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE bench.delete (k INT PRIMARY KEY, v1 INT, v2 INT, v3 INT)`); err != nil {
		b.Fatal(err)
	}
	defer func() {
		if _, err := db.Exec(`DROP TABLE bench.delete`); err != nil {
			b.Fatal(err)
		}
	}()

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
		if _, err := db.Exec(buf.String()); err != nil {
			b.Fatal(err)
		}
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
		if _, err := db.Exec(buf.String()); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// runBenchmarkScan benchmarks scanning a table containing count rows.
func runBenchmarkScan(b *testing.B, db *gosql.DB, count int, limit int) {
	if _, err := db.Exec(`DROP TABLE IF EXISTS bench.scan`); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE bench.scan (k INT PRIMARY KEY)`); err != nil {
		b.Fatal(err)
	}

	var buf bytes.Buffer
	buf.WriteString(`INSERT INTO bench.scan VALUES `)
	for i := 0; i < count; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "(%d)", i)
	}
	if _, err := db.Exec(buf.String()); err != nil {
		b.Fatal(err)
	}

	query := `SELECT * FROM bench.scan`
	if limit != 0 {
		query = fmt.Sprintf(`%s LIMIT %d`, query, limit)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(query)
		if err != nil {
			b.Fatal(err)
		}
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

	if _, err := db.Exec(`DROP TABLE bench.scan`); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkScan(b *testing.B) {
	for _, count := range []int{1, 10, 100, 1000, 10000} {
		for _, limit := range []int{0, 1, 10, 100} {
			name := fmt.Sprintf("%dLimit%d", count, limit)
			b.Run(name, func(b *testing.B) {
				forEachDB(b, func(b *testing.B, db *gosql.DB) {
					runBenchmarkScan(b, db, count, limit)
				})
			})
		}
	}
}

// runBenchmarkScanFilter benchmarks scanning (w/filter) from a table containing count1 * count2 rows.
func runBenchmarkScanFilter(
	b *testing.B, db *gosql.DB, count1, count2 int, limit int, filter string,
) {
	if _, err := db.Exec(`DROP TABLE IF EXISTS bench.scan2`); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE bench.scan2 (a INT, b INT, PRIMARY KEY (a, b))`); err != nil {
		b.Fatal(err)
	}

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
	if _, err := db.Exec(buf.String()); err != nil {
		b.Fatal(err)
	}

	query := fmt.Sprintf(`SELECT * FROM bench.scan2 WHERE %s`, filter)
	if limit != 0 {
		query += fmt.Sprintf(` LIMIT %d`, limit)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(query)
		if err != nil {
			b.Fatal(err)
		}
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

	if _, err := db.Exec(`DROP TABLE bench.scan2`); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkScanFilter(b *testing.B) {
	const count1 = 25
	const count2 = 400
	for _, limit := range []int{1, 10, 50} {
		name := fmt.Sprintf("%dLimit%d", count1*count2, limit)
		b.Run(name, func(b *testing.B) {
			forEachDB(b, func(b *testing.B, db *gosql.DB) {
				runBenchmarkScanFilter(
					b, db, 25, 400, limit,
					`a IN (1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 20, 21, 23) AND b < 10*a`,
				)
			})
		})
	}
}

func runBenchmarkInterleavedSelect(b *testing.B, db *gosql.DB, count int) {
	if _, err := db.Exec(`DROP TABLE IF EXISTS bench.interleaved_select1`); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE IF EXISTS bench.interleaved_select2`); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE bench.interleaved_select1 (a INT PRIMARY KEY, b INT)`); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE bench.interleaved_select2 (c INT PRIMARY KEY, d INT) INTERLEAVE IN PARENT interleaved_select1 (c)`); err != nil {
		b.Fatal(err)
	}

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
	if _, err := db.Exec(buf1.String()); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(buf2.String()); err != nil {
		b.Fatal(err)
	}

	query := `SELECT * FROM bench.interleaved_select1 is1 INNER JOIN bench.interleaved_select2 is2 on is1.a = is2.c`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(query)
		if err != nil {
			b.Fatal(err)
		}
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

// runBenchmarkOrderBy benchmarks scanning a table and sorting the results.
func runBenchmarkOrderBy(b *testing.B, db *gosql.DB, count int, limit int, distinct bool) {
	if _, err := db.Exec(`DROP TABLE IF EXISTS bench.sort`); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE bench.sort (k INT PRIMARY KEY, v INT, w INT)`); err != nil {
		b.Fatal(err)
	}

	var buf bytes.Buffer
	buf.WriteString(`INSERT INTO bench.sort VALUES `)
	for i := 0; i < count; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "(%d, %d, %d)", i, i%(count*4/limit), i%2)
	}
	if _, err := db.Exec(buf.String()); err != nil {
		b.Fatal(err)
	}

	var dist string
	if distinct {
		dist = `DISTINCT `
	}
	query := fmt.Sprintf(`SELECT %sv FROM bench.sort`, dist)
	if limit != 0 {
		query = fmt.Sprintf(`%s ORDER BY v DESC, w ASC, k DESC LIMIT %d`, query, limit)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(query)
		if err != nil {
			b.Fatal(err)
		}
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

	if _, err := db.Exec(`DROP TABLE bench.sort`); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkOrderBy(b *testing.B) {
	const count = 100000
	const limit = 10
	for _, distinct := range []bool{false, true} {
		name := fmt.Sprintf("%dLimit%d", count, limit)
		if distinct {
			name += "Distinct"
		}
		b.Run(name, func(b *testing.B) {
			forEachDB(b, func(b *testing.B, db *gosql.DB) {
				runBenchmarkOrderBy(b, db, count, limit, distinct)
			})
		})
	}
}

func runBenchmarkTrackChoices(b *testing.B, db *gosql.DB, batchSize int) {
	const numOptions = 10000

	if _, err := db.Exec(`DROP TABLE IF EXISTS bench.track_choices`); err != nil {
		b.Fatal(err)
	}
	// The CREATE INDEX statements are separate in order to be compatible with
	// Postgres.
	const createStmt = `
CREATE TABLE IF NOT EXISTS bench.track_choices (
  user_id bigint NOT NULL DEFAULT 0,
  track_id bigint NOT NULL DEFAULT 0,
  created_at timestamp NOT NULL,
  PRIMARY KEY (user_id, track_id)
);
CREATE INDEX user_created_at ON bench.track_choices (user_id, created_at);
CREATE INDEX track_created_at ON bench.track_choices (track_id, created_at);
`
	if _, err := db.Exec(createStmt); err != nil {
		b.Fatal(err)
	}

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
			fmt.Fprintf(&buf, "(%d, %d, NOW())", rand.Int63(), rand.Int63n(numOptions))
		}
		if _, err := db.Exec(buf.String()); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// Benchmark inserting distinct rows in batches where the min and max rows in
// separate batches overlap. This stresses the command queue implementation and
// verifies that we're allowing parallel execution of commands where possible.
func runBenchmarkInsertDistinct(b *testing.B, db *gosql.DB, numUsers int) {
	if _, err := db.Exec(`DROP TABLE IF EXISTS bench.insert_distinct`); err != nil {
		b.Fatal(err)
	}
	const schema = `
CREATE TABLE bench.insert_distinct (
  articleID INT,
  userID INT,
  uniqueID INT DEFAULT unique_rowid(),
  PRIMARY KEY (articleID, userID, uniqueID))
`
	if _, err := db.Exec(schema); err != nil {
		b.Fatal(err)
	}

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

					if _, err := db.Exec(buf.String()); err != nil {
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
func runBenchmarkWideTable(b *testing.B, db *gosql.DB, count int, bigColumnBytes int) {
	if _, err := db.Exec(`DROP TABLE IF EXISTS bench.widetable`); err != nil {
		b.Fatal(err)
	}
	const schema = `CREATE TABLE bench.widetable (
    f1 INT, f2 INT, f3 INT, f4 INT, f5 INT, f6 INT, f7 INT, f8 INT, f9 INT, f10 INT,
    f11 TEXT, f12 TEXT, f13 TEXT, f14 TEXT, f15 TEXT, f16 TEXT, f17 TEXT, f18 TEXT, f19 TEXT,
	f20 TEXT,
    PRIMARY KEY (f1, f2, f3)
  )`
	if _, err := db.Exec(schema); err != nil {
		b.Fatal(err)
	}

	defer func() {
		if _, err := db.Exec(`DROP TABLE bench.widetable`); err != nil {
			b.Fatal(err)
		}
	}()

	s := rand.New(rand.NewSource(5432))

	var buf bytes.Buffer

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()

		buf.WriteString(`INSERT INTO bench.widetable VALUES `)
		for j := 0; j < count; j++ {
			if j != 0 {
				if j%3 == 0 {
					buf.WriteString(`;`)
					if _, err := db.Exec(buf.String()); err != nil {
						b.Fatal(err)
					}
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

		if _, err := db.Exec(buf.String()); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func BenchmarkWideTable(b *testing.B) {
	const count = 10
	for _, bigColumnBytes := range []int{10, 100, 1000, 10000, 100000, 1000000} {
		b.Run(strconv.Itoa(bigColumnBytes), func(b *testing.B) {
			forEachDB(b, func(b *testing.B, db *gosql.DB) {
				runBenchmarkWideTable(b, db, count, bigColumnBytes)
			})
		})
	}
}
