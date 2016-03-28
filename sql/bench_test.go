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
	"database/sql"
	"fmt"
	"math/rand"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/util/tracing"
	_ "github.com/cockroachdb/pq"
)

func benchmarkCockroach(b *testing.B, f func(b *testing.B, db *sql.DB)) {
	defer tracing.Disable()()
	s := server.StartTestServer(b)
	defer s.Stop()

	pgURL, cleanupFn := sqlutils.PGUrl(b, s, security.RootUser, "benchmarkCockroach")
	pgURL.Path = "bench"
	defer cleanupFn()

	db, err := sql.Open("postgres", pgURL.String())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS bench`); err != nil {
		b.Fatal(err)
	}

	f(b, db)
}

func benchmarkPostgres(b *testing.B, f func(b *testing.B, db *sql.DB)) {
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
	// $ cat /usr/local/var/postgres/postgresql.conf | grep ssl
	// ssl = on                            # (change requires restart)
	// ssl_cert_file = '/Users/tamird/src/go/src/github.com/cockroachdb/cockroach/resource/test_certs/node.server.crt'             # (change requires restart)
	// ssl_key_file = '/Users/tamird/src/go/src/github.com/cockroachdb/cockroach/resource/test_certs/node.server.key'              # (change requires restart)
	// ssl_ca_file = '/Users/tamird/src/go/src/github.com/cockroachdb/cockroach/resource/test_certs/ca.crt'                        # (change requires restart)
	// ```
	// Where `/Users/tamird/src/go/src/github.com/cockroachdb/cockroach`
	// is replaced with your local Cockroach source directory.
	// Be sure to restart Postgres for this to take effect.

	db, err := sql.Open("postgres", "sslmode=require host=localhost port=5432")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE SCHEMA IF NOT EXISTS bench`); err != nil {
		b.Fatal(err)
	}

	f(b, db)
}

func benchmarkMySQL(b *testing.B, f func(b *testing.B, db *sql.DB)) {
	db, err := sql.Open("mysql", "root@tcp(localhost:3306)/")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS bench`); err != nil {
		b.Fatal(err)
	}

	f(b, db)
}

func runBenchmarkSelect1(b *testing.B, db *sql.DB) {
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

func BenchmarkSelect1_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkSelect1)
}

func BenchmarkSelect1_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkSelect1)
}

func BenchmarkSelect1_MySQL(b *testing.B) {
	benchmarkMySQL(b, runBenchmarkSelect1)
}

// runBenchmarkSelect2 Runs a SELECT query with non-trivial expressions. The main purpose is to
// detect major regressions in query expression processing.
func runBenchmarkSelect2(b *testing.B, db *sql.DB) {
	if _, err := db.Exec(`DROP TABLE IF EXISTS bench.select`); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE bench.select (k INT PRIMARY KEY, a INT, b INT, c INT, d INT)`); err != nil {
		b.Fatal(err)
	}

	var buf bytes.Buffer
	buf.WriteString(`INSERT INTO bench.select VALUES `)

	// We insert all combinations of values between 1 and num for columns a, b, c. The intention is
	// to benchrmark the expression parsing and query setup so we don't want to have many rows to go
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
		targets := `a, b, c, a+b, a+1, (a+2)*(b+3)*(c+4)`
		filter := `(a = 1) OR ((a = 2) and (b = c)) OR (a + b = 3) OR (2*a + 4*b = 4*c)`
		rows, err := db.Query(fmt.Sprintf(`SELECT %s FROM bench.select WHERE %s`, targets, filter))
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
	b.StopTimer()
}

func BenchmarkSelect2_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkSelect2)
}

func BenchmarkSelect2_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkSelect2)
}

func BenchmarkSelect2_MySQL(b *testing.B) {
	benchmarkMySQL(b, runBenchmarkSelect2)
}

// runBenchmarkInsert benchmarks inserting count rows into a table.
func runBenchmarkInsert(b *testing.B, db *sql.DB, count int) {
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

func runBenchmarkInsert1(b *testing.B, db *sql.DB) {
	runBenchmarkInsert(b, db, 1)
}

func runBenchmarkInsert10(b *testing.B, db *sql.DB) {
	runBenchmarkInsert(b, db, 10)
}

func runBenchmarkInsert100(b *testing.B, db *sql.DB) {
	runBenchmarkInsert(b, db, 100)
}

func runBenchmarkInsert1000(b *testing.B, db *sql.DB) {
	runBenchmarkInsert(b, db, 1000)
}

func BenchmarkInsert1_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkInsert1)
}

func BenchmarkInsert1_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkInsert1)
}

func BenchmarkInsert10_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkInsert10)
}

func BenchmarkInsert10_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkInsert10)
}

func BenchmarkInsert100_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkInsert100)
}

func BenchmarkInsert100_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkInsert100)
}

func BenchmarkInsert1000_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkInsert1000)
}

func BenchmarkInsert1000_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkInsert1000)
}

// runBenchmarkUpdate benchmarks updating count random rows in a table.
func runBenchmarkUpdate(b *testing.B, db *sql.DB, count int) {
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

func runBenchmarkUpdate1(b *testing.B, db *sql.DB) {
	runBenchmarkUpdate(b, db, 1)
}

func runBenchmarkUpdate10(b *testing.B, db *sql.DB) {
	runBenchmarkUpdate(b, db, 10)
}

func runBenchmarkUpdate100(b *testing.B, db *sql.DB) {
	runBenchmarkUpdate(b, db, 100)
}

func runBenchmarkUpdate1000(b *testing.B, db *sql.DB) {
	runBenchmarkUpdate(b, db, 1000)
}

func BenchmarkUpdate1_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkUpdate1)
}

func BenchmarkUpdate1_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkUpdate1)
}

func BenchmarkUpdate10_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkUpdate10)
}

func BenchmarkUpdate10_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkUpdate10)
}

func BenchmarkUpdate100_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkUpdate100)
}

func BenchmarkUpdate100_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkUpdate100)
}

func BenchmarkUpdate1000_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkUpdate1000)
}

func BenchmarkUpdate1000_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkUpdate1000)
}

// runBenchmarkDelete benchmarks deleting count rows from a table.
func runBenchmarkDelete(b *testing.B, db *sql.DB, rows int) {
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

func runBenchmarkDelete1(b *testing.B, db *sql.DB) {
	runBenchmarkDelete(b, db, 1)
}

func runBenchmarkDelete10(b *testing.B, db *sql.DB) {
	runBenchmarkDelete(b, db, 10)
}

func runBenchmarkDelete100(b *testing.B, db *sql.DB) {
	runBenchmarkDelete(b, db, 100)
}

func runBenchmarkDelete1000(b *testing.B, db *sql.DB) {
	runBenchmarkDelete(b, db, 1000)
}

func BenchmarkDelete1_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkDelete1)
}

func BenchmarkDelete1_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkDelete1)
}

func BenchmarkDelete10_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkDelete10)
}

func BenchmarkDelete10_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkDelete10)
}

func BenchmarkDelete100_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkDelete100)
}

func BenchmarkDelete100_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkDelete100)
}

func BenchmarkDelete1000_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkDelete1000)
}

func BenchmarkDelete1000_Postgres(b *testing.B) {
	benchmarkPostgres(b, runBenchmarkDelete1000)
}

// runBenchmarkScan benchmarks scanning a table containing count rows.
func runBenchmarkScan(b *testing.B, db *sql.DB, count int, limit int) {
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
		if limit != 0 {
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

func BenchmarkScan1_Cockroach(b *testing.B) {
	benchmarkCockroach(b, func(b *testing.B, db *sql.DB) { runBenchmarkScan(b, db, 1, 0) })
}

func BenchmarkScan1_Postgres(b *testing.B) {
	benchmarkPostgres(b, func(b *testing.B, db *sql.DB) { runBenchmarkScan(b, db, 1, 0) })
}

func BenchmarkScan10_Cockroach(b *testing.B) {
	benchmarkCockroach(b, func(b *testing.B, db *sql.DB) { runBenchmarkScan(b, db, 10, 0) })
}

func BenchmarkScan10_Postgres(b *testing.B) {
	benchmarkPostgres(b, func(b *testing.B, db *sql.DB) { runBenchmarkScan(b, db, 10, 0) })
}

func BenchmarkScan100_Cockroach(b *testing.B) {
	benchmarkCockroach(b, func(b *testing.B, db *sql.DB) { runBenchmarkScan(b, db, 100, 0) })
}

func BenchmarkScan100_Postgres(b *testing.B) {
	benchmarkPostgres(b, func(b *testing.B, db *sql.DB) { runBenchmarkScan(b, db, 100, 0) })
}

func BenchmarkScan1000_Cockroach(b *testing.B) {
	benchmarkCockroach(b, func(b *testing.B, db *sql.DB) { runBenchmarkScan(b, db, 1000, 0) })
}

func BenchmarkScan1000_Postgres(b *testing.B) {
	benchmarkPostgres(b, func(b *testing.B, db *sql.DB) { runBenchmarkScan(b, db, 1000, 0) })
}

func BenchmarkScan10000_Cockroach(b *testing.B) {
	benchmarkCockroach(b, func(b *testing.B, db *sql.DB) { runBenchmarkScan(b, db, 10000, 0) })
}

func BenchmarkScan10000_Postgres(b *testing.B) {
	benchmarkPostgres(b, func(b *testing.B, db *sql.DB) { runBenchmarkScan(b, db, 10000, 0) })
}

func BenchmarkScan1000Limit1_Cockroach(b *testing.B) {
	benchmarkCockroach(b, func(b *testing.B, db *sql.DB) { runBenchmarkScan(b, db, 1000, 1) })
}

func BenchmarkScan1000Limit1_Postgres(b *testing.B) {
	benchmarkPostgres(b, func(b *testing.B, db *sql.DB) { runBenchmarkScan(b, db, 1000, 1) })
}

func BenchmarkScan1000Limit10_Cockroach(b *testing.B) {
	benchmarkCockroach(b, func(b *testing.B, db *sql.DB) { runBenchmarkScan(b, db, 1000, 10) })
}

func BenchmarkScan1000Limit10_Postgres(b *testing.B) {
	benchmarkPostgres(b, func(b *testing.B, db *sql.DB) { runBenchmarkScan(b, db, 1000, 10) })
}

func BenchmarkScan1000Limit100_Cockroach(b *testing.B) {
	benchmarkCockroach(b, func(b *testing.B, db *sql.DB) { runBenchmarkScan(b, db, 1000, 100) })
}

func BenchmarkScan1000Limit100_Postgres(b *testing.B) {
	benchmarkPostgres(b, func(b *testing.B, db *sql.DB) { runBenchmarkScan(b, db, 1000, 100) })
}

// runBenchmarkScanFilter benchmarks scanning (w/filter) from a table containing count1 * count2 rows.
func runBenchmarkScanFilter(b *testing.B, db *sql.DB, count1, count2 int, limit int, filter string) {
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

func filterLimitBenchFn(limit int) func(*testing.B, *sql.DB) {
	return func(b *testing.B, db *sql.DB) {
		runBenchmarkScanFilter(b, db, 25, 400, limit,
			`a IN (1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 20, 21, 23) AND b < 10*a`)
	}
}

func BenchmarkScan10000FilterLimit1_Cockroach(b *testing.B) {
	benchmarkCockroach(b, filterLimitBenchFn(1))
}

func BenchmarkScan10000FilterLimit1_Postgres(b *testing.B) {
	benchmarkPostgres(b, filterLimitBenchFn(1))
}

func BenchmarkScan10000FilterLimit10_Cockroach(b *testing.B) {
	benchmarkCockroach(b, filterLimitBenchFn(10))
}

func BenchmarkScan10000FilterLimit10_Postgres(b *testing.B) {
	benchmarkPostgres(b, filterLimitBenchFn(10))
}

func BenchmarkScan10000FilterLimit50_Cockroach(b *testing.B) {
	benchmarkCockroach(b, filterLimitBenchFn(50))
}

func BenchmarkScan10000FilterLimit50_Postgres(b *testing.B) {
	benchmarkPostgres(b, filterLimitBenchFn(50))
}

// runBenchmarkOrderBy benchmarks scanning a table and sorting the results.
func runBenchmarkOrderBy(b *testing.B, db *sql.DB, count int, limit int, distinct bool) {
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

func BenchmarkSort100000Limit10_Cockroach(b *testing.B) {
	benchmarkCockroach(b, func(b *testing.B, db *sql.DB) { runBenchmarkOrderBy(b, db, 100000, 10, false) })
}

func BenchmarkSort100000Limit10_Postgres(b *testing.B) {
	benchmarkPostgres(b, func(b *testing.B, db *sql.DB) { runBenchmarkOrderBy(b, db, 100000, 10, false) })
}

func BenchmarkSort100000Limit10Distinct_Cockroach(b *testing.B) {
	benchmarkCockroach(b, func(b *testing.B, db *sql.DB) { runBenchmarkOrderBy(b, db, 100000, 10, true) })
}

func BenchmarkSort100000Limit10Distinct_Postgres(b *testing.B) {
	benchmarkPostgres(b, func(b *testing.B, db *sql.DB) { runBenchmarkOrderBy(b, db, 100000, 10, true) })
}
