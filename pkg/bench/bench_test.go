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

package bench

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"strconv"

	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

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
	ForEachDB(b, runBenchmarkSelect1)
}

func runBenchmarkSelectWithTargetsAndFilter(
	b *testing.B, db *gosql.DB, targets, filter string, args ...interface{},
) {
	defer func() {
		if _, err := db.Exec(`DROP TABLE IF EXISTS bench.select`); err != nil {
			b.Fatal(err)
		}
	}()

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
	ForEachDB(b, runBenchmarkSelect2)
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
	ForEachDB(b, runBenchmarkSelect3)
}

func BenchmarkCount(b *testing.B) {
	ForEachDB(b, func(b *testing.B, db *gosql.DB) {
		defer func() {
			if _, err := db.Exec(`DROP TABLE IF EXISTS bench.count`); err != nil {
				b.Fatal(err)
			}
		}()

		if _, err := db.Exec(`CREATE TABLE bench.count (k INT PRIMARY KEY, v TEXT)`); err != nil {
			b.Fatal(err)
		}

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
			if _, err := db.Exec(buf.String()); err != nil {
				b.Fatal(err)
			}
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := db.Exec("SELECT COUNT(*) FROM bench.count"); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})
}

func BenchmarkSort(b *testing.B) {
	ForEachDB(b, func(b *testing.B, db *gosql.DB) {
		defer func() {
			if _, err := db.Exec(`DROP TABLE IF EXISTS bench.sort`); err != nil {
				b.Fatal(err)
			}
		}()

		if _, err := db.Exec(`CREATE TABLE bench.sort (k INT PRIMARY KEY, v INT)`); err != nil {
			b.Fatal(err)
		}

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
			if _, err := db.Exec(buf.String()); err != nil {
				b.Fatal(err)
			}
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := db.Exec("SELECT * FROM bench.sort ORDER BY v"); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})
}

// runBenchmarkInsert benchmarks inserting count rows into a table.
func runBenchmarkInsert(b *testing.B, db *gosql.DB, count int) {
	defer func() {
		if _, err := db.Exec(`DROP TABLE IF EXISTS bench.insert`); err != nil {
			b.Fatal(err)
		}
	}()

	if _, err := db.Exec(`CREATE TABLE bench.insert (k INT PRIMARY KEY)`); err != nil {
		b.Fatal(err)
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
			fmt.Fprintf(&buf, "(%d)", val)
			val++
		}
		if _, err := db.Exec(buf.String()); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

}

// runBenchmarkInsertFK benchmarks inserting count rows into a table with a
// present foreign key into another table.
func runBenchmarkInsertFK(b *testing.B, db *gosql.DB, count int) {
	for _, nFks := range []int{1, 5, 10} {
		b.Run(fmt.Sprintf("nFks=%d", nFks), func(b *testing.B) {
			defer func() {
				dropStmt := "DROP TABLE IF EXISTS bench.insert"
				for i := 0; i < nFks; i++ {
					dropStmt += fmt.Sprintf(",bench.fk%d", i)
				}
				if _, err := db.Exec(dropStmt); err != nil {
					b.Fatal(err)
				}
			}()

			for i := 0; i < nFks; i++ {
				if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE bench.fk%d (k INT PRIMARY KEY)`, i)); err != nil {
					b.Fatal(err)
				}
				if _, err := db.Exec(fmt.Sprintf(`INSERT INTO bench.fk%d VALUES(1), (2), (3)`, i)); err != nil {
					b.Fatal(err)
				}
			}

			createStmt := `CREATE TABLE bench.insert (k INT PRIMARY KEY`
			valuesStr := "(%d"
			for i := 0; i < nFks; i++ {
				createStmt += fmt.Sprintf(",fk%d INT, FOREIGN KEY(fk%d) REFERENCES bench.fk%d(k)", i, i, i)
				valuesStr += ",1"
			}
			createStmt += ")"
			valuesStr += ")"
			if _, err := db.Exec(createStmt); err != nil {
				b.Fatal(err)
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
					fmt.Fprintf(&buf, valuesStr, val)
					val++
				}
				if _, err := db.Exec(buf.String()); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()

		})
	}
}

// runBenchmarkInsertSecondaryIndex benchmarks inserting count rows into a table with a
// secondary index.
func runBenchmarkInsertSecondaryIndex(b *testing.B, db *gosql.DB, count int) {
	defer func() {
		if _, err := db.Exec(`DROP TABLE IF EXISTS bench.insert`); err != nil {
			b.Fatal(err)
		}
	}()

	if _, err := db.Exec(`CREATE TABLE bench.insert (k INT PRIMARY KEY, v INT,  INDEX(v))`); err != nil {
		b.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if _, err := db.Exec(`CREATE INDEX ON bench.insert (v)`); err != nil {
			b.Fatal(err)
		}
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
		if _, err := db.Exec(buf.String()); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func BenchmarkSQL(b *testing.B) {
	ForEachDB(b, func(b *testing.B, db *gosql.DB) {
		for _, runFn := range []func(*testing.B, *gosql.DB, int){
			runBenchmarkDelete,
			runBenchmarkInsert,
			runBenchmarkInsertDistinct,
			runBenchmarkInsertFK,
			runBenchmarkInsertSecondaryIndex,
			runBenchmarkInterleavedSelect,
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

// runBenchmarkUpdate benchmarks updating count random rows in a table.
func runBenchmarkUpdate(b *testing.B, db *gosql.DB, count int) {
	defer func() {
		if _, err := db.Exec(`DROP TABLE IF EXISTS bench.update`); err != nil {
			b.Fatal(err)
		}
	}()

	const rows = 10000
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
	defer func() {
		if _, err := db.Exec(`DROP TABLE IF EXISTS bench.upsert`); err != nil {
			b.Fatal(err)
		}
	}()

	if _, err := db.Exec(`CREATE TABLE bench.upsert (k INT PRIMARY KEY, v INT)`); err != nil {
		b.Fatal(err)
	}

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
		if _, err := db.Exec(upsertBuf.String(), key); err != nil {
			b.Fatal(err)
		}
		if _, err := db.Exec(upsertBuf.String(), key); err != nil {
			b.Fatal(err)
		}
		key += count
	}
	b.StopTimer()
}

// runBenchmarkDelete benchmarks deleting count rows from a table.
func runBenchmarkDelete(b *testing.B, db *gosql.DB, rows int) {
	defer func() {
		if _, err := db.Exec(`DROP TABLE IF EXISTS bench.delete`); err != nil {
			b.Fatal(err)
		}
	}()

	if _, err := db.Exec(`CREATE TABLE bench.delete (k INT PRIMARY KEY, v1 INT, v2 INT, v3 INT)`); err != nil {
		b.Fatal(err)
	}

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
	defer func() {
		if _, err := db.Exec(`DROP TABLE IF EXISTS bench.scan`); err != nil {
			b.Fatal(err)
		}
	}()

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
}

func BenchmarkScan(b *testing.B) {
	ForEachDB(b, func(b *testing.B, db *gosql.DB) {
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
	b *testing.B, db *gosql.DB, count1, count2 int, limit int, filter string,
) {
	defer func() {
		if _, err := db.Exec(`DROP TABLE IF EXISTS bench.scan2`); err != nil {
			b.Fatal(err)
		}
	}()

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
}

func BenchmarkScanFilter(b *testing.B) {
	const count1 = 25
	const count2 = 400
	ForEachDB(b, func(b *testing.B, db *gosql.DB) {
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

func runBenchmarkInterleavedSelect(b *testing.B, db *gosql.DB, count int) {
	defer func() {
		if _, err := db.Exec(`DROP TABLE IF EXISTS bench.interleaved_select2`); err != nil {
			b.Fatal(err)
		}
		if _, err := db.Exec(`DROP TABLE IF EXISTS bench.interleaved_select1`); err != nil {
			b.Fatal(err)
		}
	}()

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
	defer func() {
		if _, err := db.Exec(`DROP TABLE IF EXISTS bench.sort`); err != nil {
			b.Fatal(err)
		}
	}()

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
	query := fmt.Sprintf(`SELECT %sv, w, k FROM bench.sort`, dist)
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
}

func BenchmarkOrderBy(b *testing.B) {
	const count = 100000
	const limit = 10
	ForEachDB(b, func(b *testing.B, db *gosql.DB) {
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

func runBenchmarkTrackChoices(b *testing.B, db *gosql.DB, batchSize int) {
	defer func() {
		if _, err := db.Exec(`DROP TABLE IF EXISTS bench.track_choices`); err != nil {
			b.Fatal(err)
		}
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
	defer func() {
		if _, err := db.Exec(`DROP TABLE IF EXISTS bench.insert_distinct`); err != nil {
			b.Fatal(err)
		}
	}()

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

const wideTableSchema = `CREATE TABLE bench.widetable (
    f1 INT, f2 INT, f3 INT, f4 INT, f5 INT, f6 INT, f7 INT, f8 INT, f9 INT, f10 INT,
    f11 TEXT, f12 TEXT, f13 TEXT, f14 TEXT, f15 TEXT, f16 TEXT, f17 TEXT, f18 TEXT, f19 TEXT,
	f20 TEXT,
    PRIMARY KEY (f1, f2, f3)
  )`

func insertIntoWideTable(
	b *testing.B, buf bytes.Buffer, i, count, bigColumnBytes int, s *rand.Rand, db *gosql.DB,
) {
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
	if _, err := db.Exec(buf.String()); err != nil {
		b.Fatal(err)
	}
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
func runBenchmarkWideTable(b *testing.B, db *gosql.DB, count int, bigColumnBytes int) {
	defer func() {
		if _, err := db.Exec(`DROP TABLE IF EXISTS bench.widetable`); err != nil {
			b.Fatal(err)
		}
	}()

	if _, err := db.Exec(wideTableSchema); err != nil {
		b.Fatal(err)
	}

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

		if _, err := db.Exec(buf.String()); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func BenchmarkWideTable(b *testing.B) {
	const count = 10
	ForEachDB(b, func(b *testing.B, db *gosql.DB) {
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
	ForEachDB(b, func(b *testing.B, db *gosql.DB) {
		if _, err := db.Exec(wideTableSchema); err != nil {
			b.Fatal(err)
		}
		var buf bytes.Buffer
		s := rand.New(rand.NewSource(5432))
		insertIntoWideTable(b, buf, 0, 10000, 10, s, db)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := db.Exec("SELECT COUNT(*) FROM bench.widetable WHERE f4 < 10"); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPlanning runs some queries on an empty table. The purpose is to
// benchmark (and get memory allocation statistics) the planning process.
func BenchmarkPlanning(b *testing.B) {
	ForEachDB(b, func(b *testing.B, db *gosql.DB) {
		sr := sqlutils.MakeSQLRunner(db)
		sr.Exec(b, `CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT, INDEX(b), UNIQUE INDEX(c))`)

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
					sr.Exec(b, q)
				}
			})
		}
	})
}

// BenchmarkIndexJoin measure an index-join with 1000 rows.
func BenchmarkIndexJoin(b *testing.B) {
	ForEachDB(b, func(b *testing.B, db *gosql.DB) {
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

		if _, err := db.Exec(create); err != nil {
			b.Fatal(err)
		}
		if _, err := db.Exec(insert); err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := db.Exec("select * from bench.tidx where v < 1000"); err != nil {
				b.Fatal(err)
			}
		}
	})
}
