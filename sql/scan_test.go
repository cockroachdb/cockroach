// Copyright 2016 The Cockroach Authors.
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
// Author: Radu Berinde (radu@cockroachlabs.com)

package sql_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	csql "github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// genRanges generates ordered, non-overlaping ranges with values in [0 and valRange).
func genRanges(num int, valRange int) [][2]int {
	// Generate num _distinct_ values. We do this by generating a partial permutation.
	perm := make([]int, valRange)
	for i := 0; i < valRange; i++ {
		perm[i] = i
	}
	for i := 0; i < num; i++ {
		// Choose a random element starting at i.
		pos := rand.Int() % (num - i)
		perm[i], perm[i+pos] = perm[i+pos], perm[i]
	}
	perm = perm[:num]
	// Sort the values. These distinct values will be the starts of our ranges.
	sort.Ints(perm)
	res := make([][2]int, num)
	for i := 0; i < num; i++ {
		res[i][0] = perm[i]
		next := valRange
		if i < num-1 {
			next = perm[i+1]
		}
		// Pick a random end in the range [perm[i], next).
		res[i][1] = perm[i] + rand.Int()%(next-perm[i])
	}
	return res
}

func testScanBatchQuery(t *testing.T, db *sql.DB, numRanges int, numRows int) {
	ranges := genRanges(numRanges, numRows)
	expected := []int(nil)
	var buf bytes.Buffer
	buf.WriteString(`SELECT k FROM test.scan`)
	for i, r := range ranges {
		if i == 0 {
			buf.WriteString(" WHERE ")
		} else {
			buf.WriteString(" OR ")
		}
		buf.WriteString(fmt.Sprintf(`(k >= %d AND k <= %d)`, r[0], r[1]))
		for j := r[0]; j <= r[1]; j++ {
			expected = append(expected, j)
		}
	}
	if len(ranges) == 0 {
		for j := 0; j < numRows; j++ {
			expected = append(expected, j)
		}
	}
	rows, err := db.Query(buf.String())
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rows.Next() {
		if n >= len(expected) {
			t.Fatalf("too many rows (expected %d)", len(expected))
		}
		var val int
		err = rows.Scan(&val)
		if err != nil {
			t.Fatal(err)
		}
		if val != expected[n] {
			t.Errorf("row %d: invalid value %d (expected %d)", n, val, expected[n])
		}
		n++
	}
	if n != len(expected) {
		t.Fatalf("too few rows %d (expected %d)", n, len(expected))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

// TestScanBatches tests the scan-in-batches code by artificially setting the batch size to
// particular values and performing queries.
func TestScanBatches(t *testing.T) {
	defer leaktest.AfterTest(t)

	s := server.StartTestServer(t)
	defer s.Stop()

	pgURL, cleanupFn := sqlutils.PGUrl(t, s, security.RootUser, "scanTestCockroach")
	pgURL.Path = "test"
	defer cleanupFn()

	db, err := sql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test`); err != nil {
		t.Fatal(err)
	}

	// The test will screw around with KVBatchSize, restore it at the end.
	origVal := csql.KVBatchSize
	defer func() { csql.KVBatchSize = origVal }()

	numRows := 100

	if _, err := db.Exec(`DROP TABLE IF EXISTS test.scan`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE test.scan (k INT PRIMARY KEY, v STRING)`); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	buf.WriteString(`INSERT INTO test.scan VALUES `)
	for i := 0; i < numRows; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		if i%2 == 0 {
			fmt.Fprintf(&buf, "(%d, 'str%d')", i, i)
		} else {
			// Every other row doesn't get the string value (to have NULLs).
			fmt.Fprintf(&buf, "(%d, NULL)", i)
		}
	}
	if _, err := db.Exec(buf.String()); err != nil {
		t.Fatal(err)
	}

	batchSizes := []int{1, 2, 3, 5, 10, 13, 99, 100, 101, 199, 249, 250, 251}
	// We can test with at most one one span for now (see kvFetcher.fetch)
	numSpanValues := []int{0, 1}

	for _, batch := range batchSizes {
		csql.KVBatchSize = batch
		for _, numSpans := range numSpanValues {
			testScanBatchQuery(t, db, numSpans, numRows)
		}
	}

	if _, err := db.Exec(`DROP TABLE test.scan`); err != nil {
		t.Fatal(err)
	}
}
