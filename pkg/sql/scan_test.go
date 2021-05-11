// Copyright 2016 The Cockroach Authors.
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
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// genAs returns num random distinct ordered values in [0, valRange).
func genValues(num, valRange int) []int {
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
	return perm
}

// testScanBatchQuery runs a query of the form
//  SELECT a,B FROM test.scan WHERE a IN (1,5,3..) AND b >= 5 AND b <= 10
// numSpans controls the number of possible values for a.
func testScanBatchQuery(t *testing.T, db *gosql.DB, numSpans, numAs, numBs int, reverse bool) {
	// Generate numSpans values for A
	aVals := genValues(numSpans, numAs)

	// Generate a random range for B
	bStart := rand.Int() % numBs
	bEnd := bStart + rand.Int()%(numBs-bStart)

	var expected [][2]int
	for _, a := range aVals {
		for b := bStart; b <= bEnd; b++ {
			expected = append(expected, [2]int{a, b})
		}
	}

	if len(aVals) == 0 {
		// No filter on a.
		for a := 0; a < numAs; a++ {
			for b := bStart; b <= bEnd; b++ {
				expected = append(expected, [2]int{a, b})
			}
		}
	}

	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("SELECT a,b FROM test.scan WHERE b >= %d AND b <= %d", bStart, bEnd))
	for i, a := range aVals {
		if i == 0 {
			buf.WriteString(fmt.Sprintf(" AND a IN (%d", a))
		} else {
			buf.WriteString(fmt.Sprintf(",%d", a))
		}
	}
	if len(aVals) > 0 {
		buf.WriteString(")")
	}

	if reverse {
		buf.WriteString(" ORDER BY a DESC, b DESC")
		for i, j := 0, len(expected)-1; i < j; i, j = i+1, j-1 {
			expected[i], expected[j] = expected[j], expected[i]
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
		var a, b int
		err = rows.Scan(&a, &b)
		if err != nil {
			t.Fatal(err)
		}
		if a != expected[n][0] || b != expected[n][1] {
			t.Errorf("row %d: invalid values %d,%d (expected %d,%d)",
				n, a, b, expected[n][0], expected[n][1])
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
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The test sets up a table with a row for each pair of (a,b) values.
	numAs := 5
	numBs := 20

	schema := `
		CREATE DATABASE test;
		CREATE TABLE test.scan (
			a INT,
			b INT,
			v STRING,
			PRIMARY KEY (a, b),
			FAMILY (a),
			FAMILY (b)
		);`

	// The table will have one key for the even rows, and two keys for the odd rows.
	numKeys := 3 * numAs * numBs / 2
	batchSizes := []int{1, 2, 5, 13, 100, numKeys - 1, numKeys, numKeys + 1}

	for _, batch := range batchSizes {
		// We must set up a separate server for each batch size, as we cannot change
		// it while the server is running (#53002).
		t.Run(fmt.Sprintf("%d", batch), func(t *testing.T) {
			restore := row.TestingSetKVBatchSize(int64(batch))
			defer restore()
			s, db, _ := serverutils.StartServer(
				t, base.TestServerArgs{UseDatabase: "test"})
			defer s.Stopper().Stop(context.Background())

			if _, err := db.Exec(schema); err != nil {
				t.Fatal(err)
			}

			var buf bytes.Buffer
			buf.WriteString(`INSERT INTO test.scan VALUES `)
			for a := 0; a < numAs; a++ {
				for b := 0; b < numBs; b++ {
					if a+b > 0 {
						buf.WriteString(", ")
					}
					if (a+b)%2 == 0 {
						fmt.Fprintf(&buf, "(%d, %d, 'str%d%d')", a, b, a, b)
					} else {
						// Every other row doesn't get the string value (to have NULLs).
						fmt.Fprintf(&buf, "(%d, %d, NULL)", a, b)
					}
				}
			}
			if _, err := db.Exec(buf.String()); err != nil {
				t.Fatal(err)
			}

			for numSpans := 0; numSpans < 4; numSpans++ {
				testScanBatchQuery(t, db, numSpans, numAs, numBs, false /* reverse */)
				testScanBatchQuery(t, db, numSpans, numAs, numBs, true /* reverse */)
			}
		})
	}
}
