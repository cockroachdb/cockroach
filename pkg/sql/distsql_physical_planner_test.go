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

package sql

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// SplitTable splits a range in the table, creates a replica for the right
// side of the split on targetNodeIdx, and moves the lease for the right
// side of the split to targetNodeIdx. This forces the querying against
// the table to be distributed. vals is a list of values forming a primary
// key for the table.
//
// TODO(radu): SplitTable or its equivalent should be added to TestCluster.
//
// TODO(radu): we should verify that the queries in tests using SplitTable
// are indeed distributed as intended.
func SplitTable(
	t *testing.T,
	tc serverutils.TestClusterInterface,
	desc *sqlbase.TableDescriptor,
	targetNodeIdx int,
	vals ...interface{},
) {
	pik, err := sqlbase.MakePrimaryIndexKey(desc, vals...)
	if err != nil {
		t.Fatal(err)
	}

	splitKey := keys.MakeRowSentinelKey(pik)
	_, rightRange, err := tc.Server(0).SplitRange(splitKey)
	if err != nil {
		t.Fatal(err)
	}

	rightRangeStartKey := rightRange.StartKey.AsRawKey()
	rightRange, err = tc.AddReplicas(rightRangeStartKey, tc.Target(targetNodeIdx))
	if err != nil {
		t.Fatal(err)
	}

	if err := tc.TransferRangeLease(rightRange, tc.Target(targetNodeIdx)); err != nil {
		t.Fatal(err)
	}
}

func TestDistSQLPlanner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This test sets up various queries using these tables:
	//  - a NumToSquare table of size N that maps integers from 1 to n to their
	//    squares
	//  - a NumToStr table of size N^2 that maps integers to their string
	//    representations. This table is split and distributed to all the nodes.
	const n = 100
	const numNodes = 5

	tc := serverutils.StartTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "test",
				Knobs: base.TestingKnobs{
					SQLSchemaChanger: &SchemaChangerTestingKnobs{
						// Aggressively write checkpoints, so that
						// we test checkpointing functionality while
						// a schema change backfill is progressing.
						WriteCheckpointInterval: time.Nanosecond,
					},
				},
			},
		})
	defer tc.Stopper().Stop()
	cdb := tc.Server(0).KVClient().(*client.DB)

	sqlutils.CreateTable(
		t, tc.ServerConn(0), "NumToSquare", "x INT PRIMARY KEY, xsquared INT",
		n,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, func(row int) parser.Datum {
			return parser.NewDInt(parser.DInt(row * row))
		}),
	)

	sqlutils.CreateTable(
		t, tc.ServerConn(0), "NumToStr", "y INT PRIMARY KEY, str STRING",
		n*n,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowEnglishFn),
	)
	// Split the table into multiple ranges.
	descNumToStr := sqlbase.GetTableDescriptor(cdb, "test", "NumToStr")
	// SplitTable moves the right range, so we split things back to front
	// in order to move less data.
	for i := numNodes - 1; i > 0; i-- {
		SplitTable(t, tc, descNumToStr, i, n*n/numNodes*i)
	}

	r := sqlutils.MakeSQLRunner(t, tc.ServerConn(0))
	r.DB.SetMaxOpenConns(1)
	r.Exec("SET DISTSQL = ALWAYS")

	t.Run("Basic", func(t *testing.T) {
		r = r.Subtest(t)
		// Query with a restricted span.
		r.CheckQueryResults(
			"SELECT 5, 2 + y, * FROM NumToStr WHERE y <= 10 ORDER BY str",
			[][]string{
				strings.Fields("5 10  8 eight"),
				strings.Fields("5  7  5 five"),
				strings.Fields("5  6  4 four"),
				strings.Fields("5 11  9 nine"),
				strings.Fields("5  3  1 one"),
				strings.Fields("5 12 10 one-zero"),
				strings.Fields("5  9  7 seven"),
				strings.Fields("5  8  6 six"),
				strings.Fields("5  5  3 three"),
				strings.Fields("5  4  2 two"),
			},
		)
		// Query which requires a full table scan.
		r.CheckQueryResults(
			"SELECT 5, 2 + y, * FROM NumToStr WHERE y % 1000 = 0 ORDER BY str",
			[][]string{
				strings.Fields("5 8002 8000 eight-zero-zero-zero"),
				strings.Fields("5 5002 5000 five-zero-zero-zero"),
				strings.Fields("5 4002 4000 four-zero-zero-zero"),
				strings.Fields("5 9002 9000 nine-zero-zero-zero"),
				strings.Fields("5 1002 1000 one-zero-zero-zero"),
				strings.Fields("5 10002 10000 one-zero-zero-zero-zero"),
				strings.Fields("5 7002 7000 seven-zero-zero-zero"),
				strings.Fields("5 6002 6000 six-zero-zero-zero"),
				strings.Fields("5 3002 3000 three-zero-zero-zero"),
				strings.Fields("5 2002 2000 two-zero-zero-zero"),
			},
		)
		// Query with a restricted span + filter.
		r.CheckQueryResults(
			"SELECT str FROM NumToStr WHERE y < 10 AND str LIKE '%e%' ORDER BY y",
			[][]string{
				{"one"},
				{"three"},
				{"five"},
				{"seven"},
				{"eight"},
				{"nine"},
			},
		)
		// Query which requires a full table scan.
		r.CheckQueryResults(
			"SELECT str FROM NumToStr WHERE y % 1000 = 0 AND str LIKE '%i%' ORDER BY y",
			[][]string{
				{"five-zero-zero-zero"},
				{"six-zero-zero-zero"},
				{"eight-zero-zero-zero"},
				{"nine-zero-zero-zero"},
			},
		)
	})

	t.Run("Join", func(t *testing.T) {
		r = r.Subtest(t)
		res := r.QueryStr("SELECT x, str FROM NumToSquare JOIN NumToStr ON y = xsquared")
		// Verify that res contains one entry for each integer, with the string
		// representation of its square, e.g.:
		//  [1, one]
		//  [2, two]
		//  [3, nine]
		//  [4, one-six]
		// (but not necessarily in order).
		if len(res) != n {
			t.Fatalf("expected %d rows, got %d", n, len(res))
		}
		resMap := make(map[int]string)
		for _, row := range res {
			if len(row) != 2 {
				t.Fatalf("invalid row %v", row)
			}
			n, err := strconv.Atoi(row[0])
			if err != nil {
				t.Fatalf("error parsing row %v: %s", row, err)
			}
			resMap[n] = row[1]
		}
		for i := 1; i <= n; i++ {
			if resMap[i] != sqlutils.IntToEnglish(i*i) {
				t.Errorf("invalid string for %d: %s", i, resMap[i])
			}
		}
	})

	t.Run("Agg", func(t *testing.T) {
		r = r.Subtest(t)
		var res [][]string
		checkRes := func(exp int) bool {
			return len(res) == 1 && len(res[0]) == 1 && res[0][0] == strconv.Itoa(exp)
		}

		// Sum the numbers in the NumToStr table.
		res = r.QueryStr("SELECT SUM(y) FROM NumToStr")
		if exp := n * n * (n*n + 1) / 2; !checkRes(exp) {
			t.Errorf("expected [[%d]], got %s", exp, res)
		}

		// Count the rows in the NumToStr table.
		res = r.QueryStr("SELECT COUNT(*) FROM NumToStr")
		if !checkRes(n * n) {
			t.Errorf("expected [[%d]], got %s", n*n, res)
		}

		// Count how many numbers contain the digit 5.
		res = r.QueryStr("SELECT COUNT(*) FROM NumToStr WHERE str LIKE '%five%'")
		exp := 0
		for i := 1; i <= n*n; i++ {
			for x := i; x > 0; x /= 10 {
				if x%10 == 5 {
					exp++
					break
				}
			}
		}
		if !checkRes(exp) {
			t.Errorf("expected [[%d]], got %s", exp, res)
		}
	})

	t.Run("Limit", func(t *testing.T) {
		r = r.Subtest(t)

		res := r.QueryStr("SELECT y FROM NumToStr LIMIT 5")
		if len(res) != 5 || len(res[0]) != 1 {
			t.Errorf("expected 5 rows, 1 cols; got %v", res)
		}

		r.CheckQueryResults(
			"SELECT y FROM NumToStr ORDER BY y LIMIT 5",
			[][]string{{"1"}, {"2"}, {"3"}, {"4"}, {"5"}},
		)

		r.CheckQueryResults(
			"SELECT y FROM NumToStr ORDER BY y OFFSET 5 LIMIT 2",
			[][]string{{"6"}, {"7"}},
		)

		r.CheckQueryResults(
			"SELECT y FROM NumToStr ORDER BY y LIMIT 0",
			[][]string{},
		)

		r.CheckQueryResults(
			"SELECT * FROM (SELECT y FROM NumToStr LIMIT 3) AS a ORDER BY y OFFSET 3",
			[][]string{},
		)

		r.CheckQueryResults(
			"SELECT y FROM NumToStr ORDER BY str LIMIT 5",
			[][]string{{"8"}, {"88"}, {"888"}, {"8888"}, {"8885"}},
		)

		r.CheckQueryResults(
			"SELECT y FROM (SELECT y FROM NumToStr ORDER BY y LIMIT 5) AS a WHERE y <> 2",
			[][]string{{"1"}, {"3"}, {"4"}, {"5"}},
		)
	})

	t.Run("Distinct", func(t *testing.T) {
		r.Subtest(t)

		// Check that DISTINCT doesn't alter results that are already distinct.
		r.CheckQueryResults(
			"SELECT DISTINCT y FROM NumToStr LIMIT 5",
			[][]string{{"1"}, {"2"}, {"3"}, {"4"}, {"5"}},
		)

		// Check that DISTINCT deduplicates.
		r.CheckQueryResults(
			"SELECT DISTINCT 5, 5 FROM NumToStr",
			[][]string{{"5", "5"}},
		)
	})

	// This test modifies the schema and can affect subsequent tests.
	t.Run("CreateIndex", func(t *testing.T) {
		r = r.Subtest(t)
		r.Exec("SET DISTSQL = OFF")
		if _, err := tc.ServerConn(0).Exec("CREATE INDEX foo ON NumToStr (str)"); err != nil {
			t.Fatal(err)
		}
		r.Exec("SET DISTSQL = ALWAYS")
		res := r.QueryStr("SELECT str FROM NumToStr@foo")
		if len(res) != n*n {
			t.Errorf("expected %d entries, got %d", n*n, len(res))
		}
		// Check res is sorted.
		curr := ""
		for i, str := range res {
			if curr > str[0] {
				t.Errorf("unexpected unsorted %s > %s at %d", curr, str[0], i)
			}
			curr = str[0]
		}
	})
}
