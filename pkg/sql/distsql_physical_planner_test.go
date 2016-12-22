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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDistSQLPlanner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	args := base.TestClusterArgs{ReplicationMode: base.ReplicationManual}
	tc := serverutils.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop()

	sqlutils.CreateTable(
		t, tc.ServerConn(0), "t",
		"num INT PRIMARY KEY, str STRING, mod INT, INDEX(mod)",
		10, sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowEnglishFn, sqlutils.RowModuloFn(3)),
	)

	r := sqlutils.MakeSQLRunner(t, tc.ServerConn(0))
	r.DB.SetMaxOpenConns(1)
	r.Exec("SET DIST_SQL = ALWAYS")
	r.CheckQueryResults(
		"SELECT 5, 2 + num, * FROM test.t ORDER BY str",
		[][]string{
			strings.Fields("5 10  8 eight    2"),
			strings.Fields("5  7  5 five     2"),
			strings.Fields("5  6  4 four     1"),
			strings.Fields("5 11  9 nine     0"),
			strings.Fields("5  3  1 one      1"),
			strings.Fields("5 12 10 one-zero 1"),
			strings.Fields("5  9  7 seven    1"),
			strings.Fields("5  8  6 six      0"),
			strings.Fields("5  5  3 three    0"),
			strings.Fields("5  4  2 two      2"),
		},
	)
	r.CheckQueryResults(
		"SELECT str FROM test.t WHERE mod=0",
		[][]string{
			{"three"},
			{"six"},
			{"nine"},
		},
	)
}

func TestDistSQLJoinAndAgg(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This test sets up a distributed join between two tables:
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
	// Split the table into multiple ranges, with each range having a single
	// replica on a certain node. This forces the query to be distributed.
	//
	// TODO(radu): this approach should be generalized into test infrastructure
	// (perhaps by adding functionality to logic tests).
	// TODO(radu): we should verify that the plan is indeed distributed as
	// intended.
	descNumToStr := sqlbase.GetTableDescriptor(cdb, "test", "NumToStr")

	// split introduces a split and moves the right range to a given node.
	split := func(val int, targetNode int) {
		pik, err := sqlbase.MakePrimaryIndexKey(descNumToStr, val)
		if err != nil {
			t.Fatal(err)
		}

		splitKey := keys.MakeRowSentinelKey(pik)
		_, rightRange, err := tc.Server(0).SplitRange(splitKey)
		if err != nil {
			t.Fatal(err)
		}
		splitKey = rightRange.StartKey.AsRawKey()
		rightRange, err = tc.AddReplicas(splitKey, tc.Target(targetNode))
		if err != nil {
			t.Fatal(err)
		}

		// This transfer is necessary to avoid waiting for the lease to expire when
		// removing the first replica.
		if err := tc.TransferRangeLease(rightRange, tc.Target(targetNode)); err != nil {
			t.Fatal(err)
		}
		if _, err := tc.RemoveReplicas(splitKey, tc.Target(0)); err != nil {
			t.Fatal(err)
		}
	}
	// split moves the right range, so we split things back to front.
	for i := numNodes - 1; i > 0; i-- {
		split(n*n/numNodes*i, i)
	}

	r := sqlutils.MakeSQLRunner(t, tc.ServerConn(0))
	r.DB.SetMaxOpenConns(1)
	r.Exec("SET DIST_SQL = ALWAYS")
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
}
