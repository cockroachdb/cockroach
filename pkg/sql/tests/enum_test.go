// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/google/btree"
	"github.com/stretchr/testify/require"
)

func TestLargeEnums(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	// The idea here is that we're going to create a very large number of
	// enum elements corresponding to non-negative integers and then we're
	// going to add them to the enum in a random order. Then we're going to
	// make sure that we can cast the integers to strings to the enums, order
	// them as enums then cast them back to ints and make sure it's what we want.
	// Ideally we'd make this number even bigger but it's slow enough as it is.
	const N = 100

	order := rand.Perm(N)
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	tdb.Exec(t, "CREATE TYPE e AS ENUM ()")
	// Construct a single transaction to insert all the values; otherwise we'd
	// have to wait for lots of versions and it would take a very long time.
	var createEnumsQuery string
	{
		alreadyInserted := btree.New(8)
		next := func(n int) (next int, ok bool) {
			alreadyInserted.AscendGreaterOrEqual(intItem(n), func(i btree.Item) (wantMore bool) {
				next, ok = int(i.(intItem)), true
				return false
			})
			return next, ok
		}
		prev := func(n int) (prev int, ok bool) {
			alreadyInserted.DescendLessOrEqual(intItem(n), func(i btree.Item) (wantMore bool) {
				prev, ok = int(i.(intItem)), true
				return false
			})
			return prev, ok
		}
		var buf strings.Builder
		buf.WriteString("BEGIN;\n")
		for i, n := range order {
			fmt.Fprintf(&buf, "\tALTER TYPE e ADD VALUE '%d'", n)
			if alreadyInserted.Len() == 0 {
				buf.WriteString(";\n")
			} else if next, ok := next(n); ok {
				fmt.Fprintf(&buf, " BEFORE '%d';\n", next)
			} else {
				prev, ok := prev(n)
				require.Truef(t, ok, "prev %v %v", n, order[:i])
				fmt.Fprintf(&buf, " AFTER '%d';\n", prev)
			}
			alreadyInserted.ReplaceOrInsert(intItem(n))
		}
		buf.WriteString("COMMIT;")
		createEnumsQuery = buf.String()
	}
	tdb.Exec(t, createEnumsQuery)

	// Okay, now we have enum values for all of these numbers.
	tdb.Exec(t, `CREATE TABLE t (i e PRIMARY KEY);`)
	tdb.Exec(t, `
  INSERT INTO t SELECT i
    FROM (
            SELECT i::STRING::e AS i
              FROM generate_series(0, $1 - 1, 1) AS t (i)
         );`, N)
	rows := tdb.Query(t, "SELECT i::STRING::INT FROM t")
	var read []int
	for rows.Next() {
		var i int
		require.NoError(t, rows.Scan(&i))
		read = append(read, i)
	}
	require.NoError(t, rows.Err())
	require.Len(t, read, N)
	require.Truef(t, sort.IntsAreSorted(read), "%v", read)
}

type intItem int

func (i intItem) Less(o btree.Item) bool {
	return i < o.(intItem)
}

// TestEnumPlaceholderWithAsOfSystemTime is a regression test for an edge case
// with bind where we would not properly deal with leases involving types.
func TestEnumPlaceholderWithAsOfSystemTime(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	db.Exec(t, "CREATE TYPE typ AS ENUM ('a', 'b')")
	db.Exec(t, "CREATE TABLE tab (k INT PRIMARY KEY, v typ)")
	db.Exec(t, "INSERT INTO tab VALUES ($1, $2)", 1, "a")
	var afterInsert string
	db.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&afterInsert)
	db.Exec(t, "ALTER TYPE typ ADD VALUE 'c'")
	db.Exec(t, "INSERT INTO tab VALUES ($1, $2)", 2, "c")
	// Before the commit which introduced this test, the below statement would
	// crash the server.
	q := fmt.Sprintf("SELECT k FROM tab AS OF SYSTEM TIME %s WHERE v = $1", afterInsert)
	require.Equal(t, [][]string{{"1"}}, db.QueryStr(t, q, "a"))
	db.Exec(t, "ALTER TYPE typ RENAME VALUE 'a' TO 'd'")
	db.Exec(t, "ALTER TYPE typ RENAME VALUE 'b' TO 'a'")
	// The AOST should apply to the transaction that binds the placeholder,
	// since the same implicit transaction is used for binding and executing.
	require.Equal(t, [][]string{{"1"}}, db.QueryStr(t, q, "a"))
	require.Equal(t, [][]string{}, db.QueryStr(t, q, "b"))
	db.ExpectErr(t, "invalid input value for enum typ: \"d\"", q, "d")
	require.Equal(t, [][]string{}, db.QueryStr(t, "SELECT k FROM tab WHERE v = $1", "a"))
	require.Equal(t, [][]string{{"1"}}, db.QueryStr(t, "SELECT k FROM tab WHERE v = $1", "d"))
}

// TestEnumDropValueCheckConstraint tests that check constraints containing
// a reference to an enum value are properly taken into account when attempting
// to drop said value.
func TestEnumDropValueCheckConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	db.Exec(t, "CREATE TYPE typ AS ENUM ('a', 'b', 'c')")

	// Check that an enum value cannot be dropped if it is referenced in a table's
	// check constraint.
	db.Exec(t, "CREATE TABLE t1 (k typ CHECK (k::typ = 'a'))")
	db.ExpectErr(t, "^pq: could not remove enum value \"a\" as it is being used in a check constraint of \"t1\"$",
		"ALTER TYPE typ DROP VALUE 'a'")

	db.Exec(t, "DROP TABLE t1")
	db.Exec(t, "ALTER TYPE typ DROP VALUE 'a'")

	// Check that the enum value cannot be dropped even if it is referenced only
	// in a table's check constraint.
	db.Exec(t, "CREATE TABLE t2 (k STRING CHECK (k::typ = 'b'))")
	db.ExpectErr(t, "^pq: could not remove enum value \"b\" as it is being used in a check constraint of \"t2\"$",
		"ALTER TYPE typ DROP VALUE 'b'")

	db.Exec(t, "DROP TABLE t2")
	db.Exec(t, "ALTER TYPE typ DROP VALUE 'b'")
}
