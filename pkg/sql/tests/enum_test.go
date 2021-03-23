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
