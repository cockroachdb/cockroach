// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package workloadindexrec

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/stretchr/testify/require"
)

func TestIndexTrie(t *testing.T) {
	testCases := []struct {
		testType int // 0: insert-only; 1: insert+removeStoring; 2: insert+assignStoring; 3: insert+removeStoring+assignStoring
		// The following two variables mean the indexed columns and storing columns for CREATE INDEX.
		ciIndexedColsArray []tree.IndexElemList
		ciStoringColsArray []tree.NameList
		// The following two variables represent the indexed columns and storing columns the system should return.
		resIndexedColsArray [][]indexedColumn
		resStoringColsArray [][]tree.Name
	}{
		{
			// Test for insert-only;
			// The trie is like:
			// i store (j)
			testType: 0,
			ciIndexedColsArray: []tree.IndexElemList{
				{
					tree.IndexElem{Column: "i", Expr: nil, Direction: tree.DefaultDirection, NullsOrder: tree.DefaultNullsOrder, OpClass: ""},
				},
				{
					tree.IndexElem{Column: "i", Expr: nil, Direction: tree.DefaultDirection, NullsOrder: tree.DefaultNullsOrder, OpClass: ""},
				},
			},
			ciStoringColsArray: []tree.NameList{
				{},
				{"j"},
			},
			resIndexedColsArray: [][]indexedColumn{
				{
					{column: "i", direction: tree.Ascending, nullsOrder: tree.NullsFirst},
				},
			},
			resStoringColsArray: [][]tree.Name{
				{"j"},
			},
		},
		{
			// Test for insert-only (different kinds of orders);
			// The trie is like:
			// i DES
			// |
			// j
			testType: 0,
			ciIndexedColsArray: []tree.IndexElemList{
				{
					tree.IndexElem{Column: "i", Expr: nil, Direction: tree.Descending, NullsOrder: tree.DefaultNullsOrder, OpClass: ""},
				},
				{
					tree.IndexElem{Column: "i", Expr: nil, Direction: tree.Descending, NullsOrder: tree.NullsLast, OpClass: ""},
					tree.IndexElem{Column: "j", Expr: nil, Direction: tree.Descending, NullsOrder: tree.NullsFirst, OpClass: ""},
				},
			},
			ciStoringColsArray: []tree.NameList{
				{},
				{},
			},
			resIndexedColsArray: [][]indexedColumn{
				{
					{column: "i", direction: tree.Descending, nullsOrder: tree.NullsLast},
					{column: "j", direction: tree.Descending, nullsOrder: tree.NullsFirst},
				},
			},
			resStoringColsArray: [][]tree.Name{
				{},
			},
		},
		{
			// Test for insert + removeStoring;
			// The trie is like:
			// i store (j)          i
			// |              ->    |
			// j 	                  j
			testType: 1,
			ciIndexedColsArray: []tree.IndexElemList{
				{
					tree.IndexElem{Column: "i", Expr: nil, Direction: tree.DefaultDirection, NullsOrder: tree.DefaultNullsOrder, OpClass: ""},
				},
				{
					tree.IndexElem{Column: "i", Expr: nil, Direction: tree.DefaultDirection, NullsOrder: tree.DefaultNullsOrder, OpClass: ""},
					tree.IndexElem{Column: "j", Expr: nil, Direction: tree.DefaultDirection, NullsOrder: tree.DefaultNullsOrder, OpClass: ""},
				},
			},
			ciStoringColsArray: []tree.NameList{
				{"j"},
				{},
			},
			resIndexedColsArray: [][]indexedColumn{
				{
					{column: "i", direction: tree.Ascending, nullsOrder: tree.NullsFirst},
					{column: "j", direction: tree.Ascending, nullsOrder: tree.NullsFirst},
				},
			},
			resStoringColsArray: [][]tree.Name{
				{},
			},
		},
		{
			// Test for insert + assignStoring;
			// The trie is like:
			// i store (k)          i
			// |              ->    |
			// j 	                  j store (k)
			testType: 2,
			ciIndexedColsArray: []tree.IndexElemList{
				{
					tree.IndexElem{Column: "i", Expr: nil, Direction: tree.DefaultDirection, NullsOrder: tree.DefaultNullsOrder, OpClass: ""},
				},
				{
					tree.IndexElem{Column: "i", Expr: nil, Direction: tree.DefaultDirection, NullsOrder: tree.DefaultNullsOrder, OpClass: ""},
					tree.IndexElem{Column: "j", Expr: nil, Direction: tree.DefaultDirection, NullsOrder: tree.DefaultNullsOrder, OpClass: ""},
				},
			},
			ciStoringColsArray: []tree.NameList{
				{"k"},
				{},
			},
			resIndexedColsArray: [][]indexedColumn{
				{
					{column: "i", direction: tree.Ascending, nullsOrder: tree.NullsFirst},
					{column: "j", direction: tree.Ascending, nullsOrder: tree.NullsFirst},
				},
			},
			resStoringColsArray: [][]tree.Name{
				{"k"},
			},
		},
		{
			// Test for insert + assignStoring;
			// The trie is like:
			//   i store (j)            i
			// /   \                  /   \
			// j 	 k            ->    j   k store (j)
			// |                      |
			// k                      k
			testType: 2,
			ciIndexedColsArray: []tree.IndexElemList{
				{
					tree.IndexElem{Column: "i", Expr: nil, Direction: tree.DefaultDirection, NullsOrder: tree.DefaultNullsOrder, OpClass: ""},
					tree.IndexElem{Column: "j", Expr: nil, Direction: tree.DefaultDirection, NullsOrder: tree.DefaultNullsOrder, OpClass: ""},
					tree.IndexElem{Column: "k", Expr: nil, Direction: tree.DefaultDirection, NullsOrder: tree.DefaultNullsOrder, OpClass: ""},
				},
				{
					tree.IndexElem{Column: "i", Expr: nil, Direction: tree.DefaultDirection, NullsOrder: tree.DefaultNullsOrder, OpClass: ""},
					tree.IndexElem{Column: "k", Expr: nil, Direction: tree.DefaultDirection, NullsOrder: tree.DefaultNullsOrder, OpClass: ""},
				},
				{
					tree.IndexElem{Column: "i", Expr: nil, Direction: tree.DefaultDirection, NullsOrder: tree.DefaultNullsOrder, OpClass: ""},
				},
			},
			ciStoringColsArray: []tree.NameList{
				{},
				{},
				{"j"},
			},
			resIndexedColsArray: [][]indexedColumn{
				{
					{column: "i", direction: tree.Ascending, nullsOrder: tree.NullsFirst},
					{column: "j", direction: tree.Ascending, nullsOrder: tree.NullsFirst},
					{column: "k", direction: tree.Ascending, nullsOrder: tree.NullsFirst},
				},
				{
					{column: "i", direction: tree.Ascending, nullsOrder: tree.NullsFirst},
					{column: "k", direction: tree.Ascending, nullsOrder: tree.NullsFirst},
				},
			},
			resStoringColsArray: [][]tree.Name{
				{},
				{"j"},
			},
		},
		{
			// Test for insert + removeStoring + assignStoring;
			// The trie is like:
			//   i store (j)            i
			// /   \                  /   \
			// j 	 k            ->    j   k
			// |                      |
			// k                      k
			testType: 3,
			ciIndexedColsArray: []tree.IndexElemList{
				{
					tree.IndexElem{Column: "i", Expr: nil, Direction: tree.DefaultDirection, NullsOrder: tree.DefaultNullsOrder, OpClass: ""},
					tree.IndexElem{Column: "j", Expr: nil, Direction: tree.DefaultDirection, NullsOrder: tree.DefaultNullsOrder, OpClass: ""},
					tree.IndexElem{Column: "k", Expr: nil, Direction: tree.DefaultDirection, NullsOrder: tree.DefaultNullsOrder, OpClass: ""},
				},
				{
					tree.IndexElem{Column: "i", Expr: nil, Direction: tree.DefaultDirection, NullsOrder: tree.DefaultNullsOrder, OpClass: ""},
					tree.IndexElem{Column: "k", Expr: nil, Direction: tree.DefaultDirection, NullsOrder: tree.DefaultNullsOrder, OpClass: ""},
				},
				{
					tree.IndexElem{Column: "i", Expr: nil, Direction: tree.DefaultDirection, NullsOrder: tree.DefaultNullsOrder, OpClass: ""},
				},
			},
			ciStoringColsArray: []tree.NameList{
				{},
				{},
				{"j"},
			},
			resIndexedColsArray: [][]indexedColumn{
				{
					{column: "i", direction: tree.Ascending, nullsOrder: tree.NullsFirst},
					{column: "j", direction: tree.Ascending, nullsOrder: tree.NullsFirst},
					{column: "k", direction: tree.Ascending, nullsOrder: tree.NullsFirst},
				},
				{
					{column: "i", direction: tree.Ascending, nullsOrder: tree.NullsFirst},
					{column: "k", direction: tree.Ascending, nullsOrder: tree.NullsFirst},
				},
			},
			resStoringColsArray: [][]tree.Name{
				{},
				{},
			},
		},
	}

	for idx, testCase := range testCases {
		trie := NewTrie()
		for i, indexedCols := range testCase.ciIndexedColsArray {
			trie.insert(indexedCols, testCase.ciStoringColsArray[i])
		}

		// Type 1 or 3 including removeStoring
		if (testCase.testType & 1) > 0 {
			removeStorings(trie)
		}

		// Type 2 or 3 including assignStoring
		if (testCase.testType & 2) > 0 {
			assignStoring(trie)
		}

		retIndexedColsArray, retStoringColsArray := collectAllLeaves4Table(trie)
		var flag = true
		if len(retIndexedColsArray) == len(testCase.resIndexedColsArray) && len(retStoringColsArray) == len(testCase.resStoringColsArray) {
			var match = make(map[int]struct{})
			for i, retIndexedCols := range retIndexedColsArray {
				// Find one matched indexes.
				for j, resIndexedCols := range testCase.resIndexedColsArray {
					if _, ok := match[j]; !ok && len(retIndexedCols) == len(resIndexedCols) && len(retStoringColsArray[i]) == len(testCase.resStoringColsArray[j]) {
						var same = true
						// Compare the indexedCol.
						for k, retIndexedCol := range retIndexedCols {
							if retIndexedCol != resIndexedCols[k] {
								same = false
								break
							}
						}
						// Compare the storingCol.
						if same {
							for k, retStoringCol := range retStoringColsArray[i] {
								if retStoringCol != testCase.resStoringColsArray[j][k] {
									same = false
									break
								}
							}
						}
						if same {
							match[j] = struct{}{}
						}
					}
				}
			}

			// Check whether all the indexes are covered.
			length := len(testCase.resIndexedColsArray)
			for i := 0; i < length; i++ {
				if _, ok := match[i]; !ok {
					flag = false
					break
				}
			}
		} else {
			flag = false
		}

		require.Equalf(t, true, flag, "failed test case %d", idx)
	}
}
