// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workloadindexrec

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestIndexTrie(t *testing.T) {
	type testType int8

	const (
		insertOnly testType = iota
		insertRemoveStoring
		insertAssignStoring
		insertRemoveAndAssignStoring
	)

	testCases := []struct {
		testType testType
		// The following two variables mean the indexed columns and storing columns for CREATE INDEX.
		indexedColLists []tree.IndexElemList
		storingColLists []tree.NameList
		// The following two variables represent the indexed columns and storing columns the system should return.
		expectedIndexedColLists [][]indexedColumn
		expectedStoringColLists [][]tree.Name
	}{
		{
			// 0:
			// Test for insert-only;
			//
			// Input indexes:
			// (i)
			// (i) STORING (j)
			//
			// Output indexes:
			// (i) STORING (j)
			//
			// The trie is like:
			// i store (j)    ->    i store (j)
			testType: insertOnly,
			indexedColLists: []tree.IndexElemList{
				{
					tree.IndexElem{Column: "i", Direction: tree.DefaultDirection},
				},
				{
					tree.IndexElem{Column: "i", Direction: tree.DefaultDirection},
				},
			},
			storingColLists: []tree.NameList{
				{},
				{"j"},
			},
			expectedIndexedColLists: [][]indexedColumn{
				{
					{column: "i", direction: tree.Ascending},
				},
			},
			expectedStoringColLists: [][]tree.Name{
				{"j"},
			},
		},
		{
			// 1:
			// Test for insert-only (different kinds of orders);
			//
			// Input indexes:
			// (i DESC)
			// (i DESC, j DESC)
			//
			// Output indexes:
			// (i DESC, j DESC)
			//
			// The trie is like:
			// i DESC          i DESC
			// |         ->    |
			// j DESC          i DESC
			testType: insertOnly,
			indexedColLists: []tree.IndexElemList{
				{
					tree.IndexElem{Column: "i", Direction: tree.Descending},
				},
				{
					tree.IndexElem{Column: "i", Direction: tree.Descending},
					tree.IndexElem{Column: "j", Direction: tree.Descending},
				},
			},
			storingColLists: []tree.NameList{
				{},
				{},
			},
			expectedIndexedColLists: [][]indexedColumn{
				{
					{column: "i", direction: tree.Descending},
					{column: "j", direction: tree.Descending},
				},
			},
			expectedStoringColLists: [][]tree.Name{
				{},
			},
		},
		{
			// 2:
			// Test for insert + removeStoring;
			//
			// Input indexes:
			// (i) STORING (j)
			// (i, j)
			//
			// Output indexes:
			// (i, j)
			//
			// The trie is like:
			// i store (j)          i
			// |              ->    |
			// j                    j
			testType: insertRemoveStoring,
			indexedColLists: []tree.IndexElemList{
				{
					tree.IndexElem{Column: "i", Direction: tree.DefaultDirection},
				},
				{
					tree.IndexElem{Column: "i", Direction: tree.DefaultDirection},
					tree.IndexElem{Column: "j", Direction: tree.DefaultDirection},
				},
			},
			storingColLists: []tree.NameList{
				{"j"},
				{},
			},
			expectedIndexedColLists: [][]indexedColumn{
				{
					{column: "i", direction: tree.Ascending},
					{column: "j", direction: tree.Ascending},
				},
			},
			expectedStoringColLists: [][]tree.Name{
				{},
			},
		},
		{
			// 3:
			// Test for insert + assignStoring;
			//
			// Input indexes:
			// (i) STORING (k)
			// (i, j)
			//
			// Output indexes:
			// (i, j) STORING (k)
			//
			// The trie is like:
			// i store (k)          i
			// |              ->    |
			// j                    j store (k)
			testType: insertAssignStoring,
			indexedColLists: []tree.IndexElemList{
				{
					tree.IndexElem{Column: "i", Direction: tree.DefaultDirection},
				},
				{
					tree.IndexElem{Column: "i", Direction: tree.DefaultDirection},
					tree.IndexElem{Column: "j", Direction: tree.DefaultDirection},
				},
			},
			storingColLists: []tree.NameList{
				{"k"},
				{},
			},
			expectedIndexedColLists: [][]indexedColumn{
				{
					{column: "i", direction: tree.Ascending},
					{column: "j", direction: tree.Ascending},
				},
			},
			expectedStoringColLists: [][]tree.Name{
				{"k"},
			},
		},
		{
			// 4:
			// Test for insert + assignStoring;
			//
			// Input indexes:
			// (i DESC, j, k)
			// (i DESC, k)
			// (i DESC) STORING (j)
			//
			// Output indexes:
			// (i DESC, j, k)
			// (i DESC, k) STORING (j)
			//
			// The trie is like:
			//   i DESC store (j)              i DESC
			// /   \                         /   \
			// j   k                   ->    j   k store (j)
			// |                             |
			// k                             k
			testType: insertAssignStoring,
			indexedColLists: []tree.IndexElemList{
				{
					tree.IndexElem{Column: "i", Direction: tree.Descending},
					tree.IndexElem{Column: "j", Direction: tree.DefaultDirection},
					tree.IndexElem{Column: "k", Direction: tree.DefaultDirection},
				},
				{
					tree.IndexElem{Column: "i", Direction: tree.Descending},
					tree.IndexElem{Column: "k", Direction: tree.DefaultDirection},
				},
				{
					tree.IndexElem{Column: "i", Direction: tree.Descending},
				},
			},
			storingColLists: []tree.NameList{
				{},
				{},
				{"j"},
			},
			expectedIndexedColLists: [][]indexedColumn{
				{
					{column: "i", direction: tree.Descending},
					{column: "j", direction: tree.Ascending},
					{column: "k", direction: tree.Ascending},
				},
				{
					{column: "i", direction: tree.Descending},
					{column: "k", direction: tree.Ascending},
				},
			},
			expectedStoringColLists: [][]tree.Name{
				{},
				{"j"},
			},
		},
		{
			// 5:
			// Test for insert + removeStoring + assignStoring;
			//
			// Input indexes:
			// (i, j, k)
			// (i, k)
			// (i) STORING (j)
			//
			// Output indexes:
			// (i, j, k)
			// (i, k)
			//
			// The trie is like:
			//   i store (j)            i
			// /   \                  /   \
			// j   k            ->    j   k
			// |                      |
			// k                      k
			indexedColLists: []tree.IndexElemList{
				{
					tree.IndexElem{Column: "i", Direction: tree.DefaultDirection},
					tree.IndexElem{Column: "j", Direction: tree.DefaultDirection},
					tree.IndexElem{Column: "k", Direction: tree.DefaultDirection},
				},
				{
					tree.IndexElem{Column: "i", Direction: tree.DefaultDirection},
					tree.IndexElem{Column: "k", Direction: tree.DefaultDirection},
				},
				{
					tree.IndexElem{Column: "i", Direction: tree.DefaultDirection},
				},
			},
			storingColLists: []tree.NameList{
				{},
				{},
				{"j"},
			},
			expectedIndexedColLists: [][]indexedColumn{
				{
					{column: "i", direction: tree.Ascending},
					{column: "j", direction: tree.Ascending},
					{column: "k", direction: tree.Ascending},
				},
				{
					{column: "i", direction: tree.Ascending},
					{column: "k", direction: tree.Ascending},
				},
			},
			expectedStoringColLists: [][]tree.Name{
				{},
				{},
			},
		},
	}

	indexOutputFunc := func(testIdx int, indexIdx int) string {
		res := "("
		for i, indexedCol := range testCases[testIdx].expectedIndexedColLists[indexIdx] {
			if i > 0 {
				res += ", "
			}
			res += string(indexedCol.column)
			if indexedCol.direction == tree.Descending {
				res += " DESC"
			}
		}
		res += ")"
		for i, storingCol := range testCases[testIdx].expectedStoringColLists[indexIdx] {
			if i == 0 {
				res += " STORING ("
			} else {
				res += ", "
			}
			res += string(storingCol)
		}
		if len(testCases[testIdx].expectedStoringColLists[indexIdx]) > 0 {
			res += ")"
		}
		return res
	}

	for idx, testCase := range testCases {
		trie := NewTrie()
		for i, indexedCols := range testCase.indexedColLists {
			trie.Insert(indexedCols, testCase.storingColLists[i])
		}

		if testCase.testType == insertRemoveStoring || testCase.testType == insertRemoveAndAssignStoring {
			trie.RemoveStorings()
		}

		if testCase.testType == insertAssignStoring || testCase.testType == insertRemoveAndAssignStoring {
			trie.AssignStoring()
		}

		retIndexedColLists, retStoringColLists := collectAllLeavesForTable(trie)
		if len(retIndexedColLists) == len(testCase.expectedIndexedColLists) && len(retStoringColLists) == len(testCase.expectedStoringColLists) {
			expectedIndexHit := make([]bool, len(testCase.expectedIndexedColLists))
			for i, retIndexedColList := range retIndexedColLists {
				// Find one matched indexes.
				for j, expectedIndexedColList := range testCase.expectedIndexedColLists {
					if !expectedIndexHit[j] && len(retIndexedColList) == len(expectedIndexedColList) && len(retStoringColLists[i]) == len(testCase.expectedStoringColLists[j]) {
						var same = true
						// Compare the indexedCol.
						for k, retIndexedCol := range retIndexedColList {
							if retIndexedCol != expectedIndexedColList[k] {
								same = false
								break
							}
						}
						// Compare the storingCol.
						if same {
							for k, retStoringCol := range retStoringColLists[i] {
								if retStoringCol != testCase.expectedStoringColLists[j][k] {
									same = false
									break
								}
							}
						}
						if same {
							expectedIndexHit[j] = true
						}
					}
				}
			}

			// Check whether all the indexes are covered.
			for i, hit := range expectedIndexHit {
				if !hit {
					t.Errorf("failed test case %d since the expected index %s is missing!", idx, indexOutputFunc(idx, i))
				}
			}
		} else {
			t.Errorf("failed test case %d since the length of returned indexes is not the same as the expected length", idx)
		}
	}
}
