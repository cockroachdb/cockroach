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
			// Test for insert-only;
			// The trie is like:
			// i store (j)
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
			// Test for insert-only (different kinds of orders);
			// The trie is like:
			// i DESC
			// |
			// j DESC
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
			// Test for insert + removeStoring;
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
			// Test for insert + assignStoring;
			// The trie is like:
			// i store (k)          i
			// |              ->    |
			// j 	                  j store (k)
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
			// Test for insert + assignStoring;
			// The trie is like:
			// i   i DESC store (j)          i   i DESC
			// |   |                         |   |
			// j 	 k                   ->    j   k store (j)
			// |                             |
			// k                             k
			testType: insertAssignStoring,
			indexedColLists: []tree.IndexElemList{
				{
					tree.IndexElem{Column: "i", Direction: tree.DefaultDirection},
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
					{column: "i", direction: tree.Ascending},
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
			// Test for insert + removeStoring + assignStoring;
			// The trie is like:
			//   i store (j)            i
			// /   \                  /   \
			// j 	 k            ->    j   k
			// |                      |
			// k                      k
			testType: insertRemoveAndAssignStoring,
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
			var match = make(map[int]struct{})
			for i, retIndexedColList := range retIndexedColLists {
				// Find one matched indexes.
				for j, expectedIndexedColList := range testCase.expectedIndexedColLists {
					if _, ok := match[j]; !ok && len(retIndexedColList) == len(expectedIndexedColList) && len(retStoringColLists[i]) == len(testCase.expectedStoringColLists[j]) {
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
							match[j] = struct{}{}
						}
					}
				}
			}

			// Check whether all the indexes are covered.
			length := len(testCase.expectedIndexedColLists)
			for i := 0; i < length; i++ {
				if _, ok := match[i]; !ok {
					t.Errorf("failed test case %d since the expected index with id %d is missing!", idx, i)
				}
			}
		} else {
			t.Errorf("failed test case %d since the length of returned indexes is not the same as the expected length", idx)
		}
	}
}
