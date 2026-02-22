// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package indexrec

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func TestCopyIndexes(t *testing.T) {
	tables, indexCols := testTablesAndIndexCols()
	inputIndexes1 := testIndexCandidates1(tables, indexCols)
	inputIndexes2 := testIndexCandidates2(tables, indexCols)
	duplicateInputIndexes1 := testDuplicateIndexCandidates1(tables, indexCols)
	testData := []struct {
		inputIndexes    map[cat.Table][][]cat.IndexColumn
		outputIndexes   map[cat.Table][][]cat.IndexColumn
		expectedIndexes map[cat.Table][][]cat.IndexColumn
	}{
		{
			inputIndexes1,
			make(map[cat.Table][][]cat.IndexColumn),
			inputIndexes1,
		},
		{
			inputIndexes2,
			make(map[cat.Table][][]cat.IndexColumn),
			inputIndexes2,
		},
		// Ensure duplicate indexes are removed when copying.
		{
			duplicateInputIndexes1,
			make(map[cat.Table][][]cat.IndexColumn),
			inputIndexes1,
		},
	}

	for _, d := range testData {
		copyIndexes(d.inputIndexes, d.outputIndexes)
		if !candidatesAreEqual(d.expectedIndexes, d.outputIndexes) {
			t.Errorf(
				"expected copied indexes to be %+v,\ngot %+v\n", d.expectedIndexes, d.outputIndexes,
			)
		}
	}
}

func TestGroupIndexesByTable(t *testing.T) {
	tables, indexCols := testTablesAndIndexCols()
	inputIndexes := testIndexCandidates2(tables, indexCols)
	outputIndexes := make(map[cat.Table][][]cat.IndexColumn)
	expectedIndexes := map[cat.Table][][]cat.IndexColumn{
		tables[1]: {[]cat.IndexColumn{indexCols[0], indexCols[2], indexCols[1]}},
	}

	groupIndexesByTable(inputIndexes, outputIndexes)

	if !candidatesAreEqual(expectedIndexes, outputIndexes) {
		t.Errorf(
			"expected grouped indexes to be %+v,\ngot %+v\n", expectedIndexes, outputIndexes,
		)
	}
}

func TestConstructIndexCombinations(t *testing.T) {
	tables, indexCols := testTablesAndIndexCols()
	inputIndexes1 := testIndexCandidates1(tables, indexCols)
	inputIndexes2 := testIndexCandidates2(tables, indexCols)
	outputIndexes := make(map[cat.Table][][]cat.IndexColumn)
	expectedIndexes := map[cat.Table][][]cat.IndexColumn{
		tables[1]: {
			[]cat.IndexColumn{
				indexCols[1], indexCols[2], indexCols[0],
			},
			[]cat.IndexColumn{
				indexCols[0], indexCols[2],
			},
			[]cat.IndexColumn{
				indexCols[0], indexCols[1],
			},
		},
	}

	constructIndexCombinations(inputIndexes1, inputIndexes2, outputIndexes)

	if !candidatesAreEqual(expectedIndexes, outputIndexes) {
		t.Errorf(
			"expected index combinations to be %+v,\ngot %+v\n", expectedIndexes, outputIndexes,
		)
	}
}

func TestAddIndexToCandidates(t *testing.T) {
	tables, indexCols := testTablesAndIndexCols()
	newIndex := []cat.IndexColumn{indexCols[0]}
	existingIndex := []cat.IndexColumn{indexCols[0], indexCols[1]}
	indexCandidates := testIndexCandidates1(tables, indexCols)
	updatedIndexCandidates := testUpdatedIndexCandidates1(tables, indexCols, newIndex)
	testData := []struct {
		newIndex        []cat.IndexColumn
		table           cat.Table
		inputIndexes    map[cat.Table][][]cat.IndexColumn
		expectedIndexes map[cat.Table][][]cat.IndexColumn
	}{
		{
			newIndex,
			tables[0],
			make(map[cat.Table][][]cat.IndexColumn),
			map[cat.Table][][]cat.IndexColumn{tables[0]: {newIndex}},
		},
		{
			existingIndex,
			tables[0],
			indexCandidates,
			indexCandidates,
		},
		{
			newIndex,
			tables[0],
			indexCandidates,
			updatedIndexCandidates,
		},
	}

	for _, d := range testData {
		addIndexToCandidates(d.newIndex, d.table, d.inputIndexes)
		if !candidatesAreEqual(d.expectedIndexes, d.inputIndexes) {
			t.Errorf("expected indexes to be %+v,\ngot %+v\n", d.expectedIndexes, d.inputIndexes)
		}
	}
}

func testIndexCandidates1(
	tables []cat.Table, indexCols []cat.IndexColumn,
) map[cat.Table][][]cat.IndexColumn {
	outputMap := map[cat.Table][][]cat.IndexColumn{
		tables[0]: {[]cat.IndexColumn{indexCols[1]}, []cat.IndexColumn{indexCols[0], indexCols[1]}},
		tables[1]: {
			[]cat.IndexColumn{indexCols[1], indexCols[2]},
			[]cat.IndexColumn{indexCols[0]},
			[]cat.IndexColumn{indexCols[0], indexCols[2], indexCols[1]},
		},
	}
	return outputMap
}

func testDuplicateIndexCandidates1(
	tables []cat.Table, indexCols []cat.IndexColumn,
) map[cat.Table][][]cat.IndexColumn {
	outputMap := testIndexCandidates1(tables, indexCols)
	outputMap[tables[0]] = append(outputMap[tables[0]], outputMap[tables[0]][0])
	outputMap[tables[1]] = append(outputMap[tables[1]], outputMap[tables[1]][0])
	outputMap[tables[1]] = append(outputMap[tables[1]], outputMap[tables[1]][1])
	return outputMap
}

func testUpdatedIndexCandidates1(
	tables []cat.Table, indexCols []cat.IndexColumn, index []cat.IndexColumn,
) map[cat.Table][][]cat.IndexColumn {
	outputMap := testIndexCandidates1(tables, indexCols)
	outputMap[tables[0]] = append(outputMap[tables[0]], index)
	return outputMap
}

func testIndexCandidates2(
	tables []cat.Table, indexCols []cat.IndexColumn,
) map[cat.Table][][]cat.IndexColumn {
	outputMap := map[cat.Table][][]cat.IndexColumn{
		tables[1]: {[]cat.IndexColumn{indexCols[0], indexCols[2]}, []cat.IndexColumn{indexCols[1]}},
	}
	return outputMap
}

// Helper function to create common columns and index columns.
func createTestColumnsAndIndexCols() (cat.IndexColumn, cat.IndexColumn, cat.IndexColumn) {
	col1 := cat.Column{}
	col2 := cat.Column{}
	col3 := cat.Column{}

	col1.Init(0,
		1,
		"k",
		cat.Ordinary,
		types.Bool,
		false,
		cat.Visible,
		nil, /* defaultExpr */
		nil, /* computedExpr */
		nil, /* onUpdateExpr */
		cat.NotGeneratedAsIdentity,
		nil /* generatedAsIdentitySequenceOption */)
	col2.Init(1,
		2,
		"i",
		cat.Ordinary,
		types.Bool,
		false,
		cat.Visible,
		nil, /* defaultExpr */
		nil, /* computedExpr */
		nil, /* onUpdateExpr */
		cat.NotGeneratedAsIdentity,
		nil /* generatedAsIdentitySequenceOption */)
	col3.Init(2,
		3,
		"j",
		cat.Ordinary,
		types.Bool,
		false,
		cat.Visible,
		nil, /* defaultExpr */
		nil, /* computedExpr */
		nil, /* onUpdateExpr */
		cat.NotGeneratedAsIdentity,
		nil /* generatedAsIdentitySequenceOption */)

	indexCol1 := cat.IndexColumn{Column: &col1, Descending: false}
	indexCol2 := cat.IndexColumn{Column: &col2, Descending: false}
	indexCol3 := cat.IndexColumn{Column: &col3, Descending: true}

	return indexCol1, indexCol2, indexCol3
}

func testTablesAndIndexCols() ([]cat.Table, []cat.IndexColumn) {
	table1 := testcat.Table{TabID: 1}
	table2 := testcat.Table{TabID: 2}
	indexCol1, indexCol2, indexCol3 := createTestColumnsAndIndexCols()

	// Add existing indexes.
	table1.Indexes = []*testcat.Index{
		{Columns: []cat.IndexColumn{indexCol1}, ExplicitColCount: 1},
		{Columns: []cat.IndexColumn{indexCol2}, ExplicitColCount: 1},
	}
	table2.Indexes = []*testcat.Index{
		{Columns: []cat.IndexColumn{indexCol1}, ExplicitColCount: 1},
		{Columns: []cat.IndexColumn{indexCol2, indexCol3}, ExplicitColCount: 2},
	}

	return []cat.Table{&table1, &table2}, []cat.IndexColumn{indexCol1, indexCol2, indexCol3}
}

func testTablesAndIndexColsWithPredicates() ([]cat.Table, []cat.IndexColumn) {
	table1 := testcat.Table{TabID: 1}
	table2 := testcat.Table{TabID: 2}
	table3 := testcat.Table{TabID: 3}
	indexCol1, indexCol2, indexCol3 := createTestColumnsAndIndexCols()

	// Create indexes with predicates for table1 (predicate on one column).
	index1WithPredicate := &testcat.Index{Columns: []cat.IndexColumn{indexCol1}, ExplicitColCount: 1}
	index1WithPredicate.SetPredicate("k > 0")

	// Create indexes with predicates for table2 (predicates on all columns).
	index2WithPredicateCol1 := &testcat.Index{Columns: []cat.IndexColumn{indexCol1}, ExplicitColCount: 1}
	index2WithPredicateCol1.SetPredicate("k > 0")

	index2WithPredicateCol2Col3 := &testcat.Index{Columns: []cat.IndexColumn{indexCol2, indexCol3}, ExplicitColCount: 2}
	index2WithPredicateCol2Col3.SetPredicate("i = true AND j = false")

	// Add existing indexes.
	// Table1: predicates on one column
	table1.Indexes = []*testcat.Index{
		index1WithPredicate,
		{Columns: []cat.IndexColumn{indexCol2}, ExplicitColCount: 1},
	}

	// Table2: predicates on all columns
	table2.Indexes = []*testcat.Index{
		index2WithPredicateCol1,
		index2WithPredicateCol2Col3,
	}

	// Table3: no predicates
	table3.Indexes = []*testcat.Index{
		{Columns: []cat.IndexColumn{indexCol1}, ExplicitColCount: 1},
		{Columns: []cat.IndexColumn{indexCol2}, ExplicitColCount: 1},
		{Columns: []cat.IndexColumn{indexCol3}, ExplicitColCount: 1},
	}

	return []cat.Table{&table1, &table2, &table3}, []cat.IndexColumn{indexCol1, indexCol2, indexCol3}
}

func candidatesAreEqual(leftCandidates, rightCandidates map[cat.Table][][]cat.IndexColumn) bool {
	// Check that both candidate sets have the same table keys.
	for t := range leftCandidates {
		if _, found := rightCandidates[t]; !found {
			return false
		}
	}
	for t := range rightCandidates {
		if _, found := leftCandidates[t]; !found {
			return false
		}
	}

	// Since the tables are the same, we can equivalently iterate over either map.
	for t := range leftCandidates {
		leftIndexes := leftCandidates[t]
		rightIndexes := rightCandidates[t]
		if len(leftIndexes) != len(rightIndexes) {
			return false
		}
		for i := range leftIndexes {
			leftIndex := leftIndexes[i]
			rightIndex := rightIndexes[i]
			if len(leftIndex) != len(rightIndex) {
				return false
			}
			for j := range leftIndex {
				if leftIndex[j] != rightIndex[j] {
					return false
				}
			}
		}
	}
	return true
}

func TestFindExistingPredicates(t *testing.T) {
	tables, _ := testTablesAndIndexColsWithPredicates()

	// Create metadata with the test tables.
	var md opt.Metadata
	for _, table := range tables {
		md.AddTable(table, &tree.TableName{})
	}

	result := findExistingPredicates(&md)

	// Expected results based on testTablesAndIndexColsWithPredicates setup:
	// table1 (tables[0]): has one index with predicate "k > 0" at ordinal 0.
	// table2 (tables[1]): has two indexes with predicates "k > 0" at ordinal 0 and "i = true AND j = false" at ordinal 1.
	// table3 (tables[2]): has no predicates.

	expectedPredicates := map[cat.Table][]string{
		tables[0]: {
			"k > 0",
		},
		tables[1]: {
			"k > 0",
			"i = true AND j = false",
		},
		// table3 should not appear in the result since it has no predicates.
	}

	// Check that we have the right number of tables.
	if len(result) != len(expectedPredicates) {
		t.Errorf("expected %d tables with predicates, got %d", len(expectedPredicates), len(result))
	}

	// Check each table's predicates
	for expectedTable, expectedPreds := range expectedPredicates {
		actualPreds, found := result[expectedTable]
		if !found {
			t.Errorf("expected table %v to have predicates, but it was not found", expectedTable)
			continue
		}

		if len(actualPreds) != len(expectedPreds) {
			t.Errorf("expected table %v to have %d predicates, got %d",
				expectedTable, len(expectedPreds), len(actualPreds))
			continue
		}

		for i, expectedPred := range expectedPreds {
			actualPred := actualPreds[i]
			if actualPred != expectedPred {
				t.Errorf("expected predicate %q at index %d for table %v, got %q",
					expectedPred, i, expectedTable, actualPred)
			}
		}
	}

	// Verify that table3 (tables[2]) is not in the result since it has no predicates.
	if _, found := result[tables[2]]; found {
		t.Errorf("expected table %v to not appear in result since it has no predicates", tables[2])
	}
}
