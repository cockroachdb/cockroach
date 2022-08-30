// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package indexrec

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
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

func testTablesAndIndexCols() ([]cat.Table, []cat.IndexColumn) {
	table1 := testcat.Table{TabID: 1}
	table2 := testcat.Table{TabID: 2}
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

// testIndexCols is a testing helper that returns an inverted index and a set of
// index columns. The index's key columns should be identical to the set of
// index columns.
func testIndexCols() (cat.Index, []cat.IndexColumn) {
	col1 := cat.Column{}
	col2 := cat.Column{}

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

	indexCol1 := cat.IndexColumn{Column: &col1, Descending: false}
	indexCol2 := cat.IndexColumn{Column: &col2, Descending: false}
	index := &testcat.Index{Columns: []cat.IndexColumn{indexCol1, indexCol2}, ExplicitColCount: 2}
	return index, []cat.IndexColumn{indexCol1, indexCol2}
}

// testStoredCols is a testing helper that returns ordinal of the columns stored
// by the index.
func testStoredCols() (cat.Index, util.FastIntSet) {
	col1 := cat.Column{}
	col2 := cat.Column{}

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

	indexCol1 := cat.IndexColumn{Column: &col1, Descending: false}
	indexCol2 := cat.IndexColumn{Column: &col2, Descending: false}
	// indexCol2 is stored in STORING clause.
	index := &testcat.Index{Columns: []cat.IndexColumn{indexCol1, indexCol2}, ExplicitColCount: 1}
	return index, util.MakeFastIntSet(1)
}

// testInvertedIndexCols is a testing helper that returns an inverted index and
// a set of index columns. If diffSourceCol is true, the inverted column has
// different source column. Otherwise, the index's key columns are identical to
// the set of index columns.
func testInvertedIndexCols(diffSourceCol bool) (cat.Index, []cat.IndexColumn) {
	col1 := cat.Column{}
	col2 := cat.Column{}
	col3 := cat.Column{}
	col4 := cat.Column{}

	col1.Init(0,
		1,
		"k",
		cat.Ordinary,
		types.Geometry,
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
		types.Geometry,
		false,
		cat.Visible,
		nil, /* defaultExpr */
		nil, /* computedExpr */
		nil, /* onUpdateExpr */
		cat.NotGeneratedAsIdentity,
		nil /* generatedAsIdentitySequenceOption */)
	col3.InitInverted(2,
		"inv",
		types.Bytes,
		false,
		1)
	col4.InitInverted(2,
		"inv",
		types.Bytes,
		false,
		0)

	indexCol1 := cat.IndexColumn{Column: &col1, Descending: false}
	indexCol2 := cat.IndexColumn{Column: &col2, Descending: false}
	indexCol3 := cat.IndexColumn{Column: &col3, Descending: false}
	indexCol4 := cat.IndexColumn{Column: &col4, Descending: false}
	sameInvertedIndex := &testcat.Index{Columns: []cat.IndexColumn{indexCol1, indexCol2, indexCol3}, ExplicitColCount: 3, Inverted: true}
	diffSourceIndex := &testcat.Index{Columns: []cat.IndexColumn{indexCol1, indexCol2, indexCol4}, ExplicitColCount: 3, Inverted: true}

	if diffSourceCol {
		return diffSourceIndex, []cat.IndexColumn{indexCol1, indexCol2, indexCol3}
	}
	return sameInvertedIndex, []cat.IndexColumn{indexCol1, indexCol2, indexCol3}
}

// testFindBestExistingIndexToReplace is a testing helper that returns a table
// with existing indexes, hypothetical indexes, scanned columns, and the
// expected best existing index to replace for index recommendation.
func testFindBestExistingIndexToReplace() (cat.Table, *hypotheticalIndex, util.FastIntSet, indexRecType, cat.Index, util.FastIntSet) {
	table := testcat.Table{TabID: 1}
	col1 := cat.Column{}
	col2 := cat.Column{}

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

	indexCol1 := cat.IndexColumn{Column: &col1, Descending: false}
	indexCol2 := cat.IndexColumn{Column: &col2, Descending: false}

	// Add existing indexes.
	table.Indexes = []*testcat.Index{
		// The first existing index stores indexCol1 (ordinal 0) as its key column.
		{Columns: []cat.IndexColumn{indexCol1}, ExplicitColCount: 1},
		// The second existing index stores indexCol1 as its key column and
		// indexCol2 in STORING clause which is closer to what is actually scanned
		// (ordinal 1 and 2), so it should be the best candidate. So this is the
		// best candidate.
		{Columns: []cat.IndexColumn{indexCol1, indexCol2}, ExplicitColCount: 1},
	}

	var hypIndex hypotheticalIndex
	hypTable := new(HypotheticalTable)
	hypTable.init(&table)
	hypIndex.init(hypTable, "hypIdx", []cat.IndexColumn{indexCol1}, 2, false, cat.EmptyZone())

	return &table, &hypIndex, util.MakeFastIntSet(1, 2), TypeReplaceIndex, table.Indexes[1], util.MakeFastIntSet(1)
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
