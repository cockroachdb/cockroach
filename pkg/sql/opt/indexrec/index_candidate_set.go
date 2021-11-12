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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// FindIndexCandidateSet returns a map storing potential indexes for each table
// referenced in a query.
//
// 	1. Add a single index on all columns in a Group By or Order By expression if
//	   the columns are from the same table. Otherwise, group expressions into
//	   indexes by table. For Order By, the first column of each index will be
//     ascending. If that is the opposite of the column's ordering, each
//     subsequent column will also be ordered opposite to its ordering (and vice
//     versa).
//  2. Add a single-column index on any Range expression or comparison
//     expression (=, <, >, <=, >=).
// 	3. Add a single-column index on any column that appears in a JOIN predicate.
//  4. If there exist multiple columns from the same table in a JOIN predicate,
//     create a single index on all such columns.
//  5. Construct three groups for each table: EQ, R, and J.
//     - EQ is a single index of all columns that appear in equality predicates.
//     - R is all indexes that come from rule 2.
//     - J is all indexes that come from rules 3 and 4.
//     From these groups, construct the following multi-column index
//     combinations: EQ + R, J + R, J + EQ, J + EQ + R.
func FindIndexCandidateSet(rootExpr opt.Expr, md *opt.Metadata) map[cat.Table][][]cat.IndexColumn {
	candidateSet := indexCandidateSet{md: md}
	candidateSet.init()
	candidateSet.categorizeIndexCandidates(rootExpr)
	candidateSet.synthesizeIndexCandidates()
	return candidateSet.overallCandidates
}

// indexCandidateSet stores potential indexes that could be recommended for a
// given query, as well as the query's metadata.
type indexCandidateSet struct {
	md                *opt.Metadata
	equalCandidates   map[cat.Table][][]cat.IndexColumn
	rangeCandidates   map[cat.Table][][]cat.IndexColumn
	joinCandidates    map[cat.Table][][]cat.IndexColumn
	overallCandidates map[cat.Table][][]cat.IndexColumn
}

// init allocates memory for the maps in the set.
func (ics *indexCandidateSet) init() {
	ics.equalCandidates = make(map[cat.Table][][]cat.IndexColumn)
	ics.rangeCandidates = make(map[cat.Table][][]cat.IndexColumn)
	ics.joinCandidates = make(map[cat.Table][][]cat.IndexColumn)
	ics.overallCandidates = make(map[cat.Table][][]cat.IndexColumn)
}

// synthesizeIndexCandidates adds index candidates that are combinations of
// candidates in the JOIN, EQUAL, and RANGE categories. See rule 5 in
// FindIndexCandidateSet.
func (ics *indexCandidateSet) synthesizeIndexCandidates() {
	// Copy indexes in each category to overallCandidates without duplicates.
	copyIndexes(ics.equalCandidates, ics.overallCandidates)
	copyIndexes(ics.rangeCandidates, ics.overallCandidates)
	copyIndexes(ics.joinCandidates, ics.overallCandidates)

	joinEqualCandidates := make(map[cat.Table][][]cat.IndexColumn)
	equalGroupedCandidates := make(map[cat.Table][][]cat.IndexColumn)

	// Construct EQ + R, J + R, J + EQ, J + EQ + R.
	groupIndexesByTable(ics.equalCandidates, equalGroupedCandidates)
	constructIndexCombinations(equalGroupedCandidates, ics.rangeCandidates, ics.overallCandidates)
	constructIndexCombinations(ics.joinCandidates, ics.rangeCandidates, ics.overallCandidates)
	constructIndexCombinations(ics.joinCandidates, equalGroupedCandidates, joinEqualCandidates)
	copyIndexes(joinEqualCandidates, ics.overallCandidates)
	constructIndexCombinations(joinEqualCandidates, ics.rangeCandidates, ics.overallCandidates)
}

// categorizeIndexCandidates finds potential index candidates for a given
// query. See FindIndexCandidateSet for the list of candidate creation rules.
//
// TODO(neha): Add information about potential STORING columns to add for
// 	indexes where adding them could avoid index-joins.
func (ics *indexCandidateSet) categorizeIndexCandidates(expr opt.Expr) {
	switch expr := expr.(type) {
	case *memo.SortExpr:
		ics.addOrderingIndex(expr.ProvidedPhysical().Ordering)
	case *memo.GroupByExpr:
		addMultiColumnIndex(expr.GroupingCols.ToList(), nil, ics.md, ics.overallCandidates)
	case *memo.RangeExpr:
		addVariableExprIndexRecursive(expr, ics.md, ics.rangeCandidates)
	case *memo.EqExpr:
		addVariableExprIndex(expr.Left, ics.md, ics.equalCandidates)
		addVariableExprIndex(expr.Right, ics.md, ics.equalCandidates)
	case *memo.LtExpr:
		addVariableExprIndex(expr.Left, ics.md, ics.rangeCandidates)
		addVariableExprIndex(expr.Right, ics.md, ics.rangeCandidates)
	case *memo.GtExpr:
		addVariableExprIndex(expr.Left, ics.md, ics.rangeCandidates)
		addVariableExprIndex(expr.Right, ics.md, ics.rangeCandidates)
	case *memo.LeExpr:
		addVariableExprIndex(expr.Left, ics.md, ics.rangeCandidates)
		addVariableExprIndex(expr.Right, ics.md, ics.rangeCandidates)
	case *memo.GeExpr:
		addVariableExprIndex(expr.Left, ics.md, ics.rangeCandidates)
		addVariableExprIndex(expr.Right, ics.md, ics.rangeCandidates)
	case *memo.InnerJoinExpr:
		ics.addJoinIndexes(expr.On)
	case *memo.LeftJoinExpr:
		ics.addJoinIndexes(expr.On)
	case *memo.RightJoinExpr:
		ics.addJoinIndexes(expr.On)
	case *memo.FullJoinExpr:
		ics.addJoinIndexes(expr.On)
	case *memo.SemiJoinExpr:
		ics.addJoinIndexes(expr.On)
	case *memo.AntiJoinExpr:
		ics.addJoinIndexes(expr.On)
	}
	for i, n := 0, expr.ChildCount(); i < n; i++ {
		ics.categorizeIndexCandidates(expr.Child(i))
	}
}

// addOrderingIndex adds indexes for a *memo.SortExpr. One index is constructed
// per table, with a column corresponding to each of the table's columns in the
// sort, in order of appearance. The first column of each table's index will be
// ordered ascending. If that is the opposite of the column's ordering in the
// sort, each subsequent column will also be ordered opposite to its ordering
// (and vice versa).
//
// TODO(neha): The convention of having the first column being ascending is to
//   avoid redundant indexes. However, since reverse scans are slightly less
//   efficient than forward scans, we shouldn't have this convention and should
//   remove redundant indexes later.
func (ics indexCandidateSet) addOrderingIndex(ordering opt.Ordering) {
	if len(ordering) == 0 {
		return
	}
	columnList := make(opt.ColList, len(ordering))
	descList := make([]bool, len(ordering))
	reverseOrder := make(map[cat.Table]bool)

	for i, orderingCol := range ordering {
		colID := orderingCol.ID()
		columnList[i] = colID
		colTable := ics.md.Table(ics.md.ColumnMeta(colID).Table)

		// Set descending bool for ordering column.
		if _, found := reverseOrder[colTable]; !found {
			reverseOrder[colTable] = orderingCol.Descending()
		}
		if reverseOrder[colTable] {
			descList[i] = orderingCol.Ascending()
		} else {
			descList[i] = orderingCol.Descending()
		}
	}

	addMultiColumnIndex(columnList, descList, ics.md, ics.overallCandidates)
}

// addJoinIndexes adds single-column indexes to joinCandidates for each outer
// column in a join predicate, if these indexes do not already exist. For each
// table with multiple columns in the JOIN predicate, it also creates a single
// index on all such columns.
func (ics *indexCandidateSet) addJoinIndexes(expr memo.FiltersExpr) {
	for _, col := range expr.OuterCols().ToList() {
		addSingleColumnIndex(col, false /* desc */, ics.md, ics.joinCandidates)
	}
	addMultiColumnIndex(expr.OuterCols().ToList(), nil /* desc */, ics.md, ics.joinCandidates)
}

// copyIndexes copies indexes from one map to another, getting rid of duplicates
// in the output map.
func copyIndexes(inputIndexMap, outputIndexMap map[cat.Table][][]cat.IndexColumn) {
	for t, indexes := range inputIndexMap {
		for _, index := range indexes {
			addIndexToCandidates(index, t, outputIndexMap)
		}
	}
}

// groupIndexesByTable creates a single multi-column index from all indexes in a
// table. It is the caller's responsibility to ensure that there are no
// duplicate columns between indexes on a given table.
func groupIndexesByTable(inputIndexMap, outputIndexMap map[cat.Table][][]cat.IndexColumn) {
	for t, indexes := range inputIndexMap {
		newIndex := make([]cat.IndexColumn, 0, len(indexes))
		for _, index := range indexes {
			newIndex = append(newIndex, index...)
		}
		outputIndexMap[t] = [][]cat.IndexColumn{newIndex}
	}
}

// constructIndexCombinations constructs all concatenated index combinations
// from indexes in the left map and indexes in the right map, with the left
// index always coming first. This is done for every table where at least one
// index exists in both maps. If there are columns in the right index that
// already exist in the left index, we discard them.
func constructIndexCombinations(
	leftIndexMap, rightIndexMap, outputIndexes map[cat.Table][][]cat.IndexColumn,
) {
	for t, leftIndexes := range leftIndexMap {
		if rightIndexes, found := rightIndexMap[t]; !found {
			continue
		} else {
			for _, leftIndex := range leftIndexes {
				constructLeftIndexCombination(leftIndex, t, rightIndexes, outputIndexes)
			}
		}
	}
}

// constructLeftIndexCombination adds all valid concatenated index combinations
// for a single left index and a slice of right indexes, with the left index
// appearing first.
func constructLeftIndexCombination(
	leftIndex []cat.IndexColumn,
	tab cat.Table,
	rightIndexes [][]cat.IndexColumn,
	outputIndexes map[cat.Table][][]cat.IndexColumn,
) {
	var leftIndexColSet util.FastIntSet
	// Store left columns in map for fast access.
	for _, leftCol := range leftIndex {
		leftIndexColSet.Add(int(leftCol.ColID()))
	}
	for _, rightIndex := range rightIndexes {
		// Remove columns in the right index that exist in the left index.
		updatedRightIndex := make([]cat.IndexColumn, 0, len(rightIndex))
		for _, rightCol := range rightIndex {
			if !leftIndexColSet.Contains(int(rightCol.ColID())) {
				updatedRightIndex = append(updatedRightIndex, rightCol)
			}
		}
		if len(updatedRightIndex) > 0 {
			addIndexToCandidates(append(leftIndex, updatedRightIndex...), tab, outputIndexes)
		}
	}
}

// addVariableExprIndexRecursive recursively walks an opt.Expr to find any
// *memo.VariableExpr, and adds these to indexCandidates.
func addVariableExprIndexRecursive(
	expr opt.Expr, md *opt.Metadata, indexCandidates map[cat.Table][][]cat.IndexColumn,
) {
	switch expr := expr.(type) {
	case *memo.VariableExpr:
		addVariableExprIndex(expr, md, indexCandidates)
	}
	for i, n := 0, expr.ChildCount(); i < n; i++ {
		addVariableExprIndexRecursive(expr.Child(i), md, indexCandidates)
	}
}

// addVariableExprIndex adds an index candidate to indexCandidates if the expr
// argument can be cast to a *memo.VariableExpr and the index does not already
// exist.
func addVariableExprIndex(
	expr opt.Expr, md *opt.Metadata, indexCandidates map[cat.Table][][]cat.IndexColumn,
) {
	switch expr := expr.(type) {
	case *memo.VariableExpr:
		addSingleColumnIndex(expr.Col, false /* desc */, md, indexCandidates)
	}
}

// addMultiColumnIndex adds indexes to indexCandidates for groups of columns
// in a column set that are from the same table, without duplicates.
func addMultiColumnIndex(
	cols opt.ColList,
	desc []bool,
	md *opt.Metadata,
	indexCandidates map[cat.Table][][]cat.IndexColumn,
) {
	// Group columns by table in a temporary map as single-column indexes,
	// getting rid of duplicates.
	tableToCols := make(map[cat.Table][][]cat.IndexColumn)
	for i, colID := range cols {
		if desc != nil {
			addSingleColumnIndex(colID, desc[i], md, tableToCols)
		} else {
			addSingleColumnIndex(colID, false /* desc */, md, tableToCols)
		}
	}

	// Combine all single-column indexes for a given table into one, and add
	// the corresponding multi-column index.
	for currTable := range tableToCols {
		index := make([]cat.IndexColumn, len(tableToCols[currTable]))
		for i, colSlice := range tableToCols[currTable] {
			index[i] = colSlice[0]
		}
		addIndexToCandidates(index, currTable, indexCandidates)
	}
}

// addSingleColumnIndex adds an index to indexCandidates on the column with the
// given opt.ColumnID if it does not already exist.
func addSingleColumnIndex(
	colID opt.ColumnID, desc bool, md *opt.Metadata, indexCandidates map[cat.Table][][]cat.IndexColumn,
) {
	// If the column is unknown, return.
	if colID == 0 {
		return
	}
	columnMeta := md.ColumnMeta(colID)

	// If there's no base table for the column, return.
	tableID := columnMeta.Table
	if tableID == 0 {
		return
	}

	// Find the column instance in the current table and add the corresponding
	// index to indexCandidates.
	currTable := md.Table(tableID)
	currCol := currTable.Column(tableID.ColumnOrdinal(colID))
	indexCol := cat.IndexColumn{Column: currCol, Descending: desc}
	addIndexToCandidates([]cat.IndexColumn{indexCol}, currTable, indexCandidates)
}

// addIndexToCandidates adds an index to indexCandidates if it does not already
// exist.
func addIndexToCandidates(
	newIndex []cat.IndexColumn,
	currTable cat.Table,
	indexCandidates map[cat.Table][][]cat.IndexColumn,
) {
	// Do not add duplicate indexes.
	for _, existingIndex := range indexCandidates[currTable] {
		if len(existingIndex) != len(newIndex) {
			continue
		}
		potentialDuplicate := true
		for i := range existingIndex {
			if newIndex[i] != existingIndex[i] {
				potentialDuplicate = false
				break
			}
		}
		if potentialDuplicate {
			// Duplicate index found, return.
			return
		}
	}
	// Index does not exist already, so add it.
	indexCandidates[currTable] = append(indexCandidates[currTable], newIndex)
}
