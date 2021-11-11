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
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
//     From these groups,construct the following multi-column index
//     combinations: EQ + R, J + R, J + EQ, J + EQ + R.
func FindIndexCandidateSet(rootExpr opt.Expr, md *opt.Metadata) map[cat.Table][][]cat.IndexColumn {
	candidateSet := indexCandidateSet{md: md}
	candidateSet.init(rootExpr)
	return candidateSet.overallCandidates
}

// FindIndexRecommendationSet finds index candidates that are used in an
// expression to determine a statement's index recommendation set.
func FindIndexRecommendationSet(expr opt.Expr, md *opt.Metadata) string {
	recommendationSet := indexRecommendationSet{md: md}
	recommendationSet.init(expr)
	return recommendationSet.getStringOutput()
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

// init allocates memory for the maps in the set. It also calls
// categorizeIndexCandidates, which walks the expression and places potential
// candidates in their respective maps. Finally, it calls
// synthesizeIndexCandidates, which adds index candidates that are combinations
// of candidates in these categories.
func (ics *indexCandidateSet) init(rootExpr opt.Expr) {
	ics.equalCandidates = make(map[cat.Table][][]cat.IndexColumn)
	ics.rangeCandidates = make(map[cat.Table][][]cat.IndexColumn)
	ics.joinCandidates = make(map[cat.Table][][]cat.IndexColumn)
	ics.overallCandidates = make(map[cat.Table][][]cat.IndexColumn)

	ics.categorizeIndexCandidates(rootExpr)
	ics.synthesizeIndexCandidates()
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
// TODO(neha): Formally test these functions.
func (ics *indexCandidateSet) categorizeIndexCandidates(expr opt.Expr) {
	switch expr := expr.(type) {
	case *memo.SortExpr:
		ics.addOrderingIndex(expr.ProvidedPhysical().Ordering)
	case *memo.GroupByExpr:
		addMultiColumnIndex(expr.GroupingCols.ToList(), nil, ics.md, ics.overallCandidates)
	case *memo.RangeExpr:
		ics.addRangeExprIndex(expr)
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

// addRangeExprIndex recursively walks a *memo.RangeExpr to find a
// *memo.VariableExpr, and adds this to rangeCandidates.
func (ics *indexCandidateSet) addRangeExprIndex(expr opt.Expr) {
	switch expr := expr.(type) {
	case *memo.VariableExpr:
		addVariableExprIndex(expr, ics.md, ics.rangeCandidates)
	}
	for i, n := 0, expr.ChildCount(); i < n; i++ {
		ics.addRangeExprIndex(expr.Child(i))
	}
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

// indexRecommendationSet stores the hypothetical indexes that are used in a
// statement's optimal plan (in usedIndexes), as well as the statement's
// metadata.
type indexRecommendationSet struct {
	md          *opt.Metadata
	usedIndexes map[cat.Table][]int
}

// init initializes an indexRecommendationSet, by allocating memory for it and
// calling synthesizeIndexRecommendations.
func (irs *indexRecommendationSet) init(expr opt.Expr) {
	irs.usedIndexes = make(map[cat.Table][]int)
	irs.synthesizeIndexRecommendations(expr)
}

// synthesizeIndexRecommendations recursively walks an expression tree to find
// hypothetical indexes that are used in it.
func (irs *indexRecommendationSet) synthesizeIndexRecommendations(expr opt.Expr) {
	switch expr := expr.(type) {
	case *memo.ScanExpr:
		irs.addIndexToRecommendationSet(expr.Index, expr.Table)
	case *memo.LookupJoinExpr:
		irs.addIndexToRecommendationSet(expr.Index, expr.Table)
	case *memo.InvertedJoinExpr:
		irs.addIndexToRecommendationSet(expr.Index, expr.Table)
	}
	for i, n := 0, expr.ChildCount(); i < n; i++ {
		irs.synthesizeIndexRecommendations(expr.Child(i))
	}
}

// addIndexToRecommendationSet adds an index to the indexes map if it does not
// exist already in the map and in the table.
func (irs *indexRecommendationSet) addIndexToRecommendationSet(
	indexOrd cat.IndexOrdinal, tabID opt.TableID,
) {
	hypTable := irs.md.TableMeta(tabID).Table.(*hypotheticalTable)
	// Do not add real table indexes (that are not hypothetical).
	if indexOrd < hypTable.Table.IndexCount() {
		return
	}
	for _, existingIndex := range irs.usedIndexes[hypTable] {
		if existingIndex == indexOrd {
			return
		}
	}
	irs.usedIndexes[hypTable] = append(irs.usedIndexes[hypTable], indexOrd)
}

// getStringOutput returns the string index recommendation output that will be
// displayed below the statement plan in EXPLAIN.
func (irs *indexRecommendationSet) getStringOutput() string {
	if len(irs.usedIndexes) == 0 {
		return ""
	}
	indexRecCount := 0
	for t := range irs.usedIndexes {
		indexRecCount += len(irs.usedIndexes[t])
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("\n\nIndex recommendations: %d\n\n", indexRecCount))
	indexRecOrd := 1
	for t, indexes := range irs.usedIndexes {
		for _, indexOrd := range indexes {
			sb.WriteString(fmt.Sprintf("%d. table: ", indexRecOrd))
			indexRecOrd++
			tableName := t.Name()
			sb.WriteString(tableName.String() + "\n")
			index := t.Index(indexOrd)
			sb.WriteString("   columns: ")
			indexCols := make([]string, index.ColumnCount())
			for i, n := 0, index.ColumnCount(); i < n; i++ {
				var colSb strings.Builder
				indexCol := index.Column(i)
				colName := indexCol.Column.ColName()
				colSb.WriteString(colName.String())
				sb.WriteString("[" + colName.String() + "] ")
				if indexCol.Descending {
					sb.WriteString("DESC")
					colSb.WriteString(" DESC")
				} else {
					sb.WriteString("ASC")
				}
				if i != (n - 1) {
					sb.WriteString(", ")
				} else {
					sb.WriteString("\n")
				}
				indexCols[i] = colSb.String()
			}
			sb.WriteString("   SQL command: ")
			sqlCmd := fmt.Sprintf("CREATE INDEX ON %s (%s);", tableName.String(), strings.Join(indexCols, ", "))
			sb.WriteString(sqlCmd + "\n\n")
		}
	}
	return sb.String()
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
	for i, col := range cols {
		if desc != nil {
			addSingleColumnIndex(col, desc[i], md, tableToCols)
		} else {
			addSingleColumnIndex(col, false /* desc */, md, tableToCols)
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
	col opt.ColumnID, desc bool, md *opt.Metadata, indexCandidates map[cat.Table][][]cat.IndexColumn,
) {
	// If the column is unknown, return.
	if col == 0 {
		return
	}
	columnMeta := md.ColumnMeta(col)

	// If there's no base table for the column, return.
	if columnMeta.Table == 0 {
		return
	}

	// Find the corresponding column instance in the current table.
	columnName := columnMeta.Alias
	currTable := md.Table(columnMeta.Table)
	var indexCol cat.IndexColumn
	for i, n := 0, currTable.ColumnCount(); i < n; i++ {
		tabCol := currTable.Column(i)
		if tabCol.ColName() == tree.Name(columnName) {
			indexCol = cat.IndexColumn{Column: tabCol, Descending: desc}
		}
	}

	if indexCol.Column != nil {
		addIndexToCandidates([]cat.IndexColumn{indexCol}, currTable, indexCandidates)
	}
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
