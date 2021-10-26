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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// FindIndexCandidates returns a map storing potential indexes for each table
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
//     - EQ is a single index of all columns that appear in equal predicates.
//     - R is all indexes that come from rule 2.
//     - J is all indexes that come from rules 3 and 4.
//     From these groups,construct the following multi-column index
//     combinations: EQ + R, J + R, J + EQ, J + EQ + R.
func FindIndexCandidates(
	f *norm.Factory, catalog cat.Catalog,
) (indexCandidates map[cat.Table][][]cat.IndexColumn) {
	equalCandidates := make(map[cat.Table][][]cat.IndexColumn)
	rangeCandidates := make(map[cat.Table][][]cat.IndexColumn)
	joinCandidates := make(map[cat.Table][][]cat.IndexColumn)
	indexCandidates = make(map[cat.Table][][]cat.IndexColumn)

	walkOptExprIndexCandidates(
		f.Memo().RootExpr(),
		f.Metadata(),
		catalog,
		equalCandidates,
		rangeCandidates,
		joinCandidates,
		indexCandidates,
	)

	// Copy indexes in each category to indexCandidates without duplicates.
	copyIndexes(equalCandidates, indexCandidates)
	copyIndexes(rangeCandidates, indexCandidates)
	copyIndexes(joinCandidates, indexCandidates)

	joinEqualCandidates := make(map[cat.Table][][]cat.IndexColumn)
	equalGroupedCandidates := make(map[cat.Table][][]cat.IndexColumn)

	// Create a single index for equal columns for each table. As described above,
	// construct EQ + R, J + R, J + EQ, J + EQ + R.
	groupIndexesByTable(equalCandidates, equalGroupedCandidates)
	constructIndexCombinations(equalGroupedCandidates, rangeCandidates, indexCandidates)
	constructIndexCombinations(joinCandidates, rangeCandidates, indexCandidates)
	constructIndexCombinations(joinCandidates, equalGroupedCandidates, joinEqualCandidates)
	copyIndexes(joinEqualCandidates, indexCandidates)
	constructIndexCombinations(joinEqualCandidates, rangeCandidates, indexCandidates)

	return indexCandidates
}

// walkOptExprIndexCandidates finds potential index candidates for a given
// query. See FindIndexCandidates for the list of candidate creation rules.
//
// TODO(neha): Add information about potential STORING columns to add for
// 	indexes where adding them could avoid index-joins.
// TODO(neha): Formally test these functions.
func walkOptExprIndexCandidates(
	expr opt.Expr,
	metadata *opt.Metadata,
	catalog cat.Catalog,
	equalCandidates, rangeCandidates, joinCandidates, indexCandidates map[cat.Table][][]cat.IndexColumn,
) {
	switch expr := expr.(type) {
	case *memo.SortExpr:
		addOrderingIndex(expr.ProvidedPhysical().Ordering, metadata, catalog, indexCandidates)
	case *memo.GroupByExpr:
		addMultiColumnIndex(expr.GroupingCols.ToList(), nil, metadata, catalog, indexCandidates)
	case *memo.RangeExpr:
		exprAnd := expr.And.(*memo.AndExpr)
		addVariableExprIndex(exprAnd.Left, metadata, catalog, rangeCandidates)
		addVariableExprIndex(exprAnd.Right, metadata, catalog, rangeCandidates)
	case *memo.EqExpr:
		addVariableExprIndex(expr.Left, metadata, catalog, equalCandidates)
		addVariableExprIndex(expr.Right, metadata, catalog, equalCandidates)
	case *memo.LtExpr:
		addVariableExprIndex(expr.Left, metadata, catalog, rangeCandidates)
		addVariableExprIndex(expr.Right, metadata, catalog, rangeCandidates)
	case *memo.GtExpr:
		addVariableExprIndex(expr.Left, metadata, catalog, rangeCandidates)
		addVariableExprIndex(expr.Right, metadata, catalog, rangeCandidates)
	case *memo.LeExpr:
		addVariableExprIndex(expr.Left, metadata, catalog, rangeCandidates)
		addVariableExprIndex(expr.Right, metadata, catalog, rangeCandidates)
	case *memo.GeExpr:
		addVariableExprIndex(expr.Left, metadata, catalog, rangeCandidates)
		addVariableExprIndex(expr.Right, metadata, catalog, rangeCandidates)
	case *memo.InnerJoinExpr:
		addFiltersExprIndex(expr.On, metadata, catalog, joinCandidates)
		joinCols := expr.On.OuterCols().ToList()
		addMultiColumnIndex(joinCols, nil /* desc */, metadata, catalog, joinCandidates)
	case *memo.LeftJoinExpr:
		addFiltersExprIndex(expr.On, metadata, catalog, joinCandidates)
		joinCols := expr.On.OuterCols().ToList()
		addMultiColumnIndex(joinCols, nil /* desc */, metadata, catalog, joinCandidates)
	case *memo.RightJoinExpr:
		addFiltersExprIndex(expr.On, metadata, catalog, joinCandidates)
		joinCols := expr.On.OuterCols().ToList()
		addMultiColumnIndex(joinCols, nil /* desc */, metadata, catalog, joinCandidates)
	case *memo.FullJoinExpr:
		addFiltersExprIndex(expr.On, metadata, catalog, joinCandidates)
		joinCols := expr.On.OuterCols().ToList()
		addMultiColumnIndex(joinCols, nil /* desc */, metadata, catalog, joinCandidates)
	case *memo.SemiJoinExpr:
		addFiltersExprIndex(expr.On, metadata, catalog, joinCandidates)
		joinCols := expr.On.OuterCols().ToList()
		addMultiColumnIndex(joinCols, nil /* desc */, metadata, catalog, joinCandidates)
	case *memo.AntiJoinExpr:
		addFiltersExprIndex(expr.On, metadata, catalog, joinCandidates)
		joinCols := expr.On.OuterCols().ToList()
		addMultiColumnIndex(joinCols, nil /* desc */, metadata, catalog, joinCandidates)
	}
	for i, n := 0, expr.ChildCount(); i < n; i++ {
		walkOptExprIndexCandidates(
			expr.Child(i),
			metadata,
			catalog,
			equalCandidates,
			rangeCandidates,
			joinCandidates,
			indexCandidates,
		)
	}
}

// WalkOptExprIndexesUsed finds indexes that are used in an expression.
func WalkOptExprIndexesUsed(expr opt.Expr, metadata *opt.Metadata, indexes map[cat.StableID][]int) {
	switch expr := expr.(type) {
	case *memo.ScanExpr:
		addIndexToOutputSet(expr.Index, expr.Table, metadata, indexes)
	case *memo.LookupJoinExpr:
		addIndexToOutputSet(expr.Index, expr.Table, metadata, indexes)
	case *memo.InvertedJoinExpr:
		addIndexToOutputSet(expr.Index, expr.Table, metadata, indexes)
	}
	for i, n := 0, expr.ChildCount(); i < n; i++ {
		WalkOptExprIndexesUsed(expr.Child(i), metadata, indexes)
	}
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
		var newIndex []cat.IndexColumn
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
		rightIndexes := rightIndexMap[t]
		if rightIndexes == nil {
			return
		}

		for _, leftIndex := range leftIndexes {
			constructLeftIndexCombination(leftIndex, t, rightIndexes, outputIndexes)
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
	leftIndexColMap := make(map[cat.IndexColumn]bool)
	// Store left columns in map for fast access.
	for _, leftCol := range leftIndex {
		leftIndexColMap[leftCol] = true
	}
	for _, rightIndex := range rightIndexes {
		// Remove columns in the right index that exist in the left index.
		var updatedRightIndex []cat.IndexColumn
		for _, rightCol := range rightIndex {
			if !leftIndexColMap[rightCol] {
				updatedRightIndex = append(updatedRightIndex, rightCol)
			}
		}
		addIndexToCandidates(append(leftIndex, updatedRightIndex...), tab, outputIndexes)
	}
}

// addIndexToOutputSet adds an index to the indexes map if it does not exist
// already.
//
// TODO(neha): Don't add indexes to recommendation if they already exist in the
//   table.
func addIndexToOutputSet(
	index cat.IndexOrdinal, tabID opt.TableID, metadata *opt.Metadata, indexes map[cat.StableID][]int,
) {
	tabStableID := metadata.TableMeta(tabID).Table.ID()
	for _, existingIndex := range indexes[tabStableID] {
		if existingIndex == index {
			return
		}
	}
	indexes[tabStableID] = append(indexes[tabStableID], index)
}

// addOrderingIndex adds indexes for a *memo.SortExpr. One index is constructed
// per table, with a column corresponding to each of the table's columns in the
// sort, in order of appearance. The first column of each table's index will be
// ordered ascending. If that is the opposite of the column's ordering in the
// sort, each subsequent column will also be ordered opposite to its ordering
// (and vice versa).
func addOrderingIndex(
	ordering opt.Ordering,
	metadata *opt.Metadata,
	catalog cat.Catalog,
	indexCandidates map[cat.Table][][]cat.IndexColumn,
) {
	if len(ordering) < 1 {
		return
	}

	columnList := make(opt.ColList, len(ordering))
	descList := make([]bool, len(ordering))
	reverseOrder := make(map[cat.Table]bool)

	for i, orderingCol := range ordering {
		colID := orderingCol.ID()
		columnList[i] = colID
		colTable := metadata.Table(metadata.ColumnMeta(colID).Table)

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

	addMultiColumnIndex(columnList, descList, metadata, catalog, indexCandidates)
}

// addVariableExprIndex adds an index candidate to indexCandidates if the expr
// argument can be cast to a *memo.VariableExpr and the index does not already
// exist.
func addVariableExprIndex(
	expr opt.Expr,
	metadata *opt.Metadata,
	catalog cat.Catalog,
	indexCandidates map[cat.Table][][]cat.IndexColumn,
) {
	switch expr := expr.(type) {
	case *memo.VariableExpr:
		addSingleColumnIndex(expr.Col, false /* desc */, metadata, catalog, indexCandidates)
	}
}

// addFiltersExprIndex adds single-column indexes to indexCandidates for each
// outer column in a *memo.FiltersExpr, if these indexes do not already exist.
func addFiltersExprIndex(
	expr memo.FiltersExpr,
	metadata *opt.Metadata,
	catalog cat.Catalog,
	indexCandidates map[cat.Table][][]cat.IndexColumn,
) {
	for _, col := range expr.OuterCols().ToList() {
		addSingleColumnIndex(col, false /* desc */, metadata, catalog, indexCandidates)
	}
}

// addMultiColumnIndex adds indexes to indexCandidates for groups of columns
// in a column set that are from the same table, without duplicates.
func addMultiColumnIndex(
	cols opt.ColList,
	desc []bool,
	metadata *opt.Metadata,
	catalog cat.Catalog,
	indexCandidates map[cat.Table][][]cat.IndexColumn,
) {
	// Group columns by table in a temporary map as single-column indexes,
	// getting rid of duplicates.
	tableToCols := make(map[cat.Table][][]cat.IndexColumn)
	for i, col := range cols {
		if desc != nil {
			addSingleColumnIndex(col, desc[i], metadata, catalog, tableToCols)
		} else {
			addSingleColumnIndex(col, false /* desc */, metadata, catalog, tableToCols)
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
	col opt.ColumnID,
	desc bool,
	metadata *opt.Metadata,
	catalog cat.Catalog,
	indexCandidates map[cat.Table][][]cat.IndexColumn,
) {
	// If the column is unknown, return.
	if col == 0 {
		return
	}
	columnMeta := metadata.ColumnMeta(col)

	// If there's no base table for the column, return.
	if columnMeta.Table == 0 {
		return
	}

	// Find the corresponding column instance in the current table.
	columnName := columnMeta.Alias
	currTable := metadata.Table(columnMeta.Table)
	var indexCol cat.IndexColumn
	for i := 0; i < currTable.ColumnCount(); i++ {
		tabCol := currTable.Column(i)
		if tabCol.ColName() == tree.Name(columnName) {
			indexCol = cat.IndexColumn{Column: tabCol, Descending: desc}
		}
	}

	addIndexToCandidates([]cat.IndexColumn{indexCol}, currTable, indexCandidates)
}

// addIndexToCandidates adds an index to indexCandidates if it does not already
// exist.
func addIndexToCandidates(
	newIndex []cat.IndexColumn,
	currTable cat.Table,
	indexCandidates map[cat.Table][][]cat.IndexColumn,
) {
	// Do not add an index if it is equivalent to the primary index.
	isPrimaryIdx := true
	primIndex := currTable.Index(cat.PrimaryIndex)
	for i := 0; i < primIndex.KeyColumnCount(); i++ {
		if len(newIndex) != primIndex.KeyColumnCount() {
			isPrimaryIdx = false
			break
		} else if primIndex.Column(i) != newIndex[i] {
			isPrimaryIdx = false
		}
	}
	if isPrimaryIdx {
		return
	}

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
