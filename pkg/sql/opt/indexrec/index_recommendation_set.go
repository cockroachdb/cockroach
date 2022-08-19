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
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// FindIndexRecommendationSet finds index candidates that are scanned in an
// expression to determine a statement's index recommendation set.
//
// Note that because the index recommendation engine stores all table columns in
// its hypothetical indexes when optimizing, and then prunes the stored columns
// after, this may result in the best plan not being chosen. Meaning, a plan
// might not be chosen because the optimizer includes the overhead of scanning
// stored columns of an index, which results in plans with hypothetical indexes
// being more expensive and reduces their likelihood of being chosen. This is a
// tradeoff we accept in order to recommend STORING columns, as the difference
// in plan costs is not very significant.
func FindIndexRecommendationSet(expr opt.Expr, md *opt.Metadata) IndexRecommendationSet {
	var recommendationSet IndexRecommendationSet
	recommendationSet.init(md)
	recommendationSet.createIndexRecommendations(expr)
	recommendationSet.pruneIndexRecommendations()
	return recommendationSet
}

// IndexRecommendationSet stores the hypothetical indexes that are scanned in a
// statement's optimal plan (in indexRecs), as well as the statement's metadata.
type IndexRecommendationSet struct {
	md        *opt.Metadata
	indexRecs map[cat.Table][]indexRecommendation
}

// init initializes an IndexRecommendationSet by allocating memory for it.
func (irs *IndexRecommendationSet) init(md *opt.Metadata) {
	numTables := len(md.AllTables())
	irs.md = md
	irs.indexRecs = make(map[cat.Table][]indexRecommendation, numTables)
}

// createIndexRecommendations recursively walks an expression tree to find
// hypothetical indexes that are scanned in it.
func (irs *IndexRecommendationSet) createIndexRecommendations(expr opt.Expr) {
	switch expr := expr.(type) {
	case *memo.ScanExpr:
		irs.addIndexToRecommendationSet(expr.Index, expr.Cols, expr.Table)
	case *memo.LookupJoinExpr:
		irs.addIndexToRecommendationSet(expr.Index, expr.Cols, expr.Table)
	case *memo.InvertedJoinExpr:
		irs.addIndexToRecommendationSet(expr.Index, expr.Cols, expr.Table)
	case *memo.ZigzagJoinExpr:
		irs.addIndexToRecommendationSet(expr.LeftIndex, expr.Cols, expr.LeftTable)
		irs.addIndexToRecommendationSet(expr.RightIndex, expr.Cols, expr.RightTable)
	}
	for i, n := 0, expr.ChildCount(); i < n; i++ {
		irs.createIndexRecommendations(expr.Child(i))
	}
}

// pruneIndexRecommendations removes redundant index recommendations from the
// recommendation set. These are recommendations where the existingIndex field
// is not nil and no new columns are being stored.
func (irs *IndexRecommendationSet) pruneIndexRecommendations() {
	for t, indexRecs := range irs.indexRecs {
		updatedIndexRecs := make([]indexRecommendation, 0, len(indexRecs))
		for _, indexRec := range indexRecs {
			if indexRec.existingIndex == nil || !indexRec.redundantRecommendation() {
				updatedIndexRecs = append(updatedIndexRecs, indexRec)
			}
		}
		irs.indexRecs[t] = updatedIndexRecs
	}
}

// getColOrdSet returns the set of column ordinals within the given table that
// are contained in cols.
func (irs *IndexRecommendationSet) getColOrdSet(
	cols opt.ColSet, tabID opt.TableID,
) util.FastIntSet {
	var colsOrdSet util.FastIntSet
	cols.ForEach(func(col opt.ColumnID) {
		table := irs.md.ColumnMeta(col).Table
		// Do not add columns from other tables.
		if table != tabID {
			return
		}
		colOrd := table.ColumnOrdinal(col)
		colsOrdSet.Add(colOrd)
	})
	return colsOrdSet
}

// addIndexToRecommendationSet adds an index to the indexes map if it does not
// exist already in the map and in the table. The scannedCols argument contains
// the columns of the index that are actually scanned, used to determine which
// columns should be stored columns in the index recommendation.
func (irs *IndexRecommendationSet) addIndexToRecommendationSet(
	indexOrd cat.IndexOrdinal, scannedCols opt.ColSet, tabID opt.TableID,
) {
	// Do not add real table indexes (non-hypothetical table indexes).
	switch hypTable := irs.md.TableMeta(tabID).Table.(type) {
	case *HypotheticalTable:
		// Do not recommend existing indexes.
		if indexOrd < hypTable.Table.IndexCount() {
			return
		}
		scannedColOrds := irs.getColOrdSet(scannedCols, tabID)
		// Try to find an identical existing index recommendation.
		for _, indexRec := range irs.indexRecs[hypTable] {
			index := indexRec.index
			if index.indexOrdinal == indexOrd {
				// Update indexRec.newStoredColOrds to include all stored column
				// ordinals that are in scannedColOrds.
				indexRec.addStoredColOrds(scannedColOrds)
				return
			}
		}
		// Create a new index recommendation with pruned stored columns. Only store
		// columns that are in scannedColOrds.
		var newIndexRec indexRecommendation
		newIndexRec.init(indexOrd, hypTable, scannedColOrds)
		irs.indexRecs[hypTable] = append(irs.indexRecs[hypTable], newIndexRec)
	}
}

// Output returns a string slice of index recommendation output that will be
// displayed below the statement plan in EXPLAIN.
func (irs *IndexRecommendationSet) Output() []string {
	indexRecCount := 0
	for t := range irs.indexRecs {
		indexRecCount += len(irs.indexRecs[t])
	}
	if indexRecCount == 0 {
		return nil
	}

	outputRowCount := 2*len(irs.indexRecs) + 1
	output := make([]string, 0, outputRowCount)
	output = append(output, fmt.Sprintf("index recommendations: %d", indexRecCount))

	sortedTables := make([]cat.Table, 0, len(irs.indexRecs))
	for t := range irs.indexRecs {
		sortedTables = append(sortedTables, t)
	}
	sort.Slice(sortedTables, func(i, j int) bool {
		return sortedTables[i].Name() < sortedTables[j].Name()
	})

	indexRecOrd := 1
	for _, t := range sortedTables {
		indexes := irs.indexRecs[t]
		for _, indexRec := range indexes {
			recTypeStr := "index creation"
			if indexRec.existingIndex != nil {
				recTypeStr = "index replacement"
			}
			indexCols := indexRec.indexCols()
			storing := indexRec.storingColumns()

			indexRecType := fmt.Sprintf("%d. type: %s", indexRecOrd, recTypeStr)
			indexRecSQL := indexRec.indexRecommendationString(indexCols, storing)
			output = append(output, indexRecType, indexRecSQL)
			indexRecOrd++
		}
	}
	return output
}

// indexRecommendation stores the information pertaining to a single index
// recommendation.
type indexRecommendation struct {
	// index stores the hypotheticalIndex that is being recommended.
	index *hypotheticalIndex

	// existingIndex stores an existing index with the same explicit columns, if
	// one exists.
	existingIndex cat.Index

	// newStoredColOrds stores the stored column ordinals that are scanned by the
	// optimizer in the expression tree passed to FindIndexRecommendationSet.
	newStoredColOrds util.FastIntSet

	// existingStoredColOrds stores the column ordinals of the existingIndex's
	// stored columns. It is empty if there is no existingIndex.
	existingStoredColOrds util.FastIntSet
}

// init initializes an index recommendation. If there is an existingIndex with
// the same explicit key columns, it is stored here.
func (ir *indexRecommendation) init(
	indexOrd int, hypTable *HypotheticalTable, scannedColOrds util.FastIntSet,
) {
	index := hypTable.Index(indexOrd).(*hypotheticalIndex)
	ir.index = index
	ir.existingIndex = hypTable.existingRedundantIndex(index)

	// Only store columns useful to the statement plan, found in scannedColOrds.
	for i := range index.storedCols {
		colOrd := index.storedCols[i].Column.Ordinal()
		if scannedColOrds.Contains(colOrd) {
			ir.newStoredColOrds.Add(colOrd)
		}
	}

	// If there is no existing index, return.
	if ir.existingIndex == nil {
		return
	}

	// Iterate through the stored columns of the existing index to set
	// existingStoredColOrds.
	var existingStoredOrds util.FastIntSet
	for i, n := ir.existingIndex.KeyColumnCount(), ir.existingIndex.ColumnCount(); i < n; i++ {
		existingStoredOrds.Add(ir.existingIndex.Column(i).Ordinal())
	}
	ir.existingStoredColOrds = existingStoredOrds

	// Update newStoredColOrds to contain the existingStoredColOrds. We want to
	// include all existing stored columns in the potential replacement
	// recommendation.
	ir.newStoredColOrds.UnionWith(ir.existingStoredColOrds)
}

// redundantRecommendation compares newStoredColOrds with the existing index's
// stored columns. It returns true if there are no new columns being stored in
// newStoredColOrds.
func (ir *indexRecommendation) redundantRecommendation() bool {
	return ir.newStoredColOrds.Difference(ir.existingStoredColOrds).Empty()
}

// addStoredColOrds updates an index recommendation's newStoredColOrds field to
// also contain the scannedColOrds columns.
func (ir *indexRecommendation) addStoredColOrds(scannedColOrds util.FastIntSet) {
	for i := range ir.index.storedCols {
		colOrd := ir.index.storedCols[i].Column.Ordinal()
		if scannedColOrds.Contains(colOrd) {
			ir.newStoredColOrds.Add(colOrd)
		}
	}
}

// indexCols returns the explicit key columns of the index, used in
// indexRecommendationString.
func (ir *indexRecommendation) indexCols() []tree.IndexElem {
	indexCols := make([]tree.IndexElem, len(ir.index.cols))

	for i := range ir.index.cols {
		indexCol := ir.index.Column(i)
		colName := indexCol.Column.ColName()

		if ir.index.IsInverted() && i == len(ir.index.cols)-1 {
			colName = ir.index.tab.Column(indexCol.InvertedSourceColumnOrdinal()).ColName()
		}

		var direction tree.Direction
		if indexCol.Descending {
			direction = tree.Descending
		}

		indexCols[i] = tree.IndexElem{Column: colName, Direction: direction}
	}

	return indexCols
}

// storingColumns returns the stored columns of an index recommendation, used in
// indexRecommendationString.
func (ir *indexRecommendation) storingColumns() []tree.Name {
	storingLen := ir.newStoredColOrds.Len()
	if storingLen == 0 {
		return nil
	}
	storingCols := make([]tree.Name, 0, storingLen)
	ir.newStoredColOrds.ForEach(func(colOrd int) {
		colName := ir.index.tab.Column(colOrd).ColName()
		storingCols = append(storingCols, colName)
	})
	return storingCols
}

// indexRecommendationString returns the string output for an index
// recommendation, containing the SQL command(s) needed to follow this
// recommendation.
func (ir *indexRecommendation) indexRecommendationString(
	indexCols []tree.IndexElem, storing []tree.Name,
) string {
	var sb strings.Builder
	tableName := tree.NewUnqualifiedTableName(ir.index.tab.Name())

	var dropCmd tree.DropIndex
	unique := false
	if ir.existingIndex != nil {
		sb.WriteString("   SQL commands: ")
		indexName := tree.UnrestrictedName(ir.existingIndex.Name())
		dropCmd.IndexList = []*tree.TableIndexName{{Table: *tableName, Index: indexName}}

		// Maintain uniqueness if the existing index is unique.
		unique = ir.existingIndex.IsUnique()
	} else {
		sb.WriteString("   SQL command: ")
	}

	createCmd := tree.CreateIndex{
		Table:    *tableName,
		Columns:  indexCols,
		Storing:  storing,
		Unique:   unique,
		Inverted: ir.index.IsInverted(),
	}
	sb.WriteString(createCmd.String() + ";")
	if len(dropCmd.IndexList) > 0 {
		sb.WriteString(" " + dropCmd.String() + ";")
	}

	return sb.String()
}
