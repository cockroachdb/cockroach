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
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// Rec represents an index recommendation in the form of a SQL statement(s)
// that can be executed to apply the recommendation.
type Rec struct {
	// SQL is the statement(s) to run that will apply the recommendation.
	SQL string
	// Replacement is true if SQL replaces an existing index, i.e., it contains
	// both a CREATE INDEX and DROP INDEX statement.
	Replacement bool
}

// FindRecs finds index candidates that are scanned in an expression to
// determine a statement's index recommendation set.
//
// Note that because the index recommendation engine stores all table columns in
// its hypothetical indexes when optimizing, and then prunes the stored columns
// after, this may result in the best plan not being chosen. Meaning, a plan
// might not be chosen because the optimizer includes the overhead of scanning
// stored columns of an index, which results in plans with hypothetical indexes
// being more expensive and reduces their likelihood of being chosen. This is a
// tradeoff we accept in order to recommend STORING columns, as the difference
// in plan costs is not very significant.
func FindRecs(expr opt.Expr, md *opt.Metadata) []Rec {
	collector := make(recCollector, len(md.AllTables()))
	collector.populate(md, expr)
	collector.prune()
	return collector.indexRecs()
}

// recCollector stores the hypothetical indexes that are scanned in a
// statement's optimal plan.
type recCollector map[cat.Table][]indexRecommendation

// populate recursively walks an expression tree to find hypothetical indexes
// that are scanned in it.
func (rc recCollector) populate(md *opt.Metadata, expr opt.Expr) {
	switch expr := expr.(type) {
	case *memo.ScanExpr:
		rc.addIndex(md, expr.Index, expr.Cols, expr.Table)
	case *memo.LookupJoinExpr:
		rc.addIndex(md, expr.Index, expr.Cols, expr.Table)
	case *memo.InvertedJoinExpr:
		rc.addIndex(md, expr.Index, expr.Cols, expr.Table)
	case *memo.ZigzagJoinExpr:
		rc.addIndex(md, expr.LeftIndex, expr.Cols, expr.LeftTable)
		rc.addIndex(md, expr.RightIndex, expr.Cols, expr.RightTable)
	}
	for i, n := 0, expr.ChildCount(); i < n; i++ {
		rc.populate(md, expr.Child(i))
	}
}

// prune removes redundant index recommendations from the recommendation set.
// These are recommendations where the existingIndex field is not nil and no new
// columns are being stored.
func (rc recCollector) prune() {
	for t, indexRecs := range rc {
		updatedIndexRecs := make([]indexRecommendation, 0, len(indexRecs))
		for _, indexRec := range indexRecs {
			if indexRec.existingIndex == nil || !indexRec.isRedundant() {
				updatedIndexRecs = append(updatedIndexRecs, indexRec)
			}
		}
		rc[t] = updatedIndexRecs
	}
}

// getColOrdSet returns the set of column ordinals within the given table that
// are contained in cols.
func getColOrdSet(md *opt.Metadata, cols opt.ColSet, tabID opt.TableID) util.FastIntSet {
	var colsOrdSet util.FastIntSet
	cols.ForEach(func(col opt.ColumnID) {
		table := md.ColumnMeta(col).Table
		// Do not add columns from other tables.
		if table != tabID {
			return
		}
		colOrd := table.ColumnOrdinal(col)
		colsOrdSet.Add(colOrd)
	})
	return colsOrdSet
}

// addIndex adds an index to the indexes map if it does not
// exist already in the map and in the table. The scannedCols argument contains
// the columns of the index that are actually scanned, used to determine which
// columns should be stored columns in the index recommendation.
func (rc recCollector) addIndex(
	md *opt.Metadata, indexOrd cat.IndexOrdinal, scannedCols opt.ColSet, tabID opt.TableID,
) {
	// Do not add real table indexes (non-hypothetical table indexes).
	switch hypTable := md.TableMeta(tabID).Table.(type) {
	case *HypotheticalTable:
		// Do not recommend existing indexes.
		if indexOrd < hypTable.Table.IndexCount() {
			return
		}
		scannedColOrds := getColOrdSet(md, scannedCols, tabID)
		// Try to find an identical existing index recommendation.
		for _, indexRec := range rc[hypTable] {
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
		rc[hypTable] = append(rc[hypTable], newIndexRec)
	}
}

// indexRecs returns a list of IndexRecs that have been collected.
func (rc recCollector) indexRecs() []Rec {
	indexRecCount := 0
	for t := range rc {
		indexRecCount += len(rc[t])
	}
	if indexRecCount == 0 {
		return nil
	}

	sortedTables := make([]cat.Table, 0, len(rc))
	for t := range rc {
		sortedTables = append(sortedTables, t)
	}
	sort.Slice(sortedTables, func(i, j int) bool {
		return sortedTables[i].Name() < sortedTables[j].Name()
	})

	output := make([]Rec, 0, indexRecCount)
	for _, t := range sortedTables {
		indexes := rc[t]
		for _, indexRec := range indexes {
			indexCols := indexRec.indexCols()
			storing := indexRec.storingColumns()
			indexRecSQL := indexRec.SQLString(indexCols, storing)
			output = append(output, Rec{SQL: indexRecSQL, Replacement: indexRec.existingIndex != nil})
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
	// optimizer in the expression tree passed to FindRecs.
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

// isRedundant compares newStoredColOrds with the existing index's
// stored columns. It returns true if there are no new columns being stored in
// newStoredColOrds.
func (ir *indexRecommendation) isRedundant() bool {
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
// SQLString.
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
// SQLString.
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

// SQLString returns the string output for an index recommendation, containing
// the SQL command(s) needed to follow this recommendation.
func (ir *indexRecommendation) SQLString(indexCols []tree.IndexElem, storing []tree.Name) string {
	var sb strings.Builder
	tableName := tree.NewUnqualifiedTableName(ir.index.tab.Name())

	var dropCmd tree.DropIndex
	unique := false
	if ir.existingIndex != nil {
		indexName := tree.UnrestrictedName(ir.existingIndex.Name())
		dropCmd.IndexList = []*tree.TableIndexName{{Table: *tableName, Index: indexName}}

		// Maintain uniqueness if the existing index is unique.
		unique = ir.existingIndex.IsUnique()
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
