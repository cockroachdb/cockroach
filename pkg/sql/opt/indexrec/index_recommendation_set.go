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
	"github.com/cockroachdb/cockroach/pkg/util"
)

// FindIndexRecommendationSet finds index candidates that are used in an
// expression to determine a statement's index recommendation set.
//
// Note that because the index recommendation engine stores all table columns in
// its hypothetical indexes when optimizing, and then prunes the stored columns
// after, this may result in the best plan not being chosen. Meaning, an index
// might not be recommended because the optimizer anticipates the cost of
// storing all table columns, when in reality it won't have to. This is a
// tradeoff we accept in order to recommend STORING columns, as the difference
// in plan costs is not very significant.
func FindIndexRecommendationSet(expr opt.Expr, md *opt.Metadata) IndexRecommendationSet {
	var recommendationSet IndexRecommendationSet
	recommendationSet.init(md)
	recommendationSet.createIndexRecommendations(expr)
	recommendationSet.pruneIndexRecommendations()
	return recommendationSet
}

// IndexRecommendationSet stores the hypothetical indexes that are used in a
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
// hypothetical indexes that are used in it.
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

// getColOrdSet returns an ordinal set of an opt.ColSet, containing all columns
// in the set from the table with id tabID.
func (irs *IndexRecommendationSet) getColOrdSet(
	cols opt.ColSet, tabID opt.TableID,
) util.FastIntSet {
	var colsOrdSet util.FastIntSet
	for _, col := range cols.ToList() {
		// Do not add unknown columns.
		if col == 0 {
			continue
		}
		table := irs.md.ColumnMeta(col).Table
		// Do not add columns from other tables.
		if table != tabID {
			continue
		}
		colOrd := table.ColumnOrdinal(col)
		colsOrdSet.Add(colOrd)
	}
	return colsOrdSet
}

// addIndexToRecommendationSet adds an index to the indexes map if it does not
// exist already in the map and in the table.
func (irs *IndexRecommendationSet) addIndexToRecommendationSet(
	indexOrd cat.IndexOrdinal, usedCols opt.ColSet, tabID opt.TableID,
) {
	// Do not recommend the primary index.
	if indexOrd == cat.PrimaryIndex {
		return
	}
	// Do not add real table indexes (non-hypothetical table indexes).
	switch hypTable := irs.md.TableMeta(tabID).Table.(type) {
	case *hypotheticalTable:
		usedColsOrds := irs.getColOrdSet(usedCols, tabID)
		// Try to find an identical existing index recommendation.
		for _, indexRec := range irs.indexRecs[hypTable] {
			index := indexRec.index
			if index.indexOrdinal == indexOrd {
				// Update newStoredColOrds to include all stored column ordinals that
				// are in usedColsOrds.
				usedStoredColOrds := index.storedColsOrdSet.Intersection(usedColsOrds)
				indexRec.newStoredColOrds.UnionWith(usedStoredColOrds)
				return
			}
		}
		// Create a new index recommendation with pruned stored columns. Only store
		// columns that are in usedColsOrds.
		var newIndexRec indexRecommendation
		newIndexRec.init(indexOrd, hypTable, usedColsOrds)
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

	var output []string
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
			var sb strings.Builder
			recTypeStr := "index creation"
			if indexRec.existingIndex != nil {
				recTypeStr = "index replacement"
			}
			sb.WriteString(fmt.Sprintf("%d. type: %s", indexRecOrd, recTypeStr))
			indexRecOrd++
			index := indexRec.index
			indexCols := make([]string, len(index.cols))

			for i, n := 0, len(index.cols); i < n; i++ {
				var indexColSb strings.Builder
				indexCol := index.Column(i)
				colName := indexCol.Column.ColName()
				indexColSb.WriteString(colName.String())

				if indexCol.Descending {
					indexColSb.WriteString(" DESC")
				}

				indexCols[i] = indexColSb.String()
			}

			storingClause := ""
			if indexRec.newStoredColOrds.Len() > 0 {
				var storingSb strings.Builder
				for i, col := range indexRec.newStoredColOrds.Ordered() {
					colName := t.Column(col).ColName()
					if i > 0 {
						storingSb.WriteString(", ")
					}
					storingSb.WriteString(colName.String())
				}
				storingClause = " STORING (" + storingSb.String() + ")"
			}

			tableName := t.Name()
			if indexRec.existingIndex != nil {
				sb.WriteString("\n   SQL commands: ")
				indexName := indexRec.existingIndex.Name()
				dropCmd := fmt.Sprintf("DROP INDEX %s@%s; ", tableName.String(), indexName.String())
				sb.WriteString(dropCmd)
			} else {
				sb.WriteString("\n   SQL command: ")
			}

			createCmd := fmt.Sprintf(
				"CREATE INDEX ON %s (%s)%s;",
				tableName.String(),
				strings.Join(indexCols, ", "),
				storingClause,
			)
			sb.WriteString(createCmd)
			output = append(output, sb.String())
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

	// newStoredColOrds stores the stored column ordinals that are used by the
	// optimizer in the expression tree passed to FindIndexRecommendationSet.
	newStoredColOrds util.FastIntSet

	// existingStoredColOrds stores the column ordinals of the existingIndex's
	// stored columns. It is nil if there is no existingIndex.
	existingStoredColOrds util.FastIntSet
}

// init initializes an index recommendation. If there is an existingIndex with
// the same explicit key columns, it is stored here.
func (ir *indexRecommendation) init(
	indexOrd int, hypTable *hypotheticalTable, usedColsOrds util.FastIntSet,
) {
	index := hypTable.Index(indexOrd).(*hypotheticalIndex)
	ir.index = index
	ir.existingIndex = hypTable.existingRedundantIndex(index)

	// Only store columns useful to the statement plan, found in usedColsOrds.
	ir.newStoredColOrds = index.storedColsOrdSet.Intersection(usedColsOrds)

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
