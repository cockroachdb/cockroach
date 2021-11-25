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
func FindIndexRecommendationSet(expr opt.Expr, md *opt.Metadata) string {
	var recommendationSet indexRecommendationSet
	recommendationSet.init(md)
	recommendationSet.createIndexRecommendations(expr)
	return recommendationSet.getStringOutput()
}

// indexRecommendationSet stores the hypothetical indexes that are used in a
// statement's optimal plan (in indexRecs), as well as the statement's metadata.
type indexRecommendationSet struct {
	md        *opt.Metadata
	indexRecs map[cat.Table][]indexRecommendation
}

type indexRecommendation struct {
	index               *hypotheticalIndex
	prunedStoredColOrds util.FastIntSet
}

// init initializes an indexRecommendationSet by allocating memory for it.
func (irs *indexRecommendationSet) init(md *opt.Metadata) {
	numTables := len(md.AllTables())
	irs.md = md
	irs.indexRecs = make(map[cat.Table][]indexRecommendation, numTables)
}

// createIndexRecommendations recursively walks an expression tree to find
// hypothetical indexes that are used in it.
func (irs *indexRecommendationSet) createIndexRecommendations(expr opt.Expr) {
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

func (irs *indexRecommendationSet) getColOrdSet(
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
func (irs *indexRecommendationSet) addIndexToRecommendationSet(
	indexOrd cat.IndexOrdinal, usedCols opt.ColSet, tabID opt.TableID,
) {
	switch hypTable := irs.md.TableMeta(tabID).Table.(type) {
	case *hypotheticalTable:
		// Do not add real table indexes (non-hypothetical indexes).
		if indexOrd < hypTable.Table.IndexCount() {
			return
		}
		usedColsOrds := irs.getColOrdSet(usedCols, tabID)
		// Try to find an identical existing index recommendation.
		for _, indexRec := range irs.indexRecs[hypTable] {
			index := indexRec.index
			if index.indexOrdinal == indexOrd {
				// Update prunedStoredColOrds to include all stored column ordinals that
				// are in usedColsOrds.
				newStoredColOrds := index.storedColsOrdSet.Intersection(usedColsOrds)
				indexRec.prunedStoredColOrds = indexRec.prunedStoredColOrds.Union(newStoredColOrds)
				return
			}
		}
		// Create a new index recommendation with pruned stored columns. Only store
		// columns that are in usedColsOrds.
		index := hypTable.Index(indexOrd).(*hypotheticalIndex)
		usedStoredColOrds := index.storedColsOrdSet.Intersection(usedColsOrds)
		newIndexRec := indexRecommendation{index: index, prunedStoredColOrds: usedStoredColOrds}
		irs.indexRecs[hypTable] = append(irs.indexRecs[hypTable], newIndexRec)
	}
}

// getStringOutput returns the string index recommendation output that will be
// displayed below the statement plan in EXPLAIN.
func (irs *indexRecommendationSet) getStringOutput() string {
	if len(irs.indexRecs) == 0 {
		return ""
	}
	indexRecCount := 0
	for t := range irs.indexRecs {
		indexRecCount += len(irs.indexRecs[t])
	}
	var indexStr string
	if indexRecCount == 1 {
		indexStr = "index"
	} else {
		indexStr = "indexes"
	}
	var sb strings.Builder
	sb.WriteString(
		fmt.Sprintf("\n\nindex recommendation: create %d %s\n\n", indexRecCount, indexStr),
	)

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
			sb.WriteString(fmt.Sprintf("%d. table: ", indexRecOrd))
			indexRecOrd++
			tableName := t.Name()
			sb.WriteString(tableName.String() + "\n   columns: ")
			index := indexRec.index
			indexCols := make([]string, len(index.cols))

			for i, n := 0, len(index.cols); i < n; i++ {
				if i > 0 {
					sb.WriteString(", ")
				}

				var indexColSb strings.Builder
				indexCol := index.Column(i)
				colName := indexCol.Column.ColName()
				indexColSb.WriteString(colName.String())
				sb.WriteString("[" + colName.String() + "] ")

				if indexCol.Descending {
					sb.WriteString("DESC")
					indexColSb.WriteString(" DESC")
				} else {
					sb.WriteString("ASC")
				}

				indexCols[i] = indexColSb.String()
			}

			storingClause := ""
			if indexRec.prunedStoredColOrds.Len() > 0 {
				sb.WriteString("\n   storing: ")
				var storingSb strings.Builder
				for i, col := range indexRec.prunedStoredColOrds.Ordered() {
					colName := t.Column(col).ColName()
					if i > 0 {
						storingSb.WriteString(", ")
					}
					storingSb.WriteString(colName.String())
				}
				sb.WriteString(storingSb.String())
				storingClause = " STORING (" + storingSb.String() + ")"
			}

			sb.WriteString("\n   SQL command: ")
			sqlCmd := fmt.Sprintf(
				"CREATE INDEX ON %s (%s)%s;\n\n",
				tableName.String(),
				strings.Join(indexCols, ", "),
				storingClause,
			)
			sb.WriteString(sqlCmd)
		}
	}
	return sb.String()
}
