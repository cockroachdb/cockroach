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
func FindIndexRecommendationSet(expr opt.Expr, md *opt.Metadata) IndexRecommendationSet {
	var recommendationSet IndexRecommendationSet
	recommendationSet.init(md)
	recommendationSet.createIndexRecommendations(expr)
	return recommendationSet
}

// IndexRecommendationSet stores the hypothetical indexes that are used in a
// statement's optimal plan (in usedIndexes), as well as the statement's
// metadata.
type IndexRecommendationSet struct {
	md          *opt.Metadata
	usedIndexes map[cat.Table]util.FastIntSet
}

// init initializes an IndexRecommendationSet by allocating memory for it.
func (irs *IndexRecommendationSet) init(md *opt.Metadata) {
	numTables := len(md.AllTables())
	irs.md = md
	irs.usedIndexes = make(map[cat.Table]util.FastIntSet, numTables)
}

// createIndexRecommendations recursively walks an expression tree to find
// hypothetical indexes that are used in it.
func (irs *IndexRecommendationSet) createIndexRecommendations(expr opt.Expr) {
	switch expr := expr.(type) {
	case *memo.ScanExpr:
		irs.addIndexToRecommendationSet(expr.Index, expr.Table)
	case *memo.LookupJoinExpr:
		irs.addIndexToRecommendationSet(expr.Index, expr.Table)
	case *memo.InvertedJoinExpr:
		irs.addIndexToRecommendationSet(expr.Index, expr.Table)
	case *memo.ZigzagJoinExpr:
		irs.addIndexToRecommendationSet(expr.LeftIndex, expr.LeftTable)
		irs.addIndexToRecommendationSet(expr.RightIndex, expr.RightTable)
	}
	for i, n := 0, expr.ChildCount(); i < n; i++ {
		irs.createIndexRecommendations(expr.Child(i))
	}
}

// addIndexToRecommendationSet adds an index to the indexes map if it does not
// exist already in the map and in the table.
func (irs *IndexRecommendationSet) addIndexToRecommendationSet(
	indexOrd cat.IndexOrdinal, tabID opt.TableID,
) {
	switch hypTable := irs.md.TableMeta(tabID).Table.(type) {
	case *hypotheticalTable:
		// Do not add real table indexes (non-hypothetical indexes).
		if indexOrd < hypTable.Table.IndexCount() {
			return
		}
		tabUsedIndexes := irs.usedIndexes[hypTable]
		tabUsedIndexes.Add(indexOrd)
		irs.usedIndexes[hypTable] = tabUsedIndexes
	}
}

// String returns the string index recommendation output that will be displayed
// below the statement plan in EXPLAIN.
func (irs *IndexRecommendationSet) String() string {
	if len(irs.usedIndexes) == 0 {
		return ""
	}
	indexRecCount := 0
	for t := range irs.usedIndexes {
		indexRecCount += irs.usedIndexes[t].Len()
	}
	var sb strings.Builder
	sb.WriteString(
		fmt.Sprintf("\n\nindex recommendations: %d\n\n", indexRecCount),
	)

	sortedTables := make([]cat.Table, 0, len(irs.usedIndexes))
	for t := range irs.usedIndexes {
		sortedTables = append(sortedTables, t)
	}
	sort.Slice(sortedTables, func(i, j int) bool {
		return sortedTables[i].Name() < sortedTables[j].Name()
	})

	indexRecOrd := 1
	for _, t := range sortedTables {
		indexes := irs.usedIndexes[t]
		for _, indexOrd := range indexes.Ordered() {
			sb.WriteString(fmt.Sprintf("%d. ", indexRecOrd))
			indexRecOrd++
			index := t.Index(indexOrd).(*hypotheticalIndex)
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

			tableName := t.Name()
			sqlCmd := fmt.Sprintf(
				"CREATE INDEX ON %s (%s);\n\n", tableName.String(), strings.Join(indexCols, ", "),
			)
			sb.WriteString(sqlCmd)
		}
	}
	return sb.String()
}
