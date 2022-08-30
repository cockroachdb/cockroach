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

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// indexRecType represents the type of index recommendation for Rec.
type indexRecType uint8

const (
	// TypeCreateIndex represents index recommendation CREATE INDEX. This is
	// recommended when there does not exist an index with the same explicit
	// columns as the hypothetical index.
	TypeCreateIndex indexRecType = iota
	// TypeReplaceIndex represents index recommendation CREATE INDEX followed by
	// DROP INDEX. This is recommended when there exists an index with the same
	// explicit columns as the hypothetical index, but it does not have enough
	// stored columns.
	TypeReplaceIndex
	// TypeUseless represents redundant index recommendation. Theoretically, index
	// recommendation should never be useless.
	TypeUseless
)

// Rec represents an index recommendation in the form of a SQL statement(s)
// that can be executed to apply the recommendation.
type Rec struct {
	// SQL is the statement(s) to run that will apply the recommendation.
	SQL string
	// Replacement is true if SQL replaces an existing index, i.e., it contains
	// both a CREATE INDEX and DROP INDEX statement.
	RecType indexRecType
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
	collector.addIndexRec(md, expr)
	return collector.outputIndexRec()
}

// recCollector stores the hypothetical indexes that are scanned in a
// statement's optimal plan.
type recCollector map[cat.Table][]indexRecommendation

// addIndexRec recursively walks an expression tree to find hypothetical indexes
// that are scanned in it and add index recommendations to recCollector.
func (rc recCollector) addIndexRec(md *opt.Metadata, expr opt.Expr) {
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
		rc.addIndexRec(md, expr.Child(i))
	}
}

// checkSameExplicitCols is a helper function that checks whether the given
// existing index has the same explicit columns as the given indexCols. If so,
// it returns true.
func checkSameExplicitCols(
	existingIndex cat.Index, indexCols []cat.IndexColumn, isInverted bool,
) bool {
	if existingIndex.ExplicitColumnCount() != len(indexCols) {
		return false
	}
	for j, m := 0, existingIndex.ExplicitColumnCount(); j < m; j++ {
		// Compare every existingIndex columns with indexCols.
		existingIndexCol := existingIndex.Column(j)
		indexCol := indexCols[j]

		if isInverted && existingIndex.IsInverted() && j == m-1 {
			// If the column is inverted, compare the source columns.
			if existingIndexCol.InvertedSourceColumnOrdinal() != indexCol.InvertedSourceColumnOrdinal() {
				return false
			}
		} else if existingIndexCol != indexCol {
			// Otherwise, compare every column directly.
			return false
		}
	}
	return true
}

// getStoredCols returns stored columns of the given existingIndex including in
// the STORING clause.
func getStoredCols(existingIndex cat.Index) util.FastIntSet {
	var existingStoredOrds util.FastIntSet
	for i, n := existingIndex.ExplicitColumnCount(), existingIndex.ColumnCount(); i < n; i++ {
		existingStoredOrds.Add(existingIndex.Column(i).Ordinal())
	}
	return existingStoredOrds
}

// findBestExistingIndexToReplace finds the best existing index to replace given
// the table with existing indexes, hypothetical index's explicit columns, and
// columns that are actually scanned.
//
// We are looking for the best existing index candidate to replace. To be a
// candidate, the existing index has to be 1. not a partial index (because we do
// not want to recommend dropping a partial index) 2. stores the same explicit
// columns as the hypIndex (because we do not want to recommend replacing an
// index with a new index having different key columns).
//
// Among all the candidates, it selects the one that stores the most columns
// from actuallyScannedCols. If found, it returns TypeReplaceIndex, the best
// candidate for existing index, and its stored columns. If not found, this
// means that there does not exist an index that satisfy the requirement to be a
// candidate. So no existing indexes can be replaced, and creating a new index
// is necessary. It returns TypeCreateIndex, nil, and util.FastIntSet{}.
func findBestExistingIndexToReplace(
	table cat.Table,
	hypIndex *hypotheticalIndex,
	actuallyScannedCols util.FastIntSet,
) (indexRecType, cat.Index, util.FastIntSet) {

	// To find the existing index with most columns in actuallyScannedCol, we keep
	// track of the best candidate for existing index and its stored columns.
	//
	// Difference is list of cols that are in actuallyScannedCol but not in the
	// existing index's stored cols. And we are looking for the minimum length of
	// difference among all existing indexes. We know that if the diff is empty,
	// that means the existing index stores every column in actuallyScannedCol.
	//
	// minColsDiff keeps track of the minimum difference so far. It is initialized
	// to the length of actuallyScannedCol which is the maximum possible
	// difference (existing index does not contain any columns in
	// actuallyScannedCol).
	minColsDiff := actuallyScannedCols.Len()
	var existingIndexCandidate cat.Index
	var existingIndexCandidateStoredCol util.FastIntSet

	for i, n := 0, table.IndexCount(); i < n; i++ {
		// Iterate through every existing index in the table.
		existingIndex := table.Index(i)
		if _, isPartialIndex := existingIndex.Predicate(); isPartialIndex {
			// Skip any partial indexes.
			continue
		}

		existingIndexStoredCols := getStoredCols(existingIndex)
		// checkSameExplicitCols returns true iff the existing index and hypIndex
		// stores the same explicit columns. If hypIndex is inverted, it also makes
		// sure that their inverted column stores the same source column.
		isSameKeyCols := checkSameExplicitCols(existingIndex, hypIndex.cols, hypIndex.IsInverted())
		if isSameKeyCols {
			// If isSameKeyCols, this existing index is a candidate for potential
			// index replacement.
			//
			// numOfStoredColsDiff is the list of cols that are in actuallyScannedCol
			// but not in the existing index's stored cols which will be the ones we
			// recommend to store later.
			numOfStoredColsDiff := actuallyScannedCols.Difference(existingIndexStoredCols)
			if numOfStoredColsDiff.Empty() {
				// If numOfStoredColsDiff is empty, that means the existing index stores
				// every column in actuallyScannedCol.
				//
				// Theoretically, this should never happen because optimizer should use
				// the existing index here and no index recommendation would be created
				// in the first place. If index recommendation has been created, that
				// means optimizer must be using some columns from the hypothetical
				// indexes that are not stored by the existing index.
				panic("Index recommendation should not be redundant.")
				return TypeUseless, nil, util.FastIntSet{}
			} else if existingIndexCandidate == nil || numOfStoredColsDiff.Len() < minColsDiff {
				// Otherwise, numOfStoredColsDiff is non-empty. The existing index is
				// missing some columns in actuallyScannedCol. If no candidate has
				// been picked yet or if this existing index has more overlaps with
				// actuallyScannedCol than the current candidate, it will become the
				// new candidate.
				existingIndexCandidate = existingIndex
				existingIndexCandidateStoredCol = existingIndexStoredCols
			}
		}
	}

	if existingIndexCandidate == nil {
		// There doesn't exist an index with same explicit columns as hypIndex.
		// Recommend index creation.
		return TypeCreateIndex, nil, util.FastIntSet{}
	}

	return TypeReplaceIndex, existingIndexCandidate, existingIndexCandidateStoredCol
}

// outputCreateIndexRec returns Rec containing a sql command "CREATE (unique,
// inverted) INDEX ON tableName(indexCols) STORING (storing);" and
// TypeCreateIndex.
func outputCreateIndexRec(indexCols []tree.IndexElem, storing []tree.Name, tableName tree.Name, inverted bool, unique bool) Rec {
	createCmd := tree.CreateIndex{
		Table:    *tree.NewUnqualifiedTableName(tableName),
		Columns:  indexCols,
		Storing:  storing,
		Unique:   unique,
		Inverted: inverted,
	}
	return Rec{createCmd.String() + ";", TypeCreateIndex}
}

// outputReplaceIndexRec returns Rec containing a sql command "CREATE (unique,
// inverted) INDEX ON tableName(indexCols) STORING (storing); DROP INDEX ON
// tableName@indexName;" and TypeReplaceIndex.
func outputReplaceIndexRec(
	indexCols []tree.IndexElem, storing []tree.Name, tableName tree.Name, inverted bool, unique bool, indexName tree.Name,
) Rec {
	dropCmd := tree.DropIndex{
		IndexList: []*tree.TableIndexName{{
			Table: *tree.NewUnqualifiedTableName(tableName),
			Index: tree.UnrestrictedName(indexName),
		}},
	}
	// Maintain uniqueness and inverted if the existing index is unique.
	createCmd := outputCreateIndexRec(indexCols, storing, tableName, inverted, unique)
	outputStr := createCmd.SQL + " " + dropCmd.String() + ";"
	return Rec{outputStr, TypeReplaceIndex}
}

// constructIndexRec is a helper function that finds the type of index
// recommendation and returns Rec for a given index recommendation.
//
// At this stage, index recommendations have been created; we know which
// hypothetical indexes are useful and which columns are actually scanned. But
// we do not know which type of index recommendation to give yet.
// constructIndexRec finds the type of index recommendation by calling
// findBestExistingIndexToReplace to find the best existing index to potentially
// replace. See comment above findBestExistingIndexToReplace to understand how
// it determines the existing index. It then calls outputCreateIndexRec or
// outputReplaceIndexRec to properly format the index recommendation to Rec,
// containing the SQL string needed to follow this recommendation and the type
// of the index recommendation.
func (ir *indexRecommendation) constructIndexRec() Rec {
	recType, existingIndex, existingIndexStoredCol := findBestExistingIndexToReplace(ir.index.tab.Table, ir.index, ir.newStoredColOrds)
	indexCols := ir.indexCols()
	if existingIndex != nil {
		// After finding out the existing index, update newStoredColOrds to contain
		// the existingStoredColOrds. We want to include all existing stored columns
		// in the replacement recommendation.
		ir.newStoredColOrds.UnionWith(existingIndexStoredCol)
	}
	storing := ir.storingColumns()

	// Formats index recommendation to its final output struct Rec.
	if recType == TypeCreateIndex {
		return outputCreateIndexRec(indexCols, storing, ir.index.tab.Name(), ir.index.IsInverted(), false)
	} else if recType == TypeReplaceIndex {
		return outputReplaceIndexRec(
			indexCols,
			storing,
			ir.index.tab.Name(),
			ir.index.IsInverted(),
			existingIndex.IsUnique(),
			existingIndex.Name(),
		)
	}
	return Rec{}
}

// outputIndexRec formats index recommendations to its final outputs and returns
// a list of Rec containing all index recs.
func (rc recCollector) outputIndexRec() []Rec {
	rcMap := make(map[cat.Table][]Rec)
	for t, indexRecs := range rc {
		updatedRecs := make([]Rec, 0, len(indexRecs))
		for _, indexRec := range indexRecs {
			// For each index recommendation, call constructIndexRec to properly
			// format it to Rec.
			updatedRecs = append(updatedRecs, indexRec.constructIndexRec())
		}
		rcMap[t] = updatedRecs
	}
	return formatToOutput(rcMap)
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

// addIndex adds an index to the indexes map if it does not exist already in the
// map and in the table. The scannedCols argument contains the columns of the
// index that are actually scanned, used to determine which columns should be
// stored columns in the index recommendation.
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

// formatToOutput flattens the given map rcMap to a 1D list of Rec containing
// all index recommendations as the final output for findRecs.
func formatToOutput(rcMap map[cat.Table][]Rec) []Rec {
	indexRecCount := 0
	for t := range rcMap {
		indexRecCount += len(rcMap[t])
	}
	if indexRecCount == 0 {
		return nil
	}

	sortedTables := make([]cat.Table, 0, len(rcMap))
	for t := range rcMap {
		sortedTables = append(sortedTables, t)
	}
	sort.Slice(sortedTables, func(i, j int) bool {
		return sortedTables[i].Name() < sortedTables[j].Name()
	})

	output := make([]Rec, 0, indexRecCount)
	for _, t := range sortedTables {
		output = append(output, rcMap[t]...)
	}
	return output
}

// indexRecommendation stores the information pertaining to a single index
// recommendation.
type indexRecommendation struct {
	// index stores the hypotheticalIndex that is being recommended.
	index *hypotheticalIndex

	// newStoredColOrds stores the stored column ordinals that are scanned by the
	// optimizer in the expression tree passed to FindRecs.
	newStoredColOrds util.FastIntSet

	outputIndexRec Rec
}

// init initializes an index recommendation. If there is an existingIndex with
// the same explicit columns, it is stored here.
func (ir *indexRecommendation) init(
	indexOrd int, hypTable *HypotheticalTable, scannedColOrds util.FastIntSet,
) {
	index := hypTable.Index(indexOrd).(*hypotheticalIndex)
	ir.index = index
	// Only store columns useful to the statement plan, found in scannedColOrds.
	for i := range index.storedCols {
		colOrd := index.storedCols[i].Column.Ordinal()
		if scannedColOrds.Contains(colOrd) {
			ir.newStoredColOrds.Add(colOrd)
		}
	}
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

// indexCols returns the explicit columns of the index, used in SQLString.
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
