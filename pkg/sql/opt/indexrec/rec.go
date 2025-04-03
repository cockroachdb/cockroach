// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package indexrec

import (
	"context"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
)

// Type represents the type of index recommendation for Rec.
type Type uint8

const (
	// TypeCreateIndex represents index recommendation CREATE INDEX. This is
	// recommended when there does not exist an index with the same explicit
	// columns as the hypothetical index.
	TypeCreateIndex Type = iota
	// TypeReplaceIndex represents index recommendation CREATE INDEX followed by
	// DROP INDEX. This is recommended when there exists an index with the same
	// explicit columns as the hypothetical index, but it does not have enough
	// stored columns.
	TypeReplaceIndex
	// TypeAlterIndex represents index recommendation ALTER INDEX...VISIBLE. This
	// is recommended when there exists an invisible index containing every
	// explicit columns in the hypothetical index and every column needed to scan.
	TypeAlterIndex
	// typeUseless represents redundant index recommendation. Theoretically, index
	// recommendation should never be useless.
	typeUseless
)

// Rec represents an index recommendation in the form of a SQL statement(s)
// that can be executed to apply the recommendation.
type Rec struct {
	// SQL is the statement(s) to run that will apply the recommendation.
	SQL string
	// Replacement is true if SQL replaces an existing index, i.e., it contains
	// both a CREATE INDEX and DROP INDEX statement.
	RecType Type
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
func FindRecs(ctx context.Context, expr opt.Expr, md *opt.Metadata) ([]Rec, error) {
	collector := make(recCollector, len(md.AllTables()))
	collector.addIndexRec(md, expr)
	return collector.outputIndexRec(ctx)
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

// getStoredCols returns stored columns of the given existingIndex.
func getStoredCols(existingIndex cat.Index) intsets.Fast {
	var existingStoredOrds intsets.Fast
	for i, n := existingIndex.KeyColumnCount(), existingIndex.ColumnCount(); i < n; i++ {
		existingStoredOrds.Add(existingIndex.Column(i).Ordinal())
	}
	return existingStoredOrds
}

// getAllCols returns columns of the given existingIndex including in the
// explicit columns and STORING clause.
func getAllCols(existingIndex cat.Index) intsets.Fast {
	var existingAllOrds intsets.Fast
	for i, n := 0, existingIndex.ColumnCount(); i < n; i++ {
		existingAllOrds.Add(existingIndex.Column(i).Ordinal())
	}
	return existingAllOrds
}

// findBestExistingIndexToReplace finds the best existing index to replace given
// the table with existing indexes, the hypothetical index's explicit columns,
// and columns that are actually scanned. If found, it returns TypeReplaceIndex,
// the best existing index to replace,and the set of columns ordinals that are
// already stored by the existing index. If not found, it returns
// TypeCreateIndex, nil, {}.
//
// It first looks for an existing invisible index that contains every column in
// hypIndex and every column from actuallyScannedCols. If found, recommend alter
// index visibility; TypeAlterIndex, existing index, {} is returned.
//
// If not found, we start looking for the best existing index candidate to
// replace. To be a candidate, the existing index has to 1. not be a partial
// index (because we do not want to recommend dropping a partial index)
// 2.visible index (because we do not want to recommend dropping an invisible
// index) 3. have the same explicit columns as the hypIndex (because we do not
// want to recommend replacing an index with a new index having different
// explicit columns).
//
// Among all the candidates, it selects the one that stores the most columns
// from newStoredCols. If found, it returns TypeReplaceIndex, the best
// candidate for existing index, and its already stored columns. If not found,
// this means that there does not exist an index that satisfy the requirement to
// be a candidate. So no existing indexes can be replaced, and creating a new
// index is necessary. It returns TypeCreateIndex, nil, and intsets.Fast. If
// there is a candidate that stores every column from newStoredCols,
// typeUseless, nil, {} is returned. Theoretically, this should never happen.
func findBestExistingIndexToReplace(
	table cat.Table, hypIndex *hypotheticalIndex, newStoredCols intsets.Fast,
) (Type, cat.Index, intsets.Fast) {

	// To find the existing index with most columns in newStoredCols, we keep
	// track of the best candidate for existing index and its stored columns.
	//
	// Difference is a list of cols that are in newStoredCols but not in the
	// existing index's stored cols. And we are looking for the minimum length of
	// difference among all existing indexes. We know that if the diff is empty,
	// that means the existing index stores every column in newStoredCols.
	//
	// minColsDiff keeps track of the minimum difference length so far. It is
	// initialized to the length of newStoredCols which is the maximum
	// possible difference (existing index does not contain any columns in
	// newStoredCols).
	minColsDiff := newStoredCols.Len()
	var existingIndexCandidate cat.Index
	var existingIndexCandidateStoredCol intsets.Fast

	for i, n := 0, table.IndexCount(); i < n; i++ {
		// Iterate through every existing index in the table.
		existingIndex := table.Index(i)
		if _, isPartialIndex := existingIndex.Predicate(); isPartialIndex {
			// Skip any partial indexes.
			continue
		}
		if existingIndex.GetInvisibility() != 0.0 {
			if hypIndex.hasPrefixOfExplicitCols(existingIndex) {
				existingIndexAllCols := getAllCols(existingIndex)
				if newStoredCols.Difference(existingIndexAllCols).Empty() {
					// There exists an invisible index containing every explicit column in
					// hypIndex and column in newStoredCols. Recommend alter index
					// visible.
					//
					// Note that we do not require an invisible index to have the same
					// explicit columns as the hypIndex. This is because: consider query
					// SELECT a FROM t WHERE b > 0, hypIndex(a), newStoredCols b.
					// invisible_idx(a, b) could still be used. Creating a new index with
					// idx(a) STORING b is unnecessary.
					return TypeAlterIndex, existingIndex, intsets.Fast{}
				}
			}
			// Skip any invisible indexes.
			continue
		}

		existingIndexStoredCols := getStoredCols(existingIndex)
		// hasSameExplicitCols returns true iff the existing index and hypIndex has
		// the same explicit columns. If hypIndex is inverted, it also makes sure
		// that their inverted column comes from the same source column.
		hasSameExplicitCols := hypIndex.hasSameExplicitCols(existingIndex)
		if hasSameExplicitCols {
			// If hasSameExplicitCols, this existing index is a candidate for
			// potential index replacement.
			//
			// storedColsDiffSet is the list of cols that are in actuallyScannedCol
			// but not in the existing index's stored cols. We are looking for the
			// minimum diff set.
			storedColsDiffSet := newStoredCols.Difference(existingIndexStoredCols)
			if storedColsDiffSet.Empty() {
				// If storedColsDiffSet is empty, that means the existing index stores
				// every column in actuallyScannedCol. This index recommendation is
				// useless.
				//
				// Theoretically, this should also never happen; at least one of the
				// scanned cols is neither included in explicit columns nor stored.
				// Otherwise, the optimizer should use the existing index, and no index
				// recommendation should be constructed.
				return typeUseless, nil, intsets.Fast{}
			} else if existingIndexCandidate == nil || storedColsDiffSet.Len() < minColsDiff {
				// Otherwise, storedColsDiffSet is non-empty. The existing index is
				// missing some columns in actuallyScannedCol. If no candidate has been
				// picked yet or if this existing index has more overlaps with
				// actuallyScannedCol than the current candidate, it will become the new
				// candidate.
				existingIndexCandidate = existingIndex
				existingIndexCandidateStoredCol = existingIndexStoredCols
			}
		}
	}

	if existingIndexCandidate == nil {
		// There doesn't exist an index with same explicit columns as hypIndex.
		// Recommend index creation.
		return TypeCreateIndex, nil, intsets.Fast{}
	}

	return TypeReplaceIndex, existingIndexCandidate, existingIndexCandidateStoredCol
}

// constructIndexRec finds the type of index recommendation and returns Rec for
// a given index recommendation.
//
// At this stage, index recommendations have been created; we know which
// hypothetical indexes are useful and which columns are actually scanned. But
// we do not know which type of index recommendation to give yet.
// constructIndexRec finds the type of index recommendation by calling
// findBestExistingIndexToReplace to find the best existing index to potentially
// replace. See comment above findBestExistingIndexToReplace to understand how
// it determines the existing index. It will then properly format the index
// recommendation to Rec, containing the SQL string needed to follow this
// recommendation and the type of the index recommendation.
func (ir *indexRecommendation) constructIndexRec(ctx context.Context) (Rec, error) {
	var sb strings.Builder
	recType, existingIndex, existingIndexStoredCol := findBestExistingIndexToReplace(
		ir.index.tab.Table, ir.index, ir.newStoredColOrds,
	)
	indexCols := ir.indexCols()
	if existingIndex != nil {
		// After finding out the existing index, update newStoredColOrds to contain
		// the existingStoredColOrds. We want to include all existing stored columns
		// in the replacement recommendation.
		ir.newStoredColOrds.UnionWith(existingIndexStoredCol)
	}
	storing := ir.storingColumns()
	tableName, err := ir.index.tab.FullyQualifiedName(ctx)
	if err != nil {
		return Rec{}, err
	}

	// Formats index recommendation to its final output struct Rec.
	switch recType {
	case TypeCreateIndex:
		createCmd := tree.CreateIndex{
			Table:   tableName,
			Columns: indexCols,
			Storing: storing,
			Unique:  false,
			Type:    ir.index.Type(),
		}
		sb.WriteString(createCmd.String())
		sb.WriteByte(';')
		return Rec{sb.String(), TypeCreateIndex}, nil
	case TypeReplaceIndex:
		dropCmd := tree.DropIndex{
			IndexList: []*tree.TableIndexName{{
				Table: tableName,
				Index: tree.UnrestrictedName(existingIndex.Name()),
			}},
		}
		createCmd := tree.CreateIndex{
			Table:   tableName,
			Columns: indexCols,
			Storing: storing,
			// Maintain uniqueness and inverted if the existing index is unique.
			Unique: existingIndex.IsUnique(),
			Type:   ir.index.Type(),
		}
		sb.WriteString(createCmd.String())
		sb.WriteByte(';')
		sb.WriteByte(' ')
		sb.WriteString(dropCmd.String())
		sb.WriteByte(';')
		return Rec{sb.String(), TypeReplaceIndex}, nil
	case TypeAlterIndex:
		alterCmd := tree.AlterIndexVisible{
			Index: tree.TableIndexName{
				Table: tableName,
				Index: tree.UnrestrictedName(existingIndex.Name()),
			},
		}
		sb.WriteString(alterCmd.String())
		sb.WriteByte(';')
		return Rec{sb.String(), TypeAlterIndex}, nil
	}
	return Rec{}, nil
}

// outputIndexRec formats index recommendations to its final outputs and returns
// a list of Rec containing all index recs.
func (rc recCollector) outputIndexRec(ctx context.Context) ([]Rec, error) {
	rcMap := make(map[cat.Table][]Rec)
	for t, indexRecs := range rc {
		updatedRecs := make([]Rec, 0, len(indexRecs))
		for _, indexRec := range indexRecs {
			// For each index recommendation, call constructIndexRec to properly
			// format it to Rec.
			rec, err := indexRec.constructIndexRec(ctx)
			if err != nil {
				return nil, err
			}
			updatedRecs = append(updatedRecs, rec)
		}
		rcMap[t] = updatedRecs
	}
	return flattenRecMap(rcMap), nil
}

// getColOrdSet returns the set of column ordinals within the given table that
// are contained in cols.
func getColOrdSet(md *opt.Metadata, cols opt.ColSet, tabID opt.TableID) intsets.Fast {
	var colsOrdSet intsets.Fast
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

// flattenRecMap flattens the given map rcMap to a 1D list of Rec containing
// all index recommendations as the final output for findRecs.
func flattenRecMap(rcMap map[cat.Table][]Rec) []Rec {
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
	newStoredColOrds intsets.Fast
}

// init initializes an index recommendation. If there is an existingIndex with
// the same explicit columns, it is stored here.
func (ir *indexRecommendation) init(
	indexOrd int, hypTable *HypotheticalTable, scannedColOrds intsets.Fast,
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
func (ir *indexRecommendation) addStoredColOrds(scannedColOrds intsets.Fast) {
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

		if ir.index.Type() == idxtype.INVERTED && i == len(ir.index.cols)-1 {
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
