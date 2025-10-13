// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package indexrec

import (
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
)

// FindIndexCandidateSet returns a map storing potential indexes for each table
// referenced in a query. The index candidates are constructed based on the
// following rules:
//
//  1. Add a single index on all columns in a Group By or Order By expression if
//     the columns are from the same table. Otherwise, group expressions into
//     indexes by table. For Order By, the index column ordering and column
//     directions are the same as how it is in the Order By.
//  2. Add a single-column index on any Range expression, comparison
//     expression (=, !=, <, >, <=, >=), IS and IS NOT expression.
//  3. Add a single-column index on any column that appears in a JOIN predicate.
//  4. If there exist multiple columns from the same table in a JOIN predicate,
//     create a single index on all such columns.
//  5. Construct three groups for each table: EQ, R, and J.
//     - eq is all single-column indexes that come from equality predicates.
//     - EQ is a single index of all columns that appear in equality predicates.
//     - R is all indexes that come from rule 2.
//     - J is all indexes that come from rules 3 and 4.
//     From these groups, construct the following multi-column index
//     combinations: EQ, EQ + R, J + R, EQ + J, EQ + J + R.
//  6. Construct two single/multi-column candidates for the output columns of
//     set operations. This is in order to allow streaming set operations to
//     be performed. All set operations are considered, except for UNION ALL,
//     because indexes do not benefit here.
//  7. For JSON and array columns, we create single column inverted indexes. We
//     also create the following multi-column combination candidates for each
//     inverted column: eq + 'inverted column', EQ + 'inverted column'.
//
// TODO(nehageorge): Add a rule for columns that are referenced in the statement
// but do not fall into one of these categories. In order to account for this,
// *memo.VariableExpr would be the final case in the switch statement, hit only
// if no other expressions have been matched. See the papers referenced in this
// RFC for inspiration: https://github.com/cockroachdb/cockroach/pull/71784. We
// may also consider matching more types of SQL expressions, including LIKE
// expressions.
func FindIndexCandidateSet(
	rootExpr opt.Expr, md *opt.Metadata,
) (map[cat.Table][]IndexCandidates, error) {
	var candidateSet indexCandidateSet
	candidateSet.init(md)
	candidateSet.categorizeIndexCandidates(rootExpr)
	candidateSet.combineIndexCandidates()
	err := candidateSet.collectCandidatesWithPredicate(md)
	if err != nil {
		return nil, err
	}

	return candidateSet.finalCandidates, nil
}

// For each table in overallCandidates, we find the predicates of all partial
// indexes that already exist on the table, and add them to the predicates field
// of the indexCandidateSet struct. We then create an IndexCandidates struct for
// each index in overallCandidates, with each predicate found as well as one
// without any predicate.
func (ics *indexCandidateSet) collectCandidatesWithPredicate(md *opt.Metadata) error {
	tablesToPredicates := findExistingPredicates(md)
	for t, indexes := range ics.overallCandidates {
		for _, index := range indexes {
			// Always add the index without any predicate first
			ics.finalCandidates[t] = append(ics.finalCandidates[t], IndexCandidates{
				columns:   index,
				predicate: "",
			})

			predicates := tablesToPredicates[t]
			for _, predicate := range predicates {
				if shouldAdd, err := shouldAddPredicate(t, index, predicate); err != nil {
					return err
				} else if shouldAdd {
					ics.finalCandidates[t] = append(ics.finalCandidates[t], IndexCandidates{
						columns:   index,
						predicate: predicate,
					})
				}
			}
		}
	}
	return nil
}

// A helper function that checks a predicate against the columns of an index.
// If every column used in the predicate is present in the index columns, it is a valid index and should be evaluated as a candidate.
func shouldAddPredicate(table cat.Table, index []cat.IndexColumn, predicate string) (bool, error) {
	expr, err := parser.ParseExpr(predicate)
	if err != nil {
		return false, err
	}

	colIDs, err := extractColumnIDs(table, expr)
	if err != nil {
		return false, err
	}
	indexCols := make(map[cat.StableID]tree.Name, len(index))
	for _, column := range index {
		indexCols[column.ColID()] = column.ColName()
	}

	for _, colID := range colIDs {
		// All columnIDs found in the predicate should match, otherwise the added predicate
		// will never be provide performance benefits on the index.
		if _, ok := indexCols[colID]; !ok {
			return false, nil
		}
	}
	return true, nil
}

// Helper function that gets all the column IDs of used in a expression.
func extractColumnIDs(table cat.Table, rootExpr tree.Expr) ([]cat.StableID, error) {
	var colIDs []cat.StableID

	_, err := tree.SimpleVisit(rootExpr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		vBase, ok := expr.(tree.VarName)
		if !ok {
			return true, expr, nil
		}

		v, err := vBase.NormalizeVarName()
		if err != nil {
			return false, nil, err
		}

		c, ok := v.(*tree.ColumnItem)
		if !ok {
			return true, expr, nil
		}

		col, err := mustFindColumnByTreeName(table, c.ColumnName)
		if err != nil {
			return false, nil, err
		}

		colIDs = append(colIDs, col.ColID())
		return false, expr, nil
	})

	return colIDs, err
}

func mustFindColumnByTreeName(tbl cat.Table, name tree.Name) (*cat.Column, error) {
	if col := findColumnByTreeName(tbl, name); col != nil {
		return col, nil
	}
	return nil, pgerror.Newf(pgcode.UndefinedColumn, "column %q does not exist", name)
}

func findColumnByTreeName(tbl cat.Table, name tree.Name) *cat.Column {
	for i := 0; i < tbl.ColumnCount(); i++ {
		col := tbl.Column(i)
		if col.ColName() == name {
			return col
		}
	}
	return nil
}

// Collects the indexes that already exist on the tables in the query, and stores
// them in the predicates field of the indexCandidateSet struct.
func findExistingPredicates(md *opt.Metadata) map[cat.Table][]string {
	tableToPredicates := make(map[cat.Table][]string)
	for _, table := range md.AllTables() {
		for i := 0; i < table.Table.IndexCount(); i++ {
			index := table.Table.Index(i)
			pred, ok := index.Predicate()
			if ok && pred != "" {
				tableToPredicates[table.Table] = append(tableToPredicates[table.Table], pred)
			}
		}
	}
	return tableToPredicates
}

// indexCandidateSet stores potential indexes that could be recommended for a
// given query, as well as the query's metadata.
type indexCandidateSet struct {
	md                 *opt.Metadata
	equalCandidates    map[cat.Table][][]cat.IndexColumn
	rangeCandidates    map[cat.Table][][]cat.IndexColumn
	joinCandidates     map[cat.Table][][]cat.IndexColumn
	invertedCandidates map[cat.Table][][]cat.IndexColumn
	overallCandidates  map[cat.Table][][]cat.IndexColumn
	finalCandidates    map[cat.Table][]IndexCandidates
}

// Wraper type that can maintain the predicate associated with an index candidate.
type IndexCandidates struct {
	columns   []cat.IndexColumn
	predicate string
}

func (ic *IndexCandidates) Columns() []cat.IndexColumn {
	return ic.columns
}

func (ic *IndexCandidates) Predicate() string {
	return ic.predicate
}

// Predicate returns the predicate string.
// init allocates memory for the maps in the set.
func (ics *indexCandidateSet) init(md *opt.Metadata) {
	numTables := len(md.AllTables())
	ics.md = md
	ics.equalCandidates = make(map[cat.Table][][]cat.IndexColumn, numTables)
	ics.rangeCandidates = make(map[cat.Table][][]cat.IndexColumn, numTables)
	ics.joinCandidates = make(map[cat.Table][][]cat.IndexColumn, numTables)
	ics.invertedCandidates = make(map[cat.Table][][]cat.IndexColumn, numTables)
	ics.overallCandidates = make(map[cat.Table][][]cat.IndexColumn, numTables)
	ics.finalCandidates = make(map[cat.Table][]IndexCandidates, numTables)
}

// combineIndexCandidates adds index candidates that are combinations of
// candidates in the JOIN, EQUAL, and RANGE categories. See rule 5 in
// FindIndexCandidateSet.
func (ics *indexCandidateSet) combineIndexCandidates() {
	// Copy indexes in each category to overallCandidates without duplicates.
	copyIndexes(ics.equalCandidates, ics.overallCandidates)
	copyIndexes(ics.rangeCandidates, ics.overallCandidates)
	copyIndexes(ics.joinCandidates, ics.overallCandidates)
	copyIndexes(ics.invertedCandidates, ics.overallCandidates)

	numTables := len(ics.overallCandidates)
	equalJoinCandidates := make(map[cat.Table][][]cat.IndexColumn, numTables)
	equalGroupedCandidates := make(map[cat.Table][][]cat.IndexColumn, numTables)

	// Construct EQ, EQ + R, J + R, EQ + J, EQ + J + R, eq + (inverted),
	// EQ + (inverted).
	groupIndexesByTable(ics.equalCandidates, equalGroupedCandidates)
	copyIndexes(equalGroupedCandidates, ics.overallCandidates)
	constructIndexCombinations(equalGroupedCandidates, ics.rangeCandidates, ics.overallCandidates)
	constructIndexCombinations(ics.joinCandidates, ics.rangeCandidates, ics.overallCandidates)
	constructIndexCombinations(equalGroupedCandidates, ics.joinCandidates, equalJoinCandidates)
	copyIndexes(equalJoinCandidates, ics.overallCandidates)
	constructIndexCombinations(equalJoinCandidates, ics.rangeCandidates, ics.overallCandidates)
	constructIndexCombinations(ics.equalCandidates, ics.invertedCandidates, ics.overallCandidates)
	constructIndexCombinations(equalGroupedCandidates, ics.invertedCandidates, ics.overallCandidates)
}

// categorizeIndexCandidates finds potential index candidates for a given
// query. See FindIndexCandidateSet for the list of candidate creation rules.
func (ics *indexCandidateSet) categorizeIndexCandidates(expr opt.Expr) {
	switch expr := expr.(type) {
	case *memo.SortExpr:
		ics.addOrderingIndex(expr.ProvidedPhysical().Ordering)
	case *memo.GroupByExpr:
		ics.addMultiColumnIndex(expr.GroupingCols.ToList(), nil /* desc */, ics.overallCandidates)
	case *memo.EqExpr:
		ics.addVariableExprIndex(expr.Left, ics.equalCandidates)
		ics.addVariableExprIndex(expr.Right, ics.equalCandidates)
	case *memo.NeExpr:
		ics.addVariableExprIndex(expr.Left, ics.rangeCandidates)
		ics.addVariableExprIndex(expr.Right, ics.rangeCandidates)
	case *memo.IsExpr:
		ics.addVariableExprIndex(expr.Left, ics.equalCandidates)
	case *memo.IsNotExpr:
		ics.addVariableExprIndex(expr.Left, ics.rangeCandidates)
	case *memo.LtExpr:
		ics.addVariableExprIndex(expr.Left, ics.rangeCandidates)
		ics.addVariableExprIndex(expr.Right, ics.rangeCandidates)
	case *memo.GtExpr:
		ics.addVariableExprIndex(expr.Left, ics.rangeCandidates)
		ics.addVariableExprIndex(expr.Right, ics.rangeCandidates)
	case *memo.LeExpr:
		ics.addVariableExprIndex(expr.Left, ics.rangeCandidates)
		ics.addVariableExprIndex(expr.Right, ics.rangeCandidates)
	case *memo.GeExpr:
		ics.addVariableExprIndex(expr.Left, ics.rangeCandidates)
		ics.addVariableExprIndex(expr.Right, ics.rangeCandidates)
	case *memo.InnerJoinExpr:
		ics.addJoinIndexes(expr.On)
		ics.categorizeIndexCandidates(expr.Left)
		ics.categorizeIndexCandidates(expr.Right)
		return
	case *memo.LeftJoinExpr:
		ics.addJoinIndexes(expr.On)
		ics.categorizeIndexCandidates(expr.Left)
		ics.categorizeIndexCandidates(expr.Right)
		return
	case *memo.RightJoinExpr:
		ics.addJoinIndexes(expr.On)
		ics.categorizeIndexCandidates(expr.Left)
		ics.categorizeIndexCandidates(expr.Right)
		return
	case *memo.FullJoinExpr:
		ics.addJoinIndexes(expr.On)
		ics.categorizeIndexCandidates(expr.Left)
		ics.categorizeIndexCandidates(expr.Right)
		return
	case *memo.SemiJoinExpr:
		ics.addJoinIndexes(expr.On)
		ics.categorizeIndexCandidates(expr.Left)
		ics.categorizeIndexCandidates(expr.Right)
		return
	case *memo.AntiJoinExpr:
		ics.addJoinIndexes(expr.On)
		ics.categorizeIndexCandidates(expr.Left)
		ics.categorizeIndexCandidates(expr.Right)
		return
	case *memo.UnionExpr:
		ics.addSetOperationIndexes(expr.LeftCols, expr.RightCols)
	case *memo.IntersectExpr:
		ics.addSetOperationIndexes(expr.LeftCols, expr.RightCols)
	case *memo.IntersectAllExpr:
		ics.addSetOperationIndexes(expr.LeftCols, expr.RightCols)
	case *memo.ExceptExpr:
		ics.addSetOperationIndexes(expr.LeftCols, expr.RightCols)
	case *memo.ExceptAllExpr:
		ics.addSetOperationIndexes(expr.LeftCols, expr.RightCols)
	case *memo.FetchValExpr:
		ics.addVariableExprIndex(expr.Json, ics.overallCandidates)
	case *memo.ContainsExpr:
		ics.addVariableExprIndex(expr.Left, ics.overallCandidates)
		ics.addVariableExprIndex(expr.Right, ics.overallCandidates)
	case *memo.ContainedByExpr:
		ics.addVariableExprIndex(expr.Left, ics.overallCandidates)
		ics.addVariableExprIndex(expr.Right, ics.overallCandidates)
	case *memo.FunctionExpr:
		ics.addGeoSpatialIndexes(expr, ics.overallCandidates)
	case *memo.BBoxCoversExpr:
		ics.addVariableExprIndex(expr.Left, ics.overallCandidates)
		ics.addVariableExprIndex(expr.Right, ics.overallCandidates)
	case *memo.BBoxIntersectsExpr:
		ics.addVariableExprIndex(expr.Left, ics.overallCandidates)
		ics.addVariableExprIndex(expr.Right, ics.overallCandidates)
	}
	for i, n := 0, expr.ChildCount(); i < n; i++ {
		ics.categorizeIndexCandidates(expr.Child(i))
	}
}

// addSetOperationIndexes is used to add index candidates on the output columns
// of set operations (UNION, INTERSECT, INTERSECT ALL, EXCEPT, EXCEPT ALL).
func (ics *indexCandidateSet) addSetOperationIndexes(leftCols, rightCols opt.ColList) {
	ics.addMultiColumnIndex(leftCols, nil /* desc */, ics.overallCandidates)
	ics.addMultiColumnIndex(rightCols, nil /* desc */, ics.overallCandidates)
}

// addOrderingIndex adds indexes for a *memo.SortExpr. One index is constructed
// per table, with a column corresponding to each of the table's columns in the
// sort, in order of appearance. For example, if we have ORDER BY k DESC, i ASC,
// where k and i come from the same table, the index candidate's key columns
// would be (k DESC, i ASC).
func (ics indexCandidateSet) addOrderingIndex(ordering opt.Ordering) {
	if len(ordering) == 0 {
		return
	}
	columnList := make(opt.ColList, 0, len(ordering))
	descList := make([]bool, 0, len(ordering))

	for _, orderingCol := range ordering {
		colID := orderingCol.ID()
		tabID := ics.md.ColumnMeta(colID).Table

		// Do not add indexes on columns with no base table.
		if tabID == 0 {
			continue
		}

		columnList = append(columnList, colID)
		descList = append(descList, orderingCol.Descending())
	}
	if len(columnList) > 0 {
		ics.addMultiColumnIndex(columnList, descList, ics.overallCandidates)
	}
}

// addJoinIndexes adds single-column indexes to joinCandidates for each outer
// column in a join predicate, if these indexes do not already exist. For each
// table with multiple columns in the JOIN predicate, it also creates a single
// index on all such columns.
func (ics *indexCandidateSet) addJoinIndexes(expr memo.FiltersExpr) {
	outerCols := expr.OuterCols().ToList()
	for _, col := range outerCols {
		// TODO (Shivam): Index recommendations should not only allow JSON columns
		// to be part of inverted indexes since they are also forward indexable.
		if colinfo.ColumnTypeIsIndexable(ics.md.ColumnMeta(col).Type) &&
			ics.md.ColumnMeta(col).Type.Family() != types.JsonFamily {
			ics.addSingleColumnIndex(col, false /* desc */, ics.joinCandidates)
		} else {
			ics.addSingleColumnIndex(col, false /* desc */, ics.invertedCandidates)
		}
	}
	ics.addMultiColumnIndex(outerCols, nil /* desc */, ics.joinCandidates)
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
		if rightIndexes, found := rightIndexMap[t]; found {
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
	var leftIndexColSet intsets.Fast
	// Store left columns in a set for fast access.
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
func (ics *indexCandidateSet) addVariableExprIndex(
	expr opt.Expr, indexCandidates map[cat.Table][][]cat.IndexColumn,
) {
	switch expr := expr.(type) {
	case *memo.VariableExpr:
		col := expr.Col
		// TODO (Shivam): Index recommendations should not only allow JSON columns
		// to be part of inverted indexes since they are also forward indexable.
		if colinfo.ColumnTypeIsIndexable(ics.md.ColumnMeta(col).Type) &&
			ics.md.ColumnMeta(col).Type.Family() != types.JsonFamily {
			ics.addSingleColumnIndex(col, false /* desc */, indexCandidates)
		} else {
			ics.addSingleColumnIndex(col, false /* desc */, ics.invertedCandidates)
		}
	}
}

// addMultiColumnIndex adds indexes to indexCandidates for groups of columns
// in a column set that are from the same table, without duplicates.
func (ics *indexCandidateSet) addMultiColumnIndex(
	cols opt.ColList, desc []bool, indexCandidates map[cat.Table][][]cat.IndexColumn,
) {
	// Group columns by table in a temporary map as single-column indexes,
	// getting rid of duplicates.
	tableToCols := make(map[cat.Table][][]cat.IndexColumn, len(ics.md.AllTables()))
	for i, colID := range cols {
		if desc != nil {
			ics.addSingleColumnIndex(colID, desc[i], tableToCols)
		} else {
			ics.addSingleColumnIndex(colID, false /* desc */, tableToCols)
		}
	}

	// Combine all single-column indexes for a given table into one, and add
	// the corresponding multi-column index.
	for currTable := range tableToCols {
		index := make([]cat.IndexColumn, 0, len(tableToCols[currTable]))
		for _, colSlice := range tableToCols[currTable] {
			indexCol := colSlice[0]
			// TODO (Shivam): Index recommendations should not only allow JSON columns
			// to be part of inverted indexes since they are also forward indexable.
			if indexCol.Column.DatumType().Family() != types.JsonFamily &&
				colinfo.ColumnTypeIsIndexable(indexCol.Column.DatumType()) {
				index = append(index, indexCol)
			}
		}
		if len(index) > 0 {
			addIndexToCandidates(index, currTable, indexCandidates)
		}
	}
}

// addSingleColumnIndex adds an index to indexCandidates on the column with the
// given opt.ColumnID if it does not already exist.
func (ics *indexCandidateSet) addSingleColumnIndex(
	colID opt.ColumnID, desc bool, indexCandidates map[cat.Table][][]cat.IndexColumn,
) {
	columnMeta := ics.md.ColumnMeta(colID)

	// If there's no base table for the column, return.
	tableID := columnMeta.Table
	if tableID == 0 {
		return
	}

	// Find the column instance in the current table and add the corresponding
	// index to indexCandidates.
	currTable := ics.md.Table(tableID)
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
	// Do not add candidates from system or virtual tables.
	if currTable.IsVirtualTable() || currTable.IsSystemTable() {
		return
	}

	// Do not add indexes to PARTITION ALL BY tables.
	// TODO(rytaft): Support these tables by adding implicit partitioning columns.
	if currTable.IsPartitionAllBy() {
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

// AddGeoSpatialIndexes is used to add single-column indexes to indexCandidates
// for spatial functions that can be index-accelerated.
func (ics *indexCandidateSet) addGeoSpatialIndexes(
	expr *memo.FunctionExpr, indexCandidates map[cat.Table][][]cat.IndexColumn,
) {
	// Ensure that the function is a spatial function AND can be index-accelerated.
	_, ok := geoindex.RelationshipMap[expr.Name]
	if ok {
		// Add arguments of the spatial function to inverted indexes.
		for i, n := 0, expr.Args.ChildCount(); i < n; i++ {
			var child = expr.Args.Child(i)
			// Spatial Indexes should be added to inverted candidates group in
			// addVariableExprIndex.
			ics.addVariableExprIndex(child, indexCandidates)
		}
	}
}
