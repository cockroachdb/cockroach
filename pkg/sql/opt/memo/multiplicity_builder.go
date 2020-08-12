// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package memo

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// initJoinMultiplicity initializes a JoinMultiplicity for the given InnerJoin,
// LeftJoin, FullJoin, or SemiJoin and returns it. initJoinMultiplicity should
// only be called during construction of the join by the initUnexportedFields
// methods. Panics if called on an operator that does not support
// JoinMultiplicity.
func initJoinMultiplicity(in RelExpr) {
	switch t := in.(type) {
	case *InnerJoinExpr, *LeftJoinExpr, *FullJoinExpr, *SemiJoinExpr:
		// Calculate JoinMultiplicity and set the multiplicity field of the join.
		left := t.Child(0).(RelExpr)
		right := t.Child(1).(RelExpr)
		filters := *t.Child(2).(*FiltersExpr)
		multiplicity := DeriveJoinMultiplicityFromInputs(left, right, filters)
		t.(joinWithMultiplicity).setMultiplicity(multiplicity)

	default:
		panic(errors.AssertionFailedf("invalid operator type: %v", t.Op()))
	}
}

// GetJoinMultiplicity returns a JoinMultiplicity struct that describes how a
// join operator will affect the rows of its left and right inputs (e.g.
// duplicated and/or filtered). Panics if the method is called on an operator
// that does not support JoinMultiplicity (any operator other than an InnerJoin,
// LeftJoin, or FullJoin).
func GetJoinMultiplicity(in RelExpr) props.JoinMultiplicity {
	if join, ok := in.(joinWithMultiplicity); ok {
		// JoinMultiplicity has already been initialized during construction of the
		// join, so simply return it.
		return join.getMultiplicity()
	}
	panic(errors.AssertionFailedf("invalid operator type: %v", in.Op()))
}

// DeriveJoinMultiplicityFromInputs returns a JoinMultiplicity that describes
// how an inner join with the given inputs and filters will affect the rows of
// its inputs. When possible, GetJoinMultiplicity should be called instead
// because DeriveJoinMultiplicityFromInputs cannot take advantage of a
// previously calculated JoinMultiplicity. The UnfilteredCols Relational
// property is used in calculating the JoinMultiplicity, and is lazily derived
// by a call to deriveUnfilteredCols.
func DeriveJoinMultiplicityFromInputs(
	left, right RelExpr, filters FiltersExpr,
) props.JoinMultiplicity {
	leftMultiplicity := getJoinLeftMultiplicityVal(left, right, filters)
	rightMultiplicity := getJoinLeftMultiplicityVal(right, left, filters)

	return props.JoinMultiplicity{
		LeftMultiplicity:  leftMultiplicity,
		RightMultiplicity: rightMultiplicity,
	}
}

// deriveUnfilteredCols recursively derives the UnfilteredCols field and
// populates the props.Relational.Rule.UnfilteredCols field as it goes to
// make future calls faster.
func deriveUnfilteredCols(in RelExpr) opt.ColSet {
	// If the UnfilteredCols property has already been derived, return it
	// immediately.
	relational := in.Relational()
	if relational.IsAvailable(props.UnfilteredCols) {
		return relational.Rule.UnfilteredCols
	}
	relational.Rule.Available |= props.UnfilteredCols
	unfilteredCols := opt.ColSet{}

	// Derive UnfilteredCols now.
	switch t := in.(type) {
	case *ScanExpr:
		// If no rows are being removed from the table, add all its columns. No rows
		// are removed from a table if this is an un-limited, unconstrained scan
		// over a non-partial index.
		//
		// We add all columns (not just output columns) because while non-output
		// columns cannot be used by operators, they can be used to make a statement
		// about the cardinality of their owner table. As an example, take this
		// query:
		//
		//   CREATE TABLE xy (x INT PRIMARY KEY, y INT);
		//   CREATE TABLE kr (k INT PRIMARY KEY, r INT NOT NULL REFERENCES xy(x));
		//   SELECT k FROM kr INNER JOIN xy ON True;
		//
		// Rows from the left side of this query will be preserved because of the
		// non-null foreign key relation - rows in kr imply rows in xy. However,
		// the columns from xy are not output columns, so in order to see that this
		// is the case we must bubble up non-output columns.
		md := t.Memo().Metadata()
		baseTable := md.Table(t.Table)
		if t.IsUnfiltered(md) {
			for i, cnt := 0, baseTable.ColumnCount(); i < cnt; i++ {
				unfilteredCols.Add(t.Table.ColumnID(i))
			}
		}

	case *ProjectExpr:
		// Project never filters rows, so it passes through unfiltered columns.
		// Include non-output columns for the same reasons as for the Scan operator.
		unfilteredCols.UnionWith(deriveUnfilteredCols(t.Input))

	case *InnerJoinExpr, *LeftJoinExpr, *FullJoinExpr:
		left := t.Child(0).(RelExpr)
		right := t.Child(1).(RelExpr)
		multiplicity := GetJoinMultiplicity(t)

		// Use the UnfilteredCols to determine whether unfiltered columns can be
		// passed through.
		if multiplicity.JoinPreservesLeftRows(t.Op()) {
			unfilteredCols.UnionWith(deriveUnfilteredCols(left))
		}
		if multiplicity.JoinPreservesRightRows(t.Op()) {
			unfilteredCols.UnionWith(deriveUnfilteredCols(right))
		}

	case *SemiJoinExpr:
		multiplicity := GetJoinMultiplicity(t)
		if multiplicity.JoinPreservesLeftRows(t.Op()) {
			unfilteredCols.UnionWith(deriveUnfilteredCols(t.Left))
		}

	default:
		// An empty ColSet is returned.
	}
	relational.Rule.UnfilteredCols = unfilteredCols
	return relational.Rule.UnfilteredCols
}

// getJoinLeftMultiplicityVal returns a MultiplicityValue that describes whether
// a join with the given properties would duplicate or filter the rows of its
// left input.
//
// The duplicated and filtered flags will be set unless it can be statically
// proven that no rows will be duplicated or filtered respectively.
func getJoinLeftMultiplicityVal(left, right RelExpr, filters FiltersExpr) props.MultiplicityValue {
	multiplicity := props.MultiplicityIndeterminateVal
	if filtersMatchLeftRowsAtMostOnce(left, right, filters) {
		multiplicity |= props.MultiplicityNotDuplicatedVal
	}
	if filtersMatchAllLeftRows(left, right, filters) {
		multiplicity |= props.MultiplicityPreservedVal
	}
	return multiplicity
}

// filtersMatchLeftRowsAtMostOnce returns true if a join expression with the
// given ON filters is guaranteed to match every left row at most once. This is
// the case when either of the following conditions is satisfied:
//
//  1. The join is a cross join and the right input has zero or one rows.
//
//  2. The equivalence closure of the left columns over the filter functional
//     dependencies forms a lax key over the right columns.
//
// Why is condition #2 sufficient to ensure that no left rows are matched more
// than once?
// * It implies that left columns are being equated with a lax key from the
//   right input.
// * A lax key means that the right rows being equated are unique apart from
//   nulls.
// * Equalities are null-rejecting and the right rows are otherwise unique, so
//   no left row can be equal to more than one right row on the filters.
// * Therefore, no left row will be matched more than once.
//
// As an example:
//
//   CREATE TABLE x_tab (x INT);
//   CREATE TABLE a_tab (a INT UNIQUE);
//
//   x     a
//   ----  ----
//   NULL  NULL
//   1     1
//   1     2
//   2     3
//
//   SELECT * FROM x_tab INNER JOIN a_tab ON x = a;
//   =>
//   x a
//   ---
//   1 1
//   1 1
//   2 2
//
// In this example, no rows from x are duplicated, while the '1' row from a is
// duplicated.
func filtersMatchLeftRowsAtMostOnce(left, right RelExpr, filters FiltersExpr) bool {
	// Condition #1.
	if len(filters) == 0 && right.Relational().Cardinality.IsZeroOrOne() {
		return true
	}

	// Condition #2.
	filtersFDs := getFiltersFDs(filters)
	closure := filtersFDs.ComputeEquivClosure(left.Relational().OutputCols)
	return right.Relational().FuncDeps.ColsAreLaxKey(closure)
}

// filtersMatchAllLeftRows returns true when each row in the given join's left
// input can be guaranteed to match at least one row from the right input,
// according to the join filters. This is true when the following conditions are
// satisfied:
//
// 1. If this is a cross join (there are no filters), then either:
//   a. The minimum cardinality of the right input is greater than zero. There
//      must be at least one right row for the left rows to be preserved.
//   b. There is a not-null foreign key column in the left input that references
//      an unfiltered column from the right input.
//
// 2. If this is not a cross join, every filter is an equality that falls under
//    one of these two cases:
//   a. The self-join case: all equalities are between ColumnIDs that come from
//      the same column on the same base table.
//   b. The foreign-key case: all equalities are between a foreign key column on
//      the left and the column it references from the right. All left columns
//      must come from the same foreign key.
//
// In both the self-join and the foreign key cases, the left columns must be
// not-null, and the right columns must be unfiltered.
//
// Why do the left columns have to be not-null and the right columns
// unfiltered? In both the self-join and the foreign-key cases, a non-null
// value in the left column guarantees a corresponding value in the right
// column. As long as no nulls have been added to the left column and no values
// have been removed from the right, this property will be valid.
//
// Why do all foreign key columns in the foreign key case have to come from the
// same foreign key? Equalities on different foreign keys may each be
// guaranteed to match with *some* right row, but they are not guaranteed to
// match on the same row. Therefore, filters on more than one foreign key are
// not guaranteed to preserve all left rows.
//
// Note: in the foreign key case, if the key's match method is match simple, all
// columns in the foreign key must be not-null in order to guarantee that all
// rows will have a match in the referenced table.
func filtersMatchAllLeftRows(left, right RelExpr, filters FiltersExpr) bool {
	if filters.IsTrue() {
		// Cross join case.
		if !right.Relational().Cardinality.CanBeZero() {
			// Case 1a.
			return true
		}
		// Case 1b. We don't have to check verifyFiltersAreValidEqualities because
		// there are no filters.
		return checkForeignKeyCase(
			left.Memo().Metadata(),
			left.Relational().NotNullCols,
			deriveUnfilteredCols(right),
			filters,
		)
	}
	if !verifyFiltersAreValidEqualities(left, right, filters) {
		return false
	}
	if checkSelfJoinCase(left.Memo().Metadata(), filters) {
		// Case 2a.
		return true
	}
	// Case 2b.
	return checkForeignKeyCase(
		left.Memo().Metadata(), left.Relational().NotNullCols, deriveUnfilteredCols(right), filters)
}

// verifyFiltersAreValidEqualities returns true when all of the following
// conditions are satisfied:
// 1. All filters are equalities.
// 2. All equalities directly compare two columns.
// 3. All equalities contain one column from the left not-null columns, and one
//    column from the right unfiltered columns.
// 4. All equality columns come from a base table.
// 5. All left columns come from a single table, and all right columns come from
//    a single table.
func verifyFiltersAreValidEqualities(left, right RelExpr, filters FiltersExpr) bool {
	md := left.Memo().Metadata()

	var leftTab, rightTab opt.TableID
	leftNotNullCols := left.Relational().NotNullCols
	rightUnfilteredCols := deriveUnfilteredCols(right)
	if rightUnfilteredCols.Empty() {
		// There are no unfiltered columns from the right input.
		return false
	}

	for i := range filters {
		eq, _ := filters[i].Condition.(*EqExpr)
		if eq == nil {
			// Condition #1: Conjunct is not an equality comparison.
			return false
		}

		leftVar, _ := eq.Left.(*VariableExpr)
		rightVar, _ := eq.Right.(*VariableExpr)
		if leftVar == nil || rightVar == nil {
			// Condition #2: Conjunct does not directly compare two columns.
			return false
		}

		leftColID := leftVar.Col
		rightColID := rightVar.Col

		// Normalize leftColID to come from leftColIDs.
		if !leftNotNullCols.Contains(leftColID) {
			leftColID, rightColID = rightColID, leftColID
		}
		if !leftNotNullCols.Contains(leftColID) || !rightUnfilteredCols.Contains(rightColID) {
			// Condition #3: Columns don't come from both the left and right ColSets.
			return false
		}

		if leftTab == 0 || rightTab == 0 {
			// Initialize the left and right tables.
			leftTab = md.ColumnMeta(leftColID).Table
			rightTab = md.ColumnMeta(rightColID).Table
			if leftTab == 0 || rightTab == 0 {
				// Condition #4: Columns don't come from base tables.
				return false
			}
		}
		if leftTab != md.ColumnMeta(leftColID).Table || rightTab != md.ColumnMeta(rightColID).Table {
			// Condition #5: The filter conditions reference more than one table from
			// each side.
			return false
		}
	}
	return true
}

// checkSelfJoinCase returns true if all equalities in the given FiltersExpr
// are between columns from the same position in the same base table. Panics
// if verifyFilters is not checked first.
func checkSelfJoinCase(md *opt.Metadata, filters FiltersExpr) bool {
	for i := range filters {
		eq, _ := filters[i].Condition.(*EqExpr)
		leftColID := eq.Left.(*VariableExpr).Col
		rightColID := eq.Right.(*VariableExpr).Col
		leftTab := md.ColumnMeta(leftColID).Table
		rightTab := md.ColumnMeta(rightColID).Table
		if md.Table(leftTab) != md.Table(rightTab) {
			// The columns are not from the same table.
			return false
		}
		if leftTab.ColumnOrdinal(leftColID) != rightTab.ColumnOrdinal(rightColID) {
			// The columns are not from the same position in the base table.
			return false
		}
	}
	return true
}

// checkForeignKeyCase returns true if all equalities in the given FiltersExpr
// are between not-null foreign key columns on the left and unfiltered
// referenced columns on the right. Panics if verifyFiltersAreValidEqualities is
// not checked first.
func checkForeignKeyCase(
	md *opt.Metadata, leftNotNullCols, rightUnfilteredCols opt.ColSet, filters FiltersExpr,
) bool {
	if rightUnfilteredCols.Empty() {
		// There are no unfiltered columns from the right; a valid foreign key
		// relation is not possible.
		return false
	}

	var rightTableIDs []opt.TableID

	seen := opt.TableID(0)
	for col, ok := leftNotNullCols.Next(0); ok; col, ok = leftNotNullCols.Next(col + 1) {
		leftTableID := md.ColumnMeta(col).Table
		if leftTableID == 0 || leftTableID == seen {
			// Either this column doesn't come from a base table or we have already
			// encountered this table. This check works because ColumnIDs from the
			// same table are sequential.
			continue
		}
		tableMeta := md.TableMeta(leftTableID)
		if tableMeta.IgnoreForeignKeys {
			// We can't use foreign keys from this table.
			continue
		}
		leftBaseTable := md.Table(leftTableID)
		for i, cnt := 0, leftBaseTable.OutboundForeignKeyCount(); i < cnt; i++ {
			fk := leftBaseTable.OutboundForeignKey(i)
			if !fk.Validated() {
				// The data is not guaranteed to follow the foreign key constraint.
				continue
			}
			if rightTableIDs == nil {
				// Lazily construct rightTableIDs.
				rightTableIDs = getTableIDsFromCols(md, rightUnfilteredCols)
			}
			rightTableID, ok := getTableIDFromStableID(md, fk.ReferencedTableID(), rightTableIDs)
			if !ok {
				// The referenced table isn't from the right input.
				continue
			}
			fkValid := true
			numMatches := 0
			for j, numCols := 0, fk.ColumnCount(); j < numCols; j++ {
				leftColOrd := fk.OriginColumnOrdinal(leftBaseTable, j)
				rightColOrd := fk.ReferencedColumnOrdinal(md.Table(rightTableID), j)
				leftColID := leftTableID.ColumnID(leftColOrd)
				rightColID := rightTableID.ColumnID(rightColOrd)
				if !leftNotNullCols.Contains(leftColID) {
					// The left column isn't a left not-null output column. It can't be
					// used in a filter (since it isn't an output column) but the foreign key may still be valid.but it may still
					// be provably not null.
					//
					// A column that isn't in leftNotNullCols is guaranteed not-null if it
					// was not nullable in the base table. Since this left table was
					// retrieved from a not-null column, we know that columns from this
					// table have not been null-extended. Therefore, if the column was
					// originally not-null, it is still not-null.
					if leftBaseTable.Column(leftColOrd).IsNullable() {
						// This FK column is not guaranteed not-null. There are two cases:
						// 1. MATCH SIMPLE/PARTIAL: if this column is nullable, rows from
						//    this foreign key are not guaranteed to match.
						// 2. MATCH FULL: FK rows are still guaranteed to match because the
						//    non-present columns can only be NULL if all FK columns are
						//    NULL.
						if fk.MatchMethod() != tree.MatchFull {
							fkValid = false
							break
						}
					}
					// The left column isn't a left not-null output column, so it can't
					// be used in a filter.
					continue
				}
				if !rightUnfilteredCols.Contains(rightColID) {
					// The right column isn't guaranteed to be unfiltered. It can't be
					// used in a filter.
					continue
				}
				if filtersHaveEquality(filters, leftColID, rightColID) {
					numMatches++
				}
			}
			if fkValid && numMatches == len(filters) {
				// The foreign key is valid and all the filters conditions follow it.
				// Checking that numMatches is equal to the length of the filters works
				// because no two foreign key columns are the same, so any given foreign
				// key relation can only match a single equality.
				return true
			}
		}
	}
	return false
}

// filtersHaveEquality returns true if one of the equalities in the given
// FiltersExpr is between the two given columns. Panics if verifyFilters is not
// checked first.
func filtersHaveEquality(filters FiltersExpr, leftCol, rightCol opt.ColumnID) bool {
	for i := range filters {
		eq, _ := filters[i].Condition.(*EqExpr)
		leftColID := eq.Left.(*VariableExpr).Col
		rightColID := eq.Right.(*VariableExpr).Col

		if (leftColID == leftCol && rightColID == rightCol) ||
			(rightColID == leftCol && leftColID == rightCol) {
			// This filter is between the two given columns.
			return true
		}
	}
	return false
}

// getTableIDFromStableID iterates through the given slice of TableIDs and
// returns the first TableID that comes from the table with the given StableID.
// If no such TableID is found, returns 0 and false.
func getTableIDFromStableID(
	md *opt.Metadata, stableID cat.StableID, tableIDs []opt.TableID,
) (opt.TableID, bool) {
	for _, tableID := range tableIDs {
		if md.Table(tableID).ID() == stableID {
			return tableID, true
		}
	}
	return opt.TableID(0), false
}

// getTableIDsFromCols returns all unique TableIDs from the given ColSet.
// Columns that don't come from a base table are ignored.
func getTableIDsFromCols(md *opt.Metadata, cols opt.ColSet) (tables []opt.TableID) {
	seen := opt.TableID(0)
	for col, ok := cols.Next(0); ok; col, ok = cols.Next(col + 1) {
		tableID := md.ColumnMeta(col).Table
		if tableID == 0 || tableID == seen {
			// Either this column doesn't come from a base table or we have already
			// encountered this table. This check works because ColumnIDs from the
			// same table are sequential.
			continue
		}
		tables = append(tables, tableID)
	}
	return tables
}

// getFiltersFDs returns a FuncDepSet with the FDs from the FiltersItems in
// the given FiltersExpr.
func getFiltersFDs(filters FiltersExpr) props.FuncDepSet {
	if len(filters) == 1 {
		return filters[0].ScalarProps().FuncDeps
	}

	filtersFDs := props.FuncDepSet{}
	for i := range filters {
		filtersFDs.AddFrom(&filters[i].ScalarProps().FuncDeps)
	}
	return filtersFDs
}
