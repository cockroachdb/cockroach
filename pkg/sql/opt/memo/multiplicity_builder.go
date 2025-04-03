// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
func initJoinMultiplicity(mem *Memo, in RelExpr) {
	switch t := in.(type) {
	case *InnerJoinExpr, *LeftJoinExpr, *FullJoinExpr, *SemiJoinExpr:
		// Calculate JoinMultiplicity and set the multiplicity field of the join.
		left := t.Child(0).(RelExpr)
		right := t.Child(1).(RelExpr)
		filters := *t.Child(2).(*FiltersExpr)
		multiplicity := DeriveJoinMultiplicityFromInputs(mem, left, right, filters)
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
	mem *Memo, left, right RelExpr, filters FiltersExpr,
) props.JoinMultiplicity {
	leftMultiplicity := getJoinLeftMultiplicityVal(mem, left, right, filters)
	rightMultiplicity := getJoinLeftMultiplicityVal(mem, right, left, filters)

	return props.JoinMultiplicity{
		LeftMultiplicity:  leftMultiplicity,
		RightMultiplicity: rightMultiplicity,
	}
}

// deriveUnfilteredCols recursively derives the UnfilteredCols field and
// populates the props.Relational.Rule.UnfilteredCols field as it goes to
// make future calls faster.
func deriveUnfilteredCols(mem *Memo, in RelExpr) opt.ColSet {
	// If the UnfilteredCols property has already been derived, return it
	// immediately.
	relational := in.Relational()
	if relational.IsAvailable(props.UnfilteredCols) {
		return relational.Rule.UnfilteredCols
	}
	relational.SetAvailable(props.UnfilteredCols)
	unfilteredCols := opt.ColSet{}

	// Derive UnfilteredCols now.
	switch t := in.(type) {
	case *ScanExpr:
		// If no rows are being removed from the table, add all its columns. No rows
		// are removed from a table if this is an un-limited, unconstrained scan
		// over a non-partial index that does not use SKIP LOCKED.
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
		md := mem.Metadata()
		baseTable := md.Table(t.Table)
		if t.IsUnfiltered(md) {
			for i, cnt := 0, baseTable.ColumnCount(); i < cnt; i++ {
				unfilteredCols.Add(t.Table.ColumnID(i))
			}
		}

	case *ProjectExpr:
		// Project never filters rows, so it passes through unfiltered columns.
		// Include non-output columns for the same reasons as for the Scan operator.
		unfilteredCols.UnionWith(deriveUnfilteredCols(mem, t.Input))

	case *InnerJoinExpr, *LeftJoinExpr, *FullJoinExpr:
		left := t.Child(0).(RelExpr)
		right := t.Child(1).(RelExpr)
		multiplicity := GetJoinMultiplicity(t)

		// Use the join's multiplicity to determine whether unfiltered columns
		// can be passed through.
		if multiplicity.JoinPreservesLeftRows(t.Op()) {
			unfilteredCols.UnionWith(deriveUnfilteredCols(mem, left))
		}
		if multiplicity.JoinPreservesRightRows(t.Op()) {
			unfilteredCols.UnionWith(deriveUnfilteredCols(mem, right))
		}

	case *SemiJoinExpr:
		multiplicity := GetJoinMultiplicity(t)
		if multiplicity.JoinPreservesLeftRows(t.Op()) {
			unfilteredCols.UnionWith(deriveUnfilteredCols(mem, t.Left))
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
func getJoinLeftMultiplicityVal(
	mem *Memo, left, right RelExpr, filters FiltersExpr,
) props.MultiplicityValue {
	multiplicity := props.MultiplicityIndeterminateVal
	if filtersMatchLeftRowsAtMostOnce(left, right, filters) {
		multiplicity |= props.MultiplicityNotDuplicatedVal
	}
	if filtersMatchAllLeftRows(mem, left, right, filters) {
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
//   - It implies that left columns are being equated with a lax key from the
//     right input.
//   - A lax key means that the right rows being equated are unique apart from
//     nulls.
//   - Equalities are null-rejecting and the right rows are otherwise unique, so
//     no left row can be equal to more than one right row on the filters.
//   - Therefore, no left row will be matched more than once.
//
// As an example:
//
//	CREATE TABLE x_tab (x INT);
//	CREATE TABLE a_tab (a INT UNIQUE);
//
//	x     a
//	----  ----
//	NULL  NULL
//	1     1
//	1     2
//	2     3
//
//	SELECT * FROM x_tab INNER JOIN a_tab ON x = a;
//	=>
//	x a
//	---
//	1 1
//	1 1
//	2 2
//
// In this example, no rows from x are duplicated, while the '1' row from a is
// duplicated.
func filtersMatchLeftRowsAtMostOnce(left, right RelExpr, filters FiltersExpr) bool {
	// Condition #1.
	if len(filters) == 0 && right.Relational().Cardinality.IsZeroOrOne() {
		return true
	}

	// Condition #2.
	equivClosure := left.Relational().OutputCols
	if len(filters) > 0 {
		// Only copy if necessary since the resulting allocations can become
		// significant for complex queries with many joins.
		equivClosure = equivClosure.Copy()
	}
	for i := range filters {
		equivClosure = filters[i].ScalarProps().FuncDeps.ComputeEquivClosureNoCopy(equivClosure)
	}
	return right.Relational().FuncDeps.ColsAreLaxKey(equivClosure)
}

// filtersMatchAllLeftRows returns true when each row in the given join's left
// input can be guaranteed to match at least one row from the right input,
// according to the join filters. This is true when the following conditions are
// satisfied:
//
//  1. If this is a cross join (there are no filters), then either:
//     a. The minimum cardinality of the right input is greater than zero. There
//     must be at least one right row for the left rows to be preserved.
//     b. There is a not-null foreign key column in the left input that references
//     an unfiltered column from the right input.
//
//  2. If this is not a cross join, every filter is an equality that falls under
//     one of these two cases:
//     a. The self-join case: all equalities are between ColumnIDs that come from
//     the same column on the same base table.
//     b. The foreign-key case: all equalities are between a foreign key column on
//     the left and the column it references from the right. All left columns
//     must come from the same foreign key.
//
// In both the self-join and the foreign key cases that are not cross-joins
// (cases 2a and 2b):
//
//   - The left columns must be not-null, and
//   - One of the following must be true:
//   - The right columns are unfiltered, or
//   - The left and right side of the join must be Select expressions where
//     the left side filters imply the right side filters, and the right
//     columns - are unfiltered in the right Select's input (see condition
//     #3b in the comment for verifyFilterAreValidEqualities).
//
// Why do the left columns have to be non-null, and the right columns unfiltered
// or filtered identically as their corresponding left column? In both the
// self-join and the foreign-key cases, a non-null value in the left column
// guarantees a corresponding value in the right column. As long as no nulls
// have been added to the left column and no values have been removed from the
// right that have not also been removed from the left, this property will be
// valid.
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
func filtersMatchAllLeftRows(mem *Memo, left, right RelExpr, filters FiltersExpr) bool {
	if filters.IsTrue() {
		// Cross join case.
		if !right.Relational().Cardinality.CanBeZero() {
			// Case 1a.
			return true
		}
		// Case 1b. We don't have to check verifyFiltersAreValidEqualities because
		// there are no filters.
		return checkForeignKeyCase(
			mem.Metadata(),
			left.Relational().NotNullCols,
			deriveUnfilteredCols(mem, right),
			filters,
		)
	}
	rightEqualityCols, ok := verifyFiltersAreValidEqualities(mem, left, right, filters)
	if !ok {
		return false
	}
	if checkSelfJoinCase(mem.Metadata(), filters) {
		// Case 2a.
		return true
	}
	// Case 2b.
	return checkForeignKeyCase(
		mem.Metadata(),
		left.Relational().NotNullCols,
		rightEqualityCols,
		filters,
	)
}

// verifyFiltersAreValidEqualities returns the set of equality columns in the
// right relation and true when all the following conditions are satisfied:
//
//  1. All filters are equalities.
//  2. All equalities directly compare two columns.
//  3. All equalities x=y (or y=x) have x as a left non-null column and y as a
//     right column, and either:
//     a. y is an unfiltered column in the right expression, or
//     b. both the left and right expressions are Selects; the left side
//     filters imply the right side filters when replacing x with y; and y
//     is an unfiltered column in the right Select's input.
//  4. All equality columns come from a base table.
//  5. All left columns come from a single table, and all right columns come
//     from a single table.
//
// Returns ok=false if any of these conditions are unsatisfied.
func verifyFiltersAreValidEqualities(
	mem *Memo, left, right RelExpr, filters FiltersExpr,
) (rightEqualityCols opt.ColSet, ok bool) {
	md := mem.Metadata()

	var leftTab, rightTab opt.TableID
	leftNotNullCols := left.Relational().NotNullCols
	rightUnfilteredCols := deriveUnfilteredCols(mem, right)

	for i := range filters {
		eq, _ := filters[i].Condition.(*EqExpr)
		if eq == nil {
			// Condition #1: Conjunct is not an equality comparison.
			return opt.ColSet{}, false
		}

		leftVar, _ := eq.Left.(*VariableExpr)
		rightVar, _ := eq.Right.(*VariableExpr)
		if leftVar == nil || rightVar == nil {
			// Condition #2: Conjunct does not directly compare two columns.
			return opt.ColSet{}, false
		}

		leftColID := leftVar.Col
		rightColID := rightVar.Col

		// Normalize leftColID to come from leftColIDs.
		if !leftNotNullCols.Contains(leftColID) {
			leftColID, rightColID = rightColID, leftColID
			if !leftNotNullCols.Contains(leftColID) {
				// Condition #3: Left column is not guaranteed to be non-null.
				return opt.ColSet{}, false
			}
		}

		switch {
		case rightUnfilteredCols.Contains(rightColID):
		// Condition #3a: the right column is unfiltered.
		case rightHasSingleFilterThatMatchesLeft(mem, left, right, leftColID, rightColID):
		// Condition #3b: The left and right are Selects where the left filters
		// imply the right filters when replacing the left column with the right
		// column, and the right column is unfiltered in the right Select's
		// input.
		default:
			return opt.ColSet{}, false
		}

		if leftTab == 0 || rightTab == 0 {
			// Initialize the left and right tables.
			leftTab = md.ColumnMeta(leftColID).Table
			rightTab = md.ColumnMeta(rightColID).Table
			if leftTab == 0 || rightTab == 0 {
				// Condition #4: Columns don't come from base tables.
				return opt.ColSet{}, false
			}
		}
		if leftTab != md.ColumnMeta(leftColID).Table || rightTab != md.ColumnMeta(rightColID).Table {
			// Condition #5: The filter conditions reference more than one table from
			// each side.
			return opt.ColSet{}, false
		}

		rightEqualityCols.Add(rightColID)
	}

	return rightEqualityCols, true
}

// rightHasSingleFilterThatMatchesLeft returns true if:
//
//  1. Both left and right are Select expressions.
//  2. rightCol is unfiltered in right's input.
//  3. The left Select has a filter in the form leftCol=const.
//  4. The right Select has a single filter in the form rightCol=const where
//     the const value is the same as the const value in (2).
//
// This function is used by verifyFiltersAreValidEqualities to try to prove that
// every row in the left input of a join will have a match in the right input
// (see condition #3b in the comment of verifyFiltersAreValidEqualities).
//
// TODO(mgartner): Extend this to return true when the left filters imply the
// right filters, after remapping leftCol to rightCol in the left filters. For
// example, leftCol<10 implies rightCol<20 when leftCol and rightCol are held
// equal by the join filters. This may be a good opportunity to reuse
// partialidx.Implicator. Be aware that it might not be possible to simply
// replace columns in a filter when one of the columns has a composite type.
func rightHasSingleFilterThatMatchesLeft(
	mem *Memo, left, right RelExpr, leftCol, rightCol opt.ColumnID,
) bool {
	leftSelect, ok := left.(*SelectExpr)
	if !ok {
		return false
	}
	rightSelect, ok := right.(*SelectExpr)
	if !ok {
		return false
	}

	// Return false if the right column has been filtered in the input to
	// rightSelect.
	rightUnfilteredCols := deriveUnfilteredCols(mem, rightSelect.Input)
	if !rightUnfilteredCols.Contains(rightCol) {
		return false
	}

	// Return false if rightSelect has more than one filter.
	if len(rightSelect.Filters) > 1 {
		return false
	}

	// constValueForCol searches for an expression in the form
	// (Eq (Var col) Const) and returns the Const expression, if one is found.
	constValueForCol := func(filters FiltersExpr, col opt.ColumnID) (_ *ConstExpr, ok bool) {
		var constant *ConstExpr
		for i := range filters {
			if !filters[i].ScalarProps().OuterCols.Contains(col) {
				continue
			}
			eq, _ := filters[i].Condition.(*EqExpr)
			if eq == nil {
				continue
			}
			v, _ := eq.Left.(*VariableExpr)
			c, _ := eq.Right.(*ConstExpr)
			if v == nil || v.Col != col || c == nil {
				continue
			}
			constant = c
		}
		return constant, constant != nil
	}

	leftConst, ok := constValueForCol(leftSelect.Filters, leftCol)
	if !ok {
		return false
	}
	rightConst, ok := constValueForCol(rightSelect.Filters, rightCol)
	if !ok {
		return false
	}
	return leftConst == rightConst
}

// checkSelfJoinCase returns true if all equalities in the given FiltersExpr
// are between columns from the same position in the same base table.
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
// referenced columns on the right.
func checkForeignKeyCase(
	md *opt.Metadata, leftNotNullCols, rightUnfilteredCols opt.ColSet, filters FiltersExpr,
) bool {
	if rightUnfilteredCols.Empty() {
		// There are no unfiltered columns from the right; a valid foreign key
		// relation is not possible. This check, which is a duplicate of a check
		// in verifyFiltersAreValidEqualities, is necessary in the case of a
		// cross-join because verifyFiltersAreValidEqualities is not called (see
		// case 1a in filtersMatchAllLeftRows).
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
					// used in a filter (since it isn't an output column) but it may still
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
					// The right column isn't guaranteed to be unfiltered. It
					// can't be used in a filter. This check, which is a
					// duplicate of a check in verifyFiltersAreValidEqualities,
					// is necessary in the case of a cross-join because
					// verifyFiltersAreValidEqualities is not called (see case
					// 1a in filtersMatchAllLeftRows).
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
