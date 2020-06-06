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
// LeftJoin or FullJoin and returns it. initJoinMultiplicity should only be
// called during construction of the join by the initUnexportedFields methods.
// Panics if called on an operator other than an InnerJoin, LeftJoin, or
// FullJoin.
func initJoinMultiplicity(in RelExpr) {
	switch t := in.(type) {
	case *InnerJoinExpr, *LeftJoinExpr, *FullJoinExpr:
		// Calculate JoinMultiplicity.
		left := t.Child(0).(RelExpr)
		right := t.Child(1).(RelExpr)
		filters := *t.Child(2).(*FiltersExpr)
		multiplicity := DeriveJoinMultiplicityFromInputs(t.Op(), left, right, filters)
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
// how a join of the given type with the given inputs and filters will affect
// the rows of its inputs. When possible, GetJoinMultiplicity should be called
// instead because DeriveJoinMultiplicityFromInputs cannot take advantage of a
// previously calculated JoinMultiplicity. The UnfilteredCols Relational
// property is used in calculating the JoinMultiplicity, and is lazily derived
// by a call to deriveUnfilteredCols.
func DeriveJoinMultiplicityFromInputs(
	joinOp opt.Operator, left, right RelExpr, filters FiltersExpr,
) props.JoinMultiplicity {

	switch joinOp {
	case opt.InnerJoinOp, opt.LeftJoinOp, opt.FullJoinOp:

	default:
		panic(errors.AssertionFailedf("invalid operator: %v", joinOp))
	}

	isLeftOuter := joinOp == opt.LeftJoinOp || joinOp == opt.FullJoinOp
	isRightOuter := joinOp == opt.FullJoinOp

	leftMultiplicity := getJoinLeftMultiplicityVal(left, right, filters, isLeftOuter)
	rightMultiplicity := getJoinLeftMultiplicityVal(right, left, filters, isRightOuter)

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
		// All un-limited, unconstrained output columns are unfiltered columns.
		if t.HardLimit == 0 && t.Constraint == nil {
			unfilteredCols.UnionWith(relational.OutputCols)
		}

	case *ProjectExpr:
		// Project never filters rows, so it passes through unfiltered columns.
		unfilteredCols.UnionWith(deriveUnfilteredCols(t.Input).Intersection(relational.OutputCols))

	case *InnerJoinExpr, *LeftJoinExpr, *FullJoinExpr:
		left := t.Child(0).(RelExpr)
		right := t.Child(1).(RelExpr)
		filters := *t.Child(2).(*FiltersExpr)
		multiplicity := DeriveJoinMultiplicityFromInputs(t.Op(), left, right, filters)

		// Use the UnfilteredCols to determine whether unfiltered columns can be
		// passed through.
		if multiplicity.JoinPreservesLeftRows() {
			unfilteredCols.UnionWith(deriveUnfilteredCols(left))
		}
		if multiplicity.JoinPreservesRightRows() {
			unfilteredCols.UnionWith(deriveUnfilteredCols(right))
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
	left, right RelExpr, filters FiltersExpr, isLeftOuter bool,
) props.MultiplicityValue {
	multiplicity := props.MultiplicityIndeterminateVal
	if filtersMatchLeftRowsAtMostOnce(left, right, filters) {
		multiplicity |= props.MultiplicityNotDuplicatedVal
	}
	if isLeftOuter || filtersMatchAllLeftRows(left, right, filters) {
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
// 2. If this is not a cross join, every filter falls under one of these two
//    cases:
//   a. The self-join case: an equality between ColumnIDs that come from the
//      same column on the same base table.
//   b. The foreign-key case: an equality between a foreign key column on the
//      left and the column it references from the right.
//
// In both the self-join and the foreign key cases, the left columns must be
// not-null, and the right columns must be unfiltered.
//
//  Why do the left columns have to be not-null and the right columns
//  unfiltered?
//  * In both the self-join and the foreign-key cases, a non-null value in
//    the left column guarantees a corresponding value in the right column. As
//    long as no nulls have been added to the left column and no values have
//    been removed from the right, this property will be valid.
//
// Note: in the foreign key case, if the key's match method is match simple, all
// columns in the foreign key must be not-null in order to guarantee that all
// rows will have a match in the referenced table.
func filtersMatchAllLeftRows(left, right RelExpr, filters FiltersExpr) bool {
	md := left.Memo().Metadata()

	// Cross join case.
	if len(filters) == 0 {
		if !right.Relational().Cardinality.CanBeZero() {
			// Case 1a: this is a cross join and there's at least one row in the right
			// input, so every left row is guaranteed to match at least once.
			return true
		}
		// Case 1b: if there is at least one not-null foreign key referencing the
		// unfiltered right columns, return true. Otherwise, false.
		return makeForeignKeyMap(
			md, left.Relational().NotNullCols, deriveUnfilteredCols(right)) != nil
	}

	leftColIDs := left.Relational().NotNullCols
	rightColIDs := deriveUnfilteredCols(right)
	if rightColIDs.Empty() {
		// Right input has no unfiltered columns.
		return false
	}

	var fkColMap map[opt.ColumnID]opt.ColumnID

	for i := range filters {
		eq, _ := filters[i].Condition.(*EqExpr)
		if eq == nil {
			// Conjunct is not an equality comparison.
			return false
		}

		leftVar, _ := eq.Left.(*VariableExpr)
		rightVar, _ := eq.Right.(*VariableExpr)
		if leftVar == nil || rightVar == nil {
			// Conjunct does not directly compare two columns.
			return false
		}

		leftColID := leftVar.Col
		rightColID := rightVar.Col

		// Normalize leftColID to come from leftColIDs.
		if !leftColIDs.Contains(leftColID) {
			leftColID, rightColID = rightColID, leftColID
		}
		if !leftColIDs.Contains(leftColID) || !rightColIDs.Contains(rightColID) {
			// Columns don't come from both sides of join, left column is nullable or
			// right column is filtered.
			return false
		}

		leftTab := md.ColumnMeta(leftColID).Table
		rightTab := md.ColumnMeta(rightColID).Table
		if leftTab == 0 || rightTab == 0 {
			// Columns don't come from base tables.
			return false
		}

		if md.TableMeta(leftTab).Table == md.TableMeta(rightTab).Table {
			// Case 2a: check self-join case.
			leftColOrd := leftTab.ColumnOrdinal(leftColID)
			rightColOrd := rightTab.ColumnOrdinal(rightColID)
			if leftColOrd != rightColOrd {
				// Left and right column ordinals do not match.
				return false
			}
		} else {
			// Case 2b: check foreign-key case.
			if fkColMap == nil {
				// Lazily construct a map from all not-null foreign key columns on the
				// left to all unfiltered referenced columns on the right.
				fkColMap = makeForeignKeyMap(md, leftColIDs, rightColIDs)
				if fkColMap == nil {
					// No valid foreign key relations were found.
					return false
				}
			}
			if refCol, ok := fkColMap[leftColID]; !ok || refCol != rightColID {
				// There is no valid foreign key relation from leftColID to
				// rightColID.
				return false
			}
		}
	}

	return true
}

// makeForeignKeyMap returns a map from left foreign key columns to right
// referenced columns. The given left columns should not be nullable and the
// right columns should be guaranteed to be unfiltered, or the foreign key
// relation may not hold. If the key's match method isn't match full, all
// foreign key columns must be not-null, or the key relation is not guaranteed
// to have a match for each row. If no valid foreign key relations are found,
// fkColMap is nil.
func makeForeignKeyMap(
	md *opt.Metadata, leftNotNullCols, rightUnfilteredCols opt.ColSet,
) map[opt.ColumnID]opt.ColumnID {
	var tableIDMap map[cat.StableID]opt.TableID
	var fkColMap map[opt.ColumnID]opt.ColumnID
	var lastSeen opt.TableID

	// Walk through the left columns and add foreign key and referenced columns to
	// the output mapping if they come from the leftNotNullCols and
	// rightUnfilteredCols ColSets respectively.
	for col, ok := leftNotNullCols.Next(0); ok; col, ok = leftNotNullCols.Next(col + 1) {
		fkTableID := md.ColumnMeta(col).Table
		if fkTableID < 1 {
			// The column does not come from a base table.
			continue
		}
		if fkTableID == lastSeen {
			// We have already encountered this TableID. (This works because ColumnIDs
			// with the same TableID are clustered together).
			continue
		}
		lastSeen = fkTableID
		fkTableMeta := md.TableMeta(fkTableID)
		if fkTableMeta.IgnoreForeignKeys {
			// We are not allowed to use any of this table's foreign keys.
			continue
		}
		fkTable := fkTableMeta.Table
		for i, cnt := 0, fkTable.OutboundForeignKeyCount(); i < cnt; i++ {
			fk := fkTable.OutboundForeignKey(i)
			if !fk.Validated() {
				// The data is not guaranteed to follow the foreign key constraint.
				continue
			}
			if tableIDMap == nil {
				// Lazily initialize tableIDMap.
				tableIDMap = makeStableTableIDMap(md, rightUnfilteredCols)
				if len(tableIDMap) == 0 {
					// No valid tables were found from the right side.
					break
				}
			}
			refTableID, ok := tableIDMap[fk.ReferencedTableID()]
			if !ok {
				// There is no valid right table corresponding to the referenced table.
				continue
			}
			var leftCols, rightCols []opt.ColumnID
			fkValid := true
			for j, numCols := 0, fk.ColumnCount(); j < numCols; j++ {
				leftOrd := fk.OriginColumnOrdinal(fkTable, j)
				rightOrd := fk.ReferencedColumnOrdinal(md.Table(refTableID), j)
				leftCol := fkTableID.ColumnID(leftOrd)
				rightCol := refTableID.ColumnID(rightOrd)
				if !leftNotNullCols.Contains(leftCol) {
					// Not all FK columns are part of the equality conditions. There are
					// two cases:
					// 1. MATCH SIMPLE/PARTIAL: if this column is nullable, rows from this
					//    foreign key are not guaranteed to match.
					// 2. MATCH FULL: FK rows are still guaranteed to match because the
					//    non-present columns can only be NULL if all FK columns are NULL.
					if fk.MatchMethod() != tree.MatchFull {
						fkValid = false
						break
					}
					continue
				}
				if !rightUnfilteredCols.Contains(rightCol) {
					continue
				}
				leftCols = append(leftCols, leftCol)
				rightCols = append(rightCols, rightCol)
			}
			if !fkValid {
				// The foreign key relations should only be added to the mapping if the
				// foreign key is guaranteed a match for every row.
				continue
			}
			for i := range leftCols {
				// Add any valid foreign key relations to the mapping.
				if fkColMap == nil {
					// Lazily initialize fkColMap
					fkColMap = map[opt.ColumnID]opt.ColumnID{}
				}
				fkColMap[leftCols[i]] = rightCols[i]
			}
		}
	}
	return fkColMap
}

// makeStableTableIDMap creates a mapping from the StableIDs of the base tables
// to the meta TableIDs for the given columns.
func makeStableTableIDMap(md *opt.Metadata, cols opt.ColSet) map[cat.StableID]opt.TableID {
	idMap := map[cat.StableID]opt.TableID{}
	for col, ok := cols.Next(0); ok; col, ok = cols.Next(col + 1) {
		metaTableID := md.ColumnMeta(col).Table
		if metaTableID == 0 {
			continue
		}
		stableTableID := md.Table(metaTableID).ID()
		if prevID, ok := idMap[stableTableID]; ok && prevID != metaTableID {
			// Avoid dealing with cases where multiple meta tables reference the same
			// base table so that only one TableID has to be stored.
			return map[cat.StableID]opt.TableID{}
		}
		idMap[stableTableID] = metaTableID
	}
	return idMap
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
