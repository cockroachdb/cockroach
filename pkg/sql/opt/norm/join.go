// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package norm

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// ----------------------------------------------------------------------
//
// Join Rules
//   Custom match and replace functions used with join.opt rules.
//
// ----------------------------------------------------------------------

// EmptyJoinPrivate returns an unset JoinPrivate.
func (c *CustomFuncs) EmptyJoinPrivate() *memo.JoinPrivate {
	return memo.EmptyJoinPrivate
}

// ConstructNonLeftJoin maps a left join to an inner join and a full join to a
// right join when it can be proved that the right side of the join always
// produces at least one row for every row on the left.
func (c *CustomFuncs) ConstructNonLeftJoin(
	joinOp opt.Operator, left, right memo.RelExpr, on memo.FiltersExpr, private *memo.JoinPrivate,
) memo.RelExpr {
	switch joinOp {
	case opt.LeftJoinOp:
		return c.f.ConstructInnerJoin(left, right, on, private)
	case opt.LeftJoinApplyOp:
		return c.f.ConstructInnerJoinApply(left, right, on, private)
	case opt.FullJoinOp:
		return c.f.ConstructRightJoin(left, right, on, private)
	}
	panic(errors.AssertionFailedf("unexpected join operator: %v", log.Safe(joinOp)))
}

// SimplifyNotNullEquality simplifies an expression of the following form:
//
//   (Is | IsNot (Eq) (True | False | Null))
//
// in the case where the Eq expression is guaranteed to never result in null.
// The testOp argument must be IsOp or IsNotOp, and the constOp argument must be
// TrueOp, FalseOp, or NullOp.
func (c *CustomFuncs) SimplifyNotNullEquality(
	eq opt.ScalarExpr, testOp, constOp opt.Operator,
) opt.ScalarExpr {
	switch testOp {
	case opt.IsOp:
		switch constOp {
		case opt.TrueOp:
			return eq
		case opt.FalseOp:
			return c.f.ConstructNot(eq)
		case opt.NullOp:
			return c.f.ConstructFalse()
		}

	case opt.IsNotOp:
		switch constOp {
		case opt.TrueOp:
			return c.f.ConstructNot(eq)
		case opt.FalseOp:
			return eq
		case opt.NullOp:
			return c.f.ConstructTrue()
		}
	}
	panic(errors.AssertionFailedf("invalid ops: %v, %v", testOp, constOp))
}

// CanMapJoinOpEqualities checks whether it is possible to map equality
// conditions in a join to use different variables so that the number of
// conditions crossing both sides of a join are minimized.
// See canMapJoinOpEquivalenceGroup for details.
func (c *CustomFuncs) CanMapJoinOpEqualities(
	filters memo.FiltersExpr, leftCols, rightCols opt.ColSet,
) bool {
	var equivFD props.FuncDepSet
	for i := range filters {
		equivFD.AddEquivFrom(&filters[i].ScalarProps().FuncDeps)
	}
	equivReps := equivFD.EquivReps()

	for col, ok := equivReps.Next(0); ok; col, ok = equivReps.Next(col + 1) {
		if c.canMapJoinOpEquivalenceGroup(filters, col, leftCols, rightCols, equivFD) {
			return true
		}
	}

	return false
}

// canMapJoinOpEquivalenceGroup checks whether it is possible to map equality
// conditions in a join that form an equivalence group to use different
// variables so that the number of conditions crossing both sides of a join
// are minimized.
//
// Specifically, it finds the set of columns containing col that forms an
// equivalence group in the provided FuncDepSet, equivFD, which should contain
// the equivalence dependencies from the filters. It splits that group into
// columns from the left and right sides of the join, and checks whether there
// are multiple equality conditions in filters that connect the two groups. If
// so, canMapJoinOpEquivalenceGroup returns true.
func (c *CustomFuncs) canMapJoinOpEquivalenceGroup(
	filters memo.FiltersExpr,
	col opt.ColumnID,
	leftCols, rightCols opt.ColSet,
	equivFD props.FuncDepSet,
) bool {
	eqCols := c.GetEquivColsWithEquivType(col, equivFD)

	// To map equality conditions, the equivalent columns must intersect
	// both sides and must be fully bound by both sides.
	if !(eqCols.Intersects(leftCols) &&
		eqCols.Intersects(rightCols) &&
		eqCols.SubsetOf(leftCols.Union(rightCols))) {
		return false
	}

	// If more than one equality condition connecting columns in the equivalence
	// group spans both sides of the join, these conditions can be remapped.
	found := 0
	for i := range filters {
		fd := &filters[i].ScalarProps().FuncDeps
		filterEqCols := fd.ComputeEquivClosure(fd.EquivReps())
		if filterEqCols.Intersects(leftCols) && filterEqCols.Intersects(rightCols) &&
			filterEqCols.SubsetOf(eqCols) {
			found++
			if found > 1 {
				return true
			}
		}
	}

	return false
}

// MapJoinOpEqualities maps all variable equality conditions in filters to
// use columns in either leftCols or rightCols where possible. See
// canMapJoinOpEquivalenceGroup and mapJoinOpEquivalenceGroup for more info.
func (c *CustomFuncs) MapJoinOpEqualities(
	filters memo.FiltersExpr, leftCols, rightCols opt.ColSet,
) memo.FiltersExpr {
	var equivFD props.FuncDepSet
	for i := range filters {
		equivFD.AddEquivFrom(&filters[i].ScalarProps().FuncDeps)
	}
	equivReps := equivFD.EquivReps()

	newFilters := filters
	equivReps.ForEach(func(col opt.ColumnID) {
		if c.canMapJoinOpEquivalenceGroup(newFilters, col, leftCols, rightCols, equivFD) {
			newFilters = c.mapJoinOpEquivalenceGroup(newFilters, col, leftCols, rightCols, equivFD)
		}
	})

	return newFilters
}

// mapJoinOpEquivalenceGroup maps equality conditions in a join that form an
// equivalence group to use different variables so that the number of
// conditions crossing both sides of a join are minimized. This is useful for
// creating additional filter conditions that can be pushed down to either side
// of the join.
//
// To perform the mapping, mapJoinOpEquivalenceGroup finds the set of columns
// containing col that forms an equivalence group in filters. The result is
// a set of columns that are all equivalent, some on the left side of the join
// and some on the right side. mapJoinOpEquivalenceGroup constructs a new set of
// equalities that implies the same equivalency group, with the property that
// there is a single condition with one left column and one right column.
// For example, consider this query:
//
//   SELECT * FROM a, b WHERE a.x = b.x AND a.x = a.y AND a.y = b.y
//
// It has an equivalence group {a.x, a.y, b.x, b.y}. The columns a.x and a.y
// are on the left side, and b.x and b.y are on the right side. Initially there
// are two conditions that cross both sides. After mapping, the query would be
// converted to:
//
//   SELECT * FROM a, b WHERE a.x = a.y AND b.x = b.y AND a.x = b.x
//
func (c *CustomFuncs) mapJoinOpEquivalenceGroup(
	filters memo.FiltersExpr,
	col opt.ColumnID,
	leftCols, rightCols opt.ColSet,
	equivFD props.FuncDepSet,
) memo.FiltersExpr {
	eqCols := c.GetEquivColsWithEquivType(col, equivFD)

	// First remove all the equality conditions for this equivalence group.
	newFilters := make(memo.FiltersExpr, 0, len(filters))
	for i := range filters {
		fd := &filters[i].ScalarProps().FuncDeps
		filterEqCols := fd.ComputeEquivClosure(fd.EquivReps())
		if !filterEqCols.Empty() && filterEqCols.SubsetOf(eqCols) {
			continue
		}
		newFilters = append(newFilters, filters[i])
	}

	// Now append new equality conditions that imply the same equivalency group,
	// but only one condition should contain columns from both sides.
	leftEqCols := leftCols.Intersection(eqCols)
	rightEqCols := rightCols.Intersection(eqCols)
	firstLeftCol, ok := leftEqCols.Next(0)
	if !ok {
		panic(errors.AssertionFailedf(
			"mapJoinOpEquivalenceGroup called with equivalence group that does not intersect both sides",
		))
	}
	firstRightCol, ok := rightEqCols.Next(0)
	if !ok {
		panic(errors.AssertionFailedf(
			"mapJoinOpEquivalenceGroup called with equivalence group that does not intersect both sides",
		))
	}

	// Connect all the columns on the left.
	for col, ok := leftEqCols.Next(firstLeftCol + 1); ok; col, ok = leftEqCols.Next(col + 1) {
		newFilters = append(newFilters, c.f.ConstructFiltersItem(
			c.f.ConstructEq(c.f.ConstructVariable(firstLeftCol), c.f.ConstructVariable(col)),
		))
	}

	// Connect all the columns on the right.
	for col, ok := rightEqCols.Next(firstRightCol + 1); ok; col, ok = rightEqCols.Next(col + 1) {
		newFilters = append(newFilters, c.f.ConstructFiltersItem(
			c.f.ConstructEq(c.f.ConstructVariable(firstRightCol), c.f.ConstructVariable(col)),
		))
	}

	// Connect the two sides.
	newFilters = append(newFilters, c.f.ConstructFiltersItem(
		c.f.ConstructEq(
			c.f.ConstructVariable(firstLeftCol), c.f.ConstructVariable(firstRightCol),
		),
	))

	return newFilters
}

// CanMapJoinOpFilter returns true if it is possible to map a boolean expression
// src, which is a conjunct in the given filters expression, to use the output
// columns of the relational expression dst.
//
// In order for one column to map to another, the two columns must be
// equivalent. This happens when there is an equality predicate such as a.x=b.x
// in the ON or WHERE clause. Additionally, the two columns must be of the same
// type (see GetEquivColsWithEquivType for details). CanMapJoinOpFilter checks
// that for each column in src, there is at least one equivalent column in dst.
//
// For example, consider this query:
//
//   SELECT * FROM a INNER JOIN b ON a.x=b.x AND a.x + b.y = 5
//
// Since there is an equality predicate on a.x=b.x, it is possible to map
// a.x + b.y = 5 to b.x + b.y = 5, and that allows the filter to be pushed down
// to the right side of the join. In this case, CanMapJoinOpFilter returns true
// when src is a.x + b.y = 5 and dst is (Scan b), but false when src is
// a.x + b.y = 5 and dst is (Scan a).
//
// If src has a correlated subquery, CanMapJoinOpFilter returns false.
func (c *CustomFuncs) CanMapJoinOpFilter(
	src *memo.FiltersItem, dstCols opt.ColSet, equivFD props.FuncDepSet,
) bool {
	// Fast path if src is already bound by dst.
	if c.IsBoundBy(src, dstCols) {
		return true
	}

	scalarProps := src.ScalarProps()
	if scalarProps.HasCorrelatedSubquery {
		return false
	}

	// For CanMapJoinOpFilter to be true, each column in src must map to at
	// least one column in dst.
	for i, ok := scalarProps.OuterCols.Next(0); ok; i, ok = scalarProps.OuterCols.Next(i + 1) {
		eqCols := c.GetEquivColsWithEquivType(i, equivFD)
		if !eqCols.Intersects(dstCols) {
			return false
		}
	}

	return true
}

// MapJoinOpFilter maps a boolean expression src, which is a conjunct in
// the given filters expression, to use the output columns of the relational
// expression dst.
//
// MapJoinOpFilter assumes that CanMapJoinOpFilter has already returned true,
// and therefore a mapping is possible (see comment above CanMapJoinOpFilter
// for details).
//
// For each column in src that is not also in dst, MapJoinOpFilter replaces it
// with an equivalent column in dst. If there are multiple equivalent columns
// in dst, it chooses one arbitrarily. MapJoinOpFilter does not replace any
// columns in subqueries, since we know there are no correlated subqueries
// (otherwise CanMapJoinOpFilter would have returned false).
//
// For example, consider this query:
//
//   SELECT * FROM a INNER JOIN b ON a.x=b.x AND a.x + b.y = 5
//
// If MapJoinOpFilter is called with src as a.x + b.y = 5 and dst as (Scan b),
// it returns b.x + b.y = 5. MapJoinOpFilter should not be called with the
// equality predicate a.x = b.x, because it would just return the tautology
// b.x = b.x.
func (c *CustomFuncs) MapJoinOpFilter(
	src *memo.FiltersItem, dstCols opt.ColSet, equivFD props.FuncDepSet,
) opt.ScalarExpr {
	// Fast path if src is already bound by dst.
	if c.IsBoundBy(src, dstCols) {
		return src.Condition
	}

	// Map each column in src to one column in dst. We choose an arbitrary column
	// (the one with the smallest ColumnID) if there are multiple choices.
	var colMap util.FastIntMap
	outerCols := src.ScalarProps().OuterCols
	for srcCol, ok := outerCols.Next(0); ok; srcCol, ok = outerCols.Next(srcCol + 1) {
		eqCols := c.GetEquivColsWithEquivType(srcCol, equivFD)
		eqCols.IntersectionWith(dstCols)
		if eqCols.Contains(srcCol) {
			colMap.Set(int(srcCol), int(srcCol))
		} else {
			dstCol, ok := eqCols.Next(0)
			if !ok {
				panic(errors.AssertionFailedf(
					"MapJoinOpFilter called on src that cannot be mapped to dst. src:\n%s\ndst:\n%s",
					src, dstCols,
				))
			}
			colMap.Set(int(srcCol), int(dstCol))
		}
	}

	// Recursively walk the scalar sub-tree looking for references to columns
	// that need to be replaced.
	var replace ReplaceFunc
	replace = func(nd opt.Expr) opt.Expr {
		switch t := nd.(type) {
		case *memo.VariableExpr:
			outCol, _ := colMap.Get(int(t.Col))
			if int(t.Col) == outCol {
				// Avoid constructing a new variable if possible.
				return nd
			}
			return c.f.ConstructVariable(opt.ColumnID(outCol))

		case *memo.SubqueryExpr, *memo.ExistsExpr, *memo.AnyExpr:
			// There are no correlated subqueries, so we don't need to recurse here.
			return nd
		}

		return c.f.Replace(nd, replace)
	}

	return replace(src.Condition).(opt.ScalarExpr)
}

// GetEquivColsWithEquivType uses the given FuncDepSet to find columns that are
// equivalent to col, and returns only those columns that also have the same
// type as col. This function is used when inferring new filters based on
// equivalent columns, because operations that are valid with one type may be
// invalid with a different type.
//
// In addition, if col has a composite key encoding, we cannot guarantee that
// it will be exactly equal to other "equivalent" columns, so in that case we
// return a set containing only col. This is a conservative measure to ensure
// that we don't infer filters incorrectly. For example, consider this query:
//
//   SELECT * FROM
//     (VALUES (1.0)) AS t1(x),
//     (VALUES (1.00)) AS t2(y)
//   WHERE x=y AND x::text = '1.0';
//
// It should return the following result:
//
//     x  |  y
//   -----+------
//    1.0 | 1.00
//
// But if we use the equality predicate x=y to map x to y and infer an
// additional filter y::text = '1.0', the query would return nothing.
//
// TODO(rytaft): In the future, we may want to allow the mapping if the
// filter involves a comparison operator, such as x < 5.
func (c *CustomFuncs) GetEquivColsWithEquivType(
	col opt.ColumnID, equivFD props.FuncDepSet,
) opt.ColSet {
	var res opt.ColSet
	colType := c.f.Metadata().ColumnMeta(col).Type

	// Don't bother looking for equivalent columns if colType has a composite
	// key encoding.
	if sqlbase.DatumTypeHasCompositeKeyEncoding(colType) {
		res.Add(col)
		return res
	}

	// Compute all equivalent columns.
	eqCols := equivFD.ComputeEquivGroup(col)

	eqCols.ForEach(func(i opt.ColumnID) {
		// Only include columns that have the same type as col.
		eqColType := c.f.Metadata().ColumnMeta(i).Type
		if colType.Equivalent(eqColType) {
			res.Add(i)
		}
	})

	return res
}

// GetEquivFD gets a FuncDepSet with all equivalence dependencies from
// filters, left and right.
func (c *CustomFuncs) GetEquivFD(
	filters memo.FiltersExpr, left, right memo.RelExpr,
) (equivFD props.FuncDepSet) {
	for i := range filters {
		equivFD.AddEquivFrom(&filters[i].ScalarProps().FuncDeps)
	}
	equivFD.AddEquivFrom(&left.Relational().FuncDeps)
	equivFD.AddEquivFrom(&right.Relational().FuncDeps)
	return equivFD
}

// eqConditionsToColMap returns a map of left columns to right columns
// that are being equated in the specified conditions. leftCols is used
// to identify which column is a left column.
func (c *CustomFuncs) eqConditionsToColMap(
	filters memo.FiltersExpr, leftCols opt.ColSet,
) map[opt.ColumnID]opt.ColumnID {
	eqColMap := make(map[opt.ColumnID]opt.ColumnID)

	for i := range filters {
		eq, _ := filters[i].Condition.(*memo.EqExpr)
		if eq == nil {
			continue
		}

		leftVarExpr, _ := eq.Left.(*memo.VariableExpr)
		rightVarExpr, _ := eq.Right.(*memo.VariableExpr)
		if leftVarExpr == nil || rightVarExpr == nil {
			continue
		}

		if leftCols.Contains(leftVarExpr.Col) {
			eqColMap[leftVarExpr.Col] = rightVarExpr.Col
		} else {
			eqColMap[rightVarExpr.Col] = leftVarExpr.Col
		}
	}

	return eqColMap
}

// JoinFiltersMatchAllLeftRows returns true when each row in the given join's
// left input matches at least one row from the right input, according to the
// join filters. This is true when the following conditions are satisfied:
//
//   1. Each conjunct in the join condition is an equality between a not-null
//      column from the left input and a not-null column from the right input.
//   2. All left input equality columns come from a single table (called its
//      "equality table"), as do all right input equality columns (can be
//      different table).
//   3. The right input contains every row from its equality table. There may be
//      a subset of columns from the table, and/or duplicate rows, but every row
//      must be present.
//   4. If the left equality table is the same as the right equality table, then
//      it's the self-join case. The columns in each equality pair must have the
//      same ordinal position in the table.
//   5. If the left equality table is different than the right equality table,
//      then it's the foreign-key case. The left equality columns must map to
//      a foreign key on the left equality table, and the right equality columns
//      to the corresponding referenced columns in the right equality table.
//
func (c *CustomFuncs) JoinFiltersMatchAllLeftRows(
	left, right memo.RelExpr, filters memo.FiltersExpr,
) bool {
	unfilteredCols := c.deriveUnfilteredCols(right)
	if unfilteredCols.Empty() {
		// Condition #3: right input has no columns which contain values from
		// every row.
		return false
	}

	leftColIDs := left.Relational().NotNullCols
	rightColIDs := right.Relational().NotNullCols

	md := c.f.Metadata()

	var leftTab, rightTab opt.TableID

	// Any left columns that don't match conditions 1-4 end up in this set.
	var remainingLeftColIDs opt.ColSet

	for i := range filters {
		eq, _ := filters[i].Condition.(*memo.EqExpr)
		if eq == nil {
			// Condition #1: conjunct is not an equality comparison.
			return false
		}

		leftVar, _ := eq.Left.(*memo.VariableExpr)
		rightVar, _ := eq.Right.(*memo.VariableExpr)
		if leftVar == nil || rightVar == nil {
			// Condition #1: conjunct does not compare two columns.
			return false
		}

		leftColID := leftVar.Col
		rightColID := rightVar.Col

		// Normalize leftColID to come from leftColIDs.
		if !leftColIDs.Contains(leftColID) {
			leftColID, rightColID = rightColID, leftColID
		}
		if !leftColIDs.Contains(leftColID) || !rightColIDs.Contains(rightColID) {
			// Condition #1: columns don't come from both sides of join, or
			// columns are nullable.
			return false
		}

		if !unfilteredCols.Contains(rightColID) {
			// Condition #3: right column doesn't contain values from every row.
			return false
		}

		if leftTab == 0 {
			leftTab = md.ColumnMeta(leftColID).Table
			rightTab = md.ColumnMeta(rightColID).Table
			if leftTab == 0 || rightTab == 0 {
				// Condition #2: Columns don't come from base tables.
				return false
			}
		} else if md.ColumnMeta(leftColID).Table != leftTab {
			// Condition #2: All left columns don't come from same table.
			return false
		} else if md.ColumnMeta(rightColID).Table != rightTab {
			// Condition #2: All right columns don't come from same table.
			return false
		}

		if md.TableMeta(leftTab).Table == md.TableMeta(rightTab).Table {
			// Check self-join case.
			leftColOrd := leftTab.ColumnOrdinal(leftColID)
			rightColOrd := rightTab.ColumnOrdinal(rightColID)
			if leftColOrd != rightColOrd {
				// Condition #4: Left and right column ordinals do not match.
				return false
			}
		} else {
			// Column could be a potential foreign key match so save it.
			remainingLeftColIDs.Add(leftColID)
		}
	}

	if remainingLeftColIDs.Empty() {
		return true
	}

	var leftRightColMap map[opt.ColumnID]opt.ColumnID
	// Condition #5: All remaining left columns correspond to a validated foreign
	// key relation.
	leftTabMeta := md.TableMeta(leftTab)
	if leftTabMeta.IgnoreForeignKeys {
		// We are not allowed to use any of the left table's outbound foreign keys.
		return false
	}
	rightTabMeta := md.TableMeta(rightTab)

	// Search for validated foreign key references from the left table to the
	// right table.
	for i, cnt := 0, leftTabMeta.Table.OutboundForeignKeyCount(); i < cnt; i++ {
		fkRef := leftTabMeta.Table.OutboundForeignKey(i)
		if fkRef.ReferencedTableID() != rightTabMeta.Table.ID() || !fkRef.Validated() {
			continue
		}
		fkTable := md.TableByStableID(fkRef.ReferencedTableID())
		if fkTable == nil {
			continue
		}

		var leftIndexCols opt.ColSet
		numCols := fkRef.ColumnCount()
		for j := 0; j < numCols; j++ {
			ord := fkRef.OriginColumnOrdinal(leftTabMeta.Table, j)
			leftIndexCols.Add(leftTab.ColumnID(ord))
		}

		if !remainingLeftColIDs.SubsetOf(leftIndexCols) {
			continue
		}

		// Build a mapping of left to right columns as specified
		// in the filter conditions - this is used to detect
		// whether the filter conditions follow the foreign key
		// constraint exactly.
		if leftRightColMap == nil {
			leftRightColMap = c.eqConditionsToColMap(filters, leftColIDs)
		}

		// Loop through all columns in fk index that also exist in LHS of match condition,
		// and ensure that they correspond to the correct RHS column according to the
		// foreign key relation. In other words, each LHS column's index ordinal
		// in the foreign key index matches that of the RHS column (in the index being
		// referenced) that it's being equated to.
		fkMatch := true
		for j := 0; j < numCols; j++ {
			indexLeftCol := leftTab.ColumnID(fkRef.OriginColumnOrdinal(leftTabMeta.Table, j))

			// Not every fk column needs to be in the equality conditions.
			if !remainingLeftColIDs.Contains(indexLeftCol) {
				continue
			}

			indexRightCol := rightTab.ColumnID(fkRef.ReferencedColumnOrdinal(fkTable, j))

			if rightCol, ok := leftRightColMap[indexLeftCol]; !ok || rightCol != indexRightCol {
				fkMatch = false
				break
			}
		}

		// Condition #5 satisfied.
		if fkMatch {
			return true
		}
	}

	return false
}

// deriveUnfilteredCols returns the subset of the given input expression's
// output columns that have values for every row in their owner table. In other
// words, columns from tables that have had none of their rows filtered (but
// it's OK if rows have been duplicated).
//
// deriveUnfilteredCols recursively derives the property, and populates the
// props.Relational.Rule.UnfilteredCols field as it goes to make future calls
// faster.
func (c *CustomFuncs) deriveUnfilteredCols(in memo.RelExpr) opt.ColSet {
	// If the UnfilteredCols property has already been derived, return it
	// immediately.
	relational := in.Relational()
	if relational.IsAvailable(props.UnfilteredCols) {
		return relational.Rule.UnfilteredCols
	}
	relational.Rule.Available |= props.UnfilteredCols

	// Derive the UnfilteredCols property now.
	// TODO(andyk): Could add other cases, such as outer joins and union.
	switch t := in.(type) {
	case *memo.ScanExpr:
		// All un-limited, unconstrained output columns are unfiltered columns.
		if t.HardLimit == 0 && t.Constraint == nil {
			relational.Rule.UnfilteredCols = relational.OutputCols
		}

	case *memo.ProjectExpr:
		// Project never filters rows, so it passes through unfiltered columns.
		unfilteredCols := c.deriveUnfilteredCols(t.Input)
		relational.Rule.UnfilteredCols = unfilteredCols.Intersection(relational.OutputCols)

	case *memo.InnerJoinExpr, *memo.InnerJoinApplyExpr:
		left := t.Child(0).(memo.RelExpr)
		right := t.Child(1).(memo.RelExpr)
		on := *t.Child(2).(*memo.FiltersExpr)

		// Cross join always preserves left/right rows.
		isCrossJoin := on.IsTrue()

		// Inner joins may preserve left/right rows, according to
		// JoinFiltersMatchAllLeftRows conditions.
		if isCrossJoin || c.JoinFiltersMatchAllLeftRows(left, right, on) {
			relational.Rule.UnfilteredCols.UnionWith(c.deriveUnfilteredCols(left))
		}
		if isCrossJoin || c.JoinFiltersMatchAllLeftRows(right, left, on) {
			relational.Rule.UnfilteredCols.UnionWith(c.deriveUnfilteredCols(right))
		}
	}

	return relational.Rule.UnfilteredCols
}

// CanExtractJoinEquality returns true if:
//   - one of a, b is bound by the left columns;
//   - the other is bound by the right columns;
//   - a and b are not "bare" variables;
//   - a and b contain no correlated subqueries;
//   - neither a or b are constants.
//
// Such an equality can be converted to a column equality by pushing down
// expressions as projections.
func (c *CustomFuncs) CanExtractJoinEquality(
	a, b opt.ScalarExpr, leftCols, rightCols opt.ColSet,
) bool {
	// Disallow simple equality between variables.
	if a.Op() == opt.VariableOp && b.Op() == opt.VariableOp {
		return false
	}

	// Recursively compute properties for left and right sides.
	var leftProps, rightProps props.Shared
	memo.BuildSharedProps(a, &leftProps)
	memo.BuildSharedProps(b, &rightProps)

	// Disallow cases when one side has a correlated subquery.
	// TODO(radu): investigate relaxing this.
	if leftProps.HasCorrelatedSubquery || rightProps.HasCorrelatedSubquery {
		return false
	}

	if leftProps.OuterCols.Empty() || rightProps.OuterCols.Empty() {
		// It's possible for one side to have no outer cols and still not be a
		// ConstValue (see #44746).
		return false
	}

	if (leftProps.OuterCols.SubsetOf(leftCols) && rightProps.OuterCols.SubsetOf(rightCols)) ||
		(leftProps.OuterCols.SubsetOf(rightCols) && rightProps.OuterCols.SubsetOf(leftCols)) {
		// The equality is of the form:
		//   expression(leftCols) = expression(rightCols)
		return true
	}
	return false
}

// ExtractJoinEquality takes an equality FiltersItem that was identified via a
// call to CanExtractJoinEquality, and converts it to an equality on "bare"
// variables, by pushing down more complicated expressions as projections. See
// the ExtractJoinEqualities rule.
func (c *CustomFuncs) ExtractJoinEquality(
	joinOp opt.Operator,
	left, right memo.RelExpr,
	filters memo.FiltersExpr,
	item *memo.FiltersItem,
	private *memo.JoinPrivate,
) memo.RelExpr {
	leftCols := c.OutputCols(left)
	rightCols := c.OutputCols(right)

	eq := item.Condition.(*memo.EqExpr)
	a, b := eq.Left, eq.Right

	var eqLeftProps props.Shared
	memo.BuildSharedProps(eq.Left, &eqLeftProps)
	if eqLeftProps.OuterCols.SubsetOf(rightCols) {
		a, b = b, a
	}

	var leftProj, rightProj projectBuilder
	leftProj.init(c.f)
	rightProj.init(c.f)

	newFilters := make(memo.FiltersExpr, len(filters))
	for i := range filters {
		if &filters[i] != item {
			newFilters[i] = filters[i]
			continue
		}

		newFilters[i] = c.f.ConstructFiltersItem(
			c.f.ConstructEq(leftProj.add(a), rightProj.add(b)),
		)
	}
	if leftProj.empty() && rightProj.empty() {
		panic(errors.AssertionFailedf("no equalities to extract"))
	}

	join := c.f.ConstructJoin(
		joinOp,
		leftProj.buildProject(left, leftCols),
		rightProj.buildProject(right, rightCols),
		newFilters,
		private,
	)

	// Project away the synthesized columns.
	return c.f.ConstructProject(join, memo.EmptyProjectionsExpr, leftCols.Union(rightCols))
}
