// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package norm

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// ----------------------------------------------------------------------
//
// Join Rules
//   Custom match and replace functions used with join.opt rules.
//
// ----------------------------------------------------------------------

// ConstructNonLeftJoin maps a left join to an inner join and a full join to a
// right join when it can be proved that the right side of the join always
// produces at least one row for every row on the left.
func (c *CustomFuncs) ConstructNonLeftJoin(
	joinOp opt.Operator, left, right memo.RelExpr, on memo.FiltersExpr,
) memo.RelExpr {
	switch joinOp {
	case opt.LeftJoinOp:
		return c.f.ConstructInnerJoin(left, right, on)
	case opt.LeftJoinApplyOp:
		return c.f.ConstructInnerJoinApply(left, right, on)
	case opt.FullJoinOp:
		return c.f.ConstructRightJoin(left, right, on)
	case opt.FullJoinApplyOp:
		return c.f.ConstructRightJoinApply(left, right, on)
	}
	panic(fmt.Sprintf("unexpected join operator: %v", joinOp))
}

// ConstructNonRightJoin maps a right join to an inner join and a full join to a
// left join when it can be proved that the left side of the join always
// produces at least one row for every row on the right.
func (c *CustomFuncs) ConstructNonRightJoin(
	joinOp opt.Operator, left, right memo.RelExpr, on memo.FiltersExpr,
) memo.RelExpr {
	switch joinOp {
	case opt.RightJoinOp:
		return c.f.ConstructInnerJoin(left, right, on)
	case opt.RightJoinApplyOp:
		return c.f.ConstructInnerJoinApply(left, right, on)
	case opt.FullJoinOp:
		return c.f.ConstructLeftJoin(left, right, on)
	case opt.FullJoinApplyOp:
		return c.f.ConstructLeftJoinApply(left, right, on)
	}
	panic(fmt.Sprintf("unexpected join operator: %v", joinOp))
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
	panic(fmt.Sprintf("invalid ops: %v, %v", testOp, constOp))
}

// CanMap returns true if it is possible to map a boolean expression src, which
// is a conjunct in the given filters expression, to use the output columns of
// the relational expression dst.
//
// In order for one column to map to another, the two columns must be
// equivalent. This happens when there is an equality predicate such as a.x=b.x
// in the ON or WHERE clause. Additionally, the two columns must be of the same
// type (see GetEquivColsWithEquivType for details). CanMap checks that for each
// column in src, there is at least one equivalent column in dst.
//
// For example, consider this query:
//
//   SELECT * FROM a INNER JOIN b ON a.x=b.x AND a.x + b.y = 5
//
// Since there is an equality predicate on a.x=b.x, it is possible to map
// a.x + b.y = 5 to b.x + b.y = 5, and that allows the filter to be pushed down
// to the right side of the join. In this case, CanMap returns true when src is
// a.x + b.y = 5 and dst is (Scan b), but false when src is a.x + b.y = 5 and
// dst is (Scan a).
//
// If src has a correlated subquery, CanMap returns false.
func (c *CustomFuncs) CanMap(
	filters memo.FiltersExpr, src *memo.FiltersItem, dst memo.RelExpr,
) bool {
	// Fast path if src is already bound by dst.
	if c.IsBoundBy(src, c.OutputCols(dst)) {
		return true
	}

	scalarProps := src.ScalarProps(c.mem)
	if scalarProps.HasCorrelatedSubquery {
		return false
	}

	// For CanMap to be true, each column in src must map to at least one column
	// in dst.
	for i, ok := scalarProps.OuterCols.Next(0); ok; i, ok = scalarProps.OuterCols.Next(i + 1) {
		eqCols := c.GetEquivColsWithEquivType(opt.ColumnID(i), filters)
		if !eqCols.Intersects(c.OutputCols(dst)) {
			return false
		}
	}

	return true
}

// Map maps a boolean expression src, which is a conjunct in the given filters
// expression, to use the output columns of the relational expression dst.
//
// Map assumes that CanMap has already returned true, and therefore a mapping
// is possible (see the comment above CanMap for details).
//
// For each column in src that is not also in dst, Map replaces it with an
// equivalent column in dst. If there are multiple equivalent columns in dst,
// it chooses one arbitrarily. Map does not replace any columns in subqueries,
// since we know there are no correlated subqueries (otherwise CanMap would
// have returned false).
//
// For example, consider this query:
//
//   SELECT * FROM a INNER JOIN b ON a.x=b.x AND a.x + b.y = 5
//
// If Map is called with src as a.x + b.y = 5 and dst as (Scan b), it returns
// b.x + b.y = 5. Map should not be called with the equality predicate
// a.x = b.x, because it would just return the tautology b.x = b.x.
func (c *CustomFuncs) Map(
	filters memo.FiltersExpr, src *memo.FiltersItem, dst memo.RelExpr,
) opt.ScalarExpr {
	// Fast path if src is already bound by dst.
	if c.IsBoundBy(src, c.OutputCols(dst)) {
		return src.Condition
	}

	// Map each column in src to one column in dst. We choose an arbitrary column
	// (the one with the smallest ColumnID) if there are multiple choices.
	var colMap util.FastIntMap
	outerCols := src.ScalarProps(c.mem).OuterCols
	for srcCol, ok := outerCols.Next(0); ok; srcCol, ok = outerCols.Next(srcCol + 1) {
		eqCols := c.GetEquivColsWithEquivType(opt.ColumnID(srcCol), filters)
		eqCols.IntersectionWith(c.OutputCols(dst))
		if eqCols.Contains(srcCol) {
			colMap.Set(srcCol, srcCol)
		} else {
			dstCol, ok := eqCols.Next(0)
			if !ok {
				panic(fmt.Errorf(
					"Map called on src that cannot be mapped to dst. src:\n%s\ndst:\n%s",
					src, dst,
				))
			}
			colMap.Set(srcCol, dstCol)
		}
	}

	// Recursively walk the scalar sub-tree looking for references to columns
	// that need to be replaced.
	var replace ReconstructFunc
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

		return c.f.Reconstruct(nd, replace)
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
	col opt.ColumnID, filters memo.FiltersExpr,
) opt.ColSet {
	var res opt.ColSet
	colType := c.f.Metadata().ColumnType(col)

	// Don't bother looking for equivalent columns if colType has a composite
	// key encoding.
	if sqlbase.DatumTypeHasCompositeKeyEncoding(colType) {
		res.Add(int(col))
		return res
	}

	// Compute all equivalent columns.
	eqCols := util.MakeFastIntSet(int(col))
	for i := range filters {
		eqCols = filters[i].ScalarProps(c.mem).FuncDeps.ComputeEquivClosure(eqCols)
	}

	eqCols.ForEach(func(i int) {
		// Only include columns that have the same type as col.
		eqColType := c.f.Metadata().ColumnType(opt.ColumnID(i))
		if colType.Equivalent(eqColType) {
			res.Add(i)
		}
	})

	return res
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

		if leftCols.Contains(int(leftVarExpr.Col)) {
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

	leftCols := left.Relational().NotNullCols
	rightCols := right.Relational().NotNullCols

	md := c.f.Metadata()

	var leftTab, rightTab opt.Table
	var leftTabID, rightTabID opt.TableID

	// Any left columns that don't match conditions 1-4 end up in this set.
	var remainingLeftCols opt.ColSet

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

		leftCol := leftVar.Col
		rightCol := rightVar.Col

		// Normalize leftCol to come from leftCols.
		if !leftCols.Contains(int(leftCol)) {
			leftCol, rightCol = rightCol, leftCol
		}
		if !leftCols.Contains(int(leftCol)) || !rightCols.Contains(int(rightCol)) {
			// Condition #1: columns don't come from both sides of join, or
			// columns are nullable.
			return false
		}

		if !unfilteredCols.Contains(int(rightCol)) {
			// Condition #3: right column doesn't contain values from every row.
			return false
		}

		if leftTab == nil {
			leftTabID = md.ColumnTableID(leftCol)
			rightTabID = md.ColumnTableID(rightCol)
			if leftTabID == 0 || rightTabID == 0 {
				// Condition #2: Columns don't come from base tables.
				return false
			}
			leftTab = md.Table(leftTabID)
			rightTab = md.Table(rightTabID)
		} else if md.Table(md.ColumnTableID(leftCol)) != leftTab {
			// Condition #2: All left columns don't come from same table.
			return false
		} else if md.Table(md.ColumnTableID(rightCol)) != rightTab {
			// Condition #2: All right columns don't come from same table.
			return false
		}

		if leftTab == rightTab {
			// Check self-join case.
			if md.ColumnOrdinal(leftCol) != md.ColumnOrdinal(rightCol) {
				// Condition #4: Left and right column ordinals do not match.
				return false
			}
		} else {
			// Column could be a potential foreign key match so save it.
			remainingLeftCols.Add(int(leftCol))
		}
	}

	if remainingLeftCols.Empty() {
		return true
	}

	var leftRightColMap map[opt.ColumnID]opt.ColumnID
	// Condition #5: All remaining left columns correspond to a foreign key relation.
	for i, cnt := 0, leftTab.IndexCount(); i < cnt; i++ {
		index := leftTab.Index(i)
		fkRef, ok := index.ForeignKey()

		if !ok {
			// No foreign key reference on this index.
			continue
		}

		fkTable := md.TableByStableID(fkRef.TableID)
		fkPrefix := int(fkRef.PrefixLen)
		if fkPrefix <= 0 {
			panic("fkPrefix should always be positive")
		}
		if fkTable == nil || fkTable.ID() != rightTab.ID() {
			continue
		}

		// Find the index corresponding to fkRef.IndexID - the index
		// on the right table that forms the destination end of
		// the fk relation.
		var fkIndex opt.Index
		found := false
		for j, cnt2 := 0, fkTable.IndexCount(); j < cnt2; j++ {
			if fkTable.Index(j).ID() == fkRef.IndexID {
				found = true
				fkIndex = fkTable.Index(j)
				break
			}
		}
		if !found {
			panic("Foreign key referenced index not found in table")
		}

		var leftIndexCols opt.ColSet
		for j := 0; j < fkPrefix; j++ {
			ord := index.Column(j).Ordinal
			leftIndexCols.Add(int(leftTabID.ColumnID(ord)))
		}

		if !remainingLeftCols.SubsetOf(leftIndexCols) {
			continue
		}

		// Build a mapping of left to right columns as specified
		// in the filter conditions - this is used to detect
		// whether the filter conditions follow the foreign key
		// constraint exactly.
		if leftRightColMap == nil {
			leftRightColMap = c.eqConditionsToColMap(filters, leftCols)
		}

		// Loop through all columns in fk index that also exist in LHS of match condition,
		// and ensure that they correspond to the correct RHS column according to the
		// foreign key relation. In other words, each LHS column's index ordinal
		// in the foreign key index matches that of the RHS column (in the index being
		// referenced) that it's being equated to.
		fkMatch := true
		for j := 0; j < fkPrefix; j++ {
			indexLeftCol := leftTabID.ColumnID(index.Column(j).Ordinal)

			// Not every fk column needs to be in the equality conditions.
			if !remainingLeftCols.Contains(int(indexLeftCol)) {
				continue
			}

			indexRightCol := rightTabID.ColumnID(fkIndex.Column(j).Ordinal)

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
	memo.BuildSharedProps(c.mem, a, &leftProps)
	memo.BuildSharedProps(c.mem, b, &rightProps)

	// Disallow cases when one side has a correlated subquery.
	// TODO(radu): investigate relaxing this.
	if leftProps.HasCorrelatedSubquery || rightProps.HasCorrelatedSubquery {
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
	joinOp opt.Operator, left, right memo.RelExpr, filters memo.FiltersExpr, item *memo.FiltersItem,
) memo.RelExpr {
	leftCols := c.OutputCols(left)
	rightCols := c.OutputCols(right)

	eq := item.Condition.(*memo.EqExpr)
	a, b := eq.Left, eq.Right

	var eqLeftProps props.Shared
	memo.BuildSharedProps(c.mem, eq.Left, &eqLeftProps)
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

		newFilters[i] = memo.FiltersItem{
			Condition: c.f.ConstructEq(leftProj.add(a), rightProj.add(b)),
		}
	}
	if leftProj.empty() && rightProj.empty() {
		panic("no equalities to extract")
	}

	join := c.ConstructJoin(
		joinOp,
		leftProj.buildProject(left, leftCols),
		rightProj.buildProject(right, rightCols),
		newFilters,
	)

	// Project away the synthesized columns.
	return c.f.ConstructProject(join, memo.EmptyProjectionsExpr, leftCols.Union(rightCols))
}
