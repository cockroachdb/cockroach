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
	joinOp opt.Operator, left, right, on memo.GroupID,
) memo.GroupID {
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
	joinOp opt.Operator, left, right, on memo.GroupID,
) memo.GroupID {
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
func (c *CustomFuncs) CanMap(filters, src, dst memo.GroupID) bool {
	// Fast path if src is already bound by dst.
	if c.IsBoundBy(src, c.OutputCols(dst)) {
		return true
	}

	if c.HasHoistableSubquery(src) {
		return false
	}

	fd := c.LookupLogical(filters).Scalar.FuncDeps

	// For CanMap to be true, each column in src must map to at least one column
	// in dst.
	for i, ok := c.OuterCols(src).Next(0); ok; i, ok = c.OuterCols(src).Next(i + 1) {
		eqCols := c.GetEquivColsWithEquivType(opt.ColumnID(i), &fd)
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
func (c *CustomFuncs) Map(filters, src, dst memo.GroupID) memo.GroupID {
	// Fast path if src is already bound by dst.
	if c.IsBoundBy(src, c.OutputCols(dst)) {
		return src
	}

	fd := c.LookupLogical(filters).Scalar.FuncDeps

	// Map each column in src to one column in dst. We choose an arbitrary column
	// (the one with the smallest ColumnID) if there are multiple choices.
	var colMap util.FastIntMap
	c.OuterCols(src).ForEach(func(srcCol int) {
		eqCols := c.GetEquivColsWithEquivType(opt.ColumnID(srcCol), &fd)
		eqCols.IntersectionWith(c.OutputCols(dst))
		if eqCols.Contains(srcCol) {
			colMap.Set(srcCol, srcCol)
		} else {
			dstCol, ok := eqCols.Next(0)
			if !ok {
				panic(fmt.Errorf(
					"Map called on src that cannot be mapped to dst. src:\n%s\ndst:\n%s",
					memo.MakeNormExprView(c.f.mem, src).FormatString(memo.ExprFmtHideScalars),
					memo.MakeNormExprView(c.f.mem, dst).FormatString(memo.ExprFmtHideScalars),
				))
			}
			colMap.Set(srcCol, dstCol)
		}
	})

	// Recursively walk the scalar sub-tree looking for references to columns
	// that need to be replaced.
	var replace memo.ReplaceChildFunc
	replace = func(child memo.GroupID) memo.GroupID {
		expr := c.f.mem.NormExpr(child)

		switch expr.Operator() {
		case opt.VariableOp:
			varColID := c.ExtractColID(expr.AsVariable().Col())
			outCol, _ := colMap.Get(int(varColID))
			if int(varColID) == outCol {
				// Avoid constructing a new variable if possible.
				return child
			}
			return c.f.ConstructVariable(c.f.InternColumnID(opt.ColumnID(outCol)))

		case opt.SubqueryOp, opt.ExistsOp, opt.AnyOp:
			// There are no correlated subqueries, so we don't need to recurse here.
			return child
		}

		ev := memo.MakeNormExprView(c.f.mem, child)
		return ev.Replace(c.f.evalCtx, replace).Group()
	}

	return replace(src)
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
func (c *CustomFuncs) GetEquivColsWithEquivType(col opt.ColumnID, fd *props.FuncDepSet) opt.ColSet {
	var res opt.ColSet
	colType := c.f.Metadata().ColumnType(col)

	// Don't bother looking for equivalent columns if colType has a composite
	// key encoding.
	if sqlbase.DatumTypeHasCompositeKeyEncoding(colType) {
		res.Add(int(col))
		return res
	}

	eqCols := fd.ComputeEquivClosure(util.MakeFastIntSet(int(col)))
	eqCols.ForEach(func(i int) {
		eqColType := c.f.Metadata().ColumnType(opt.ColumnID(i))
		// Only include columns that have the same type as col.
		if colType.Equivalent(eqColType) {
			res.Add(i)
		}
	})

	return res
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
func (c *CustomFuncs) JoinFiltersMatchAllLeftRows(left, right, filters memo.GroupID) bool {
	unfilteredCols := c.deriveUnfilteredCols(right)
	if unfilteredCols.Empty() {
		// Condition #3: right input has no columns which contain values from
		// every row.
		return false
	}

	filtersExpr := c.f.mem.NormExpr(filters).AsFilters()
	if filtersExpr == nil {
		return false
	}

	leftCols := c.LookupLogical(left).Relational.NotNullCols
	rightCols := c.LookupLogical(right).Relational.NotNullCols

	md := c.f.Metadata()

	var leftTab, rightTab opt.Table
	for _, condition := range c.f.mem.LookupList(filtersExpr.Conditions()) {
		eqExpr := c.f.mem.NormExpr(condition).AsEq()
		if eqExpr == nil {
			// Condition #1: conjunct is not an equality comparison.
			return false
		}

		leftVarExpr := c.f.mem.NormExpr(eqExpr.Left()).AsVariable()
		rightVarExpr := c.f.mem.NormExpr(eqExpr.Right()).AsVariable()
		if leftVarExpr == nil || rightVarExpr == nil {
			// Condition #1: conjunct does not compare two columns.
			return false
		}

		leftCol := c.ExtractColID(leftVarExpr.Col())
		rightCol := c.ExtractColID(rightVarExpr.Col())

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
			leftTabID := md.ColumnTableID(leftCol)
			rightTabID := md.ColumnTableID(rightCol)
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
			// TODO(andyk): Check foreign key case.
			return false
		}
	}

	return true
}

// deriveUnfilteredCols returns the subset of the given group's output columns
// that have values for every row in their owner table. In other words, columns
// from tables that have had none of their rows filtered (but it's OK if rows
// have been duplicated).
//
// deriveUnfilteredCols recursively derives the property, and populates the
// props.Relational.Rule.UnfilteredCols field as it goes to make future calls
// faster.
func (c *CustomFuncs) deriveUnfilteredCols(group memo.GroupID) opt.ColSet {
	// If the UnfilteredCols property has already been derived, return it
	// immediately.
	relational := c.LookupLogical(group).Relational
	if relational.IsAvailable(props.UnfilteredCols) {
		return relational.Rule.UnfilteredCols
	}
	relational.Rule.Available |= props.UnfilteredCols

	// Derive the UnfilteredCols property now.
	// TODO(andyk): Could add other cases, such as outer joins and union.
	expr := c.f.mem.NormExpr(group)
	switch expr.Operator() {
	case opt.ScanOp:
		// All un-limited, unconstrained output columns are unfiltered columns.
		def := expr.Private(c.f.mem).(*memo.ScanOpDef)
		if def.HardLimit == 0 && def.Constraint == nil {
			relational.Rule.UnfilteredCols = relational.OutputCols
		}

	case opt.ProjectOp:
		// Project never filters rows, so it passes through unfiltered columns.
		unfilteredCols := c.deriveUnfilteredCols(expr.ChildGroup(c.f.mem, 0))
		relational.Rule.UnfilteredCols = unfilteredCols.Intersection(relational.OutputCols)

	case opt.InnerJoinOp, opt.InnerJoinApplyOp:
		left := expr.ChildGroup(c.f.mem, 0)
		right := expr.ChildGroup(c.f.mem, 1)
		on := expr.ChildGroup(c.f.mem, 2)

		// Cross join always preserves left/right rows.
		isCrossJoin := c.f.mem.NormExpr(on).Operator() == opt.TrueOp

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
