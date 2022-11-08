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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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
	panic(errors.AssertionFailedf("unexpected join operator: %v", redact.Safe(joinOp)))
}

// SimplifyNotNullEquality simplifies an expression of the following form:
//
//	(Is | IsNot (Eq) (True | False | Null))
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
	eqCols := c.GetEquivColsWithEquivType(col, equivFD, false /* allowCompositeEncoding */)

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

		// Note that EquivReps creates a new ColSet, so it is safe to modify it
		// in-place with ComputeEquivClosureNoCopy.
		filterEqCols := fd.ComputeEquivClosureNoCopy(fd.EquivReps())
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
//	SELECT * FROM a, b WHERE a.x = b.x AND a.x = a.y AND a.y = b.y
//
// It has an equivalence group {a.x, a.y, b.x, b.y}. The columns a.x and a.y
// are on the left side, and b.x and b.y are on the right side. Initially there
// are two conditions that cross both sides. After mapping, the query would be
// converted to:
//
//	SELECT * FROM a, b WHERE a.x = a.y AND b.x = b.y AND a.x = b.x
func (c *CustomFuncs) mapJoinOpEquivalenceGroup(
	filters memo.FiltersExpr,
	col opt.ColumnID,
	leftCols, rightCols opt.ColSet,
	equivFD props.FuncDepSet,
) memo.FiltersExpr {
	eqCols := c.GetEquivColsWithEquivType(col, equivFD, false /* allowCompositeEncoding */)

	// First remove all the equality conditions for this equivalence group.
	newFilters := make(memo.FiltersExpr, 0, len(filters))
	for i := range filters {
		fd := &filters[i].ScalarProps().FuncDeps

		// Note that EquivReps creates a new ColSet, so it is safe to modify it
		// in-place with ComputeEquivClosureNoCopy.
		filterEqCols := fd.ComputeEquivClosureNoCopy(fd.EquivReps())
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
//	SELECT * FROM a INNER JOIN b ON a.x=b.x AND a.x + b.y = 5
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

	allowCompositeEncoding := !memo.CanBeCompositeSensitive(c.mem.Metadata(), src)

	// For CanMapJoinOpFilter to be true, each column in src must map to at
	// least one column in dst.
	for i, ok := scalarProps.OuterCols.Next(0); ok; i, ok = scalarProps.OuterCols.Next(i + 1) {
		eqCols := c.GetEquivColsWithEquivType(i, equivFD, allowCompositeEncoding)
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
//	SELECT * FROM a INNER JOIN b ON a.x=b.x AND a.x + b.y = 5
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

	allowCompositeEncoding := !memo.CanBeCompositeSensitive(c.mem.Metadata(), src)

	// Map each column in src to one column in dst. We choose an arbitrary column
	// (the one with the smallest ColumnID) if there are multiple choices.
	var colMap util.FastIntMap
	outerCols := src.ScalarProps().OuterCols
	for srcCol, ok := outerCols.Next(0); ok; srcCol, ok = outerCols.Next(srcCol + 1) {
		eqCols := c.GetEquivColsWithEquivType(srcCol, equivFD, allowCompositeEncoding)
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
// If the column has a composite key encoding, there are extra considerations.
// In general, replacing composite columns with "equivalent" (equal) columns
// might change the result of an expression. For example, consider this query:
//
//	SELECT * FROM
//	  (VALUES (1.0)) AS t1(x),
//	  (VALUES (1.00)) AS t2(y)
//	WHERE x=y AND x::text = '1.0';
//
// It should return the following result:
//
//	  x  |  y
//	-----+------
//	 1.0 | 1.00
//
// But if we use the equality predicate x=y to map x to y and infer an
// additional filter y::text = '1.0', the query would return nothing.
//
// The calling code needs to decide if the remapping of this column is allowed
// or not (depending on the actual expression). If it is allowed,
// allowCompositeEncoding should be true. If allowComposite is false and the
// column has composite encoding, the function returns a set containing only
// col.
func (c *CustomFuncs) GetEquivColsWithEquivType(
	col opt.ColumnID, equivFD props.FuncDepSet, allowCompositeEncoding bool,
) opt.ColSet {
	var res opt.ColSet
	colType := c.f.Metadata().ColumnMeta(col).Type

	// Don't bother looking for equivalent columns if colType has a composite
	// key encoding.
	if !allowCompositeEncoding && colinfo.CanHaveCompositeKeyEncoding(colType) {
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

// JoinFiltersMatchAllLeftRows returns true when each row in the given join's
// left input matches at least one row from the right input, according to the
// join filters.
func (c *CustomFuncs) JoinFiltersMatchAllLeftRows(
	left, right memo.RelExpr, on memo.FiltersExpr,
) bool {
	multiplicity := memo.DeriveJoinMultiplicityFromInputs(left, right, on)
	return multiplicity.JoinFiltersMatchAllLeftRows()
}

// CanExtractJoinComparison returns true if:
//   - one of a, b is bound by the left columns;
//   - the other is bound by the right columns;
//   - a and b are not "bare" variables;
//   - a and b contain no correlated subqueries;
//   - neither a or b are constants.
//   - the comparison is either an equality or an inequality.
//
// Such a comparison can be converted to a column comparison by pushing down
// expressions as projections.
func (c *CustomFuncs) CanExtractJoinComparison(
	a, b opt.ScalarExpr, leftCols, rightCols opt.ColSet,
) bool {
	// Disallow simple comparison between variables.
	if a.Op() == opt.VariableOp && b.Op() == opt.VariableOp {
		return false
	}

	// Recursively compute properties for left and right sides.
	var leftProps, rightProps props.Shared
	memo.BuildSharedProps(a, &leftProps, c.f.evalCtx)
	memo.BuildSharedProps(b, &rightProps, c.f.evalCtx)

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
		// The comparison is of the form:
		//   expression(leftCols) op expression(rightCols)
		return true
	}
	return false
}

// ExtractJoinComparison takes an equality or inequality FiltersItem that was
// identified via a call to CanExtractJoinComparison, and converts it to an
// equality or inequality on "bare" variables, by pushing down more complicated
// expressions as projections. See the ExtractJoinComparisons rule.
func (c *CustomFuncs) ExtractJoinComparison(
	joinOp opt.Operator,
	left, right memo.RelExpr,
	filters memo.FiltersExpr,
	item *memo.FiltersItem,
	private *memo.JoinPrivate,
) memo.RelExpr {
	leftCols := c.OutputCols(left)
	rightCols := c.OutputCols(right)

	cmp := item.Condition
	condLeft := cmp.Child(0).(opt.ScalarExpr)
	a, b := cmp.Child(0).(opt.ScalarExpr), cmp.Child(1).(opt.ScalarExpr)
	op := cmp.Op()

	var cmpLeftProps props.Shared
	memo.BuildSharedProps(condLeft, &cmpLeftProps, c.f.evalCtx)
	if cmpLeftProps.OuterCols.SubsetOf(rightCols) {
		a, b = b, a
		op = opt.CommuteEqualityOrInequalityOp(op)
	}

	var leftProj, rightProj projectBuilder
	leftProj.init(c, leftCols)
	rightProj.init(c, rightCols)

	newFilters := make(memo.FiltersExpr, len(filters))
	for i := range filters {
		if &filters[i] != item {
			newFilters[i] = filters[i]
			continue
		}

		newFilters[i] = c.f.ConstructFiltersItem(
			c.f.DynamicConstruct(op, leftProj.add(a), rightProj.add(b)).(opt.ScalarExpr),
		)
	}

	join := c.f.ConstructJoin(
		joinOp,
		leftProj.buildProject(left),
		rightProj.buildProject(right),
		newFilters,
		private,
	)

	if leftProj.empty() && rightProj.empty() {
		// If no new projections were created, then there are no synthesized
		// columns to project away, so we can return the join. This is possible
		// when projections that are added to left and right are identical to
		// computed columns that are already output left and right. There's no
		// need to re-project these expressions, so projectBuilder will simply
		// pass them through.
		return join
	}

	// Otherwise, project away the synthesized columns.
	outputCols := leftCols
	if joinOp != opt.SemiJoinOp && joinOp != opt.AntiJoinOp {
		// Semi/Anti join only produce the left side columns. All other join types
		// produce columns from both sides.
		outputCols = leftCols.Union(rightCols)
	}
	return c.f.ConstructProject(join, memo.EmptyProjectionsExpr, outputCols)
}

// CommuteJoinFlags returns a join private for the commuted join (where the left
// and right sides are swapped). It adjusts any join flags that are specific to
// one side.
func (c *CustomFuncs) CommuteJoinFlags(p *memo.JoinPrivate) *memo.JoinPrivate {
	if p.Flags.Empty() {
		return p
	}

	// swap is a helper function which swaps the values of two (single-bit) flags.
	swap := func(f, a, b memo.JoinFlags) memo.JoinFlags {
		// If the bits are different, flip them both.
		if f.Has(a) != f.Has(b) {
			f ^= (a | b)
		}
		return f
	}
	f := p.Flags
	f = swap(f, memo.DisallowLookupJoinIntoLeft, memo.DisallowLookupJoinIntoRight)
	f = swap(f, memo.DisallowInvertedJoinIntoLeft, memo.DisallowInvertedJoinIntoRight)
	f = swap(f, memo.DisallowHashJoinStoreLeft, memo.DisallowHashJoinStoreRight)
	f = swap(f, memo.PreferLookupJoinIntoLeft, memo.PreferLookupJoinIntoRight)
	if p.Flags == f {
		return p
	}
	res := *p
	res.Flags = f
	return &res
}

// MakeProjectionsFromValues converts single-row values into projections, for
// use when transforming inner joins with a values operator into a projection.
func (c *CustomFuncs) MakeProjectionsFromValues(values *memo.ValuesExpr) memo.ProjectionsExpr {
	if len(values.Rows) != 1 {
		panic(errors.AssertionFailedf("MakeProjectionsFromValues expects 1 row, got %d",
			len(values.Rows)))
	}
	projections := make(memo.ProjectionsExpr, 0, len(values.Cols))
	elems := values.Rows[0].(*memo.TupleExpr).Elems
	for i, col := range values.Cols {
		projections = append(projections, c.f.ConstructProjectionsItem(elems[i], col))
	}
	return projections
}

// ForeignKeyConstraintFilters examines `fkChild`, the left input to lookup join
// (and possible foreign key child table), and `fkParentScanPrivate`, the lookup
// table of lookup join (and possible foreign key parent table), along with the
// `indexCols` of the current index being considered for lookup join.
// `fkParentJoinKey` contains the ON clause equijoin columns which form a
// reduced strict key and `fkParentEquijoinCols` contains the full set of
// equijoin columns on the foreign key parent lookup table. If `fkParentJoinKey`
// is a proper subset of FK constraint referencing columns on `fkChild`, then
// equijoin predicates are built between referenced and referencing columns of
// the FK constraint not currently represented in `fkParentEquijoinCols`, and
// returned to the caller.
func (c *CustomFuncs) ForeignKeyConstraintFilters(
	fkChild memo.RelExpr,
	fkParentScanPrivate *memo.ScanPrivate,
	indexCols, fkParentJoinKey opt.ColSet,
	fkParentEquijoinCols, fkChildEquijoinCols opt.ColList,
) (fkFilters memo.FiltersExpr) {
	md := c.mem.Metadata()
	fkChildEquijoinColSet := fkChildEquijoinCols.ToSet()

	tableIDs := make(map[opt.TableID]struct{})
	fkChildEquijoinColSet.ForEach(func(x opt.ColumnID) {
		tabID := md.ColumnMeta(x).Table
		if tabID != opt.TableID(0) {
			tableIDs[tabID] = struct{}{}
		}
	})

	matchedEquijoinCols := make(map[opt.ColumnID]opt.ColumnID)
	if len(fkParentEquijoinCols) != len(fkChildEquijoinCols) {
		panic(errors.AssertionFailedf("ForeignKeyConstraintFilters expects fkParentEquijoinCols and fkChildEquijoinCols to have the same number of columns."))
	}
	for i := range fkParentEquijoinCols {
		matchedEquijoinCols[fkParentEquijoinCols[i]] = fkChildEquijoinCols[i]
	}

	parentTable := md.Table(fkParentScanPrivate.Table)
	fkChildNotNullCols := fkChild.Relational().NotNullCols

	for fkChildTableID := range tableIDs {
		fkChildTable := md.Table(fkChildTableID)
		fkChildTableMeta := md.TableMeta(fkChildTableID)
		if fkChildTableMeta.IgnoreForeignKeys {
			// We can't use foreign keys from this table.
			continue
		}

		for i := 0; i < fkChildTable.OutboundForeignKeyCount(); i++ {
			fk := fkChildTable.OutboundForeignKey(i)
			if !fk.Validated() {
				// The data is not guaranteed to follow the foreign key constraint.
				continue
			}
			if parentTable.ID() != fk.ReferencedTableID() {
				continue
			}
			fkFilters = nil
			fkFiltersValid := true
			var fkParentColSet opt.ColSet
			for j := 0; j < fk.ColumnCount() && fkFiltersValid; j++ {
				fkParentColumnOrd := fk.ReferencedColumnOrdinal(parentTable, j)
				fkParentColID := fkParentScanPrivate.Table.ColumnID(fkParentColumnOrd)
				fkParentColSet.Add(fkParentColID)
			}

			// The strict key covered by equijoin columns must be a subset of the FK
			// constraint referenced columns to guarantee equating the remaining FK
			// columns is legal.
			if !fkParentJoinKey.SubsetOf(fkParentColSet) {
				continue
			}
			for j := 0; j < fk.ColumnCount() && fkFiltersValid; j++ {
				fkParentColumnOrd := fk.ReferencedColumnOrdinal(parentTable, j)
				fkChildColumnOrd := fk.OriginColumnOrdinal(fkChildTable, j)
				fkChildColID := fkChildTableID.ColumnID(fkChildColumnOrd)
				fkParentColID := fkParentScanPrivate.Table.ColumnID(fkParentColumnOrd)
				if inputJoinCol, ok := matchedEquijoinCols[fkParentColID]; ok {
					if inputJoinCol != fkChildColID {
						// The equijoin term on fkParentColID in the ON clause doesn't
						// equate to the same child table column as in the FK constraint. Do
						// not derive terms from this constraint.
						fkFiltersValid = false
						break
					}
					// This FK constraint equality predicate is already in the ON clause;
					// no need to build a new one.
					continue
				} else if fkParentJoinKey.Contains(fkParentColID) {
					// If the parent column we're looking at is part of the join key, we
					// expect an entry in matchedEquijoinCols to verify the join predicate
					// columns and FK constraint matched columns are the same. If the
					// entry doesn't exist, perhaps the key was reduced to a different
					// column id than was present in the predicate, in which case we can't
					// verify the predicate.
					fkFiltersValid = false
					break
				}
				// If the lookup table's ScanPrivate does not include the join column
				// for the predicate we want to build, then don't build the predicate.
				if !fkParentScanPrivate.Cols.Contains(fkParentColID) {
					fkFiltersValid = false
					break
				}
				// If the FK child table column is not included in the fkChild's output
				// columns, it would be incorrect to build a predicate on this column.
				if fkChild != nil && !fkChild.Relational().OutputCols.Contains(fkChildColID) {
					fkFiltersValid = false
					break
				}
				if !indexCols.Contains(fkParentColID) {
					fkFiltersValid = false
					break
				}
				if !fkChildNotNullCols.Contains(fkChildColID) {
					// If the base table column is not nullable, the output column will
					// also contain no nulls, unless it comes from the outer table of an
					// outer join, in which case all columns from that source table will
					// be null for any given output row and therefore the ON clause
					// equijoin constraints overlapping the FK constraint will have
					// already disqualified the row. So, addition of derived join
					// constraints is OK.
					if fk.MatchMethod() != tree.MatchFull {
						// The FK child column isn't a not-null output column, so it can't
						// be used in a filter unless this is a MATCH FULL foreign key,
						// which only allows an FK column to be NULL if all FK columns are
						// NULL.
						fkFiltersValid = false
						break
					}
				}
				if fkFiltersValid {
					fkFilters = append(fkFilters,
						c.f.ConstructFiltersItem(
							c.f.ConstructEq(c.f.ConstructVariable(fkChildColID),
								c.f.ConstructVariable(fkParentColID)),
						),
					)
				}
			}
			if fkFiltersValid && len(fkFilters) > 0 {
				// If we got this far without fkFiltersValid being unset, then we found
				// a useful foreign key and have built equality predicates on all PK/FK
				// columns not contained in fkParentEquijoinCols.
				return fkFilters
			}
		}
	}
	return nil
}

// AddDerivedOnClauseConditionsFromFKContraints examines any strict keys from
// the left and right relations and for each key which is a strict subset of the
// referencing columns in a PK/FK constraint, and also has equijoin predicates
// on the strict key columns, new equijoin predicates are built involving the
// missing PK/FK constraints columns and appended to a copy of the ON clause.
func (c *CustomFuncs) AddDerivedOnClauseConditionsFromFKContraints(
	on memo.FiltersExpr, leftRelExpr, rightRelExpr memo.RelExpr,
) memo.FiltersExpr {

	leftPossibleScan := leftRelExpr
	rightPossibleScan := rightRelExpr

	if selectExpr, ok := leftPossibleScan.(*memo.SelectExpr); ok {
		leftPossibleScan = selectExpr.Input
	}

	if selectExpr, ok := rightPossibleScan.(*memo.SelectExpr); ok {
		rightPossibleScan = selectExpr.Input
	}

	leftScan, leftScanFound := leftPossibleScan.(*memo.ScanExpr)
	rightScan, rightScanFound := rightPossibleScan.(*memo.ScanExpr)
	if !leftScanFound && !rightScanFound {
		return on
	}
	var leftUniqueKeyCols, rightUniqueKeyCols opt.ColSet
	var leftjoinCols, rightjoinCols, fkChildLeftTableJoinCols, fkChildRightTableJoinCols opt.ColList
	var okLeft, okRight bool
	if leftScanFound {
		leftUniqueKeyCols, leftjoinCols, fkChildRightTableJoinCols, okLeft =
			c.GetEquijoinStrictKeyCols(on, &leftScan.ScanPrivate, rightRelExpr)
	}
	if rightScanFound {
		rightUniqueKeyCols, rightjoinCols, fkChildLeftTableJoinCols, okRight =
			c.GetEquijoinStrictKeyCols(on, &rightScan.ScanPrivate, leftRelExpr)
	}

	if !okLeft && !okRight {
		return on
	}
	newOn := make(memo.FiltersExpr, len(on))
	copy(newOn, on)
	if okLeft {
		// Pass `leftScan.Cols` to ForeignKeyConstraintFilters because we want to
		// allow derivation of join terms on columns in any index. It may be a waste
		// of CPU to enumerate each index and do this call for every index as the
		// main caller of this function is only determining the scan columns to
		// include. The same applies to the 2nd call below and `rightScan.Cols`.
		fkFiltersFromLeftUniqueIndex := c.ForeignKeyConstraintFilters(
			rightRelExpr, &leftScan.ScanPrivate, leftScan.Cols,
			leftUniqueKeyCols, leftjoinCols, fkChildRightTableJoinCols)
		if len(fkFiltersFromLeftUniqueIndex) > 0 {
			newOn = append(newOn, fkFiltersFromLeftUniqueIndex...)
		}
	}
	if okRight {
		fkFiltersFromRightUniqueIndex := c.ForeignKeyConstraintFilters(
			leftRelExpr, &rightScan.ScanPrivate, rightScan.Cols,
			rightUniqueKeyCols, rightjoinCols, fkChildLeftTableJoinCols)
		if len(fkFiltersFromRightUniqueIndex) > 0 {
			newOn = append(newOn, fkFiltersFromRightUniqueIndex...)
		}
	}
	return newOn
}

// GetEquijoinStrictKeyCols collects the ON clause columns present in equijoin
// predicates in a join between a Scan, `sp`, and another `input` relation of
// indeterminate operation type. If those columns contain a strict key on `sp`,
// the function returns the reduced strict key, plus the list of table equijoin
// columns and positionally-matched list of `input` relation equijoin columns.
func (c *CustomFuncs) GetEquijoinStrictKeyCols(
	on memo.FiltersExpr, sp *memo.ScanPrivate, input memo.RelExpr,
) (tableKeyCols opt.ColSet, tableJoinCols, inputRelJoinCols opt.ColList, ok bool) {
	if sp == nil {
		return opt.ColSet{}, opt.ColList{}, opt.ColList{}, false
	}
	md := c.mem.Metadata()
	funcDeps := memo.MakeTableFuncDep(md, sp.Table)

	// If there is no strict key, no need to proceed further.
	_, ok = funcDeps.StrictKey()
	if !ok {
		return opt.ColSet{}, opt.ColList{}, opt.ColList{}, false
	}

	tempInputRelJoinCols, tempTableCols := memo.ExtractJoinEqualityColumns(
		input.Relational().OutputCols,
		sp.Cols,
		on,
	)

	return getJoinKeyAndEquijoinCols(funcDeps, tempTableCols, tempInputRelJoinCols)
}

// getJoinKeyAndEquijoinCols tests if `tableJoinCols` is a strict key given a
// the table's `funcDeps`, and if so, returns the set
// of reduced join key columns plus the list of table equijoin
// columns and positionally-matched list of input relation equijoin columns.
func getJoinKeyAndEquijoinCols(
	funcDeps *props.FuncDepSet, tableJoinCols, inputRelJoinCols opt.ColList,
) (tableKeyCols opt.ColSet, reducedTableJoinCols, reducedInputRelJoinCols opt.ColList, ok bool) {
	parentTableJoinColSet := tableJoinCols.ToSet()
	if funcDeps.ColsAreStrictKey(parentTableJoinColSet) {
		tableKeyCols = funcDeps.ReduceCols(parentTableJoinColSet)
		reducedInputRelJoinCols = make(opt.ColList, 0, tableKeyCols.Len())
		reducedTableJoinCols = make(opt.ColList, 0, tableKeyCols.Len())
		for i, tableJoinCol := range tableJoinCols {
			if tableKeyCols.Contains(tableJoinCol) {
				reducedInputRelJoinCols = append(reducedInputRelJoinCols, inputRelJoinCols[i])
				reducedTableJoinCols = append(reducedTableJoinCols, tableJoinCol)
			}
		}
		return tableKeyCols, reducedTableJoinCols, reducedInputRelJoinCols, true
	}
	return opt.ColSet{}, opt.ColList{}, opt.ColList{}, false
}
