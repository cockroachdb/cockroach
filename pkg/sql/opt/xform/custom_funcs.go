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

package xform

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/idxconstraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// CustomFuncs contains all the custom match and replace functions used by
// the exploration rules. The unnamed xfunc.CustomFuncs allows
// CustomFuncs to provide a clean interface for calling functions from both the
// xform and xfunc packages using the same struct.
type CustomFuncs struct {
	norm.CustomFuncs
	e *explorer
}

// Init initializes a new CustomFuncs with the given explorer.
func (c *CustomFuncs) Init(e *explorer) {
	c.CustomFuncs.Init(e.f)
	c.e = e
}

// ----------------------------------------------------------------------
//
// Scan Rules
//   Custom match and replace functions used with scan.opt rules.
//
// ----------------------------------------------------------------------

// IsCanonicalScan returns true if the given ScanOpDef is an original unaltered
// primary index Scan operator (i.e. unconstrained and not limited).
func (c *CustomFuncs) IsCanonicalScan(def memo.PrivateID) bool {
	scanOpDef := c.e.mem.LookupPrivate(def).(*memo.ScanOpDef)
	return scanOpDef.Index == opt.PrimaryIndex &&
		scanOpDef.Constraint == nil &&
		scanOpDef.HardLimit == 0
}

// CanGenerateInvertedIndexScans returns true if new index Scan operators can
// be generated on inverted indexes. Same as IsCanonicalScan, but with
// the additional check that we have at least one inverted index on the table.
func (c *CustomFuncs) CanGenerateInvertedIndexScans(def memo.PrivateID) bool {
	if !c.IsCanonicalScan(def) {
		return false
	}

	// Don't bother matching unless there's an inverted index.
	scanOpDef := c.e.mem.LookupPrivate(def).(*memo.ScanOpDef)
	md := c.e.mem.Metadata()
	tab := md.Table(scanOpDef.Table)
	// Index 0 is the primary index and is never inverted.
	for i, n := 1, tab.IndexCount(); i < n; i++ {
		if tab.Index(i).IsInverted() {
			return true
		}
	}
	return false
}

// GenerateIndexScans enumerates all indexes on the scan operator's table and
// generates an alternate scan operator for each index that includes the set of
// needed columns.
func (c *CustomFuncs) GenerateIndexScans(def memo.PrivateID) []memo.Expr {
	c.e.exprs = c.e.exprs[:0]
	scanOpDef := c.e.mem.LookupPrivate(def).(*memo.ScanOpDef)
	md := c.e.mem.Metadata()
	tab := md.Table(scanOpDef.Table)

	// Iterate over all secondary indexes (index 0 is the primary index).
	var pkCols opt.ColList
	for i := 1; i < tab.IndexCount(); i++ {
		if tab.Index(i).IsInverted() {
			// Ignore inverted indexes.
			continue
		}
		if scanOpDef.Flags.ForceIndex && scanOpDef.Flags.Index != i {
			// If we are forcing a specific index, don't bother with the others.
			continue
		}

		indexCols := md.IndexColumns(scanOpDef.Table, i)

		// If the alternate index includes the set of needed columns (def.Cols),
		// then construct a new Scan operator using that index.
		if scanOpDef.Cols.SubsetOf(indexCols) {
			newDef := *scanOpDef
			newDef.Index = i
			indexScan := memo.MakeScanExpr(c.e.mem.InternScanOpDef(&newDef))
			c.e.exprs = append(c.e.exprs, memo.Expr(indexScan))
		} else if !scanOpDef.Flags.NoIndexJoin {
			// The alternate index was missing columns, so in order to satisfy the
			// requirements, we need to perform an index join with the primary index.
			if pkCols == nil {
				primaryIndex := md.Table(scanOpDef.Table).Index(opt.PrimaryIndex)
				pkCols = make(opt.ColList, primaryIndex.KeyColumnCount())
				for i := range pkCols {
					pkCols[i] = scanOpDef.Table.ColumnID(primaryIndex.Column(i).Ordinal)
				}
			}

			// We scan whatever columns we need which are available from the index,
			// plus the PK columns. The main reason is to allow pushing of filters as
			// much as possible.
			scanCols := indexCols.Intersection(scanOpDef.Cols)
			for _, c := range pkCols {
				scanCols.Add(int(c))
			}

			indexScanOpDef := memo.ScanOpDef{
				Table: scanOpDef.Table,
				Index: i,
				Cols:  scanCols,
				Flags: scanOpDef.Flags,
			}

			input := c.e.f.ConstructScan(c.e.mem.InternScanOpDef(&indexScanOpDef))

			def := memo.IndexJoinDef{
				Table: scanOpDef.Table,
				Cols:  scanOpDef.Cols,
			}

			private := c.e.mem.InternIndexJoinDef(&def)
			indexJoin := memo.MakeIndexJoinExpr(input, private)
			c.e.exprs = append(c.e.exprs, memo.Expr(indexJoin))
		}
	}

	return c.e.exprs
}

// GenerateInvertedIndexScans enumerates all inverted indexes on the scan
// operator's table and generates a scan for each inverted index that can
// service the query.
// The resulting scan operator is pre-constrained and may come with an index join.
// The reason it's pre-constrained is that we cannot treat an inverted index in
// the same way as a regular index, since it does not actually contain the
// indexed column.
func (c *CustomFuncs) GenerateInvertedIndexScans(
	def memo.PrivateID, filter memo.GroupID,
) []memo.Expr {
	c.e.exprs = c.e.exprs[:0]
	scanOpDef := c.e.mem.LookupPrivate(def).(*memo.ScanOpDef)
	md := c.e.mem.Metadata()
	tab := md.Table(scanOpDef.Table)

	primaryIndex := md.Table(scanOpDef.Table).Index(opt.PrimaryIndex)
	var pkColSet opt.ColSet
	for i := 0; i < primaryIndex.KeyColumnCount(); i++ {
		pkColSet.Add(int(scanOpDef.Table.ColumnID(primaryIndex.Column(i).Ordinal)))
	}

	// Iterate over all inverted indexes (index 0 is the primary index and is
	// never inverted).
	for i := 1; i < tab.IndexCount(); i++ {
		if !tab.Index(i).IsInverted() {
			continue
		}
		if scanOpDef.Flags.ForceIndex && scanOpDef.Flags.Index != i {
			// If we are forcing a specific index, ignore the others.
			continue
		}

		preDef := &memo.ScanOpDef{
			Table: scanOpDef.Table,
			Index: i,
			// Though the index is marked as containing the JSONB column being
			// indexed, it doesn't actually, and it's only valid to extract the
			// primary key columns from it.
			Cols:  pkColSet,
			Flags: scanOpDef.Flags,
		}

		constrainedScan, remainingFilter, ok := c.constrainedScanOpDef(filter, c.e.mem.InternScanOpDef(preDef), true /* isInverted */)
		if !ok {
			continue
		}

		// TODO(justin): We might not need to do an index join in order to get the
		// correct columns, but it's difficult to tell at this point.
		def := c.e.mem.InternIndexJoinDef(&memo.IndexJoinDef{
			Table: scanOpDef.Table,
			Cols:  scanOpDef.Cols,
		})
		scan := c.e.f.ConstructScan(c.e.mem.InternScanOpDef(&constrainedScan))

		if c.e.mem.NormExpr(remainingFilter).Operator() == opt.TrueOp {
			c.e.exprs = append(
				c.e.exprs,
				memo.Expr(memo.MakeIndexJoinExpr(scan, def)),
			)
		} else {
			c.e.exprs = append(
				c.e.exprs,
				memo.Expr(
					memo.MakeSelectExpr(
						c.e.f.ConstructIndexJoin(scan, def),
						remainingFilter,
					),
				),
			)
		}
	}

	return c.e.exprs
}

// ----------------------------------------------------------------------
//
// Select Rules
//   Custom match and replace functions used with select.opt rules.
//
// ----------------------------------------------------------------------

// CanConstrainScan returns true if the scan could potentially have a constraint
// applied to it from the given filter. This is only allowed when the scan
//  - does not already have constraints, and
//  - does not have a limit (which applies pre-filter).
// The function also makes some fast checks on the filter and returns false if
// it detects that the filter will not result in any index constraints.
func (c *CustomFuncs) CanConstrainScan(def memo.PrivateID, filter memo.GroupID) bool {
	scanOpDef := c.e.mem.LookupPrivate(def).(*memo.ScanOpDef)
	if scanOpDef.Constraint != nil || scanOpDef.HardLimit != 0 {
		return false
	}
	md := c.e.mem.Metadata()
	index := md.Table(scanOpDef.Table).Index(scanOpDef.Index)
	firstIndexCol := scanOpDef.Table.ColumnID(index.Column(0).Ordinal)
	// If the filter does not involve the first index column, we won't be able to
	// generate a constraint.
	filterProps := c.LookupLogical(filter).Scalar
	if !filterProps.OuterCols.Contains(int(firstIndexCol)) {
		return false
	}
	// If the constraints are tight, check if there is a constraint that starts
	// with the first index column. If the constraints are not tight, it's
	// possible that index constraints can still be generated (that code currently
	// supports more expressions).
	if filterProps.TightConstraints {
		cset := filterProps.Constraints
		for i := 0; i < cset.Length(); i++ {
			firstCol := cset.Constraint(i).Columns.Get(0).ID()
			if firstCol == firstIndexCol {
				return true
			}
		}
		// None of the constraints start with firstIndexCol.
		return false
	}
	return true
}

// constrainedScanOpDef tries to push a filter into a scanOpDef as a
// constraint. If it cannot push the filter down (i.e. the resulting constraint
// is unconstrained), then it returns ok = false in the third return value.
func (c *CustomFuncs) constrainedScanOpDef(
	filterGroup memo.GroupID, scanDef memo.PrivateID, isInverted bool,
) (newDef memo.ScanOpDef, remainingFilter memo.GroupID, ok bool) {
	scanOpDef := c.e.mem.LookupPrivate(scanDef).(*memo.ScanOpDef)

	// Fill out data structures needed to initialize the idxconstraint library.
	// Use LaxKeyColumnCount, since all columns <= LaxKeyColumnCount are
	// guaranteed to be part of each row's key (i.e. not stored in row's value,
	// which does not take part in an index scan).
	md := c.e.mem.Metadata()
	index := md.Table(scanOpDef.Table).Index(scanOpDef.Index)
	columns := make([]opt.OrderingColumn, index.LaxKeyColumnCount())
	var notNullCols opt.ColSet
	for i := range columns {
		col := index.Column(i)
		colID := scanOpDef.Table.ColumnID(col.Ordinal)
		columns[i] = opt.MakeOrderingColumn(colID, col.Descending)
		if !col.Column.IsNullable() {
			notNullCols.Add(int(colID))
		}
	}

	// Generate index constraints.
	var ic idxconstraint.Instance
	filter := memo.MakeNormExprView(c.e.mem, filterGroup)
	ic.Init(filter, columns, notNullCols, isInverted, c.e.evalCtx, c.e.f)
	constraint := ic.Constraint()
	if constraint.IsUnconstrained() {
		return memo.ScanOpDef{}, 0, false
	}
	newDef = *scanOpDef
	newDef.Constraint = constraint

	remainingFilter = ic.RemainingFilter()
	return newDef, remainingFilter, true
}

// ConstrainScan tries to push filters into Scan operations as constraints. It
// is applied on a Select -> Scan pattern. The scan operation is assumed to have
// no constraints.
//
// There are three cases:
//
//  - if the filter can be completely converted to constraints, we return a
//    constrained scan expression (to be added to the same group as the select
//    operator).
//
//  - if the filter can be partially converted to constraints, we construct the
//    constrained scan and we return a select expression with the remaining
//    filter (to be added to the same group as the select operator).
//
//  - if the filter cannot be converted to constraints, does and returns
//    nothing.
//
func (c *CustomFuncs) ConstrainScan(filterGroup memo.GroupID, scanDef memo.PrivateID) []memo.Expr {
	c.e.exprs = c.e.exprs[:0]

	newDef, remainingFilter, ok := c.constrainedScanOpDef(filterGroup, scanDef, false /* isInverted */)
	if !ok {
		return nil
	}

	if c.e.mem.NormExpr(remainingFilter).Operator() == opt.TrueOp {
		// No remaining filter. Add the constrained scan node to select's group.
		constrainedScan := memo.MakeScanExpr(c.e.mem.InternScanOpDef(&newDef))
		c.e.exprs = append(c.e.exprs, memo.Expr(constrainedScan))
	} else {
		// We have a remaining filter. We create the constrained scan in a new group
		// and create a select node in the same group with the original select.
		constrainedScan := c.e.f.ConstructScan(c.e.mem.InternScanOpDef(&newDef))
		newSelect := memo.MakeSelectExpr(constrainedScan, remainingFilter)
		c.e.exprs = append(c.e.exprs, memo.Expr(newSelect))
	}
	return c.e.exprs
}

// ConstrainIndexJoinScan tries to push filters into Index Join index
// scan operations as constraints. It is applied on a Select -> IndexJoin ->
// Scan pattern. The scan operation is assumed to have no constraints.
//
// There are three cases, similar to ConstrainScan:
//
//  - if the filter can be completely converted to constraints, we return a
//    constrained scan expression wrapped in a lookup join (to be added to the
//    same group as the select operator).
//
//  - if the filter can be partially converted to constraints, we construct the
//    constrained scan wrapped in a lookup join, and we return a select
//    expression with the remaining filter (to be added to the same group as
//    the select operator).
//
//  - if the filter cannot be converted to constraints, does and returns
//    nothing.
//
func (c *CustomFuncs) ConstrainIndexJoinScan(
	filterGroup memo.GroupID, scanDef, indexJoinDef memo.PrivateID,
) []memo.Expr {
	c.e.exprs = c.e.exprs[:0]

	newDef, remainingFilter, ok := c.constrainedScanOpDef(filterGroup, scanDef, false /* isInverted */)
	if !ok {
		return nil
	}
	constrainedScan := c.e.f.ConstructScan(c.e.mem.InternScanOpDef(&newDef))

	if c.e.mem.NormExpr(remainingFilter).Operator() == opt.TrueOp {
		// No remaining filter. Add the constrained lookup join index scan node to
		// select's group.
		lookupJoin := memo.MakeIndexJoinExpr(constrainedScan, indexJoinDef)
		c.e.exprs = append(c.e.exprs, memo.Expr(lookupJoin))
	} else {
		// We have a remaining filter. We create the constrained lookup join index
		// scan in a new group and create a select node in the same group with the
		// original select.
		indexJoin := c.e.f.ConstructIndexJoin(constrainedScan, indexJoinDef)
		newSelect := memo.MakeSelectExpr(indexJoin, remainingFilter)
		c.e.exprs = append(c.e.exprs, memo.Expr(newSelect))
	}
	return c.e.exprs
}

// ----------------------------------------------------------------------
//
// Limit Rules
//   Custom match and replace functions used with limit.opt rules.
//
// ----------------------------------------------------------------------

// CanLimitScan returns true if the given expression can have its output row
// count limited. This is only possible when there is no existing limit and
// when the required ordering of the rows to be limited can be satisfied by the
// scan operator.
func (c *CustomFuncs) CanLimitScan(def, limit, ordering memo.PrivateID) bool {
	limitVal := int64(*c.e.mem.LookupPrivate(limit).(*tree.DInt))
	if limitVal <= 0 {
		// Can't push limit into scan if it's zero or negative.
		return false
	}

	scanOpDef := c.e.mem.LookupPrivate(def).(*memo.ScanOpDef)
	if scanOpDef.HardLimit != 0 {
		// Don't push limit into scan if scan is already limited. This should
		// only happen when normalizations haven't run, as otherwise redundant
		// Limit operators would be discarded.
		return false
	}

	required := c.e.mem.LookupPrivate(ordering).(*props.OrderingChoice)
	ok, _ := scanOpDef.CanProvideOrdering(c.e.mem.Metadata(), required)
	return ok
}

// LimitScanDef constructs a new ScanOpDef private value that is based on the
// given ScanOpDef. The new def's HardLimit is set to the given limit, which
// must be a constant int datum value. The other fields are inherited from the
// existing def.
func (c *CustomFuncs) LimitScanDef(def, limit, ordering memo.PrivateID) memo.PrivateID {
	// Determine the scan direction necessary to provide the required ordering.
	scanOpDef := c.e.mem.LookupPrivate(def).(*memo.ScanOpDef)
	required := c.e.mem.LookupPrivate(ordering).(*props.OrderingChoice)
	_, reverse := scanOpDef.CanProvideOrdering(c.e.mem.Metadata(), required)

	defCopy := *scanOpDef
	defCopy.HardLimit = memo.MakeScanLimit(int64(*c.e.mem.LookupPrivate(limit).(*tree.DInt)), reverse)
	return c.e.mem.InternScanOpDef(&defCopy)
}

// ----------------------------------------------------------------------
//
// Join Rules
//   Custom match and replace functions used with join.opt rules.
//
// ----------------------------------------------------------------------

// ConstructMergeJoins spawns MergeJoinOps, based on any interesting orderings.
func (c *CustomFuncs) ConstructMergeJoins(
	originalOp opt.Operator, left memo.GroupID, right memo.GroupID, on memo.GroupID,
) []memo.Expr {
	c.e.exprs = c.e.exprs[:0]
	leftProps := c.e.mem.GroupProperties(left).Relational
	rightProps := c.e.mem.GroupProperties(right).Relational

	onExpr := memo.MakeNormExprView(c.e.mem, on)

	leftEq, rightEq := harvestEqualityColumns(leftProps.OutputCols, rightProps.OutputCols, onExpr)
	n := len(leftEq)
	if n == 0 {
		return nil
	}

	// We generate MergeJoin expressions based on interesting orderings from the
	// left side. The CommuteJoin rule will ensure that we actually try both
	// sides.
	leftOrders := DeriveInterestingOrderings(memo.MakeNormExprView(c.e.mem, left)).Copy()
	leftOrders.RestrictToCols(opt.ColListToSet(leftEq))
	if len(leftOrders) == 0 {
		return nil
	}

	var colToEq util.FastIntMap
	for i := range leftEq {
		colToEq.Set(int(leftEq[i]), i)
		colToEq.Set(int(rightEq[i]), i)
	}

	for _, o := range leftOrders {
		if len(o) < n {
			// TODO(radu): we have a partial ordering on the equality columns. We
			// should augment it with the other columns (in arbitrary order) in the
			// hope that we can get the full ordering cheaply using a "streaming"
			// sort. This would not useful now since we don't support streaming sorts.
			continue
		}
		def := memo.MergeOnDef{JoinType: originalOp}
		def.LeftEq = make(opt.Ordering, n)
		def.RightEq = make(opt.Ordering, n)
		def.LeftOrdering.Columns = make([]props.OrderingColumnChoice, 0, n)
		def.RightOrdering.Columns = make([]props.OrderingColumnChoice, 0, n)
		for i := 0; i < n; i++ {
			eqIdx, _ := colToEq.Get(int(o[i].ID()))
			l, r, descending := leftEq[eqIdx], rightEq[eqIdx], o[i].Descending()
			def.LeftEq[i] = opt.MakeOrderingColumn(l, descending)
			def.RightEq[i] = opt.MakeOrderingColumn(r, descending)
			def.LeftOrdering.AppendCol(l, descending)
			def.RightOrdering.AppendCol(r, descending)
		}

		// Simplify the orderings with the corresponding FD sets.
		def.LeftOrdering.Simplify(&c.e.mem.GroupProperties(left).Relational.FuncDeps)
		def.RightOrdering.Simplify(&c.e.mem.GroupProperties(right).Relational.FuncDeps)

		// TODO(radu): simplify the ON condition (we can remove the equalities we
		// extracted).
		mergeOn := c.e.f.ConstructMergeOn(on, c.e.mem.InternMergeOnDef(&def))
		// Create a merge join expression in the same group.
		mergeJoin := memo.MakeMergeJoinExpr(left, right, mergeOn)
		c.e.exprs = append(c.e.exprs, memo.Expr(mergeJoin))
	}

	return c.e.exprs
}

// harvestEqualityColumns returns a list of pairs of columns (one from the left
// side, one from the right side) which are constrained to be equal.
func harvestEqualityColumns(
	leftCols, rightCols opt.ColSet, on memo.ExprView,
) (leftEq opt.ColList, rightEq opt.ColList) {
	if on.Operator() != opt.FiltersOp {
		return nil, nil
	}
	for i := 0; i < on.ChildCount(); i++ {
		e := on.Child(i)
		ok, left, right := isJoinEquality(leftCols, rightCols, e)
		if !ok {
			continue
		}
		// Don't allow any column to show up twice.
		// TODO(radu): need to figure out the right thing to do in cases
		// like: left.a = right.a AND left.a = right.b
		duplicate := false
		for i := range leftEq {
			if leftEq[i] == left || rightEq[i] == right {
				duplicate = true
				break
			}
		}
		if !duplicate {
			leftEq = append(leftEq, left)
			rightEq = append(rightEq, right)
		}
	}
	return leftEq, rightEq
}

func isJoinEquality(
	leftCols, rightCols opt.ColSet, expr memo.ExprView,
) (ok bool, left, right opt.ColumnID) {
	if expr.Operator() != opt.EqOp {
		return false, 0, 0
	}
	lhs, rhs := expr.Child(0), expr.Child(1)
	if lhs.Operator() != opt.VariableOp || rhs.Operator() != opt.VariableOp {
		return false, 0, 0
	}
	// Don't allow mixed types (see #22519).
	if !lhs.Logical().Scalar.Type.Equivalent(rhs.Logical().Scalar.Type) {
		return false, 0, 0
	}
	lhsCol := lhs.Private().(opt.ColumnID)
	rhsCol := rhs.Private().(opt.ColumnID)
	if leftCols.Contains(int(lhsCol)) && rightCols.Contains(int(rhsCol)) {
		return true, lhsCol, rhsCol
	}
	if leftCols.Contains(int(rhsCol)) && rightCols.Contains(int(lhsCol)) {
		return true, rhsCol, lhsCol
	}
	return false, 0, 0
}

// CanUseLookupJoin returns true if a join with the given join type can be
// converted to a lookup join. This is only possible when:
//  - the scan has no constraints, and
//  - the scan has no limit, and
//  - a prefix of the scan index columns have equality constraints.
func (c *CustomFuncs) CanUseLookupJoin(
	input memo.GroupID, scanDefID memo.PrivateID, on memo.GroupID,
) bool {
	md := c.e.mem.Metadata()
	scanDef := c.e.mem.LookupPrivate(scanDefID).(*memo.ScanOpDef)
	if scanDef.Constraint != nil || scanDef.HardLimit != 0 {
		return false
	}

	inputProps := c.e.mem.GroupProperties(input).Relational
	onExpr := memo.MakeNormExprView(c.e.mem, on)
	_, rightEq := harvestEqualityColumns(inputProps.OutputCols, scanDef.Cols, onExpr)

	// Check if the first column in the index has an equality constraint.
	idx := md.Table(scanDef.Table).Index(scanDef.Index)
	firstCol := scanDef.Table.ColumnID(idx.Column(0).Ordinal)
	for i := range rightEq {
		if rightEq[i] == firstCol {
			return true
		}
	}
	return false
}

// GenerateLookupJoins looks at the possible indexes and creates lookup join
// expressions in the current group.
func (c *CustomFuncs) GenerateLookupJoins(
	joinType opt.Operator, input memo.GroupID, scanDefID memo.PrivateID, on memo.GroupID,
) []memo.Expr {
	c.e.exprs = c.e.exprs[:0]
	md := c.e.mem.Metadata()
	scanDef := c.e.mem.LookupPrivate(scanDefID).(*memo.ScanOpDef)
	inputProps := c.e.mem.GroupProperties(input).Relational
	onExpr := memo.MakeNormExprView(c.e.mem, on)

	leftEq, rightEq := harvestEqualityColumns(inputProps.OutputCols, scanDef.Cols, onExpr)
	n := len(leftEq)
	if n == 0 {
		return nil
	}

	tab := md.Table(scanDef.Table)
	var pkCols opt.ColList

	for i := 0; i < tab.IndexCount(); i++ {
		if !c.canGenerateLookupJoin(scanDef, rightEq, i) {
			continue
		}

		idx := tab.Index(i)
		lookupJoinDef := memo.LookupJoinDef{
			JoinType: joinType,
			Table:    scanDef.Table,
			Index:    i,
		}

		// Find the longest prefix of index key columns that are equality columns.
		numIndexKeyCols := idx.LaxKeyColumnCount()
		lookupJoinDef.KeyCols = make(opt.ColList, 0, numIndexKeyCols)
		for j := 0; j < numIndexKeyCols; j++ {
			idxCol := scanDef.Table.ColumnID(idx.Column(j).Ordinal)
			eqIdx, ok := opt.FindInColList(rightEq, idxCol)
			if !ok {
				break
			}
			lookupJoinDef.KeyCols = append(lookupJoinDef.KeyCols, leftEq[eqIdx])
		}

		// TODO(radu): simplify the ON condition (we can remove the equalities on
		// KeyCols).
		lookupJoinOn := on

		indexCols := md.IndexColumns(scanDef.Table, i)
		if scanDef.Cols.SubsetOf(indexCols) {
			// The index covers all the columns we need.
			//
			//     Join                       LookupJoin(t@idx))
			//     /   \                           |
			//    /     \            ->            |
			//  Input  Scan(t)                   Input
			//
			lookupJoinDef.LookupCols = scanDef.Cols
			lookupJoin := memo.MakeLookupJoinExpr(
				input,
				lookupJoinOn,
				c.e.mem.InternLookupJoinDef(&lookupJoinDef),
			)
			c.e.exprs = append(c.e.exprs, memo.Expr(lookupJoin))
			continue
		}

		// The index is not covering. We have to generate an index join above the
		// lookup join. Note that this index join is also a LookupJoin, because an
		// IndexJoin can only output columns from one table, whereas we also need to
		// output columns from Left.
		//
		//     Join                       LookupJoin(t@primary)
		//     /   \                           |
		//    /     \            ->            |
		//  Input  Scan(t)                LookupJoin(t@idx)
		//                                     |
		//                                     |
		//                                   Input
		//
		// For example:
		//  CREATE TABLE abc (a PRIMARY KEY, b INT, c INT)
		//  CREATE TABLE xyz (x PRIMARY KEY, y INT, z INT, INDEX (y))
		//  SELECT * FROM abc JOIN xyz ON a=y
		//
		// We want to first join abc with the index on y (which provides columns y, x)
		// and then use a lookup join to retrieve column z. The "index join" (top
		// LookupJoin) will produce columns a,b,c,x,y; the lookup columns are just z
		// (the original index join produced x,y,z).
		//
		// Note that the top LookupJoin "sees" column IDs from the table on both
		// "sides" (in this example x,y on the left and z on the right) but there is
		// no overlap.
		//
		if scanDef.Flags.NoIndexJoin {
			continue
		}

		if pkCols == nil {
			pkIndex := md.Table(scanDef.Table).Index(opt.PrimaryIndex)
			pkCols = make(opt.ColList, pkIndex.KeyColumnCount())
			for i := range pkCols {
				pkCols[i] = scanDef.Table.ColumnID(pkIndex.Column(i).Ordinal)
			}
		}

		// The lower LookupJoin must return all PK columns (they are needed as key
		// columns for the index join).
		lookupJoinDef.LookupCols = scanDef.Cols.Intersection(indexCols)
		for i := range pkCols {
			lookupJoinDef.LookupCols.Add(int(pkCols[i]))
		}

		var indexJoinOn memo.GroupID

		// onCols are the columns that the ON condition in the (lower) lookup join
		// can refer to: input columns, or columns available in the index.
		onCols := indexCols.Union(inputProps.OutputCols)
		if c.IsBoundBy(lookupJoinOn, onCols) {
			// The ON condition refers only to the columns available in the index.
			//
			// For LeftJoin, both LookupJoins perform a LeftJoin. A null-extended row
			// from the lower LookupJoin will not have any matches in the top
			// LookupJoin (it has NULLs on key columns) and will get null-extended
			// there as well.
			indexJoinOn = c.e.f.ConstructTrue()
		} else {
			// ON has some conditions that are bound by the columns in the index (at
			// the very least, the equality conditions we used for KeyCols), and some
			// conditions that refer to other columns. We can put the former in the
			// lower LookupJoin and the latter in the index join.
			//
			// This works for InnerJoin but not for LeftJoin because of a
			// technicality: if an input (left) row has matches in the lower
			// LookupJoin but has no matches in the index join, only the columns
			// looked up by the top index join get NULL-extended.
			if joinType == opt.LeftJoinOp {
				// TODO(radu): support LeftJoin, perhaps by looking up all columns and
				// discarding columns that are already available from the lower
				// LookupJoin. This requires a projection to avoid having the same
				// ColumnIDs on both sides of the index join.
				continue
			}
			conditions := c.e.mem.NormExpr(lookupJoinOn).AsFilters().Conditions()
			lookupJoinOn = c.e.f.ConstructFilters(c.ExtractBoundConditions(conditions, onCols))
			indexJoinOn = c.e.f.ConstructFilters(c.ExtractUnboundConditions(conditions, onCols))
		}

		indexJoinDef := memo.LookupJoinDef{
			JoinType:   joinType,
			Table:      scanDef.Table,
			Index:      opt.PrimaryIndex,
			KeyCols:    pkCols,
			LookupCols: scanDef.Cols.Difference(indexCols),
		}

		// Create the LookupJoin for the index join in the same group.
		indexJoin := memo.MakeLookupJoinExpr(
			c.e.f.ConstructLookupJoin(
				input,
				lookupJoinOn,
				c.e.mem.InternLookupJoinDef(&lookupJoinDef),
			),
			indexJoinOn,
			c.e.f.InternLookupJoinDef(&indexJoinDef),
		)
		c.e.exprs = append(c.e.exprs, memo.Expr(indexJoin))
	}

	return c.e.exprs
}

// canGenerateLookupJoin determines if the given index can be used to perform a
// lookup join with equality constraints on eqCols.
func (c *CustomFuncs) canGenerateLookupJoin(
	scanDef *memo.ScanOpDef, eqCols opt.ColList, indexOrdinal int,
) bool {
	if scanDef.Flags.ForceIndex && scanDef.Flags.Index != indexOrdinal {
		// If we are forcing a specific index, don't bother with the others.
		return false
	}
	idx := c.e.mem.Metadata().Table(scanDef.Table).Index(indexOrdinal)
	if idx.IsInverted() {
		// Ignore inverted indexes.
		return false
	}

	// Check if the first column in the index has an equality constraint.
	firstIdxCol := scanDef.Table.ColumnID(idx.Column(0).Ordinal)
	_, ok := opt.FindInColList(eqCols, firstIdxCol)
	return ok
}

// ----------------------------------------------------------------------
//
// GroupBy Rules
//   Custom match and replace functions used with groupby.opt rules.
//
// ----------------------------------------------------------------------

// MakeOrderingChoiceFromColumn constructs a new OrderingChoice with
// one element in the sequence: the columnID in the order defined by
// (MIN/MAX) operator. This function was originally created to be used
// with the Replace(Min|Max)WithLimit exploration rules.
func (c *CustomFuncs) MakeOrderingChoiceFromColumn(
	op opt.Operator, col memo.PrivateID,
) memo.PrivateID {
	oc := props.OrderingChoice{}
	switch op {
	case opt.MinOp:
		oc.AppendCol(c.ExtractColID(col), false /* descending */)
	case opt.MaxOp:
		oc.AppendCol(c.ExtractColID(col), true /* descending */)
	}
	return c.e.f.InternOrderingChoice(&oc)
}
