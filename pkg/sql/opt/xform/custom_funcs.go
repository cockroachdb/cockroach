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

// ----------------------------------------------------------------------
//
// Scan Rules
//   Custom match and replace functions used with scan.opt rules.
//
// ----------------------------------------------------------------------

// CanGenerateIndexScans returns true if new index Scan operators can be
// generated, based on the given ScanOpDef. Index scans should only be generated
// from the original unaltered primary index Scan operator (i.e. unconstrained
// and not limited).
func (c *CustomFuncs) CanGenerateIndexScans(def memo.PrivateID) bool {
	scanOpDef := c.e.mem.LookupPrivate(def).(*memo.ScanOpDef)
	return scanOpDef.Index == opt.PrimaryIndex &&
		scanOpDef.Constraint == nil &&
		!scanOpDef.Reverse &&
		scanOpDef.HardLimit == 0
}

// CanGenerateInvertedIndexScans returns true if new index Scan operators can
// be generated on inverted indexes. Same as CanGenerateIndexScans, but with
// the additional check that we have at least one inverted index on the table.
func (c *CustomFuncs) CanGenerateInvertedIndexScans(def memo.PrivateID) bool {
	if !c.CanGenerateIndexScans(def) {
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

	primaryIndex := md.Table(scanOpDef.Table).Index(opt.PrimaryIndex)
	pkCols := make(opt.ColList, primaryIndex.KeyColumnCount())
	for i := range pkCols {
		pkCols[i] = md.TableColumn(scanOpDef.Table, primaryIndex.Column(i).Ordinal)
	}

	// Add a reverse index scan memo group for the primary index.
	newDef := &memo.ScanOpDef{Table: scanOpDef.Table, Index: 0, Cols: scanOpDef.Cols, Reverse: true}
	indexScan := memo.MakeScanExpr(c.e.mem.InternScanOpDef(newDef))
	c.e.exprs = append(c.e.exprs, memo.Expr(indexScan))

	// Iterate over all secondary indexes (index 0 is the primary index).
	for i := 1; i < tab.IndexCount(); i++ {
		if tab.Index(i).IsInverted() {
			// Ignore inverted indexes.
			continue
		}
		indexCols := md.IndexColumns(scanOpDef.Table, i)

		// If the alternate index includes the set of needed columns (def.Cols),
		// then construct a new Scan operator using that index.
		if scanOpDef.Cols.SubsetOf(indexCols) {
			newDef := &memo.ScanOpDef{Table: scanOpDef.Table, Index: i, Cols: scanOpDef.Cols}
			indexScan := memo.MakeScanExpr(c.e.mem.InternScanOpDef(newDef))
			c.e.exprs = append(c.e.exprs, memo.Expr(indexScan))

			newDefRev := &memo.ScanOpDef{Table: scanOpDef.Table, Index: i, Cols: scanOpDef.Cols, Reverse: true}
			indexScanRev := memo.MakeScanExpr(c.e.mem.InternScanOpDef(newDefRev))
			c.e.exprs = append(c.e.exprs, memo.Expr(indexScanRev))
		} else {
			// The alternate index was missing columns, so in order to satisfy the
			// requirements, we need to perform an index join with the primary index.

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
			}

			indexScanOpDefRev := memo.ScanOpDef{
				Table:   scanOpDef.Table,
				Index:   i,
				Cols:    scanCols,
				Reverse: true,
			}

			input := c.e.f.ConstructScan(c.e.mem.InternScanOpDef(&indexScanOpDef))
			inputRev := c.e.f.ConstructScan(c.e.mem.InternScanOpDef(&indexScanOpDefRev))

			def := memo.IndexJoinDef{
				Table: scanOpDef.Table,
				Cols:  scanOpDef.Cols,
			}

			private := c.e.mem.InternIndexJoinDef(&def)
			join := memo.MakeIndexJoinExpr(input, private)
			joinRev := memo.MakeIndexJoinExpr(inputRev, private)

			c.e.exprs = append(c.e.exprs, memo.Expr(join))
			c.e.exprs = append(c.e.exprs, memo.Expr(joinRev))
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
		pkColSet.Add(int(md.TableColumn(scanOpDef.Table, primaryIndex.Column(i).Ordinal)))
	}

	// Iterate over all inverted indexes (index 0 is the primary index and is
	// never inverted).
	for i := 1; i < tab.IndexCount(); i++ {
		if !tab.Index(i).IsInverted() {
			continue
		}

		preDef := &memo.ScanOpDef{
			Table: scanOpDef.Table,
			Index: i,
			// Though the index is marked as containing the JSONB column being
			// indexed, it doesn't actually, and it's only valid to extract the
			// primary key columns from it.
			Cols: pkColSet,
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

// CanConstrainScan returns true if the scan can have a constraint applied to
// it. This is only possible when it has no constraints and when it does not
// have a limit applied to it (since limit is applied after the constraint).
func (c *CustomFuncs) CanConstrainScan(def memo.PrivateID) bool {
	scanOpDef := c.e.mem.LookupPrivate(def).(*memo.ScanOpDef)
	return scanOpDef.Constraint == nil && scanOpDef.HardLimit == 0
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
		colID := md.TableColumn(scanOpDef.Table, col.Ordinal)
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
		lookupJoin := c.e.f.ConstructIndexJoin(constrainedScan, indexJoinDef)
		newSelect := memo.MakeSelectExpr(lookupJoin, remainingFilter)
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
	return scanOpDef.CanProvideOrdering(c.e.mem.Metadata(), required)
}

// LimitScanDef constructs a new ScanOpDef private value that is based on the
// given ScanOpDef. The new def's HardLimit is set to the given limit, which
// must be a constant int datum value. The other fields are inherited from the
// existing def.
func (c *CustomFuncs) LimitScanDef(def, limit memo.PrivateID) memo.PrivateID {
	defCopy := *c.e.mem.LookupPrivate(def).(*memo.ScanOpDef)
	defCopy.HardLimit = int64(*c.e.mem.LookupPrivate(limit).(*tree.DInt))
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
	leftOrders := GetInterestingOrderings(memo.MakeNormExprView(c.e.mem, left)).Copy()
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
			break
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
// We also disallow reverse scans since it's enough to apply the rule on the
// corresponding forward scan.
func (c *CustomFuncs) CanUseLookupJoin(
	input memo.GroupID, scanDefID memo.PrivateID, on memo.GroupID,
) bool {
	md := c.e.mem.Metadata()
	scanDef := c.e.mem.LookupPrivate(scanDefID).(*memo.ScanOpDef)
	if scanDef.Constraint != nil || scanDef.HardLimit != 0 || scanDef.Reverse {
		return false
	}

	inputProps := c.e.mem.GroupProperties(input).Relational
	onExpr := memo.MakeNormExprView(c.e.mem, on)
	_, rightEq := harvestEqualityColumns(inputProps.OutputCols, scanDef.Cols, onExpr)

	// Check if the first column in the index has an equality constraint.
	idx := md.Table(scanDef.Table).Index(scanDef.Index)
	firstCol := md.TableColumn(scanDef.Table, idx.Column(0).Ordinal)
	for i := range rightEq {
		if rightEq[i] == firstCol {
			return true
		}
	}
	return false
}

// ConstructLookupJoin creates a lookup join expression.
func (c *CustomFuncs) ConstructLookupJoin(
	joinType opt.Operator, input memo.GroupID, scanDefID memo.PrivateID, on memo.GroupID,
) []memo.Expr {
	md := c.e.mem.Metadata()
	scanDef := c.e.mem.LookupPrivate(scanDefID).(*memo.ScanOpDef)
	inputProps := c.e.mem.GroupProperties(input).Relational
	onExpr := memo.MakeNormExprView(c.e.mem, on)
	leftEq, rightEq := harvestEqualityColumns(inputProps.OutputCols, scanDef.Cols, onExpr)
	n := len(leftEq)
	if n == 0 {
		return nil
	}

	idx := md.Table(scanDef.Table).Index(scanDef.Index)
	numIndexCols := idx.LaxKeyColumnCount()
	lookupJoinDef := memo.LookupJoinDef{
		JoinType:   joinType,
		Table:      scanDef.Table,
		Index:      scanDef.Index,
		LookupCols: scanDef.Cols,
	}
	for i := 0; i < numIndexCols; i++ {
		idxCol := md.TableColumn(scanDef.Table, idx.Column(i).Ordinal)
		found := -1
		for j := range rightEq {
			if rightEq[j] == idxCol {
				found = j
			}
		}
		if found == -1 {
			break
		}
		inputCol := leftEq[found]
		if lookupJoinDef.KeyCols == nil {
			lookupJoinDef.KeyCols = make(opt.ColList, 0, numIndexCols)
		}
		lookupJoinDef.KeyCols = append(lookupJoinDef.KeyCols, inputCol)
	}
	if lookupJoinDef.KeyCols == nil {
		// CanUseLookupJoin ensures this is not possible.
		panic("lookup join not possible")
	}

	// Create a lookup join expression in the same group.
	// TODO(radu): simplify the ON condition (we can remove the equalities on
	// KeyCols).
	lookupJoin := memo.MakeLookupJoinExpr(input, on, c.e.mem.InternLookupJoinDef(&lookupJoinDef))
	c.e.exprs = append(c.e.exprs[:0], memo.Expr(lookupJoin))
	return c.e.exprs
}

// ----------------------------------------------------------------------
//
// GroupBy Rules
//   Custom match and replace functions used with groupby.opt rules.
//
// ----------------------------------------------------------------------

// MakeOne returns a memo.GroupID pointing
// to a Constant Datum Int with value 1. This is required
// because optgen cannot create constants inline.
func (c *CustomFuncs) MakeOne() memo.GroupID {
	return c.e.f.ConstructConst(c.e.f.InternDatum(tree.NewDInt(1)))
}

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
