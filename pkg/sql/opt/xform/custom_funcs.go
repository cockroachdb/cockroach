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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xfunc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// CustomFuncs contains all the custom match and replace functions used by
// the exploration rules. The unnamed xfunc.CustomFuncs allows
// CustomFuncs to provide a clean interface for calling functions from both the
// xform and xfunc packages using the same struct.
type CustomFuncs struct {
	xfunc.CustomFuncs
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
		scanOpDef.HardLimit == 0
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

	// Iterate over all secondary indexes (index 0 is the primary index).
	for i := 1; i < tab.IndexCount(); i++ {
		if tab.Index(i).IsInverted() {
			// Ignore inverted indexes for now.
			continue
		}
		indexCols := md.IndexColumns(scanOpDef.Table, i)

		// If the alternate index includes the set of needed columns (def.Cols),
		// then construct a new Scan operator using that index.
		if scanOpDef.Cols.SubsetOf(indexCols) {
			newDef := &memo.ScanOpDef{Table: scanOpDef.Table, Index: i, Cols: scanOpDef.Cols}
			indexScan := memo.MakeScanExpr(c.e.mem.InternScanOpDef(newDef))
			c.e.exprs = append(c.e.exprs, memo.Expr(indexScan))
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

			input := c.e.f.ConstructScan(c.e.mem.InternScanOpDef(&indexScanOpDef))

			def := memo.IndexJoinDef{
				Table: scanOpDef.Table,
				Cols:  scanOpDef.Cols,
			}

			join := memo.MakeIndexJoinExpr(input, c.e.mem.InternIndexJoinDef(&def))

			c.e.exprs = append(c.e.exprs, memo.Expr(join))
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

// CanConstrainScan returns true if the given expression can have a constraint
// applied to it. This is only possible when it has no constraints and when it
// does not have a limit applied to it (since limit is applied after the
// constraint).
func (c *CustomFuncs) CanConstrainScan(def memo.PrivateID) bool {
	scanOpDef := c.e.mem.LookupPrivate(def).(*memo.ScanOpDef)
	return scanOpDef.Constraint == nil && scanOpDef.HardLimit == 0
}

// constrainedScanOpDef tries to push a filter into a scanOpDef as a
// constraint. If it cannot push the filter down (i.e. the resulting constraint
// is unconstrained), then it returns ok = false in the third return value.
func (c *CustomFuncs) constrainedScanOpDef(
	filterGroup memo.GroupID, scanDef memo.PrivateID,
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
	ic.Init(filter, columns, notNullCols, false /* isInverted */, c.e.evalCtx, c.e.f)
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

	newDef, remainingFilter, ok := c.constrainedScanOpDef(filterGroup, scanDef)
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

	newDef, remainingFilter, ok := c.constrainedScanOpDef(filterGroup, scanDef)
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
		def.LeftEq.Columns = make([]props.OrderingColumnChoice, 0, n)
		def.RightEq.Columns = make([]props.OrderingColumnChoice, 0, n)
		for i := 0; i < n; i++ {
			eqIdx, _ := colToEq.Get(int(o[i].ID()))
			def.LeftEq.AppendCol(leftEq[eqIdx], o[i].Descending())
			def.RightEq.AppendCol(rightEq[eqIdx], o[i].Descending())
		}
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

// MakeAscOrderingChoiceFromColumn constructs a new OrderingChoice with
// one element in the sequence: the columnID in the ascending direction.
func (c *CustomFuncs) MakeAscOrderingChoiceFromColumn(col memo.PrivateID) memo.PrivateID {
	oc := props.OrderingChoice{}
	oc.AppendCol(c.ExtractColID(col), false /* descending */)
	return c.e.f.InternOrderingChoice(&oc)
}

// AnyType returns the private ID of a new AnyTime type.
func (c *CustomFuncs) AnyType() memo.PrivateID {
	return c.e.f.InternType(types.Any)
}
