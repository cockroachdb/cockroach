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
	pkCols := make(opt.ColList, primaryIndex.UniqueColumnCount())
	for i := range pkCols {
		pkCols[i] = md.TableColumn(scanOpDef.Table, primaryIndex.Column(i).Ordinal)
	}

	// Iterate over all secondary indexes (index 0 is the primary index).
	for i := 1; i < tab.IndexCount(); i++ {
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

			// We scan whatever needed columns are left from the primary index.
			def := memo.LookupJoinDef{
				Table:      scanOpDef.Table,
				Index:      opt.PrimaryIndex,
				KeyCols:    pkCols,
				LookupCols: scanOpDef.Cols.Difference(indexScanOpDef.Cols),
			}

			join := memo.MakeLookupJoinExpr(input, c.e.mem.InternLookupJoinDef(&def))

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
	md := c.e.mem.Metadata()
	index := md.Table(scanOpDef.Table).Index(scanOpDef.Index)
	columns := make([]opt.OrderingColumn, index.UniqueColumnCount())
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

// ConstrainLookupJoinIndexScan tries to push filters into Lookup Join index
// scan operations as constraints. It is applied on a Select -> LookupJoin ->
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
// TODO(rytaft): In the future we want to do the following:
// 1. push constraints in the scan.
// 2. put whatever part of the remaining filter that uses just index columns
//    into a select above the index scan (and below the lookup join).
// 3. put what is left of the filter on top of the lookup join.
func (c *CustomFuncs) ConstrainLookupJoinIndexScan(
	filterGroup memo.GroupID, scanDef, lookupJoinDef memo.PrivateID,
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
		lookupJoin := memo.MakeLookupJoinExpr(constrainedScan, lookupJoinDef)
		c.e.exprs = append(c.e.exprs, memo.Expr(lookupJoin))
	} else {
		// We have a remaining filter. We create the constrained lookup join index
		// scan in a new group and create a select node in the same group with the
		// original select.
		lookupJoin := c.e.f.ConstructLookupJoin(constrainedScan, lookupJoinDef)
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

	required := c.e.mem.LookupPrivate(ordering).(props.Ordering)
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
