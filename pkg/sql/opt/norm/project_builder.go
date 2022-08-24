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
)

// projectBuilder is a helper for constructing a ProjectOp that augments an
// input with new synthesized and passthrough columns. Sample usage:
//
//   var pb projectBuilder
//   pb.init(c, passthrough)
//   e1 := pb.add(some expression)
//   e2 := pb.add(some other expression)
//   augmentedInput := pb.buildProject(input)
//   // e1 and e2 are VariableOp expressions, with input columns
//   // produced by augmentedInput.
//
type projectBuilder struct {
	c           *CustomFuncs
	projections memo.ProjectionsExpr
	passthrough opt.ColSet
}

func (pb *projectBuilder) init(c *CustomFuncs, passthrough opt.ColSet) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*pb = projectBuilder{c: c, passthrough: passthrough}
}

// empty returns true if there are no synthesized columns (and hence a
// projection is not necessary).
func (pb *projectBuilder) empty() bool {
	return len(pb.projections) == 0
}

// add attempts to incorporate the given expression as a projection. If the
// expression is already a "bare" variable, no projection is created and the
// variable is returned. If the expression matches a computed column expression
// in a base table, then the computed column is used as the projection column
// and is returned. If this computed column already exists as a passthrough
// column, then no projection is added. If the expression is not a "bare"
// variable and does not match a computed column expression, a new column is
// synthesized, a projection is created, and the synthesized column is returned.
func (pb *projectBuilder) add(e opt.ScalarExpr) opt.ScalarExpr {
	if v, ok := e.(*memo.VariableExpr); ok {
		// The expression is a bare variable; we don't need to synthesize a column.
		return v
	}

	// Look for a computed column in a base table with an identical expression.
	// If one exists, we can use it as the projection column instead of
	// synthesizing a new column.
	var projectCol opt.ColumnID
	if cols := pb.c.OuterCols(e); !cols.Empty() {
		// Get the base table of the first column referenced in e.
		col, _ := cols.Next(0)
		if tabID := pb.c.f.Metadata().ColumnMeta(col).Table; tabID != 0 {
			// If the column has a base table (i.e., it is not a synthesized
			// column), search for a computed column expression in the base
			// table identical to e.
			for compCol, expr := range pb.c.f.Metadata().TableMeta(tabID).ComputedCols {
				if e == expr {
					projectCol = compCol
					break
				}
			}
		}
	}

	// If the input to the Project already contains the computed column, then
	// there is no need to re-project its expression.
	if pb.passthrough.Contains(projectCol) {
		return pb.c.f.ConstructVariable(projectCol)
	}

	// If we did not find an existing computed column with the same expression,
	// synthesize a new column.
	if projectCol == 0 {
		projectCol = pb.c.f.Metadata().AddColumn("", e.DataType())
	}

	pb.projections = append(pb.projections, pb.c.f.ConstructProjectionsItem(e, projectCol))
	return pb.c.f.ConstructVariable(projectCol)
}

// buildProject creates the ProjectOp (if needed). The ProjectOp passes through
// the given passthrough columns and adds any synthesized columns.
func (pb *projectBuilder) buildProject(input memo.RelExpr) memo.RelExpr {
	if pb.empty() {
		// Avoid creating a Project that does nothing and just gets elided.
		return input
	}
	return pb.c.f.ConstructProject(input, pb.projections, pb.passthrough)
}
