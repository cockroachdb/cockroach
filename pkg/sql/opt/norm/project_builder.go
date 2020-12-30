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
//   pb.init(c)
//   e1 := pb.add(some expression)
//   e2 := pb.add(some other expression)
//   augmentedInput := pb.buildProject(input, passthrough)
//   // e1 and e2 are VariableOp expressions, with input columns
//   // produced by augmentedInput.
//
type projectBuilder struct {
	f           *Factory
	projections memo.ProjectionsExpr
}

func (pb *projectBuilder) init(f *Factory) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*pb = projectBuilder{f: f}
}

// empty returns true if there are no synthesized columns (and hence a
// projection is not necessary).
func (pb *projectBuilder) empty() bool {
	return len(pb.projections) == 0
}

// add incorporates the given expression as a projection, unless the expression
// is already a "bare" variable. Returns a bare variable expression referring to
// the synthesized column.
func (pb *projectBuilder) add(e opt.ScalarExpr) opt.ScalarExpr {
	if v, ok := e.(*memo.VariableExpr); ok {
		// The expression is a bare variable; we don't need to synthesize a column.
		return v
	}

	newCol := pb.f.Metadata().AddColumn("", e.DataType())
	pb.projections = append(pb.projections, pb.f.ConstructProjectionsItem(e, newCol))
	return pb.f.ConstructVariable(newCol)
}

// buildProject creates the ProjectOp (if needed). The ProjectOp passes through
// the given passthrough columns and adds any synthesized columns.
func (pb *projectBuilder) buildProject(input memo.RelExpr, passthrough opt.ColSet) memo.RelExpr {
	if pb.empty() {
		// Avoid creating a Project that does nothing and just gets elided.
		return input
	}
	return pb.f.ConstructProject(input, pb.projections, passthrough)
}
