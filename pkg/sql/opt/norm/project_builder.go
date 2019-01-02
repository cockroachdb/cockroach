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
	pb.f = f
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
	pb.projections = append(pb.projections, memo.ProjectionsItem{
		Element:    e,
		ColPrivate: memo.ColPrivate{Col: newCol},
	})
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
