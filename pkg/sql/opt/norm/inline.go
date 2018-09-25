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

// HasDuplicateRefs returns true if the target scalar expression references any
// outer column more than one time, or if it has correlated subqueries. For
// example:
//
//   SELECT x+1, x+2, y FROM a
//
// HasDuplicateRefs would be true for the Projections expression, since the x
// column is referenced twice.
//
// Correlated subqueries are disallowed since it introduces additional
// complexity for a case that's not important for inlining (correlated
// subqueries are hoisted to a higher context anyway).
func (c *CustomFuncs) HasDuplicateRefs(target memo.GroupID) bool {
	// Don't bother traversing the expression tree if there is a correlated
	// subquery.
	scalar := c.LookupScalar(target)
	if scalar.HasCorrelatedSubquery {
		return true
	}

	var refs opt.ColSet

	// When a column reference is found, add it to the refs set. If the set
	// already contains a reference to that column, then there is a duplicate.
	// findDupRefs returns true if the subtree contains at least one duplicate.
	var findDupRefs func(group memo.GroupID) bool
	findDupRefs = func(group memo.GroupID) bool {
		expr := c.f.mem.NormExpr(group)
		if !expr.IsScalar() {
			// We know that this is not a correlated subquery since
			// scalar.HasCorrelatedSubquery was already checked above.
			// Uncorrelated subqueries never have references.
			return false
		}

		switch expr.Operator() {
		case opt.VariableOp:
			// Count Variable references.
			colID := c.ExtractColID(expr.AsVariable().Col())
			if refs.Contains(int(colID)) {
				return true
			}
			refs.Add(int(colID))
			return false

		case opt.ProjectionsOp:
			// Process the pass-through columns, in addition to the children.
			def := c.ExtractProjectionsOpDef(expr.AsProjections().Def())
			if def.PassthroughCols.Intersects(refs) {
				return true
			}
			refs.UnionWith(def.PassthroughCols)
		}

		for i := 0; i < expr.ChildCount(); i++ {
			if findDupRefs(expr.ChildGroup(c.mem, i)) {
				return true
			}
		}
		return false
	}

	return findDupRefs(target)
}

// CanInline returns true if the given expression consists only of "simple"
// operators like Variable, Const, Eq, and Plus. These operators are assumed to
// be relatively inexpensive to evaluate, and therefore potentially evaluating
// them multiple times is not a big concern.
func (c *CustomFuncs) CanInline(group memo.GroupID) bool {
	expr := c.f.mem.NormExpr(group)
	switch expr.Operator() {
	case opt.ProjectionsOp,
		opt.AndOp, opt.OrOp, opt.NotOp, opt.TrueOp, opt.FalseOp,
		opt.EqOp, opt.NeOp, opt.LeOp, opt.LtOp, opt.GeOp, opt.GtOp,
		opt.IsOp, opt.IsNotOp, opt.InOp, opt.NotInOp,
		opt.VariableOp, opt.ConstOp, opt.NullOp,
		opt.PlusOp, opt.MinusOp, opt.MultOp:

		// Recursively verify that children are also inlinable.
		for i := 0; i < expr.ChildCount(); i++ {
			if !c.CanInline(expr.ChildGroup(c.mem, i)) {
				return false
			}
		}
		return true
	}
	return false
}

// InlineProjections searches the target scalar expression for any references to
// columns in the projections expression. Target variable references are
// replaced by the inlined projection expression.
func (c *CustomFuncs) InlineProjections(target, projections memo.GroupID) memo.GroupID {
	projectionsExpr := c.f.mem.NormExpr(projections).AsProjections()
	projectionsElems := c.f.mem.LookupList(projectionsExpr.Elems())
	projectionsDef := c.ExtractProjectionsOpDef(projectionsExpr.Def())

	// Recursively walk the tree looking for references to projection expressions
	// that need to be replaced.
	var replace memo.ReplaceChildFunc
	replace = func(child memo.GroupID) memo.GroupID {
		expr := c.f.mem.NormExpr(child)
		if !expr.IsScalar() {
			if !c.OuterCols(child).Empty() {
				// Should have prevented this in HasDuplicateRefs/HasCorrelatedSubquery.
				panic("cannot inline references within correlated subqueries")
			}
			return child
		}

		switch expr.Operator() {
		case opt.VariableOp:
			varColID := c.ExtractColID(expr.AsVariable().Col())
			for i, id := range projectionsDef.SynthesizedCols {
				if varColID == id {
					return projectionsElems[i]
				}
			}
			return child

		case opt.ProjectionsOp:
			pb := projectionsBuilder{f: c.f}
			targetProjections := expr.AsProjections()
			targetDef := c.ExtractProjectionsOpDef(targetProjections.Def())
			targetPassthroughCols := targetDef.PassthroughCols
			targetSynthesizedCols := targetDef.SynthesizedCols
			targetElems := c.f.mem.LookupList(targetProjections.Elems())

			// Start by inlining any references within synthesized columns.
			for i, elem := range targetElems {
				pb.addSynthesized(replace(elem), targetSynthesizedCols[i])
			}

			// Now inline any references within passthrough columns. Do this by
			// iterating over the underlying projection's synthesized columns,
			// looking for any that are passed through.
			for i, id := range projectionsDef.SynthesizedCols {
				if targetPassthroughCols.Contains(int(id)) {
					pb.addSynthesized(projectionsElems[i], id)
				}
			}

			// Finally, add any passthrough columns that are also passthrough
			// columns in the underlying projection.
			pb.addPassthroughCols(targetPassthroughCols.Intersection(projectionsDef.PassthroughCols))

			return pb.buildProjections()
		}

		return c.f.DynamicConstruct(expr.Operator(), expr.ReplaceOperands(c.mem, replace))
	}

	return replace(target)
}
