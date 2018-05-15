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

// hasDuplicateRefs returns true if the target expression references any
// variable more than one time. For example:
//
//   SELECT * FROM a WHERE x=1 OR x=2 OR y=0
//
// hasDuplicateRefs would be true for the filter expression, since the x column
// is referenced twice.
func (f *Factory) hasDuplicateRefs(target memo.GroupID) bool {
	var refs opt.ColSet

	// When a column reference is found, add it to the refs set. If the set
	// already contains a reference to that column, then there is a duplicate.
	var countRefs func(group memo.GroupID) bool
	countRefs = func(group memo.GroupID) bool {
		expr := f.mem.NormExpr(group)
		if expr.Operator() == opt.VariableOp {
			colID := f.extractColID(expr.AsVariable().Col())
			if refs.Contains(int(colID)) {
				return true
			}
			refs.Add(int(colID))
			return false
		}

		if expr.Operator() == opt.ProjectionsOp {
			// Process the pass-through columns, in addition to the children.
			def := expr.Private(f.mem).(*memo.ProjectionsOpDef)
			for i, ok := def.PassthroughCols.Next(0); ok; i, ok = def.PassthroughCols.Next(i + 1) {
				if refs.Contains(i) {
					return true
				}
				refs.Add(i)
			}
		}

		for i := 0; i < expr.ChildCount(); i++ {
			if countRefs(expr.ChildGroup(f.mem, i)) {
				return true
			}
		}
		return false
	}
	return countRefs(target)
}

// canInline returns true if the given expression consists only of "simple"
// operators like Variable, Const, Eq, and Plus. These operators are assumed to
// be relatively inexpensive to evaluate, and therefore potentially evaluating
// them multiple times is not a big concern.
func (f *Factory) canInline(group memo.GroupID) bool {
	expr := f.mem.NormExpr(group)
	switch expr.Operator() {
	case opt.ProjectionsOp,
		opt.AndOp, opt.OrOp, opt.NotOp, opt.TrueOp, opt.FalseOp,
		opt.EqOp, opt.NeOp, opt.LeOp, opt.LtOp, opt.GeOp, opt.GtOp,
		opt.IsOp, opt.IsNotOp, opt.InOp, opt.NotInOp,
		opt.VariableOp, opt.ConstOp, opt.NullOp,
		opt.PlusOp, opt.MinusOp, opt.MultOp:

		// Recursively verify that children are also inlinable.
		for i := 0; i < expr.ChildCount(); i++ {
			if !f.canInline(expr.ChildGroup(f.mem, i)) {
				return false
			}
		}
		return true
	}
	return false
}

// inlineProjections searches the target expression for any references to
// columns in the projections expression. Target variable references are
// replaced by the inlined projection expression.
func (f *Factory) inlineProjections(target, projections memo.GroupID) memo.GroupID {
	projectionsExpr := f.mem.NormExpr(projections).AsProjections()
	projectionsElems := f.mem.LookupList(projectionsExpr.Elems())
	projectionsDef := f.mem.LookupPrivate(projectionsExpr.Def()).(*memo.ProjectionsOpDef)

	// Recursively walk the tree looking for references to projection expressions
	// that need to be replaced.
	var replace memo.ReplaceChildFunc
	replace = func(child memo.GroupID) memo.GroupID {
		varExpr := f.mem.NormExpr(child).AsVariable()
		if varExpr != nil {
			varColID := f.extractColID(varExpr.Col())
			for i, id := range projectionsDef.SynthesizedCols {
				if varColID == id {
					return projectionsElems[i]
				}
			}
		}
		ev := memo.MakeNormExprView(f.mem, child)
		return ev.Replace(f.evalCtx, replace).Group()
	}

	ev := memo.MakeNormExprView(f.mem, target)
	return ev.Replace(f.evalCtx, replace).Group()
}
