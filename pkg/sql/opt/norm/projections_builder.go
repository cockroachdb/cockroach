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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
)

// projectionsBuilder is a helper class that conveniently and efficiently builds
// Projections operators. It:
//
//  1. Allows the Projections operator to be built incrementally.
//  2. Offers several convenient helper methods to add passthrough vs.
//     synthesized columns.
//  3. Automatically unions duplicate columns.
//  4. Reuses "scratch" objects from Factory to hold temporary results.
//  5. Only allocates objects when it becomes absolutely necessary.
//
// Example usage:
//
//   pb := projectionsBuilder{f: f}
//   pb.addPassthroughCol(opt.ColumnID(1))
//   pb.addSynthesized(f.ConstructTrue(), opt.ColumnID(2))
//   pb.addProjections(anotherProjections)
//   res := pb.buildProjections()
//
// The projectionsBuilder should always be constructed on the stack so that it's
// safe to recurse on factory construction methods without danger of trying to
// reuse a scratch object that's already in use further up the stack.
type projectionsBuilder struct {
	f *Factory

	// passthroughCols is the set of columns that are passed through unchanged
	// from the Project operator's input.
	passthroughCols opt.ColSet

	// copyPassthrough is a "copy-on-write" flag. If true, then the passthrough
	// colSet needs to be copied before additional columns can be added to it.
	copyPassthrough bool

	// synthesized is the list of projections that are synthesized from input
	// columns or constant values, rather than being passed through unchanged.
	synthesized []memo.GroupID

	// synthesizedCols is the list of ids of the corresponding synthesized
	// columns.
	synthesizedCols opt.ColList
}

func (b *projectionsBuilder) hasSynthesizedCols() bool {
	return len(b.synthesizedCols) > 0
}

// addPassthroughCol adds a single passthrough column to the set of columns that
// are passed through unchanged from the Project operator's input.
func (b *projectionsBuilder) addPassthroughCol(colID opt.ColumnID) {
	b.ensureMutablePassthrough()
	b.passthroughCols.Add(int(colID))
}

// addPassthroughCols adds a set of passthrough columns to the set of columns
// that are passed through unchanged from the Project operator's input.
func (b *projectionsBuilder) addPassthroughCols(cols opt.ColSet) {
	b.ensureMutablePassthrough()
	if b.passthroughCols.Empty() {
		b.passthroughCols = cols
		b.copyPassthrough = true
	} else {
		b.passthroughCols.UnionWith(cols)
	}
}

// addSynthesized adds a new synthesized column to the Projections operator
// under construction, given the new column's memo group and ID.
func (b *projectionsBuilder) addSynthesized(elem memo.GroupID, colID opt.ColumnID) {
	b.ensureSlices()

	// Check for duplicate column ID.
	for i, synthID := range b.synthesizedCols {
		if synthID == colID {
			if b.synthesized[i] != elem {
				panic(fmt.Sprintf("two different expressions cannot have same column id %d", colID))
			}
			return
		}
	}

	b.synthesized = append(b.synthesized, elem)
	b.synthesizedCols = append(b.synthesizedCols, colID)
}

// addProjections is a helper that adds all passthrough and synthesized columns
// from the given Projections expression. This is equivalent to calling
// addPassthroughCol for each passthrough column in projections and
// addSynthesized for each synthesized column in projections.
func (b *projectionsBuilder) addProjections(projections memo.GroupID) {
	projectionsExpr := b.f.mem.NormExpr(projections).AsProjections()
	def := b.f.funcs.ExtractProjectionsOpDef(projectionsExpr.Def())
	b.ensureSlices()
	b.addPassthroughCols(def.PassthroughCols)

	if len(b.synthesized) == 0 {
		// Copy new set over, as there can be no duplicates.
		b.synthesized = append(b.synthesized, b.f.mem.LookupList(projectionsExpr.Elems())...)
		b.synthesizedCols = append(b.synthesizedCols, def.SynthesizedCols...)
	} else {
		// Add elements one-by-one in order to check for duplicate column ids.
		for i, item := range b.f.mem.LookupList(projectionsExpr.Elems()) {
			b.addSynthesized(item, def.SynthesizedCols[i])
		}
	}
}

// buildProjections constructs the final Projections operator using the
// passthrough and synthesized columns added to the builder. The state of the
// projections builder is reset.
func (b *projectionsBuilder) buildProjections() memo.GroupID {
	// Make copy of synthesized column ids, since correct size is now known, and
	// the scratch ColList needs to be returned to the factory for reuse.
	synthCols := make(opt.ColList, len(b.synthesizedCols))
	copy(synthCols, b.synthesizedCols)

	def := memo.ProjectionsOpDef{
		SynthesizedCols: synthCols,
		PassthroughCols: b.passthroughCols,
	}
	projections := b.f.ConstructProjections(
		b.f.InternList(b.synthesized),
		b.f.InternProjectionsOpDef(&def),
	)

	// Restore the factory scratch lists.
	b.f.scratchItems = b.synthesized
	b.synthesized = nil
	b.f.scratchColList = b.synthesizedCols
	b.synthesizedCols = nil

	// Reset other state.
	b.passthroughCols = opt.ColSet{}
	b.copyPassthrough = false

	return projections
}

func (b *projectionsBuilder) ensureMutablePassthrough() {
	if b.copyPassthrough {
		b.passthroughCols = b.passthroughCols.Copy()
		b.copyPassthrough = false
	}
}

func (b *projectionsBuilder) ensureSlices() {
	// Try to reuse scratch slices stored in factory.
	if b.synthesized == nil {
		b.synthesized = b.f.scratchItems
		b.synthesized = b.synthesized[:0]

		// Set the factory scratch list to nil so that recursive calls won't try
		// to use it when it's already in use.
		b.f.scratchItems = nil
	}

	if b.synthesizedCols == nil {
		b.synthesizedCols = b.f.scratchColList
		b.synthesizedCols = b.synthesizedCols[:0]

		// Set the factory scratch list to nil so that recursive calls won't try
		// to use it when it's already in use.
		b.f.scratchColList = nil
	}
}

// projectBuilder is a helper for constructing a ProjectOp that augments an
// input with new synthesized columns. Sample usage:
//
//   var pb projectBuilder
//   pb.init(c)
//   e1 := pb.add(some expression)
//   e2 := pb.add(some other expression)
//   augmentedInput := pb.buildProject(input)
//   // e1 and e2 are VariableOp expressions, with input columns
//   // produced by augmentedInput.
//
type projectBuilder struct {
	p projectionsBuilder
}

func (pb *projectBuilder) init(c *CustomFuncs) {
	pb.p = projectionsBuilder{f: c.f}
}

// empty returns true if there are no synthesized columns (and hence a
// projection is not necessary).
func (pb *projectBuilder) empty() bool {
	return !pb.p.hasSynthesizedCols()
}

// add incorporates the given expression as a projection, unless the expression
// is already a "bare" variable. Returns a bare variable expression referring to
// the synthesized column.
func (pb *projectBuilder) add(expr memo.GroupID) (varExpr memo.GroupID) {
	mem := pb.p.f.Memo()
	if mem.NormOp(expr) == opt.VariableOp {
		// The expression is a bare variable; we don't need to synthesize a column.
		return expr
	}

	typ := mem.GroupProperties(expr).Scalar.Type
	newCol := pb.p.f.Metadata().AddColumn("", typ)
	pb.p.addSynthesized(expr, newCol)
	return pb.p.f.ConstructVariable(mem.InternColumnID(newCol))
}

// buildProject creates the ProjectOp (if needed). The ProjectOp passes through
// all columns of the input and adds any synthesized columns.
func (pb *projectBuilder) buildProject(input memo.GroupID) memo.GroupID {
	if pb.empty() {
		// Avoid creating a Project that does nothing and just gets elided.
		return input
	}
	pb.p.addPassthroughCols(pb.p.f.Memo().GroupProperties(input).Relational.OutputCols)
	return pb.p.f.ConstructProject(input, pb.p.buildProjections())
}
