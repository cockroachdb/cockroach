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

package testutils

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

func init() {
	// Install the interceptor that implements the ExprFmtHideScalars functionality.
	memo.ExprFmtInterceptor = fmtInterceptor
}

// fmtInterceptor is a function suitable for memo.ExprFmtInterceptor. It detects
// if an expression tree contains only scalar expressions; if so, it tries to
// execbuild them and print the SQL expressions.
func fmtInterceptor(f *memo.ExprFmtCtx, tp treeprinter.Node, ev memo.ExprView, showAllProps bool) bool {
	if !f.HasFlags(memo.ExprFmtHideScalars) || !onlyScalars(ev) {
		return false
	}

	if ev.ChildCount() == 0 && ev.Operator() != opt.FunctionOp {
		// Don't use this code on leaves.
		return false
	}

	if ev.Operator() == opt.FiltersOp {
		// Let the filters node show up; we will apply the code on each filter.
		return false
	}

	// Build the scalar expression and format it as a single tree node.
	bld := execbuilder.New(nil /* factory */, ev, nil /* evalCtx */)
	md := ev.Metadata()
	ivh := tree.MakeIndexedVarHelper(nil /* container */, md.NumColumns())
	expr, err := bld.BuildScalar(&ivh)
	if err != nil {
		// Not all scalar operators are supported (e.g. Projections).
		return false
	}
	f.Buffer.Reset()
	fmtCtx := tree.MakeFmtCtx(f.Buffer, tree.FmtSimple)
	fmtCtx.WithIndexedVarFormat(func(ctx *tree.FmtCtx, idx int) {
		fullyQualify := !f.HasFlags(memo.ExprFmtHideQualifications)
		label := md.QualifiedColumnLabel(opt.ColumnID(idx+1), fullyQualify)
		ctx.WriteString(label)
	})
	expr.Format(&fmtCtx)
	ev.FormatScalarProps(f, showAllProps)
	tp.Child(f.Buffer.String())
	return true
}

func onlyScalars(ev memo.ExprView) bool {
	if !ev.IsScalar() {
		return false
	}
	for i, n := 0, ev.ChildCount(); i < n; i++ {
		if !onlyScalars(ev.Child(i)) {
			return false
		}
	}
	return true
}

type indexedVarContainer struct {
	md *opt.Metadata
}

var _ tree.IndexedVarContainer = &indexedVarContainer{}

// IndexedVarEval is part of the tree.IndexedVarContainer interface.
func (c *indexedVarContainer) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	panic("not implemented")
}

// IndexedVarResolvedType is part of the tree.IndexedVarContainer interface.
func (c *indexedVarContainer) IndexedVarResolvedType(idx int) types.T {
	return c.md.ColumnType(opt.ColumnID(idx))
}

// IndexedVarEval is part of the tree.IndexedVarContainer interface.
func (c *indexedVarContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return &varFormatter{label: c.md.ColumnLabel(opt.ColumnID(idx))}
}

type varFormatter struct {
	label string
}

var _ tree.NodeFormatter = &varFormatter{}

// Format is part of the tree.NodeFormatter interface.
func (v *varFormatter) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(v.label)
}
