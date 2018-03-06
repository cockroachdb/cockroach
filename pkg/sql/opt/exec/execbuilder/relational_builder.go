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

package execbuilder

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

type execPlan struct {
	root exec.Node

	// outputCols maps columns in the output set of a relational expression to
	// indices in the result columns of the ExecNode.
	//
	// The reason we need to keep track of this (instead of using just the
	// relational properties) is that the relational properties don't force a
	// single "schema": any ordering of the output columns is possible. We choose
	// the schema that is most convenient: for scans, we use the table's column
	// ordering. Consider:
	//   SELECT a, b FROM t WHERE a = b
	// and the following two cases:
	//   1. The table is defined as (k INT PRIMARY KEY, a INT, b INT). The scan will
	//      return (k, a, b).
	//   2. The table is defined as (k INT PRIMARY KEY, b INT, a INT). The scan will
	//      return (k, b, a).
	// In these two cases, the relational properties are effectively the same.
	//
	// An alternative to this would be to always use a "canonical" schema, for
	// example the output columns in increasing index order. However, this would
	// require a lot of otherwise unnecessary projections.
	//
	// The number of entries set in the map is always the same with the number of
	// columns emitted by Node.
	//
	// Note: conceptually, this could be a ColList; however, the map is more
	// convenient when converting VariableOps to IndexedVars.
	outputCols opt.ColMap
}

// makeBuildScalarCtx returns a buildScalarCtx that can be used with expressions
// that refer the output columns of this plan.
func (ep *execPlan) makeBuildScalarCtx() buildScalarCtx {
	return buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, ep.outputCols.Len()),
		ivarMap: ep.outputCols,
	}
}

func (b *Builder) buildRelational(ev xform.ExprView) (execPlan, error) {
	var ep execPlan
	var err error
	switch ev.Operator() {
	case opt.ValuesOp:
		ep, err = b.buildValues(ev)

	case opt.ScanOp:
		ep, err = b.buildScan(ev)

	case opt.SelectOp:
		ep, err = b.buildSelect(ev)

	case opt.ProjectOp:
		ep, err = b.buildProject(ev)

	case opt.InnerJoinOp:
		ep, err = b.buildJoin(ev, sqlbase.InnerJoin)

	case opt.LeftJoinOp:
		ep, err = b.buildJoin(ev, sqlbase.LeftOuterJoin)

	case opt.RightJoinOp:
		ep, err = b.buildJoin(ev, sqlbase.RightOuterJoin)

	case opt.FullJoinOp:
		ep, err = b.buildJoin(ev, sqlbase.FullOuterJoin)

	case opt.SemiJoinOp:
		ep, err = b.buildJoin(ev, sqlbase.LeftSemiJoin)

	case opt.AntiJoinOp:
		ep, err = b.buildJoin(ev, sqlbase.LeftAntiJoin)

	default:
		return execPlan{}, errors.Errorf("unsupported relational op %s", ev.Operator())
	}
	if err != nil {
		return execPlan{}, err
	}
	if p := ev.Physical().Presentation; p.Defined() {
		ep, err = b.applyPresentation(ep, ev.Metadata(), p)
	}
	return ep, err
}

func (b *Builder) buildValues(ev xform.ExprView) (execPlan, error) {
	md := ev.Metadata()
	cols := *ev.Private().(*opt.ColList)
	numCols := len(cols)

	rows := make([][]tree.TypedExpr, ev.ChildCount())
	rowBuf := make([]tree.TypedExpr, len(rows)*numCols)
	scalarCtx := buildScalarCtx{}
	for i := range rows {
		row := ev.Child(i)
		if row.ChildCount() != numCols {
			return execPlan{}, fmt.Errorf("inconsistent row length %d vs %d", ev.ChildCount(), numCols)
		}
		// Chop off prefix of rowBuf and limit its capacity.
		rows[i] = rowBuf[:numCols:numCols]
		rowBuf = rowBuf[numCols:]
		for j := 0; j < numCols; j++ {
			rows[i][j] = b.buildScalar(&scalarCtx, row.Child(j))
		}
	}

	resultCols := make(sqlbase.ResultColumns, numCols)
	for i, col := range cols {
		resultCols[i].Name = md.ColumnLabel(col)
		resultCols[i].Typ = md.ColumnType(col)
	}
	node, err := b.factory.ConstructValues(rows, resultCols)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i, col := range cols {
		ep.outputCols.Set(int(col), i)
	}

	return ep, nil
}

func (b *Builder) buildScan(ev xform.ExprView) (execPlan, error) {
	def := ev.Private().(*opt.ScanOpDef)
	md := ev.Metadata()
	tbl := md.Table(def.Table)
	res := execPlan{}

	// Determine output columns for the builder and for ConstructScan.
	n := 0
	cols := make([]optbase.Column, 0, def.Cols.Len())
	for i := 0; i < tbl.ColumnCount(); i++ {
		colIndex := md.TableColumn(def.Table, i)
		if !def.Cols.Contains(int(colIndex)) {
			// Skip columns that don't need to be returned by the scan.
			continue
		}
		cols = append(cols, tbl.Column(i))
		res.outputCols.Set(int(colIndex), n)
		n++
	}

	var err error
	res.root, err = b.factory.ConstructScan(tbl, cols)
	if err != nil {
		return execPlan{}, err
	}

	return res, nil
}

func (b *Builder) buildSelect(ev xform.ExprView) (execPlan, error) {
	input, err := b.buildRelational(ev.Child(0))
	if err != nil {
		return execPlan{}, err
	}
	ctx := input.makeBuildScalarCtx()
	filter := b.buildScalar(&ctx, ev.Child(1))
	node, err := b.factory.ConstructFilter(input.root, filter)
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{
		root: node,
		// A filtering node does not modify the schema.
		outputCols: input.outputCols,
	}, nil
}

func (b *Builder) buildProject(ev xform.ExprView) (execPlan, error) {
	input, err := b.buildRelational(ev.Child(0))
	if err != nil {
		return execPlan{}, err
	}
	projections := ev.Child(1)
	colList := *projections.Private().(*opt.ColList)
	exprs := make(tree.TypedExprs, len(colList))
	colNames := make([]string, len(exprs))
	ctx := input.makeBuildScalarCtx()
	for i, col := range colList {
		exprs[i] = b.buildScalar(&ctx, projections.Child(i))
		colNames[i] = ev.Metadata().ColumnLabel(col)
	}
	node, err := b.factory.ConstructProject(input.root, exprs, colNames)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i, col := range colList {
		ep.outputCols.Set(int(col), i)
	}
	return ep, nil
}

func (b *Builder) buildJoin(ev xform.ExprView, joinType sqlbase.JoinType) (execPlan, error) {
	left, err := b.buildRelational(ev.Child(0))
	if err != nil {
		return execPlan{}, err
	}
	right, err := b.buildRelational(ev.Child(1))
	if err != nil {
		return execPlan{}, err
	}

	// Calculate the outputCols map for the join plan: the first numLeftCols
	// correspond to the columns from the left, the rest correspond to columns
	// from the right (except for Anti and Semi joins).
	var ep execPlan
	numLeftCols := left.outputCols.Len()
	ep.outputCols = left.outputCols.Copy()
	if joinType != sqlbase.LeftSemiJoin && joinType != sqlbase.LeftAntiJoin {
		right.outputCols.ForEach(func(colIdx, rightIdx int) {
			ep.outputCols.Set(colIdx, rightIdx+numLeftCols)
		})
	}

	ctx := ep.makeBuildScalarCtx()
	onExpr := b.buildScalar(&ctx, ev.Child(2))

	ep.root, err = b.factory.ConstructJoin(joinType, left.root, right.root, onExpr)
	if err != nil {
		return execPlan{}, err
	}
	return ep, nil
}

// applyPresentation adds a projection to a plan to satisfy a required
// Presentation property.
func (b *Builder) applyPresentation(
	inputPlan execPlan, md *opt.Metadata, p opt.Presentation,
) (execPlan, error) {
	exprs := make(tree.TypedExprs, len(p))
	colNames := make([]string, len(p))
	ivh := tree.MakeIndexedVarHelper(nil /* container */, inputPlan.outputCols.Len())
	for i := range p {
		idx, ok := inputPlan.outputCols.Get(int(p[i].Index))
		if !ok {
			return execPlan{}, errors.Errorf("presentation includes absent column %d", p[i].Index)
		}
		exprs[i] = ivh.IndexedVarWithType(idx, md.ColumnType(p[i].Index))
		colNames[i] = p[i].Label
	}
	node, err := b.factory.ConstructProject(inputPlan.root, exprs, colNames)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i := range p {
		ep.outputCols.Set(int(p[i].Index), i)
	}
	return ep, nil
}
