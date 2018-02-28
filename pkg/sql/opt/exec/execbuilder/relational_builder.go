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

	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
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
	switch ev.Operator() {
	case opt.ValuesOp:
		return b.buildValues(ev)

	case opt.ScanOp:
		return b.buildScan(ev)

	case opt.SelectOp:
		return b.buildSelect(ev)

	case opt.ProjectOp:
		return b.buildProject(ev)

	case opt.InnerJoinOp:
		return b.buildJoin(ev, sqlbase.InnerJoin)

	case opt.LeftJoinOp:
		return b.buildJoin(ev, sqlbase.LeftOuterJoin)

	case opt.RightJoinOp:
		return b.buildJoin(ev, sqlbase.RightOuterJoin)

	case opt.FullJoinOp:
		return b.buildJoin(ev, sqlbase.FullOuterJoin)

	case opt.SemiJoinOp:
		return b.buildJoin(ev, sqlbase.LeftSemiJoin)

	case opt.AntiJoinOp:
		return b.buildJoin(ev, sqlbase.LeftAntiJoin)

	default:
		return execPlan{}, errors.Errorf("unsupported relational op %s", ev.Operator())
	}
}

func (b *Builder) buildValues(ev xform.ExprView) (execPlan, error) {
	cols := *ev.Private().(*opt.ColList)
	if len(cols) > 0 {
		return execPlan{}, fmt.Errorf("ValuesOp with columns not implemented")
	}
	rows := make([][]tree.TypedExpr, ev.ChildCount())
	node, err := b.factory.ConstructValues(rows, nil /* colTypes */, nil /* colNames */)
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{root: node}, nil
}

func (b *Builder) buildScan(ev xform.ExprView) (execPlan, error) {
	tblIndex := ev.Private().(opt.TableIndex)
	md := ev.Metadata()
	tbl := md.Table(tblIndex)
	node, err := b.factory.ConstructScan(tbl)
	if err != nil {
		return execPlan{}, err
	}
	res := execPlan{root: node}
	for i := 0; i < tbl.ColumnCount(); i++ {
		res.outputCols.Set(int(md.TableColumn(tblIndex, i)), i)
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
