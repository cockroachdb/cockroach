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

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// NewExecEngine is used from exec tests to create and execute plans.
func (e *Executor) NewExecEngine() (exec.Engine, optbase.Catalog) {
	txn := client.NewTxn(e.cfg.DB, e.cfg.NodeID.Get(), client.RootTxn)
	p, cleanup := newInternalPlanner("opt", txn, "root", &MemoryMetrics{}, &e.cfg)
	ee := &execEngine{
		planner: p,
		cleanup: cleanup,
	}
	return ee, ee
}

type execEngine struct {
	planner *planner
	cleanup func()
}

var _ exec.Engine = &execEngine{}
var _ exec.Factory = &execEngine{}

// Factory is part of the exec.Engine interface.
func (ee *execEngine) Factory() exec.Factory {
	return ee
}

// Close is part of the exec.Engine interface.
func (ee *execEngine) Close() {
	ee.cleanup()
}

func (ee *execEngine) ConstructValues(
	rows [][]tree.TypedExpr, colTypes []types.T, colNames []string,
) (exec.Node, error) {
	if len(colTypes) != 0 {
		panic("values with columns not implemented")
	}
	values := ee.planner.newContainerValuesNode(sqlbase.ResultColumns{}, len(rows))
	for range rows {
		values.rows.AddRow(context.TODO(), tree.Datums{})
	}
	return values, nil
}

// ConstructScan is part of the exec.Factory interface.
func (ee *execEngine) ConstructScan(table optbase.Table) (exec.Node, error) {
	desc := table.(*sqlbase.TableDescriptor)

	columns := make([]tree.ColumnID, len(desc.Columns))
	for i := range columns {
		columns[i] = tree.ColumnID(desc.Columns[i].ID)
	}
	// Create a scanNode.
	scan := ee.planner.Scan()
	if err := scan.initTable(context.TODO(), ee.planner, desc, nil /* hints */, publicColumns, columns); err != nil {
		return nil, err
	}
	var err error
	scan.spans, err = unconstrainedSpans(desc, &desc.PrimaryIndex)
	if err != nil {
		return nil, err
	}
	return scan, nil
}

func asDataSource(n exec.Node) planDataSource {
	plan := n.(planNode)
	return planDataSource{
		info: &sqlbase.DataSourceInfo{SourceColumns: planColumns(plan)},
		plan: plan,
	}
}

// ConstructFilter is part of the exec.Factory interface.
func (ee *execEngine) ConstructFilter(n exec.Node, filter tree.TypedExpr) (exec.Node, error) {
	src := asDataSource(n)
	f := &filterNode{
		source: src,
	}
	f.ivarHelper = tree.MakeIndexedVarHelper(f, len(src.info.SourceColumns))
	f.filter = filter
	f.ivarHelper.Rebind(filter, true /* alsoReset */, false /* normalizeToNonNil */)
	return f, nil
}

// ConstructProject is part of the exec.Factory interface.
func (ee *execEngine) ConstructProject(
	n exec.Node, exprs tree.TypedExprs, colNames []string,
) (exec.Node, error) {
	src := asDataSource(n)
	r := &renderNode{
		source:     src,
		sourceInfo: sqlbase.MultiSourceInfo{src.info},
	}
	r.ivarHelper = tree.MakeIndexedVarHelper(r, len(src.info.SourceColumns))
	for i, expr := range exprs {
		expr = r.ivarHelper.Rebind(expr, false /* alsoReset */, false /* normalizeToNonNil */)
		col := sqlbase.ResultColumn{Name: colNames[i], Typ: expr.ResolvedType()}
		// We don't need to pass the render string, it is only used
		// in planning code.
		r.addRenderColumn(expr, "" /* exprStr */, col)
	}
	return r, nil
}

// ConstructInnerJoin is part of the exec.Factory interface.
func (ee *execEngine) ConstructInnerJoin(
	left, right exec.Node, onCond tree.TypedExpr,
) (exec.Node, error) {
	p := ee.planner
	leftSrc := asDataSource(left)
	rightSrc := asDataSource(right)
	pred, _, err := p.makeJoinPredicate(
		context.TODO(), leftSrc.info, rightSrc.info, joinTypeInner, nil, /* cond */
	)
	if err != nil {
		return nil, err
	}
	onCond = pred.iVarHelper.Rebind(
		onCond, false /* alsoReset */, false, /* normmalizeToNonNil */
	)
	// Try to harvest equality columns from the ON expression.
	onAndExprs := splitAndExpr(p.EvalContext(), onCond, nil /* exprs */)
	for _, e := range onAndExprs {
		if e != tree.DBoolTrue && !pred.tryAddEqualityFilter(e, leftSrc.info, rightSrc.info) {
			pred.onCond = mergeConj(pred.onCond, e)
		}
	}

	return p.makeJoinNode(leftSrc, rightSrc, pred), nil
}

// Execute is part of the exec.Engine interface.
func (ee *execEngine) Execute(n exec.Node) ([]tree.Datums, error) {
	plan := n.(planNode)

	params := runParams{
		ctx:             context.TODO(),
		extendedEvalCtx: &ee.planner.extendedEvalCtx,
		p:               ee.planner,
	}
	if err := startPlan(params, plan); err != nil {
		return nil, err
	}
	var res []tree.Datums
	for {
		ok, err := plan.Next(params)
		if err != nil {
			return res, nil
		}
		if !ok {
			break
		}
		res = append(res, append(tree.Datums(nil), plan.Values()...))
	}
	plan.Close(context.TODO())
	return res, nil
}

// Explain is part of the exec.Engine interface.
func (ee *execEngine) Explain(n exec.Node) ([]tree.Datums, error) {
	plan := n.(planNode)

	// Add an explain node to the plan and run that.
	flags := explainFlags{
		showMetadata: true,
		showExprs:    true,
		qualifyNames: true,
	}
	explainNode, err := ee.planner.makeExplainPlanNodeWithPlan(
		context.TODO(), flags, false /* expanded */, false /* optimized */, plan,
	)
	if err != nil {
		return nil, err
	}

	// Execute the explain node.
	return ee.Execute(explainNode)
}

var _ optbase.Catalog = &execEngine{}

// FindTable is part of the optbase.Catalog interface.
func (ee *execEngine) FindTable(ctx context.Context, name *tree.TableName) (optbase.Table, error) {
	p := ee.planner
	return p.Tables().getTableVersion(ctx, p.txn, p.getVirtualTabler(), name)
}
