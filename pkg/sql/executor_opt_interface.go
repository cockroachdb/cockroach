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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// NewExecEngine is used from exec tests to create and execute plans.
func (e *Executor) NewExecEngine() exec.Engine {
	txn := client.NewTxn(e.cfg.DB, e.cfg.NodeID.Get(), client.RootTxn)
	p, cleanup := newInternalPlanner("opt", txn, "root", &MemoryMetrics{}, &e.cfg)
	return &execEngine{
		planner: p,
		cleanup: cleanup,
	}
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

// ConstructFilter is part of the exec.Factory interface.
func (ee *execEngine) ConstructFilter(n exec.Node, filter tree.TypedExpr) (exec.Node, error) {
	plan := n.(planNode)
	src := planDataSource{
		info: &sqlbase.DataSourceInfo{SourceColumns: planColumns(plan)},
		plan: plan,
	}
	f := &filterNode{
		source: src,
	}
	f.ivarHelper = tree.MakeIndexedVarHelper(f, len(src.info.SourceColumns))
	f.filter = filter
	f.ivarHelper.Rebind(filter, true /* alsoReset */, false /* normalizeToNonNil */)
	return f, nil
}

// ConstructProjection is part of the exec.Factory interface.
func (ee *execEngine) ConstructProjection(n exec.Node, exprs tree.TypedExprs) (exec.Node, error) {
	plan := n.(planNode)
	cols := planColumns(plan)
	src := planDataSource{
		info: &sqlbase.DataSourceInfo{SourceColumns: cols},
		plan: plan,
	}
	r := &renderNode{
		source:     src,
		sourceInfo: sqlbase.MultiSourceInfo{src.info},
	}
	r.ivarHelper = tree.MakeIndexedVarHelper(r, len(cols))
	for _, expr := range exprs {
		expr = r.ivarHelper.Rebind(expr, false /* alsoReset */, false /* normalizeToNonNil */)
		exprStr := symbolicExprStr(expr)
		col := sqlbase.ResultColumn{Name: exprStr, Typ: expr.ResolvedType()}
		r.addRenderColumn(expr, symbolicExprStr(expr), col)
	}
	return r, nil
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
