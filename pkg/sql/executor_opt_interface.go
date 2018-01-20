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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// NewExecFactory is used from opt tests to create and execute plans.
func (e *Executor) NewExecFactory() opt.ExecFactory {
	txn := client.NewTxn(e.cfg.DB, e.cfg.NodeID.Get(), client.RootTxn)
	p, cleanup := newInternalPlanner("opt", txn, "root", &MemoryMetrics{}, &e.cfg)
	return &execFactory{
		planner: p,
		cleanup: cleanup,
	}
}

type execFactory struct {
	planner *planner
	cleanup func()
}

var _ opt.ExecFactory = &execFactory{}

// Close is part of the opt.ExecFactory interface.
func (ef *execFactory) Close() {
	ef.cleanup()
}

// ConstructScan is part of the opt.ExecFactory interface.
func (ef *execFactory) ConstructScan(table optbase.Table) (opt.ExecNode, error) {
	desc := table.(*sqlbase.TableDescriptor)

	columns := make([]tree.ColumnID, len(desc.Columns))
	for i := range columns {
		columns[i] = tree.ColumnID(desc.Columns[i].ID)
	}
	// Create a scanNode.
	scan := ef.planner.Scan()
	if err := scan.initTable(ef.planner, desc, nil /* hints */, publicColumns, columns); err != nil {
		return nil, err
	}
	var err error
	scan.spans, err = makeSpans(
		ef.planner.EvalContext(), nil /* constraints */, desc, &desc.PrimaryIndex,
	)
	if err != nil {
		return nil, err
	}
	return &execNode{
		execFactory: *ef,
		plan:        scan,
	}, nil
}

// ConstructFilter is part of the opt.ExecFactory interface.
func (ef *execFactory) ConstructFilter(
	n opt.ExecNode, filter tree.TypedExpr,
) (opt.ExecNode, error) {
	en := n.(*execNode)
	src := planDataSource{
		info: &dataSourceInfo{sourceColumns: planColumns(en.plan)},
		plan: en.plan,
	}
	f := &filterNode{
		source: src,
	}
	f.ivarHelper = tree.MakeIndexedVarHelper(f, len(src.info.sourceColumns))
	f.filter = filter
	f.ivarHelper.Rebind(filter, true /* alsoReset */, false /* normalizeToNonNil */)
	return &execNode{
		execFactory: *ef,
		plan:        f,
	}, nil
}

type execNode struct {
	execFactory

	plan planNode
}

var _ opt.ExecNode = &execNode{}

// Explain is part of the opt.ExecNode interface.
func (en *execNode) Explain() ([]tree.Datums, error) {
	// Add an explain node to the plan and run that.
	flags := explainFlags{
		showMetadata: true,
		showExprs:    true,
		qualifyNames: true,
	}
	explainNode, err := en.planner.makeExplainPlanNodeWithPlan(
		context.TODO(), flags, false /* expanded */, false /* optimized */, en.plan,
	)
	if err != nil {
		return nil, err
	}
	// Run the node.
	explainExecNode := execNode{
		execFactory: en.execFactory,
		plan:        explainNode,
	}
	return explainExecNode.Run()
}

// Run is part of the opt.ExecNode interface.
func (en *execNode) Run() ([]tree.Datums, error) {
	params := runParams{
		ctx:             context.TODO(),
		extendedEvalCtx: &en.planner.extendedEvalCtx,
		p:               en.planner,
	}
	if err := startPlan(params, en.plan); err != nil {
		return nil, err
	}
	var res []tree.Datums
	for {
		ok, err := en.plan.Next(params)
		if err != nil {
			return res, nil
		}
		if !ok {
			break
		}
		res = append(res, append(tree.Datums(nil), en.plan.Values()...))
	}
	en.plan.Close(context.TODO())
	return res, nil
}
