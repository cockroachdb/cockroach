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
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

var _ opt.ExecBuilderFactory = &Executor{}

// NewExecBuilder is part of the opt.ExecBuilderFactory interface.
func (e *Executor) NewExecBuilder() opt.ExecBuilder {
	metrics := MakeMemMetrics("opt", time.Second)
	session := NewSession(context.TODO(), SessionArgs{User: "root"}, e, nil /* remote */, &metrics)
	txn := client.NewTxn(e.cfg.DB, e.cfg.NodeID.Get())
	return &execBuilder{
		planner: session.newPlanner(e, txn),
	}
}

type execBuilder struct {
	planner *planner
}

var _ opt.ExecBuilder = &execBuilder{}

// Scan is part of the opt.ExecBuilder interface.
func (eb *execBuilder) Scan(table optbase.Table) (opt.ExecNode, error) {
	desc := table.(*sqlbase.TableDescriptor)

	columns := make([]tree.ColumnID, len(desc.Columns))
	for i := range columns {
		columns[i] = tree.ColumnID(desc.Columns[i].ID)
	}
	// Create a scanNode.
	scan := eb.planner.Scan()
	if err := scan.initTable(eb.planner, desc, nil /* hints */, publicColumns, columns); err != nil {
		return nil, err
	}
	var err error
	scan.spans, err = makeSpans(
		eb.planner.EvalContext(), nil /* constraints */, desc, &desc.PrimaryIndex,
	)
	if err != nil {
		return nil, err
	}
	return &execNode{
		execBuilder: *eb,
		plan:        scan,
	}, nil
}

type execNode struct {
	execBuilder

	plan planNode
}

var _ opt.ExecNode = &execNode{}

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
