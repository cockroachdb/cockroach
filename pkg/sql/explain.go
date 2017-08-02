// Copyright 2015 The Cockroach Authors.
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
	"fmt"
	"strings"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type explainMode int

const (
	explainNone explainMode = iota
	explainPlan
	// explainDistSQL shows the physical distsql plan for a query and whether a
	// query would be run in "auto" DISTSQL mode. See explainDistSQLNode for
	// details.
	explainDistSQL
)

var explainStrings = map[explainMode]string{
	explainPlan:    "plan",
	explainDistSQL: "distsql",
}

// Explain executes the explain statement, providing debugging and analysis
// info about the wrapped statement.
//
// Privileges: the same privileges as the statement being explained.
func (p *planner) Explain(ctx context.Context, n *parser.Explain) (planNode, error) {
	mode := explainNone

	optimized := true
	expanded := true
	normalizeExprs := true
	explainer := explainer{
		showMetadata: false,
		showExprs:    false,
		showTypes:    false,
		doIndent:     false,
	}

	for _, opt := range n.Options {
		optLower := strings.ToLower(opt)
		newMode := explainNone
		// Search for the string in `explainStrings`.
		for mode, modeStr := range explainStrings {
			if optLower == modeStr {
				newMode = mode
				break
			}
		}
		if newMode == explainNone {
			switch optLower {
			case "types":
				newMode = explainPlan
				explainer.showExprs = true
				explainer.showTypes = true
				// TYPES implies METADATA.
				explainer.showMetadata = true

			case "indent":
				explainer.doIndent = true

			case "symvars":
				explainer.symbolicVars = true

			case "metadata":
				explainer.showMetadata = true

			case "qualify":
				explainer.qualifyNames = true

			case "verbose":
				// VERBOSE implies EXPRS.
				explainer.showExprs = true
				// VERBOSE implies QUALIFY.
				explainer.qualifyNames = true
				// VERBOSE implies METADATA.
				explainer.showMetadata = true

			case "exprs":
				explainer.showExprs = true

			case "noexpand":
				expanded = false

			case "nonormalize":
				normalizeExprs = false

			case "nooptimize":
				optimized = false

			default:
				return nil, fmt.Errorf("unsupported EXPLAIN option: %s", opt)
			}
		}
		if newMode != explainNone {
			if mode != explainNone {
				return nil, fmt.Errorf("cannot set EXPLAIN mode more than once: %s", opt)
			}
			mode = newMode
		}
	}
	if mode == explainNone {
		mode = explainPlan
	}

	p.evalCtx.SkipNormalize = !normalizeExprs

	plan, err := p.newPlan(ctx, n.Statement, nil)
	if err != nil {
		return nil, err
	}
	switch mode {
	case explainDistSQL:
		return &explainDistSQLNode{
			plan:           plan,
			distSQLPlanner: p.session.distSQLPlanner,
			txn:            p.txn,
		}, nil

	case explainPlan:
		// We may want to show placeholder types, so ensure no values
		// are missing.
		p.semaCtx.Placeholders.FillUnassigned()
		return p.makeExplainPlanNode(explainer, expanded, optimized, plan), nil

	default:
		return nil, fmt.Errorf("unsupported EXPLAIN mode: %d", mode)
	}
}

// explainDistSQLNode is a planNode that wraps a plan and returns
// information related to running that plan under DistSQL.
type explainDistSQLNode struct {
	optColumnsSlot

	plan           planNode
	distSQLPlanner *distSQLPlanner

	// txn is the current transaction (used for the fake span resolver).
	txn *client.Txn

	// The single row returned by the node.
	values parser.Datums

	// done is set if Next() was called.
	done bool
}

func (*explainDistSQLNode) Close(context.Context) {}

var explainDistSQLColumns = sqlbase.ResultColumns{
	{Name: "Automatic", Typ: parser.TypeBool},
	{Name: "URL", Typ: parser.TypeString},
	{Name: "JSON", Typ: parser.TypeString, Hidden: true},
}

func (n *explainDistSQLNode) Start(params runParams) error {
	// Trigger limit propagation.
	setUnlimited(n.plan)

	auto, err := n.distSQLPlanner.CheckSupport(n.plan)
	if err != nil {
		return err
	}

	planCtx := n.distSQLPlanner.NewPlanningCtx(params.ctx, n.txn)
	plan, err := n.distSQLPlanner.createPlanForNode(&planCtx, n.plan)
	if err != nil {
		return err
	}
	n.distSQLPlanner.FinalizePlan(&planCtx, &plan)
	flows := plan.GenerateFlowSpecs()
	planJSON, planURL, err := distsqlrun.GeneratePlanDiagramWithURL(flows)
	if err != nil {
		return err
	}

	n.values = parser.Datums{
		parser.MakeDBool(parser.DBool(auto)),
		parser.NewDString(planURL.String()),
		parser.NewDString(planJSON),
	}
	return nil
}

func (n *explainDistSQLNode) Next(runParams) (bool, error) {
	if n.done {
		return false, nil
	}
	n.done = true
	return true, nil
}

func (n *explainDistSQLNode) Values() parser.Datums {
	return n.values
}
