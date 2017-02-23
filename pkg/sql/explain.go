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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"fmt"
	"strings"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

type explainMode int

const (
	explainNone explainMode = iota
	explainDebug
	explainPlan
	explainTrace
	explainDistSQL
)

var explainStrings = []string{"", "debug", "plan", "trace", "types", "dist_sql"}

// Explain executes the explain statement, providing debugging and analysis
// info about the wrapped statement.
//
// Privileges: the same privileges as the statement being explained.
func (p *planner) Explain(n *parser.Explain, autoCommit bool) (planNode, error) {
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
	omitDistSQLPlan := false

	for _, opt := range n.Options {
		newMode := explainNone
		switch {
		case strings.EqualFold(opt, "DEBUG"):
			newMode = explainDebug
		case strings.EqualFold(opt, "TRACE"):
			newMode = explainTrace
		case strings.EqualFold(opt, "PLAN"):
			newMode = explainPlan
		case strings.EqualFold(opt, "TYPES"):
			newMode = explainPlan
			explainer.showExprs = true
			explainer.showTypes = true
			// TYPES implies METADATA.
			explainer.showMetadata = true
		case strings.EqualFold(opt, "INDENT"):
			explainer.doIndent = true
		case strings.EqualFold(opt, "SYMVARS"):
			explainer.symbolicVars = true
		case strings.EqualFold(opt, "METADATA"):
			explainer.showMetadata = true
		case strings.EqualFold(opt, "QUALIFY"):
			explainer.qualifyNames = true
		case strings.EqualFold(opt, "VERBOSE"):
			// VERBOSE implies EXPRS.
			explainer.showExprs = true
			// VERBOSE implies QUALIFY.
			explainer.qualifyNames = true
			// VERBOSE implies METADATA.
			explainer.showMetadata = true
		case strings.EqualFold(opt, "EXPRS"):
			explainer.showExprs = true
		case strings.EqualFold(opt, "NOEXPAND"):
			expanded = false
		case strings.EqualFold(opt, "NONORMALIZE"):
			normalizeExprs = false
		case strings.EqualFold(opt, "NOOPTIMIZE"):
			optimized = false
		case strings.EqualFold(opt, "DIST_SQL"):
			mode = explainDistSQL
		case strings.EqualFold(opt, "NOPLAN"):
			omitDistSQLPlan = true
		default:
			return nil, fmt.Errorf("unsupported EXPLAIN option: %s", opt)
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

	plan, err := p.newPlan(n.Statement, nil, autoCommit)
	if err != nil {
		return nil, err
	}
	switch mode {
	case explainDebug:
		return &explainDebugNode{plan}, nil

	case explainDistSQL:
		return &explainDistSQLNode{
			plan:           plan,
			distSQLPlanner: p.distSQLPlanner,
			txn:            p.txn,
			omitPlan:       omitDistSQLPlan,
		}, nil

	case explainPlan:
		// We may want to show placeholder types, so ensure no values
		// are missing.
		p.semaCtx.Placeholders.FillUnassigned()
		return p.makeExplainPlanNode(explainer, expanded, optimized, plan), nil

	case explainTrace:
		return p.makeTraceNode(plan), nil

	default:
		return nil, fmt.Errorf("unsupported EXPLAIN mode: %d", mode)
	}
}

type debugValueType int

const (
	// The debug values do not refer to a full result row.
	debugValuePartial debugValueType = iota

	// The debug values refer to a full result row but the row was filtered out.
	debugValueFiltered

	// The debug value refers to a full result row that has been stored in a buffer
	// and will be emitted later.
	debugValueBuffered

	// The debug values refer to a full result row.
	debugValueRow
)

func (t debugValueType) String() string {
	switch t {
	case debugValuePartial:
		return "PARTIAL"

	case debugValueFiltered:
		return "FILTERED"

	case debugValueBuffered:
		return "BUFFERED"

	case debugValueRow:
		return "ROW"

	default:
		panic(fmt.Sprintf("invalid debugValueType %d", t))
	}
}

// debugValues is a set of values used to implement EXPLAIN (DEBUG).
type debugValues struct {
	rowIdx int
	key    string
	value  string
	output debugValueType
}

func (vals *debugValues) AsRow() parser.Datums {
	keyVal := parser.DNull
	if vals.key != "" {
		keyVal = parser.NewDString(vals.key)
	}

	// The "output" value is NULL for partial rows, or a DBool indicating if the row passed the
	// filtering.
	outputVal := parser.DNull

	switch vals.output {
	case debugValueFiltered:
		outputVal = parser.MakeDBool(false)

	case debugValueRow:
		outputVal = parser.MakeDBool(true)
	}

	return parser.Datums{
		parser.NewDInt(parser.DInt(vals.rowIdx)),
		keyVal,
		parser.NewDString(vals.value),
		outputVal,
	}
}

// explainDebugNode is a planNode that wraps another node and converts DebugValues() results to a
// row of Values(). It is used as the top-level node for EXPLAIN (DEBUG) statements.
type explainDebugNode struct {
	plan planNode
}

// Columns for explainDebug mode.
var debugColumns = ResultColumns{
	{Name: "RowIdx", Typ: parser.TypeInt},
	{Name: "Key", Typ: parser.TypeString},
	{Name: "Value", Typ: parser.TypeString},
	{Name: "Disposition", Typ: parser.TypeString},
}

func (*explainDebugNode) Columns() ResultColumns { return debugColumns }
func (*explainDebugNode) Ordering() orderingInfo { return orderingInfo{} }
func (n *explainDebugNode) Start() error         { return n.plan.Start() }
func (n *explainDebugNode) Next() (bool, error)  { return n.plan.Next() }
func (n *explainDebugNode) Close()               { n.plan.Close() }

func (n *explainDebugNode) Values() parser.Datums {
	vals := n.plan.DebugValues()

	keyVal := parser.DNull
	if vals.key != "" {
		keyVal = parser.NewDString(vals.key)
	}

	return parser.Datums{
		parser.NewDInt(parser.DInt(vals.rowIdx)),
		keyVal,
		parser.NewDString(vals.value),
		parser.NewDString(vals.output.String()),
	}
}

func (*explainDebugNode) MarkDebug(_ explainMode)  {}
func (*explainDebugNode) DebugValues() debugValues { return debugValues{} }

// explainDistSQLNode is a planNode that wraps a plan and returns
// information related to running that plan under DistSQL.
type explainDistSQLNode struct {
	plan           planNode
	distSQLPlanner *distSQLPlanner
	txn            *client.Txn
	omitPlan       bool

	rowIdx int
	rows   []parser.Datums
}

func (*explainDistSQLNode) Ordering() orderingInfo   { return orderingInfo{} }
func (*explainDistSQLNode) MarkDebug(_ explainMode)  {}
func (*explainDistSQLNode) DebugValues() debugValues { return debugValues{} }
func (n *explainDistSQLNode) Close()                 {}

func (*explainDistSQLNode) Columns() ResultColumns {
	return ResultColumns{
		{Name: "Field", Typ: parser.TypeString},
		{Name: "Value", Typ: parser.TypeString},
	}
}

func (n *explainDistSQLNode) Start() error {
	// Trigger limit propagation.
	setUnlimited(n.plan)

	auto, err := n.distSQLPlanner.CheckSupport(n.plan)
	if err != nil {
		return err
	}
	autoStr := "No"
	if auto {
		autoStr = "Yes"
	}
	n.rows = []parser.Datums{{
		parser.NewDString("Runs in auto mode"),
		parser.NewDString(autoStr),
	}}

	planCtx := n.distSQLPlanner.NewPlanningCtx(context.TODO(), n.txn)
	plan, err := n.distSQLPlanner.createPlanForNode(&planCtx, n.plan)
	if err != nil {
		return err
	}
	if n.omitPlan {
		return nil
	}
	n.distSQLPlanner.FinalizePlan(&planCtx, &plan)
	flows := plan.GenerateFlowSpecs(planCtx.nodeAddresses)
	planJSON, planURL, err := distsqlrun.GeneratePlanDiagramWithURL(flows)
	if err != nil {
		return err
	}
	n.rows = append(n.rows,
		parser.Datums{
			parser.NewDString("Plan JSON"),
			parser.NewDString(planJSON),
		},
		parser.Datums{
			parser.NewDString("Plan URL"),
			parser.NewDString(planURL.String()),
		},
	)
	return nil
}

func (n *explainDistSQLNode) Next() (bool, error) {
	if n.rowIdx >= len(n.rows) {
		return false, nil
	}
	n.rowIdx++
	return true, nil
}

func (n *explainDistSQLNode) Values() parser.Datums {
	return n.rows[n.rowIdx-1]
}
