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

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
)

type explainMode int

const (
	explainNone explainMode = iota
	explainDebug
	explainPlan
	explainTrace
)

var explainStrings = []string{"", "debug", "plan", "trace", "types"}

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

	for _, opt := range n.Options {
		newMode := explainNone
		if strings.EqualFold(opt, "DEBUG") {
			newMode = explainDebug
		} else if strings.EqualFold(opt, "TRACE") {
			newMode = explainTrace
		} else if strings.EqualFold(opt, "PLAN") {
			newMode = explainPlan
		} else if strings.EqualFold(opt, "TYPES") {
			newMode = explainPlan
			explainer.showExprs = true
			explainer.showTypes = true
			// TYPES implies METADATA.
			explainer.showMetadata = true
		} else if strings.EqualFold(opt, "INDENT") {
			explainer.doIndent = true
		} else if strings.EqualFold(opt, "SYMVARS") {
			explainer.symbolicVars = true
		} else if strings.EqualFold(opt, "METADATA") {
			explainer.showMetadata = true
		} else if strings.EqualFold(opt, "QUALIFY") {
			explainer.qualifyNames = true
		} else if strings.EqualFold(opt, "VERBOSE") {
			// VERBOSE implies EXPRS.
			explainer.showExprs = true
			// VERBOSE implies QUALIFY.
			explainer.qualifyNames = true
			// VERBOSE implies METADATA.
			explainer.showMetadata = true
		} else if strings.EqualFold(opt, "EXPRS") {
			explainer.showExprs = true
		} else if strings.EqualFold(opt, "NOEXPAND") {
			expanded = false
		} else if strings.EqualFold(opt, "NONORMALIZE") {
			normalizeExprs = false
		} else if strings.EqualFold(opt, "NOOPTIMIZE") {
			optimized = false
		} else {
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

	if mode == explainTrace {
		sp, err := tracing.JoinOrNewSnowball("coordinator", nil, func(sp basictracer.RawSpan) {
			// Some things are done async wrt running the statement (e.g. the
			// TxnCoordSender heartbeat loop), and these things might finish spans
			// after the txn is completed. We ignore them.
			if p.txn != nil {
				p.txn.CollectedSpans = append(p.txn.CollectedSpans, sp)
			}
		})
		if err != nil {
			return nil, err
		}
		p.txn.Context = opentracing.ContextWithSpan(p.txn.Context, sp)
	}

	p.evalCtx.SkipNormalize = !normalizeExprs

	plan, err := p.newPlan(n.Statement, nil, autoCommit)
	if err != nil {
		return nil, err
	}
	switch mode {
	case explainDebug:
		return &explainDebugNode{plan}, nil

	case explainPlan:
		// We may want to show placeholder types, so ensure no values
		// are missing.
		p.semaCtx.Placeholders.FillUnassigned()
		return p.makeExplainPlanNode(explainer, expanded, optimized, plan), nil

	case explainTrace:
		return p.makeTraceNode(plan, p.txn), nil

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

func (vals *debugValues) AsRow() parser.DTuple {
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

	return parser.DTuple{
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

func (n *explainDebugNode) Values() parser.DTuple {
	vals := n.plan.DebugValues()

	keyVal := parser.DNull
	if vals.key != "" {
		keyVal = parser.NewDString(vals.key)
	}

	return parser.DTuple{
		parser.NewDInt(parser.DInt(vals.rowIdx)),
		keyVal,
		parser.NewDString(vals.value),
		parser.NewDString(vals.output.String()),
	}
}

func (*explainDebugNode) MarkDebug(_ explainMode)  {}
func (*explainDebugNode) DebugValues() debugValues { return debugValues{} }
