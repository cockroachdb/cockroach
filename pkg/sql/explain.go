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
	"bytes"
	"fmt"
	"math"
	"strings"

	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

type explainMode int

const (
	explainNone explainMode = iota
	explainDebug
	explainPlan
	explainTrace
	explainTypes
)

var explainStrings = []string{"", "debug", "plan", "trace", "types"}

// Explain executes the explain statement, providing debugging and analysis
// info about the wrapped statement.
//
// Privileges: the same privileges as the statement being explained.
func (p *planner) Explain(n *parser.Explain, autoCommit bool) (planNode, error) {
	mode := explainNone
	verbose := false
	expanded := true
	normalizedExplainTypes := false
	for _, opt := range n.Options {
		newMode := explainNone
		if strings.EqualFold(opt, "DEBUG") {
			newMode = explainDebug
		} else if strings.EqualFold(opt, "TRACE") {
			newMode = explainTrace
		} else if strings.EqualFold(opt, "PLAN") {
			newMode = explainPlan
		} else if strings.EqualFold(opt, "TYPES") {
			newMode = explainTypes
		} else if strings.EqualFold(opt, "VERBOSE") {
			verbose = true
		} else if strings.EqualFold(opt, "NOEXPAND") {
			expanded = false
		} else if strings.EqualFold(opt, "NORMALIZE") {
			normalizedExplainTypes = true
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
			p.txn.CollectedSpans = append(p.txn.CollectedSpans, sp)
		})
		if err != nil {
			return nil, err
		}
		p.txn.Context = opentracing.ContextWithSpan(p.txn.Context, sp)
	}

	if mode == explainTypes {
		p.evalCtx.SkipNormalize = !normalizedExplainTypes
	}

	plan, err := p.newPlan(n.Statement, nil, autoCommit)
	if err != nil {
		return nil, err
	}
	switch mode {
	case explainDebug:
		return &explainDebugNode{plan}, nil

	case explainTypes:
		columns := ResultColumns{
			{Name: "Level", Typ: parser.TypeInt},
			{Name: "Type", Typ: parser.TypeString},
			{Name: "Element", Typ: parser.TypeString},
			{Name: "Description", Typ: parser.TypeString},
		}
		node := &explainTypesNode{
			ctx:      p.ctx(),
			plan:     plan,
			expanded: expanded,
			results:  p.newContainerValuesNode(columns, 0),
		}
		return node, nil

	case explainPlan:
		columns := ResultColumns{
			{Name: "Level", Typ: parser.TypeInt},
			{Name: "Type", Typ: parser.TypeString},
			{Name: "Description", Typ: parser.TypeString},
		}
		if verbose {
			columns = append(columns, ResultColumn{Name: "Columns", Typ: parser.TypeString})
			columns = append(columns, ResultColumn{Name: "Ordering", Typ: parser.TypeString})
		}

		node := &explainPlanNode{
			ctx:     p.ctx(),
			verbose: verbose,
			plan:    plan,
			results: p.newContainerValuesNode(columns, 0),
		}
		return node, nil

	case explainTrace:
		return p.makeTraceNode(plan, p.txn), nil

	default:
		return nil, fmt.Errorf("unsupported EXPLAIN mode: %d", mode)
	}
}

type explainTypesNode struct {
	ctx      context.Context
	plan     planNode
	expanded bool
	results  *valuesNode
}

func (e *explainTypesNode) ExplainTypes(fn func(string, string)) {}
func (e *explainTypesNode) Next() (bool, error)                  { return e.results.Next() }
func (e *explainTypesNode) Columns() ResultColumns               { return e.results.Columns() }
func (e *explainTypesNode) Ordering() orderingInfo               { return e.results.Ordering() }
func (e *explainTypesNode) Values() parser.DTuple                { return e.results.Values() }
func (e *explainTypesNode) DebugValues() debugValues             { return e.results.DebugValues() }
func (e *explainTypesNode) SetLimitHint(n int64, s bool)         { e.results.SetLimitHint(n, s) }
func (e *explainTypesNode) MarkDebug(mode explainMode)           {}
func (e *explainTypesNode) ExplainPlan(v bool) (string, string, []planNode) {
	return "explain", "types", []planNode{e.plan}
}

func (e *explainTypesNode) expandPlan() error {
	if e.expanded {
		if err := e.plan.expandPlan(); err != nil {
			return err
		}
		// Trigger limit hint propagation, which would otherwise only
		// occur during the plan's Start() phase. This may trigger
		// additional optimizations (eg. in sortNode) which the user of
		// EXPLAIN will be interested in.
		e.plan.SetLimitHint(math.MaxInt64, true)
	}
	return nil
}

func (e *explainTypesNode) Start() error {
	return populateTypes(e.ctx, e.results, e.plan, 0)
}

func (e *explainTypesNode) Close() {
	e.plan.Close()
	e.results.Close()
}

func formatColumns(cols ResultColumns, printTypes bool) string {
	var buf bytes.Buffer
	buf.WriteByte('(')
	for i, rCol := range cols {
		if i > 0 {
			buf.WriteString(", ")
		}
		parser.Name(rCol.Name).Format(&buf, parser.FmtSimple)
		if rCol.hidden {
			buf.WriteString("[hidden]")
		}
		if printTypes {
			buf.WriteByte(' ')
			buf.WriteString(rCol.Typ.String())
		}
	}
	buf.WriteByte(')')
	return buf.String()
}

func populateTypes(ctx context.Context, v *valuesNode, plan planNode, level int) error {
	name, _, children := plan.ExplainPlan(true)

	// Format the result column types.
	row := parser.DTuple{
		parser.NewDInt(parser.DInt(level)),
		parser.NewDString(name),
		parser.NewDString("result"),
		parser.NewDString(formatColumns(plan.Columns(), true)),
	}
	if err := v.rows.AddRow(ctx, row); err != nil {
		return err
	}

	// Format the node's typing details.
	var err error
	regType := func(elt string, desc string) {
		if err != nil {
			return
		}

		row := parser.DTuple{
			parser.NewDInt(parser.DInt(level)),
			parser.NewDString(name),
			parser.NewDString(elt),
			parser.NewDString(desc),
		}
		err = v.rows.AddRow(ctx, row)
	}
	plan.ExplainTypes(regType)

	if err != nil {
		return err
	}

	// Recurse into sub-nodes.
	for _, child := range children {
		if err := populateTypes(ctx, v, child, level+1); err != nil {
			return err
		}
	}

	return nil
}

type explainPlanNode struct {
	verbose bool
	ctx     context.Context
	plan    planNode
	results *valuesNode
}

func (e *explainPlanNode) ExplainTypes(fn func(string, string)) {}
func (e *explainPlanNode) Next() (bool, error)                  { return e.results.Next() }
func (e *explainPlanNode) Columns() ResultColumns               { return e.results.Columns() }
func (e *explainPlanNode) Ordering() orderingInfo               { return e.results.Ordering() }
func (e *explainPlanNode) Values() parser.DTuple                { return e.results.Values() }
func (e *explainPlanNode) DebugValues() debugValues             { return debugValues{} }
func (e *explainPlanNode) SetLimitHint(n int64, s bool)         { e.results.SetLimitHint(n, s) }
func (e *explainPlanNode) MarkDebug(mode explainMode)           {}
func (e *explainPlanNode) expandPlan() error {
	if err := e.plan.expandPlan(); err != nil {
		return err
	}
	// Trigger limit hint propagation, which would otherwise only occur
	// during the plan's Start() phase. This may trigger additional
	// optimizations (eg. in sortNode) which the user of EXPLAIN will be
	// interested in.
	e.plan.SetLimitHint(math.MaxInt64, true)
	return nil
}
func (e *explainPlanNode) ExplainPlan(v bool) (string, string, []planNode) {
	return "explain", "plan", []planNode{e.plan}
}

func (e *explainPlanNode) Start() error {
	return populateExplain(e.ctx, e.verbose, e.results, e.plan, 0)
}

func (e *explainPlanNode) Close() {
	e.plan.Close()
	e.results.Close()
}

func populateExplain(
	ctx context.Context, verbose bool, v *valuesNode, plan planNode, level int,
) error {
	name, description, children := plan.ExplainPlan(verbose)

	row := parser.DTuple{
		parser.NewDInt(parser.DInt(level)),
		parser.NewDString(name),
		parser.NewDString(description),
	}
	if verbose {
		row = append(row, parser.NewDString(formatColumns(plan.Columns(), false)))
		row = append(row, parser.NewDString(plan.Ordering().AsString(plan.Columns())))
	}
	if err := v.rows.AddRow(ctx, row); err != nil {
		return err
	}

	for _, child := range children {
		if err := populateExplain(ctx, verbose, v, child, level+1); err != nil {
			return err
		}
	}
	return nil
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

func (n *explainDebugNode) expandPlan() error {
	if err := n.plan.expandPlan(); err != nil {
		return err
	}
	n.plan.MarkDebug(explainDebug)
	return nil
}

func (n *explainDebugNode) Start() error        { return n.plan.Start() }
func (n *explainDebugNode) Next() (bool, error) { return n.plan.Next() }
func (n *explainDebugNode) Close()              { n.plan.Close() }

func (n *explainDebugNode) ExplainPlan(v bool) (name, description string, children []planNode) {
	return n.plan.ExplainPlan(v)
}

func (n *explainDebugNode) ExplainTypes(fn func(string, string)) {}

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

func (*explainDebugNode) MarkDebug(_ explainMode)      {}
func (*explainDebugNode) DebugValues() debugValues     { return debugValues{} }
func (*explainDebugNode) SetLimitHint(_ int64, _ bool) {}
