// Copyright 2016 The Cockroach Authors.
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

package sql

import (
	"bytes"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

// explainer represents the run-time state of the EXPLAIN logic.
type explainer struct {
	// showMetadata indicates whether the output has separate columns for the
	// schema signature and ordering information of the intermediate
	// nodes.
	showMetadata bool

	// showExprs indicates whether the plan prints expressions
	// embedded inside the node.
	showExprs bool

	// qualifyNames determines whether column names in expressions
	// should be fully qualified during pretty-printing.
	qualifyNames bool

	// symbolicVars determines whether ordinal column references
	// should be printed numerically.
	symbolicVars bool

	// fmtFlags is the formatter to use for pretty-printing expressions.
	fmtFlags parser.FmtFlags

	// showTypes indicates whether to print the type of embedded
	// expressions and result columns.
	showTypes bool

	// showSelectTop indicates whether intermediate selectTopNodes
	// are rendered.
	showSelectTop bool

	// level is the current depth in the tree of planNodes.
	level int

	// doIndent indicates whether the output should be clarified
	// with leading white spaces.
	doIndent bool

	// makeRow produces one row of EXPLAIN output.
	makeRow func(level int, typ, field, desc string, plan planNode)

	// err remembers whether any error was encountered by makeRow.
	err error
}

// newExplainPlanNode instantiates a planNode that runs an EXPLAIN query.
func (p *planner) makeExplainPlanNode(explainer explainer, expanded bool, plan planNode) planNode {
	columns := ResultColumns{
		// Level is the depth of the node in the tree.
		{Name: "Level", Typ: parser.TypeInt},
		// Type is the node type.
		{Name: "Type", Typ: parser.TypeString},
		// Field is the part of the node that a row of output pertains to.
		// For example a select node may have separate "render" and
		// "filter" fields.
		{Name: "Field", Typ: parser.TypeString},
		// Description contains details about the field.
		{Name: "Description", Typ: parser.TypeString},
	}
	if explainer.showMetadata {
		// Columns is the type signature of the data source.
		columns = append(columns, ResultColumn{Name: "Columns", Typ: parser.TypeString})
		// Ordering indicates the known ordering of the data from this source.
		columns = append(columns, ResultColumn{Name: "Ordering", Typ: parser.TypeString})
	}

	explainer.fmtFlags = parser.FmtExpr(
		explainer.showTypes, explainer.symbolicVars, explainer.qualifyNames)

	node := &explainPlanNode{
		p:         p,
		explainer: explainer,
		expanded:  expanded,
		plan:      plan,
		results:   p.newContainerValuesNode(columns, 0),
	}
	return node
}

var emptyString = parser.NewDString("")

// populateExplain invokes explain() with a makeRow method
// which populates a valuesNode.
func (p *planner) populateExplain(e *explainer, v *valuesNode, plan planNode) error {
	e.makeRow = func(level int, name, field, description string, plan planNode) {
		if e.err != nil {
			return
		}

		row := parser.DTuple{
			parser.NewDInt(parser.DInt(level)),
			parser.NewDString(name),
			parser.NewDString(field),
			parser.NewDString(description),
		}
		if e.showMetadata {
			if plan != nil {
				row = append(row, parser.NewDString(formatColumns(plan.Columns(), e.showTypes)))
				row = append(row, parser.NewDString(plan.Ordering().AsString(plan.Columns())))
			} else {
				row = append(row, emptyString, emptyString)
			}
		}
		if _, err := v.rows.AddRow(row); err != nil {
			e.err = err
		}
	}

	e.err = nil
	visitPlan := planVisitor{p: p, observer: e}
	visitPlan.visit(plan)
	return e.err
}

// planToString uses explain() to build a string representation of the planNode.
func planToString(plan planNode) string {
	var buf bytes.Buffer
	e := explainer{
		showMetadata:  true,
		showTypes:     true,
		showExprs:     true,
		showSelectTop: true,
		makeRow: func(level int, name, field, description string, plan planNode) {
			if field != "" {
				field = "." + field
			}
			fmt.Fprintf(&buf, "%d %s%s %s %s %s\n", level, name, field, description,
				formatColumns(plan.Columns(), true),
				plan.Ordering().AsString(plan.Columns()),
			)
		},
	}
	v := planVisitor{p: makePlanner("planToString"), observer: &e}
	v.visit(plan)
	return buf.String()
}

// expr implements the planObserver interface.
func (e *explainer) expr(nodeName, fieldName string, n int, expr parser.Expr) {
	if e.showExprs && expr != nil {
		if nodeName == "join" {
			qualifySave := e.fmtFlags.ShowTableAliases
			e.fmtFlags.ShowTableAliases = true
			defer func() { e.fmtFlags.ShowTableAliases = qualifySave }()
		}
		if n >= 0 {
			fieldName = fmt.Sprintf("%s %d", fieldName, n)
		}
		e.attr(nodeName, fieldName,
			parser.AsStringWithFlags(expr, e.fmtFlags))
	}
}

// enterNode implements the planObserver interface.
func (e *explainer) enterNode(name string, plan planNode) bool {
	if !e.showSelectTop && name == "select" {
		return true
	}
	if !e.showExprs && name == "render/filter" {
		return true
	}

	desc := ""
	if e.doIndent {
		desc = fmt.Sprintf("%*s-> %s", e.level*3, " ", name)
	}
	e.makeRow(e.level, name, "", desc, plan)

	e.level++
	return true
}

// attr implements the planObserver interface.
func (e *explainer) attr(nodeName, fieldName, attr string) {
	if e.doIndent {
		attr = fmt.Sprintf("%*s%s", e.level*3, " ", attr)
	}
	e.makeRow(e.level-1, "", fieldName, attr, nil)
}

// leaveNode implements the planObserver interface.
func (e *explainer) leaveNode(name string) {
	if !e.showSelectTop && name == "select" {
		return
	}
	if !e.showExprs && name == "render/filter" {
		return
	}
	e.level--
}

// formatColumns converts a column signature for a data source /
// planNode to a string. The column types are printed iff the 2nd
// argument specifies so.
func formatColumns(cols ResultColumns, printTypes bool) string {
	var buf bytes.Buffer
	buf.WriteByte('(')
	for i, rCol := range cols {
		if i > 0 {
			buf.WriteString(", ")
		}
		parser.Name(rCol.Name).Format(&buf, parser.FmtSimple)
		// Output extra properties like [hidden,omitted].
		hasProps := false
		outputProp := func(prop string) {
			if hasProps {
				buf.WriteByte(',')
			} else {
				buf.WriteByte('[')
			}
			hasProps = true
			buf.WriteString(prop)
		}
		if rCol.hidden {
			outputProp("hidden")
		}
		if rCol.omitted {
			outputProp("omitted")
		}
		if hasProps {
			buf.WriteByte(']')
		}

		if printTypes {
			buf.WriteByte(' ')
			buf.WriteString(rCol.Typ.String())
		}
	}
	buf.WriteByte(')')
	return buf.String()
}

// explainPlanNode wraps the logic for EXPLAIN as a planNode.
type explainPlanNode struct {
	p         *planner
	explainer explainer
	expanded  bool
	plan      planNode
	results   *valuesNode
}

func (e *explainPlanNode) Next() (bool, error)          { return e.results.Next() }
func (e *explainPlanNode) Columns() ResultColumns       { return e.results.Columns() }
func (e *explainPlanNode) Ordering() orderingInfo       { return e.results.Ordering() }
func (e *explainPlanNode) Values() parser.DTuple        { return e.results.Values() }
func (e *explainPlanNode) DebugValues() debugValues     { return debugValues{} }
func (e *explainPlanNode) SetLimitHint(n int64, s bool) { e.results.SetLimitHint(n, s) }
func (e *explainPlanNode) setNeededColumns(_ []bool)    {}
func (e *explainPlanNode) MarkDebug(mode explainMode)   {}
func (e *explainPlanNode) expandPlan() error {
	if e.expanded {
		if err := e.plan.expandPlan(); err != nil {
			return err
		}
		// Trigger limit hint propagation, which would otherwise only occur
		// during the plan's Start() phase. This may trigger additional
		// optimizations (eg. in sortNode) which the user of EXPLAIN will be
		// interested in.
		e.plan.SetLimitHint(math.MaxInt64, true)
	}
	return nil
}

func (e *explainPlanNode) Start() error {
	return e.p.populateExplain(&e.explainer, e.results, e.plan)
}

func (e *explainPlanNode) Close() {
	e.plan.Close()
	e.results.Close()
}
