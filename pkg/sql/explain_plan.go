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

// newExplainPlanNode instantiates a planNode that runs an EXPLAIN query.
func (p *planner) makeExplainPlanNode(verbose, expanded bool, plan planNode) planNode {
	columns := ResultColumns{
		{Name: "Level", Typ: parser.TypeInt},
		{Name: "Type", Typ: parser.TypeString},
		{Name: "Field", Typ: parser.TypeString},
		{Name: "Description", Typ: parser.TypeString},
	}
	if verbose {
		columns = append(columns, ResultColumn{Name: "Columns", Typ: parser.TypeString})
		columns = append(columns, ResultColumn{Name: "Ordering", Typ: parser.TypeString})
	}

	node := &explainPlanNode{
		explainer: explainer{
			verbose: verbose,
		},
		expanded: expanded,
		plan:     plan,
		results:  p.newContainerValuesNode(columns, 0),
	}
	return node
}

// explainer represents the run-time state of the EXPLAIN logic.
type explainer struct {
	// verbose indicates whether the output has separate columns for the
	// schema signature and ordering information of the intermediate
	// nodes.
	verbose bool

	// level is the current depth in the tree of planNodes.
	level int

	// makeRow produces one row of EXPLAIN output.
	makeRow func(level int, typ, field, desc string, plan planNode)

	// err remembers whether any error was encountered by makeRow.
	err error
}

// populateExplain invokes explain() with a makeRow method
// which populates a valuesNode.
func (e *explainer) populateExplain(v *valuesNode, plan planNode) error {
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
		if e.verbose {
			row = append(row, parser.NewDString(formatColumns(plan.Columns(), false)))
			row = append(row, parser.NewDString(plan.Ordering().AsString(plan.Columns())))
		}
		if _, err := v.rows.AddRow(row); err != nil {
			e.err = err
		}
	}

	e.err = nil
	e.explain(plan)
	return e.err
}

// planToString uses explain() to build a string representation of the planNode.
func planToString(plan planNode) string {
	var buf bytes.Buffer
	e := explainer{
		verbose: true,
		makeRow: func(level int, name, field, description string, plan planNode) {
			if field != "" {
				field = "." + field
			}
			fmt.Fprintf(&buf, "%d %s%s %s %s %s\n", level, name, field, description,
				formatColumns(plan.Columns(), false),
				plan.Ordering().AsString(plan.Columns()),
			)
		},
	}
	e.explain(plan)
	return buf.String()
}

// explain extract information from planNodes and produces the EXPLAIN
// output via the makeRow callback in the explainer.
func (e *explainer) explain(plan planNode) {
	if e.err != nil {
		return
	}

	name, description, children := plan.ExplainPlan(e.verbose)
	e.makeRow(e.level, name, "", description, plan)

	e.level++
	defer func() { e.level-- }()
	for _, child := range children {
		e.explain(child)
	}
}

// explainPlanNode implements the logic for EXPLAIN.
type explainPlanNode struct {
	explainer explainer
	expanded  bool
	plan      planNode
	results   *valuesNode
}

func (e *explainPlanNode) ExplainTypes(fn func(string, string)) {}
func (e *explainPlanNode) Next() (bool, error)                  { return e.results.Next() }
func (e *explainPlanNode) Columns() ResultColumns               { return e.results.Columns() }
func (e *explainPlanNode) Ordering() orderingInfo               { return e.results.Ordering() }
func (e *explainPlanNode) Values() parser.DTuple                { return e.results.Values() }
func (e *explainPlanNode) DebugValues() debugValues             { return debugValues{} }
func (e *explainPlanNode) SetLimitHint(n int64, s bool)         { e.results.SetLimitHint(n, s) }
func (e *explainPlanNode) setNeededColumns(_ []bool)            {}
func (e *explainPlanNode) MarkDebug(mode explainMode)           {}
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
func (e *explainPlanNode) ExplainPlan(v bool) (string, string, []planNode) {
	return "explain", "plan", []planNode{e.plan}
}

func (e *explainPlanNode) Start() error {
	return e.explainer.populateExplain(e.results, e.plan)
}

func (e *explainPlanNode) Close() {
	e.plan.Close()
	e.results.Close()
}
