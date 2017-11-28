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

package sql

import (
	"bytes"
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	fmtFlags tree.FmtFlags

	// showTypes indicates whether to print the type of embedded
	// expressions and result columns.
	showTypes bool

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
func (p *planner) makeExplainPlanNode(
	explainer explainer, expanded, optimized bool, origStmt tree.Statement, plan planNode,
) planNode {
	columns := sqlbase.ResultColumns{
		// Level is the depth of the node in the tree.
		{Name: "Level", Typ: types.Int},
		// Type is the node type.
		{Name: "Type", Typ: types.String},
		// Field is the part of the node that a row of output pertains to.
		{Name: "Field", Typ: types.String},
		// Description contains details about the field.
		{Name: "Description", Typ: types.String},
	}
	if explainer.showMetadata {
		// Columns is the type signature of the data source.
		columns = append(columns, sqlbase.ResultColumn{Name: "Columns", Typ: types.String})
		// Ordering indicates the known ordering of the data from this source.
		columns = append(columns, sqlbase.ResultColumn{Name: "Ordering", Typ: types.String})
	}

	noPlaceholderFlags := tree.FmtExpr(
		tree.FmtSimple, explainer.showTypes, explainer.symbolicVars, explainer.qualifyNames,
	)
	explainer.fmtFlags = tree.FmtPlaceholderFormat(noPlaceholderFlags,
		func(buf *bytes.Buffer, f tree.FmtFlags, placeholder *tree.Placeholder) {
			d, err := placeholder.Eval(&p.evalCtx)
			if err != nil {
				placeholder.Format(buf, noPlaceholderFlags)
				return
			}
			d.Format(buf, f)
		})

	node := &explainPlanNode{
		explainer: explainer,
		expanded:  expanded,
		optimized: optimized,
		plan:      plan,
		run: explainPlanRun{
			results: p.newContainerValuesNode(columns, 0),
		},
	}
	return node
}

var emptyString = tree.NewDString("")

// populateExplain invokes explain() with a makeRow method
// which populates a valuesNode.
func (p *planner) populateExplain(
	ctx context.Context, e *explainer, v *valuesNode, plan planNode,
) error {
	e.makeRow = func(level int, name, field, description string, plan planNode) {
		if e.err != nil {
			return
		}

		row := tree.Datums{
			tree.NewDInt(tree.DInt(level)),
			tree.NewDString(name),
			tree.NewDString(field),
			tree.NewDString(description),
		}
		if e.showMetadata {
			if plan != nil {
				cols := planColumns(plan)
				row = append(row, tree.NewDString(formatColumns(cols, e.showTypes)))
				row = append(row, tree.NewDString(planPhysicalProps(plan).AsString(cols)))
			} else {
				row = append(row, emptyString, emptyString)
			}
		}
		if _, err := v.rows.AddRow(ctx, row); err != nil {
			e.err = err
		}
	}

	e.err = nil
	_ = walkPlan(ctx, plan, e.observer())
	return e.err
}

// planToString uses explain() to build a string representation of the planNode.
func planToString(ctx context.Context, plan planNode) string {
	var buf bytes.Buffer
	e := explainer{
		showMetadata: true,
		showExprs:    true,
		showTypes:    true,
		fmtFlags:     tree.FmtExpr(tree.FmtSimple, true, true, true),
		makeRow: func(level int, name, field, description string, plan planNode) {
			if field != "" {
				field = "." + field
			}
			if plan == nil {
				fmt.Fprintf(&buf, "%d %s%s %s\n", level, name, field, description)
			} else {
				cols := planColumns(plan)
				fmt.Fprintf(&buf, "%d %s%s %s %s %s\n", level, name, field, description,
					formatColumns(cols, true),
					planPhysicalProps(plan).AsString(cols),
				)
			}
		},
	}
	_ = walkPlan(ctx, plan, e.observer())
	return buf.String()
}

func (e *explainer) observer() planObserver {
	return planObserver{
		enterNode: e.enterNode,
		expr:      e.expr,
		attr:      e.attr,
		leaveNode: e.leaveNode,
	}
}

// expr implements the planObserver interface.
func (e *explainer) expr(nodeName, fieldName string, n int, expr tree.Expr) {
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
			tree.AsStringWithFlags(expr, e.fmtFlags))
	}
}

// enterNode implements the planObserver interface.
func (e *explainer) enterNode(_ context.Context, name string, plan planNode) bool {
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
	e.level--
}

// formatColumns converts a column signature for a data source /
// planNode to a string. The column types are printed iff the 2nd
// argument specifies so.
func formatColumns(cols sqlbase.ResultColumns, printTypes bool) string {
	var buf bytes.Buffer
	buf.WriteByte('(')
	for i, rCol := range cols {
		if i > 0 {
			buf.WriteString(", ")
		}
		tree.FormatNode(&buf, tree.FmtSimple, tree.Name(rCol.Name))
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
		if rCol.Hidden {
			outputProp("hidden")
		}
		if rCol.Omitted {
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
	explainer explainer

	// plan is the sub-node being explained.
	plan planNode

	// expanded indicates whether to invoke expandPlan() on the sub-node.
	expanded bool

	// optimized indicates whether to invoke setNeededColumns() on the sub-node.
	optimized bool

	run explainPlanRun
}

// explainPlanRun is the run-time state of explainPlanNode during local execution.
type explainPlanRun struct {
	// results is the container for EXPLAIN's output.
	results *valuesNode
}

func (e *explainPlanNode) Next(params runParams) (bool, error) { return e.run.results.Next(params) }
func (e *explainPlanNode) Values() tree.Datums                 { return e.run.results.Values() }

func (e *explainPlanNode) Start(params runParams) error {
	// Note that we don't call start on e.plan. That's on purpose, Start() can
	// have side effects. And it's supposed to not be needed for the way in which
	// we're going to use e.plan.
	return params.p.populateExplain(params.ctx, &e.explainer, e.run.results, e.plan)
}

func (e *explainPlanNode) Close(ctx context.Context) {
	e.plan.Close(ctx)
	e.run.results.Close(ctx)
}
