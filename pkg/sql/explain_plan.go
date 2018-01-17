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
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// explainPlanNode wraps the logic for EXPLAIN as a planNode.
type explainPlanNode struct {
	explainer explainer

	// plan is the sub-node being explained.
	plan planNode

	// subqueryPlans contains the subquery plans for the explained query.
	subqueryPlans []subquery

	// expanded indicates whether to invoke expandPlan() on the sub-node.
	expanded bool

	// optimized indicates whether to invoke setNeededColumns() on the sub-node.
	optimized bool

	run explainPlanRun
}

var explainPlanColumns = sqlbase.ResultColumns{
	// Tree shows the node type with the tree structure.
	{Name: "Tree", Typ: types.String},
	// Field is the part of the node that a row of output pertains to.
	{Name: "Field", Typ: types.String},
	// Description contains details about the field.
	{Name: "Description", Typ: types.String},
}

var explainPlanVerboseColumns = sqlbase.ResultColumns{
	// Tree shows the node type with the tree structure.
	{Name: "Tree", Typ: types.String},
	// Level is the depth of the node in the tree.
	{Name: "Level", Typ: types.Int},
	// Type is the node type.
	{Name: "Type", Typ: types.String},
	// Field is the part of the node that a row of output pertains to.
	{Name: "Field", Typ: types.String},
	// Description contains details about the field.
	{Name: "Description", Typ: types.String},
	// Columns is the type signature of the data source.
	{Name: "Columns", Typ: types.String},
	// Ordering indicates the known ordering of the data from this source.
	{Name: "Ordering", Typ: types.String},
}

// newExplainPlanNode instantiates a planNode that runs an EXPLAIN query.
func (p *planner) makeExplainPlanNode(
	ctx context.Context, flags explainFlags, expanded, optimized bool, origStmt tree.Statement,
) (planNode, error) {
	// Build the plan for the query being explained.  We want to capture
	// all the analyzed sub-queries in the explain node, so we are going
	// to override the planner's subquery plan slice.
	defer func(s []subquery) { p.curPlan.subqueryPlans = s }(p.curPlan.subqueryPlans)
	p.curPlan.subqueryPlans = nil

	plan, err := p.newPlan(ctx, origStmt, nil)
	if err != nil {
		return nil, err
	}
	return p.makeExplainPlanNodeWithPlan(ctx, flags, expanded, optimized, plan)
}

// newExplainPlanNodeWithPlan instantiates a planNode that EXPLAINs an
// underlying plan.
func (p *planner) makeExplainPlanNodeWithPlan(
	ctx context.Context, flags explainFlags, expanded, optimized bool, plan planNode,
) (planNode, error) {
	columns := explainPlanColumns

	if flags.showMetadata {
		columns = explainPlanVerboseColumns
	}

	e := explainer{explainFlags: flags}

	noPlaceholderFlags := tree.FmtExpr(
		tree.FmtSymbolicSubqueries, flags.showTypes, flags.symbolicVars, flags.qualifyNames,
	)
	e.fmtFlags = noPlaceholderFlags
	e.showPlaceholderValues = func(ctx *tree.FmtCtx, placeholder *tree.Placeholder) {
		d, err := placeholder.Eval(p.EvalContext())
		if err != nil {
			// Disable the placeholder formatter so that
			// we don't recurse infinitely trying to evaluate.
			//
			// We also avoid calling ctx.FormatNode because when
			// types are visible, this would cause the type information
			// to be printed twice.
			nCtx := *ctx
			placeholder.Format(nCtx.WithPlaceholderFormat(nil))
			return
		}
		ctx.FormatNode(d)
	}

	node := &explainPlanNode{
		explainer:     e,
		expanded:      expanded,
		optimized:     optimized,
		plan:          plan,
		subqueryPlans: p.curPlan.subqueryPlans,
		run: explainPlanRun{
			results: p.newContainerValuesNode(columns, 0),
		},
	}
	return node, nil
}

// explainPlanRun is the run-time state of explainPlanNode during local execution.
type explainPlanRun struct {
	// results is the container for EXPLAIN's output.
	results *valuesNode
}

func (e *explainPlanNode) startExec(params runParams) error {
	// The sub-plan's subqueries have been captured local to the EXPLAIN
	// node so that they would not be automatically started for
	// execution by planTop.start(). But this also means they were not
	// yet processed by makePlan()/optimizePlan(). Do it here.
	for i := range e.subqueryPlans {
		if err := params.p.optimizeSubquery(params.ctx, &e.subqueryPlans[i]); err != nil {
			return err
		}

		// Trigger limit propagation. This would be done otherwise when
		// starting the plan. However we do not want to start the plan.
		params.p.setUnlimited(e.subqueryPlans[i].plan)
	}

	return params.p.populateExplain(params.ctx, &e.explainer, e.run.results, e.plan, e.subqueryPlans)
}

func (e *explainPlanNode) Next(params runParams) (bool, error) { return e.run.results.Next(params) }
func (e *explainPlanNode) Values() tree.Datums                 { return e.run.results.Values() }

func (e *explainPlanNode) Close(ctx context.Context) {
	e.plan.Close(ctx)
	for i := range e.subqueryPlans {
		e.subqueryPlans[i].plan.Close(ctx)
	}
	e.run.results.Close(ctx)
}

// explainEntry is a representation of the info that makes it into an output row
// of an EXPLAIN statement.
type explainEntry struct {
	level                 int
	node, field, fieldVal string
	plan                  planNode
}

// explainFlags contains parameters for the EXPLAIN logic.
type explainFlags struct {
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

	// showTypes indicates whether to print the type of embedded
	// expressions and result columns.
	showTypes bool
}

// explainFlags represents the run-time state of the EXPLAIN logic.
type explainer struct {
	explainFlags

	// fmtFlags is the formatter to use for pretty-printing expressions.
	// This can change during the execution of EXPLAIN.
	fmtFlags tree.FmtFlags

	// showPlaceholderValues is a formatting overload function
	// that will try to evaluate the placeholders if possible.
	// Meant for use with FmtCtx.WithPlaceholderFormat().
	showPlaceholderValues func(ctx *tree.FmtCtx, placeholder *tree.Placeholder)

	// level is the current depth in the tree of planNodes.
	level int

	// explainEntry accumulates entries (nodes or attributes).
	entries []explainEntry
}

var emptyString = tree.NewDString("")

// populateExplain walks the plan and generates rows in a valuesNode.
// The subquery plans, if any are known to the planner, are printed
// at the bottom.
func (p *planner) populateExplain(
	ctx context.Context, e *explainer, v *valuesNode, plan planNode, subqueryPlans []subquery,
) error {
	e.populateEntries(ctx, plan, subqueryPlans)

	tp := treeprinter.New()
	// n keeps track of the current node on each level.
	n := []treeprinter.Node{tp}

	for _, entry := range e.entries {
		if entry.plan != nil {
			n = append(n[:entry.level+1], n[entry.level].Child(entry.node))
		} else {
			tp.AddEmptyLine()
		}
	}

	treeRows := tp.FormattedRows()

	for i, entry := range e.entries {
		var row tree.Datums
		if !e.showMetadata {
			row = tree.Datums{
				tree.NewDString(treeRows[i]),    // Tree
				tree.NewDString(entry.field),    // Field
				tree.NewDString(entry.fieldVal), // Description
			}
		} else {
			row = tree.Datums{
				tree.NewDString(treeRows[i]),         // Tree
				tree.NewDInt(tree.DInt(entry.level)), // Level
				tree.NewDString(entry.node),          // Type
				tree.NewDString(entry.field),         // Field
				tree.NewDString(entry.fieldVal),      // Description
				emptyString,                          // Columns
				emptyString,                          // Ordering
			}
			if entry.plan != nil {
				cols := planColumns(entry.plan)
				// Columns metadata.
				row[5] = tree.NewDString(formatColumns(cols, e.showTypes))
				// Ordering metadata.
				row[6] = tree.NewDString(planPhysicalProps(entry.plan).AsString(cols))
			}
		}
		if _, err := v.rows.AddRow(ctx, row); err != nil {
			return err
		}
	}

	return nil
}

func (e *explainer) populateEntries(ctx context.Context, plan planNode, subqueryPlans []subquery) {
	e.entries = nil
	observer := e.observer()

	// If there are any subqueries in the plan, we enclose both the main
	// plan and the sub-queries as children of a virtual "root"
	// node. This is not introduced in the common case where there are
	// no subqueries.
	if len(subqueryPlans) > 0 {
		_, _ = e.enterNode(ctx, "root", plan)
	}

	// Explain the main plan.
	_ = walkPlan(ctx, plan, observer)

	// Explain the subqueries.
	for i := range subqueryPlans {
		_, _ = e.enterNode(ctx, "subquery", plan)
		e.attr("subquery", "id", fmt.Sprintf("@S%d", i+1))
		e.attr("subquery", "sql", subqueryPlans[i].subquery.String())
		e.attr("subquery", "exec mode", execModeNames[subqueryPlans[i].execMode])
		if subqueryPlans[i].plan != nil {
			_ = walkPlan(ctx, subqueryPlans[i].plan, observer)
		} else if subqueryPlans[i].started {
			e.expr("subquery", "result", -1, subqueryPlans[i].result)
		}
		_ = e.leaveNode("subquery", subqueryPlans[i].plan)
	}

	if len(subqueryPlans) > 0 {
		_ = e.leaveNode("root", plan)
	}
}

// planToString uses explain() to build a string representation of the planNode.
func planToString(ctx context.Context, plan planNode, subqueryPlans []subquery) string {
	e := explainer{
		explainFlags: explainFlags{
			showMetadata: true,
			showExprs:    true,
			showTypes:    true,
		},
		fmtFlags: tree.FmtExpr(tree.FmtSymbolicSubqueries, true, true, true),
	}
	e.populateEntries(ctx, plan, subqueryPlans)
	var buf bytes.Buffer
	for _, e := range e.entries {
		field := e.field
		if field != "" {
			field = "." + field
		}
		if plan == nil {
			fmt.Fprintf(&buf, "%d %s%s %s\n", e.level, e.node, field, e.fieldVal)
		} else {
			cols := planColumns(plan)
			fmt.Fprintf(
				&buf, "%d %s%s %s %s %s\n", e.level, e.node, field, e.fieldVal,
				formatColumns(cols, true),
				planPhysicalProps(plan).AsString(cols),
			)
		}
	}
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
			qualifySave := e.fmtFlags
			e.fmtFlags.SetFlags(tree.FmtShowTableAliases)
			defer func(e *explainer, f tree.FmtFlags) { e.fmtFlags = f }(e, qualifySave)
		}
		if n >= 0 {
			fieldName = fmt.Sprintf("%s %d", fieldName, n)
		}

		f := tree.NewFmtCtxWithBuf(e.fmtFlags)
		f.WithPlaceholderFormat(e.showPlaceholderValues)
		f.FormatNode(expr)
		e.attr(nodeName, fieldName, f.CloseAndGetString())
	}
}

// enterNode implements the planObserver interface.
func (e *explainer) enterNode(_ context.Context, name string, plan planNode) (bool, error) {
	e.entries = append(e.entries, explainEntry{
		level: e.level,
		node:  name,
		plan:  plan,
	})

	e.level++
	return true, nil
}

// attr implements the planObserver interface.
func (e *explainer) attr(nodeName, fieldName, attr string) {
	e.entries = append(e.entries, explainEntry{
		level:    e.level - 1,
		field:    fieldName,
		fieldVal: attr,
	})
}

// leaveNode implements the planObserver interface.
func (e *explainer) leaveNode(name string, _ planNode) error {
	e.level--
	return nil
}

// formatColumns converts a column signature for a data source /
// planNode to a string. The column types are printed iff the 2nd
// argument specifies so.
func formatColumns(cols sqlbase.ResultColumns, printTypes bool) string {
	f := tree.NewFmtCtxWithBuf(tree.FmtSimple)
	f.WriteByte('(')
	for i := range cols {
		rCol := &cols[i]
		if i > 0 {
			f.WriteString(", ")
		}
		f.FormatNameP(&rCol.Name)
		// Output extra properties like [hidden,omitted].
		hasProps := false
		outputProp := func(prop string) {
			if hasProps {
				f.WriteByte(',')
			} else {
				f.WriteByte('[')
			}
			hasProps = true
			f.WriteString(prop)
		}
		if rCol.Hidden {
			outputProp("hidden")
		}
		if rCol.Omitted {
			outputProp("omitted")
		}
		if hasProps {
			f.WriteByte(']')
		}

		if printTypes {
			f.WriteByte(' ')
			f.WriteString(rCol.Typ.String())
		}
	}
	f.WriteByte(')')
	return f.CloseAndGetString()
}
