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
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

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
	explainer explainer, expanded, optimized bool, origStmt tree.Statement, plan planNode,
) planNode {
	columns := explainPlanColumns

	if explainer.showMetadata {
		columns = explainPlanVerboseColumns
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

// explainPlanRun is the run-time state of explainPlanNode during local execution.
type explainPlanRun struct {
	// results is the container for EXPLAIN's output.
	results *valuesNode
}

func (e *explainPlanNode) startExec(params runParams) error {
	return params.p.populateExplain(params.ctx, &e.explainer, e.run.results, e.plan)
}

func (e *explainPlanNode) Next(params runParams) (bool, error) { return e.run.results.Next(params) }
func (e *explainPlanNode) Values() tree.Datums                 { return e.run.results.Values() }

func (e *explainPlanNode) Close(ctx context.Context) {
	e.plan.Close(ctx)
	e.run.results.Close(ctx)
}

// explainEntry is a representation of the info that makes it into an output row
// of an EXPLAIN statement.
type explainEntry struct {
	level                 int
	node, field, fieldVal string
	plan                  planNode
}

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

	// explainEntry accumulates entries (nodes or attributes).
	entries []explainEntry
}

var emptyString = tree.NewDString("")

// populateExplain walks the plan and generates rows in a valuesNode.
func (p *planner) populateExplain(
	ctx context.Context, e *explainer, v *valuesNode, plan planNode,
) error {
	e.entries = nil
	_ = walkPlan(ctx, plan, e.observer())

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
				cols := planColumns(plan)
				// Columns metadata.
				row[5] = tree.NewDString(formatColumns(cols, e.showTypes))
				// Ordering metadata.
				row[6] = tree.NewDString(planPhysicalProps(plan).AsString(cols))
			}
		}
		if _, err := v.rows.AddRow(ctx, row); err != nil {
			return err
		}
	}

	return nil
}

// planToString uses explain() to build a string representation of the planNode.
func planToString(ctx context.Context, plan planNode) string {
	e := explainer{
		showMetadata: true,
		showExprs:    true,
		showTypes:    true,
		fmtFlags:     tree.FmtExpr(tree.FmtSimple, true, true, true),
	}
	_ = walkPlan(ctx, plan, e.observer())
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
