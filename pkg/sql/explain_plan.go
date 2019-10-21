// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

const (
	// explainSubqueryFmtFlags is the format for subqueries within `EXPLAIN SQL` statements.
	// Since these are individually run, we don't need to scrub any data from subqueries.
	explainSubqueryFmtFlags = tree.FmtSimple

	// sampledLogicalPlanFmtFlags is the format for sampled logical plans. Because these exposed
	// in the Admin UI, sampled plans should be scrubbed of sensitive information.
	sampledLogicalPlanFmtFlags = tree.FmtHideConstants
)

// explainPlanNode wraps the logic for EXPLAIN as a planNode.
type explainPlanNode struct {
	explainer explainer

	// plan is the sub-node being explained.
	plan planNode

	// subqueryPlans contains the subquery plans for the explained query.
	subqueryPlans []subquery

	// postqueryPlans contains the postquery plans for the explained query.
	postqueryPlans []postquery

	stmtType tree.StatementType

	run explainPlanRun
}

// makeExplainPlanNodeWithPlan instantiates a planNode that EXPLAINs an
// underlying plan.
func (p *planner) makeExplainPlanNodeWithPlan(
	ctx context.Context,
	opts *tree.ExplainOptions,
	plan planNode,
	subqueryPlans []subquery,
	postqueryPlans []postquery,
	stmtType tree.StatementType,
) (planNode, error) {
	flags := explainFlags{
		symbolicVars: opts.Flags.Contains(tree.ExplainFlagSymVars),
	}
	if opts.Flags.Contains(tree.ExplainFlagVerbose) {
		flags.showMetadata = true
		flags.qualifyNames = true
	}
	if opts.Flags.Contains(tree.ExplainFlagTypes) {
		flags.showMetadata = true
		flags.showTypes = true
	}

	columns := sqlbase.ExplainPlanColumns
	if flags.showMetadata {
		columns = sqlbase.ExplainPlanVerboseColumns
	}
	// Make a copy (to allow changes through planMutableColumns).
	columns = append(sqlbase.ResultColumns(nil), columns...)

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
			ctx.WithPlaceholderFormat(nil, func() {
				placeholder.Format(ctx)
			})
			return
		}
		ctx.FormatNode(d)
	}

	node := &explainPlanNode{
		explainer:      e,
		plan:           plan,
		subqueryPlans:  subqueryPlans,
		postqueryPlans: postqueryPlans,
		stmtType:       stmtType,
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
	return params.p.populateExplain(params, &e.explainer, e.run.results, e.plan, e.subqueryPlans, e.postqueryPlans,
		e.stmtType)
}

func (e *explainPlanNode) Next(params runParams) (bool, error) { return e.run.results.Next(params) }
func (e *explainPlanNode) Values() tree.Datums                 { return e.run.results.Values() }

func (e *explainPlanNode) Close(ctx context.Context) {
	e.plan.Close(ctx)
	for i := range e.subqueryPlans {
		e.subqueryPlans[i].plan.Close(ctx)
	}
	for i := range e.postqueryPlans {
		e.postqueryPlans[i].plan.Close(ctx)
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
	params runParams,
	e *explainer,
	v *valuesNode,
	plan planNode,
	subqueryPlans []subquery,
	postqueryPlans []postquery,
	stmtType tree.StatementType,
) error {
	ctx := params.ctx
	e.populateEntries(ctx, plan, subqueryPlans, postqueryPlans, explainSubqueryFmtFlags)

	var isDistSQL, isVec bool
	distSQLPlanner := params.extendedEvalCtx.DistSQLPlanner
	isDistSQL, _ = willDistributePlan(distSQLPlanner, plan, params)
	outerSubqueries := params.p.curPlan.subqueryPlans
	planCtx := makeExplainVecPlanningCtx(distSQLPlanner, params, stmtType, subqueryPlans, isDistSQL)
	defer func() {
		planCtx.planner.curPlan.subqueryPlans = outerSubqueries
	}()
	physicalPlan, err := makePhysicalPlan(planCtx, distSQLPlanner, plan)
	if err == nil {
		// There might be an issue making the physical plan, but that should not
		// cause an error or panic, so swallow the error. See #40677 for example.
		distSQLPlanner.FinalizePlan(planCtx, &physicalPlan)
		flows := physicalPlan.GenerateFlowSpecs(params.extendedEvalCtx.NodeID)
		flowCtx := makeFlowCtx(planCtx, physicalPlan, params)

		ctxSessionData := flowCtx.EvalCtx.SessionData
		vectorizedThresholdMet := physicalPlan.MaxEstimatedRowCount >= ctxSessionData.VectorizeRowCountThreshold
		isVec = true
		if ctxSessionData.VectorizeMode == sessiondata.VectorizeOff {
			isVec = false
		} else if !vectorizedThresholdMet && ctxSessionData.VectorizeMode == sessiondata.VectorizeAuto {
			isVec = false
		} else {
			thisNodeID := distSQLPlanner.nodeDesc.NodeID
			for nodeID, flow := range flows {
				fuseOpt := flowinfra.FuseNormally
				if nodeID == thisNodeID && !isDistSQL {
					fuseOpt = flowinfra.FuseAggressively
				}
				_, err := colflow.SupportsVectorized(params.ctx, flowCtx, flow.Processors, fuseOpt)
				isVec = isVec && (err == nil)
			}
		}
	}

	if err := appendExecutionDetails(ctx, v, e.showMetadata, isDistSQL, isVec); err != nil {
		return err
	}

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
				row[6] = tree.NewDString(formatOrdering(planReqOrdering(entry.plan), cols))
			}
		}
		if _, err := v.rows.AddRow(ctx, row); err != nil {
			return err
		}
	}

	return nil
}

func appendExecutionDetails(
	ctx context.Context, v *valuesNode, showMetadata, isDistSQL, isVec bool,
) error {
	var distSQLRow, vecRow tree.Datums
	distSQLFieldName := tree.NewDString("distributed")
	distSQLValue := tree.NewDString(fmt.Sprintf("%t", isDistSQL))
	vecFieldName := tree.NewDString("vectorized")
	vecValue := tree.NewDString(fmt.Sprintf("%t", isVec))
	if !showMetadata {
		distSQLRow = tree.Datums{
			emptyString,      // Tree
			distSQLFieldName, // Field
			distSQLValue,     // Description
		}
		vecRow = tree.Datums{
			emptyString,  // Tree
			vecFieldName, // Field
			vecValue,     // Description
		}
	} else {
		distSQLRow = tree.Datums{
			emptyString,      // Tree
			tree.NewDInt(0),  // Level
			emptyString,      // Type
			distSQLFieldName, // Field
			distSQLValue,     // Description
			emptyString,      // Columns
			emptyString,      // Ordering
		}
		vecRow = tree.Datums{
			emptyString,     // Tree
			tree.NewDInt(0), // Level
			emptyString,     // Type
			vecFieldName,    // Field
			vecValue,        // Description
			emptyString,     // Columns
			emptyString,     // Ordering
		}
	}
	if _, err := v.rows.AddRow(ctx, distSQLRow); err != nil {
		return err
	}
	if _, err := v.rows.AddRow(ctx, vecRow); err != nil {
		return err
	}
	return nil
}

func (e *explainer) populateEntries(
	ctx context.Context,
	plan planNode,
	subqueryPlans []subquery,
	postqueryPlans []postquery,
	subqueryFmtFlags tree.FmtFlags,
) {
	e.entries = nil
	observer := e.observer()
	_ = populateEntriesForObserver(ctx, plan, subqueryPlans, postqueryPlans, observer, false /* returnError */, subqueryFmtFlags)
}

func populateEntriesForObserver(
	ctx context.Context,
	plan planNode,
	subqueryPlans []subquery,
	postqueryPlans []postquery,
	observer planObserver,
	returnError bool,
	subqueryFmtFlags tree.FmtFlags,
) error {
	// If there are any sub- or postqueries in the plan, we enclose both the main
	// plan and the sub- and postqueries as children of a virtual "root" node.
	// This is not introduced in the common case where there are no sub- and
	// postqueries.
	if len(subqueryPlans) > 0 || len(postqueryPlans) > 0 {
		if _, err := observer.enterNode(ctx, "root", plan); err != nil && returnError {
			return err
		}
	}

	// Explain the main plan.
	if err := walkPlan(ctx, plan, observer); err != nil && returnError {
		return err
	}

	// Explain the subqueries.
	for i := range subqueryPlans {
		if _, err := observer.enterNode(ctx, "subquery", plan); err != nil && returnError {
			return err
		}
		observer.attr("subquery", "id", fmt.Sprintf("@S%d", i+1))
		// This field contains the original subquery (which could have been modified
		// by optimizer transformations).
		observer.attr(
			"subquery",
			"original sql",
			tree.AsStringWithFlags(subqueryPlans[i].subquery, subqueryFmtFlags),
		)
		observer.attr("subquery", "exec mode", rowexec.SubqueryExecModeNames[subqueryPlans[i].execMode])
		if subqueryPlans[i].plan != nil {
			if err := walkPlan(ctx, subqueryPlans[i].plan, observer); err != nil && returnError {
				return err
			}
		} else if subqueryPlans[i].started {
			observer.expr(observeAlways, "subquery", "result", -1, subqueryPlans[i].result)
		}
		if err := observer.leaveNode("subquery", subqueryPlans[i].plan); err != nil && returnError {
			return err
		}
	}

	// Explain the postqueries.
	for i := range postqueryPlans {
		if _, err := observer.enterNode(ctx, "postquery", plan); err != nil && returnError {
			return err
		}
		if postqueryPlans[i].plan != nil {
			if err := walkPlan(ctx, postqueryPlans[i].plan, observer); err != nil && returnError {
				return err
			}
		}
		if err := observer.leaveNode("postquery", postqueryPlans[i].plan); err != nil && returnError {
			return err
		}
	}

	if len(subqueryPlans) > 0 || len(postqueryPlans) > 0 {
		if err := observer.leaveNode("root", plan); err != nil && returnError {
			return err
		}
	}

	return nil
}

// planToString uses explain() to build a string representation of the planNode.
func planToString(
	ctx context.Context, plan planNode, subqueryPlans []subquery, postqueryPlans []postquery,
) string {
	e := explainer{
		explainFlags: explainFlags{
			showMetadata: true,
			showTypes:    true,
		},
		fmtFlags: tree.FmtExpr(tree.FmtSymbolicSubqueries, true, true, true),
	}
	e.populateEntries(ctx, plan, subqueryPlans, postqueryPlans, explainSubqueryFmtFlags)
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
				formatOrdering(planReqOrdering(plan), cols),
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
		spans:     e.spans,
		leaveNode: e.leaveNode,
	}
}

// spans implements the planObserver interface.
func (e *explainer) spans(
	nodeName, fieldName string, index *sqlbase.IndexDescriptor, spans []roachpb.Span,
) {
	spanss := sqlbase.PrettySpans(index, spans, 2)
	if spanss != "" {
		if spanss == "-" {
			spanss = "ALL"
		}
		e.attr(nodeName, fieldName, spanss)
	}
}

// expr implements the planObserver interface.
func (e *explainer) expr(v observeVerbosity, nodeName, fieldName string, n int, expr tree.Expr) {
	if expr != nil {
		if !e.showMetadata && v == observeMetadata {
			return
		}
		if nodeName == "join" {
			qualifySave := e.fmtFlags
			e.fmtFlags.SetFlags(tree.FmtShowTableAliases)
			defer func(e *explainer, f tree.FmtFlags) { e.fmtFlags = f }(e, qualifySave)
		}
		if n >= 0 {
			fieldName = fmt.Sprintf("%s %d", fieldName, n)
		}

		f := tree.NewFmtCtx(e.fmtFlags)
		f.SetPlaceholderFormat(e.showPlaceholderValues)
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
	f := tree.NewFmtCtx(tree.FmtSimple)
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
