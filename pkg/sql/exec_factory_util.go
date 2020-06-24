// Copyright 2020 The Cockroach Authors.
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
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

func constructPlan(
	planner *planner,
	root exec.Node,
	subqueries []exec.Subquery,
	cascades []exec.Cascade,
	checks []exec.Node,
) (exec.Plan, error) {
	res := &planTop{
		// TODO(radu): these fields can be modified by planning various opaque
		// statements. We should have a cleaner way of plumbing these.
		avoidBuffering:  planner.curPlan.avoidBuffering,
		auditEvents:     planner.curPlan.auditEvents,
		instrumentation: planner.curPlan.instrumentation,
	}
	assignPlan := func(plan *planMaybePhysical, node exec.Node) {
		switch n := node.(type) {
		case planNode:
			plan.planNode = n
		case planMaybePhysical:
			*plan = n
		default:
			panic(fmt.Sprintf("unexpected node type %T", node))
		}
	}
	assignPlan(&res.main, root)
	if len(subqueries) > 0 {
		res.subqueryPlans = make([]subquery, len(subqueries))
		for i := range subqueries {
			in := &subqueries[i]
			out := &res.subqueryPlans[i]
			out.subquery = in.ExprNode
			switch in.Mode {
			case exec.SubqueryExists:
				out.execMode = rowexec.SubqueryExecModeExists
			case exec.SubqueryOneRow:
				out.execMode = rowexec.SubqueryExecModeOneRow
			case exec.SubqueryAnyRows:
				out.execMode = rowexec.SubqueryExecModeAllRowsNormalized
			case exec.SubqueryAllRows:
				out.execMode = rowexec.SubqueryExecModeAllRows
			default:
				return nil, errors.Errorf("invalid SubqueryMode %d", in.Mode)
			}
			out.expanded = true
			assignPlan(&out.plan, in.Root)
		}
	}
	if len(cascades) > 0 {
		res.cascades = make([]cascadeMetadata, len(cascades))
		for i := range cascades {
			res.cascades[i].Cascade = cascades[i]
		}
	}
	if len(checks) > 0 {
		res.checkPlans = make([]checkPlan, len(checks))
		for i := range checks {
			assignPlan(&res.checkPlans[i].plan, checks[i])
		}
	}

	return res, nil
}

// makeScanColumnsConfig builds a scanColumnsConfig struct by constructing a
// list of descriptor IDs for columns in the given cols set. Columns are
// identified by their ordinal position in the table schema.
func makeScanColumnsConfig(table cat.Table, cols exec.TableColumnOrdinalSet) scanColumnsConfig {
	// Set visibility=execinfra.ScanVisibilityPublicAndNotPublic, since all
	// columns in the "cols" set should be projected, regardless of whether
	// they're public or non-public. The caller decides which columns to
	// include (or not include). Note that when wantedColumns is non-empty,
	// the visibility flag will never trigger the addition of more columns.
	colCfg := scanColumnsConfig{
		wantedColumns: make([]tree.ColumnID, 0, cols.Len()),
		visibility:    execinfra.ScanVisibilityPublicAndNotPublic,
	}
	for c, ok := cols.Next(0); ok; c, ok = cols.Next(c + 1) {
		desc := table.Column(c).(*sqlbase.ColumnDescriptor)
		colCfg.wantedColumns = append(colCfg.wantedColumns, tree.ColumnID(desc.ID))
	}
	return colCfg
}

func constructExplainPlanNode(
	options *tree.ExplainOptions, stmtType tree.StatementType, p *planTop, planner *planner,
) (exec.Node, error) {
	analyzeSet := options.Flags[tree.ExplainFlagAnalyze]

	if options.Flags[tree.ExplainFlagEnv] {
		return nil, errors.New("ENV only supported with (OPT) option")
	}

	switch options.Mode {
	case tree.ExplainDistSQL:
		return &explainDistSQLNode{
			options:  options,
			plan:     p.planComponents,
			analyze:  analyzeSet,
			stmtType: stmtType,
		}, nil

	case tree.ExplainVec:
		return &explainVecNode{
			options:       options,
			plan:          p.main,
			subqueryPlans: p.subqueryPlans,
			stmtType:      stmtType,
		}, nil

	case tree.ExplainPlan:
		if analyzeSet {
			return nil, errors.New("EXPLAIN ANALYZE only supported with (DISTSQL) option")
		}
		return planner.makeExplainPlanNodeWithPlan(
			context.TODO(),
			options,
			&p.planComponents,
			stmtType,
		)

	default:
		panic(fmt.Sprintf("unsupported explain mode %v", options.Mode))
	}
}

// getResultColumnsForSimpleProject populates result columns for a simple
// projection. It supports two configurations:
// 1. colNames and resultTypes are non-nil. resultTypes indicates the updated
//    types (after the projection has been applied)
// 2. if colNames is nil, then inputCols must be non-nil (which are the result
//    columns before the projection has been applied).
func getResultColumnsForSimpleProject(
	cols []exec.NodeColumnOrdinal,
	colNames []string,
	resultTypes []*types.T,
	inputCols sqlbase.ResultColumns,
) sqlbase.ResultColumns {
	resultCols := make(sqlbase.ResultColumns, len(cols))
	for i, col := range cols {
		if colNames == nil {
			resultCols[i] = inputCols[col]
			// If we have a SimpleProject, we should clear the hidden bit on any
			// column since it indicates it's been explicitly selected.
			resultCols[i].Hidden = false
		} else {
			resultCols[i] = sqlbase.ResultColumn{
				Name: colNames[i],
				Typ:  resultTypes[i],
			}
		}
	}
	return resultCols
}

func getEqualityIndicesAndMergeJoinOrdering(
	leftOrdering, rightOrdering sqlbase.ColumnOrdering,
) (
	leftEqualityIndices, rightEqualityIndices []exec.NodeColumnOrdinal,
	mergeJoinOrdering sqlbase.ColumnOrdering,
	err error,
) {
	n := len(leftOrdering)
	if n == 0 || len(rightOrdering) != n {
		return nil, nil, nil, errors.Errorf(
			"orderings from the left and right side must be the same non-zero length",
		)
	}
	leftEqualityIndices = make([]exec.NodeColumnOrdinal, n)
	rightEqualityIndices = make([]exec.NodeColumnOrdinal, n)
	for i := 0; i < n; i++ {
		leftColIdx, rightColIdx := leftOrdering[i].ColIdx, rightOrdering[i].ColIdx
		leftEqualityIndices[i] = exec.NodeColumnOrdinal(leftColIdx)
		rightEqualityIndices[i] = exec.NodeColumnOrdinal(rightColIdx)
	}

	mergeJoinOrdering = make(sqlbase.ColumnOrdering, n)
	for i := 0; i < n; i++ {
		// The mergeJoinOrdering "columns" are equality column indices.  Because of
		// the way we constructed the equality indices, the ordering will always be
		// 0,1,2,3..
		mergeJoinOrdering[i].ColIdx = i
		mergeJoinOrdering[i].Direction = leftOrdering[i].Direction
	}
	return leftEqualityIndices, rightEqualityIndices, mergeJoinOrdering, nil
}

func getResultColumnsForGroupBy(
	inputCols sqlbase.ResultColumns, groupCols []exec.NodeColumnOrdinal, aggregations []exec.AggInfo,
) sqlbase.ResultColumns {
	columns := make(sqlbase.ResultColumns, 0, len(groupCols)+len(aggregations))
	for _, col := range groupCols {
		columns = append(columns, inputCols[col])
	}
	for _, agg := range aggregations {
		columns = append(columns, sqlbase.ResultColumn{
			Name: agg.FuncName,
			Typ:  agg.ResultType,
		})
	}
	return columns
}

// convertOrdinalsToInts converts a slice of exec.NodeColumnOrdinals to a slice
// of ints.
func convertOrdinalsToInts(ordinals []exec.NodeColumnOrdinal) []int {
	ints := make([]int, len(ordinals))
	for i := range ordinals {
		ints[i] = int(ordinals[i])
	}
	return ints
}

func constructSimpleProjectForPlanNode(
	n planNode, cols []exec.NodeColumnOrdinal, colNames []string, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	// If the top node is already a renderNode, just rearrange the columns. But
	// we don't want to duplicate a rendering expression (in case it is expensive
	// to compute or has side-effects); so if we have duplicates we avoid this
	// optimization (and add a new renderNode).
	if r, ok := n.(*renderNode); ok && !hasDuplicates(cols) {
		oldCols, oldRenders := r.columns, r.render
		r.columns = make(sqlbase.ResultColumns, len(cols))
		r.render = make([]tree.TypedExpr, len(cols))
		for i, ord := range cols {
			r.columns[i] = oldCols[ord]
			if colNames != nil {
				r.columns[i].Name = colNames[i]
			}
			r.render[i] = oldRenders[ord]
		}
		r.reqOrdering = ReqOrdering(reqOrdering)
		return r, nil
	}
	var inputCols sqlbase.ResultColumns
	if colNames == nil {
		// We will need the names of the input columns.
		inputCols = planColumns(n.(planNode))
	}

	var rb renderBuilder
	rb.init(n, reqOrdering)

	exprs := make(tree.TypedExprs, len(cols))
	for i, col := range cols {
		exprs[i] = rb.r.ivarHelper.IndexedVar(int(col))
	}
	var resultTypes []*types.T
	if colNames != nil {
		// We will need updated result types.
		resultTypes = make([]*types.T, len(cols))
		for i := range exprs {
			resultTypes[i] = exprs[i].ResolvedType()
		}
	}
	resultCols := getResultColumnsForSimpleProject(cols, colNames, resultTypes, inputCols)
	rb.setOutput(exprs, resultCols)
	return rb.res, nil
}

func hasDuplicates(cols []exec.NodeColumnOrdinal) bool {
	var set util.FastIntSet
	for _, c := range cols {
		if set.Contains(int(c)) {
			return true
		}
		set.Add(int(c))
	}
	return false
}
