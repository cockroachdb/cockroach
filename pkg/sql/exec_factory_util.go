// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

func constructPlan(
	planner *planner,
	root exec.Node,
	subqueries []exec.Subquery,
	cascades, triggers []exec.PostQuery,
	checks []exec.Node,
	rootRowCount int64,
	flags exec.PlanFlags,
) (exec.Plan, error) {
	res := &planComponents{}
	assignPlan := func(plan *planMaybePhysical, node exec.Node) {
		switch n := node.(type) {
		case planNode:
			plan.planNode = n
		case planMaybePhysical:
			*plan = n
		default:
			panic(errors.AssertionFailedf("unexpected node type %T", node))
		}
	}
	assignPlan(&res.main, root)
	res.mainRowCount = rootRowCount
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
			case exec.SubqueryDiscardAllRows:
				out.execMode = rowexec.SubqueryExecModeDiscardAllRows
			default:
				return nil, errors.Errorf("invalid SubqueryMode %d", in.Mode)
			}
			out.expanded = true
			out.rowCount = in.RowCount
			assignPlan(&out.plan, in.Root)
		}
	}
	if len(cascades) > 0 {
		res.cascades = make([]postQueryMetadata, len(cascades))
		for i := range cascades {
			res.cascades[i].PostQuery = cascades[i]
		}
	}
	if len(checks) > 0 {
		res.checkPlans = make([]checkPlan, len(checks))
		for i := range checks {
			assignPlan(&res.checkPlans[i].plan, checks[i])
		}
	}
	if len(triggers) > 0 {
		res.triggers = make([]postQueryMetadata, len(triggers))
		for i := range triggers {
			res.triggers[i].PostQuery = triggers[i]
		}
	}
	if flags.IsSet(exec.PlanFlagIsDDL) {
		res.flags.Set(planFlagIsDDL)
	}
	if flags.IsSet(exec.PlanFlagContainsFullTableScan) {
		res.flags.Set(planFlagContainsFullTableScan)
	}
	if flags.IsSet(exec.PlanFlagContainsFullIndexScan) {
		res.flags.Set(planFlagContainsFullIndexScan)
	}
	if flags.IsSet(exec.PlanFlagContainsLargeFullTableScan) {
		res.flags.Set(planFlagContainsLargeFullTableScan)
	}
	if flags.IsSet(exec.PlanFlagContainsLargeFullIndexScan) {
		res.flags.Set(planFlagContainsLargeFullIndexScan)
	}
	if flags.IsSet(exec.PlanFlagContainsMutation) {
		res.flags.Set(planFlagContainsMutation)
	}
	if flags.IsSet(exec.PlanFlagContainsLocking) {
		res.flags.Set(planFlagContainsLocking)
	}
	if flags.IsSet(exec.PlanFlagCheckContainsLocking) {
		res.flags.Set(planFlagCheckContainsLocking)
	}
	if flags.IsSet(exec.PlanFlagContainsDelete) {
		res.flags.Set(planFlagContainsDelete)
	}
	if flags.IsSet(exec.PlanFlagContainsInsert) {
		res.flags.Set(planFlagContainsInsert)
	}
	if flags.IsSet(exec.PlanFlagContainsUpdate) {
		res.flags.Set(planFlagContainsUpdate)
	}
	if flags.IsSet(exec.PlanFlagContainsUpsert) {
		res.flags.Set(planFlagContainsUpsert)
	}

	return res, nil
}

// makeScanColumnsConfig builds a scanColumnsConfig struct by constructing a
// list of descriptor IDs for columns in the given cols set. Columns are
// identified by their ordinal position in the table schema.
func makeScanColumnsConfig(table cat.Table, cols exec.TableColumnOrdinalSet) scanColumnsConfig {
	colCfg := scanColumnsConfig{
		wantedColumns: make([]tree.ColumnID, 0, cols.Len()),
	}
	for ord, ok := cols.Next(0); ok; ord, ok = cols.Next(ord + 1) {
		col := table.Column(ord)
		if col.Kind() == cat.Inverted {
			colCfg.invertedColumnType = col.DatumType()
			colOrd := col.InvertedSourceColumnOrdinal()
			col = table.Column(colOrd)
			colCfg.invertedColumnID = tree.ColumnID(col.ColID())
		}
		colCfg.wantedColumns = append(colCfg.wantedColumns, tree.ColumnID(col.ColID()))
	}
	return colCfg
}

// tableToScanOrdinals finds for each table column ordinal in cols the
// corresponding index in the scan columns.
func tableToScanOrdinals(
	scanCols exec.TableColumnOrdinalSet, cols []exec.TableColumnOrdinal,
) ([]int, error) {
	result := make([]int, len(cols))
	for i, colOrd := range cols {
		// Find the position in the scanCols set (makeScanColumnsConfig sets up the
		// scan columns in increasing ordinal order).
		j := 0
		for ord, ok := scanCols.Next(0); ; ord, ok = scanCols.Next(ord + 1) {
			if !ok {
				return nil, errors.AssertionFailedf("column not among scanned columns")
			}
			if ord == int(colOrd) {
				result[i] = j
				break
			}
			j++
		}
	}
	return result, nil
}

// getResultColumnsForSimpleProject populates result columns for a simple
// projection. inputCols must be non-nil and contain the result columns before
// the projection has been applied. It supports two configurations:
//  1. colNames and resultTypes are non-nil. resultTypes indicates the updated
//     types (after the projection has been applied)
//  2. colNames is nil.
func getResultColumnsForSimpleProject(
	cols []exec.NodeColumnOrdinal,
	colNames []string,
	resultTypes []*types.T,
	inputCols colinfo.ResultColumns,
) colinfo.ResultColumns {
	resultCols := make(colinfo.ResultColumns, len(cols))
	for i, col := range cols {
		if colNames == nil {
			resultCols[i] = inputCols[col]
			// If we have a SimpleProject, we should clear the hidden bit on any
			// column since it indicates it's been explicitly selected.
			resultCols[i].Hidden = false
		} else {
			resultCols[i] = colinfo.ResultColumn{
				Name:           colNames[i],
				Typ:            resultTypes[i],
				TableID:        inputCols[col].TableID,
				PGAttributeNum: inputCols[col].PGAttributeNum,
			}
		}
	}
	return resultCols
}

func getEqualityIndicesAndMergeJoinOrdering(
	leftOrdering, rightOrdering colinfo.ColumnOrdering,
) (
	leftEqualityIndices, rightEqualityIndices []exec.NodeColumnOrdinal,
	mergeJoinOrdering colinfo.ColumnOrdering,
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

	mergeJoinOrdering = make(colinfo.ColumnOrdering, n)
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
	inputCols colinfo.ResultColumns, groupCols []exec.NodeColumnOrdinal, aggregations []exec.AggInfo,
) colinfo.ResultColumns {
	columns := make(colinfo.ResultColumns, 0, len(groupCols)+len(aggregations))
	for _, col := range groupCols {
		columns = append(columns, inputCols[col])
	}
	for _, agg := range aggregations {
		columns = append(columns, colinfo.ResultColumn{
			Name: agg.FuncName,
			Typ:  agg.ResultType,
		})
	}
	return columns
}

func constructVirtualScan(
	ef exec.Factory,
	p *planner,
	table cat.Table,
	index cat.Index,
	params exec.ScanParams,
	reqOrdering exec.OutputOrdering,
	// delayedNodeCallback is a callback function that performs custom setup
	// that varies by exec.Factory implementations.
	delayedNodeCallback func(*delayedNode) (exec.Node, error),
) (exec.Node, error) {
	tn := &table.(*optVirtualTable).name
	virtual, err := p.getVirtualTabler().getVirtualTableEntry(tn, p)
	if err != nil {
		return nil, err
	}
	if !canQueryVirtualTable(p.EvalContext(), virtual) {
		return nil, newUnimplementedVirtualTableError(tn.Schema(), tn.Table())
	}
	idx := index.(*optVirtualIndex).idx
	columns, constructor := virtual.getPlanInfo(
		table.(*optVirtualTable).desc, idx, params.IndexConstraint, p.execCfg.Stopper,
	)

	n, err := delayedNodeCallback(&delayedNode{
		name:    fmt.Sprintf("%s@%s", table.Name(), index.Name()),
		columns: columns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			return constructor(ctx, p, tn.Catalog())
		},
	})
	if err != nil {
		return nil, err
	}

	// Check for explicit use of the dummy column.
	if params.NeededCols.Contains(0) {
		return nil, errors.Errorf("use of %s column not allowed.", table.Column(0).ColName())
	}
	if !params.Locking.IsNoOp() {
		// We shouldn't have allowed SELECT FOR UPDATE for a virtual table.
		return nil, errors.AssertionFailedf("locking cannot be used with virtual table")
	}
	if needed := params.NeededCols; needed.Len() != len(columns) {
		// We are selecting a subset of columns; we need a projection.
		cols := make([]exec.NodeColumnOrdinal, 0, needed.Len())
		for ord, ok := needed.Next(0); ok; ord, ok = needed.Next(ord + 1) {
			cols = append(cols, exec.NodeColumnOrdinal(ord-1))
		}
		n, err = ef.ConstructSimpleProject(n, cols, nil /* reqOrdering */)
		if err != nil {
			return nil, err
		}
	}
	if params.HardLimit != 0 {
		n, err = ef.ConstructLimit(n, tree.NewDInt(tree.DInt(params.HardLimit)), nil /* offset */)
		if err != nil {
			return nil, err
		}
	}
	// reqOrdering will be set if the optimizer expects that the output of the
	// exec.Node that we're returning will actually have a legitimate ordering.
	// Virtual indexes never provide a legitimate ordering, so we have to make
	// sure to sort if we have a required ordering.
	if len(reqOrdering) != 0 {
		n, err = ef.ConstructSort(n, reqOrdering, 0 /* alreadyOrderedPrefix */, 0 /* estimatedInputRowCount */)
		if err != nil {
			return nil, err
		}
	}
	return n, nil
}

func scanContainsSystemColumns(colCfg *scanColumnsConfig) bool {
	for _, id := range colCfg.wantedColumns {
		if colinfo.IsColIDSystemColumn(id) {
			return true
		}
	}
	return false
}

func constructOpaque(metadata opt.OpaqueMetadata) (planNode, error) {
	o, ok := metadata.(*opaqueMetadata)
	if !ok {
		return nil, errors.AssertionFailedf("unexpected OpaqueMetadata object type %T", metadata)
	}
	return o.plan, nil
}

func convertFastIntSetToUint32Slice(colIdxs intsets.Fast) []uint32 {
	cols := make([]uint32, 0, colIdxs.Len())
	for i, ok := colIdxs.Next(0); ok; i, ok = colIdxs.Next(i + 1) {
		cols = append(cols, uint32(i))
	}
	return cols
}
