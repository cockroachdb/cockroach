// Copyright 2018 The Cockroach Authors.
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
	"context"
	"fmt"
	"math"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

var _ exec.TestEngineFactory = &Executor{}

// NewTestEngine is part of the exec.TestEngineFactory interface.
func (e *Executor) NewTestEngine(defaultDatabase string) exec.TestEngine {
	txn := client.NewTxn(e.cfg.DB, e.cfg.NodeID.Get(), client.RootTxn)
	p, cleanup := newInternalPlanner("opt", txn, "root", &MemoryMetrics{}, &e.cfg)
	// TODO(radu): Setting this directly is a hack.
	p.extendedEvalCtx.SessionData.Database = defaultDatabase
	return newExecEngine(p, cleanup)
}

type execEngine struct {
	catalog optCatalog
	planner *planner
	cleanup func()
}

var _ exec.TestEngine = &execEngine{}

func newExecEngine(p *planner, cleanup func()) *execEngine {
	ee := &execEngine{planner: p, cleanup: cleanup}
	ee.catalog.init(p)
	return ee
}

// Factory is part of the exec.TestEngine interface.
func (ee *execEngine) Factory() exec.Factory {
	return ee
}

// Catalog is part of the exec.TestEngine interface.
func (ee *execEngine) Catalog() opt.Catalog {
	return &ee.catalog
}

// Columns is part of the exec.TestEngine interface.
func (ee *execEngine) Columns(n exec.Node) sqlbase.ResultColumns {
	return planColumns(n.(planNode))
}

// Execute is part of the exec.TestEngine interface.
func (ee *execEngine) Execute(n exec.Node) ([]tree.Datums, error) {
	plan := n.(planNode)

	params := runParams{
		ctx:             context.TODO(),
		extendedEvalCtx: &ee.planner.extendedEvalCtx,
		p:               ee.planner,
	}
	if err := startPlan(params, plan); err != nil {
		return nil, err
	}
	var res []tree.Datums
	for {
		ok, err := plan.Next(params)
		if err != nil {
			return res, nil
		}
		if !ok {
			break
		}
		res = append(res, append(tree.Datums(nil), plan.Values()...))
	}
	plan.Close(context.TODO())
	return res, nil
}

// Explain is part of the exec.TestEngine interface.
func (ee *execEngine) Explain(n exec.Node) ([]tree.Datums, error) {
	plan := n.(planNode)

	// Add an explain node to the plan and run that.
	flags := explainFlags{
		showMetadata: true,
		showExprs:    true,
		qualifyNames: true,
	}
	explainNode, err := ee.planner.makeExplainPlanNodeWithPlan(
		context.TODO(), flags, false /* expanded */, false /* optimized */, plan,
	)
	if err != nil {
		return nil, err
	}

	// Execute the explain node.
	return ee.Execute(explainNode)
}

// Close is part of the exec.TestEngine interface.
func (ee *execEngine) Close() {
	if ee.cleanup != nil {
		ee.cleanup()
	}
}

var _ exec.Factory = &execEngine{}

// ConstructValues is part of the exec.Factory interface.
func (ee *execEngine) ConstructValues(
	rows [][]tree.TypedExpr, cols sqlbase.ResultColumns,
) (exec.Node, error) {
	return &valuesNode{
		columns: cols,
		tuples:  rows,
	}, nil
}

// ConstructScan is part of the exec.Factory interface.
func (ee *execEngine) ConstructScan(
	table opt.Table,
	index opt.Index,
	cols exec.ColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	hardLimit int64,
) (exec.Node, error) {
	tabDesc := table.(*optTable).desc
	indexDesc := index.(*optIndex).desc
	// Create a scanNode.
	scan := ee.planner.Scan()
	colCfg := scanColumnsConfig{
		wantedColumns: make([]tree.ColumnID, 0, cols.Len()),
	}
	for c, ok := cols.Next(0); ok; c, ok = cols.Next(c + 1) {
		colCfg.wantedColumns = append(colCfg.wantedColumns, tree.ColumnID(tabDesc.Columns[c].ID))
	}
	if err := scan.initTable(context.TODO(), ee.planner, tabDesc, nil, colCfg); err != nil {
		return nil, err
	}
	scan.index = indexDesc
	scan.run.isSecondaryIndex = (indexDesc != &tabDesc.PrimaryIndex)
	scan.hardLimit = hardLimit
	var err error
	scan.spans, err = spansFromConstraint(tabDesc, indexDesc, indexConstraint)
	if err != nil {
		return nil, err
	}
	return scan, nil
}

func asDataSource(n exec.Node) planDataSource {
	plan := n.(planNode)
	return planDataSource{
		info: &sqlbase.DataSourceInfo{SourceColumns: planColumns(plan)},
		plan: plan,
	}
}

// ConstructFilter is part of the exec.Factory interface.
func (ee *execEngine) ConstructFilter(n exec.Node, filter tree.TypedExpr) (exec.Node, error) {
	src := asDataSource(n)
	f := &filterNode{
		source: src,
	}
	f.ivarHelper = tree.MakeIndexedVarHelper(f, len(src.info.SourceColumns))
	f.filter = f.ivarHelper.Rebind(filter, true /* alsoReset */, false /* normalizeToNonNil */)
	return f, nil
}

// ConstructSimpleProject is part of the exec.Factory interface.
func (ee *execEngine) ConstructSimpleProject(
	n exec.Node, cols []exec.ColumnOrdinal, colNames []string,
) (exec.Node, error) {
	var inputCols sqlbase.ResultColumns
	if colNames == nil {
		// We will need the names of the input columns.
		inputCols = planColumns(n.(planNode))
	}
	src := asDataSource(n)
	r := &renderNode{
		source:     src,
		sourceInfo: sqlbase.MultiSourceInfo{src.info},
		render:     make([]tree.TypedExpr, len(cols)),
		columns:    make([]sqlbase.ResultColumn, len(cols)),
	}
	r.ivarHelper = tree.MakeIndexedVarHelper(r, len(src.info.SourceColumns))
	for i, col := range cols {
		v := r.ivarHelper.IndexedVar(int(col))
		r.render[i] = v
		if colNames == nil {
			r.columns[i].Name = inputCols[col].Name
		} else {
			r.columns[i].Name = colNames[i]
		}
		r.columns[i].Typ = v.ResolvedType()
	}
	return r, nil
}

// ConstructRender is part of the exec.Factory interface.
func (ee *execEngine) ConstructRender(
	n exec.Node, exprs tree.TypedExprs, colNames []string,
) (exec.Node, error) {
	src := asDataSource(n)
	r := &renderNode{
		source:     src,
		sourceInfo: sqlbase.MultiSourceInfo{src.info},
		render:     make([]tree.TypedExpr, len(exprs)),
		columns:    make([]sqlbase.ResultColumn, len(exprs)),
	}
	r.ivarHelper = tree.MakeIndexedVarHelper(r, len(src.info.SourceColumns))
	for i, expr := range exprs {
		expr = r.ivarHelper.Rebind(expr, false /* alsoReset */, true /* normalizeToNonNil */)
		r.render[i] = expr
		r.columns[i] = sqlbase.ResultColumn{Name: colNames[i], Typ: expr.ResolvedType()}
	}
	return r, nil
}

// RenameColumns is part of the exec.Factory interface.
func (ee *execEngine) RenameColumns(n exec.Node, colNames []string) (exec.Node, error) {
	inputCols := planMutableColumns(n.(planNode))
	for i := range inputCols {
		inputCols[i].Name = colNames[i]
	}
	return n, nil
}

// ConstructJoin is part of the exec.Factory interface.
func (ee *execEngine) ConstructJoin(
	joinType sqlbase.JoinType, left, right exec.Node, onCond tree.TypedExpr,
) (exec.Node, error) {
	p := ee.planner
	leftSrc := asDataSource(left)
	rightSrc := asDataSource(right)
	pred, _, err := p.makeJoinPredicate(
		context.TODO(), leftSrc.info, rightSrc.info, joinType, nil, /* cond */
	)
	if err != nil {
		return nil, err
	}
	onCond = pred.iVarHelper.Rebind(
		onCond, false /* alsoReset */, false, /* normmalizeToNonNil */
	)
	// Try to harvest equality columns from the ON expression.
	onAndExprs := splitAndExpr(p.EvalContext(), onCond, nil /* exprs */)
	for _, e := range onAndExprs {
		if e != tree.DBoolTrue && !pred.tryAddEqualityFilter(e, leftSrc.info, rightSrc.info) {
			pred.onCond = mergeConj(pred.onCond, e)
		}
	}

	return p.makeJoinNode(leftSrc, rightSrc, pred), nil
}

// ConstructGroupBy is part of the exec.Factory interface.
func (ee *execEngine) ConstructGroupBy(
	input exec.Node, groupCols []exec.ColumnOrdinal, aggregations []exec.AggInfo,
) (exec.Node, error) {
	n := &groupNode{
		plan:      input.(planNode),
		funcs:     make([]*aggregateFuncHolder, 0, len(groupCols)+len(aggregations)),
		columns:   make(sqlbase.ResultColumns, 0, len(groupCols)+len(aggregations)),
		groupCols: make([]int, len(groupCols)),
	}
	for i, col := range groupCols {
		n.groupCols[i] = int(col)
	}
	inputCols := planColumns(n.plan)
	for _, idx := range groupCols {
		// TODO(radu): only generate the grouping columns we actually need.
		f := n.newAggregateFuncHolder(
			"", /* funcName */
			inputCols[idx].Typ,
			int(idx),
			builtins.NewIdentAggregate,
			ee.planner.EvalContext().Mon.MakeBoundAccount(),
		)
		n.funcs = append(n.funcs, f)
		n.columns = append(n.columns, inputCols[idx])
	}

	for i := range aggregations {
		agg := &aggregations[i]
		builtin := agg.Builtin
		var renderIdx int
		var aggFn func(*tree.EvalContext) tree.AggregateFunc

		switch len(agg.ArgCols) {
		case 0:
			renderIdx = noRenderIdx
			aggFn = func(evalCtx *tree.EvalContext) tree.AggregateFunc {
				return builtin.AggregateFunc([]types.T{}, evalCtx)
			}

		case 1:
			renderIdx = int(agg.ArgCols[0])
			aggFn = func(evalCtx *tree.EvalContext) tree.AggregateFunc {
				return builtin.AggregateFunc([]types.T{inputCols[renderIdx].Typ}, evalCtx)
			}

		default:
			return nil, errors.Errorf("multi-argument aggregation functions not implemented")
		}

		f := n.newAggregateFuncHolder(
			agg.FuncName,
			agg.ResultType,
			renderIdx,
			aggFn,
			ee.planner.EvalContext().Mon.MakeBoundAccount(),
		)
		n.funcs = append(n.funcs, f)
		n.columns = append(n.columns, sqlbase.ResultColumn{
			Name: fmt.Sprintf("agg%d", i),
			Typ:  agg.ResultType,
		})
	}

	// Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was aggregated.
	n.run.addNullBucketIfEmpty = len(groupCols) == 0
	n.run.buckets = make(map[string]struct{})
	return n, nil
}

// ConstructSetOp is part of the exec.Factory interface.
func (ee *execEngine) ConstructSetOp(
	typ tree.UnionType, all bool, left, right exec.Node,
) (exec.Node, error) {
	return ee.planner.newUnionNode(typ, all, left.(planNode), right.(planNode))
}

// ConstructSort is part of the exec.Factory interface.
func (ee *execEngine) ConstructSort(
	input exec.Node, ordering sqlbase.ColumnOrdering,
) (exec.Node, error) {
	plan := input.(planNode)
	inputColumns := planColumns(plan)
	return &sortNode{
		plan:     plan,
		columns:  inputColumns,
		ordering: ordering,
		needSort: true,
	}, nil
}

// ConstructLimit is part of the exec.Factory interface.
func (ee *execEngine) ConstructLimit(
	input exec.Node, limit int64, offset int64,
) (exec.Node, error) {
	var limitVal, offsetVal tree.Datum
	if limit != math.MaxInt64 {
		limitVal = tree.NewDInt(tree.DInt(limit))
	}
	if offset != 0 {
		offsetVal = tree.NewDInt(tree.DInt(offset))
	}
	plan := input.(planNode)
	// If the input plan is also a limitNode that has just an offset, and we are
	// only applying a limit, update the existing node. This is useful because
	// Limit and Offset are separate operators which result in separate calls to
	// this function.
	if l, ok := plan.(*limitNode); ok && l.countExpr == nil && offsetVal == nil {
		l.countExpr = limitVal
		return l, nil
	}
	return &limitNode{
		plan:       plan,
		evaluated:  true,
		countExpr:  limitVal,
		offsetExpr: offsetVal,
	}, nil
}
