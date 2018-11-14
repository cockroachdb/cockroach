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

package bench

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// stubFactory is a do-nothing implementation of exec.Factory, used for testing.
type stubFactory struct{}

func (f *stubFactory) ConstructValues(
	rows [][]tree.TypedExpr, cols sqlbase.ResultColumns,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructScan(
	table opt.Table,
	index opt.Index,
	needed exec.ColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	hardLimit int64,
	reverse bool,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructVirtualScan(table opt.Table) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructFilter(
	n exec.Node, filter tree.TypedExpr, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructSimpleProject(
	n exec.Node, cols []exec.ColumnOrdinal, colNames []string, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructRender(
	n exec.Node, exprs tree.TypedExprs, colNames []string, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructHashJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	leftEqCols, rightEqCols []exec.ColumnOrdinal,
	leftEqColsAreKey, rightEqColsAreKey bool,
	extraOnCond tree.TypedExpr,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructMergeJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	onCond tree.TypedExpr,
	leftOrdering, rightOrdering sqlbase.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructGroupBy(
	input exec.Node,
	groupCols []exec.ColumnOrdinal,
	orderedGroupCols exec.ColumnOrdinalSet,
	aggregations []exec.AggInfo,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructScalarGroupBy(
	input exec.Node, aggregations []exec.AggInfo,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructDistinct(
	input exec.Node, distinctCols, orderedCols exec.ColumnOrdinalSet,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructSetOp(
	typ tree.UnionType, all bool, left, right exec.Node,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructSort(
	input exec.Node, ordering sqlbase.ColumnOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructOrdinality(input exec.Node, colName string) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructIndexJoin(
	input exec.Node, table opt.Table, cols exec.ColumnOrdinalSet, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructLookupJoin(
	joinType sqlbase.JoinType,
	input exec.Node,
	table opt.Table,
	index opt.Index,
	keyCols []exec.ColumnOrdinal,
	lookupCols exec.ColumnOrdinalSet,
	onCond tree.TypedExpr,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructLimit(
	input exec.Node, limit, offset tree.TypedExpr,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructProjectSet(
	n exec.Node, exprs tree.TypedExprs, zipCols sqlbase.ResultColumns, numColsPerGen []int,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) RenameColumns(input exec.Node, colNames []string) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructPlan(root exec.Node, subqueries []exec.Subquery) (exec.Plan, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructExplain(
	options *tree.ExplainOptions, plan exec.Plan,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructShowTrace(typ tree.ShowTraceType, compact bool) (exec.Node, error) {
	return struct{}{}, nil
}
