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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/pkg/errors"
)

type distSQLFactory struct {
	planner *planner
}

var _ exec.Factory = &distSQLFactory{}

func makeDistSQLFactory(p *planner) distSQLFactory {
	return distSQLFactory{planner: p}
}

func (f *distSQLFactory) ConstructValues(
	rows [][]tree.TypedExpr, cols sqlbase.ResultColumns,
) (exec.Node, error) {
	return struct{}{}, errors.Errorf("ConstructValues: not yet implemented")
}

func (f *distSQLFactory) distSQLPlanCtx() *PlanningCtx {

	evalCtx := f.planner.ExtendedEvalContext()
	planCtx := f.planner.ExecCfg().DistSQLPlanner.NewPlanningCtx(context.TODO(), evalCtx, f.planner.txn)

	planCtx.isLocal = false
	planCtx.planner = f.planner
	planCtx.stmtType = tree.Rows

	return planCtx
}

func (f *distSQLFactory) getIndexIdx(
	desc *sqlbase.TableDescriptor, index *sqlbase.IndexDescriptor,
) (uint32, error) {
	if index == &desc.PrimaryIndex {
		return 0, nil
	}
	for i := range desc.Indexes {
		if index == &desc.Indexes[i] {
			// IndexIdx is 1 based (0 means primary index).
			return uint32(i + 1), nil
		}
	}
	return 0, errors.Errorf("invalid index %v (table %s)", index, desc.Name)
}

// initTableReaderSpec initializes a TableReaderSpec/PostProcessSpec that
// corresponds to a scanNode, except for the Spans and OutputColumns.
func (f *distSQLFactory) initTableReaderSpec(
	desc *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	hardLimit int64,
	reverse bool,
	planCtx *PlanningCtx,
) (distsqlrun.TableReaderSpec, distsqlrun.PostProcessSpec, error) {
	s := distsqlrun.TableReaderSpec{
		Table:      *desc,
		Reverse:    reverse,
		IsCheck:    false,
		Visibility: distsqlrun.ScanVisibility_PUBLIC,
	}
	indexIdx, err := f.getIndexIdx(desc, index)
	if err != nil {
		return distsqlrun.TableReaderSpec{}, distsqlrun.PostProcessSpec{}, err
	}
	s.IndexIdx = indexIdx

	post := distsqlrun.PostProcessSpec{}
	if hardLimit != 0 {
		post.Limit = uint64(hardLimit)
	}
	return s, post, nil
}

// spansFromConstraint converts the spans in a Constraint to roachpb.Spans.
//
// interstices are pieces of the key that need to be inserted after each column
// (for interleavings).
func (f *distSQLFactory) spansFromConstraint(
	tableDesc *sqlbase.TableDescriptor, index *sqlbase.IndexDescriptor, c *constraint.Constraint,
) (roachpb.Spans, error) {
	interstices := make([][]byte, len(index.ColumnDirections)+len(index.ExtraColumnIDs)+1)
	interstices[0] = sqlbase.MakeIndexKeyPrefix(tableDesc, index.ID)
	if len(index.Interleave.Ancestors) > 0 {
		// TODO(eisen): too much of this code is copied from EncodePartialIndexKey.
		sharedPrefixLen := 0
		for i, ancestor := range index.Interleave.Ancestors {
			// The first ancestor is already encoded in interstices[0].
			if i != 0 {
				interstices[sharedPrefixLen] =
					encoding.EncodeUvarintAscending(interstices[sharedPrefixLen], uint64(ancestor.TableID))
				interstices[sharedPrefixLen] =
					encoding.EncodeUvarintAscending(interstices[sharedPrefixLen], uint64(ancestor.IndexID))
			}
			sharedPrefixLen += int(ancestor.SharedPrefixLen)
			interstices[sharedPrefixLen] = encoding.EncodeInterleavedSentinel(interstices[sharedPrefixLen])
		}
		interstices[sharedPrefixLen] =
			encoding.EncodeUvarintAscending(interstices[sharedPrefixLen], uint64(tableDesc.ID))
		interstices[sharedPrefixLen] =
			encoding.EncodeUvarintAscending(interstices[sharedPrefixLen], uint64(index.ID))
	}

	if c == nil || c.IsUnconstrained() {
		// Encode a full span.
		sp, err := spanFromConstraintSpan(tableDesc, index, &constraint.UnconstrainedSpan, interstices)
		if err != nil {
			return nil, err
		}
		return roachpb.Spans{sp}, nil
	}

	spans := make(roachpb.Spans, c.Spans.Count())
	for i := range spans {
		s, err := spanFromConstraintSpan(tableDesc, index, c.Spans.Get(i), interstices)
		if err != nil {
			return nil, err
		}
		spans[i] = s
	}
	return spans, nil
}

func (f *distSQLFactory) makeTableReaderSpans(spans roachpb.Spans) []distsqlrun.TableReaderSpan {
	trSpans := make([]distsqlrun.TableReaderSpan, len(spans))
	for i, span := range spans {
		trSpans[i].Span = span
	}

	return trSpans
}

func (f *distSQLFactory) ConstructScan(
	table opt.Table,
	index opt.Index,
	needed exec.ColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	hardLimit int64,
	reverse bool,
	reqOrdering exec.OutputOrdering,
	reqDistSQLOrdering distsqlrun.Ordering,
) (exec.Node, error) {

	tabDesc := table.(*optTable).desc
	indexDesc := index.(*optIndex).desc
	dsp := f.planner.execCfg.DistSQLPlanner
	spans, err := f.spansFromConstraint(tabDesc, indexDesc, indexConstraint)
	if err != nil {
		return nil, err
	}

	planCtx := f.distSQLPlanCtx()
	f.planner.execCfg.PlanningCtx = planCtx

	spec, post, err := f.initTableReaderSpec(tabDesc, indexDesc, hardLimit, reverse, planCtx)
	if err != nil {
		return PhysicalPlan{}, err
	}

	var spanPartitions []SpanPartition
	if planCtx.isLocal {
		spanPartitions = []SpanPartition{{dsp.nodeDesc.NodeID, spans}}
	} else if hardLimit == 0 {
		// No limit - plan all table readers where their data live.
		spanPartitions, err = dsp.PartitionSpans(planCtx, spans)
		if err != nil {
			return PhysicalPlan{}, err
		}
	} else {
		// If the scan is limited, use a single TableReader to avoid reading more
		// rows than necessary. Note that distsql is currently only enabled for hard
		// limits since the TableReader will still read too eagerly in the soft
		// limit case. To prevent this we'll need a new mechanism on the execution
		// side to modulate table reads.
		nodeID, err := dsp.getNodeIDForScan(planCtx, spans, reverse)
		if err != nil {
			return PhysicalPlan{}, err
		}
		spanPartitions = []SpanPartition{{nodeID, spans}}
	}

	var p PhysicalPlan
	stageID := p.NewStageID()

	p.ResultRouters = make([]distsqlplan.ProcessorIdx, len(spanPartitions))

	for i, sp := range spanPartitions {
		tr := &distsqlrun.TableReaderSpec{}
		*tr = spec
		tr.Spans = f.makeTableReaderSpans(sp.Spans)

		proc := distsqlplan.Processor{
			Node: sp.Node,
			Spec: distsqlrun.ProcessorSpec{
				Core:    distsqlrun.ProcessorCoreUnion{TableReader: tr},
				Output:  []distsqlrun.OutputRouterSpec{{Type: distsqlrun.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}

		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}

	for i := range reqOrdering {
		if reqOrdering[i].ColIdx >= needed.Len() {
			return nil, errors.Errorf("invalid reqOrdering: %v", reqOrdering)
		}
	}

	if len(p.ResultRouters) > 1 && len(reqDistSQLOrdering.Columns) > 0 {
		// Make a note of the fact that we have to maintain a certain ordering
		// between the parallel streams.
		//
		// This information is taken into account by the AddProjection call below:
		// specifically, it will make sure these columns are kept even if they are
		// not in the projection (e.g. "SELECT v FROM kv ORDER BY k").
		p.SetMergeOrdering(reqDistSQLOrdering)
	}

	types := make([]sqlbase.ColumnType, 0, len(tabDesc.Columns))
	for i := range tabDesc.Columns {
		types = append(types, tabDesc.Columns[i].Type)
	}

	p.SetLastStagePost(post, types)

	planToStreamColMap := make([]int, needed.Len())
	outCols := make([]uint32, needed.Len())
	i := 0
	for j, ok := needed.Next(0); ok; j, ok = needed.Next(j + 1) {
		planToStreamColMap[i] = j
		outCols[i] = uint32(j)
		i++
	}
	p.AddProjection(outCols)

	p.PlanToStreamColMap = planToStreamColMap
	return &p, nil
}

func (f *distSQLFactory) ConstructVirtualScan(table opt.Table) (exec.Node, error) {
	return struct{}{}, errors.Errorf("ConstructVirtualScan: not yet implemented")
}

func (f *distSQLFactory) ConstructFilter(n exec.Node, filter tree.TypedExpr) (exec.Node, error) {
	return struct{}{}, errors.Errorf("ConstructFilter: not yet implemented")
}

func (f *distSQLFactory) ConstructSimpleProject(
	n exec.Node, cols []exec.ColumnOrdinal, colNames []string,
) (exec.Node, error) {
	return struct{}{}, errors.Errorf("ConstructSimpleProject: not yet implemented")
}

func (f *distSQLFactory) ConstructRender(
	n exec.Node, exprs tree.TypedExprs, colNames []string,
) (exec.Node, error) {
	return struct{}{}, errors.Errorf("ConstructRender: not yet implemented")
}

func (f *distSQLFactory) ConstructHashJoin(
	joinType sqlbase.JoinType, left, right exec.Node, onCond tree.TypedExpr,
) (exec.Node, error) {
	return struct{}{}, errors.Errorf("ConstructHashJoin: not yet implemented")
}

func (f *distSQLFactory) ConstructMergeJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	onCond tree.TypedExpr,
	leftOrdering, rightOrdering sqlbase.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, errors.Errorf("ConstructMergeJoin: not yet implemented")
}

func (f *distSQLFactory) ConstructGroupBy(
	input exec.Node,
	groupCols []exec.ColumnOrdinal,
	orderedGroupCols exec.ColumnOrdinalSet,
	aggregations []exec.AggInfo,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, errors.Errorf("ConstructGroupBy: not yet implemented")
}

func (f *distSQLFactory) ConstructScalarGroupBy(
	input exec.Node, aggregations []exec.AggInfo,
) (exec.Node, error) {
	return struct{}{}, errors.Errorf("ConstructScalarGroupBy: not yet implemented")
}

func (f *distSQLFactory) ConstructDistinct(
	input exec.Node, distinctCols, orderedCols exec.ColumnOrdinalSet,
) (exec.Node, error) {
	return struct{}{}, errors.Errorf("ConstructDistinct: not yet implemented")
}

func (f *distSQLFactory) ConstructSetOp(
	typ tree.UnionType, all bool, left, right exec.Node,
) (exec.Node, error) {
	return struct{}{}, errors.Errorf("ConstructSetOp: not yet implemented")
}

func (f *distSQLFactory) ConstructSort(
	input exec.Node, ordering sqlbase.ColumnOrdering,
) (exec.Node, error) {
	return struct{}{}, errors.Errorf("ConstructSort: not yet implemented")
}

func (f *distSQLFactory) ConstructOrdinality(input exec.Node, colName string) (exec.Node, error) {
	return struct{}{}, errors.Errorf("ConstructOrdinality: not yet implemented")
}

func (f *distSQLFactory) ConstructIndexJoin(
	input exec.Node, table opt.Table, cols exec.ColumnOrdinalSet, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, errors.Errorf("ConstructIndexJoin: not yet implemented")
}

func (f *distSQLFactory) ConstructLookupJoin(
	joinType sqlbase.JoinType,
	input exec.Node,
	table opt.Table,
	index opt.Index,
	keyCols []exec.ColumnOrdinal,
	lookupCols exec.ColumnOrdinalSet,
	onCond tree.TypedExpr,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, errors.Errorf("ConstructLookupJoin: not yet implemented")
}

func (f *distSQLFactory) ConstructLimit(
	input exec.Node, limit, offset tree.TypedExpr,
) (exec.Node, error) {
	return struct{}{}, errors.Errorf("ConstructLimit: not yet implemented")
}

func (f *distSQLFactory) ConstructProjectSet(
	n exec.Node, exprs tree.TypedExprs, zipCols sqlbase.ResultColumns, numColsPerGen []int,
) (exec.Node, error) {
	return struct{}{}, errors.Errorf("ConstructProjectSet: not yet implemented")
}

func (f *distSQLFactory) RenameColumns(n exec.Node, colNames []string) (exec.Node, error) {
	// This is handled elsewhere for DistSQL.
	return n, nil
}

func (f *distSQLFactory) ConstructPlan(root exec.Node, subqueries []exec.Subquery) (exec.Plan, error) {
	if len(subqueries) > 0 {
		return struct{}{}, errors.Errorf("ConstructPlan: subqueries not yet implemented")
	}
	return root, nil

}

func (f *distSQLFactory) ConstructExplain(
	options *tree.ExplainOptions, plan exec.Plan,
) (exec.Node, error) {
	return struct{}{}, errors.Errorf("ConstructExplain: not yet implemented")
}

func (f *distSQLFactory) ConstructShowTrace(typ tree.ShowTraceType, compact bool) (exec.Node, error) {
	return struct{}{}, errors.Errorf("ConstructShowTrace: not yet implemented")
}
