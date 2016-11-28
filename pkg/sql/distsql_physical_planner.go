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
// Author: Radu Berinde (radu@cockroachlabs.com)

package sql

import (
	"bytes"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// distSQLPLanner implements distSQL physical planning and running logic.
//
// A rough overview of the process:
//
//  - the plan is based on a planNode tree (in the future it will be based on an
//    intermediate representation tree). Only a subset of the possible trees is
//    supported (this can be checked via CheckSupport).
//
//  - we generate a physicalPlan for the planNode tree recursively. The
//    physicalPlan consists of a network of processors and streams, with a set
//    of unconnected "result routers". The physicalPlan also has information on
//    ordering and on the mapping planNode columns to columns in the result
//    streams (all result routers output streams with the same schema).
//
//    The physicalPlan for a scanNode leaf consists of TableReaders, one for each node
//    that has one or more ranges.
//
//  - for each an internal planNode we start with the plan of the child node(s)
//    and add processing stages (connected to the result routers of the children
//    node).
type distSQLPlanner struct {
	nodeDesc     roachpb.NodeDescriptor
	rpcContext   *rpc.Context
	distSQLSrv   *distsql.ServerImpl
	spanResolver *distsql.SpanResolver
}

const resolverPolicy = distsql.BinPackingLeaseHolderChoice

func newDistSQLPlanner(
	nodeDesc roachpb.NodeDescriptor,
	rpcCtx *rpc.Context,
	distSQLSrv *distsql.ServerImpl,
	distSender *kv.DistSender,
	gossip *gossip.Gossip,
) *distSQLPlanner {
	return &distSQLPlanner{
		nodeDesc:     nodeDesc,
		rpcContext:   rpcCtx,
		distSQLSrv:   distSQLSrv,
		spanResolver: distsql.NewSpanResolver(distSender, gossip, nodeDesc, resolverPolicy),
	}
}

// distSQLExprCheckVisitor is a parser.Visitor that checks if expressions
// contain things not supported by distSQL (like subqueries).
type distSQLExprCheckVisitor struct {
	err error
}

var _ parser.Visitor = &distSQLExprCheckVisitor{}

func (v *distSQLExprCheckVisitor) VisitPre(expr parser.Expr) (recurse bool, newExpr parser.Expr) {
	if v.err != nil {
		return false, expr
	}
	switch t := expr.(type) {
	case *subquery, *parser.Subquery:
		v.err = errors.Errorf("subqueries not supported yet")
		return false, expr

	case *parser.FuncExpr:
		if t.IsContextDependent() {
			v.err = errors.Errorf("context-dependent function %s not supported", t)
			return false, expr
		}
	case *parser.CastExpr:
		switch t.Type.(type) {
		case *parser.DateColType, *parser.TimestampTZColType:
			// Casting to a Date or TimestampTZ involves the current timezone.
			v.err = errors.Errorf("context-dependent cast to %s not supported", t.Type)
			return false, expr
		}
	}
	return true, expr
}

func (v *distSQLExprCheckVisitor) VisitPost(expr parser.Expr) parser.Expr { return expr }

// checkExpr verifies that an expression doesn't contain things that are not yet
// supported by distSQL, like subqueries.
func checkExpr(expr parser.Expr) error {
	if expr == nil {
		return nil
	}
	v := distSQLExprCheckVisitor{}
	parser.WalkExprConst(&v, expr)
	return v.err
}

// CheckSupport looks at a planNode tree and decides if DistSQL is equipped to
// handle the query.
func (dsp *distSQLPlanner) CheckSupport(tree planNode) error {
	switch n := tree.(type) {
	case *selectTopNode:
		if n.group != nil {
			return errors.Errorf("grouping not supported yet")
		}
		if n.window != nil {
			return errors.Errorf("windows not supported yet")
		}
		if n.distinct != nil {
			return errors.Errorf("distinct not supported yet")
		}
		if n.limit != nil {
			return errors.Errorf("limit not supported yet")
		}
		return dsp.CheckSupport(n.source)

	case *selectNode:
		if n.filter != nil {
			// The Evaluator processors we use for select don't support filters yet.
			// This is easily fixed, but it will only matter when we support joins
			// (normally, all filters are pushed down to scanNodes).
			return errors.Errorf("filter not supported at select level yet")
		}
		for i, e := range n.render {
			if typ := n.columns[i].Typ; typ.FamilyEqual(parser.TypeTuple) ||
				typ.FamilyEqual(parser.TypeStringArray) ||
				typ.FamilyEqual(parser.TypeIntArray) {
				return errors.Errorf("unsupported render type %s", typ)
			}
			if err := checkExpr(e); err != nil {
				return err
			}
		}
		return dsp.CheckSupport(n.source.plan)

	case *scanNode:
		return checkExpr(n.filter)

	case *indexJoinNode:
		if err := dsp.CheckSupport(n.index); err != nil {
			return err
		}
		return dsp.CheckSupport(n.table)

	default:
		return errors.Errorf("unsupported node %T", tree)
	}
}

// planningCtx contains data used and updated throughout the planning process of
// a single query.
type planningCtx struct {
	ctx      context.Context
	spanIter *distsql.SpanResolverIterator
	// nodeAddresses contains addresses for all NodeIDs that are referenced by any
	// physicalPlan we generate with this context.
	nodeAddresses map[roachpb.NodeID]string
}

type processor struct {
	node roachpb.NodeID
	// the spec for the processor; note that the StreamEndpointSpecs in the input
	// synchronizers and output routers are not set until the end of the planning
	// process.
	spec distsql.ProcessorSpec
}

// processorIdx identifies a processor by its index in physicalPlan.processors.
type processorIdx int

type stream struct {
	sourceProcessor processorIdx
	destProcessor   processorIdx

	// destInput identifies the input (some processor have multiple inputs).
	destInput int
}

// physicalPlan is a partial physical plan which corresponds to a planNode
// (partial in that it can correspond to a planNode subtree and not necessarily
// to the entire planNode for a given query).
//
// It contains a network of processors along with information about the results
// of the plan and how they correspond to the columns in the planNode.
//
// These plans are built recursively on a planNode tree.
type physicalPlan struct {
	processors []processor

	// streams accumulates the streams - both local (intra-node) and remote
	// (inter-node) - in the plan; when we have a final plan, these are used to
	// generate processor input and output specs (see populateEndpoints).
	streams []stream

	// resultRouters identifies the output routers which output the results of the
	// plan. These are the routers to which we have to connect new streams in
	// order to extend the plan. All routers have the same "schema".
	//
	// We assume all processors have a single output so we only need the processor
	// index.
	resultRouters []processorIdx

	// planToStreamColMap maps planNode Columns() to columns in the result streams.
	// Note that in some cases, not all columns in the result streams are
	// referenced in the map (this is due to some processors not being
	// configurable to output only certain columns and will be fixed).
	//
	// Conversely, in some cases not all planNode columns have a corresponding
	// result stream column (these map to index -1); this is the case for scanNode
	// and indexJoinNode where not all columns in the table are used.
	planToStreamColMap []int

	// ordering guarantee for the result streams that must be maintained in order
	// to guarantee the same ordering as the corresponding planNode.
	//
	// TODO(radu): in general, guaranteeing an ordering is not free (requires
	// ordered synchronizers when merging streams). We should determine if (and to
	// what extent) the ordering of a planNode is actually used by its parent.
	ordering distsql.Ordering
}

// The distsql Expression uses the placeholder syntax (@1, @2, @3..) to
// refer to columns. We format the expression using an IndexedVar formatting
// interceptor. A columnMap can optionally be used to remap the indices.
func distSQLExpression(expr parser.TypedExpr, columnMap []int) distsql.Expression {
	if expr == nil {
		return distsql.Expression{}
	}
	var f parser.FmtFlags
	if columnMap == nil {
		f = parser.FmtIndexedVarFormat(
			func(buf *bytes.Buffer, _ parser.FmtFlags, _ parser.IndexedVarContainer, idx int) {
				fmt.Fprintf(buf, "@%d", idx+1)
			},
		)
	} else {
		f = parser.FmtIndexedVarFormat(
			func(buf *bytes.Buffer, _ parser.FmtFlags, _ parser.IndexedVarContainer, idx int) {
				remappedIdx := columnMap[idx]
				if remappedIdx < 0 {
					panic(fmt.Sprintf("unmapped index %d", idx))
				}
				fmt.Fprintf(buf, "@%d", remappedIdx+1)
			},
		)
	}
	var buf bytes.Buffer
	expr.Format(&buf, f)
	return distsql.Expression{Expr: buf.String()}
}

// spanPartition is the intersection between a set of spans for a certain
// operation (e.g table scan) and the set of ranges owned by a given node.
type spanPartition struct {
	node  roachpb.NodeID
	spans roachpb.Spans
}

// partitionSpans finds out which nodes are owners for ranges touching the given
// spans, and splits the spans according to owning nodes. The result is a set of
// spanPartitions (one for each relevant node), which form a partitioning of the
// spans (i.e. they are non-overlapping and their union is exactly the original
// set of spans).
func (dsp *distSQLPlanner) partitionSpans(
	planCtx *planningCtx, spans roachpb.Spans,
) ([]spanPartition, error) {
	if len(spans) == 0 {
		panic("no spans")
	}
	ctx := planCtx.ctx
	splits := make([]spanPartition, 0, 1)
	// nodeMap maps a nodeID to an index inside the splits array.
	nodeMap := make(map[roachpb.NodeID]int)
	it := planCtx.spanIter
	for _, span := range spans {
		var rspan roachpb.RSpan
		var err error
		if rspan.Key, err = keys.Addr(span.Key); err != nil {
			return nil, err
		}
		if rspan.EndKey, err = keys.Addr(span.EndKey); err != nil {
			return nil, err
		}

		var lastNodeID roachpb.NodeID
		for it.Seek(ctx, span, kv.Ascending); ; it.Next(ctx) {
			if !it.Valid() {
				return nil, it.Error()
			}
			replInfo, err := it.ReplicaInfo(ctx)
			if err != nil {
				return nil, err
			}
			desc := it.Desc()

			var trimmedSpan roachpb.Span
			if rspan.Key.Less(desc.StartKey) {
				trimmedSpan.Key = desc.StartKey.AsRawKey()
			} else {
				trimmedSpan.Key = span.Key
			}
			if desc.EndKey.Less(rspan.EndKey) {
				trimmedSpan.EndKey = desc.EndKey.AsRawKey()
			} else {
				trimmedSpan.EndKey = span.EndKey
			}

			nodeID := replInfo.NodeDesc.NodeID
			idx, ok := nodeMap[nodeID]
			if !ok {
				idx = len(splits)
				splits = append(splits, spanPartition{node: nodeID})
				nodeMap[nodeID] = idx
				if _, ok := planCtx.nodeAddresses[nodeID]; !ok {
					planCtx.nodeAddresses[nodeID] = replInfo.NodeDesc.Address.String()
				}
			}
			split := &splits[idx]

			if lastNodeID == nodeID {
				// Two consecutive ranges on the same node, merge the spans.
				if !split.spans[len(split.spans)-1].EndKey.Equal(trimmedSpan.Key) {
					log.Fatalf(ctx, "expected consecutive span pieces %v %v", split.spans, trimmedSpan)
				}
				split.spans[len(split.spans)-1].EndKey = trimmedSpan.EndKey
			} else {
				split.spans = append(split.spans, trimmedSpan)
			}

			lastNodeID = nodeID
			if !it.NeedAnother() {
				break
			}
		}
	}
	return splits, nil
}

// initTableReaderSpec initializes a TableReaderSpec that corresponds to a
// scanNode, except for the Spans and OutputColumns.
func initTableReaderSpec(n *scanNode) (distsql.TableReaderSpec, error) {
	s := distsql.TableReaderSpec{
		Table:   n.desc,
		Reverse: n.reverse,
	}
	if n.index != &n.desc.PrimaryIndex {
		for i := range n.desc.Indexes {
			if n.index == &n.desc.Indexes[i] {
				// IndexIdx is 1 based (0 means primary index).
				s.IndexIdx = uint32(i + 1)
				break
			}
		}
		if s.IndexIdx == 0 {
			err := errors.Errorf("invalid scanNode index %v (table %s)", n.index, n.desc.Name)
			return distsql.TableReaderSpec{}, err
		}
	}
	if n.limitSoft {
		s.SoftLimit = n.limitHint
	} else {
		s.HardLimit = n.limitHint
	}

	s.Filter = distSQLExpression(n.filter, nil)
	return s, nil
}

// getOutputColumnsFromScanNode returns the indices of the columns that are
// returned by a scanNode.
func getOutputColumnsFromScanNode(n *scanNode) []uint32 {
	num := 0
	for i := range n.resultColumns {
		if n.valNeededForCol[i] {
			num++
		}
	}
	outputColumns := make([]uint32, 0, num)
	for i := range n.resultColumns {
		// TODO(radu): if we have a scan with a filter, valNeededForCol will include
		// the columns needed for the filter, even if they aren't needed for the
		// next stage.
		if n.valNeededForCol[i] {
			outputColumns = append(outputColumns, uint32(i))
		}
	}
	return outputColumns
}

func (dsp *distSQLPlanner) convertOrdering(
	planOrdering sqlbase.ColumnOrdering, planToStreamColMap []int,
) distsql.Ordering {
	if len(planOrdering) == 0 {
		return distsql.Ordering{}
	}
	ordering := distsql.Ordering{
		Columns: make([]distsql.Ordering_Column, 0, len(planOrdering)),
	}
	for _, col := range planOrdering {
		streamColIdx := planToStreamColMap[col.ColIdx]
		if streamColIdx == -1 {
			// This column is not part of the output. The rest of the ordering is
			// irrelevant.
			break
		}
		oc := distsql.Ordering_Column{
			ColIdx:    uint32(streamColIdx),
			Direction: distsql.Ordering_Column_ASC,
		}
		if col.Direction == encoding.Descending {
			oc.Direction = distsql.Ordering_Column_DESC
		}
		ordering.Columns = append(ordering.Columns, oc)
	}
	return ordering
}

// createTableReaders generates a plan consisting of table reader processors,
// one for each node that has spans that we are reading.
// overrideResultColumns is optional.
func (dsp *distSQLPlanner) createTableReaders(
	planCtx *planningCtx, n *scanNode, overrideResultColumns []uint32,
) (physicalPlan, error) {
	spec, err := initTableReaderSpec(n)
	if err != nil {
		return physicalPlan{}, err
	}
	if overrideResultColumns != nil {
		spec.OutputColumns = overrideResultColumns
	} else {
		spec.OutputColumns = getOutputColumnsFromScanNode(n)
	}
	planToStreamColMap := make([]int, len(n.resultColumns))
	for i := range planToStreamColMap {
		planToStreamColMap[i] = -1
	}
	for i, col := range spec.OutputColumns {
		planToStreamColMap[col] = i
	}
	ordering := dsp.convertOrdering(n.ordering.ordering, planToStreamColMap)

	spans := n.spans
	if len(n.spans) == 0 {
		// If no spans were specified retrieve all of the keys that start with our
		// index key prefix.
		start := roachpb.Key(sqlbase.MakeIndexKeyPrefix(&n.desc, n.index.ID))
		spans = roachpb.Spans{{Key: start, EndKey: start.PrefixEnd()}}
	}

	spanPartitions, err := dsp.partitionSpans(planCtx, spans)
	if err != nil {
		return physicalPlan{}, err
	}
	var p physicalPlan
	pIdx := processorIdx(len(p.processors))
	for _, sp := range spanPartitions {
		proc := processor{
			node: sp.node,
		}

		tr := &distsql.TableReaderSpec{}
		*tr = spec
		tr.Spans = make([]distsql.TableReaderSpan, len(sp.spans))
		for i := range sp.spans {
			tr.Spans[i].Span = sp.spans[i]
		}

		proc.spec.Core.SetValue(tr)
		proc.spec.Output = make([]distsql.OutputRouterSpec, 1)
		proc.spec.Output[0].Type = distsql.OutputRouterSpec_MIRROR

		p.processors = append(p.processors, proc)
		p.resultRouters = append(p.resultRouters, pIdx)
		p.planToStreamColMap = planToStreamColMap
		p.ordering = ordering
		pIdx++
	}
	return p, nil
}

// addNoGroupingStage adds a processor for each result router, on the same node
// with the source of the stream; all processors have the same core. This is for
// stages that correspond to logical blocks that don't require any grouping
// (e.g. evaluator, sorting, etc).
func (dsp *distSQLPlanner) addNoGroupingStage(p *physicalPlan, core distsql.ProcessorCoreUnion) {
	pIdx := processorIdx(len(p.processors))
	for i, resultProc := range p.resultRouters {
		prevProc := &p.processors[resultProc]

		proc := processor{
			node: prevProc.node,
			spec: distsql.ProcessorSpec{
				Input: []distsql.InputSyncSpec{{
					Type: distsql.InputSyncSpec_UNORDERED,
				}},
				Core: core,
				Output: []distsql.OutputRouterSpec{{
					Type: distsql.OutputRouterSpec_MIRROR,
				}},
			},
		}

		p.streams = append(p.streams, stream{
			sourceProcessor: resultProc,
			destProcessor:   pIdx,
			destInput:       0,
		})

		p.resultRouters[i] = pIdx
		p.processors = append(p.processors, proc)
		pIdx++
	}
}

// selectRenders takes a physicalPlan that produces the results corresponding to
// the select data source (a n.source) and updates it to produce results
// corresponding to the select node itself. An evaluator stage is added if the
// select node has any expressions which are not just simple column references.
func (dsp *distSQLPlanner) selectRenders(
	planCtx *planningCtx, p *physicalPlan, n *selectNode,
) error {
	// First check if we need an Evaluator, or we are just returning values.
	needEval := false
	for _, e := range n.render {
		if _, ok := e.(*parser.IndexedVar); !ok {
			needEval = true
			break
		}
	}
	if !needEval {
		// We don't need an evaluator stage. However, we do need to update
		// p.planToStreamColMap to make the plan correspond to the selectNode
		// (rather than n.source).
		planToStreamColMap := make([]int, len(n.render))
		for i := range planToStreamColMap {
			planToStreamColMap[i] = -1
		}
		for i, e := range n.render {
			idx := e.(*parser.IndexedVar).Idx
			streamCol := p.planToStreamColMap[idx]
			if streamCol == -1 {
				panic(fmt.Sprintf("render %d refers to column %d not in source", i, idx))
			}
			planToStreamColMap[i] = streamCol
		}
		p.planToStreamColMap = planToStreamColMap
		p.ordering = dsp.convertOrdering(n.ordering.ordering, planToStreamColMap)
		return nil
	}
	// Add a stage with Evaluator processors.
	evalSpec := distsql.EvaluatorSpec{
		Exprs: make([]distsql.Expression, len(n.render)),
	}
	for i := range n.render {
		evalSpec.Exprs[i] = distSQLExpression(n.render[i], p.planToStreamColMap)
	}

	dsp.addNoGroupingStage(p, distsql.ProcessorCoreUnion{Evaluator: &evalSpec})

	// Update p.planToStreamColMap; we now have a simple 1-to-1 mapping of
	// planNode columns to stream columns because the evaluator has been
	// programmed to produce the columns in selectNode.render order.
	p.planToStreamColMap = p.planToStreamColMap[:0]
	for i := range n.render {
		p.planToStreamColMap = append(p.planToStreamColMap, i)
	}
	p.ordering = dsp.convertOrdering(n.ordering.ordering, p.planToStreamColMap)
	return nil
}

// addSorters adds sorters corresponding to a sortNode and updates the plan to
// reflect the sort node.
func (dsp *distSQLPlanner) addSorters(
	planCtx *planningCtx, p *physicalPlan, n *sortNode, sourceNode planNode,
) {
	sorterSpec := distsql.SorterSpec{
		OutputOrdering: dsp.convertOrdering(n.ordering, p.planToStreamColMap),
		OrderingMatchLen: uint32(computeOrderingMatch(
			n.ordering, sourceNode.Ordering(), false, /* reverse */
		)),
	}
	if len(sorterSpec.OutputOrdering.Columns) != len(n.ordering) {
		panic(fmt.Sprintf(
			"not all columns in sort ordering available: %v; %v",
			n.ordering, sorterSpec.OutputOrdering.Columns,
		))
	}
	dsp.addNoGroupingStage(p, distsql.ProcessorCoreUnion{Sorter: &sorterSpec})

	p.ordering = sorterSpec.OutputOrdering
	// In cases like:
	//   SELECT a FROM t ORDER BY b
	// we have columns (b) that are only used for sorting. These columns are not
	// in the output columns of the sortNode; we remove them from
	// planToStreamColMap.
	//
	// TODO(radu): these columns are not referenceable in the plan anymore, but
	// the values are still transferred in physical streams. We should improve the
	// sorter processor to be configurable with a projection so it only outputs
	// the columns that are needed.
	p.planToStreamColMap = p.planToStreamColMap[:len(n.columns)]
}

// addSingleGroupStage adds a "single group" stage (one that cannot be
// parallelized) which consists of a single processor on the specified node.
func (dsp *distSQLPlanner) addSingleGroupStage(
	p *physicalPlan, nodeID roachpb.NodeID, core distsql.ProcessorCoreUnion,
) {
	proc := processor{
		node: nodeID,
		spec: distsql.ProcessorSpec{
			Input: []distsql.InputSyncSpec{{}},
			Core:  core,
			Output: []distsql.OutputRouterSpec{{
				Type: distsql.OutputRouterSpec_MIRROR,
			}},
		},
	}

	// Add the processor.
	pIdx := processorIdx(len(p.processors))
	p.processors = append(p.processors, proc)

	// Connect the result routers to the no-op processor.
	dsp.mergeResultStreams(p, p.resultRouters, p.ordering, pIdx, 0)

	// We now have a single result stream.
	p.resultRouters = p.resultRouters[:1]
	p.resultRouters[0] = pIdx
}

func (dsp *distSQLPlanner) createPlanForIndexJoin(
	planCtx *planningCtx, n *indexJoinNode,
) (physicalPlan, error) {
	priCols := make([]uint32, len(n.index.desc.PrimaryIndex.ColumnIDs))

ColLoop:
	for i, colID := range n.index.desc.PrimaryIndex.ColumnIDs {
		for j, c := range n.index.desc.Columns {
			if c.ID == colID {
				priCols[i] = uint32(j)
				continue ColLoop
			}
		}
		panic(fmt.Sprintf("PK column %d not found in index", colID))
	}

	plan, err := dsp.createTableReaders(planCtx, n.index, priCols)
	if err != nil {
		return physicalPlan{}, err
	}

	joinReaderSpec := distsql.JoinReaderSpec{
		Table:         n.index.desc,
		IndexIdx:      0,
		Filter:        distSQLExpression(n.table.filter, nil),
		OutputColumns: getOutputColumnsFromScanNode(n.table),
	}

	// TODO(radu): we currently use a single JoinReader. We could have multiple.
	// Note that in that case, if the index ordering is used, we must make sure
	// that the index columns are part of the output (so that ordered
	// synchronizers down the road can maintain the order).

	dsp.addSingleGroupStage(
		&plan, dsp.nodeDesc.NodeID, distsql.ProcessorCoreUnion{JoinReader: &joinReaderSpec},
	)
	// Recalculate planToStreamColMap: it now maps to columns in the JoinReader's
	// output stream.
	for i := range plan.planToStreamColMap {
		plan.planToStreamColMap[i] = -1
	}
	for i, col := range joinReaderSpec.OutputColumns {
		plan.planToStreamColMap[col] = i
	}
	return plan, nil
}

func (dsp *distSQLPlanner) createPlanForNode(
	planCtx *planningCtx, node planNode,
) (physicalPlan, error) {
	switch n := node.(type) {
	case *scanNode:
		return dsp.createTableReaders(planCtx, n, nil)

	case *indexJoinNode:
		return dsp.createPlanForIndexJoin(planCtx, n)

	case *selectNode:
		plan, err := dsp.createPlanForNode(planCtx, n.source.plan)
		if err != nil {
			return physicalPlan{}, err
		}
		if err := dsp.selectRenders(planCtx, &plan, n); err != nil {
			return physicalPlan{}, err
		}
		return plan, nil

	case *selectTopNode:
		plan, err := dsp.createPlanForNode(planCtx, n.source)
		if err != nil {
			return physicalPlan{}, err
		}
		if n.sort != nil {
			dsp.addSorters(planCtx, &plan, n.sort, n.source)
		}
		return plan, nil

	default:
		panic(fmt.Sprintf("unsupported node type %T", n))
	}
}

// mergeResultStreams connects a set of resultRouters to a synchronizer. The
// synchronizer is configured with the provided ordering.
func (dsp *distSQLPlanner) mergeResultStreams(
	p *physicalPlan,
	resultRouters []processorIdx,
	ordering distsql.Ordering,
	destProcessor processorIdx,
	destInput int,
) {
	proc := &p.processors[destProcessor]
	if len(ordering.Columns) == 0 || len(resultRouters) == 1 {
		proc.spec.Input[destInput].Type = distsql.InputSyncSpec_UNORDERED
	} else {
		proc.spec.Input[destInput].Type = distsql.InputSyncSpec_ORDERED
		proc.spec.Input[destInput].Ordering = ordering
	}

	for _, resultProc := range resultRouters {
		p.streams = append(p.streams, stream{
			sourceProcessor: resultProc,
			destProcessor:   destProcessor,
			destInput:       destInput,
		})
	}
}

// populateEndpoints processes p.streams and adds the corresponding
// StreamEndpointSpects to the processors' input and output specs.
func (dsp *distSQLPlanner) populateEndpoints(planCtx *planningCtx, p *physicalPlan) {
	// Note: we could fill in the input/output specs directly instead of adding
	// streams to p.streams, but this makes the rest of the code a bit simpler.
	for sIdx, s := range p.streams {
		p1 := &p.processors[s.sourceProcessor]
		p2 := &p.processors[s.destProcessor]
		endpoint := distsql.StreamEndpointSpec{StreamID: distsql.StreamID(sIdx)}
		if p1.node == p2.node {
			endpoint.Type = distsql.StreamEndpointSpec_LOCAL
		} else {
			endpoint.Type = distsql.StreamEndpointSpec_REMOTE
		}
		p2.spec.Input[s.destInput].Streams = append(p2.spec.Input[s.destInput].Streams, endpoint)
		if endpoint.Type == distsql.StreamEndpointSpec_REMOTE {
			endpoint.TargetAddr = planCtx.nodeAddresses[p2.node]
		}
		p1.spec.Output[0].Streams = append(p1.spec.Output[0].Streams, endpoint)
	}
}

// PlanAndRun generates a physical plan from a planNode tree and executes it. It
// assumes that the tree is supported (see CheckSupport).
//
// Note that errors that happen while actually running the flow are reported to
// recv, not returned by this function.
func (dsp *distSQLPlanner) PlanAndRun(
	ctx context.Context, txn *client.Txn, tree planNode, recv *distSQLReceiver,
) error {
	// Trigger limit propagation.
	tree.SetLimitHint(math.MaxInt64, true)

	planCtx := planningCtx{
		ctx:           ctx,
		spanIter:      dsp.spanResolver.NewSpanResolverIterator(),
		nodeAddresses: make(map[roachpb.NodeID]string),
	}
	thisNodeID := dsp.nodeDesc.NodeID
	planCtx.nodeAddresses[thisNodeID] = dsp.nodeDesc.Address.String()

	plan, err := dsp.createPlanForNode(&planCtx, tree)
	if err != nil {
		return err
	}

	// If we don't already have a single result router on this node, add a final
	// stage.
	if len(plan.resultRouters) != 1 ||
		plan.processors[plan.resultRouters[0]].node != thisNodeID {
		dsp.addSingleGroupStage(
			&plan, thisNodeID, distsql.ProcessorCoreUnion{Noop: &distsql.NoopCoreSpec{}},
		)
		if len(plan.resultRouters) != 1 {
			panic(fmt.Sprintf("%d results after single group stage", len(plan.resultRouters)))
		}
	}

	// Set up the endpoints for p.streams.
	dsp.populateEndpoints(&planCtx, &plan)

	// Set up the endpoint for the final result.
	finalOut := &plan.processors[plan.resultRouters[0]].spec.Output[0]
	finalOut.Streams = append(finalOut.Streams, distsql.StreamEndpointSpec{
		Type: distsql.StreamEndpointSpec_SYNC_RESPONSE,
	})

	recv.resultToStreamColMap = plan.planToStreamColMap

	// Split the processors by nodeID to create the FlowSpecs.
	flowID := distsql.FlowID{UUID: uuid.MakeV4()}
	nodeIDMap := make(map[roachpb.NodeID]int)
	// nodeAddresses contains addresses for the nodes that were referenced during
	// planning, so we're likely going to have this many nodes (and we have one
	// flow per node).
	nodeIDs := make([]roachpb.NodeID, 0, len(planCtx.nodeAddresses))
	flows := make([]distsql.FlowSpec, 0, len(planCtx.nodeAddresses))

	for _, p := range plan.processors {
		idx, ok := nodeIDMap[p.node]
		if !ok {
			flow := distsql.FlowSpec{FlowID: flowID}
			idx = len(flows)
			flows = append(flows, flow)
			nodeIDs = append(nodeIDs, p.node)
			nodeIDMap[p.node] = idx
		}
		flows[idx].Processors = append(flows[idx].Processors, p.spec)
	}

	// Start the flows on all other nodes.
	for i, nodeID := range nodeIDs {
		if nodeID == thisNodeID {
			// Skip this node.
			continue
		}
		req := distsql.SetupFlowRequest{
			Txn:  txn.Proto,
			Flow: flows[i],
		}
		if err := distsql.SetFlowRequestTrace(ctx, &req); err != nil {
			return err
		}
		conn, err := dsp.rpcContext.GRPCDial(planCtx.nodeAddresses[nodeID])
		if err != nil {
			return err
		}
		client := distsql.NewDistSQLClient(conn)
		// TODO(radu): we are not waiting for the flows to complete, but we are
		// still waiting for a round trip; we should start the flows in parallel, at
		// least if there are enough of them.
		if resp, err := client.SetupFlow(context.Background(), &req); err != nil {
			return err
		} else if resp.Error != nil {
			return resp.Error.GoError()
		}
	}
	localReq := distsql.SetupFlowRequest{
		Txn:  txn.Proto,
		Flow: flows[nodeIDMap[thisNodeID]],
	}
	if err := distsql.SetFlowRequestTrace(ctx, &localReq); err != nil {
		return err
	}
	flow, err := dsp.distSQLSrv.SetupSyncFlow(ctx, &localReq, recv)
	if err != nil {
		return err
	}
	// TODO(radu): this should go through the flow scheduler.
	flow.Start(func() {})
	flow.Wait()
	flow.Cleanup()

	return nil
}

type distSQLReceiver struct {
	// rows is the container where we store the results; if we only need the count
	// of the rows, it is nil.
	rows *RowContainer
	// resultToStreamColMap maps result columns to columns in the distsql results
	// stream.
	resultToStreamColMap []int
	// numRows counts the number of rows we received when rows is nil.
	numRows int64
	err     error
	row     parser.DTuple
	alloc   sqlbase.DatumAlloc
	closed  bool
}

var _ distsql.RowReceiver = &distSQLReceiver{}

// PushRow is part of the RowReceiver interface.
func (r *distSQLReceiver) PushRow(row sqlbase.EncDatumRow) bool {
	if r.err != nil {
		return false
	}
	if r.rows == nil {
		r.numRows++
		return true
	}
	if r.row == nil {
		r.row = make(parser.DTuple, len(r.resultToStreamColMap))
	}
	for i, resIdx := range r.resultToStreamColMap {
		err := row[resIdx].Decode(&r.alloc)
		if err != nil {
			r.err = err
			return false
		}
		r.row[i] = row[resIdx].Datum
	}
	// Note that AddRow accounts for the memory used by the Datums.
	if _, err := r.rows.AddRow(r.row); err != nil {
		r.err = err
		return false
	}
	return true
}

// Close is part of the RowReceiver interface.
func (r *distSQLReceiver) Close(err error) {
	if r.closed {
		panic("double close")
	}
	r.closed = true
	if r.err == nil {
		r.err = err
	}
}
