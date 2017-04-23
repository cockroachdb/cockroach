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
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package sql

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// distSQLPlanner implements distSQL physical planning and running logic.
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
	// The node descriptor for the gateway node that initiated this query.
	nodeDesc     roachpb.NodeDescriptor
	rpcContext   *rpc.Context
	stopper      *stop.Stopper
	distSQLSrv   *distsqlrun.ServerImpl
	spanResolver distsqlplan.SpanResolver

	// runnerChan is used to send out requests (for running SetupFlow RPCs) to a
	// pool of workers.
	runnerChan chan runnerRequest
}

const resolverPolicy = distsqlplan.BinPackingLeaseHolderChoice

// If true, the plan diagram (in JSON) is logged for each plan (used for
// debugging).
var logPlanDiagram = envutil.EnvOrDefaultBool("COCKROACH_DISTSQL_LOG_PLAN", false)

// If true, for index joins  we instantiate a join reader on every node that
// has a stream (usually from a table reader). If false, there is a single join
// reader.
var distributeIndexJoin = envutil.EnvOrDefaultBool("COCKROACH_DISTSQL_DISTRIBUTE_INDEX_JOIN", true)

func newDistSQLPlanner(
	nodeDesc roachpb.NodeDescriptor,
	rpcCtx *rpc.Context,
	distSQLSrv *distsqlrun.ServerImpl,
	distSender *kv.DistSender,
	gossip *gossip.Gossip,
	stopper *stop.Stopper,
) *distSQLPlanner {
	dsp := &distSQLPlanner{
		nodeDesc:     nodeDesc,
		rpcContext:   rpcCtx,
		stopper:      stopper,
		distSQLSrv:   distSQLSrv,
		spanResolver: distsqlplan.NewSpanResolver(distSender, gossip, nodeDesc, resolverPolicy),
	}
	dsp.initRunners()
	return dsp
}

// setSpanResolver switches to a different SpanResolver. It is the caller's
// responsibility to make sure the distSQLPlanner is not in use.
func (dsp *distSQLPlanner) setSpanResolver(spanResolver distsqlplan.SpanResolver) {
	dsp.spanResolver = spanResolver
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
func (dsp *distSQLPlanner) checkExpr(expr parser.Expr) error {
	if expr == nil {
		return nil
	}
	v := distSQLExprCheckVisitor{}
	parser.WalkExprConst(&v, expr)
	return v.err
}

// CheckSupport looks at a planNode tree and decides:
//  - whether DistSQL is equipped to handle the query (if not, an error is
//    returned).
//  - whether it is recommended that the query be run with DistSQL.
func (dsp *distSQLPlanner) CheckSupport(node planNode) (bool, error) {
	rec, err := dsp.checkSupportForNode(node)
	if err != nil {
		return false, err
	}
	return (rec == shouldDistribute), nil
}

type distRecommendation int

const (
	// shouldNotDistribute indicates that a plan could suffer if run
	// under DistSQL
	shouldNotDistribute distRecommendation = iota

	// canDistribute indicates that a plan will probably not benefit but will
	// probably not suffer if run under DistSQL.
	canDistribute

	// shouldDistribute indicates that a plan will likely benefit if run under
	// DistSQL.
	shouldDistribute
)

// compose returns the recommendation for a plan given recommendations for two
// parts of it: if we shouldNotDistribute either part, then we
// shouldNotDistribute the overall plan either.
func (a distRecommendation) compose(b distRecommendation) distRecommendation {
	if a == shouldNotDistribute || b == shouldNotDistribute {
		return shouldNotDistribute
	}
	if a == shouldDistribute || b == shouldDistribute {
		return shouldDistribute
	}
	return canDistribute
}

// checkSupportForNode returns a distRecommendation (as described above) or an
// error if the plan subtree is not supported by DistSQL.
// TODO(radu): add tests for this.
func (dsp *distSQLPlanner) checkSupportForNode(node planNode) (distRecommendation, error) {
	switch n := node.(type) {
	case *filterNode:
		if err := dsp.checkExpr(n.filter); err != nil {
			return 0, err
		}
		return dsp.checkSupportForNode(n.source.plan)

	case *renderNode:
		for i, e := range n.render {
			if typ := n.columns[i].Typ; typ.FamilyEqual(parser.TypeTuple) ||
				typ.FamilyEqual(parser.TypeStringArray) ||
				typ.FamilyEqual(parser.TypeIntArray) {
				return 0, errors.Errorf("unsupported render type %s", typ)
			}
			if err := dsp.checkExpr(e); err != nil {
				return 0, err
			}
		}
		return dsp.checkSupportForNode(n.source.plan)

	case *sortNode:
		rec, err := dsp.checkSupportForNode(n.plan)
		if err != nil {
			return 0, err
		}
		// If we have to sort, distribute the query.
		if n.needSort {
			rec = rec.compose(shouldDistribute)
		}
		return rec, nil

	case *joinNode:
		if n.joinType != joinTypeInner {
			return 0, errors.Errorf("only inner join supported")
		}
		if err := dsp.checkExpr(n.pred.onCond); err != nil {
			return 0, err
		}
		recLeft, err := dsp.checkSupportForNode(n.left.plan)
		if err != nil {
			return 0, err
		}
		recRight, err := dsp.checkSupportForNode(n.right.plan)
		if err != nil {
			return 0, err
		}
		// If either the left or the right side can benefit from distribution, we
		// should distribute.
		rec := recLeft.compose(recRight)
		// If we can do a hash join, we distribute if possible.
		if len(n.pred.leftEqualityIndices) > 0 {
			rec = rec.compose(shouldDistribute)
		}
		return rec, nil

	case *scanNode:
		rec := canDistribute
		if n.hardLimit != 0 || n.softLimit != 0 {
			// We don't yet recommend distributing plans where limits propagate
			// to scan nodes; we don't have infrastructure to only plan for a few
			// ranges at a time.
			rec = shouldNotDistribute
		}
		// We recommend running scans distributed if we have a filtering
		// expression or if we have a full table scan.
		if n.filter != nil {
			if err := dsp.checkExpr(n.filter); err != nil {
				return 0, err
			}
			rec = rec.compose(shouldDistribute)
		}
		// Check if we are doing a full scan.
		if len(n.spans) == 1 && n.spans[0].Equal(n.desc.IndexSpan(n.index.ID)) {
			rec = rec.compose(shouldDistribute)
		}
		return rec, nil

	case *indexJoinNode:
		// n.table doesn't have meaningful spans, but we need to check support (e.g.
		// for any filtering expression).
		if _, err := dsp.checkSupportForNode(n.table); err != nil {
			return 0, err
		}
		return dsp.checkSupportForNode(n.index)

	case *groupNode:
		if n.having != nil {
			return 0, errors.Errorf("group with having not supported yet")
		}
		for _, fholder := range n.funcs {
			if fholder.filter != nil {
				return 0, errors.Errorf("aggregation with FILTER not supported yet")
			}
			if f, ok := fholder.expr.(*parser.FuncExpr); ok {
				if strings.ToUpper(f.Func.FunctionReference.String()) == "ARRAY_AGG" {
					return 0, errors.Errorf("ARRAY_AGG aggregation not supported yet")
				}
			}
		}
		rec, err := dsp.checkSupportForNode(n.plan)
		if err != nil {
			return 0, err
		}
		// Distribute aggregations if possible.
		return rec.compose(shouldDistribute), nil

	case *limitNode:
		if err := dsp.checkExpr(n.countExpr); err != nil {
			return 0, err
		}
		if err := dsp.checkExpr(n.offsetExpr); err != nil {
			return 0, err
		}
		return dsp.checkSupportForNode(n.plan)

	case *distinctNode:
		return dsp.checkSupportForNode(n.plan)

	default:
		return 0, errors.Errorf("unsupported node %T", node)
	}
}

// planningCtx contains data used and updated throughout the planning process of
// a single query.
type planningCtx struct {
	ctx      context.Context
	spanIter distsqlplan.SpanResolverIterator
	// nodeAddresses contains addresses for all NodeIDs that are referenced by any
	// physicalPlan we generate with this context.
	nodeAddresses map[roachpb.NodeID]string
}

// physicalPlan is a partial physical plan which corresponds to a planNode
// (partial in that it can correspond to a planNode subtree and not necessarily
// to the entire planNode for a given query).
//
// It augments distsqlplan.PhysicalPlan with information relating the physical
// plan to a planNode subtree.
//
// These plans are built recursively on a planNode tree.
type physicalPlan struct {
	distsqlplan.PhysicalPlan

	// planToStreamColMap maps planNode Columns() to columns in the result
	// streams. Note that in some cases, not all columns in the result streams
	// are referenced in the map (this is due to some processors not being
	// configurable to output only certain columns and will be fixed).
	//
	// Conversely, in some cases not all planNode columns have a corresponding
	// result stream column (these map to index -1); this is the case for scanNode
	// and indexJoinNode where not all columns in the table are actually used in
	// the plan.
	planToStreamColMap []int
}

// orderingTerminated is used when
var orderingTerminated = distsqlrun.Ordering{}

// makePlanToStreamColMap initializes a new physicalPlan.planToStreamColMap. The
// columns that are present in the result stream(s) should be set in the map.
func makePlanToStreamColMap(numCols int) []int {
	m := make([]int, numCols)
	for i := 0; i < numCols; i++ {
		m[i] = -1
	}
	return m
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
		// lastKey maintains the EndKey of the last piece of `span`.
		lastKey := rspan.Key
		for it.Seek(ctx, span, kv.Ascending); ; it.Next(ctx) {
			if !it.Valid() {
				return nil, it.Error()
			}
			replInfo, err := it.ReplicaInfo(ctx)
			if err != nil {
				return nil, err
			}
			desc := it.Desc()

			if !desc.ContainsKey(lastKey) {
				// This range must contain the last range's EndKey.
				log.Fatalf(ctx, "next range doesn't cover last end key: %#v %v", splits, desc.RSpan())
			}

			// Limit the end key to the end of the span we are resolving.
			endKey := desc.EndKey
			if rspan.EndKey.Less(endKey) {
				endKey = rspan.EndKey
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
				split.spans[len(split.spans)-1].EndKey = endKey.AsRawKey()
			} else {
				split.spans = append(split.spans, roachpb.Span{
					Key:    lastKey.AsRawKey(),
					EndKey: endKey.AsRawKey(),
				})
			}

			if !endKey.Less(rspan.EndKey) {
				// Done.
				break
			}

			lastKey = endKey
			lastNodeID = nodeID
		}
	}
	return splits, nil
}

// initTableReaderSpec initializes a TableReaderSpec/PostProcessSpec that
// corresponds to a scanNode, except for the Spans and OutputColumns.
func initTableReaderSpec(
	n *scanNode,
) (distsqlrun.TableReaderSpec, distsqlrun.PostProcessSpec, error) {
	s := distsqlrun.TableReaderSpec{
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
			return distsqlrun.TableReaderSpec{}, distsqlrun.PostProcessSpec{}, err
		}
	}

	post := distsqlrun.PostProcessSpec{
		Filter: distsqlplan.MakeExpression(n.filter, nil),
	}

	if n.hardLimit != 0 {
		post.Limit = uint64(n.hardLimit)
	} else if n.softLimit != 0 {
		s.LimitHint = n.softLimit
	}
	return s, post, nil
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
) distsqlrun.Ordering {
	if len(planOrdering) == 0 {
		return distsqlrun.Ordering{}
	}
	ordering := distsqlrun.Ordering{
		Columns: make([]distsqlrun.Ordering_Column, 0, len(planOrdering)),
	}
	for _, col := range planOrdering {
		streamColIdx := planToStreamColMap[col.ColIdx]
		if streamColIdx == -1 {
			panic("column in ordering not part of processor output")
		}
		oc := distsqlrun.Ordering_Column{
			ColIdx:    uint32(streamColIdx),
			Direction: distsqlrun.Ordering_Column_ASC,
		}
		if col.Direction == encoding.Descending {
			oc.Direction = distsqlrun.Ordering_Column_DESC
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
	spec, post, err := initTableReaderSpec(n)
	if err != nil {
		return physicalPlan{}, err
	}

	spanPartitions, err := dsp.partitionSpans(planCtx, n.spans)
	if err != nil {
		return physicalPlan{}, err
	}

	var p physicalPlan

	for _, sp := range spanPartitions {
		tr := &distsqlrun.TableReaderSpec{}
		*tr = spec
		tr.Spans = make([]distsqlrun.TableReaderSpan, len(sp.spans))
		for i := range sp.spans {
			tr.Spans[i].Span = sp.spans[i]
		}

		proc := distsqlplan.Processor{
			Node: sp.node,
			Spec: distsqlrun.ProcessorSpec{
				Core:   distsqlrun.ProcessorCoreUnion{TableReader: tr},
				Output: []distsqlrun.OutputRouterSpec{{Type: distsqlrun.OutputRouterSpec_PASS_THROUGH}},
			},
		}

		pIdx := p.AddProcessor(proc)
		p.ResultRouters = append(p.ResultRouters, pIdx)
	}

	if overrideResultColumns != nil {
		post.OutputColumns = overrideResultColumns
	} else {
		post.OutputColumns = getOutputColumnsFromScanNode(n)
	}

	planToStreamColMap := makePlanToStreamColMap(len(n.resultColumns))
	for i, col := range post.OutputColumns {
		planToStreamColMap[col] = i
	}
	if len(p.ResultRouters) > 1 && len(n.ordering.ordering) > 0 {
		// We have to maintain a certain ordering between the parallel streams. This
		// might mean we need to add output columns.
		for _, col := range n.ordering.ordering {
			if planToStreamColMap[col.ColIdx] == -1 {
				// This column is not part of the output; add it.
				planToStreamColMap[col.ColIdx] = len(post.OutputColumns)
				post.OutputColumns = append(post.OutputColumns, uint32(col.ColIdx))
			}
		}
		p.SetMergeOrdering(dsp.convertOrdering(n.ordering.ordering, planToStreamColMap))
	}
	p.SetLastStagePost(post, getTypesForPlanResult(n, planToStreamColMap))
	p.planToStreamColMap = planToStreamColMap
	return p, nil
}

func initBackfillerSpec(
	backfillType backfillType,
	desc sqlbase.TableDescriptor,
	duration time.Duration,
	chunkSize int64,
	otherTables []sqlbase.TableDescriptor,
) (distsqlrun.BackfillerSpec, error) {
	ret := distsqlrun.BackfillerSpec{
		Table:       desc,
		Duration:    duration,
		ChunkSize:   chunkSize,
		OtherTables: otherTables,
	}
	switch backfillType {
	case indexBackfill:
		ret.Type = distsqlrun.BackfillerSpec_Index
	case columnBackfill:
		ret.Type = distsqlrun.BackfillerSpec_Column
	default:
		return distsqlrun.BackfillerSpec{}, errors.Errorf("bad backfill type %d", backfillType)
	}
	return ret, nil
}

// CreateBackfiller generates a plan consisting of index/column backfiller
// processors, one for each node that has spans that we are reading. The plan is
// finalized.
func (dsp *distSQLPlanner) CreateBackfiller(
	planCtx *planningCtx,
	backfillType backfillType,
	desc sqlbase.TableDescriptor,
	duration time.Duration,
	chunkSize int64,
	spans []roachpb.Span,
	otherTables []sqlbase.TableDescriptor,
) (physicalPlan, error) {
	spec, err := initBackfillerSpec(backfillType, desc, duration, chunkSize, otherTables)
	if err != nil {
		return physicalPlan{}, err
	}

	spanPartitions, err := dsp.partitionSpans(planCtx, spans)
	if err != nil {
		return physicalPlan{}, err
	}

	p := physicalPlan{}
	for _, sp := range spanPartitions {
		ib := &distsqlrun.BackfillerSpec{}
		*ib = spec
		ib.Spans = make([]distsqlrun.TableReaderSpan, len(sp.spans))
		for i := range sp.spans {
			ib.Spans[i].Span = sp.spans[i]
		}

		proc := distsqlplan.Processor{
			Node: sp.node,
			Spec: distsqlrun.ProcessorSpec{
				Core:   distsqlrun.ProcessorCoreUnion{Backfiller: ib},
				Output: []distsqlrun.OutputRouterSpec{{Type: distsqlrun.OutputRouterSpec_PASS_THROUGH}},
			},
		}

		pIdx := p.AddProcessor(proc)
		p.ResultRouters = append(p.ResultRouters, pIdx)
	}
	dsp.FinalizePlan(planCtx, &p)
	return p, nil
}

// selectRenders takes a physicalPlan that produces the results corresponding to
// the select data source (a n.source) and updates it to produce results
// corresponding to the render node itself. An evaluator stage is added if the
// render node has any expressions which are not just simple column references.
func (dsp *distSQLPlanner) selectRenders(p *physicalPlan, n *renderNode) {
	p.AddRendering(n.render, p.planToStreamColMap, getTypesForPlanResult(n, nil))

	// Update p.planToStreamColMap; we will have a simple 1-to-1 mapping of
	// planNode columns to stream columns because the evaluator has been
	// programmed to produce the columns in renderNode.render order.
	p.planToStreamColMap = p.planToStreamColMap[:0]
	for i := range n.render {
		p.planToStreamColMap = append(p.planToStreamColMap, i)
	}
}

// addSorters adds sorters corresponding to a sortNode and updates the plan to
// reflect the sort node.
func (dsp *distSQLPlanner) addSorters(p *physicalPlan, n *sortNode) {

	matchLen := n.plan.Ordering().computeMatch(n.ordering)

	if matchLen < len(n.ordering) {
		// Sorting is needed; we add a stage of sorting processors.
		ordering := dsp.convertOrdering(n.ordering, p.planToStreamColMap)
		if len(ordering.Columns) != len(n.ordering) {
			panic(fmt.Sprintf(
				"not all columns in sort ordering available: %v; %v", n.ordering, ordering.Columns,
			))
		}
		p.AddNoGroupingStage(
			distsqlrun.ProcessorCoreUnion{
				Sorter: &distsqlrun.SorterSpec{
					OutputOrdering:   ordering,
					OrderingMatchLen: uint32(matchLen),
				},
			},
			distsqlrun.PostProcessSpec{},
			p.ResultTypes,
			ordering,
		)
	}

	if len(n.columns) != len(p.planToStreamColMap) {
		// In cases like:
		//   SELECT a FROM t ORDER BY b
		// we have columns (b) that are only used for sorting. These columns are not
		// in the output columns of the sortNode; we set a projection such that the
		// plan results map 1-to-1 to sortNode columns.
		//
		// Note that internally, AddProjection might retain more columns as
		// necessary so we can preserve the p.Ordering between parallel streams when
		// they merge later.
		p.planToStreamColMap = p.planToStreamColMap[:len(n.columns)]
		columns := make([]uint32, len(n.columns))
		for i, col := range p.planToStreamColMap {
			columns[i] = uint32(col)
			p.planToStreamColMap[i] = i
		}
		p.AddProjection(columns)
	}
}

// addAggregators adds aggregators corresponding to a groupNode and updates the plan to
// reflect the groupNode. An evaluator stage is added if necessary.
// Invariants assumed:
//  - There is strictly no "pre-evaluation" necessary. If the given query is
//  'SELECT COUNT(k), v + w FROM kv GROUP BY v + w', the evaluation of the first
//  'v + w' is done at the source of the groupNode.
//  - We only operate on the following expressions:
//      - ONLY aggregation functions, with arguments pre-evaluated. So for
//        COUNT(k + v), we assume a stream of evaluated 'k + v' values.
//      - Expressions that CONTAIN an aggregation function, e.g. 'COUNT(k) + 1'.
//        This is evaluated the post aggregation evaluator attached after.
//      - Expressions that also appear verbatim in the GROUP BY expressions.
//        For 'SELECT k GROUP BY k', the aggregation function added is IDENT,
//        therefore k just passes through unchanged.
//    All other expressions simply pass through unchanged, for e.g. '1' in
//    'SELECT 1 GROUP BY k'.
func (dsp *distSQLPlanner) addAggregators(
	planCtx *planningCtx, p *physicalPlan, n *groupNode,
) error {
	aggregations, err := dsp.extractAggExprs(n.render)
	if err != nil {
		return err
	}
	for i := range aggregations {
		aggregations[i].ColIdx = uint32(p.planToStreamColMap[i])
	}

	inputTypes := p.ResultTypes

	groupCols := make([]uint32, len(n.groupByIdx))
	for i, idx := range n.groupByIdx {
		groupCols[i] = uint32(p.planToStreamColMap[idx])
	}

	// We either have a local stage on each stream followed by a final stage, or
	// just a final stage. We only use a local stage if:
	//  - the previous stage is distributed on multiple nodes, and
	//  - all aggregation functions support it. TODO(radu): we could relax this by
	//    splitting the aggregation into two different paths and joining on the
	//    results.
	//  - we have a mix of aggregations that use distinct and aggregations that
	//    don't use distinct. TODO(arjun): This would require doing the same as
	//    the todo as above.
	multiStage := false
	allDistinct := true
	anyDistinct := false

	// Check if the previous stage is all on one node.
	prevStageNode := p.Processors[p.ResultRouters[0]].Node
	for i := 1; i < len(p.ResultRouters); i++ {
		if n := p.Processors[p.ResultRouters[i]].Node; n != prevStageNode {
			prevStageNode = 0
			break
		}
	}

	if prevStageNode == 0 {
		// Check that all aggregation functions support a local stage.
		multiStage = true
		for _, e := range aggregations {
			if e.Distinct {
				// We can't do local aggregation for functions with distinct.
				multiStage = false
				anyDistinct = true
			} else {
				// We can't do local distinct if we have a mix of distinct and
				// non-distinct aggregations.
				allDistinct = false
			}
			if _, ok := distsqlplan.DistAggregationTable[e.Func]; !ok {
				multiStage = false
				break
			}
		}
	}
	if !anyDistinct {
		allDistinct = false
	}

	var finalAggSpec distsqlrun.AggregatorSpec
	var finalAggPost distsqlrun.PostProcessSpec

	if !multiStage && allDistinct {
		// We can't do local aggregation, but we can do local distinct processing
		// to reduce streaming duplicates, and aggregate on the final node.

		ordering := dsp.convertOrdering(n.plan.Ordering().ordering, p.planToStreamColMap).Columns
		orderedColsMap := make(map[uint32]struct{})
		for _, ord := range ordering {
			orderedColsMap[ord.ColIdx] = struct{}{}
		}
		distinctColsMap := make(map[uint32]struct{})
		for _, agg := range aggregations {
			distinctColsMap[agg.ColIdx] = struct{}{}
		}
		orderedColumns := make([]uint32, 0, len(orderedColsMap))
		for o := range orderedColsMap {
			orderedColumns = append(orderedColumns, o)
		}
		distinctColumns := make([]uint32, 0, len(distinctColsMap))
		for o := range distinctColsMap {
			distinctColumns = append(distinctColumns, o)
		}

		sort.Slice(orderedColumns, func(i, j int) bool { return orderedColumns[i] < orderedColumns[j] })
		sort.Slice(distinctColumns, func(i, j int) bool { return distinctColumns[i] < distinctColumns[j] })

		distinctSpec := distsqlrun.ProcessorCoreUnion{
			Distinct: &distsqlrun.DistinctSpec{
				OrderedColumns:  orderedColumns,
				DistinctColumns: distinctColumns,
			},
		}

		// Add distinct processors local to each existing current result processor.
		p.AddNoGroupingStage(distinctSpec, distsqlrun.PostProcessSpec{}, p.ResultTypes, p.MergeOrdering)
	}

	if !multiStage {
		finalAggSpec = distsqlrun.AggregatorSpec{
			Aggregations: aggregations,
			GroupCols:    groupCols,
		}
	} else {
		// Some aggregations might need multiple aggregation as part of their local
		// and final stages (along with a final render expression to combine the
		// multiple aggregations into a single result).
		//
		// Count the total number of aggregation in the local/final stages and keep
		// track of whether any of them needs a final rendering.
		numAgg := 0
		needRender := false
		for _, e := range aggregations {
			info := distsqlplan.DistAggregationTable[e.Func]
			numAgg += len(info.LocalStage)
			if info.FinalRendering != nil {
				needRender = true
			}
		}

		localAgg := make([]distsqlrun.AggregatorSpec_Aggregation, numAgg, numAgg+len(groupCols))
		intermediateTypes := make([]sqlbase.ColumnType, numAgg, numAgg+len(groupCols))
		finalAgg := make([]distsqlrun.AggregatorSpec_Aggregation, numAgg)
		finalGroupCols := make([]uint32, len(groupCols))
		var finalPreRenderTypes []sqlbase.ColumnType
		if needRender {
			finalPreRenderTypes = make([]sqlbase.ColumnType, numAgg)
		}

		// Each aggregation can have multiple aggregations in the local/final
		// stages. We concatenate all these into localAgg/finalAgg; aIdx is an index
		// inside localAgg/finalAgg.
		aIdx := 0
		for _, e := range aggregations {
			info := distsqlplan.DistAggregationTable[e.Func]
			for i, localFunc := range info.LocalStage {
				localAgg[aIdx] = distsqlrun.AggregatorSpec_Aggregation{
					Func:   localFunc,
					ColIdx: e.ColIdx,
				}

				_, localResultType, err := distsqlrun.GetAggregateInfo(localFunc, inputTypes[e.ColIdx])
				if err != nil {
					return err
				}
				intermediateTypes[aIdx] = localResultType

				finalAgg[aIdx] = distsqlrun.AggregatorSpec_Aggregation{
					Func: info.FinalStage[i],
					// The input of final expression aIdx is the output of the
					// local expression aIdx.
					ColIdx: uint32(aIdx),
				}
				if needRender {
					_, finalPreRenderTypes[aIdx], err = distsqlrun.GetAggregateInfo(
						info.FinalStage[i], localResultType,
					)
					if err != nil {
						return err
					}
				}
				aIdx++
			}
		}

		// Add IDENT expressions for the group columns; these need to be part of the
		// output of the local stage because the final stage needs them.
		for i, groupColIdx := range groupCols {
			agg := distsqlrun.AggregatorSpec_Aggregation{
				Func:   distsqlrun.AggregatorSpec_IDENT,
				ColIdx: groupColIdx,
			}
			// See if there already is an aggregation like the one we want to add.
			idx := -1
			for j, jAgg := range localAgg {
				if jAgg == agg {
					idx = j
					break
				}
			}
			if idx == -1 {
				// Not already there, add it.
				idx = len(localAgg)
				localAgg = append(localAgg, agg)
				intermediateTypes = append(intermediateTypes, inputTypes[groupColIdx])
			}
			finalGroupCols[i] = uint32(idx)
		}

		localAggSpec := distsqlrun.AggregatorSpec{
			Aggregations: localAgg,
			GroupCols:    groupCols,
		}

		p.AddNoGroupingStage(
			distsqlrun.ProcessorCoreUnion{Aggregator: &localAggSpec},
			distsqlrun.PostProcessSpec{},
			intermediateTypes,
			orderingTerminated, // The local aggregators don't guarantee any output ordering.
		)

		finalAggSpec = distsqlrun.AggregatorSpec{
			Aggregations: finalAgg,
			GroupCols:    finalGroupCols,
		}

		if needRender {
			// Build rendering expressions.
			renderExprs := make([]distsqlrun.Expression, len(aggregations))
			h := distsqlplan.MakeTypeIndexedVarHelper(finalPreRenderTypes)
			// aIdx is an index inside finalAgg. It is used to keep track of the
			// finalAgg results that correspond to each aggregation.
			aIdx := 0
			for i, e := range aggregations {
				info := distsqlplan.DistAggregationTable[e.Func]
				if info.FinalRendering == nil {
					renderExprs[i] = distsqlplan.MakeExpression(h.IndexedVar(aIdx), nil)
				} else {
					expr, err := info.FinalRendering(&h, aIdx)
					if err != nil {
						return err
					}
					renderExprs[i] = distsqlplan.MakeExpression(expr, nil)
				}
				aIdx += len(info.LocalStage)
			}
			finalAggPost.RenderExprs = renderExprs
		}
	}

	// Set up the final stage.

	finalOutTypes := make([]sqlbase.ColumnType, len(aggregations))
	for i, agg := range aggregations {
		var err error
		_, finalOutTypes[i], err = distsqlrun.GetAggregateInfo(agg.Func, inputTypes[agg.ColIdx])
		if err != nil {
			return err
		}
	}

	if len(finalAggSpec.GroupCols) == 0 || len(p.ResultRouters) == 1 {
		// No GROUP BY, or we have a single stream. Use a single final aggregator.
		// If the previous stage was all on a single node, put the final
		// aggregator there. Otherwise, bring the results back on this node.
		node := dsp.nodeDesc.NodeID
		if prevStageNode != 0 {
			node = prevStageNode
		}
		p.AddSingleGroupStage(
			node,
			distsqlrun.ProcessorCoreUnion{Aggregator: &finalAggSpec},
			finalAggPost,
			finalOutTypes,
		)
	} else {
		// We distribute (by group columns) to multiple processors.

		// Set up the output routers from the previous stage.
		for _, resultProc := range p.ResultRouters {
			p.Processors[resultProc].Spec.Output[0] = distsqlrun.OutputRouterSpec{
				Type:        distsqlrun.OutputRouterSpec_BY_HASH,
				HashColumns: finalAggSpec.GroupCols,
			}
		}

		// We have one final stage processor for each result router. This is a
		// somewhat arbitrary decision; we could have a different number of nodes
		// working on the final stage.
		pIdxStart := distsqlplan.ProcessorIdx(len(p.Processors))
		for _, resultProc := range p.ResultRouters {
			proc := distsqlplan.Processor{
				Node: p.Processors[resultProc].Node,
				Spec: distsqlrun.ProcessorSpec{
					Input: []distsqlrun.InputSyncSpec{{
						// The other fields will be filled in by mergeResultStreams.
						ColumnTypes: p.ResultTypes,
					}},
					Core: distsqlrun.ProcessorCoreUnion{Aggregator: &finalAggSpec},
					Post: finalAggPost,
					Output: []distsqlrun.OutputRouterSpec{{
						Type: distsqlrun.OutputRouterSpec_PASS_THROUGH,
					}},
				},
			}
			p.AddProcessor(proc)
		}

		// Connect the streams.
		for bucket := 0; bucket < len(p.ResultRouters); bucket++ {
			pIdx := pIdxStart + distsqlplan.ProcessorIdx(bucket)
			p.MergeResultStreams(p.ResultRouters, bucket, distsqlrun.Ordering{}, pIdx, 0)
		}

		// Set the new result routers.
		for i := 0; i < len(p.ResultRouters); i++ {
			p.ResultRouters[i] = pIdxStart + distsqlplan.ProcessorIdx(i)
		}
		p.ResultTypes = finalOutTypes
		p.SetMergeOrdering(orderingTerminated)
	}

	evalExprs := dsp.extractPostAggrExprs(n.render)
	p.AddRendering(evalExprs, p.planToStreamColMap, getTypesForPlanResult(n, nil))

	// Update p.planToStreamColMap; we will have a simple 1-to-1 mapping of
	// planNode columns to stream columns because the aggregator (and possibly
	// evaluator) have been programmed to produce the columns in order.
	p.planToStreamColMap = p.planToStreamColMap[:0]
	for i := range n.Columns() {
		p.planToStreamColMap = append(p.planToStreamColMap, i)
	}
	return nil
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

	joinReaderSpec := distsqlrun.JoinReaderSpec{
		Table:    n.index.desc,
		IndexIdx: 0,
	}

	post := distsqlrun.PostProcessSpec{
		Filter:        distsqlplan.MakeExpression(n.table.filter, nil),
		OutputColumns: getOutputColumnsFromScanNode(n.table),
	}

	// Recalculate planToStreamColMap: it now maps to columns in the JoinReader's
	// output stream.
	for i := range plan.planToStreamColMap {
		plan.planToStreamColMap[i] = -1
	}
	for i, col := range post.OutputColumns {
		plan.planToStreamColMap[col] = i
	}

	if distributeIndexJoin && len(plan.ResultRouters) > 1 {
		// Instantiate one join reader for every stream.
		plan.AddNoGroupingStage(
			distsqlrun.ProcessorCoreUnion{JoinReader: &joinReaderSpec},
			post,
			getTypesForPlanResult(n, plan.planToStreamColMap),
			dsp.convertOrdering(n.Ordering().ordering, plan.planToStreamColMap),
		)
	} else {
		// Use a single join reader (if there is a single stream, on that node; if
		// not, on the gateway node).
		node := dsp.nodeDesc.NodeID
		if len(plan.ResultRouters) == 1 {
			node = plan.Processors[plan.ResultRouters[0]].Node
		}
		plan.AddSingleGroupStage(
			node,
			distsqlrun.ProcessorCoreUnion{JoinReader: &joinReaderSpec},
			post,
			getTypesForPlanResult(n, plan.planToStreamColMap),
		)
	}
	return plan, nil
}

// getTypesForPlanResult returns the types of the elements in the result streams
// of a plan that corresponds to a given planNode. If planToSreamColMap is nil,
// a 1-1 mapping is assumed.
func getTypesForPlanResult(node planNode, planToStreamColMap []int) []sqlbase.ColumnType {
	nodeColumns := node.Columns()
	if planToStreamColMap == nil {
		// No remapping.
		types := make([]sqlbase.ColumnType, len(nodeColumns))
		for i := range nodeColumns {
			types[i] = sqlbase.DatumTypeToColumnType(nodeColumns[i].Typ)
		}
		return types
	}
	numCols := 0
	for _, streamCol := range planToStreamColMap {
		if numCols <= streamCol {
			numCols = streamCol + 1
		}
	}
	types := make([]sqlbase.ColumnType, numCols)
	for nodeCol, streamCol := range planToStreamColMap {
		if streamCol != -1 {
			types[streamCol] = sqlbase.DatumTypeToColumnType(nodeColumns[nodeCol].Typ)
		}
	}
	return types
}

func (dsp *distSQLPlanner) createPlanForJoin(
	planCtx *planningCtx, n *joinNode,
) (physicalPlan, error) {

	// Outline of the planning process for joins:
	//
	//  - We create physicalPlans for the left and right side. Each plan has a set
	//    of output routers with result that will serve as input for the join.
	//
	//  - We merge the list of processors and streams into a single plan. We keep
	//    track of the output routers for the left and right results.
	//
	//  - We add a set of joiner processors (say K of them).
	//
	//  - We configure the left and right output routers to send results to
	//    these joiners, distributing rows by hash (on the join equality columns).
	//    We are thus breaking up all input rows into K buckets such that rows
	//    that match on the equality columns end up in the same bucket. If there
	//    are no equality columns, we cannot distribute rows so we use a single
	//    joiner.
	//
	//  - The routers of the joiner processors are the result routers of the plan.

	leftPlan, err := dsp.createPlanForNode(planCtx, n.left.plan)
	if err != nil {
		return physicalPlan{}, err
	}
	rightPlan, err := dsp.createPlanForNode(planCtx, n.right.plan)
	if err != nil {
		return physicalPlan{}, err
	}

	var p physicalPlan
	var leftRouters, rightRouters []distsqlplan.ProcessorIdx
	p.PhysicalPlan, leftRouters, rightRouters = distsqlplan.MergePlans(
		&leftPlan.PhysicalPlan, &rightPlan.PhysicalPlan,
	)

	joinToStreamColMap := makePlanToStreamColMap(len(n.columns))

	// Nodes where we will run the join processors.
	var nodes []roachpb.NodeID
	var joinerSpec distsqlrun.HashJoinerSpec

	if n.joinType != joinTypeInner {
		panic("only inner join supported for now")
	}
	joinerSpec.Type = distsqlrun.JoinType_INNER

	// Figure out the left and right types.
	leftTypes := leftPlan.ResultTypes
	rightTypes := rightPlan.ResultTypes

	// Set up the output columns.
	if numEq := len(n.pred.leftEqualityIndices); numEq != 0 {
		// TODO(radu): for now we run a join processor on every node that produces
		// data for either source. In the future we should be smarter here.
		seen := make(map[roachpb.NodeID]struct{})
		for _, pIdx := range leftRouters {
			n := p.Processors[pIdx].Node
			if _, ok := seen[n]; !ok {
				seen[n] = struct{}{}
				nodes = append(nodes, n)
			}
		}
		for _, pIdx := range rightRouters {
			n := p.Processors[pIdx].Node
			if _, ok := seen[n]; !ok {
				seen[n] = struct{}{}
				nodes = append(nodes, n)
			}
		}

		// Set up the equality columns.
		joinerSpec.LeftEqColumns = make([]uint32, numEq)
		for i, leftPlanCol := range n.pred.leftEqualityIndices {
			joinerSpec.LeftEqColumns[i] = uint32(leftPlan.planToStreamColMap[leftPlanCol])
		}
		joinerSpec.RightEqColumns = make([]uint32, numEq)
		for i, rightPlanCol := range n.pred.rightEqualityIndices {
			joinerSpec.RightEqColumns[i] = uint32(rightPlan.planToStreamColMap[rightPlanCol])
		}
	} else {
		// Without column equality, we cannot distribute the join. Run a
		// single processor on this node.
		nodes = []roachpb.NodeID{dsp.nodeDesc.NodeID}
	}

	var post distsqlrun.PostProcessSpec
	// addOutCol appends to post.OutputColumns and returns the index
	// in the slice of the added column.
	addOutCol := func(col uint32) int {
		idx := len(post.OutputColumns)
		post.OutputColumns = append(post.OutputColumns, col)
		return idx
	}

	// The join columns are in three groups:
	//  - numMergedEqualityColumns "merged" columns (corresponding to the equality columns)
	//  - the columns on the left side (numLeftCols)
	//  - the columns on the right side (numRightCols)
	joinCol := 0
	for i := 0; i < n.pred.numMergedEqualityColumns; i++ {
		if !n.columns[joinCol].omitted {
			// TODO(radu): for full outer joins, this will be more tricky: we would
			// need an output column that outputs either the left or the right
			// equality column, whichever is not NULL.
			joinToStreamColMap[joinCol] = addOutCol(joinerSpec.LeftEqColumns[i])
		}
		joinCol++
	}
	for i := 0; i < n.pred.numLeftCols; i++ {
		if !n.columns[joinCol].omitted {
			joinToStreamColMap[joinCol] = addOutCol(uint32(leftPlan.planToStreamColMap[i]))
		}
		joinCol++
	}
	for i := 0; i < n.pred.numRightCols; i++ {
		if !n.columns[joinCol].omitted {
			joinToStreamColMap[joinCol] = addOutCol(
				uint32(rightPlan.planToStreamColMap[i] + len(leftTypes)),
			)
		}
		joinCol++
	}

	if n.pred.onCond != nil {
		// We have to remap ordinal references in the on condition (which refer to
		// the join columns as described above) to values that make sense in the
		// joiner (0 to N-1 for the left input columns, N to N+M-1 for the right
		// input columns).
		joinColMap := make([]int, 0, len(n.columns))
		for i := 0; i < n.pred.numMergedEqualityColumns; i++ {
			// Merged column. See TODO above.
			joinColMap = append(joinColMap, int(joinerSpec.LeftEqColumns[i]))
		}
		for i := 0; i < n.pred.numLeftCols; i++ {
			joinColMap = append(joinColMap, leftPlan.planToStreamColMap[i])
		}
		for i := 0; i < n.pred.numRightCols; i++ {
			joinColMap = append(joinColMap, rightPlan.planToStreamColMap[i]+len(leftTypes))
		}
		joinerSpec.OnExpr = distsqlplan.MakeExpression(n.pred.onCond, joinColMap)
	}

	pIdxStart := distsqlplan.ProcessorIdx(len(p.Processors))

	if len(nodes) == 1 {
		proc := distsqlplan.Processor{
			Node: nodes[0],
			Spec: distsqlrun.ProcessorSpec{
				Input: []distsqlrun.InputSyncSpec{
					{ColumnTypes: leftTypes},
					{ColumnTypes: rightTypes},
				},
				Core:   distsqlrun.ProcessorCoreUnion{HashJoiner: &joinerSpec},
				Post:   post,
				Output: []distsqlrun.OutputRouterSpec{{Type: distsqlrun.OutputRouterSpec_PASS_THROUGH}},
			},
		}
		p.Processors = append(p.Processors, proc)
	} else {
		// Parallel hash join: we distribute rows (by hash of equality columns) to
		// len(nodes) join processors.

		// Each node has a join processor.
		for _, n := range nodes {
			proc := distsqlplan.Processor{
				Node: n,
				Spec: distsqlrun.ProcessorSpec{
					Input: []distsqlrun.InputSyncSpec{
						{ColumnTypes: leftTypes},
						{ColumnTypes: rightTypes},
					},
					Core:   distsqlrun.ProcessorCoreUnion{HashJoiner: &joinerSpec},
					Post:   post,
					Output: []distsqlrun.OutputRouterSpec{{Type: distsqlrun.OutputRouterSpec_PASS_THROUGH}},
				},
			}
			p.Processors = append(p.Processors, proc)
		}

		// Set up the left routers.
		for _, resultProc := range leftRouters {
			p.Processors[resultProc].Spec.Output[0] = distsqlrun.OutputRouterSpec{
				Type:        distsqlrun.OutputRouterSpec_BY_HASH,
				HashColumns: joinerSpec.LeftEqColumns,
			}
		}
		// Set up the right routers.
		for _, resultProc := range rightRouters {
			p.Processors[resultProc].Spec.Output[0] = distsqlrun.OutputRouterSpec{
				Type:        distsqlrun.OutputRouterSpec_BY_HASH,
				HashColumns: joinerSpec.RightEqColumns,
			}
		}
	}
	p.ResultRouters = p.ResultRouters[:0]

	// Connect the left and right routers to the output joiners. Each joiner
	// corresponds to a hash bucket.
	for bucket := 0; bucket < len(nodes); bucket++ {
		pIdx := pIdxStart + distsqlplan.ProcessorIdx(bucket)

		// Connect left routers to the processor's first input. Currently the join
		// node doesn't care about the orderings of the left and right results.
		p.MergeResultStreams(leftRouters, bucket, distsqlrun.Ordering{}, pIdx, 0)
		// Connect right routers to the processor's second input.
		p.MergeResultStreams(rightRouters, bucket, distsqlrun.Ordering{}, pIdx, 1)

		p.ResultRouters = append(p.ResultRouters, pIdx)
	}

	p.planToStreamColMap = joinToStreamColMap
	p.ResultTypes = getTypesForPlanResult(n, joinToStreamColMap)
	p.SetMergeOrdering(orderingTerminated)
	return p, nil
}

func (dsp *distSQLPlanner) createPlanForNode(
	planCtx *planningCtx, node planNode,
) (physicalPlan, error) {
	switch n := node.(type) {
	case *scanNode:
		return dsp.createTableReaders(planCtx, n, nil)

	case *indexJoinNode:
		return dsp.createPlanForIndexJoin(planCtx, n)

	case *joinNode:
		return dsp.createPlanForJoin(planCtx, n)

	case *renderNode:
		plan, err := dsp.createPlanForNode(planCtx, n.source.plan)
		if err != nil {
			return physicalPlan{}, err
		}
		dsp.selectRenders(&plan, n)
		return plan, nil

	case *groupNode:
		plan, err := dsp.createPlanForNode(planCtx, n.plan)
		if err != nil {
			return physicalPlan{}, err
		}

		if err := dsp.addAggregators(planCtx, &plan, n); err != nil {
			return physicalPlan{}, err
		}

		return plan, nil

	case *sortNode:
		plan, err := dsp.createPlanForNode(planCtx, n.plan)
		if err != nil {
			return physicalPlan{}, err
		}

		dsp.addSorters(&plan, n)

		return plan, nil

	case *filterNode:
		plan, err := dsp.createPlanForNode(planCtx, n.source.plan)
		if err != nil {
			return physicalPlan{}, err
		}

		plan.AddFilter(n.filter, plan.planToStreamColMap)

		return plan, nil

	case *limitNode:
		plan, err := dsp.createPlanForNode(planCtx, n.plan)
		if err != nil {
			return physicalPlan{}, err
		}
		if err := n.evalLimit(); err != nil {
			return physicalPlan{}, err
		}
		if err := plan.AddLimit(n.count, n.offset, dsp.nodeDesc.NodeID); err != nil {
			return physicalPlan{}, err
		}
		return plan, nil

	case *distinctNode:
		return dsp.createPlanForDistinct(planCtx, n)

	default:
		panic(fmt.Sprintf("unsupported node type %T", n))
	}
}

func (dsp *distSQLPlanner) createPlanForDistinct(
	planCtx *planningCtx, n *distinctNode,
) (physicalPlan, error) {
	plan, err := dsp.createPlanForNode(planCtx, n.plan)
	if err != nil {
		return physicalPlan{}, err
	}
	currentResultRouters := plan.ResultRouters
	var orderedColumns []uint32
	for i := 0; i < len(n.columnsInOrder); i++ {
		if n.columnsInOrder[i] {
			orderedColumns = append(orderedColumns, uint32(plan.planToStreamColMap[i]))
		}
	}
	var distinctColumns []uint32
	for i := range n.Columns() {
		if plan.planToStreamColMap[i] != -1 {
			distinctColumns = append(distinctColumns, uint32(plan.planToStreamColMap[i]))
		}
	}

	distinctSpec := distsqlrun.ProcessorCoreUnion{
		Distinct: &distsqlrun.DistinctSpec{
			OrderedColumns:  orderedColumns,
			DistinctColumns: distinctColumns,
		},
	}

	if len(currentResultRouters) == 1 {
		plan.AddNoGroupingStage(distinctSpec, distsqlrun.PostProcessSpec{}, plan.ResultTypes, plan.MergeOrdering)
		return plan, nil
	}

	// TODO(arjun): This is potentially memory inefficient if we don't have any sorted columns.

	// Add distinct processors local to each existing current result processor.
	plan.AddNoGroupingStage(distinctSpec, distsqlrun.PostProcessSpec{}, plan.ResultTypes, plan.MergeOrdering)

	// TODO(arjun): We could distribute this final stage by hash.
	plan.AddSingleGroupStage(dsp.nodeDesc.NodeID, distinctSpec, distsqlrun.PostProcessSpec{}, plan.ResultTypes)
	return plan, nil
}

func (dsp *distSQLPlanner) NewPlanningCtx(ctx context.Context, txn *client.Txn) planningCtx {
	planCtx := planningCtx{
		ctx:           ctx,
		spanIter:      dsp.spanResolver.NewSpanResolverIterator(txn),
		nodeAddresses: make(map[roachpb.NodeID]string),
	}
	planCtx.nodeAddresses[dsp.nodeDesc.NodeID] = dsp.nodeDesc.Address.String()
	return planCtx
}

// FinalizePlan adds a final "result" stage if necessary and populates the
// endpoints of the plan.
func (dsp *distSQLPlanner) FinalizePlan(planCtx *planningCtx, plan *physicalPlan) {
	thisNodeID := dsp.nodeDesc.NodeID
	// If we don't already have a single result router on this node, add a final
	// stage.
	if len(plan.ResultRouters) != 1 ||
		plan.Processors[plan.ResultRouters[0]].Node != thisNodeID {
		plan.AddSingleGroupStage(
			thisNodeID,
			distsqlrun.ProcessorCoreUnion{Noop: &distsqlrun.NoopCoreSpec{}},
			distsqlrun.PostProcessSpec{},
			plan.ResultTypes,
		)
		if len(plan.ResultRouters) != 1 {
			panic(fmt.Sprintf("%d results after single group stage", len(plan.ResultRouters)))
		}
	}

	// Set up the endpoints for p.streams.
	plan.PopulateEndpoints(planCtx.nodeAddresses)

	// Set up the endpoint for the final result.
	finalOut := &plan.Processors[plan.ResultRouters[0]].Spec.Output[0]
	finalOut.Streams = append(finalOut.Streams, distsqlrun.StreamEndpointSpec{
		Type: distsqlrun.StreamEndpointSpec_SYNC_RESPONSE,
	})
}
