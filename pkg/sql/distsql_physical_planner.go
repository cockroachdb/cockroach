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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	distSQLSrv   *distsqlrun.ServerImpl
	spanResolver distsqlplan.SpanResolver
}

const resolverPolicy = distsqlplan.BinPackingLeaseHolderChoice

// If true, the plan diagram (in JSON) is logged for each plan (used for
// debugging).
var logPlanDiagram = envutil.EnvOrDefaultBool("COCKROACH_DISTSQL_LOG_PLAN", false)

func newDistSQLPlanner(
	nodeDesc roachpb.NodeDescriptor,
	rpcCtx *rpc.Context,
	distSQLSrv *distsqlrun.ServerImpl,
	distSender *kv.DistSender,
	gossip *gossip.Gossip,
) *distSQLPlanner {
	return &distSQLPlanner{
		nodeDesc:     nodeDesc,
		rpcContext:   rpcCtx,
		distSQLSrv:   distSQLSrv,
		spanResolver: distsqlplan.NewSpanResolver(distSender, gossip, nodeDesc, resolverPolicy),
	}
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

	case *parser.CollateExpr:
		v.err = errors.Errorf("collations not supported yet (#13496)")
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
		// TODO(radu): remove this limitation.
		for _, id := range n.index.CompositeColumnIDs {
			idx, ok := n.colIdxMap[id]
			if ok && n.valNeededForCol[idx] {
				return 0, errors.Errorf("cannot scan composite column")
			}
		}
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
	backfillType backfillType, desc sqlbase.TableDescriptor, duration time.Duration, chunkSize int64,
) (distsqlrun.BackfillerSpec, error) {
	switch backfillType {
	case indexBackfill:
		return distsqlrun.BackfillerSpec{
			Type:      distsqlrun.BackfillerSpec_Index,
			Table:     desc,
			Duration:  duration,
			ChunkSize: chunkSize,
		}, nil

	case columnBackfill:
		return distsqlrun.BackfillerSpec{}, errors.Errorf("column backfillNode not implemented")

	default:
		return distsqlrun.BackfillerSpec{}, errors.Errorf("bad backfill type %d", backfillType)
	}
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
) (physicalPlan, error) {
	spec, err := initBackfillerSpec(backfillType, desc, duration, chunkSize)
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

	// The way our planNode construction currently orders columns between
	// groupNode and its source is that the first len(n.funcs) columns are the
	// arguments for the aggregation functions. The remaining columns are
	// columns we group by.
	// For 'SELECT 1, SUM(k) GROUP BY k', the output schema of groupNode's
	// source will be [1 k k], with 1, k being arguments fed to the aggregation
	// functions and k being the column we group by.
	groupCols := make([]uint32, 0, len(n.plan.Columns())-len(n.funcs))
	for i := len(n.funcs); i < len(n.plan.Columns()); i++ {
		groupCols = append(groupCols, uint32(p.planToStreamColMap[i]))
	}

	// We either have a local stage on each stream followed by a final stage, or
	// just a final stage. We only use a local stage if:
	//  - the previous stage is distributed on multiple nodes, and
	//  - all aggregation functions support it. TODO(radu): we could relax this by
	//    splitting the aggregation into two different paths and joining on the
	//    results.
	multiStage := false

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
				// We can't do local aggregation for functions with distinct (at least not
				// in general).
				multiStage = false
				break
			}
			if _, ok := distsqlplan.DistAggregationTable[e.Func]; !ok {
				multiStage = false
				break
			}
		}
	}

	var finalAggSpec distsqlrun.AggregatorSpec

	if !multiStage {
		finalAggSpec = distsqlrun.AggregatorSpec{
			Aggregations: aggregations,
			GroupCols:    groupCols,
		}
	} else {
		localAgg := make([]distsqlrun.AggregatorSpec_Aggregation, len(aggregations)+len(groupCols))
		intermediateTypes := make([]sqlbase.ColumnType, len(aggregations)+len(groupCols))
		finalAgg := make([]distsqlrun.AggregatorSpec_Aggregation, len(aggregations))
		finalGroupCols := make([]uint32, len(groupCols))

		for i, e := range aggregations {
			info := distsqlplan.DistAggregationTable[e.Func]
			localAgg[i] = distsqlrun.AggregatorSpec_Aggregation{
				Func:   info.LocalStage,
				ColIdx: e.ColIdx,
			}
			finalAgg[i] = distsqlrun.AggregatorSpec_Aggregation{
				Func: info.FinalStage,
				// The input of the i-th final expression is the output of the i-th
				// local expression.
				ColIdx: uint32(i),
			}
			var err error
			_, intermediateTypes[i], err = distsqlrun.GetAggregateInfo(e.Func, p.ResultTypes[e.ColIdx])
			if err != nil {
				return err
			}
		}

		// Add IDENT expressions for the group columns; these need to be part of the
		// output of the local stage because the final stage needs them.
		for i, groupColIdx := range groupCols {
			exprIdx := len(aggregations) + i
			localAgg[exprIdx] = distsqlrun.AggregatorSpec_Aggregation{
				Func:   distsqlrun.AggregatorSpec_IDENT,
				ColIdx: groupColIdx,
			}
			intermediateTypes[exprIdx] = p.ResultTypes[groupColIdx]
			finalGroupCols[i] = uint32(exprIdx)
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
	}

	// TODO(radu): we could distribute the final stage by hash.

	// If the previous stage was all on a single node, put the final stage there.
	// Otherwise, bring the results back on this node.
	node := dsp.nodeDesc.NodeID
	if prevStageNode != 0 {
		node = prevStageNode
	}

	finalOutTypes := make([]sqlbase.ColumnType, len(finalAggSpec.Aggregations))
	for i, agg := range finalAggSpec.Aggregations {
		var err error
		_, finalOutTypes[i], err = distsqlrun.GetAggregateInfo(agg.Func, p.ResultTypes[agg.ColIdx])
		if err != nil {
			return err
		}
	}
	p.AddSingleGroupStage(
		node,
		distsqlrun.ProcessorCoreUnion{Aggregator: &finalAggSpec},
		distsqlrun.PostProcessSpec{},
		finalOutTypes,
	)

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

	// TODO(radu): we currently use a single JoinReader. We could have multiple.
	// Note that in that case, if the index ordering is used, we must make sure
	// that the index columns are part of the output (so that ordered
	// synchronizers down the road can maintain the order).

	plan.AddSingleGroupStage(
		dsp.nodeDesc.NodeID,
		distsqlrun.ProcessorCoreUnion{JoinReader: &joinReaderSpec},
		post,
		getTypesForPlanResult(n, plan.planToStreamColMap),
	)
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

	default:
		panic(fmt.Sprintf("unsupported node type %T", n))
	}
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

// PlanAndRun generates a physical plan from a planNode tree and executes it. It
// assumes that the tree is supported (see CheckSupport).
//
// Note that errors that happen while actually running the flow are reported to
// recv, not returned by this function.
func (dsp *distSQLPlanner) PlanAndRun(
	ctx context.Context, txn *client.Txn, tree planNode, recv *distSQLReceiver,
) error {
	planCtx := dsp.NewPlanningCtx(ctx, txn)

	log.VEvent(ctx, 1, "creating DistSQL plan")

	plan, err := dsp.createPlanForNode(&planCtx, tree)
	if err != nil {
		return err
	}
	dsp.FinalizePlan(&planCtx, &plan)
	return dsp.Run(&planCtx, txn, &plan, recv)
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

// Run executes a physical plan. The plan should have been finalized using
// FinalizePlan.
//
// Note that errors that happen while actually running the flow are reported to
// recv, not returned by this function.
func (dsp *distSQLPlanner) Run(
	planCtx *planningCtx, txn *client.Txn, plan *physicalPlan, recv *distSQLReceiver,
) error {
	ctx := planCtx.ctx

	flows := plan.GenerateFlowSpecs()

	if logPlanDiagram {
		log.VEvent(ctx, 1, "creating plan diagram")
		json, url, err := distsqlrun.GeneratePlanDiagramWithURL(flows)
		if err != nil {
			log.Infof(ctx, "Error generating diagram: %s", err)
		} else {
			log.Infof(ctx, "Plan diagram JSON:\n%s", json)
			log.Infof(ctx, "Plan diagram URL:\n%s", url.String())
		}
	}

	log.VEvent(ctx, 1, "running DistSQL plan")

	recv.resultToStreamColMap = plan.planToStreamColMap
	thisNodeID := dsp.nodeDesc.NodeID

	// Start the flows on all other nodes.
	for nodeID, flowSpec := range flows {
		if nodeID == thisNodeID {
			// Skip this node.
			continue
		}
		req := distsqlrun.SetupFlowRequest{
			Txn:  *txn.Proto(),
			Flow: flowSpec,
		}
		if err := distsqlrun.SetFlowRequestTrace(ctx, &req); err != nil {
			return err
		}
		conn, err := dsp.rpcContext.GRPCDial(planCtx.nodeAddresses[nodeID])
		if err != nil {
			return err
		}
		client := distsqlrun.NewDistSQLClient(conn)
		// TODO(radu): we are not waiting for the flows to complete, but we are
		// still waiting for a round trip; we should start the flows in parallel, at
		// least if there are enough of them.
		if resp, err := client.SetupFlow(context.Background(), &req); err != nil {
			return err
		} else if resp.Error != nil {
			return resp.Error.GoError()
		}
	}
	localReq := distsqlrun.SetupFlowRequest{
		Txn:  *txn.Proto(),
		Flow: flows[thisNodeID],
	}
	if err := distsqlrun.SetFlowRequestTrace(ctx, &localReq); err != nil {
		return err
	}
	ctx, flow, err := dsp.distSQLSrv.SetupSyncFlow(ctx, &localReq, recv)
	if err != nil {
		return err
	}
	// TODO(radu): this should go through the flow scheduler.
	flow.Start(ctx, func() {})
	flow.Wait()
	flow.Cleanup(ctx)

	return nil
}

type distSQLReceiver struct {
	ctx context.Context

	// rows is the container where we store the results; if we only need the count
	// of the rows, it is nil.
	rows *RowContainer
	// resultToStreamColMap maps result columns to columns in the distsqlrun results
	// stream.
	resultToStreamColMap []int
	// numRows counts the number of rows we received when rows is nil.
	numRows int64
	err     error
	row     parser.Datums
	status  distsqlrun.ConsumerStatus
	alloc   sqlbase.DatumAlloc
	closed  bool
}

var _ distsqlrun.RowReceiver = &distSQLReceiver{}

func makeDistSQLReceiver(ctx context.Context, sink *RowContainer) distSQLReceiver {
	return distSQLReceiver{ctx: ctx, rows: sink}
}

// Push is part of the RowReceiver interface.
func (r *distSQLReceiver) Push(
	row sqlbase.EncDatumRow, meta distsqlrun.ProducerMetadata,
) distsqlrun.ConsumerStatus {
	if !meta.Empty() {
		if meta.Err != nil && r.err == nil {
			r.err = meta.Err
		}
		// TODO(andrei): do something with the metadata - update the descriptor
		// caches.
		return r.status
	}
	if r.err != nil {
		return distsqlrun.ConsumerClosed
	}
	if r.status != distsqlrun.NeedMoreRows {
		return r.status
	}

	if r.rows == nil {
		// We only need the row count.
		r.numRows++
		return r.status
	}
	if r.row == nil {
		r.row = make(parser.Datums, len(r.resultToStreamColMap))
	}
	for i, resIdx := range r.resultToStreamColMap {
		err := row[resIdx].EnsureDecoded(&r.alloc)
		if err != nil {
			r.err = err
			r.status = distsqlrun.ConsumerClosed
			return r.status
		}
		r.row[i] = row[resIdx].Datum
	}
	// Note that AddRow accounts for the memory used by the Datums.
	if _, err := r.rows.AddRow(r.ctx, r.row); err != nil {
		r.err = err
		// TODO(andrei): We should drain here. Metadata from this query would be
		// useful, particularly as it was likely a large query (since AddRow()
		// above failed, presumably with an out-of-memory error).
		r.status = distsqlrun.ConsumerClosed
		return r.status
	}
	return r.status
}

// ProducerDone is part of the RowReceiver interface.
func (r *distSQLReceiver) ProducerDone() {
	if r.closed {
		panic("double close")
	}
	r.closed = true
}
