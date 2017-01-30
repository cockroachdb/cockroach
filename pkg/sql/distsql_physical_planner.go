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
	"bytes"
	"fmt"
	"strings"

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
	distSQLSrv   *distsqlrun.ServerImpl
	spanResolver *distsqlplan.SpanResolver
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
func (dsp *distSQLPlanner) CheckSupport(tree planNode) (shouldRunDist bool, notSuppErr error) {
	switch n := tree.(type) {
	case *filterNode:
		if err := dsp.checkExpr(n.filter); err != nil {
			return false, err
		}
		return dsp.CheckSupport(n.source.plan)

	case *renderNode:
		for i, e := range n.render {
			if typ := n.columns[i].Typ; typ.FamilyEqual(parser.TypeTuple) ||
				typ.FamilyEqual(parser.TypeStringArray) ||
				typ.FamilyEqual(parser.TypeIntArray) {
				return false, errors.Errorf("unsupported render type %s", typ)
			}
			if err := dsp.checkExpr(e); err != nil {
				return false, err
			}
		}
		return dsp.CheckSupport(n.source.plan)

	case *sortNode:
		shouldDistribute, err := dsp.CheckSupport(n.plan)
		if err != nil {
			return false, err
		}
		// If we have to sort, distribute the query.
		return shouldDistribute || n.needSort, nil

	case *joinNode:
		if n.joinType != joinTypeInner {
			return false, errors.Errorf("only inner join supported")
		}
		if err := dsp.checkExpr(n.pred.onCond); err != nil {
			return false, err
		}
		shouldRunDistLeft, err := dsp.CheckSupport(n.left.plan)
		if err != nil {
			return false, err
		}
		shouldRunDistRight, err := dsp.CheckSupport(n.right.plan)
		if err != nil {
			return false, err
		}
		// If either the left or the right side can benefit from distribution, we
		// should distribute.
		shouldDistribute := shouldRunDistLeft || shouldRunDistRight
		// If we can do a hash join, we should distribute.
		shouldDistribute = shouldDistribute || len(n.pred.leftEqualityIndices) > 0
		return shouldDistribute, nil

	case *scanNode:
		// We recommend running scans distributed if we have a filtering
		// expression or if we have a full table scan.
		if n.filter != nil {
			if err := dsp.checkExpr(n.filter); err != nil {
				return false, err
			}
			return true, nil
		}
		if len(n.spans) == 0 {
			// No spans means we are doing a full table scan.
			return true, nil
		}
		return false, nil

	case *indexJoinNode:
		// n.table doesn't have meaningful spans, but we need to check support (e.g.
		// for any filtering expression).
		if _, err := dsp.CheckSupport(n.table); err != nil {
			return false, err
		}
		return dsp.CheckSupport(n.index)

	case *groupNode:
		if n.having != nil {
			return false, errors.Errorf("group with having not supported yet")
		}
		for _, fholder := range n.funcs {
			if fholder.filter != nil {
				return false, errors.Errorf("aggregation with FILTER not supported yet")
			}
			if f, ok := fholder.expr.(*parser.FuncExpr); ok {
				if strings.ToUpper(f.Func.FunctionReference.String()) == "ARRAY_AGG" {
					return false, errors.Errorf("ARRAY_AGG aggregation not supported yet")
				}
			}
		}
		return dsp.CheckSupport(n.plan)

	default:
		return false, errors.Errorf("unsupported node %T", tree)
	}
}

// planningCtx contains data used and updated throughout the planning process of
// a single query.
type planningCtx struct {
	ctx      context.Context
	spanIter *distsqlplan.SpanResolverIterator
	// nodeAddresses contains addresses for all NodeIDs that are referenced by any
	// physicalPlan we generate with this context.
	nodeAddresses map[roachpb.NodeID]string
}

type processor struct {
	node roachpb.NodeID
	// the spec for the processor; note that the StreamEndpointSpecs in the input
	// synchronizers and output routers are not set until the end of the planning
	// process.
	spec distsqlrun.ProcessorSpec
}

// processorIdx identifies a processor by its index in physicalPlan.processors.
type processorIdx int

type stream struct {
	sourceProcessor processorIdx

	// sourceRouterSlot identifies the position of this stream among the streams
	// that originate from the same router. This is important for things like
	// routing by hash where the order of the streams in the OutputRouterSpec
	// matters.
	sourceRouterSlot int

	destProcessor processorIdx

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
	// order to extend the plan. All result routers have the same "schema".
	//
	// We assume all processors have a single output so we only need the processor
	// index.
	resultRouters []processorIdx

	// resultTypes are column types (schema) of the rows produced
	// by the resultRouters.
	//
	// This is aliased with InputSyncSpec.ColumnTypes, so it shouldn't be modified
	// in-place during planning.
	resultTypes []sqlbase.ColumnType

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
	ordering distsqlrun.Ordering
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

// addProcessor adds a processor to a physicalPlan and returns the index that
// can be used to refer to that processor.
func (p *physicalPlan) addProcessor(proc processor) processorIdx {
	idx := processorIdx(len(p.processors))
	p.processors = append(p.processors, proc)
	return idx
}

// mergePlans merges the processors and streams of two plan into a new plan.
// The result routers for each side are also returned (they point at processors
// in the merged plan).
func mergePlans(
	left, right *physicalPlan,
) (mergedPlan physicalPlan, leftRouters []processorIdx, rightRouters []processorIdx) {
	mergedPlan.processors = append(left.processors, right.processors...)
	rightProcStart := processorIdx(len(left.processors))

	mergedPlan.streams = append(left.streams, right.streams...)

	// Update the processor indices in the right streams.
	for i := len(left.streams); i < len(mergedPlan.streams); i++ {
		mergedPlan.streams[i].sourceProcessor += rightProcStart
		mergedPlan.streams[i].destProcessor += rightProcStart
	}

	leftRouters = left.resultRouters
	rightRouters = append([]processorIdx(nil), right.resultRouters...)
	// Update the processor indices in the right routers.
	for i := range rightRouters {
		rightRouters[i] += rightProcStart
	}

	return mergedPlan, leftRouters, rightRouters
}

// exprFmtFlagsBase are FmtFlags used for serializing expressions; a proper
// IndexedVar formatting function needs to be added on.
var exprFmtFlagsBase = parser.FmtStarDatumFormat(
	parser.FmtParsable,
	func(buf *bytes.Buffer, _ parser.FmtFlags) {
		fmt.Fprintf(buf, "0")
	},
)

// exprFmtFlagsNoMap are FmtFlags used for serializing expressions that don't
// need to remap IndexedVars.
var exprFmtFlagsNoMap = parser.FmtIndexedVarFormat(
	exprFmtFlagsBase,
	func(buf *bytes.Buffer, _ parser.FmtFlags, _ parser.IndexedVarContainer, idx int) {
		fmt.Fprintf(buf, "@%d", idx+1)
	},
)

// The distsqlrun Expression uses the placeholder syntax (@1, @2, @3..) to
// refer to columns. We format the expression using the IndexedVar and StarDatum
// formatting interceptors. An indexVarMap can optionally be used to remap the
// indices.
func distSQLExpression(expr parser.TypedExpr, indexVarMap []int) distsqlrun.Expression {
	if expr == nil {
		return distsqlrun.Expression{}
	}

	var f parser.FmtFlags
	if indexVarMap == nil {
		f = exprFmtFlagsNoMap
	} else {
		f = parser.FmtIndexedVarFormat(
			exprFmtFlagsBase,
			func(buf *bytes.Buffer, _ parser.FmtFlags, _ parser.IndexedVarContainer, idx int) {
				remappedIdx := indexVarMap[idx]
				if remappedIdx < 0 {
					panic(fmt.Sprintf("unmapped index %d", idx))
				}
				fmt.Fprintf(buf, "@%d", remappedIdx+1)
			},
		)
	}
	var buf bytes.Buffer
	expr.Format(&buf, f)
	if log.V(1) {
		log.Infof(context.TODO(), "Expr %s:\n%s", buf.String(), parser.ExprDebugString(expr))
	}
	return distsqlrun.Expression{Expr: buf.String()}
}

// getLastStagePost returns the PostProcessSpec for the current result
// processors in the plan.
func (p *physicalPlan) getLastStagePost() distsqlrun.PostProcessSpec {
	post := p.processors[p.resultRouters[0]].spec.Post

	// All processors of a stage should be identical in terms of post-processing;
	// verify this assumption.
	for i := 1; i < len(p.resultRouters); i++ {
		pi := &p.processors[p.resultRouters[i]].spec.Post
		if pi.Filter != post.Filter || len(pi.OutputColumns) != len(post.OutputColumns) {
			panic(fmt.Sprintf("inconsistent post-processing: %v vs %v", post, pi))
		}
		for j, col := range pi.OutputColumns {
			if col != post.OutputColumns[j] {
				panic(fmt.Sprintf("inconsistent post-processing: %v vs %v", post, pi))
			}
		}
	}

	return post
}

// setLastStagePost changes the PostProcess spec of the processors in the last
// stage (resultRouters).
func (p *physicalPlan) setLastStagePost(
	post distsqlrun.PostProcessSpec, outputTypes []sqlbase.ColumnType,
) {
	for _, pIdx := range p.resultRouters {
		p.processors[pIdx].spec.Post = post
	}
	p.resultTypes = outputTypes
}

// reverseProjection remaps expression variable indices to refer to internal
// columns (i.e. before post-processing) of a processor instead of output
// columns (i.e. after post-processing).
//
// Inputs:
//   indexVarMap is a mapping from columns that appear in an expression
//               (planNode columns) to columns in the output stream of a
//               processor.
//   outputColumns is the list of output columns in the processor's
//                 PostProcessSpec; it is effectively a mapping from the output
//                 schema to the internal schema of a processor. If nil, an
//                 identity mapping is assumed.
//
// Result: a "composite map" that maps the planNode columns to the internal
//         columns of the processor.
//
// For efficiency, the indexVarMap and the resulting map are represented as
// slices, with missing elements having values -1.
//
// Used when adding expressions (filtering, rendering) to a processor's
// PostProcessSpec. For example:
//
//  TableReader // table columns A,B,C,D
//  Internal schema (before post-processing): A, B, C, D
//  OutputColumns:  [1 3]
//  Output schema (after post-processing): B, D
//
//  Expression "B < D" might be represented as:
//    IndexedVar(4) < IndexedVar(1)
//  with associated indexVarMap:
//    [-1 1 -1 -1 0]  // 1->1, 4->0
//  This is effectively equivalent to "IndexedVar(0) < IndexedVar(1)"; 0 means
//  the first output column (B), 1 means the second output column (D).
//
//  To get an index var map that refers to the internal schema:
//    reverseProjection(
//      [1 3],           // OutputColumns
//      [-1 1 -1 -1 0],
//    ) =
//      [-1 3 -1 -1 1]   // 1->3, 4->1
//  This is effectively equivalent to "IndexedVar(1) < IndexedVar(3)"; 1
//  means the second internal column (B), 3 means the fourth internal column
//  (D).
func reverseProjection(outputColumns []uint32, indexVarMap []int) []int {
	if len(outputColumns) == 0 {
		// No projection.
		return indexVarMap
	}
	compositeMap := make([]int, len(indexVarMap))
	for i, col := range indexVarMap {
		if col == -1 {
			compositeMap[i] = -1
		} else {
			compositeMap[i] = int(outputColumns[col])
		}
	}
	return compositeMap
}

// addFilter adds a filter on the output of a plan. The filter is added either
// as a post-processing step to the last stage or to a new "no-op" stage, as
// necessary.
func (dsp *distSQLPlanner) addFilter(p *physicalPlan, expr parser.TypedExpr, indexVarMap []int) {
	post := p.getLastStagePost()
	if len(post.RenderExprs) > 0 {
		// The last stage contains render expressions. The filter refers to the
		// output of these, so we need to add another "no-op" stage to which to
		// attach the filter.
		post = distsqlrun.PostProcessSpec{}
		dsp.addNoGroupingStage(
			p,
			distsqlrun.ProcessorCoreUnion{Noop: &distsqlrun.NoopCoreSpec{}},
			post,
			p.resultTypes,
		)
	}

	compositeMap := reverseProjection(post.OutputColumns, indexVarMap)
	filter := distSQLExpression(expr, compositeMap)
	if post.Filter.Expr != "" {
		filter.Expr = fmt.Sprintf("(%s) AND (%s)", post.Filter.Expr, filter.Expr)
	}
	for _, pIdx := range p.resultRouters {
		p.processors[pIdx].spec.Post.Filter = filter
	}
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
func initTableReaderSpec(n *scanNode) (distsqlrun.TableReaderSpec, error) {
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
			return distsqlrun.TableReaderSpec{}, err
		}
	}
	if n.limitSoft {
		s.SoftLimit = n.limitHint
	} else {
		s.HardLimit = n.limitHint
	}
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
			// This column is not part of the output. The rest of the ordering is
			// irrelevant.
			break
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
	spec, err := initTableReaderSpec(n)
	if err != nil {
		return physicalPlan{}, err
	}

	post := distsqlrun.PostProcessSpec{
		Filter: distSQLExpression(n.filter, nil),
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

	p := physicalPlan{
		ordering:           ordering,
		planToStreamColMap: planToStreamColMap,
		resultTypes:        getTypesForPlanResult(n, planToStreamColMap),
	}
	for _, sp := range spanPartitions {
		proc := processor{
			node: sp.node,
		}

		tr := &distsqlrun.TableReaderSpec{}
		*tr = spec
		tr.Spans = make([]distsqlrun.TableReaderSpan, len(sp.spans))
		for i := range sp.spans {
			tr.Spans[i].Span = sp.spans[i]
		}

		proc.spec.Core.SetValue(tr)
		proc.spec.Post = post
		proc.spec.Output = make([]distsqlrun.OutputRouterSpec, 1)
		proc.spec.Output[0].Type = distsqlrun.OutputRouterSpec_PASS_THROUGH

		pIdx := p.addProcessor(proc)
		p.resultRouters = append(p.resultRouters, pIdx)
	}
	return p, nil
}

// addNoGroupingStage adds a processor for each result router, on the same node
// with the source of the stream; all processors have the same core. This is for
// stages that correspond to logical blocks that don't require any grouping
// (e.g. evaluator, sorting, etc).
// The caller needs to update p.planToStreamColMap, p.ordering.
func (dsp *distSQLPlanner) addNoGroupingStage(
	p *physicalPlan,
	core distsqlrun.ProcessorCoreUnion,
	post distsqlrun.PostProcessSpec,
	outputTypes []sqlbase.ColumnType,
) {
	for i, resultProc := range p.resultRouters {
		prevProc := &p.processors[resultProc]

		proc := processor{
			node: prevProc.node,
			spec: distsqlrun.ProcessorSpec{
				Input: []distsqlrun.InputSyncSpec{{
					Type:        distsqlrun.InputSyncSpec_UNORDERED,
					ColumnTypes: p.resultTypes,
				}},
				Core: core,
				Post: post,
				Output: []distsqlrun.OutputRouterSpec{{
					Type: distsqlrun.OutputRouterSpec_PASS_THROUGH,
				}},
			},
		}

		pIdx := p.addProcessor(proc)

		p.streams = append(p.streams, stream{
			sourceProcessor:  resultProc,
			destProcessor:    pIdx,
			sourceRouterSlot: 0,
			destInput:        0,
		})

		p.resultRouters[i] = pIdx
	}
	p.resultTypes = outputTypes
}

// addRendering adds a rendering (expression evaluation) to the output of a
// plan. The rendering is achieved either through an adjustment on the last
// stage post-process spec, or via a new stage.
// The caller must update p.ordering, p.planToStreamColMap.
func (dsp *distSQLPlanner) addRendering(
	planCtx *planningCtx,
	p *physicalPlan,
	exprs []parser.TypedExpr,
	indexVarMap []int,
	outTypes []sqlbase.ColumnType,
) {
	// First check if we need an Evaluator, or we are just shuffling values.
	needRendering := false

	for _, e := range exprs {
		if _, ok := e.(*parser.IndexedVar); !ok {
			needRendering = true
			break
		}
	}

	post := p.getLastStagePost()
	if !needRendering {
		// We don't need to do any rendering; we just need to adjust the projection
		// to output only the columns in the rendering.

		oldOutCols := post.OutputColumns
		newOutCols := make([]uint32, len(exprs))
		for i, e := range exprs {
			idx := e.(*parser.IndexedVar).Idx
			streamCol := indexVarMap[idx]
			if streamCol == -1 {
				panic(fmt.Sprintf("render %d refers to column %d not in source", i, idx))
			}
			if oldOutCols != nil {
				newOutCols[i] = oldOutCols[streamCol]
			} else {
				newOutCols[i] = uint32(streamCol)
			}
		}

		if post.RenderExprs != nil {
			oldRenders := post.RenderExprs
			// Apply the new projection to the existing rendering; in other words,
			// keep only the renders needed by the new output columns, and reorder
			// them accordingly.
			post.RenderExprs = make([]distsqlrun.Expression, len(newOutCols))
			for i, c := range newOutCols {
				post.RenderExprs[i] = oldRenders[c]
			}
		} else {
			// We didn't have a rendering before and we also don't need one for the
			// new expressions. Simply overwrite the projection with the columns
			// needed indicated by the new expressions.
			post.OutputColumns = newOutCols
		}
	} else {
		if len(post.RenderExprs) > 0 {
			post = distsqlrun.PostProcessSpec{}
			// The last stage contains render expressions. The new renders refer to
			// the output of these, so we need to add another "no-op" stage to which
			// to attach the new rendering.
			dsp.addNoGroupingStage(
				p,
				distsqlrun.ProcessorCoreUnion{Noop: &distsqlrun.NoopCoreSpec{}},
				post,
				p.resultTypes,
			)
		}

		compositeMap := reverseProjection(post.OutputColumns, indexVarMap)
		post.RenderExprs = make([]distsqlrun.Expression, len(exprs))
		for i, e := range exprs {
			post.RenderExprs[i] = distSQLExpression(e, compositeMap)
		}
		post.OutputColumns = nil
	}
	p.setLastStagePost(post, outTypes)
}

// selectRenders takes a physicalPlan that produces the results corresponding to
// the select data source (a n.source) and updates it to produce results
// corresponding to the render node itself. An evaluator stage is added if the
// render node has any expressions which are not just simple column references.
func (dsp *distSQLPlanner) selectRenders(planCtx *planningCtx, p *physicalPlan, n *renderNode) {
	dsp.addRendering(planCtx, p, n.render, p.planToStreamColMap, getTypesForPlanResult(n, nil))

	// Update p.planToStreamColMap; we will have a simple 1-to-1 mapping of
	// planNode columns to stream columns because the evaluator has been
	// programmed to produce the columns in renderNode.render order.
	p.planToStreamColMap = p.planToStreamColMap[:0]
	for i := range n.render {
		p.planToStreamColMap = append(p.planToStreamColMap, i)
	}

	p.ordering = dsp.convertOrdering(n.ordering.ordering, p.planToStreamColMap)
}

// addSorters adds sorters corresponding to a sortNode and updates the plan to
// reflect the sort node.
func (dsp *distSQLPlanner) addSorters(planCtx *planningCtx, p *physicalPlan, n *sortNode) {
	sorterSpec := distsqlrun.SorterSpec{
		OutputOrdering: dsp.convertOrdering(n.ordering, p.planToStreamColMap),
		OrderingMatchLen: uint32(computeOrderingMatch(
			n.ordering, n.plan.Ordering(), false, /* reverse */
		)),
	}
	if len(sorterSpec.OutputOrdering.Columns) != len(n.ordering) {
		panic(fmt.Sprintf(
			"not all columns in sort ordering available: %v; %v",
			n.ordering, sorterSpec.OutputOrdering.Columns,
		))
	}

	var post distsqlrun.PostProcessSpec
	if len(n.columns) != len(p.planToStreamColMap) {
		// In cases like:
		//   SELECT a FROM t ORDER BY b
		// we have columns (b) that are only used for sorting. These columns are not
		// in the output columns of the sortNode; we set a projection on the
		// processors we just added.
		p.planToStreamColMap = p.planToStreamColMap[:len(n.columns)]
		post.OutputColumns = make([]uint32, len(n.columns))
		for i, col := range p.planToStreamColMap {
			post.OutputColumns[i] = uint32(col)
			p.planToStreamColMap[i] = i
		}
	}

	dsp.addNoGroupingStage(
		p,
		distsqlrun.ProcessorCoreUnion{Sorter: &sorterSpec},
		post,
		getTypesForPlanResult(n, p.planToStreamColMap),
	)
	p.ordering = sorterSpec.OutputOrdering
}

// addSingleGroupStage adds a "single group" stage (one that cannot be
// parallelized) which consists of a single processor on the specified node.
// The caller needs to update p.planToStreamColMap, p.resultTypes, p.ordering.
func (dsp *distSQLPlanner) addSingleGroupStage(
	p *physicalPlan,
	nodeID roachpb.NodeID,
	core distsqlrun.ProcessorCoreUnion,
	post distsqlrun.PostProcessSpec,
	outputTypes []sqlbase.ColumnType,
) {
	proc := processor{
		node: nodeID,
		spec: distsqlrun.ProcessorSpec{
			Input: []distsqlrun.InputSyncSpec{{
				// The other fields will be filled in by mergeResultStreams.
				ColumnTypes: p.resultTypes,
			}},
			Core: core,
			Post: post,
			Output: []distsqlrun.OutputRouterSpec{{
				Type: distsqlrun.OutputRouterSpec_PASS_THROUGH,
			}},
		},
	}

	pIdx := p.addProcessor(proc)

	// Connect the result routers to the no-op processor.
	dsp.mergeResultStreams(p, p.resultRouters, 0, p.ordering, pIdx, 0)

	// We now have a single result stream.
	p.resultRouters = p.resultRouters[:1]
	p.resultRouters[0] = pIdx

	p.resultTypes = outputTypes
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
	prevStageNode := p.processors[p.resultRouters[0]].node
	for i := 1; i < len(p.resultRouters); i++ {
		if n := p.processors[p.resultRouters[i]].node; n != prevStageNode {
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

	// The aggregators don't care about the input ordering, so don't guarantee
	// one (p.ordering gets factored in when we add stages). This one-off
	// optimization is a symptom of the larger issue that we don't yet determine
	// what orderings are actually needed by each node's parent.
	//
	// Note that this won't be the case if we implement certain optimizations
	// (like groupNode.needOnlyOneRow).
	p.ordering = distsqlrun.Ordering{}

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
			_, intermediateTypes[i], err = distsqlrun.GetAggregateInfo(e.Func, p.resultTypes[e.ColIdx])
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
			intermediateTypes[exprIdx] = p.resultTypes[groupColIdx]
			finalGroupCols[i] = uint32(exprIdx)
		}

		localAggSpec := distsqlrun.AggregatorSpec{
			Aggregations: localAgg,
			GroupCols:    groupCols,
		}

		dsp.addNoGroupingStage(
			p,
			distsqlrun.ProcessorCoreUnion{Aggregator: &localAggSpec},
			distsqlrun.PostProcessSpec{},
			intermediateTypes,
		)
		// The local aggregators don't guarantee any output ordering.
		p.ordering = orderingTerminated

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
		_, finalOutTypes[i], err = distsqlrun.GetAggregateInfo(agg.Func, p.resultTypes[agg.ColIdx])
		if err != nil {
			return err
		}
	}
	dsp.addSingleGroupStage(
		p, node,
		distsqlrun.ProcessorCoreUnion{Aggregator: &finalAggSpec},
		distsqlrun.PostProcessSpec{},
		finalOutTypes,
	)

	evalExprs := dsp.extractPostAggrExprs(n.render)
	dsp.addRendering(planCtx, p, evalExprs, p.planToStreamColMap, getTypesForPlanResult(n, nil))

	// Update p.planToStreamColMap; we will have a simple 1-to-1 mapping of
	// planNode columns to stream columns because the aggregator (and possibly
	// evaluator) have been programmed to produce the columns in order.
	p.planToStreamColMap = p.planToStreamColMap[:0]
	for i := range n.Columns() {
		p.planToStreamColMap = append(p.planToStreamColMap, i)
	}

	// We don't guarantee any ordering. Thankfully the groupNode doesn't either.
	p.ordering = orderingTerminated
	if len(n.Ordering().ordering) != 0 {
		panic("groupNode promises ordering")
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
		Filter:        distSQLExpression(n.table.filter, nil),
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

	dsp.addSingleGroupStage(
		&plan,
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

	p, leftRouters, rightRouters := mergePlans(&leftPlan, &rightPlan)

	joinToStreamColMap := makePlanToStreamColMap(len(n.columns))

	// Nodes where we will run the join processors.
	var nodes []roachpb.NodeID
	var joinerSpec distsqlrun.HashJoinerSpec

	if n.joinType != joinTypeInner {
		panic("only inner join supported for now")
	}
	joinerSpec.Type = distsqlrun.JoinType_INNER

	// Figure out the left and right types.
	leftTypes := leftPlan.resultTypes
	rightTypes := rightPlan.resultTypes

	// Set up the output columns.
	if numEq := len(n.pred.leftEqualityIndices); numEq != 0 {
		// TODO(radu): for now we run a join processor on every node that produces
		// data for either source. In the future we should be smarter here.
		seen := make(map[roachpb.NodeID]struct{})
		for _, pIdx := range leftRouters {
			n := p.processors[pIdx].node
			if _, ok := seen[n]; !ok {
				seen[n] = struct{}{}
				nodes = append(nodes, n)
			}
		}
		for _, pIdx := range rightRouters {
			n := p.processors[pIdx].node
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
		joinerSpec.OnExpr = distSQLExpression(n.pred.onCond, joinColMap)
	}

	pIdxStart := processorIdx(len(p.processors))

	if len(nodes) == 1 {
		procSpec := distsqlrun.ProcessorSpec{
			Input: []distsqlrun.InputSyncSpec{
				{ColumnTypes: leftTypes},
				{ColumnTypes: rightTypes},
			},
			Core:   distsqlrun.ProcessorCoreUnion{HashJoiner: &joinerSpec},
			Post:   post,
			Output: []distsqlrun.OutputRouterSpec{{Type: distsqlrun.OutputRouterSpec_PASS_THROUGH}},
		}

		p.processors = append(p.processors, processor{node: nodes[0], spec: procSpec})
	} else {
		// Parallel hash join: we distribute rows (by hash of equality columns) to
		// len(nodes) join processors.

		// Each node has a join processor.
		for _, n := range nodes {
			procSpec := distsqlrun.ProcessorSpec{
				Input: []distsqlrun.InputSyncSpec{
					{ColumnTypes: leftTypes},
					{ColumnTypes: rightTypes},
				},
				Core:   distsqlrun.ProcessorCoreUnion{HashJoiner: &joinerSpec},
				Post:   post,
				Output: []distsqlrun.OutputRouterSpec{{Type: distsqlrun.OutputRouterSpec_PASS_THROUGH}},
			}
			p.processors = append(p.processors, processor{node: n, spec: procSpec})
		}

		// Set up the left routers.
		for _, resultProc := range leftRouters {
			p.processors[resultProc].spec.Output[0] = distsqlrun.OutputRouterSpec{
				Type:        distsqlrun.OutputRouterSpec_BY_HASH,
				HashColumns: joinerSpec.LeftEqColumns,
			}
		}
		// Set up the right routers.
		for _, resultProc := range rightRouters {
			p.processors[resultProc].spec.Output[0] = distsqlrun.OutputRouterSpec{
				Type:        distsqlrun.OutputRouterSpec_BY_HASH,
				HashColumns: joinerSpec.RightEqColumns,
			}
		}
	}
	p.resultRouters = p.resultRouters[:0]

	// Connect the left and right routers to the output joiners. Each joiner
	// corresponds to a hash bucket.
	for bucket := 0; bucket < len(nodes); bucket++ {
		pIdx := pIdxStart + processorIdx(bucket)

		// Connect left routers to the processor's first input. Currently the join
		// node doesn't care about the orderings of the left and right results.
		dsp.mergeResultStreams(&p, leftRouters, bucket, distsqlrun.Ordering{}, pIdx, 0)
		// Connect right routers to the processor's second input.
		dsp.mergeResultStreams(&p, rightRouters, bucket, distsqlrun.Ordering{}, pIdx, 1)

		p.resultRouters = append(p.resultRouters, pIdx)
	}

	p.planToStreamColMap = joinToStreamColMap
	p.resultTypes = getTypesForPlanResult(n, joinToStreamColMap)
	p.ordering = dsp.convertOrdering(n.Ordering().ordering, joinToStreamColMap)
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
		dsp.selectRenders(planCtx, &plan, n)
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

		dsp.addSorters(planCtx, &plan, n)

		return plan, nil

	case *filterNode:
		plan, err := dsp.createPlanForNode(planCtx, n.source.plan)
		if err != nil {
			return physicalPlan{}, err
		}

		dsp.addFilter(&plan, n.filter, plan.planToStreamColMap)

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
	sourceRouterSlot int,
	ordering distsqlrun.Ordering,
	destProcessor processorIdx,
	destInput int,
) {
	proc := &p.processors[destProcessor]
	if len(ordering.Columns) == 0 || len(resultRouters) == 1 {
		proc.spec.Input[destInput].Type = distsqlrun.InputSyncSpec_UNORDERED
	} else {
		proc.spec.Input[destInput].Type = distsqlrun.InputSyncSpec_ORDERED
		proc.spec.Input[destInput].Ordering = ordering
	}

	for _, resultProc := range resultRouters {
		p.streams = append(p.streams, stream{
			sourceProcessor:  resultProc,
			sourceRouterSlot: sourceRouterSlot,
			destProcessor:    destProcessor,
			destInput:        destInput,
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
		endpoint := distsqlrun.StreamEndpointSpec{StreamID: distsqlrun.StreamID(sIdx)}
		if p1.node == p2.node {
			endpoint.Type = distsqlrun.StreamEndpointSpec_LOCAL
		} else {
			endpoint.Type = distsqlrun.StreamEndpointSpec_REMOTE
		}
		p2.spec.Input[s.destInput].Streams = append(p2.spec.Input[s.destInput].Streams, endpoint)
		if endpoint.Type == distsqlrun.StreamEndpointSpec_REMOTE {
			endpoint.TargetAddr = planCtx.nodeAddresses[p2.node]
		}

		router := &p1.spec.Output[0]
		// We are about to put this stream on the len(router.Streams) position in
		// the router; verify this matches the sourceRouterSlot. We expect it to
		// because the streams should be in order; if that assumption changes we can
		// reorder them here according to sourceRouterSlot.
		if len(router.Streams) != s.sourceRouterSlot {
			panic(fmt.Sprintf(
				"sourceRouterSlot mismatch: %d, expected %d", len(router.Streams), s.sourceRouterSlot,
			))
		}
		router.Streams = append(router.Streams, endpoint)
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
	setUnlimited(tree)

	planCtx := planningCtx{
		ctx:           ctx,
		spanIter:      dsp.spanResolver.NewSpanResolverIterator(),
		nodeAddresses: make(map[roachpb.NodeID]string),
	}
	thisNodeID := dsp.nodeDesc.NodeID
	planCtx.nodeAddresses[thisNodeID] = dsp.nodeDesc.Address.String()

	log.VEvent(ctx, 1, "creating DistSQL plan")

	plan, err := dsp.createPlanForNode(&planCtx, tree)
	if err != nil {
		return err
	}

	// If we don't already have a single result router on this node, add a final
	// stage.
	if len(plan.resultRouters) != 1 ||
		plan.processors[plan.resultRouters[0]].node != thisNodeID {
		dsp.addSingleGroupStage(
			&plan,
			thisNodeID,
			distsqlrun.ProcessorCoreUnion{Noop: &distsqlrun.NoopCoreSpec{}},
			distsqlrun.PostProcessSpec{},
			plan.resultTypes,
		)
		if len(plan.resultRouters) != 1 {
			panic(fmt.Sprintf("%d results after single group stage", len(plan.resultRouters)))
		}
	}

	// Set up the endpoints for p.streams.
	dsp.populateEndpoints(&planCtx, &plan)

	// Set up the endpoint for the final result.
	finalOut := &plan.processors[plan.resultRouters[0]].spec.Output[0]
	finalOut.Streams = append(finalOut.Streams, distsqlrun.StreamEndpointSpec{
		Type: distsqlrun.StreamEndpointSpec_SYNC_RESPONSE,
	})

	recv.resultToStreamColMap = plan.planToStreamColMap

	// Split the processors by nodeID to create the FlowSpecs.
	flowID := distsqlrun.FlowID{UUID: uuid.MakeV4()}
	nodeIDMap := make(map[roachpb.NodeID]int)
	// nodeAddresses contains addresses for the nodes that were referenced during
	// planning, so we're likely going to have this many nodes (and we have one
	// flow per node).
	nodeIDs := make([]roachpb.NodeID, 0, len(planCtx.nodeAddresses))
	flows := make([]distsqlrun.FlowSpec, 0, len(planCtx.nodeAddresses))

	for _, p := range plan.processors {
		idx, ok := nodeIDMap[p.node]
		if !ok {
			flow := distsqlrun.FlowSpec{FlowID: flowID}
			idx = len(flows)
			flows = append(flows, flow)
			nodeIDs = append(nodeIDs, p.node)
			nodeIDMap[p.node] = idx
		}
		flows[idx].Processors = append(flows[idx].Processors, p.spec)
	}

	if logPlanDiagram {
		log.VEvent(ctx, 1, "creating plan diagram")
		nodeNames := make([]string, len(nodeIDs))
		for i, n := range nodeIDs {
			nodeNames[i] = n.String()
		}

		var buf bytes.Buffer
		if err := distsqlrun.GeneratePlanDiagram(flows, nodeNames, &buf); err != nil {
			log.Infof(ctx, "Error generating diagram: %s", err)
		} else {
			log.Infof(ctx, "Plan diagram JSON:\n%s", buf.String())
		}
	}

	log.VEvent(ctx, 1, "running DistSQL plan")

	// Start the flows on all other nodes.
	for i, nodeID := range nodeIDs {
		if nodeID == thisNodeID {
			// Skip this node.
			continue
		}
		req := distsqlrun.SetupFlowRequest{
			Txn:  txn.Proto,
			Flow: flows[i],
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
		Txn:  txn.Proto,
		Flow: flows[nodeIDMap[thisNodeID]],
	}
	if err := distsqlrun.SetFlowRequestTrace(ctx, &localReq); err != nil {
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
	// resultToStreamColMap maps result columns to columns in the distsqlrun results
	// stream.
	resultToStreamColMap []int
	// numRows counts the number of rows we received when rows is nil.
	numRows int64
	err     error
	row     parser.DTuple
	alloc   sqlbase.DatumAlloc
	closed  bool
}

var _ distsqlrun.RowReceiver = &distSQLReceiver{}

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
		err := row[resIdx].EnsureDecoded(&r.alloc)
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
