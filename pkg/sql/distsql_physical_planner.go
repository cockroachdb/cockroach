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

package sql

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// DistSQLPlanner is used to generate distributed plans from logical
// plans. A rough overview of the process:
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
type DistSQLPlanner struct {
	// planVersion is the version of DistSQL targeted by the plan we're building.
	// This is currently only assigned to the node's current DistSQL version and
	// is used to skip incompatible nodes when mapping spans.
	planVersion distsqlrun.DistSQLVersion

	st *cluster.Settings
	// The node descriptor for the gateway node that initiated this query.
	nodeDesc     roachpb.NodeDescriptor
	rpcContext   *rpc.Context
	stopper      *stop.Stopper
	distSQLSrv   *distsqlrun.ServerImpl
	spanResolver distsqlplan.SpanResolver
	testingKnobs DistSQLPlannerTestingKnobs

	// runnerChan is used to send out requests (for running SetupFlow RPCs) to a
	// pool of workers.
	runnerChan chan runnerRequest

	// gossip handle use to check node version compatibility
	gossip *gossip.Gossip
}

const resolverPolicy = distsqlplan.BinPackingLeaseHolderChoice

// If true, the plan diagram (in JSON) is logged for each plan (used for
// debugging).
var logPlanDiagram = envutil.EnvOrDefaultBool("COCKROACH_DISTSQL_LOG_PLAN", false)

// If true, for index joins  we instantiate a join reader on every node that
// has a stream (usually from a table reader). If false, there is a single join
// reader.
var distributeIndexJoin = settings.RegisterBoolSetting(
	"sql.distsql.distribute_index_joins",
	"if set, for index joins we instantiate a join reader on every node that has a "+
		"stream; if not set, we use a single join reader",
	true,
)

var planMergeJoins = settings.RegisterBoolSetting(
	"sql.distsql.merge_joins.enabled",
	"if set, we plan merge joins when possible",
	true,
)

// NewDistSQLPlanner initializes a DistSQLPlanner
func NewDistSQLPlanner(
	ctx context.Context,
	planVersion distsqlrun.DistSQLVersion,
	st *cluster.Settings,
	nodeDesc roachpb.NodeDescriptor,
	rpcCtx *rpc.Context,
	distSQLSrv *distsqlrun.ServerImpl,
	distSender *kv.DistSender,
	gossip *gossip.Gossip,
	stopper *stop.Stopper,
	testingKnobs DistSQLPlannerTestingKnobs,
) *DistSQLPlanner {
	log.Infof(ctx, "creating DistSQLPlanner with address %s", nodeDesc.Address)
	dsp := &DistSQLPlanner{
		planVersion:  planVersion,
		st:           st,
		nodeDesc:     nodeDesc,
		rpcContext:   rpcCtx,
		stopper:      stopper,
		distSQLSrv:   distSQLSrv,
		gossip:       gossip,
		spanResolver: distsqlplan.NewSpanResolver(distSender, gossip, nodeDesc, resolverPolicy),
		testingKnobs: testingKnobs,
	}
	dsp.initRunners()
	return dsp
}

// setSpanResolver switches to a different SpanResolver. It is the caller's
// responsibility to make sure the DistSQLPlanner is not in use.
func (dsp *DistSQLPlanner) setSpanResolver(spanResolver distsqlplan.SpanResolver) {
	dsp.spanResolver = spanResolver
}

// distSQLExprCheckVisitor is a tree.Visitor that checks if expressions
// contain things not supported by distSQL (like subqueries).
type distSQLExprCheckVisitor struct {
	err error
}

var _ tree.Visitor = &distSQLExprCheckVisitor{}

func (v *distSQLExprCheckVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if v.err != nil {
		return false, expr
	}
	switch t := expr.(type) {
	case *subquery, *tree.Subquery:
		v.err = newQueryNotSupportedError("subqueries not supported yet")
		return false, expr

	case *tree.FuncExpr:
		if t.IsDistSQLBlacklist() {
			v.err = newQueryNotSupportedErrorf("function %s cannot be executed with distsql", t)
			return false, expr
		}
	}
	return true, expr
}

func (v *distSQLExprCheckVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

// checkExpr verifies that an expression doesn't contain things that are not yet
// supported by distSQL, like subqueries.
func (dsp *DistSQLPlanner) checkExpr(expr tree.Expr) error {
	if expr == nil {
		return nil
	}
	v := distSQLExprCheckVisitor{}
	tree.WalkExprConst(&v, expr)
	return v.err
}

// CheckSupport looks at a planNode tree and decides:
//  - whether DistSQL is equipped to handle the query (if not, an error is
//    returned).
//  - whether it is recommended that the query be run with DistSQL.
func (dsp *DistSQLPlanner) CheckSupport(node planNode) (bool, error) {
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

type queryNotSupportedError struct {
	msg string
}

func (e *queryNotSupportedError) Error() string {
	return e.msg
}

func newQueryNotSupportedError(msg string) error {
	return &queryNotSupportedError{msg: msg}
}

func newQueryNotSupportedErrorf(format string, args ...interface{}) error {
	return &queryNotSupportedError{msg: fmt.Sprintf(format, args...)}
}

var mutationsNotSupportedError = newQueryNotSupportedError("mutations not supported")
var setNotSupportedError = newQueryNotSupportedError("SET / SET CLUSTER SETTING should never distribute")

// leafType returns the element type if the given type is an array, and the type
// itself otherwise.
func leafType(t types.T) types.T {
	if a, ok := t.(types.TArray); ok {
		return leafType(a.Typ)
	}
	return t
}

// checkSupportForNode returns a distRecommendation (as described above) or an
// error if the plan subtree is not supported by DistSQL.
// TODO(radu): add tests for this.
func (dsp *DistSQLPlanner) checkSupportForNode(node planNode) (distRecommendation, error) {
	switch n := node.(type) {
	case *filterNode:
		if err := dsp.checkExpr(n.filter); err != nil {
			return 0, err
		}
		return dsp.checkSupportForNode(n.source.plan)

	case *renderNode:
		for i, e := range n.render {
			typ := n.columns[i].Typ
			if leafType(typ).FamilyEqual(types.FamTuple) {
				return 0, newQueryNotSupportedErrorf("unsupported render type %s", typ)
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
		if len(n.spans) == 1 && n.spans[0].EqualValue(n.desc.IndexSpan(n.index.ID)) {
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
		for _, fholder := range n.funcs {
			if f, ok := fholder.expr.(*tree.FuncExpr); ok {
				if strings.ToUpper(f.Func.FunctionReference.String()) == "ARRAY_AGG" {
					return 0, newQueryNotSupportedError("ARRAY_AGG aggregation not supported yet")
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

	case *valuesNode:
		if n.n == nil {
			return 0, newQueryNotSupportedErrorf("unsupported node %T without SQL VALUES clause", node)
		}

		for _, tuple := range n.tuples {
			for _, expr := range tuple {
				if err := dsp.checkExpr(expr); err != nil {
					return 0, err
				}
			}
		}
		return shouldDistribute, nil

	case *createStatsNode:
		return shouldDistribute, nil

	case *insertNode, *updateNode, *deleteNode:
		// This is a potential hot path.
		return 0, mutationsNotSupportedError

	case *setVarNode, *setClusterSettingNode:
		// SET statements are never distributed.
		return 0, setNotSupportedError

	default:
		return 0, newQueryNotSupportedErrorf("unsupported node %T", node)
	}
}

// planningCtx contains data used and updated throughout the planning process of
// a single query.
type planningCtx struct {
	ctx      context.Context
	evalCtx  *tree.EvalContext
	spanIter distsqlplan.SpanResolverIterator
	// nodeAddresses contains addresses for all NodeIDs that are referenced by any
	// physicalPlan we generate with this context.
	// Nodes that fail a health check have empty addresses.
	nodeAddresses map[roachpb.NodeID]string
}

// sanityCheckAddresses returns an error if the same address is used by two
// nodes.
func (p *planningCtx) sanityCheckAddresses() error {
	inverted := make(map[string]roachpb.NodeID)
	for nodeID, addr := range p.nodeAddresses {
		if otherNodeID, ok := inverted[addr]; ok {
			return util.UnexpectedWithIssueErrorf(
				12876,
				"different nodes %d and %d with the same address '%s'", nodeID, otherNodeID, addr)
		}
		inverted[addr] = nodeID
	}
	return nil
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

// orderingTerminated is used when streams can be joined without needing to be
// merged with respect to a particular ordering.
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

// identityMap returns the slice {0, 1, 2, ..., numCols-1}.
// buf can be optionally provided as a buffer.
func identityMap(buf []int, numCols int) []int {
	buf = buf[:0]
	for i := 0; i < numCols; i++ {
		buf = append(buf, i)
	}
	return buf
}

// identityMapInPlace returns the modified slice such that it contains
// {0, 1, ..., len(slice)-1}.
func identityMapInPlace(slice []int) []int {
	for i := range slice {
		slice[i] = i
	}
	return slice
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
//
// partitionSpans does its best to not assign ranges on nodes that are known to
// either be unhealthy or running an incompatible version. The ranges owned by
// such nodes are assigned to the gateway.
func (dsp *DistSQLPlanner) partitionSpans(
	planCtx *planningCtx, spans roachpb.Spans,
) ([]spanPartition, error) {
	if len(spans) == 0 {
		panic("no spans")
	}
	ctx := planCtx.ctx
	partitions := make([]spanPartition, 0, 1)
	// nodeMap maps a nodeID to an index inside the partitions array.
	nodeMap := make(map[roachpb.NodeID]int)
	// nodeVerCompatMap maintains info about which nodes advertise DistSQL
	// versions compatible with this plan and which ones don't.
	nodeVerCompatMap := make(map[roachpb.NodeID]bool)
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
		if log.V(1) {
			log.Infof(ctx, "partitioning span %s", span)
		}
		for it.Seek(ctx, span, kv.Ascending); ; it.Next(ctx) {
			if !it.Valid() {
				return nil, it.Error()
			}
			replInfo, err := it.ReplicaInfo(ctx)
			if err != nil {
				return nil, err
			}
			desc := it.Desc()
			if log.V(1) {
				log.Infof(ctx, "lastKey: %s desc: %s", lastKey, desc)
			}

			if !desc.ContainsKey(lastKey) {
				// This range must contain the last range's EndKey.
				log.Fatalf(
					ctx, "next range %v doesn't cover last end key %v. Partitions: %#v",
					desc.RSpan(), lastKey, partitions,
				)
			}

			// Limit the end key to the end of the span we are resolving.
			endKey := desc.EndKey
			if rspan.EndKey.Less(endKey) {
				endKey = rspan.EndKey
			}

			nodeID := replInfo.NodeDesc.NodeID
			partitionIdx, inNodeMap := nodeMap[nodeID]
			if !inNodeMap {
				// This is the first time we are seeing nodeID for these spans. Check
				// its health.
				addr, inAddrMap := planCtx.nodeAddresses[nodeID]
				if !inAddrMap {
					addr = replInfo.NodeDesc.Address.String()
					checkNodeHealth := func() error {
						// Check if the node is still in gossip - i.e. if it hasn't been
						// decommissioned or overridden by another node at the same address.
						if _, err := dsp.gossip.GetNodeIDAddress(nodeID); err != nil {
							log.VEventf(ctx, 1, "not using n%d because gossip doesn't know about it. "+
								"It might have gone away from the cluster. Gossip said: %s.", nodeID, err)
							return err
						}

						var err error
						if dsp.testingKnobs.OverrideHealthCheck != nil {
							err = dsp.testingKnobs.OverrideHealthCheck(replInfo.NodeDesc.NodeID, addr)
						} else {
							err = dsp.rpcContext.ConnHealth(addr)
						}
						if err != nil && err != rpc.ErrNotConnected && err != rpc.ErrNotHeartbeated {
							// This host is known to be unhealthy. Don't use it (use the gateway
							// instead). Note: this can never happen for our nodeID (which
							// always has its address in the nodeMap).
							log.VEventf(ctx, 1, "marking n%d as unhealthy for this plan: %v", nodeID, err)
							return err
						}
						return nil
					}
					if err := checkNodeHealth(); err != nil {
						addr = ""
					}
					if err == nil && addr != "" {
						planCtx.nodeAddresses[nodeID] = addr
					}
				}
				compat := true
				if addr != "" {
					// Check if the node's DistSQL version is compatible with this plan.
					// If it isn't, we'll use the gateway.
					var ok bool
					if compat, ok = nodeVerCompatMap[nodeID]; !ok {
						compat = dsp.nodeVersionIsCompatible(nodeID, dsp.planVersion)
						nodeVerCompatMap[nodeID] = compat
					}
				}
				// If the node is unhealthy or its DistSQL version is incompatible, use
				// the gateway to process this span instead of the unhealthy host.
				// An empty address indicates an unhealthy host.
				if addr == "" || !compat {
					log.Eventf(ctx, "not planning on node %d. unhealthy: %t, incompatible version: %t",
						nodeID, addr == "", !compat)
					nodeID = dsp.nodeDesc.NodeID
					partitionIdx, inNodeMap = nodeMap[nodeID]
				}

				if !inNodeMap {
					partitionIdx = len(partitions)
					partitions = append(partitions, spanPartition{node: nodeID})
					nodeMap[nodeID] = partitionIdx
				}
			}
			partition := &partitions[partitionIdx]

			if lastNodeID == nodeID {
				// Two consecutive ranges on the same node, merge the spans.
				partition.spans[len(partition.spans)-1].EndKey = endKey.AsRawKey()
			} else {
				partition.spans = append(partition.spans, roachpb.Span{
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
	return partitions, nil
}

// nodeVersionIsCompatible decides whether a particular node's DistSQL version
// is compatible with planVer. It uses gossip to find out the node's version
// range.
func (dsp *DistSQLPlanner) nodeVersionIsCompatible(
	nodeID roachpb.NodeID, planVer distsqlrun.DistSQLVersion,
) bool {
	var v distsqlrun.DistSQLVersionGossipInfo
	if err := dsp.gossip.GetInfoProto(gossip.MakeDistSQLNodeVersionKey(nodeID), &v); err != nil {
		return false
	}
	return distsqlrun.FlowVerIsCompatible(dsp.planVersion, v.MinAcceptedVersion, v.Version)
}

// initTableReaderSpec initializes a TableReaderSpec/PostProcessSpec that
// corresponds to a scanNode, except for the Spans and OutputColumns.
func initTableReaderSpec(
	n *scanNode, evalCtx *tree.EvalContext,
) (distsqlrun.TableReaderSpec, distsqlrun.PostProcessSpec, error) {
	s := distsqlrun.TableReaderSpec{
		Table:   *n.desc,
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
		Filter: distsqlplan.MakeExpression(n.filter, evalCtx, nil),
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
	var outputColumns []uint32
	// TODO(radu): if we have a scan with a filter, valNeededForCol will include
	// the columns needed for the filter, even if they aren't needed for the
	// next stage.
	n.valNeededForCol.ForEach(func(i int) {
		outputColumns = append(outputColumns, uint32(i))
	})
	return outputColumns
}

// convertOrdering maps the columns in ord.ordering to the output columns of a
// processor.
func (dsp *DistSQLPlanner) convertOrdering(
	props physicalProps, planToStreamColMap []int,
) distsqlrun.Ordering {
	if len(props.ordering) == 0 {
		return distsqlrun.Ordering{}
	}
	result := distsqlrun.Ordering{
		Columns: make([]distsqlrun.Ordering_Column, len(props.ordering)),
	}
	for i, o := range props.ordering {
		streamColIdx := planToStreamColMap[o.ColIdx]
		if streamColIdx == -1 {
			// Find any column in the equivalency group that is part of the processor
			// output.
			group := props.eqGroups.Find(o.ColIdx)
			for col, pos := range planToStreamColMap {
				if pos != -1 && props.eqGroups.Find(col) == group {
					streamColIdx = pos
					break
				}
			}
			if streamColIdx == -1 {
				panic("column in ordering not part of processor output")
			}
		}
		result.Columns[i].ColIdx = uint32(streamColIdx)
		dir := distsqlrun.Ordering_Column_ASC
		if o.Direction == encoding.Descending {
			dir = distsqlrun.Ordering_Column_DESC
		}
		result.Columns[i].Direction = dir
	}
	return result
}

// createTableReaders generates a plan consisting of table reader processors,
// one for each node that has spans that we are reading.
// overridesResultColumns is optional.
func (dsp *DistSQLPlanner) createTableReaders(
	planCtx *planningCtx, n *scanNode, overrideResultColumns []uint32,
) (physicalPlan, error) {
	spec, post, err := initTableReaderSpec(n, planCtx.evalCtx)
	if err != nil {
		return physicalPlan{}, err
	}

	spanPartitions, err := dsp.partitionSpans(planCtx, n.spans)
	if err != nil {
		return physicalPlan{}, err
	}

	var p physicalPlan
	stageID := p.NewStageID()

	p.ResultRouters = make([]distsqlplan.ProcessorIdx, len(spanPartitions))
	for i, sp := range spanPartitions {
		tr := &distsqlrun.TableReaderSpec{}
		*tr = spec
		tr.Spans = make([]distsqlrun.TableReaderSpan, len(sp.spans))
		for j := range sp.spans {
			tr.Spans[j].Span = sp.spans[j]
		}

		proc := distsqlplan.Processor{
			Node: sp.node,
			Spec: distsqlrun.ProcessorSpec{
				Core:    distsqlrun.ProcessorCoreUnion{TableReader: tr},
				Output:  []distsqlrun.OutputRouterSpec{{Type: distsqlrun.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}

		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}

	planToStreamColMap := identityMapInPlace(make([]int, len(n.resultColumns)))

	if len(p.ResultRouters) > 1 && len(n.props.ordering) > 0 {
		// Make a note of the fact that we have to maintain a certain ordering
		// between the parallel streams.
		//
		// This information is taken into account by the AddProjection call below:
		// specifically, it will make sure these columns are kept even if they are
		// not in the projection (e.g. "SELECT v FROM kv ORDER BY k").
		p.SetMergeOrdering(dsp.convertOrdering(n.props, planToStreamColMap))
	}
	p.SetLastStagePost(post, getTypesForPlanResult(n, planToStreamColMap))

	outCols := overrideResultColumns
	if outCols == nil {
		outCols = getOutputColumnsFromScanNode(n)
	}
	p.AddProjection(outCols)

	post = p.GetLastStagePost()
	for i := range planToStreamColMap {
		planToStreamColMap[i] = -1
	}
	for i, col := range post.OutputColumns {
		planToStreamColMap[col] = i
	}
	p.planToStreamColMap = planToStreamColMap
	return p, nil
}

// selectRenders takes a physicalPlan that produces the results corresponding to
// the select data source (a n.source) and updates it to produce results
// corresponding to the render node itself. An evaluator stage is added if the
// render node has any expressions which are not just simple column references.
func (dsp *DistSQLPlanner) selectRenders(
	p *physicalPlan, n *renderNode, evalCtx *tree.EvalContext,
) {
	// We want to skip any unused renders.
	planToStreamColMap := makePlanToStreamColMap(len(n.render))
	renders := make([]tree.TypedExpr, 0, len(n.render))
	for i, r := range n.render {
		if !n.columns[i].Omitted {
			planToStreamColMap[i] = len(renders)
			renders = append(renders, r)
		}
	}

	p.AddRendering(renders, evalCtx, p.planToStreamColMap, getTypesForPlanResult(n, planToStreamColMap))
	p.planToStreamColMap = planToStreamColMap
}

// addSorters adds sorters corresponding to a sortNode and updates the plan to
// reflect the sort node.
func (dsp *DistSQLPlanner) addSorters(p *physicalPlan, n *sortNode) {

	matchLen := planPhysicalProps(n.plan).computeMatch(n.ordering)

	if matchLen < len(n.ordering) {
		// Sorting is needed; we add a stage of sorting processors.
		var ordering distsqlrun.Ordering
		ordering.Columns = make([]distsqlrun.Ordering_Column, len(n.ordering))
		for i, o := range n.ordering {
			streamColIdx := p.planToStreamColMap[o.ColIdx]
			if streamColIdx == -1 {
				panic(fmt.Sprintf("column %d in sort ordering not available", o.ColIdx))
			}
			ordering.Columns[i].ColIdx = uint32(streamColIdx)
			ordering.Columns[i].Direction = distsqlrun.Ordering_Column_ASC
			if o.Direction == encoding.Descending {
				ordering.Columns[i].Direction = distsqlrun.Ordering_Column_DESC
			}
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
func (dsp *DistSQLPlanner) addAggregators(
	planCtx *planningCtx, p *physicalPlan, n *groupNode,
) error {
	aggregations := make([]distsqlrun.AggregatorSpec_Aggregation, len(n.funcs))
	for i, fholder := range n.funcs {
		// An aggregateFuncHolder either contains an aggregation function or an
		// expression that also appears as one of the GROUP BY expressions.
		f, ok := fholder.expr.(*tree.FuncExpr)
		if !ok || f.GetAggregateConstructor() == nil {
			aggregations[i].Func = distsqlrun.AggregatorSpec_IDENT
		} else {
			// Convert the aggregate function to the enum value with the same string
			// representation.
			funcStr := strings.ToUpper(f.Func.FunctionReference.String())
			funcIdx, ok := distsqlrun.AggregatorSpec_Func_value[funcStr]
			if !ok {
				return errors.Errorf("unknown aggregate %s", funcStr)
			}
			aggregations[i].Func = distsqlrun.AggregatorSpec_Func(funcIdx)
			aggregations[i].Distinct = (f.Type == tree.DistinctFuncType)
		}
		if fholder.argRenderIdx != noRenderIdx {
			aggregations[i].ColIdx = []uint32{uint32(p.planToStreamColMap[fholder.argRenderIdx])}
		}
		if fholder.hasFilter {
			col := uint32(p.planToStreamColMap[fholder.filterRenderIdx])
			aggregations[i].FilterColIdx = &col
		}
	}

	inputTypes := p.ResultTypes

	groupCols := make([]uint32, n.numGroupCols)
	for i := 0; i < n.numGroupCols; i++ {
		groupCols[i] = uint32(p.planToStreamColMap[i])
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

	var finalAggsSpec distsqlrun.AggregatorSpec
	var finalAggsPost distsqlrun.PostProcessSpec

	if !multiStage && allDistinct {
		// We can't do local aggregation, but we can do local distinct processing
		// to reduce streaming duplicates, and aggregate on the final node.

		ordering := dsp.convertOrdering(planPhysicalProps(n.plan), p.planToStreamColMap).Columns
		orderedColsMap := make(map[uint32]struct{})
		for _, ord := range ordering {
			orderedColsMap[ord.ColIdx] = struct{}{}
		}
		distinctColsMap := make(map[uint32]struct{})
		for _, agg := range aggregations {
			for _, c := range agg.ColIdx {
				distinctColsMap[c] = struct{}{}
			}
		}
		orderedColumns := make([]uint32, len(orderedColsMap))
		idx := 0
		for o := range orderedColsMap {
			orderedColumns[idx] = o
			idx++
		}
		distinctColumns := make([]uint32, len(distinctColsMap))
		idx = 0
		for o := range distinctColsMap {
			distinctColumns[idx] = o
			idx++
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

	// planToStreamMapSet keeps track of whether or not
	// p.planToStreamColMap has been set to its desired mapping or not.
	planToStreamMapSet := false
	if !multiStage {
		finalAggsSpec = distsqlrun.AggregatorSpec{
			Aggregations: aggregations,
			GroupCols:    groupCols,
		}
	} else {
		// Some aggregations might need multiple aggregation as part of
		// their local and final stages (along with a final render
		// expression to combine the multiple aggregations into a
		// single result).
		//
		// Count the total number of aggregation in the local/final
		// stages and keep track of whether any of them needs a final
		// rendering.
		nLocalAgg := 0
		nFinalAgg := 0
		needRender := false
		for _, e := range aggregations {
			info := distsqlplan.DistAggregationTable[e.Func]
			nLocalAgg += len(info.LocalStage)
			nFinalAgg += len(info.FinalStage)
			if info.FinalRendering != nil {
				needRender = true
			}
		}

		// We alloc the maximum possible number of unique local and final
		// aggregations but do not initialize any aggregations
		// since we can de-duplicate equivalent local and final aggregations.
		localAggs := make([]distsqlrun.AggregatorSpec_Aggregation, 0, nLocalAgg+len(groupCols))
		intermediateTypes := make([]sqlbase.ColumnType, 0, nLocalAgg+len(groupCols))
		finalAggs := make([]distsqlrun.AggregatorSpec_Aggregation, 0, nFinalAgg)
		// finalIdxMap maps the index i of the final aggregation (with
		// respect to the i-th final aggregation out of all final
		// aggregations) to its index in the finalAggs slice.
		finalIdxMap := make([]uint32, nFinalAgg)
		finalGroupCols := make([]uint32, len(groupCols))

		// finalPreRenderTypes is passed to an IndexVarHelper which
		// helps type-check the indexed variables passed into
		// FinalRendering for some aggregations.
		// This has a 1-1 mapping to finalAggs
		var finalPreRenderTypes []sqlbase.ColumnType
		if needRender {
			finalPreRenderTypes = make([]sqlbase.ColumnType, 0, nFinalAgg)
		}

		// Each aggregation can have multiple aggregations in the
		// local/final stages. We concatenate all these into
		// localAggs/finalAggs.
		// finalIdx is the index of the final aggregation with respect
		// to all final aggregations.
		finalIdx := 0
		for _, e := range aggregations {
			info := distsqlplan.DistAggregationTable[e.Func]

			// relToAbsLocalIdx maps each local stage for the given
			// aggregation e to its final index in localAggs.  This
			// is necessary since we de-duplicate equivalent local
			// aggregations and need to correspond the one copy of
			// local aggregation required by the final stage to its
			// input, which is specified as a relative local stage
			// index (see `Aggregations` in aggregators_func.go).
			// We use a slice here instead of a map because we have
			// a small, bounded domain to map and runtime hash
			// operations are relatively expensive.
			relToAbsLocalIdx := make([]uint32, len(info.LocalStage))
			// First prepare and spec local aggregations.
			// Note the planNode first feeds the input (inputTypes)
			// into the local aggregators.
			for i, localFunc := range info.LocalStage {
				localAgg := distsqlrun.AggregatorSpec_Aggregation{
					Func:         localFunc,
					ColIdx:       e.ColIdx,
					FilterColIdx: e.FilterColIdx,
				}

				isNewAgg := true
				for j, prevLocalAgg := range localAggs {
					if localAgg.Equals(prevLocalAgg) {
						// Found existing, equivalent local agg.
						// Map the relative index (i)
						// for the current local agg
						// to the absolute index (j) of
						// the existing local agg.
						relToAbsLocalIdx[i] = uint32(j)
						isNewAgg = false
						break
					}
				}

				if isNewAgg {
					// Append the new local aggregation
					// and map to its index in localAggs.
					relToAbsLocalIdx[i] = uint32(len(localAggs))
					localAggs = append(localAggs, localAgg)

					// Keep track of the new local
					// aggregation's output type.
					argTypes := make([]sqlbase.ColumnType, len(e.ColIdx))
					for j, c := range e.ColIdx {
						argTypes[j] = inputTypes[c]
					}
					_, outputType, err := distsqlrun.GetAggregateInfo(localFunc, argTypes...)
					if err != nil {
						return err
					}
					intermediateTypes = append(intermediateTypes, outputType)
				}
			}

			for _, finalInfo := range info.FinalStage {
				// The input of the final aggregators are
				// specified as the relative indices of the
				// local aggregation values. We need to map
				// these to the corresponding absolute indices
				// in localAggs.
				// argIdxs consists of the absolute indices
				// in localAggs.
				argIdxs := make([]uint32, len(finalInfo.LocalIdxs))
				for i, relIdx := range finalInfo.LocalIdxs {
					argIdxs[i] = relToAbsLocalIdx[relIdx]
				}
				finalAgg := distsqlrun.AggregatorSpec_Aggregation{
					Func:   finalInfo.Fn,
					ColIdx: argIdxs,
				}

				isNewAgg := true
				for i, prevFinalAgg := range finalAggs {
					if finalAgg.Equals(prevFinalAgg) {
						// Found existing, equivalent
						// final agg.  Map the finalIdx
						// for the current final agg to
						// its index (i) in finalAggs.
						finalIdxMap[finalIdx] = uint32(i)
						isNewAgg = false
						break
					}
				}

				// Append the final agg if there is no existing
				// equivalent.
				if isNewAgg {
					finalIdxMap[finalIdx] = uint32(len(finalAggs))
					finalAggs = append(finalAggs, finalAgg)

					if needRender {
						argTypes := make([]sqlbase.ColumnType, len(finalInfo.LocalIdxs))
						for i := range finalInfo.LocalIdxs {
							// Map the corresponding local
							// aggregation output types for
							// the current aggregation e.
							argTypes[i] = intermediateTypes[argIdxs[i]]
						}
						_, outputType, err := distsqlrun.GetAggregateInfo(
							finalInfo.Fn, argTypes...,
						)
						if err != nil {
							return err
						}
						finalPreRenderTypes = append(finalPreRenderTypes, outputType)
					}
				}
				finalIdx++
			}
		}

		// Add IDENT expressions for the group columns; these need to
		// be part of the output of the local stage because the final
		// stage needs them.
		for i, groupColIdx := range groupCols {
			agg := distsqlrun.AggregatorSpec_Aggregation{
				Func:   distsqlrun.AggregatorSpec_IDENT,
				ColIdx: []uint32{groupColIdx},
			}
			// See if there already is an aggregation like the one
			// we want to add.
			idx := -1
			for j := range localAggs {
				if localAggs[j].Equals(agg) {
					idx = j
					break
				}
			}
			if idx == -1 {
				// Not already there, add it.
				idx = len(localAggs)
				localAggs = append(localAggs, agg)
				intermediateTypes = append(intermediateTypes, inputTypes[groupColIdx])
			}
			finalGroupCols[i] = uint32(idx)
		}

		localAggsSpec := distsqlrun.AggregatorSpec{
			Aggregations: localAggs,
			GroupCols:    groupCols,
		}

		p.AddNoGroupingStage(
			distsqlrun.ProcessorCoreUnion{Aggregator: &localAggsSpec},
			distsqlrun.PostProcessSpec{},
			intermediateTypes,
			orderingTerminated, // The local aggregators don't guarantee any output ordering.
		)

		finalAggsSpec = distsqlrun.AggregatorSpec{
			Aggregations: finalAggs,
			GroupCols:    finalGroupCols,
		}

		if needRender {
			// Build rendering expressions.
			renderExprs := make([]distsqlrun.Expression, len(aggregations))
			h := distsqlplan.MakeTypeIndexedVarHelper(finalPreRenderTypes)
			// finalIdx is an index inside finalAggs. It is used to
			// keep track of the finalAggs results that correspond
			// to each aggregation.
			finalIdx := 0
			for i, e := range aggregations {
				info := distsqlplan.DistAggregationTable[e.Func]
				if info.FinalRendering == nil {
					// mappedIdx corresponds to the index
					// location of the result for this
					// final aggregation in finalAggs. This
					// is necessary since we re-use final
					// aggregations if they are equivalent
					// across and within stages.
					mappedIdx := int(finalIdxMap[finalIdx])
					renderExprs[i] = distsqlplan.MakeExpression(h.IndexedVar(mappedIdx), planCtx.evalCtx, nil)
				} else {
					// We have multiple final aggregation
					// values that we need to be mapped to
					// their corresponding index in
					// finalAggs for FinalRendering.
					mappedIdxs := make([]int, len(info.FinalStage))
					for j := range info.FinalStage {
						mappedIdxs[j] = int(finalIdxMap[finalIdx+j])
					}
					// Map the final aggrgation values
					// to their corresponding
					expr, err := info.FinalRendering(&h, mappedIdxs)
					if err != nil {
						return err
					}
					renderExprs[i] = distsqlplan.MakeExpression(expr, planCtx.evalCtx, nil)
				}
				finalIdx += len(info.FinalStage)
			}
			finalAggsPost.RenderExprs = renderExprs
		} else if len(finalAggs) < len(aggregations) {
			// We want to ensure we map the streams properly now
			// that we've potential reduced the number of final
			// aggregation output streams. We use finalIdxMap to
			// create a 1-1 mapping from the final aggregators to
			// their corresponding column index in the map.
			p.planToStreamColMap = p.planToStreamColMap[:0]
			for _, idx := range finalIdxMap {
				p.planToStreamColMap = append(p.planToStreamColMap, int(idx))
			}
			planToStreamMapSet = true
		}
	}

	// Set up the final stage.

	finalOutTypes := make([]sqlbase.ColumnType, len(aggregations))
	for i, agg := range aggregations {
		argTypes := make([]sqlbase.ColumnType, len(agg.ColIdx))
		for i, c := range agg.ColIdx {
			argTypes[i] = inputTypes[c]
		}
		var err error
		_, finalOutTypes[i], err = distsqlrun.GetAggregateInfo(agg.Func, argTypes...)
		if err != nil {
			return err
		}
	}

	if len(finalAggsSpec.GroupCols) == 0 || len(p.ResultRouters) == 1 {
		// No GROUP BY, or we have a single stream. Use a single final aggregator.
		// If the previous stage was all on a single node, put the final
		// aggregator there. Otherwise, bring the results back on this node.
		node := dsp.nodeDesc.NodeID
		if prevStageNode != 0 {
			node = prevStageNode
		}
		p.AddSingleGroupStage(
			node,
			distsqlrun.ProcessorCoreUnion{Aggregator: &finalAggsSpec},
			finalAggsPost,
			finalOutTypes,
		)
	} else {
		// We distribute (by group columns) to multiple processors.

		// Set up the output routers from the previous stage.
		for _, resultProc := range p.ResultRouters {
			p.Processors[resultProc].Spec.Output[0] = distsqlrun.OutputRouterSpec{
				Type:        distsqlrun.OutputRouterSpec_BY_HASH,
				HashColumns: finalAggsSpec.GroupCols,
			}
		}

		stageID := p.NewStageID()

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
					Core: distsqlrun.ProcessorCoreUnion{Aggregator: &finalAggsSpec},
					Post: finalAggsPost,
					Output: []distsqlrun.OutputRouterSpec{{
						Type: distsqlrun.OutputRouterSpec_PASS_THROUGH,
					}},
					StageID: stageID,
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

	// Update p.planToStreamColMap; we will have a simple 1-to-1 mapping of
	// planNode columns to stream columns because the aggregator
	// has been programmed to produce the same columns as the groupNode.
	if !planToStreamMapSet {
		p.planToStreamColMap = identityMap(p.planToStreamColMap, len(aggregations))
	}
	return nil
}

func (dsp *DistSQLPlanner) createPlanForIndexJoin(
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
		Table:    *n.index.desc,
		IndexIdx: 0,
	}

	post := distsqlrun.PostProcessSpec{
		Filter:        distsqlplan.MakeExpression(n.table.filter, planCtx.evalCtx, nil),
		Projection:    true,
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

	if distributeIndexJoin.Get(&dsp.st.SV) && len(plan.ResultRouters) > 1 {
		// Instantiate one join reader for every stream.
		plan.AddNoGroupingStage(
			distsqlrun.ProcessorCoreUnion{JoinReader: &joinReaderSpec},
			post,
			getTypesForPlanResult(n, plan.planToStreamColMap),
			dsp.convertOrdering(planPhysicalProps(n), plan.planToStreamColMap),
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
	nodeColumns := planColumns(node)
	if planToStreamColMap == nil {
		// No remapping.
		types := make([]sqlbase.ColumnType, len(nodeColumns))
		for i := range nodeColumns {
			colTyp, err := sqlbase.DatumTypeToColumnType(nodeColumns[i].Typ)
			if err != nil {
				// TODO(radu): propagate this instead of panicking
				panic(err)
			}
			types[i] = colTyp
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
			colTyp, err := sqlbase.DatumTypeToColumnType(nodeColumns[nodeCol].Typ)
			if err != nil {
				// TODO(radu): propagate this instead of panicking
				panic(err)
			}
			types[streamCol] = colTyp
		}
	}
	return types
}

func (dsp *DistSQLPlanner) createPlanForJoin(
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

	// We initialize these properties of the joiner. They will then be used to
	// fill in the processor spec. See descriptions for HashJoinerSpec.
	var joinType distsqlrun.JoinType
	var onExpr distsqlrun.Expression
	var leftEqCols, rightEqCols []uint32
	var leftMergeOrd, rightMergeOrd distsqlrun.Ordering

	switch n.joinType {
	case joinTypeInner:
		joinType = distsqlrun.JoinType_INNER
	case joinTypeFullOuter:
		joinType = distsqlrun.JoinType_FULL_OUTER
	case joinTypeRightOuter:
		joinType = distsqlrun.JoinType_RIGHT_OUTER
	case joinTypeLeftOuter:
		joinType = distsqlrun.JoinType_LEFT_OUTER
	default:
		panic(fmt.Sprintf("invalid join type %d", n.joinType))
	}

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
		leftEqCols = make([]uint32, numEq)
		for i, leftPlanCol := range n.pred.leftEqualityIndices {
			leftEqCols[i] = uint32(leftPlan.planToStreamColMap[leftPlanCol])
		}
		rightEqCols = make([]uint32, numEq)
		for i, rightPlanCol := range n.pred.rightEqualityIndices {
			rightEqCols[i] = uint32(rightPlan.planToStreamColMap[rightPlanCol])
		}
		if planMergeJoins.Get(&dsp.st.SV) && len(n.mergeJoinOrdering) > 0 {
			// TODO(radu): we currently only use merge joins when we have an ordering on
			// all equality columns. We should relax this by either:
			//  - implementing a hybrid hash/merge processor which implements merge
			//    logic on the columns we have an ordering on, and within each merge
			//    group uses a hashmap on the remaining columns
			//  - or: adding a sort processor to complete the order
			if len(n.mergeJoinOrdering) == len(n.pred.leftEqualityIndices) {
				// Excellent! We can use the merge joiner.
				leftMergeOrd.Columns = make([]distsqlrun.Ordering_Column, len(n.mergeJoinOrdering))
				rightMergeOrd.Columns = make([]distsqlrun.Ordering_Column, len(n.mergeJoinOrdering))
				for i, c := range n.mergeJoinOrdering {
					leftMergeOrd.Columns[i].ColIdx = leftEqCols[c.ColIdx]
					rightMergeOrd.Columns[i].ColIdx = rightEqCols[c.ColIdx]
					dir := distsqlrun.Ordering_Column_ASC
					if c.Direction == encoding.Descending {
						dir = distsqlrun.Ordering_Column_DESC
					}
					leftMergeOrd.Columns[i].Direction = dir
					rightMergeOrd.Columns[i].Direction = dir
				}
			}
		}
	} else {
		// Without column equality, we cannot distribute the join. Run a
		// single processor.
		nodes = []roachpb.NodeID{dsp.nodeDesc.NodeID}

		// If either side has a single stream, put the processor on that node. We
		// prefer the left side because that is processed first by the hash joiner.
		if len(leftRouters) == 1 {
			nodes[0] = p.Processors[leftRouters[0]].Node
		} else if len(rightRouters) == 1 {
			nodes[0] = p.Processors[rightRouters[0]].Node
		}
	}

	post := distsqlrun.PostProcessSpec{
		Projection: true,
	}
	// addOutCol appends to post.OutputColumns and returns the index
	// in the slice of the added column.
	addOutCol := func(col uint32) int {
		idx := len(post.OutputColumns)
		post.OutputColumns = append(post.OutputColumns, col)
		return idx
	}

	// The join columns are in two groups:
	//  - the columns on the left side (numLeftCols)
	//  - the columns on the right side (numRightCols)
	joinCol := 0

	for i := 0; i < n.pred.numLeftCols; i++ {
		if !n.columns[joinCol].Omitted {
			joinToStreamColMap[joinCol] = addOutCol(uint32(leftPlan.planToStreamColMap[i]))
		}
		joinCol++
	}
	for i := 0; i < n.pred.numRightCols; i++ {
		if !n.columns[joinCol].Omitted {
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
		joinColMap := make([]int, len(n.columns))
		idx := 0
		for i := 0; i < n.pred.numLeftCols; i++ {
			joinColMap[idx] = leftPlan.planToStreamColMap[i]
			idx++
		}
		for i := 0; i < n.pred.numRightCols; i++ {
			joinColMap[idx] = rightPlan.planToStreamColMap[i] + len(leftTypes)
			idx++
		}
		onExpr = distsqlplan.MakeExpression(n.pred.onCond, planCtx.evalCtx, joinColMap)
	}

	// Create the Core spec.
	var core distsqlrun.ProcessorCoreUnion
	if leftMergeOrd.Columns == nil {
		core.HashJoiner = &distsqlrun.HashJoinerSpec{
			LeftEqColumns:  leftEqCols,
			RightEqColumns: rightEqCols,
			OnExpr:         onExpr,
			Type:           joinType,
		}
	} else {
		core.MergeJoiner = &distsqlrun.MergeJoinerSpec{
			LeftOrdering:  leftMergeOrd,
			RightOrdering: rightMergeOrd,
			OnExpr:        onExpr,
			Type:          joinType,
		}
	}

	pIdxStart := distsqlplan.ProcessorIdx(len(p.Processors))
	stageID := p.NewStageID()

	if len(nodes) == 1 {
		proc := distsqlplan.Processor{
			Node: nodes[0],
			Spec: distsqlrun.ProcessorSpec{
				Input: []distsqlrun.InputSyncSpec{
					{ColumnTypes: leftTypes},
					{ColumnTypes: rightTypes},
				},
				Core:    core,
				Post:    post,
				Output:  []distsqlrun.OutputRouterSpec{{Type: distsqlrun.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
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
					Core:    core,
					Post:    post,
					Output:  []distsqlrun.OutputRouterSpec{{Type: distsqlrun.OutputRouterSpec_PASS_THROUGH}},
					StageID: stageID,
				},
			}
			p.Processors = append(p.Processors, proc)
		}

		// Set up the left routers.
		for _, resultProc := range leftRouters {
			p.Processors[resultProc].Spec.Output[0] = distsqlrun.OutputRouterSpec{
				Type:        distsqlrun.OutputRouterSpec_BY_HASH,
				HashColumns: leftEqCols,
			}
		}
		// Set up the right routers.
		for _, resultProc := range rightRouters {
			p.Processors[resultProc].Spec.Output[0] = distsqlrun.OutputRouterSpec{
				Type:        distsqlrun.OutputRouterSpec_BY_HASH,
				HashColumns: rightEqCols,
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
		p.MergeResultStreams(leftRouters, bucket, leftMergeOrd, pIdx, 0)
		// Connect right routers to the processor's second input.
		p.MergeResultStreams(rightRouters, bucket, rightMergeOrd, pIdx, 1)

		p.ResultRouters = append(p.ResultRouters, pIdx)
	}

	p.planToStreamColMap = joinToStreamColMap
	p.ResultTypes = getTypesForPlanResult(n, joinToStreamColMap)

	// Joiners may guarantee an ordering to outputs, so we ensure that
	// ordering is propagated through the input synchronizer of the next stage.
	// We can propagate the ordering from either side, we use the left side here.
	p.SetMergeOrdering(dsp.convertOrdering(n.props, p.planToStreamColMap))
	return p, nil
}

func (dsp *DistSQLPlanner) createPlanForNode(
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
		dsp.selectRenders(&plan, n, planCtx.evalCtx)
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

		plan.AddFilter(n.filter, planCtx.evalCtx, plan.planToStreamColMap)

		return plan, nil

	case *limitNode:
		plan, err := dsp.createPlanForNode(planCtx, n.plan)
		if err != nil {
			return physicalPlan{}, err
		}
		if err := n.evalLimit(planCtx.evalCtx); err != nil {
			return physicalPlan{}, err
		}
		if err := plan.AddLimit(n.count, n.offset, dsp.nodeDesc.NodeID); err != nil {
			return physicalPlan{}, err
		}
		return plan, nil

	case *distinctNode:
		return dsp.createPlanForDistinct(planCtx, n)

	case *valuesNode:
		return dsp.createPlanForValues(planCtx, n)

	case *createStatsNode:
		return dsp.createPlanForCreateStats(planCtx, n)

	default:
		panic(fmt.Sprintf("unsupported node type %T", n))
	}
}

func (dsp *DistSQLPlanner) createPlanForValues(
	planCtx *planningCtx, n *valuesNode,
) (physicalPlan, error) {
	columns := len(n.columns)

	s := distsqlrun.ValuesCoreSpec{
		Columns: make([]distsqlrun.DatumInfo, columns),
	}
	types := make([]sqlbase.ColumnType, columns)

	for i, t := range n.columns {
		colTyp, err := sqlbase.DatumTypeToColumnType(t.Typ)
		if err != nil {
			return physicalPlan{}, err
		}
		types[i] = colTyp
		s.Columns[i].Encoding = sqlbase.DatumEncoding_VALUE
		s.Columns[i].Type = types[i]
	}

	var a sqlbase.DatumAlloc
	params := runParams{
		ctx:     planCtx.ctx,
		evalCtx: planCtx.evalCtx,
		p:       nil,
	}
	if err := n.Start(params); err != nil {
		return physicalPlan{}, err
	}
	defer n.Close(planCtx.ctx)

	s.RawBytes = make([][]byte, n.rows.Len())
	for i := 0; i < n.rows.Len(); i++ {
		if next, err := n.Next(runParams{ctx: planCtx.ctx}); !next {
			return physicalPlan{}, err
		}

		var buf []byte
		datums := n.Values()
		for j := range n.columns {
			var err error
			datum := sqlbase.DatumToEncDatum(types[j], datums[j])
			buf, err = datum.Encode(&types[j], &a, s.Columns[j].Encoding, buf)
			if err != nil {
				return physicalPlan{}, err
			}
		}
		s.RawBytes[i] = buf
	}

	plan := distsqlplan.PhysicalPlan{
		Processors: []distsqlplan.Processor{{
			// TODO: find a better node to place processor at
			Node: dsp.nodeDesc.NodeID,
			Spec: distsqlrun.ProcessorSpec{
				Core:   distsqlrun.ProcessorCoreUnion{Values: &s},
				Output: []distsqlrun.OutputRouterSpec{{Type: 0}},
			},
		}},
		ResultRouters: []distsqlplan.ProcessorIdx{0},
		ResultTypes:   types,
	}

	return physicalPlan{
		PhysicalPlan:       plan,
		planToStreamColMap: identityMapInPlace(make([]int, columns)),
	}, nil
}

func (dsp *DistSQLPlanner) createPlanForDistinct(
	planCtx *planningCtx, n *distinctNode,
) (physicalPlan, error) {
	plan, err := dsp.createPlanForNode(planCtx, n.plan)
	if err != nil {
		return physicalPlan{}, err
	}
	currentResultRouters := plan.ResultRouters
	var orderedColumns []uint32
	for i, o := range n.columnsInOrder {
		if o {
			orderedColumns = append(orderedColumns, uint32(plan.planToStreamColMap[i]))
		}
	}

	var distinctColumns []uint32
	for i := range planColumns(n) {
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

func (dsp *DistSQLPlanner) newPlanningCtx(
	ctx context.Context, evalCtx *tree.EvalContext, txn *client.Txn,
) planningCtx {
	planCtx := planningCtx{
		ctx:           ctx,
		evalCtx:       evalCtx,
		spanIter:      dsp.spanResolver.NewSpanResolverIterator(txn),
		nodeAddresses: make(map[roachpb.NodeID]string),
	}
	planCtx.nodeAddresses[dsp.nodeDesc.NodeID] = dsp.nodeDesc.Address.String()
	return planCtx
}

// FinalizePlan adds a final "result" stage if necessary and populates the
// endpoints of the plan.
func (dsp *DistSQLPlanner) FinalizePlan(planCtx *planningCtx, plan *physicalPlan) {
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
