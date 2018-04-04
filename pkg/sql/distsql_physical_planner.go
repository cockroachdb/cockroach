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
	"context"
	"fmt"
	"reflect"
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
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
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

	// metadataTestTolerance is the minimum level required to plan metadata test
	// processors.
	metadataTestTolerance distsqlrun.MetadataTestLevel

	// runnerChan is used to send out requests (for running SetupFlow RPCs) to a
	// pool of workers.
	runnerChan chan runnerRequest

	// gossip handle used to check node version compatibility.
	gossip *gossip.Gossip
	// liveness is used to avoid planning on down nodes.
	liveness *storage.NodeLiveness
}

const resolverPolicy = distsqlplan.BinPackingLeaseHolderChoice

// If true, the plan diagram (in JSON) is logged for each plan (used for
// debugging).
var logPlanDiagram = envutil.EnvOrDefaultBool("COCKROACH_DISTSQL_LOG_PLAN", false)

// If true, for index joins we instantiate a join reader on every node that
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

// NewDistSQLPlanner initializes a DistSQLPlanner.
//
// nodeDesc is the descriptor of the node on which this planner runs. It is used
// to favor itself and other close-by nodes when planning. An empty descriptor
// can be passed to aid bootstrapping, but then SetNodeDesc() needs to be called
// before this planner is used.
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
	liveness *storage.NodeLiveness,
	testingKnobs DistSQLPlannerTestingKnobs,
) *DistSQLPlanner {
	if liveness == nil {
		panic("must specify liveness")
	}
	dsp := &DistSQLPlanner{
		planVersion:           planVersion,
		st:                    st,
		nodeDesc:              nodeDesc,
		rpcContext:            rpcCtx,
		stopper:               stopper,
		distSQLSrv:            distSQLSrv,
		gossip:                gossip,
		spanResolver:          distsqlplan.NewSpanResolver(distSender, gossip, nodeDesc, resolverPolicy),
		liveness:              liveness,
		testingKnobs:          testingKnobs,
		metadataTestTolerance: distsqlrun.NoExplain,
	}
	dsp.initRunners()
	return dsp
}

func (dsp *DistSQLPlanner) shouldPlanTestMetadata() bool {
	return dsp.distSQLSrv.TestingKnobs.MetadataTestLevel >= dsp.metadataTestTolerance
}

// SetNodeDesc sets the planner's node descriptor.
func (dsp *DistSQLPlanner) SetNodeDesc(desc roachpb.NodeDescriptor) {
	dsp.nodeDesc = desc
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
	case *tree.FuncExpr:
		if t.IsDistSQLBlacklist() {
			v.err = newQueryNotSupportedErrorf("function %s cannot be executed with distsql", t)
			return false, expr
		}
	case *tree.DOid:
		v.err = newQueryNotSupportedError("OID expressions are not supported by distsql")
		return false, expr
	case *tree.CastExpr:
		switch t.Type.(type) {
		case *coltypes.TOid:
			v.err = newQueryNotSupportedErrorf("cast to %s is not supported by distsql", t.Type)
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
		if n.softLimit != 0 {
			// We don't yet recommend distributing plans where soft limits propagate
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

	case *unionNode:
		recLeft, err := dsp.checkSupportForNode(n.left)
		if err != nil {
			return 0, err
		}
		recRight, err := dsp.checkSupportForNode(n.right)
		if err != nil {
			return 0, err
		}
		return recLeft.compose(recRight), nil

	case *valuesNode:
		if !n.specifiedInQuery {
			return 0, newQueryNotSupportedErrorf("unsupported node %T", node)
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

	case *insertNode, *updateNode, *deleteNode, *upsertNode:
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
	ctx             context.Context
	extendedEvalCtx *extendedEvalContext
	spanIter        distsqlplan.SpanResolverIterator
	// nodeAddresses contains addresses for all NodeIDs that are referenced by any
	// physicalPlan we generate with this context.
	// Nodes that fail a health check have empty addresses.
	nodeAddresses map[roachpb.NodeID]string
}

func (p *planningCtx) EvalContext() *tree.EvalContext {
	return &p.extendedEvalCtx.EvalContext
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

	// planToStreamColMap maps planNode columns (see planColumns()) to columns in
	// the result streams. These stream indices correspond to the streams
	// referenced in ResultTypes.
	//
	// Note that in some cases, not all columns in the result streams are
	// referenced in the map; for example, columns that are only required for
	// stream merges in downstream input synchronizers are not included here.
	// (This is due to some processors not being configurable to output only
	// certain columns and will be fixed.)
	//
	// Conversely, in some cases not all planNode columns have a corresponding
	// result stream column (these map to index -1); this is the case for scanNode
	// and indexJoinNode where not all columns in the table are actually used in
	// the plan, but are kept for possible use downstream (e.g., sorting).
	//
	// When the query is run, the output processor's planToStreamColMap is used
	// by distSQLReceiver to create an implicit projection on the processor's
	// output for client consumption (see distSQLReceiver.Push()). Therefore,
	// "invisible" columns (e.g., columns required for merge ordering) will not
	// be output.
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

func (dsp *DistSQLPlanner) checkNodeHealth(
	ctx context.Context, nodeID roachpb.NodeID, addr string,
) error {
	// NB: not all tests populate a NodeLiveness. Everything using the
	// proper constructor NewDistSQLPlanner will, though.
	isLive := func(_ roachpb.NodeID) (bool, error) {
		return true, nil
	}
	if dsp.liveness != nil {
		isLive = dsp.liveness.IsLive
	}
	return checkNodeHealth(ctx, nodeID, addr, dsp.testingKnobs, dsp.gossip, dsp.rpcContext.ConnHealth, isLive)
}

func checkNodeHealth(
	ctx context.Context,
	nodeID roachpb.NodeID,
	addr string,
	knobs DistSQLPlannerTestingKnobs,
	g *gossip.Gossip,
	connHealth func(string) error,
	isLive func(roachpb.NodeID) (bool, error),
) error {
	// Check if the target's node descriptor is gossiped. If it isn't, the node
	// is definitely gone and has been for a while.
	//
	// TODO(tschottdorf): it's not clear that this adds anything to the liveness
	// check below. The node descriptor TTL is an hour as of 03/2018.
	if _, err := g.GetNodeIDAddress(nodeID); err != nil {
		log.VEventf(ctx, 1, "not using n%d because gossip doesn't know about it. "+
			"It might have gone away from the cluster. Gossip said: %s.", nodeID, err)
		return err
	}

	{
		// NB: as of #22658, ConnHealth does not work as expected; see the
		// comment within. We still keep this code for now because in
		// practice, once the node is down it will prevent using this node
		// 90% of the time (it gets used around once per second as an
		// artifact of rpcContext's reconnection mechanism at the time of
		// writing). This is better than having it used in 100% of cases
		// (until the liveness check below kicks in).
		var err error
		if knobs.OverrideHealthCheck != nil {
			err = knobs.OverrideHealthCheck(nodeID, addr)
		} else {
			err = connHealth(addr)
		}

		if err != nil && err != rpc.ErrNotConnected && err != rpc.ErrNotHeartbeated {
			// This host is known to be unhealthy. Don't use it (use the gateway
			// instead). Note: this can never happen for our nodeID (which
			// always has its address in the nodeMap).
			log.VEventf(ctx, 1, "marking n%d as unhealthy for this plan: %v", nodeID, err)
			return err
		}
	}
	{
		live, err := isLive(nodeID)
		if err == nil && !live {
			err = errors.New("node is not live")
		}
		if err != nil {
			return errors.Wrapf(err, "not using n%d due to liveness", nodeID)
		}
	}

	// Check that the node is not draining.
	drainingInfo := &distsqlrun.DistSQLDrainingInfo{}
	if err := g.GetInfoProto(gossip.MakeDistSQLDrainingKey(nodeID), drainingInfo); err != nil {
		// Because draining info has no expiration, an error
		// implies that we have not yet received a node's
		// draining information. Since this information is
		// written on startup, the most likely scenario is
		// that the node is ready. We therefore return no
		// error.
		return nil
	}

	if drainingInfo.Draining {
		errMsg := fmt.Sprintf("not using n%d because it is draining", nodeID)
		log.VEvent(ctx, 1, errMsg)
		return errors.New(errMsg)
	}

	return nil
}

// partitionSpans finds out which nodes are owners for ranges touching the
// given spans, and splits the spans according to owning nodes. The result is a
// set of spanPartitions (guaranteed one for each relevant node), which form a
// partitioning of the spans (i.e. they are non-overlapping and their union is
// exactly the original set of spans).
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
		// rspan is the span we are currently partitioning.
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
		// We break up rspan into its individual ranges (which may or
		// may not be on separate nodes). We then create "partitioned
		// spans" using the end keys of these individual ranges.
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
					if err := dsp.checkNodeHealth(ctx, nodeID, addr); err != nil {
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
		IsCheck: n.run.isCheck,
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

	// When a TableReader is running scrub checks, do not allow a
	// post-processor. This is because the outgoing stream is a fixed
	// format (distsqlrun.ScrubTypes).
	if n.run.isCheck {
		return s, distsqlrun.PostProcessSpec{}, nil
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

// convertOrdering maps the columns in props.ordering to the output columns of a
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

// getNodeIDForScan retrieves the node ID where the single table reader should
// reside for a limited scan. Ideally this is the lease holder for the first
// range in the specified spans. But if that node is unhealthy or incompatible,
// we use the gateway node instead.
func (dsp *DistSQLPlanner) getNodeIDForScan(
	planCtx *planningCtx, spans []roachpb.Span, reverse bool,
) (roachpb.NodeID, error) {
	if len(spans) == 0 {
		panic("no spans")
	}

	// Determine the node ID for the first range to be scanned.
	it := planCtx.spanIter
	if reverse {
		it.Seek(planCtx.ctx, spans[len(spans)-1], kv.Descending)
	} else {
		it.Seek(planCtx.ctx, spans[0], kv.Ascending)
	}
	if !it.Valid() {
		return 0, it.Error()
	}
	replInfo, err := it.ReplicaInfo(planCtx.ctx)
	if err != nil {
		return 0, err
	}
	nodeID := replInfo.NodeID

	// Check node health and compatibility.
	addr := replInfo.NodeDesc.Address.String()
	if err := dsp.checkNodeHealth(planCtx.ctx, nodeID, addr); err != nil {
		log.Eventf(planCtx.ctx, "not planning on node %d. unhealthy", nodeID)
		return dsp.nodeDesc.NodeID, nil
	}
	if !dsp.nodeVersionIsCompatible(nodeID, dsp.planVersion) {
		log.Eventf(planCtx.ctx, "not planning on node %d. incompatible version", nodeID)
		return dsp.nodeDesc.NodeID, nil
	}

	planCtx.nodeAddresses[nodeID] = addr
	return nodeID, nil
}

// createTableReaders generates a plan consisting of table reader processors,
// one for each node that has spans that we are reading.
// overridesResultColumns is optional.
func (dsp *DistSQLPlanner) createTableReaders(
	planCtx *planningCtx, n *scanNode, overrideResultColumns []uint32,
) (physicalPlan, error) {
	spec, post, err := initTableReaderSpec(n, planCtx.EvalContext())
	if err != nil {
		return physicalPlan{}, err
	}

	var spanPartitions []spanPartition
	if n.hardLimit == 0 && n.softLimit == 0 {
		spanPartitions, err = dsp.partitionSpans(planCtx, n.spans)
		if err != nil {
			return physicalPlan{}, err
		}
	} else {
		// If the scan is limited, use a single TableReader to avoid reading more
		// rows than necessary. Note that distsql is currently only enabled for hard
		// limits since the TableReader will still read too eagerly in the soft
		// limit case. To prevent this we'll need a new mechanism on the execution
		// side to modulate table reads.
		nodeID, err := dsp.getNodeIDForScan(planCtx, n.spans, n.reverse)
		if err != nil {
			return physicalPlan{}, err
		}
		spanPartitions = []spanPartition{{nodeID, n.spans}}
	}

	var p physicalPlan
	stageID := p.NewStageID()

	p.ResultRouters = make([]distsqlplan.ProcessorIdx, len(spanPartitions))

	for i, sp := range spanPartitions {
		tr := &distsqlrun.TableReaderSpec{}
		*tr = spec
		tr.Spans = makeTableReaderSpans(sp.spans)

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
	if post.Projection {
		for i := range planToStreamColMap {
			planToStreamColMap[i] = -1
		}
		for i, col := range post.OutputColumns {
			planToStreamColMap[col] = i
		}
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
		ordering := distsqlrun.ConvertToMappedSpecOrdering(n.ordering, p.planToStreamColMap)

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
		// Note that internally, AddProjection might retain more columns than
		// necessary so we can preserve the p.Ordering between parallel streams
		// when they merge later.
		p.planToStreamColMap = p.planToStreamColMap[:len(n.columns)]
		columns := make([]uint32, 0, len(n.columns))
		for i, col := range p.planToStreamColMap {
			if col < 0 {
				// This column isn't needed; ignore it.
				continue
			}
			p.planToStreamColMap[i] = len(columns)
			columns = append(columns, uint32(col))
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
		if fholder.isIdentAggregate() {
			aggregations[i].Func = distsqlrun.AggregatorSpec_IDENT
		} else {
			// Convert the aggregate function to the enum value with the same string
			// representation.
			funcStr := strings.ToUpper(fholder.funcName)
			funcIdx, ok := distsqlrun.AggregatorSpec_Func_value[funcStr]
			if !ok {
				return errors.Errorf("unknown aggregate %s", funcStr)
			}
			aggregations[i].Func = distsqlrun.AggregatorSpec_Func(funcIdx)
			aggregations[i].Distinct = fholder.isDistinct()
		}
		if fholder.argRenderIdx != noRenderIdx {
			aggregations[i].ColIdx = []uint32{uint32(p.planToStreamColMap[fholder.argRenderIdx])}
		}
		if fholder.hasFilter() {
			col := uint32(p.planToStreamColMap[fholder.filterRenderIdx])
			aggregations[i].FilterColIdx = &col
		}
	}

	inputTypes := p.ResultTypes

	groupCols := make([]uint32, len(n.groupCols))
	for i, idx := range n.groupCols {
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
			h := tree.MakeTypesOnlyIndexedVarHelper(
				sqlbase.ColumnTypesToDatumTypes(finalPreRenderTypes),
			)
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
					renderExprs[i] = distsqlplan.MakeExpression(
						h.IndexedVar(mappedIdx), planCtx.EvalContext(),
						nil /* indexVarMap */)
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
					renderExprs[i] = distsqlplan.MakeExpression(
						expr, planCtx.EvalContext(),
						nil /* indexVarMap */)
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
		Filter: distsqlplan.MakeExpression(
			n.table.filter, planCtx.EvalContext(), nil /* indexVarMap */),
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
	// See if we can create an interleave join plan.
	if planInterleavedJoins.Get(&dsp.st.SV) {
		plan, ok, err := dsp.tryCreatePlanForInterleavedJoin(planCtx, n)
		if err != nil {
			return physicalPlan{}, err
		}
		// An interleave join plan could be used. Return it.
		if ok {
			return plan, nil
		}
	}

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

	// Nodes where we will run the join processors.
	var nodes []roachpb.NodeID

	// We initialize these properties of the joiner. They will then be used to
	// fill in the processor spec. See descriptions for HashJoinerSpec.
	var leftEqCols, rightEqCols []uint32
	var leftMergeOrd, rightMergeOrd distsqlrun.Ordering
	joinType := n.joinType

	// Figure out the left and right types.
	leftTypes := leftPlan.ResultTypes
	rightTypes := rightPlan.ResultTypes

	// Set up the equality columns.
	if numEq := len(n.pred.leftEqualityIndices); numEq != 0 {
		leftEqCols = eqCols(n.pred.leftEqualityIndices, leftPlan.planToStreamColMap)
		rightEqCols = eqCols(n.pred.rightEqualityIndices, rightPlan.planToStreamColMap)
	}

	// Can use a lookupJoiner if there is a scan node on the right that uses the
	// table's primary index.
	// TODO(pbardea): Loosen restriction when joinReader takes secondary indexes.
	lookupJoinEnabled := planCtx.EvalContext().SessionData.LookupJoinEnabled
	isLookupJoin, lookupJoinScan, lookupFailReason := verifyLookupJoin(leftEqCols, rightEqCols, n, joinType)

	if lookupJoinEnabled {
		if !isLookupJoin {
			log.Warningf(planCtx.ctx, "lookup join was forced, but could not be used because %s", lookupFailReason)
		}
	} else {
		isLookupJoin = false
	}

	var p physicalPlan
	var leftRouters, rightRouters []distsqlplan.ProcessorIdx
	if isLookupJoin {
		// Lookup joins only take the left side as input. The right side will
		// be retrieved via index lookups specified in the joinReader spec.
		p.PhysicalPlan = leftPlan.PhysicalPlan
		leftRouters = leftPlan.ResultRouters
	} else {
		p.PhysicalPlan, leftRouters, rightRouters = distsqlplan.MergePlans(
			&leftPlan.PhysicalPlan, &rightPlan.PhysicalPlan,
		)
	}

	// Set up the output columns.
	if numEq := len(n.pred.leftEqualityIndices); numEq != 0 {
		nodes = findJoinProcessorNodes(leftRouters, rightRouters, p.Processors, !isLookupJoin)

		if planMergeJoins.Get(&dsp.st.SV) && len(n.mergeJoinOrdering) > 0 {
			// TODO(radu): we currently only use merge joins when we have an ordering on
			// all equality columns. We should relax this by either:
			//  - implementing a hybrid hash/merge processor which implements merge
			//    logic on the columns we have an ordering on, and within each merge
			//    group uses a hashmap on the remaining columns
			//  - or: adding a sort processor to complete the order
			if len(n.mergeJoinOrdering) == len(n.pred.leftEqualityIndices) {
				// Excellent! We can use the merge joiner.
				leftMergeOrd = distsqlOrdering(n.mergeJoinOrdering, leftEqCols)
				rightMergeOrd = distsqlOrdering(n.mergeJoinOrdering, rightEqCols)
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

	post, joinToStreamColMap := joinOutColumns(n, leftPlan, rightPlan)
	onExpr := remapOnExpr(planCtx.EvalContext(), n, leftPlan, rightPlan)

	// Refer to comment about JoinReaderSpec.IndexMap for description.
	indexMap := make([]uint32, 0, len(rightPlan.planToStreamColMap))
	for i, m := range rightPlan.planToStreamColMap {
		if m >= 0 {
			indexMap = append(indexMap, uint32(i))
		}
	}

	// Create the Core spec.
	var core distsqlrun.ProcessorCoreUnion
	if isLookupJoin {
		lookupExpr := shiftExprCols(
			planCtx.EvalContext(), lookupJoinScan.origFilter, rightPlan.planToStreamColMap, leftPlan.planToStreamColMap,
		)
		post.Filter = lookupExpr
		var indexColumns = make([]uint32, len(lookupJoinScan.index.ColumnIDs))
		for i, id := range lookupJoinScan.index.ColumnIDs {
			indexColumns[i] = uint32(id)
		}
		// Lookup columns are the columns on the left side of the relation which
		// indicate which columns on the left side of the join match with the index
		// being joined on. First the index columns are re-arranged with respect
		// to the rightEqualityColumns on the join. The lookup columns are the
		// left equality columns re-arranged with respect to the new arrangement of
		// the indexColumns.
		// E.g.
		// If rightEqCols = [2, 3], indexColumns = [3, 2, 1], leftEqCols = [1, 2]
		// The re-arranged indexColumns would be [3, 2] and the lookup columns is
		// [2, 1].
		// If rightEqCols = [3, 2], indexColumns = [3, 2, 1], leftEqCols = [1, 2]
		// The re-arranged indexColumns would be [2, 3] and the lookup columns is
		// [1, 2].
		indexColumns, err := applySortBasedOnFirst(rightEqCols, indexColumns)
		if err != nil {
			return physicalPlan{}, err
		}
		lookupCols, err := applySortBasedOnFirst(indexColumns, leftEqCols)
		if err != nil {
			return physicalPlan{}, err
		}

		core.JoinReader = &distsqlrun.JoinReaderSpec{
			Table:         *(lookupJoinScan.desc),
			IndexIdx:      0,
			LookupColumns: lookupCols,
			IndexMap:      indexMap,
			OnExpr:        onExpr,
		}
	} else if leftMergeOrd.Columns == nil {
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

	p.AddJoinStage(
		nodes, core, post, leftEqCols, rightEqCols, leftTypes, rightTypes,
		leftMergeOrd, rightMergeOrd, leftRouters, rightRouters, !isLookupJoin,
	)

	p.planToStreamColMap = joinToStreamColMap
	p.ResultTypes = getTypesForPlanResult(n, joinToStreamColMap)

	// Joiners may guarantee an ordering to outputs, so we ensure that
	// ordering is propagated through the input synchronizer of the next stage.
	// We can propagate the ordering from either side, we use the left side here.
	// Note that n.props only has a non-empty ordering for inner joins, where it
	// uses the mergeJoinOrdering.
	p.SetMergeOrdering(dsp.convertOrdering(n.props, p.planToStreamColMap))
	return p, nil
}

// Verifies that a lookup join can be used. Either returns true and empty
// string, or returns false and an explanation behind why the lookup join
// could not be used.
func verifyLookupJoin(
	leftEqCols, rightEqCols []uint32, n *joinNode, joinType sqlbase.JoinType,
) (bool, *scanNode, string) {
	lookupJoinScan, ok := n.right.plan.(*scanNode)
	if !ok {
		return false, nil, "lookup join's right side must be a scan"
	}

	if lookupJoinScan.index != &lookupJoinScan.desc.PrimaryIndex {
		return false, nil, "lookup joins can only perform lookups through the primary index"
	}

	if joinType != sqlbase.InnerJoin {
		return false, nil, "lookup joins are only supported for inner joins"
	}

	// Check if equality columns still allow for lookup join.
	if len(rightEqCols) > len(lookupJoinScan.index.ColumnIDs) {
		return false, nil, "cannot have more equality columns than index columns"
	}
	if leftEqCols == nil {
		return false, nil, "lookup columns cannot be empty for lookup join"
	}

	// Check if rightEqCols are prefix of index columns in scanNode lookupJoinScan.
	rightEqColsMap := make(map[int]bool, len(n.pred.rightEqualityIndices))
	for _, rightColID := range n.pred.rightEqualityIndices {
		rightEqColsMap[rightColID+1] = true
	}
	for i := 0; i < len(n.pred.rightEqualityIndices); i++ {
		indexColID := int(lookupJoinScan.index.ColumnIDs[i])
		if rightEqColsMap[indexColID] {
			delete(rightEqColsMap, indexColID)
		} else {
			return false, nil, "right equality columns are not a prefix of index columns"
		}
	}
	return true, lookupJoinScan, ""
}

// Given two arrays, sort the first array and mirror the movement of the first
// array in the second array. Return the changed second array up until the
// length of the first.
// Required: len(source) <= len(target) and source contains distinct elements.
// E.g. Inputs: [5, 2, 4, 1] and [a, b, c, d, e] returns [d, b, c, a].
func applySortBasedOnFirst(source, target []uint32) ([]uint32, error) {
	if len(source) > len(target) {
		return nil, errors.Errorf("source array had length %d, expecting at most %d", len(source), len(target))
	}
	sorted := make([]uint32, len(source))
	copy(sorted, source)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	idxMap := make(map[uint32]int)
	for i, val := range sorted {
		idxMap[val] = i
	}

	result := make([]uint32, len(source))
	for i, val := range source {
		result[idxMap[val]] = target[i]
	}
	return result, nil
}

func (dsp *DistSQLPlanner) createPlanForNode(
	planCtx *planningCtx, node planNode,
) (plan physicalPlan, err error) {
	switch n := node.(type) {
	case *explainDistSQLNode:
		curTol := dsp.metadataTestTolerance
		dsp.metadataTestTolerance = distsqlrun.On
		defer func() { dsp.metadataTestTolerance = curTol }()
		plan, err = dsp.createPlanForNode(planCtx, n.plan)

	case *scanNode:
		plan, err = dsp.createTableReaders(planCtx, n, nil)

	case *indexJoinNode:
		plan, err = dsp.createPlanForIndexJoin(planCtx, n)

	case *joinNode:
		plan, err = dsp.createPlanForJoin(planCtx, n)

	case *renderNode:
		plan, err = dsp.createPlanForNode(planCtx, n.source.plan)
		if err != nil {
			return physicalPlan{}, err
		}
		dsp.selectRenders(&plan, n, planCtx.EvalContext())

	case *groupNode:
		plan, err = dsp.createPlanForNode(planCtx, n.plan)
		if err != nil {
			return physicalPlan{}, err
		}

		if err := dsp.addAggregators(planCtx, &plan, n); err != nil {
			return physicalPlan{}, err
		}

	case *sortNode:
		plan, err = dsp.createPlanForNode(planCtx, n.plan)
		if err != nil {
			return physicalPlan{}, err
		}

		dsp.addSorters(&plan, n)

	case *filterNode:
		plan, err = dsp.createPlanForNode(planCtx, n.source.plan)
		if err != nil {
			return physicalPlan{}, err
		}

		plan.AddFilter(n.filter, planCtx.EvalContext(), plan.planToStreamColMap)

	case *limitNode:
		plan, err = dsp.createPlanForNode(planCtx, n.plan)
		if err != nil {
			return physicalPlan{}, err
		}
		if err := n.evalLimit(planCtx.EvalContext()); err != nil {
			return physicalPlan{}, err
		}
		if err := plan.AddLimit(n.count, n.offset, dsp.nodeDesc.NodeID); err != nil {
			return physicalPlan{}, err
		}

	case *distinctNode:
		plan, err = dsp.createPlanForDistinct(planCtx, n)

	case *unionNode:
		plan, err = dsp.createPlanForSetOp(planCtx, n)

	case *valuesNode:
		plan, err = dsp.createPlanForValues(planCtx, n)

	case *createStatsNode:
		plan, err = dsp.createPlanForCreateStats(planCtx, n)

	default:
		panic(fmt.Sprintf("unsupported node type %T", n))
	}

	if dsp.shouldPlanTestMetadata() {
		plan.AddNoGroupingStageWithCoreFunc(
			func(_ int, _ *distsqlplan.Processor) distsqlrun.ProcessorCoreUnion {
				return distsqlrun.ProcessorCoreUnion{
					MetadataTestSender: &distsqlrun.MetadataTestSenderSpec{
						ID: uuid.MakeV4().String(),
					},
				}
			},
			distsqlrun.PostProcessSpec{},
			plan.ResultTypes,
			plan.MergeOrdering,
		)
	}

	return plan, err
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
		ctx:             planCtx.ctx,
		extendedEvalCtx: planCtx.extendedEvalCtx,
		p:               nil,
	}
	if err := n.startExec(params); err != nil {
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
	if !n.distinctOnColIdxs.Empty() {
		for planCol, streamCol := range plan.planToStreamColMap {
			if streamCol != -1 && n.distinctOnColIdxs.Contains(planCol) {
				distinctColumns = append(distinctColumns, uint32(streamCol))
			}
		}
	} else {
		// If no distinct columns were specified, run distinct on the entire row.
		for planCol := range planColumns(n) {
			if streamCol := plan.planToStreamColMap[planCol]; streamCol != -1 {
				distinctColumns = append(distinctColumns, uint32(streamCol))
			}
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

// isOnlyOnGateway returns true if a physical plan is executed entirely on the
// gateway node.
func (dsp *DistSQLPlanner) isOnlyOnGateway(plan *physicalPlan) bool {
	if len(plan.ResultRouters) == 1 {
		processorIdx := plan.ResultRouters[0]
		if plan.Processors[processorIdx].Node == dsp.nodeDesc.NodeID {
			return true
		}
	}
	return false
}

// TODO(abhimadan): Refactor this function to reduce the UNION vs
// EXCEPT/INTERSECT and DISTINCT vs ALL branching.
//
// createPlanForSetOp creates a physical plan for "set operations". UNION plans
// are created by merging the left and right plans together, and INTERSECT and
// EXCEPT plans are created by performing a special type of join on the left and
// right sides. In the UNION DISTINCT case, a distinct stage is placed after the
// plans are merged, and in the INTERSECT/EXCEPT DISTINCT cases, distinct stages
// are added as the inputs of the join stage. In all DISTINCT cases, an
// additional distinct stage is placed at the end of the left and right plans if
// there are multiple nodes involved in the query, to reduce the amount of
// unnecessary network I/O.
//
// Examples (single node):
// - Query: ( VALUES (1), (2), (2) ) UNION ( VALUES (2), (3) )
//   Plan:
//   VALUES        VALUES
//     |             |
//      -------------
//            |
//        DISTINCT
//
// - Query: ( VALUES (1), (2), (2) ) INTERSECT ALL ( VALUES (2), (3) )
//   Plan:
//   VALUES        VALUES
//     |             |
//      -------------
//            |
//          JOIN
//
// - Query: ( VALUES (1), (2), (2) ) EXCEPT ( VALUES (2), (3) )
//   Plan:
//   VALUES        VALUES
//     |             |
//  DISTINCT       DISTINCT
//     |             |
//      -------------
//            |
//          JOIN
func (dsp *DistSQLPlanner) createPlanForSetOp(
	planCtx *planningCtx, n *unionNode,
) (physicalPlan, error) {
	leftLogicalPlan := n.left
	leftPlan, err := dsp.createPlanForNode(planCtx, n.left)
	if err != nil {
		return physicalPlan{}, err
	}
	rightLogicalPlan := n.right
	rightPlan, err := dsp.createPlanForNode(planCtx, n.right)
	if err != nil {
		return physicalPlan{}, err
	}
	if n.inverted {
		leftPlan, rightPlan = rightPlan, leftPlan
		leftLogicalPlan, rightLogicalPlan = rightLogicalPlan, leftLogicalPlan
	}
	childPhysicalPlans := []*physicalPlan{&leftPlan, &rightPlan}
	childLogicalPlans := []planNode{leftLogicalPlan, rightLogicalPlan}

	// Check that the left and right side planToStreamColMaps are equivalent.
	// TODO(solon): Are there any valid UNION/INTERSECT/EXCEPT cases where these
	// differ? If we encounter any, we could handle them by adding a projection on
	// the unioned columns on each side, similar to how we handle mismatched
	// ResultTypes.
	if !reflect.DeepEqual(leftPlan.planToStreamColMap, rightPlan.planToStreamColMap) {
		return physicalPlan{}, errors.Errorf(
			"planToStreamColMap mismatch: %v, %v", leftPlan.planToStreamColMap,
			rightPlan.planToStreamColMap)
	}
	planToStreamColMap := leftPlan.planToStreamColMap
	planCols := make([]int, 0, len(planToStreamColMap))
	streamCols := make([]uint32, 0, len(planToStreamColMap))
	for planCol, streamCol := range planToStreamColMap {
		if streamCol < 0 {
			continue
		}
		planCols = append(planCols, planCol)
		streamCols = append(streamCols, uint32(streamCol))
	}

	leftProps, rightProps := planPhysicalProps(leftLogicalPlan), planPhysicalProps(rightLogicalPlan)
	var distinctSpecs [2]distsqlrun.ProcessorCoreUnion

	if !n.all {
		leftProps = leftProps.project(planCols)
		rightProps = rightProps.project(planCols)

		var distinctOrds [2]distsqlrun.Ordering
		distinctOrds[0] = distsqlrun.ConvertToMappedSpecOrdering(
			leftProps.ordering, leftPlan.planToStreamColMap,
		)
		distinctOrds[1] = distsqlrun.ConvertToMappedSpecOrdering(
			rightProps.ordering, rightPlan.planToStreamColMap,
		)

		// Build distinct processor specs for the left and right child plans.
		//
		// Note there is the potential for further network I/O optimization here
		// in the UNION case, since rows are not deduplicated between left and right
		// until the single group stage. In the worst case (total duplication), this
		// causes double the amount of data to be streamed as necessary.
		for side, plan := range childPhysicalPlans {
			sortCols := make([]uint32, len(distinctOrds[side].Columns))
			for i, ord := range distinctOrds[side].Columns {
				sortCols[i] = ord.ColIdx
			}
			distinctSpec := &distinctSpecs[side]
			distinctSpec.Distinct = &distsqlrun.DistinctSpec{
				DistinctColumns: streamCols,
				OrderedColumns:  sortCols,
			}
			if !dsp.isOnlyOnGateway(plan) {
				// TODO(solon): We could skip this stage if there is a strong key on
				// the result columns.
				plan.AddNoGroupingStage(
					*distinctSpec, distsqlrun.PostProcessSpec{}, plan.ResultTypes, distinctOrds[side])
				plan.AddProjection(streamCols)
			}
		}
	}

	var p physicalPlan

	// Merge the plans' planToStreamColMap, which we know are equivalent.
	p.planToStreamColMap = planToStreamColMap

	// Merge the plans' result types and merge ordering.
	// TODO(abhimadan): This merge ordering can still contain columns from child
	// ORDER BY clauses, which will neither be accessible nor used in ordering
	// after the UNION. Since they aren't accessible and UNION/EXCEPT/INTERSECT
	// are not required to propagate orderings from their subqueries, this doesn't
	// affect correctness. However, it does force us to stream unnecessary data
	// until the ordering is eventually updated and they are projected out. This
	// projection happens for every set operation except for UNION ALL, which only
	// uses the naive ordering propagation below. To fix this, use similar logic
	// to the distinct case above to get the new ordering, and add a projection in
	// a no-grouping no-op stage.
	resultTypes, err := distsqlplan.MergeResultTypes(leftPlan.ResultTypes, rightPlan.ResultTypes)
	mergeOrdering := leftPlan.MergeOrdering
	if n.unionType != tree.UnionOp {
		// In INTERSECT and EXCEPT cases where the merge ordering contains columns
		// that don't appear in the output (e.g. SELECT k FROM kv ORDER BY v), we
		// cannot keep the ordering, since some ORDER BY columns are not also
		// equality columns. As a result, create a new ordering that only contains
		// columns in the result.
		newOrdering := computeMergeJoinOrdering(leftProps, rightProps, planCols, planCols)
		mergeOrdering = distsqlrun.ConvertToMappedSpecOrdering(newOrdering, p.planToStreamColMap)

		var childResultTypes [2][]sqlbase.ColumnType
		for side, plan := range childPhysicalPlans {
			childResultTypes[side] =
				getTypesForPlanResult(childLogicalPlans[side], plan.planToStreamColMap)
		}
		resultTypes, err = distsqlplan.MergeResultTypes(childResultTypes[0], childResultTypes[1])
		if err != nil {
			return physicalPlan{}, err
		}
	} else if err != nil || !mergeOrdering.Equal(rightPlan.MergeOrdering) {
		// The result types or merge ordering can differ between the two sides in
		// pathological cases, like if they have incompatible ORDER BY clauses.
		// Resolve this by collecting results on a single node and adding a
		// projection to the results that will be unioned.
		for _, plan := range childPhysicalPlans {
			plan.AddSingleGroupStage(
				dsp.nodeDesc.NodeID,
				distsqlrun.ProcessorCoreUnion{Noop: &distsqlrun.NoopCoreSpec{}},
				distsqlrun.PostProcessSpec{},
				plan.ResultTypes)
			plan.AddProjection(streamCols)
		}

		// Result types should now be mergeable.
		resultTypes, err = distsqlplan.MergeResultTypes(leftPlan.ResultTypes, rightPlan.ResultTypes)
		if err != nil {
			return physicalPlan{}, err
		}
		mergeOrdering = distsqlrun.Ordering{}
	}

	// Merge processors, streams, result routers, and stage counter.
	var leftRouters, rightRouters []distsqlplan.ProcessorIdx
	p.PhysicalPlan, leftRouters, rightRouters = distsqlplan.MergePlans(
		&leftPlan.PhysicalPlan, &rightPlan.PhysicalPlan)

	if n.unionType == tree.UnionOp {
		// We just need to append the left and right streams together, so append
		// the left and right output routers.
		p.ResultRouters = append(leftRouters, rightRouters...)

		p.ResultTypes = resultTypes
		p.SetMergeOrdering(mergeOrdering)

		if !n.all {
			// TODO(abhimadan): use columns from mergeOrdering to fill in the
			// OrderingColumns field in DistinctSpec once the unused columns
			// are projected out.
			distinctSpec := distsqlrun.ProcessorCoreUnion{
				Distinct: &distsqlrun.DistinctSpec{DistinctColumns: streamCols},
			}
			p.AddSingleGroupStage(
				dsp.nodeDesc.NodeID, distinctSpec, distsqlrun.PostProcessSpec{}, p.ResultTypes)
		}
	} else {
		// We plan INTERSECT and EXCEPT queries with joiners. Get the appropriate
		// join type.
		joinType := distsqlSetOpJoinType(n.unionType)

		// Nodes where we will run the join processors.
		nodes := findJoinProcessorNodes(leftRouters, rightRouters, p.Processors, true /* includeRight */)

		// Set up the equality columns.
		eqCols := streamCols

		// Project the left-side columns only.
		post := distsqlrun.PostProcessSpec{Projection: true}
		post.OutputColumns = make([]uint32, len(streamCols))
		copy(post.OutputColumns, streamCols)

		// Create the Core spec.
		//
		// TODO(radu): we currently only use merge joins when we have an ordering on
		// all equality columns. We should relax this by either:
		//  - implementing a hybrid hash/merge processor which implements merge
		//    logic on the columns we have an ordering on, and within each merge
		//    group uses a hashmap on the remaining columns
		//  - or: adding a sort processor to complete the order
		var core distsqlrun.ProcessorCoreUnion
		if !planMergeJoins.Get(&dsp.st.SV) || len(mergeOrdering.Columns) < len(streamCols) {
			core.HashJoiner = &distsqlrun.HashJoinerSpec{
				LeftEqColumns:  eqCols,
				RightEqColumns: eqCols,
				Type:           joinType,
			}
		} else {
			core.MergeJoiner = &distsqlrun.MergeJoinerSpec{
				LeftOrdering:  mergeOrdering,
				RightOrdering: mergeOrdering,
				Type:          joinType,
			}
		}

		if n.all {
			p.AddJoinStage(
				nodes, core, post, eqCols, eqCols,
				leftPlan.ResultTypes, rightPlan.ResultTypes,
				leftPlan.MergeOrdering, rightPlan.MergeOrdering,
				leftRouters, rightRouters, true, /* includeRight */
			)
		} else {
			p.AddDistinctSetOpStage(
				nodes, core, distinctSpecs[:], post, eqCols,
				leftPlan.ResultTypes, rightPlan.ResultTypes,
				leftPlan.MergeOrdering, rightPlan.MergeOrdering,
				leftRouters, rightRouters,
			)
		}

		// An EXCEPT ALL is like a left outer join, so there is no guaranteed ordering.
		if n.unionType == tree.ExceptOp {
			mergeOrdering = distsqlrun.Ordering{}
		}

		p.ResultTypes = resultTypes
		p.SetMergeOrdering(mergeOrdering)
	}

	return p, nil
}

func (dsp *DistSQLPlanner) newPlanningCtx(
	ctx context.Context, evalCtx *extendedEvalContext, txn *client.Txn,
) planningCtx {
	planCtx := planningCtx{
		ctx:             ctx,
		extendedEvalCtx: evalCtx,
		spanIter:        dsp.spanResolver.NewSpanResolverIterator(txn),
		nodeAddresses:   make(map[roachpb.NodeID]string),
	}
	planCtx.nodeAddresses[dsp.nodeDesc.NodeID] = dsp.nodeDesc.Address.String()
	return planCtx
}

// FinalizePlan adds a final "result" stage if necessary and populates the
// endpoints of the plan.
func (dsp *DistSQLPlanner) FinalizePlan(planCtx *planningCtx, plan *physicalPlan) {
	// Find all MetadataTestSenders in the plan, so that the MetadataTestReceiver
	// knows how many sender IDs it should expect.
	var metadataSenders []string
	for _, proc := range plan.Processors {
		if proc.Spec.Core.MetadataTestSender != nil {
			metadataSenders = append(metadataSenders, proc.Spec.Core.MetadataTestSender.ID)
		}
	}
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

	if len(metadataSenders) > 0 {
		plan.AddSingleGroupStage(
			thisNodeID,
			distsqlrun.ProcessorCoreUnion{
				MetadataTestReceiver: &distsqlrun.MetadataTestReceiverSpec{
					SenderIDs: metadataSenders,
				},
			},
			distsqlrun.PostProcessSpec{},
			plan.ResultTypes,
		)
	}

	// Set up the endpoints for p.streams.
	plan.PopulateEndpoints(planCtx.nodeAddresses)

	// Set up the endpoint for the final result.
	finalOut := &plan.Processors[plan.ResultRouters[0]].Spec.Output[0]
	finalOut.Streams = append(finalOut.Streams, distsqlrun.StreamEndpointSpec{
		Type: distsqlrun.StreamEndpointSpec_SYNC_RESPONSE,
	})
}

func makeTableReaderSpans(spans roachpb.Spans) []distsqlrun.TableReaderSpan {
	trSpans := make([]distsqlrun.TableReaderSpan, len(spans))
	for i, span := range spans {
		trSpans[i].Span = span
	}

	return trSpans
}
