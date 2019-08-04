// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan/replicaoracle"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// DistSQLPlanner is used to generate distributed plans from logical
// plans. A rough overview of the process:
//
//  - the plan is based on a planNode tree (in the future it will be based on an
//    intermediate representation tree). Only a subset of the possible trees is
//    supported (this can be checked via CheckSupport).
//
//  - we generate a PhysicalPlan for the planNode tree recursively. The
//    PhysicalPlan consists of a network of processors and streams, with a set
//    of unconnected "result routers". The PhysicalPlan also has information on
//    ordering and on the mapping planNode columns to columns in the result
//    streams (all result routers output streams with the same schema).
//
//    The PhysicalPlan for a scanNode leaf consists of TableReaders, one for each node
//    that has one or more ranges.
//
//  - for each an internal planNode we start with the plan of the child node(s)
//    and add processing stages (connected to the result routers of the children
//    node).
type DistSQLPlanner struct {
	// planVersion is the version of DistSQL targeted by the plan we're building.
	// This is currently only assigned to the node's current DistSQL version and
	// is used to skip incompatible nodes when mapping spans.
	planVersion distsqlpb.DistSQLVersion

	st *cluster.Settings
	// The node descriptor for the gateway node that initiated this query.
	nodeDesc     roachpb.NodeDescriptor
	stopper      *stop.Stopper
	distSQLSrv   *distsqlrun.ServerImpl
	spanResolver distsqlplan.SpanResolver

	// metadataTestTolerance is the minimum level required to plan metadata test
	// processors.
	metadataTestTolerance distsqlrun.MetadataTestLevel

	// runnerChan is used to send out requests (for running SetupFlow RPCs) to a
	// pool of workers.
	runnerChan chan runnerRequest

	// gossip handle used to check node version compatibility and to construct
	// the spanResolver.
	gossip *gossip.Gossip

	nodeDialer *nodedialer.Dialer

	// nodeHealth encapsulates the various node health checks to avoid planning
	// on unhealthy nodes.
	nodeHealth distSQLNodeHealth

	// distSender is used to construct the spanResolver upon SetNodeDesc.
	distSender *kv.DistSender
	// rpcCtx is used to construct the spanResolver upon SetNodeDesc.
	rpcCtx *rpc.Context
}

// ReplicaOraclePolicy controls which policy the physical planner uses to choose
// a replica for a given range. It is exported so that it may be overwritten
// during initialization by CCL code to enable follower reads.
var ReplicaOraclePolicy = replicaoracle.BinPackingChoice

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

// livenessProvider provides just the methods of storage.NodeLiveness that the
// DistSQLPlanner needs, to avoid importing all of storage.
type livenessProvider interface {
	IsLive(roachpb.NodeID) (bool, error)
}

// NewDistSQLPlanner initializes a DistSQLPlanner.
//
// nodeDesc is the descriptor of the node on which this planner runs. It is used
// to favor itself and other close-by nodes when planning. An empty descriptor
// can be passed to aid bootstrapping, but then SetNodeDesc() needs to be called
// before this planner is used.
func NewDistSQLPlanner(
	ctx context.Context,
	planVersion distsqlpb.DistSQLVersion,
	st *cluster.Settings,
	nodeDesc roachpb.NodeDescriptor,
	rpcCtx *rpc.Context,
	distSQLSrv *distsqlrun.ServerImpl,
	distSender *kv.DistSender,
	gossip *gossip.Gossip,
	stopper *stop.Stopper,
	liveness livenessProvider,
	nodeDialer *nodedialer.Dialer,
) *DistSQLPlanner {
	if liveness == nil {
		log.Fatal(ctx, "must specify liveness")
	}
	dsp := &DistSQLPlanner{
		planVersion: planVersion,
		st:          st,
		nodeDesc:    nodeDesc,
		stopper:     stopper,
		distSQLSrv:  distSQLSrv,
		gossip:      gossip,
		nodeDialer:  nodeDialer,
		nodeHealth: distSQLNodeHealth{
			gossip:     gossip,
			connHealth: nodeDialer.ConnHealth,
		},
		distSender:            distSender,
		rpcCtx:                rpcCtx,
		metadataTestTolerance: distsqlrun.NoExplain,
	}
	dsp.nodeHealth.isLive = liveness.IsLive

	dsp.initRunners()
	return dsp
}

func (dsp *DistSQLPlanner) shouldPlanTestMetadata() bool {
	return dsp.distSQLSrv.TestingKnobs.MetadataTestLevel >= dsp.metadataTestTolerance
}

// SetNodeDesc sets the planner's node descriptor.
// The first call to SetNodeDesc leads to the construction of the SpanResolver.
func (dsp *DistSQLPlanner) SetNodeDesc(desc roachpb.NodeDescriptor) {
	dsp.nodeDesc = desc
	if dsp.spanResolver == nil {
		sr := distsqlplan.NewSpanResolver(dsp.st, dsp.distSender, dsp.gossip, desc,
			dsp.rpcCtx, ReplicaOraclePolicy)
		dsp.SetSpanResolver(sr)
	}
}

// SetSpanResolver switches to a different SpanResolver. It is the caller's
// responsibility to make sure the DistSQLPlanner is not in use.
func (dsp *DistSQLPlanner) SetSpanResolver(spanResolver distsqlplan.SpanResolver) {
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
		if t.Type.Family() == types.OidFamily {
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

type distRecommendation int

const (
	// cannotDistribute indicates that a plan cannot be distributed.
	cannotDistribute distRecommendation = iota

	// shouldNotDistribute indicates that a plan could suffer if distributed.
	shouldNotDistribute

	// canDistribute indicates that a plan will probably not benefit but will
	// probably not suffer if distributed.
	canDistribute

	// shouldDistribute indicates that a plan will likely benefit if distributed.
	shouldDistribute
)

// compose returns the recommendation for a plan given recommendations for two
// parts of it: if we shouldNotDistribute either part, then we
// shouldNotDistribute the overall plan either.
func (a distRecommendation) compose(b distRecommendation) distRecommendation {
	if a == cannotDistribute || b == cannotDistribute {
		return cannotDistribute
	}
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

// mustWrapNode returns true if a node has no DistSQL-processor equivalent.
// This must be kept in sync with createPlanForNode.
// TODO(jordan): refactor these to use the observer pattern to avoid duplication.
func (dsp *DistSQLPlanner) mustWrapNode(planCtx *PlanningCtx, node planNode) bool {
	switch n := node.(type) {
	case *scanNode:
	case *indexJoinNode:
	case *lookupJoinNode:
	case *zigzagJoinNode:
	case *joinNode:
	case *renderNode:
	case *groupNode:
	case *sortNode:
	case *filterNode:
	case *limitNode:
	case *distinctNode:
	case *unionNode:
	case *virtualTableNode:
	case *projectSetNode:
	case *unaryNode:
	case *windowNode:
	case *zeroNode:
	case *valuesNode:
		// This is unfortunately duplicated by createPlanForNode, and must be kept
		// in sync with its implementation.
		if !n.specifiedInQuery || planCtx.isLocal || planCtx.noEvalSubqueries {
			return true
		}
		return false
	default:
		return true
	}
	return false
}

// checkSupportForNode returns a distRecommendation (as described above) or
// cannotDistribute and an error if the plan subtree is not distributable.
// The error doesn't indicate complete failure - it's instead the reason that
// this plan couldn't be distributed.
// TODO(radu): add tests for this.
func (dsp *DistSQLPlanner) checkSupportForNode(node planNode) (distRecommendation, error) {
	switch n := node.(type) {
	case *filterNode:
		if err := dsp.checkExpr(n.filter); err != nil {
			return cannotDistribute, err
		}
		return dsp.checkSupportForNode(n.source.plan)

	case *renderNode:
		for _, e := range n.render {
			if err := dsp.checkExpr(e); err != nil {
				return cannotDistribute, err
			}
		}
		return dsp.checkSupportForNode(n.source.plan)

	case *sortNode:
		rec, err := dsp.checkSupportForNode(n.plan)
		if err != nil {
			return cannotDistribute, err
		}
		// If we have to sort, distribute the query.
		if n.needSort {
			rec = rec.compose(shouldDistribute)
		}
		return rec, nil

	case *joinNode:
		if err := dsp.checkExpr(n.pred.onCond); err != nil {
			return cannotDistribute, err
		}
		recLeft, err := dsp.checkSupportForNode(n.left.plan)
		if err != nil {
			return cannotDistribute, err
		}
		recRight, err := dsp.checkSupportForNode(n.right.plan)
		if err != nil {
			return cannotDistribute, err
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
				return cannotDistribute, err
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
			return cannotDistribute, err
		}
		return dsp.checkSupportForNode(n.index)

	case *lookupJoinNode:
		if err := dsp.checkExpr(n.onCond); err != nil {
			return cannotDistribute, err
		}
		if _, err := dsp.checkSupportForNode(n.input); err != nil {
			return cannotDistribute, err
		}
		return shouldDistribute, nil

	case *zigzagJoinNode:
		if err := dsp.checkExpr(n.onCond); err != nil {
			return cannotDistribute, err
		}
		return shouldDistribute, nil

	case *groupNode:
		rec, err := dsp.checkSupportForNode(n.plan)
		if err != nil {
			return cannotDistribute, err
		}
		// Distribute aggregations if possible.
		return rec.compose(shouldDistribute), nil

	case *limitNode:
		if err := dsp.checkExpr(n.countExpr); err != nil {
			return cannotDistribute, err
		}
		if err := dsp.checkExpr(n.offsetExpr); err != nil {
			return cannotDistribute, err
		}
		return dsp.checkSupportForNode(n.plan)

	case *distinctNode:
		return dsp.checkSupportForNode(n.plan)

	case *unionNode:
		recLeft, err := dsp.checkSupportForNode(n.left)
		if err != nil {
			return cannotDistribute, err
		}
		recRight, err := dsp.checkSupportForNode(n.right)
		if err != nil {
			return cannotDistribute, err
		}
		return recLeft.compose(recRight), nil

	case *valuesNode:
		if !n.specifiedInQuery {
			// This condition indicates that the valuesNode was created by planning,
			// not by the user, like the way vtables are expanded into valuesNodes. We
			// don't want to distribute queries like this across the network.
			return cannotDistribute, newQueryNotSupportedErrorf("unsupported valuesNode, not specified in query")
		}

		for _, tuple := range n.tuples {
			for _, expr := range tuple {
				if err := dsp.checkExpr(expr); err != nil {
					return cannotDistribute, err
				}
			}
		}
		return canDistribute, nil

	case *insertNode, *updateNode, *deleteNode, *upsertNode:
		// This is a potential hot path.
		return cannotDistribute, mutationsNotSupportedError

	case *setVarNode, *setClusterSettingNode:
		// SET statements are never distributed.
		return cannotDistribute, setNotSupportedError

	case *projectSetNode:
		return dsp.checkSupportForNode(n.source)

	case *unaryNode:
		return canDistribute, nil

	case *zeroNode:
		return canDistribute, nil

	case *windowNode:
		return dsp.checkSupportForNode(n.plan)

	default:
		return cannotDistribute, newQueryNotSupportedErrorf("unsupported node %T", node)
	}
}

// PlanningCtx contains data used and updated throughout the planning process of
// a single query.
type PlanningCtx struct {
	ctx             context.Context
	ExtendedEvalCtx *extendedEvalContext
	spanIter        distsqlplan.SpanResolverIterator
	// NodeAddresses contains addresses for all NodeIDs that are referenced by any
	// PhysicalPlan we generate with this context.
	// Nodes that fail a health check have empty addresses.
	NodeAddresses map[roachpb.NodeID]string

	// isLocal is set to true if we're planning this query on a single node.
	isLocal bool
	planner *planner
	// ignoreClose, when set to true, will prevent the closing of the planner's
	// current plan. Only the top-level query needs to close it, but everything
	// else (like sub- and postqueries, or EXPLAIN ANALYZE) should set this to
	// true to avoid double closes of the planNode tree.
	ignoreClose bool
	stmtType    tree.StatementType
	// planDepth is set to the current depth of the planNode tree. It's used to
	// keep track of whether it's valid to run a root node in a special fast path
	// mode.
	planDepth int

	// noEvalSubqueries indicates that the plan expects any subqueries to not
	// be replaced by evaluation. Should only be set by EXPLAIN.
	noEvalSubqueries bool
}

var _ distsqlplan.ExprContext = &PlanningCtx{}

// EvalContext returns the associated EvalContext, or nil if there isn't one.
func (p *PlanningCtx) EvalContext() *tree.EvalContext {
	if p.ExtendedEvalCtx == nil {
		return nil
	}
	return &p.ExtendedEvalCtx.EvalContext
}

// IsLocal returns true if this PlanningCtx is being used to plan a query that
// has no remote flows.
func (p *PlanningCtx) IsLocal() bool {
	return p.isLocal
}

// EvaluateSubqueries returns true if this plan requires subqueries be fully
// executed before trying to marshal. This is normally true except for in the
// case of EXPLAIN queries, which ultimately want to describe the subquery that
// will run, without actually running it.
func (p *PlanningCtx) EvaluateSubqueries() bool {
	return !p.noEvalSubqueries
}

// sanityCheckAddresses returns an error if the same address is used by two
// nodes.
func (p *PlanningCtx) sanityCheckAddresses() error {
	inverted := make(map[string]roachpb.NodeID)
	for nodeID, addr := range p.NodeAddresses {
		if otherNodeID, ok := inverted[addr]; ok {
			return errors.Errorf(
				"different nodes %d and %d with the same address '%s'", nodeID, otherNodeID, addr)
		}
		inverted[addr] = nodeID
	}
	return nil
}

// PhysicalPlan is a partial physical plan which corresponds to a planNode
// (partial in that it can correspond to a planNode subtree and not necessarily
// to the entire planNode for a given query).
//
// It augments distsqlplan.PhysicalPlan with information relating the physical
// plan to a planNode subtree.
//
// These plans are built recursively on a planNode tree.
type PhysicalPlan struct {
	distsqlplan.PhysicalPlan

	// PlanToStreamColMap maps planNode columns (see planColumns()) to columns in
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
	// When the query is run, the output processor's PlanToStreamColMap is used
	// by DistSQLReceiver to create an implicit projection on the processor's
	// output for client consumption (see DistSQLReceiver.Push()). Therefore,
	// "invisible" columns (e.g., columns required for merge ordering) will not
	// be output.
	PlanToStreamColMap []int
}

// makePlanToStreamColMap initializes a new PhysicalPlan.PlanToStreamColMap. The
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

// SpanPartition is the intersection between a set of spans for a certain
// operation (e.g table scan) and the set of ranges owned by a given node.
type SpanPartition struct {
	Node  roachpb.NodeID
	Spans roachpb.Spans
}

type distSQLNodeHealth struct {
	gossip     *gossip.Gossip
	connHealth func(roachpb.NodeID) error
	isLive     func(roachpb.NodeID) (bool, error)
}

func (h *distSQLNodeHealth) check(ctx context.Context, nodeID roachpb.NodeID) error {
	{
		// NB: as of #22658, ConnHealth does not work as expected; see the
		// comment within. We still keep this code for now because in
		// practice, once the node is down it will prevent using this node
		// 90% of the time (it gets used around once per second as an
		// artifact of rpcContext's reconnection mechanism at the time of
		// writing). This is better than having it used in 100% of cases
		// (until the liveness check below kicks in).
		err := h.connHealth(nodeID)
		if err != nil && err != rpc.ErrNotHeartbeated {
			// This host is known to be unhealthy. Don't use it (use the gateway
			// instead). Note: this can never happen for our nodeID (which
			// always has its address in the nodeMap).
			log.VEventf(ctx, 1, "marking n%d as unhealthy for this plan: %v", nodeID, err)
			return err
		}
	}
	{
		live, err := h.isLive(nodeID)
		if err == nil && !live {
			err = pgerror.Newf(pgcode.CannotConnectNow,
				"node n%d is not live", errors.Safe(nodeID))
		}
		if err != nil {
			return pgerror.Wrapf(err, pgcode.CannotConnectNow,
				"not using n%d due to liveness", errors.Safe(nodeID))
		}
	}

	// Check that the node is not draining.
	drainingInfo := &distsqlpb.DistSQLDrainingInfo{}
	if err := h.gossip.GetInfoProto(gossip.MakeDistSQLDrainingKey(nodeID), drainingInfo); err != nil {
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

// PartitionSpans finds out which nodes are owners for ranges touching the
// given spans, and splits the spans according to owning nodes. The result is a
// set of SpanPartitions (guaranteed one for each relevant node), which form a
// partitioning of the spans (i.e. they are non-overlapping and their union is
// exactly the original set of spans).
//
// PartitionSpans does its best to not assign ranges on nodes that are known to
// either be unhealthy or running an incompatible version. The ranges owned by
// such nodes are assigned to the gateway.
func (dsp *DistSQLPlanner) PartitionSpans(
	planCtx *PlanningCtx, spans roachpb.Spans,
) ([]SpanPartition, error) {
	if len(spans) == 0 {
		panic("no spans")
	}
	ctx := planCtx.ctx
	partitions := make([]SpanPartition, 0, 1)
	if planCtx.isLocal {
		// If we're planning locally, map all spans to the local node.
		partitions = append(partitions,
			SpanPartition{dsp.nodeDesc.NodeID, spans})
		return partitions, nil
	}
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
				descCpy := desc // don't let desc escape
				log.Infof(ctx, "lastKey: %s desc: %s", lastKey, &descCpy)
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
				addr, inAddrMap := planCtx.NodeAddresses[nodeID]
				if !inAddrMap {
					addr = replInfo.NodeDesc.Address.String()
					if err := dsp.nodeHealth.check(ctx, nodeID); err != nil {
						addr = ""
					}
					if err == nil && addr != "" {
						planCtx.NodeAddresses[nodeID] = addr
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
					partitions = append(partitions, SpanPartition{Node: nodeID})
					nodeMap[nodeID] = partitionIdx
				}
			}
			partition := &partitions[partitionIdx]

			if lastNodeID == nodeID {
				// Two consecutive ranges on the same node, merge the spans.
				partition.Spans[len(partition.Spans)-1].EndKey = endKey.AsRawKey()
			} else {
				partition.Spans = append(partition.Spans, roachpb.Span{
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
	nodeID roachpb.NodeID, planVer distsqlpb.DistSQLVersion,
) bool {
	var v distsqlpb.DistSQLVersionGossipInfo
	if err := dsp.gossip.GetInfoProto(gossip.MakeDistSQLNodeVersionKey(nodeID), &v); err != nil {
		return false
	}
	return distsqlrun.FlowVerIsCompatible(dsp.planVersion, v.MinAcceptedVersion, v.Version)
}

func getIndexIdx(n *scanNode) (uint32, error) {
	if n.index.ID == n.desc.PrimaryIndex.ID {
		return 0, nil
	}
	for i := range n.desc.Indexes {
		if n.index.ID == n.desc.Indexes[i].ID {
			// IndexIdx is 1 based (0 means primary index).
			return uint32(i + 1), nil
		}
	}
	return 0, errors.Errorf("invalid scanNode index %v (table %s)", n.index, n.desc.Name)
}

// initTableReaderSpec initializes a TableReaderSpec/PostProcessSpec that
// corresponds to a scanNode, except for the Spans and OutputColumns.
func initTableReaderSpec(
	n *scanNode, planCtx *PlanningCtx, indexVarMap []int,
) (*distsqlpb.TableReaderSpec, distsqlpb.PostProcessSpec, error) {
	s := distsqlplan.NewTableReaderSpec()
	*s = distsqlpb.TableReaderSpec{
		Table:      *n.desc.TableDesc(),
		Reverse:    n.reverse,
		IsCheck:    n.isCheck,
		Visibility: n.colCfg.visibility.toDistSQLScanVisibility(),

		// Retain the capacity of the spans slice.
		Spans: s.Spans[:0],
	}
	indexIdx, err := getIndexIdx(n)
	if err != nil {
		return nil, distsqlpb.PostProcessSpec{}, err
	}
	s.IndexIdx = indexIdx

	// When a TableReader is running scrub checks, do not allow a
	// post-processor. This is because the outgoing stream is a fixed
	// format (distsqlrun.ScrubTypes).
	if n.isCheck {
		return s, distsqlpb.PostProcessSpec{}, nil
	}

	filter, err := distsqlplan.MakeExpression(n.filter, planCtx, indexVarMap)
	if err != nil {
		return nil, distsqlpb.PostProcessSpec{}, err
	}
	post := distsqlpb.PostProcessSpec{
		Filter: filter,
	}

	if n.hardLimit != 0 {
		post.Limit = uint64(n.hardLimit)
	} else if n.softLimit != 0 {
		s.LimitHint = n.softLimit
	}
	return s, post, nil
}

// scanNodeOrdinal returns the index of a column with the given ID.
func tableOrdinal(
	desc *sqlbase.ImmutableTableDescriptor, colID sqlbase.ColumnID, visibility scanVisibility,
) int {
	for i := range desc.Columns {
		if desc.Columns[i].ID == colID {
			return i
		}
	}
	if visibility == publicAndNonPublicColumns {
		offset := len(desc.Columns)
		for i, col := range desc.MutationColumns() {
			if col.ID == colID {
				return offset + i
			}
		}
	}
	panic(fmt.Sprintf("column %d not in desc.Columns", colID))
}

// getScanNodeToTableOrdinalMap returns a map from scan node column ordinal to
// table reader column ordinal. Returns nil if the map is identity.
//
// scanNodes can have columns set up in a few different ways, depending on the
// colCfg. The heuristic planner always creates scanNodes with all public
// columns (even if some of them aren't even in the index we are scanning).
// The optimizer creates scanNodes with a specific set of wanted columns; in
// this case we have to create a map from scanNode column ordinal to table
// column ordinal (which is what the TableReader uses).
func getScanNodeToTableOrdinalMap(n *scanNode) []int {
	if n.colCfg.wantedColumns == nil {
		return nil
	}
	if n.colCfg.addUnwantedAsHidden {
		panic("addUnwantedAsHidden not supported")
	}
	res := make([]int, len(n.cols))
	for i := range res {
		res[i] = tableOrdinal(n.desc, n.cols[i].ID, n.colCfg.visibility)
	}
	return res
}

// getOutputColumnsFromScanNode returns the indices of the columns that are
// returned by a scanNode.
// If remap is not nil, the column ordinals are remapped accordingly.
func getOutputColumnsFromScanNode(n *scanNode, remap []int) []uint32 {
	outputColumns := make([]uint32, 0, n.valNeededForCol.Len())
	// TODO(radu): if we have a scan with a filter, valNeededForCol will include
	// the columns needed for the filter, even if they aren't needed for the
	// next stage.
	n.valNeededForCol.ForEach(func(i int) {
		if remap != nil {
			i = remap[i]
		}
		outputColumns = append(outputColumns, uint32(i))
	})
	return outputColumns
}

// convertOrdering maps the columns in props.ordering to the output columns of a
// processor.
func (dsp *DistSQLPlanner) convertOrdering(
	props physicalProps, planToStreamColMap []int,
) distsqlpb.Ordering {
	if len(props.ordering) == 0 {
		return distsqlpb.Ordering{}
	}
	result := distsqlpb.Ordering{
		Columns: make([]distsqlpb.Ordering_Column, len(props.ordering)),
	}
	for i, o := range props.ordering {
		streamColIdx := o.ColIdx
		if planToStreamColMap != nil {
			streamColIdx = planToStreamColMap[o.ColIdx]
		}
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
		dir := distsqlpb.Ordering_Column_ASC
		if o.Direction == encoding.Descending {
			dir = distsqlpb.Ordering_Column_DESC
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
	planCtx *PlanningCtx, spans []roachpb.Span, reverse bool,
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

	nodeID := replInfo.NodeDesc.NodeID
	if err := dsp.CheckNodeHealthAndVersion(planCtx, replInfo.NodeDesc); err != nil {
		log.Eventf(planCtx.ctx, "not planning on node %d. %v", nodeID, err)
		return dsp.nodeDesc.NodeID, nil
	}
	return nodeID, nil
}

// CheckNodeHealthAndVersion adds the node to planCtx if it is healthy and
// has a compatible version. An error is returned otherwise.
func (dsp *DistSQLPlanner) CheckNodeHealthAndVersion(
	planCtx *PlanningCtx, desc *roachpb.NodeDescriptor,
) error {
	nodeID := desc.NodeID
	var err error

	if err = dsp.nodeHealth.check(planCtx.ctx, nodeID); err != nil {
		err = errors.New("unhealthy")
	} else if !dsp.nodeVersionIsCompatible(nodeID, dsp.planVersion) {
		err = errors.New("incompatible version")
	} else {
		planCtx.NodeAddresses[nodeID] = desc.Address.String()
	}
	return err
}

// createTableReaders generates a plan consisting of table reader processors,
// one for each node that has spans that we are reading.
// overridesResultColumns is optional.
func (dsp *DistSQLPlanner) createTableReaders(
	planCtx *PlanningCtx, n *scanNode, overrideResultColumns []sqlbase.ColumnID,
) (PhysicalPlan, error) {

	scanNodeToTableOrdinalMap := getScanNodeToTableOrdinalMap(n)
	spec, post, err := initTableReaderSpec(n, planCtx, scanNodeToTableOrdinalMap)
	if err != nil {
		return PhysicalPlan{}, err
	}

	var spanPartitions []SpanPartition
	if planCtx.isLocal {
		spanPartitions = []SpanPartition{{dsp.nodeDesc.NodeID, n.spans}}
	} else if n.hardLimit == 0 && n.softLimit == 0 {
		// No limit - plan all table readers where their data live.
		spanPartitions, err = dsp.PartitionSpans(planCtx, n.spans)
		if err != nil {
			return PhysicalPlan{}, err
		}
	} else {
		// If the scan is limited, use a single TableReader to avoid reading more
		// rows than necessary. Note that distsql is currently only enabled for hard
		// limits since the TableReader will still read too eagerly in the soft
		// limit case. To prevent this we'll need a new mechanism on the execution
		// side to modulate table reads.
		nodeID, err := dsp.getNodeIDForScan(planCtx, n.spans, n.reverse)
		if err != nil {
			return PhysicalPlan{}, err
		}
		spanPartitions = []SpanPartition{{nodeID, n.spans}}
	}

	var p PhysicalPlan
	stageID := p.NewStageID()

	p.ResultRouters = make([]distsqlplan.ProcessorIdx, len(spanPartitions))
	p.Processors = make([]distsqlplan.Processor, 0, len(spanPartitions))

	returnMutations := n.colCfg.visibility == publicAndNonPublicColumns

	for i, sp := range spanPartitions {
		var tr *distsqlpb.TableReaderSpec
		if i == 0 {
			// For the first span partition, we can just directly use the spec we made
			// above.
			tr = spec
		} else {
			// For the rest, we have to copy the spec into a fresh spec.
			tr = distsqlplan.NewTableReaderSpec()
			// Grab the Spans field of the new spec, and reuse it in case the pooled
			// TableReaderSpec we got has pre-allocated Spans memory.
			newSpansSlice := tr.Spans
			*tr = *spec
			tr.Spans = newSpansSlice
		}
		for j := range sp.Spans {
			tr.Spans = append(tr.Spans, distsqlpb.TableReaderSpan{Span: sp.Spans[j]})
		}

		tr.MaxResults = n.maxResults

		proc := distsqlplan.Processor{
			Node: sp.Node,
			Spec: distsqlpb.ProcessorSpec{
				Core:    distsqlpb.ProcessorCoreUnion{TableReader: tr},
				Output:  []distsqlpb.OutputRouterSpec{{Type: distsqlpb.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}

		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}

	if len(p.ResultRouters) > 1 && len(n.props.ordering) > 0 {
		// Make a note of the fact that we have to maintain a certain ordering
		// between the parallel streams.
		//
		// This information is taken into account by the AddProjection call below:
		// specifically, it will make sure these columns are kept even if they are
		// not in the projection (e.g. "SELECT v FROM kv ORDER BY k").
		p.SetMergeOrdering(dsp.convertOrdering(n.props, scanNodeToTableOrdinalMap))
	}

	var typs []types.T
	if returnMutations {
		typs = make([]types.T, 0, len(n.desc.Columns)+len(n.desc.MutationColumns()))
	} else {
		typs = make([]types.T, 0, len(n.desc.Columns))
	}
	for i := range n.desc.Columns {
		typs = append(typs, n.desc.Columns[i].Type)
	}
	if returnMutations {
		for _, col := range n.desc.MutationColumns() {
			typs = append(typs, col.Type)
		}
	}
	p.SetLastStagePost(post, typs)

	var outCols []uint32
	if overrideResultColumns == nil {
		outCols = getOutputColumnsFromScanNode(n, scanNodeToTableOrdinalMap)
	} else {
		outCols = make([]uint32, len(overrideResultColumns))
		for i, id := range overrideResultColumns {
			outCols[i] = uint32(tableOrdinal(n.desc, id, n.colCfg.visibility))
		}
	}
	planToStreamColMap := make([]int, len(n.cols))
	descColumnIDs := make([]sqlbase.ColumnID, 0, len(n.desc.Columns))
	for i := range n.desc.Columns {
		descColumnIDs = append(descColumnIDs, n.desc.Columns[i].ID)
	}
	if returnMutations {
		for _, c := range n.desc.MutationColumns() {
			descColumnIDs = append(descColumnIDs, c.ID)
		}
	}
	for i := range planToStreamColMap {
		planToStreamColMap[i] = -1
		for j, c := range outCols {
			if descColumnIDs[c] == n.cols[i].ID {
				planToStreamColMap[i] = j
				break
			}
		}
	}
	p.AddProjection(outCols)

	p.PlanToStreamColMap = planToStreamColMap
	return p, nil
}

// selectRenders takes a PhysicalPlan that produces the results corresponding to
// the select data source (a n.source) and updates it to produce results
// corresponding to the render node itself. An evaluator stage is added if the
// render node has any expressions which are not just simple column references.
func (dsp *DistSQLPlanner) selectRenders(
	p *PhysicalPlan, n *renderNode, planCtx *PlanningCtx,
) error {
	// We want to skip any unused renders.
	planToStreamColMap := makePlanToStreamColMap(len(n.render))
	renders := make([]tree.TypedExpr, 0, len(n.render))
	for i, r := range n.render {
		if !n.columns[i].Omitted {
			planToStreamColMap[i] = len(renders)
			renders = append(renders, r)
		}
	}

	typs, err := getTypesForPlanResult(n, planToStreamColMap)
	if err != nil {
		return err
	}
	err = p.AddRendering(renders, planCtx, p.PlanToStreamColMap, typs)
	if err != nil {
		return err
	}
	p.PlanToStreamColMap = planToStreamColMap
	return nil
}

// addSorters adds sorters corresponding to a sortNode and updates the plan to
// reflect the sort node.
func (dsp *DistSQLPlanner) addSorters(p *PhysicalPlan, n *sortNode) {

	matchLen := planPhysicalProps(n.plan).computeMatch(n.ordering)

	if matchLen < len(n.ordering) {
		// Sorting is needed; we add a stage of sorting processors.
		ordering := distsqlpb.ConvertToMappedSpecOrdering(n.ordering, p.PlanToStreamColMap)

		p.AddNoGroupingStage(
			distsqlpb.ProcessorCoreUnion{
				Sorter: &distsqlpb.SorterSpec{
					OutputOrdering:   ordering,
					OrderingMatchLen: uint32(matchLen),
				},
			},
			distsqlpb.PostProcessSpec{},
			p.ResultTypes,
			ordering,
		)
	}

	if len(n.columns) != len(p.PlanToStreamColMap) {
		// In cases like:
		//   SELECT a FROM t ORDER BY b
		// we have columns (b) that are only used for sorting. These columns are not
		// in the output columns of the sortNode; we set a projection such that the
		// plan results map 1-to-1 to sortNode columns.
		//
		// Note that internally, AddProjection might retain more columns than
		// necessary so we can preserve the p.Ordering between parallel streams
		// when they merge later.
		p.PlanToStreamColMap = p.PlanToStreamColMap[:len(n.columns)]
		columns := make([]uint32, 0, len(n.columns))
		for i, col := range p.PlanToStreamColMap {
			if col < 0 {
				// This column isn't needed; ignore it.
				continue
			}
			p.PlanToStreamColMap[i] = len(columns)
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
//        This is evaluated in the post aggregation evaluator attached after.
//      - Expressions that also appear verbatim in the GROUP BY expressions.
//        For 'SELECT k GROUP BY k', the aggregation function added is IDENT,
//        therefore k just passes through unchanged.
//    All other expressions simply pass through unchanged, for e.g. '1' in
//    'SELECT 1 GROUP BY k'.
func (dsp *DistSQLPlanner) addAggregators(
	planCtx *PlanningCtx, p *PhysicalPlan, n *groupNode,
) error {
	aggregations := make([]distsqlpb.AggregatorSpec_Aggregation, len(n.funcs))
	aggregationsColumnTypes := make([][]types.T, len(n.funcs))
	for i, fholder := range n.funcs {
		// Convert the aggregate function to the enum value with the same string
		// representation.
		funcStr := strings.ToUpper(fholder.funcName)
		funcIdx, ok := distsqlpb.AggregatorSpec_Func_value[funcStr]
		if !ok {
			return errors.Errorf("unknown aggregate %s", funcStr)
		}
		aggregations[i].Func = distsqlpb.AggregatorSpec_Func(funcIdx)
		aggregations[i].Distinct = fholder.isDistinct()
		if fholder.argRenderIdx != noRenderIdx {
			aggregations[i].ColIdx = []uint32{uint32(p.PlanToStreamColMap[fholder.argRenderIdx])}
		}
		if fholder.hasFilter() {
			col := uint32(p.PlanToStreamColMap[fholder.filterRenderIdx])
			aggregations[i].FilterColIdx = &col
		}
		aggregations[i].Arguments = make([]distsqlpb.Expression, len(fholder.arguments))
		aggregationsColumnTypes[i] = make([]types.T, len(fholder.arguments))
		for j, argument := range fholder.arguments {
			var err error
			aggregations[i].Arguments[j], err = distsqlplan.MakeExpression(argument, planCtx, nil)
			if err != nil {
				return err
			}
			aggregationsColumnTypes[i][j] = *argument.ResolvedType()
			if err != nil {
				return err
			}
		}
	}

	aggType := distsqlpb.AggregatorSpec_NON_SCALAR
	if n.isScalar {
		aggType = distsqlpb.AggregatorSpec_SCALAR
	}

	inputTypes := p.ResultTypes

	groupCols := make([]uint32, len(n.groupCols))
	for i, idx := range n.groupCols {
		groupCols[i] = uint32(p.PlanToStreamColMap[idx])
	}
	orderedGroupCols := make([]uint32, len(n.groupColOrdering))
	var orderedGroupColSet util.FastIntSet
	for i, c := range n.groupColOrdering {
		orderedGroupCols[i] = uint32(p.PlanToStreamColMap[c.ColIdx])
		orderedGroupColSet.Add(c.ColIdx)
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

	var finalAggsSpec distsqlpb.AggregatorSpec
	var finalAggsPost distsqlpb.PostProcessSpec

	if !multiStage && allDistinct {
		// We can't do local aggregation, but we can do local distinct processing
		// to reduce streaming duplicates, and aggregate on the final node.

		ordering := dsp.convertOrdering(planPhysicalProps(n.plan), p.PlanToStreamColMap).Columns
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

		distinctSpec := distsqlpb.ProcessorCoreUnion{
			Distinct: &distsqlpb.DistinctSpec{
				OrderedColumns:  orderedColumns,
				DistinctColumns: distinctColumns,
			},
		}

		// Add distinct processors local to each existing current result processor.
		p.AddNoGroupingStage(distinctSpec, distsqlpb.PostProcessSpec{}, p.ResultTypes, p.MergeOrdering)
	}

	// planToStreamMapSet keeps track of whether or not
	// p.PlanToStreamColMap has been set to its desired mapping or not.
	planToStreamMapSet := false
	if !multiStage {
		finalAggsSpec = distsqlpb.AggregatorSpec{
			Type:             aggType,
			Aggregations:     aggregations,
			GroupCols:        groupCols,
			OrderedGroupCols: orderedGroupCols,
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
		localAggs := make([]distsqlpb.AggregatorSpec_Aggregation, 0, nLocalAgg+len(groupCols))
		intermediateTypes := make([]types.T, 0, nLocalAgg+len(groupCols))
		finalAggs := make([]distsqlpb.AggregatorSpec_Aggregation, 0, nFinalAgg)
		// finalIdxMap maps the index i of the final aggregation (with
		// respect to the i-th final aggregation out of all final
		// aggregations) to its index in the finalAggs slice.
		finalIdxMap := make([]uint32, nFinalAgg)

		// finalPreRenderTypes is passed to an IndexVarHelper which
		// helps type-check the indexed variables passed into
		// FinalRendering for some aggregations.
		// This has a 1-1 mapping to finalAggs
		var finalPreRenderTypes []*types.T
		if needRender {
			finalPreRenderTypes = make([]*types.T, 0, nFinalAgg)
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
				localAgg := distsqlpb.AggregatorSpec_Aggregation{
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
					argTypes := make([]types.T, len(e.ColIdx))
					for j, c := range e.ColIdx {
						argTypes[j] = inputTypes[c]
					}
					_, outputType, err := distsqlrun.GetAggregateInfo(localFunc, argTypes...)
					if err != nil {
						return err
					}
					intermediateTypes = append(intermediateTypes, *outputType)
				}
			}

			for _, finalInfo := range info.FinalStage {
				// The input of the final aggregators is
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
				finalAgg := distsqlpb.AggregatorSpec_Aggregation{
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
						argTypes := make([]types.T, len(finalInfo.LocalIdxs))
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

		// In queries like SELECT min(v) FROM kv GROUP BY k, not all group columns
		// appear in the rendering. Add IDENT expressions for them, as they need to
		// be part of the output of the local stage for the final stage to know
		// about them.
		finalGroupCols := make([]uint32, len(groupCols))
		finalOrderedGroupCols := make([]uint32, 0, len(orderedGroupCols))
		for i, groupColIdx := range groupCols {
			agg := distsqlpb.AggregatorSpec_Aggregation{
				Func:   distsqlpb.AggregatorSpec_ANY_NOT_NULL,
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
			if orderedGroupColSet.Contains(n.groupCols[i]) {
				finalOrderedGroupCols = append(finalOrderedGroupCols, uint32(idx))
			}
		}

		// Create the merge ordering for the local stage (this will be maintained
		// for results going into the final stage).
		ordCols := make([]distsqlpb.Ordering_Column, len(n.groupColOrdering))
		for i, o := range n.groupColOrdering {
			// Find the group column.
			found := false
			for j, col := range n.groupCols {
				if col == o.ColIdx {
					ordCols[i].ColIdx = finalGroupCols[j]
					found = true
					break
				}
			}
			if !found {
				return errors.AssertionFailedf("group column ordering contains non-grouping column %d", o.ColIdx)
			}
			if o.Direction == encoding.Descending {
				ordCols[i].Direction = distsqlpb.Ordering_Column_DESC
			} else {
				ordCols[i].Direction = distsqlpb.Ordering_Column_ASC
			}
		}

		localAggsSpec := distsqlpb.AggregatorSpec{
			Type:             aggType,
			Aggregations:     localAggs,
			GroupCols:        groupCols,
			OrderedGroupCols: orderedGroupCols,
		}

		p.AddNoGroupingStage(
			distsqlpb.ProcessorCoreUnion{Aggregator: &localAggsSpec},
			distsqlpb.PostProcessSpec{},
			intermediateTypes,
			distsqlpb.Ordering{Columns: ordCols},
		)

		finalAggsSpec = distsqlpb.AggregatorSpec{
			Type:             aggType,
			Aggregations:     finalAggs,
			GroupCols:        finalGroupCols,
			OrderedGroupCols: finalOrderedGroupCols,
		}

		if needRender {
			// Build rendering expressions.
			renderExprs := make([]distsqlpb.Expression, len(aggregations))
			h := tree.MakeTypesOnlyIndexedVarHelper(finalPreRenderTypes)
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
					var err error
					renderExprs[i], err = distsqlplan.MakeExpression(
						h.IndexedVar(mappedIdx), planCtx, nil /* indexVarMap */)
					if err != nil {
						return err
					}
				} else {
					// We have multiple final aggregation
					// values that we need to be mapped to
					// their corresponding index in
					// finalAggs for FinalRendering.
					mappedIdxs := make([]int, len(info.FinalStage))
					for j := range info.FinalStage {
						mappedIdxs[j] = int(finalIdxMap[finalIdx+j])
					}
					// Map the final aggregation values
					// to their corresponding indices.
					expr, err := info.FinalRendering(&h, mappedIdxs)
					if err != nil {
						return err
					}
					renderExprs[i], err = distsqlplan.MakeExpression(
						expr, planCtx,
						nil /* indexVarMap */)
					if err != nil {
						return err
					}
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
			p.PlanToStreamColMap = p.PlanToStreamColMap[:0]
			for _, idx := range finalIdxMap {
				p.PlanToStreamColMap = append(p.PlanToStreamColMap, int(idx))
			}
			planToStreamMapSet = true
		}
	}

	// Set up the final stage.

	finalOutTypes := make([]types.T, len(aggregations))
	for i, agg := range aggregations {
		argTypes := make([]types.T, len(agg.ColIdx)+len(agg.Arguments))
		for j, c := range agg.ColIdx {
			argTypes[j] = inputTypes[c]
		}
		for j, argumentColumnType := range aggregationsColumnTypes[i] {
			argTypes[len(agg.ColIdx)+j] = argumentColumnType
		}
		var err error
		_, returnTyp, err := distsqlrun.GetAggregateInfo(agg.Func, argTypes...)
		if err != nil {
			return err
		}
		finalOutTypes[i] = *returnTyp
	}

	// Update p.PlanToStreamColMap; we will have a simple 1-to-1 mapping of
	// planNode columns to stream columns because the aggregator
	// has been programmed to produce the same columns as the groupNode.
	if !planToStreamMapSet {
		p.PlanToStreamColMap = identityMap(p.PlanToStreamColMap, len(aggregations))
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
			distsqlpb.ProcessorCoreUnion{Aggregator: &finalAggsSpec},
			finalAggsPost,
			finalOutTypes,
		)
	} else {
		// We distribute (by group columns) to multiple processors.

		// Set up the output routers from the previous stage.
		for _, resultProc := range p.ResultRouters {
			p.Processors[resultProc].Spec.Output[0] = distsqlpb.OutputRouterSpec{
				Type:        distsqlpb.OutputRouterSpec_BY_HASH,
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
				Spec: distsqlpb.ProcessorSpec{
					Input: []distsqlpb.InputSyncSpec{{
						// The other fields will be filled in by mergeResultStreams.
						ColumnTypes: p.ResultTypes,
					}},
					Core: distsqlpb.ProcessorCoreUnion{Aggregator: &finalAggsSpec},
					Post: finalAggsPost,
					Output: []distsqlpb.OutputRouterSpec{{
						Type: distsqlpb.OutputRouterSpec_PASS_THROUGH,
					}},
					StageID: stageID,
				},
			}
			p.AddProcessor(proc)
		}

		// Connect the streams.
		for bucket := 0; bucket < len(p.ResultRouters); bucket++ {
			pIdx := pIdxStart + distsqlplan.ProcessorIdx(bucket)
			p.MergeResultStreams(p.ResultRouters, bucket, p.MergeOrdering, pIdx, 0)
		}

		// Set the new result routers.
		for i := 0; i < len(p.ResultRouters); i++ {
			p.ResultRouters[i] = pIdxStart + distsqlplan.ProcessorIdx(i)
		}

		p.ResultTypes = finalOutTypes
		p.SetMergeOrdering(dsp.convertOrdering(n.props, p.PlanToStreamColMap))
	}

	return nil
}

func (dsp *DistSQLPlanner) createPlanForIndexJoin(
	planCtx *PlanningCtx, n *indexJoinNode,
) (PhysicalPlan, error) {
	plan, err := dsp.createTableReaders(planCtx, n.index, n.index.desc.PrimaryIndex.ColumnIDs)
	if err != nil {
		return PhysicalPlan{}, err
	}

	joinReaderSpec := distsqlpb.JoinReaderSpec{
		Table:      *n.index.desc.TableDesc(),
		IndexIdx:   0,
		Visibility: n.table.colCfg.visibility.toDistSQLScanVisibility(),
	}

	filter, err := distsqlplan.MakeExpression(
		n.table.filter, planCtx, nil /* indexVarMap */)
	if err != nil {
		return PhysicalPlan{}, err
	}
	post := distsqlpb.PostProcessSpec{
		Filter:     filter,
		Projection: true,
	}

	// Calculate the output columns from n.cols.
	post.OutputColumns = make([]uint32, 0, len(n.cols))
	plan.PlanToStreamColMap = makePlanToStreamColMap(len(n.cols))

	for i := range n.cols {
		if !n.resultColumns[i].Omitted {
			plan.PlanToStreamColMap[i] = len(post.OutputColumns)
			ord := tableOrdinal(n.table.desc, n.cols[i].ID, n.table.colCfg.visibility)
			post.OutputColumns = append(post.OutputColumns, uint32(ord))
		}
	}

	types, err := getTypesForPlanResult(n, plan.PlanToStreamColMap)
	if err != nil {
		return PhysicalPlan{}, err
	}
	if distributeIndexJoin.Get(&dsp.st.SV) && len(plan.ResultRouters) > 1 {
		// Instantiate one join reader for every stream.
		plan.AddNoGroupingStage(
			distsqlpb.ProcessorCoreUnion{JoinReader: &joinReaderSpec},
			post,
			types,
			dsp.convertOrdering(planPhysicalProps(n), plan.PlanToStreamColMap),
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
			distsqlpb.ProcessorCoreUnion{JoinReader: &joinReaderSpec},
			post,
			types,
		)
	}
	return plan, nil
}

// createPlanForLookupJoin creates a distributed plan for a lookupJoinNode.
// Note that this is a separate code path from the experimental path which
// converts joins to lookup joins.
func (dsp *DistSQLPlanner) createPlanForLookupJoin(
	planCtx *PlanningCtx, n *lookupJoinNode,
) (PhysicalPlan, error) {
	plan, err := dsp.createPlanForNode(planCtx, n.input)
	if err != nil {
		return PhysicalPlan{}, err
	}

	joinReaderSpec := distsqlpb.JoinReaderSpec{
		Table:      *n.table.desc.TableDesc(),
		Type:       n.joinType,
		Visibility: n.table.colCfg.visibility.toDistSQLScanVisibility(),
	}
	joinReaderSpec.IndexIdx, err = getIndexIdx(n.table)
	if err != nil {
		return PhysicalPlan{}, err
	}
	joinReaderSpec.LookupColumns = make([]uint32, len(n.eqCols))
	for i, col := range n.eqCols {
		if plan.PlanToStreamColMap[col] == -1 {
			panic("lookup column not in planToStreamColMap")
		}
		joinReaderSpec.LookupColumns[i] = uint32(plan.PlanToStreamColMap[col])
	}
	joinReaderSpec.LookupColumnsAreKey = n.eqColsAreKey

	// The n.table node can be configured with an arbitrary set of columns. Apply
	// the corresponding projection.
	// The internal schema of the join reader is:
	//    <input columns>... <table columns>...
	numLeftCols := len(plan.ResultTypes)
	numOutCols := numLeftCols + len(n.table.cols)
	post := distsqlpb.PostProcessSpec{Projection: true}

	post.OutputColumns = make([]uint32, numOutCols)
	types := make([]types.T, numOutCols)

	for i := 0; i < numLeftCols; i++ {
		types[i] = plan.ResultTypes[i]
		post.OutputColumns[i] = uint32(i)
	}
	for i := range n.table.cols {
		types[numLeftCols+i] = n.table.cols[i].Type
		ord := tableOrdinal(n.table.desc, n.table.cols[i].ID, n.table.colCfg.visibility)
		post.OutputColumns[numLeftCols+i] = uint32(numLeftCols + ord)
	}

	// Map the columns of the lookupJoinNode to the result streams of the
	// JoinReader.
	numInputNodeCols := len(planColumns(n.input))
	planToStreamColMap := makePlanToStreamColMap(numInputNodeCols + len(n.table.cols))
	copy(planToStreamColMap, plan.PlanToStreamColMap)
	for i := range n.table.cols {
		planToStreamColMap[numInputNodeCols+i] = numLeftCols + i
	}

	// Set the ON condition.
	if n.onCond != nil {
		// Note that (regardless of the join type or the OutputColumns projection)
		// the ON condition refers to the input columns with var indexes 0 to
		// numInputNodeCols-1 and to table columns with var indexes starting from
		// numInputNodeCols.
		indexVarMap := makePlanToStreamColMap(numInputNodeCols + len(n.table.cols))
		copy(indexVarMap, plan.PlanToStreamColMap)
		for i := range n.table.cols {
			indexVarMap[numInputNodeCols+i] = int(post.OutputColumns[numLeftCols+i])
		}
		var err error
		joinReaderSpec.OnExpr, err = distsqlplan.MakeExpression(
			n.onCond, planCtx, indexVarMap,
		)
		if err != nil {
			return PhysicalPlan{}, err
		}
	}

	if n.joinType == sqlbase.LeftSemiJoin || n.joinType == sqlbase.LeftAntiJoin {
		// For anti/semi join, we only produce the input columns.
		planToStreamColMap = planToStreamColMap[:numInputNodeCols]
		post.OutputColumns = post.OutputColumns[:numInputNodeCols]
	}

	// Instantiate one join reader for every stream.
	plan.AddNoGroupingStage(
		distsqlpb.ProcessorCoreUnion{JoinReader: &joinReaderSpec},
		post,
		types,
		dsp.convertOrdering(planPhysicalProps(n), planToStreamColMap),
	)
	plan.PlanToStreamColMap = planToStreamColMap
	return plan, nil
}

// createPlanForZigzagJoin creates a distributed plan for a zigzagJoinNode.
func (dsp *DistSQLPlanner) createPlanForZigzagJoin(
	planCtx *PlanningCtx, n *zigzagJoinNode,
) (plan PhysicalPlan, err error) {

	tables := make([]sqlbase.TableDescriptor, len(n.sides))
	indexIds := make([]uint32, len(n.sides))
	cols := make([]distsqlpb.Columns, len(n.sides))
	numStreamCols := 0
	for i, side := range n.sides {
		tables[i] = *side.scan.desc.TableDesc()
		indexIds[i], err = getIndexIdx(side.scan)
		if err != nil {
			return PhysicalPlan{}, err
		}

		cols[i].Columns = make([]uint32, len(side.eqCols))
		for j, col := range side.eqCols {
			cols[i].Columns[j] = uint32(col)
		}

		numStreamCols += len(side.scan.desc.Columns)
	}

	// The zigzag join node only represents inner joins, so hardcode Type to
	// InnerJoin.
	zigzagJoinerSpec := distsqlpb.ZigzagJoinerSpec{
		Tables:    tables,
		IndexIds:  indexIds,
		EqColumns: cols,
		Type:      sqlbase.InnerJoin,
	}
	zigzagJoinerSpec.FixedValues = make([]*distsqlpb.ValuesCoreSpec, len(n.sides))

	// The fixed values are represented as a Values node with one tuple.
	for i := range n.sides {
		valuesPlan, err := dsp.createPlanForValues(planCtx, n.sides[i].fixedVals)
		if err != nil {
			return PhysicalPlan{}, err
		}
		zigzagJoinerSpec.FixedValues[i] = valuesPlan.PhysicalPlan.Processors[0].Spec.Core.Values
	}

	// The internal schema of the zigzag joiner is:
	//    <side 1 table columns> ... <side 2 table columns> ...
	// with only the columns in the specified index populated.
	//
	// The schema of the zigzagJoinNode is:
	//    <side 1 index columns> ... <side 2 index columns> ...
	// so the planToStreamColMap has to basically map index ordinals
	// to table ordinals.
	post := distsqlpb.PostProcessSpec{Projection: true}
	numOutCols := len(n.columns)

	post.OutputColumns = make([]uint32, numOutCols)
	types := make([]types.T, numOutCols)
	planToStreamColMap := makePlanToStreamColMap(numOutCols)
	colOffset := 0
	i := 0

	// Populate post.OutputColumns (the implicit projection), result types,
	// and the planToStreamColMap for index columns from all sides.
	for _, side := range n.sides {
		// Note that the side's scanNode only contains the columns from that
		// index that are also in n.columns. This is because we generated
		// colCfg.wantedColumns for only the necessary columns in
		// opt/exec/execbuilder/relational_builder.go, similar to lookup joins.
		for colIdx := range side.scan.cols {
			ord := tableOrdinal(side.scan.desc, side.scan.cols[colIdx].ID, side.scan.colCfg.visibility)
			post.OutputColumns[i] = uint32(colOffset + ord)
			types[i] = side.scan.cols[colIdx].Type
			planToStreamColMap[i] = i

			i++
		}

		colOffset += len(side.scan.desc.Columns)
	}

	// Figure out the node where this zigzag joiner goes.
	//
	// TODO(itsbilal): Add support for restricting the Zigzag joiner
	// to a certain set of spans (similar to the InterleavedReaderJoiner)
	// on one side. Once that's done, we can split this processor across
	// multiple nodes here. Until then, schedule on the current node.
	nodeID := dsp.nodeDesc.NodeID

	stageID := plan.NewStageID()
	// Set the ON condition.
	if n.onCond != nil {
		// Note that the ON condition refers to the *internal* columns of the
		// processor (before the OutputColumns projection).
		indexVarMap := makePlanToStreamColMap(len(n.columns))
		for i := range n.columns {
			indexVarMap[i] = int(post.OutputColumns[i])
		}
		zigzagJoinerSpec.OnExpr, err = distsqlplan.MakeExpression(
			n.onCond, planCtx, indexVarMap,
		)
		if err != nil {
			return PhysicalPlan{}, err
		}
	}

	// Build the PhysicalPlan.
	proc := distsqlplan.Processor{
		Node: nodeID,
		Spec: distsqlpb.ProcessorSpec{
			Core:    distsqlpb.ProcessorCoreUnion{ZigzagJoiner: &zigzagJoinerSpec},
			Post:    post,
			Output:  []distsqlpb.OutputRouterSpec{{Type: distsqlpb.OutputRouterSpec_PASS_THROUGH}},
			StageID: stageID,
		},
	}

	plan.Processors = append(plan.Processors, proc)

	// Each result router correspond to each of the processors we appended.
	plan.ResultRouters = []distsqlplan.ProcessorIdx{distsqlplan.ProcessorIdx(0)}

	plan.PlanToStreamColMap = planToStreamColMap
	plan.ResultTypes = types

	return plan, nil
}

// getTypesForPlanResult returns the types of the elements in the result streams
// of a plan that corresponds to a given planNode. If planToStreamColMap is nil,
// a 1-1 mapping is assumed.
func getTypesForPlanResult(node planNode, planToStreamColMap []int) ([]types.T, error) {
	nodeColumns := planColumns(node)
	if planToStreamColMap == nil {
		// No remapping.
		types := make([]types.T, len(nodeColumns))
		for i := range nodeColumns {
			types[i] = *nodeColumns[i].Typ
		}
		return types, nil
	}
	numCols := 0
	for _, streamCol := range planToStreamColMap {
		if numCols <= streamCol {
			numCols = streamCol + 1
		}
	}
	types := make([]types.T, numCols)
	for nodeCol, streamCol := range planToStreamColMap {
		if streamCol != -1 {
			types[streamCol] = *nodeColumns[nodeCol].Typ
		}
	}
	return types, nil
}

func (dsp *DistSQLPlanner) createPlanForJoin(
	planCtx *PlanningCtx, n *joinNode,
) (PhysicalPlan, error) {
	// See if we can create an interleave join plan.
	if planInterleavedJoins.Get(&dsp.st.SV) {
		plan, ok, err := dsp.tryCreatePlanForInterleavedJoin(planCtx, n)
		if err != nil {
			return PhysicalPlan{}, err
		}
		// An interleave join plan could be used. Return it.
		if ok {
			return plan, nil
		}
	}

	// Outline of the planning process for joins:
	//
	//  - We create PhysicalPlans for the left and right side. Each plan has a set
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
		return PhysicalPlan{}, err
	}
	rightPlan, err := dsp.createPlanForNode(planCtx, n.right.plan)
	if err != nil {
		return PhysicalPlan{}, err
	}

	// Nodes where we will run the join processors.
	var nodes []roachpb.NodeID

	// We initialize these properties of the joiner. They will then be used to
	// fill in the processor spec. See descriptions for HashJoinerSpec.
	var leftEqCols, rightEqCols []uint32
	var leftMergeOrd, rightMergeOrd distsqlpb.Ordering
	joinType := n.joinType

	// Figure out the left and right types.
	leftTypes := leftPlan.ResultTypes
	rightTypes := rightPlan.ResultTypes

	// Set up the equality columns.
	if numEq := len(n.pred.leftEqualityIndices); numEq != 0 {
		leftEqCols = eqCols(n.pred.leftEqualityIndices, leftPlan.PlanToStreamColMap)
		rightEqCols = eqCols(n.pred.rightEqualityIndices, rightPlan.PlanToStreamColMap)
	}

	var p PhysicalPlan
	var leftRouters, rightRouters []distsqlplan.ProcessorIdx
	p.PhysicalPlan, leftRouters, rightRouters = distsqlplan.MergePlans(
		&leftPlan.PhysicalPlan, &rightPlan.PhysicalPlan,
	)

	// Set up the output columns.
	if numEq := len(n.pred.leftEqualityIndices); numEq != 0 {
		nodes = findJoinProcessorNodes(leftRouters, rightRouters, p.Processors)

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

	rightMap := rightPlan.PlanToStreamColMap
	post, joinToStreamColMap := joinOutColumns(n, leftPlan.PlanToStreamColMap, rightMap)
	onExpr, err := remapOnExpr(planCtx, n, leftPlan.PlanToStreamColMap, rightMap)
	if err != nil {
		return PhysicalPlan{}, err
	}

	// Create the Core spec.
	var core distsqlpb.ProcessorCoreUnion
	if leftMergeOrd.Columns == nil {
		core.HashJoiner = &distsqlpb.HashJoinerSpec{
			LeftEqColumns:        leftEqCols,
			RightEqColumns:       rightEqCols,
			OnExpr:               onExpr,
			Type:                 joinType,
			LeftEqColumnsAreKey:  n.pred.leftEqKey,
			RightEqColumnsAreKey: n.pred.rightEqKey,
		}
	} else {
		core.MergeJoiner = &distsqlpb.MergeJoinerSpec{
			LeftOrdering:  leftMergeOrd,
			RightOrdering: rightMergeOrd,
			OnExpr:        onExpr,
			Type:          joinType,
		}
	}

	p.AddJoinStage(
		nodes, core, post, leftEqCols, rightEqCols, leftTypes, rightTypes,
		leftMergeOrd, rightMergeOrd, leftRouters, rightRouters,
	)

	p.PlanToStreamColMap = joinToStreamColMap
	p.ResultTypes, err = getTypesForPlanResult(n, joinToStreamColMap)
	if err != nil {
		return PhysicalPlan{}, err
	}

	// Joiners may guarantee an ordering to outputs, so we ensure that
	// ordering is propagated through the input synchronizer of the next stage.
	// We can propagate the ordering from either side, we use the left side here.
	// Note that n.props only has a non-empty ordering for inner joins, where it
	// uses the mergeJoinOrdering.
	p.SetMergeOrdering(dsp.convertOrdering(n.props, p.PlanToStreamColMap))
	return p, nil
}

func (dsp *DistSQLPlanner) createPlanForNode(
	planCtx *PlanningCtx, node planNode,
) (plan PhysicalPlan, err error) {
	planCtx.planDepth++

	switch n := node.(type) {
	case *scanNode:
		plan, err = dsp.createTableReaders(planCtx, n, nil)

	case *indexJoinNode:
		plan, err = dsp.createPlanForIndexJoin(planCtx, n)

	case *lookupJoinNode:
		plan, err = dsp.createPlanForLookupJoin(planCtx, n)

	case *zigzagJoinNode:
		plan, err = dsp.createPlanForZigzagJoin(planCtx, n)

	case *joinNode:
		plan, err = dsp.createPlanForJoin(planCtx, n)

	case *renderNode:
		plan, err = dsp.createPlanForNode(planCtx, n.source.plan)
		if err != nil {
			return PhysicalPlan{}, err
		}
		err = dsp.selectRenders(&plan, n, planCtx)
		if err != nil {
			return PhysicalPlan{}, err
		}

	case *groupNode:
		plan, err = dsp.createPlanForNode(planCtx, n.plan)
		if err != nil {
			return PhysicalPlan{}, err
		}

		if err := dsp.addAggregators(planCtx, &plan, n); err != nil {
			return PhysicalPlan{}, err
		}

	case *sortNode:
		plan, err = dsp.createPlanForNode(planCtx, n.plan)
		if err != nil {
			return PhysicalPlan{}, err
		}

		dsp.addSorters(&plan, n)

	case *filterNode:
		plan, err = dsp.createPlanForNode(planCtx, n.source.plan)
		if err != nil {
			return PhysicalPlan{}, err
		}

		if err := plan.AddFilter(n.filter, planCtx, plan.PlanToStreamColMap); err != nil {
			return PhysicalPlan{}, err
		}

	case *limitNode:
		plan, err = dsp.createPlanForNode(planCtx, n.plan)
		if err != nil {
			return PhysicalPlan{}, err
		}
		if err := n.evalLimit(planCtx.EvalContext()); err != nil {
			return PhysicalPlan{}, err
		}
		if err := plan.AddLimit(n.count, n.offset, planCtx, dsp.nodeDesc.NodeID); err != nil {
			return PhysicalPlan{}, err
		}

	case *distinctNode:
		plan, err = dsp.createPlanForDistinct(planCtx, n)

	case *ordinalityNode:
		plan, err = dsp.createPlanForOrdinality(planCtx, n)

	case *unionNode:
		plan, err = dsp.createPlanForSetOp(planCtx, n)

	case *valuesNode:
		// Just like in checkSupportForNode, if a valuesNode wasn't specified in
		// the query, it means that it was autogenerated for things that we don't
		// want to be distributing, like populating values from a virtual table. So,
		// we wrap the plan instead.
		//
		// If the plan is local, we also wrap the plan to avoid pointless
		// serialization of the values, and also to avoid situations in which
		// expressions within the valuesNode were not distributable in the first
		// place.
		//
		// Finally, if noEvalSubqueries is set, it means that nothing has replaced
		// the subqueries with their results yet, which again means that we can't
		// plan a DistSQL values node, which requires that all expressions be
		// evaluatable.
		//
		// NB: If you change this conditional, you must also change it in
		// checkSupportForNode!
		if !n.specifiedInQuery || planCtx.isLocal || planCtx.noEvalSubqueries {
			plan, err = dsp.wrapPlan(planCtx, n)
		} else {
			plan, err = dsp.createPlanForValues(planCtx, n)
		}

	case *projectSetNode:
		plan, err = dsp.createPlanForProjectSet(planCtx, n)

	case *unaryNode:
		plan, err = dsp.createPlanForUnary(planCtx, n)

	case *zeroNode:
		plan, err = dsp.createPlanForZero(planCtx, n)

	case *windowNode:
		plan, err = dsp.createPlanForWindow(planCtx, n)

	default:
		// Can't handle a node? We wrap it and continue on our way.
		plan, err = dsp.wrapPlan(planCtx, n)
	}

	if err != nil {
		return plan, err
	}

	if dsp.shouldPlanTestMetadata() {
		if err := plan.CheckLastStagePost(); err != nil {
			log.Fatal(planCtx.ctx, err)
		}
		plan.AddNoGroupingStageWithCoreFunc(
			func(_ int, _ *distsqlplan.Processor) distsqlpb.ProcessorCoreUnion {
				return distsqlpb.ProcessorCoreUnion{
					MetadataTestSender: &distsqlpb.MetadataTestSenderSpec{
						ID: uuid.MakeV4().String(),
					},
				}
			},
			distsqlpb.PostProcessSpec{},
			plan.ResultTypes,
			plan.MergeOrdering,
		)
	}

	return plan, err
}

// wrapPlan produces a DistSQL processor for an arbitrary planNode. This is
// invoked when a particular planNode can't be distributed for some reason. It
// will create a planNodeToRowSource wrapper for the sub-tree that's not
// plannable by DistSQL. If that sub-tree has DistSQL-plannable sources, they
// will be planned by DistSQL and connected to the wrapper.
func (dsp *DistSQLPlanner) wrapPlan(planCtx *PlanningCtx, n planNode) (PhysicalPlan, error) {
	useFastPath := planCtx.planDepth == 1 && planCtx.stmtType == tree.RowsAffected

	// First, we search the planNode tree we're trying to wrap for the first
	// DistSQL-enabled planNode in the tree. If we find one, we ask the planner to
	// continue the DistSQL planning recursion on that planNode.
	seenTop := false
	nParents := uint32(0)
	var p PhysicalPlan
	// This will be set to first DistSQL-enabled planNode we find, if any. We'll
	// modify its parent later to connect its source to the DistSQL-planned
	// subtree.
	var firstNotWrapped planNode
	if err := walkPlan(planCtx.ctx, n, planObserver{
		enterNode: func(ctx context.Context, nodeName string, plan planNode) (bool, error) {
			switch plan.(type) {
			case *explainDistSQLNode, *explainPlanNode:
				// Don't continue recursing into explain nodes - they need to be left
				// alone since they handle their own planning later.
				return false, nil
			}
			if !seenTop {
				// We know we're wrapping the first node, so ignore it.
				seenTop = true
				return true, nil
			}
			var err error
			// Continue walking until we find a node that has a DistSQL
			// representation - that's when we'll quit the wrapping process and hand
			// control of planning back to the DistSQL physical planner.
			if !dsp.mustWrapNode(planCtx, plan) {
				firstNotWrapped = plan
				p, err = dsp.createPlanForNode(planCtx, plan)
				if err != nil {
					return false, err
				}
				nParents++
				return false, nil
			}
			return true, nil
		},
	}); err != nil {
		return PhysicalPlan{}, err
	}
	if nParents > 1 {
		return PhysicalPlan{}, errors.Errorf("can't wrap plan %v %T with more than one input", n, n)
	}

	// Copy the evalCtx.
	evalCtx := *planCtx.ExtendedEvalCtx
	// We permit the planNodeToRowSource to trigger the wrapped planNode's fast
	// path if its the very first node in the flow, and if the statement type we're
	// expecting is in fact RowsAffected. RowsAffected statements return a single
	// row with the number of rows affected by the statement, and are the only
	// types of statement where it's valid to invoke a plan's fast path.
	wrapper, err := makePlanNodeToRowSource(n,
		runParams{
			extendedEvalCtx: &evalCtx,
			p:               planCtx.planner,
		},
		useFastPath,
	)
	if err != nil {
		return PhysicalPlan{}, err
	}
	wrapper.firstNotWrapped = firstNotWrapped

	idx := uint32(len(p.LocalProcessors))
	p.LocalProcessors = append(p.LocalProcessors, wrapper)
	p.LocalProcessorIndexes = append(p.LocalProcessorIndexes, &idx)
	var input []distsqlpb.InputSyncSpec
	if firstNotWrapped != nil {
		// We found a DistSQL-plannable subtree - create an input spec for it.
		input = []distsqlpb.InputSyncSpec{{
			Type:        distsqlpb.InputSyncSpec_UNORDERED,
			ColumnTypes: p.ResultTypes,
		}}
	}
	name := nodeName(n)
	proc := distsqlplan.Processor{
		Node: dsp.nodeDesc.NodeID,
		Spec: distsqlpb.ProcessorSpec{
			Input: input,
			Core: distsqlpb.ProcessorCoreUnion{LocalPlanNode: &distsqlpb.LocalPlanNodeSpec{
				RowSourceIdx: &idx,
				NumInputs:    &nParents,
				Name:         &name,
			}},
			Post: distsqlpb.PostProcessSpec{},
			Output: []distsqlpb.OutputRouterSpec{{
				Type: distsqlpb.OutputRouterSpec_PASS_THROUGH,
			}},
			StageID: p.NewStageID(),
		},
	}
	pIdx := p.AddProcessor(proc)
	p.ResultTypes = wrapper.outputTypes
	p.PlanToStreamColMap = identityMapInPlace(make([]int, len(p.ResultTypes)))
	if firstNotWrapped != nil {
		// If we found a DistSQL-plannable subtree, we need to add a result stream
		// between it and the physicalPlan we're creating here.
		p.MergeResultStreams(p.ResultRouters, 0, p.MergeOrdering, pIdx, 0)
	}
	// ResultRouters gets overwritten each time we add a new PhysicalPlan. We will
	// just have a single result router, since local processors aren't
	// distributed, so make sure that p.ResultRouters has at least 1 slot and
	// write the new processor index there.
	if cap(p.ResultRouters) < 1 {
		p.ResultRouters = make([]distsqlplan.ProcessorIdx, 1)
	} else {
		p.ResultRouters = p.ResultRouters[:1]
	}
	p.ResultRouters[0] = pIdx
	return p, nil
}

// createValuesPlan creates a plan with a single Values processor
// located on the gateway node and initialized with given numRows
// and rawBytes that need to be precomputed beforehand.
func (dsp *DistSQLPlanner) createValuesPlan(
	resultTypes []types.T, numRows int, rawBytes [][]byte,
) (PhysicalPlan, error) {
	numColumns := len(resultTypes)
	s := distsqlpb.ValuesCoreSpec{
		Columns: make([]distsqlpb.DatumInfo, numColumns),
	}

	for i, t := range resultTypes {
		s.Columns[i].Encoding = sqlbase.DatumEncoding_VALUE
		s.Columns[i].Type = t
	}

	s.NumRows = uint64(numRows)
	s.RawBytes = rawBytes

	plan := distsqlplan.PhysicalPlan{
		Processors: []distsqlplan.Processor{{
			// TODO: find a better node to place processor at
			Node: dsp.nodeDesc.NodeID,
			Spec: distsqlpb.ProcessorSpec{
				Core:   distsqlpb.ProcessorCoreUnion{Values: &s},
				Output: []distsqlpb.OutputRouterSpec{{Type: distsqlpb.OutputRouterSpec_PASS_THROUGH}},
			},
		}},
		ResultRouters: []distsqlplan.ProcessorIdx{0},
		ResultTypes:   resultTypes,
	}

	return PhysicalPlan{
		PhysicalPlan:       plan,
		PlanToStreamColMap: identityMapInPlace(make([]int, numColumns)),
	}, nil
}

func (dsp *DistSQLPlanner) createPlanForValues(
	planCtx *PlanningCtx, n *valuesNode,
) (PhysicalPlan, error) {
	params := runParams{
		ctx:             planCtx.ctx,
		extendedEvalCtx: planCtx.ExtendedEvalCtx,
	}

	types, err := getTypesForPlanResult(n, nil /* planToStreamColMap */)
	if err != nil {
		return PhysicalPlan{}, err
	}

	if err := n.startExec(params); err != nil {
		return PhysicalPlan{}, err
	}
	defer n.Close(planCtx.ctx)

	var a sqlbase.DatumAlloc

	numRows := n.rows.Len()
	rawBytes := make([][]byte, numRows)
	for i := 0; i < numRows; i++ {
		if next, err := n.Next(runParams{ctx: planCtx.ctx}); !next {
			return PhysicalPlan{}, err
		}

		var buf []byte
		datums := n.Values()
		for j := range n.columns {
			var err error
			datum := sqlbase.DatumToEncDatum(&types[j], datums[j])
			buf, err = datum.Encode(&types[j], &a, sqlbase.DatumEncoding_VALUE, buf)
			if err != nil {
				return PhysicalPlan{}, err
			}
		}
		rawBytes[i] = buf
	}

	return dsp.createValuesPlan(types, numRows, rawBytes)
}

func (dsp *DistSQLPlanner) createPlanForUnary(
	planCtx *PlanningCtx, n *unaryNode,
) (PhysicalPlan, error) {
	types, err := getTypesForPlanResult(n, nil /* planToStreamColMap */)
	if err != nil {
		return PhysicalPlan{}, err
	}

	return dsp.createValuesPlan(types, 1 /* numRows */, nil /* rawBytes */)
}

func (dsp *DistSQLPlanner) createPlanForZero(
	planCtx *PlanningCtx, n *zeroNode,
) (PhysicalPlan, error) {
	types, err := getTypesForPlanResult(n, nil /* planToStreamColMap */)
	if err != nil {
		return PhysicalPlan{}, err
	}

	return dsp.createValuesPlan(types, 0 /* numRows */, nil /* rawBytes */)
}

func createDistinctSpec(n *distinctNode, cols []int) *distsqlpb.DistinctSpec {
	var orderedColumns []uint32
	if !n.columnsInOrder.Empty() {
		orderedColumns = make([]uint32, 0, n.columnsInOrder.Len())
		for i, ok := n.columnsInOrder.Next(0); ok; i, ok = n.columnsInOrder.Next(i + 1) {
			orderedColumns = append(orderedColumns, uint32(cols[i]))
		}
	}

	var distinctColumns []uint32
	if !n.distinctOnColIdxs.Empty() {
		for planCol, streamCol := range cols {
			if streamCol != -1 && n.distinctOnColIdxs.Contains(planCol) {
				distinctColumns = append(distinctColumns, uint32(streamCol))
			}
		}
	} else {
		// If no distinct columns were specified, run distinct on the entire row.
		for planCol := range planColumns(n) {
			if streamCol := cols[planCol]; streamCol != -1 {
				distinctColumns = append(distinctColumns, uint32(streamCol))
			}
		}
	}

	return &distsqlpb.DistinctSpec{
		OrderedColumns:  orderedColumns,
		DistinctColumns: distinctColumns,
	}
}

func (dsp *DistSQLPlanner) createPlanForDistinct(
	planCtx *PlanningCtx, n *distinctNode,
) (PhysicalPlan, error) {
	plan, err := dsp.createPlanForNode(planCtx, n.plan)
	if err != nil {
		return PhysicalPlan{}, err
	}
	currentResultRouters := plan.ResultRouters

	distinctSpec := distsqlpb.ProcessorCoreUnion{
		Distinct: createDistinctSpec(n, plan.PlanToStreamColMap),
	}

	if len(currentResultRouters) == 1 {
		plan.AddNoGroupingStage(distinctSpec, distsqlpb.PostProcessSpec{}, plan.ResultTypes, plan.MergeOrdering)
		return plan, nil
	}

	// TODO(arjun): This is potentially memory inefficient if we don't have any sorted columns.

	// Add distinct processors local to each existing current result processor.
	plan.AddNoGroupingStage(distinctSpec, distsqlpb.PostProcessSpec{}, plan.ResultTypes, plan.MergeOrdering)

	// TODO(arjun): We could distribute this final stage by hash.
	plan.AddSingleGroupStage(dsp.nodeDesc.NodeID, distinctSpec, distsqlpb.PostProcessSpec{}, plan.ResultTypes)

	return plan, nil
}

func (dsp *DistSQLPlanner) createPlanForOrdinality(
	planCtx *PlanningCtx, n *ordinalityNode,
) (PhysicalPlan, error) {
	plan, err := dsp.createPlanForNode(planCtx, n.source)
	if err != nil {
		return PhysicalPlan{}, err
	}

	ordinalitySpec := distsqlpb.ProcessorCoreUnion{
		Ordinality: &distsqlpb.OrdinalitySpec{},
	}

	plan.PlanToStreamColMap = append(plan.PlanToStreamColMap, len(plan.ResultTypes))
	outputTypes := append(plan.ResultTypes, *types.Int)

	// WITH ORDINALITY never gets distributed so that the gateway node can
	// always number each row in order.
	plan.AddSingleGroupStage(dsp.nodeDesc.NodeID, ordinalitySpec, distsqlpb.PostProcessSpec{}, outputTypes)

	return plan, nil
}

func createProjectSetSpec(
	planCtx *PlanningCtx, n *projectSetNode, indexVarMap []int,
) (*distsqlpb.ProjectSetSpec, error) {
	spec := distsqlpb.ProjectSetSpec{
		Exprs:            make([]distsqlpb.Expression, len(n.exprs)),
		GeneratedColumns: make([]types.T, len(n.columns)-n.numColsInSource),
		NumColsPerGen:    make([]uint32, len(n.exprs)),
	}
	for i, expr := range n.exprs {
		var err error
		spec.Exprs[i], err = distsqlplan.MakeExpression(expr, planCtx, indexVarMap)
		if err != nil {
			return nil, err
		}
	}
	for i, col := range n.columns[n.numColsInSource:] {
		spec.GeneratedColumns[i] = *col.Typ
	}
	for i, n := range n.numColsPerGen {
		spec.NumColsPerGen[i] = uint32(n)
	}
	return &spec, nil
}

func (dsp *DistSQLPlanner) createPlanForProjectSet(
	planCtx *PlanningCtx, n *projectSetNode,
) (PhysicalPlan, error) {
	plan, err := dsp.createPlanForNode(planCtx, n.source)
	if err != nil {
		return PhysicalPlan{}, err
	}
	numResults := len(plan.ResultTypes)

	indexVarMap := makePlanToStreamColMap(len(n.columns))
	copy(indexVarMap, plan.PlanToStreamColMap)

	// Create the project set processor spec.
	projectSetSpec, err := createProjectSetSpec(planCtx, n, indexVarMap)
	if err != nil {
		return PhysicalPlan{}, err
	}
	spec := distsqlpb.ProcessorCoreUnion{
		ProjectSet: projectSetSpec,
	}

	// Since ProjectSet tends to be a late stage which produces more rows than its
	// source, we opt to perform it only on the gateway node. If we encounter
	// cases in the future where this is non-optimal (perhaps if its output is
	// filtered), we could try to detect these cases and use AddNoGroupingStage
	// instead.
	outputTypes := append(plan.ResultTypes, projectSetSpec.GeneratedColumns...)
	plan.AddSingleGroupStage(dsp.nodeDesc.NodeID, spec, distsqlpb.PostProcessSpec{}, outputTypes)

	// Add generated columns to PlanToStreamColMap.
	for i := range projectSetSpec.GeneratedColumns {
		plan.PlanToStreamColMap = append(plan.PlanToStreamColMap, numResults+i)
	}

	return plan, nil
}

// isOnlyOnGateway returns true if a physical plan is executed entirely on the
// gateway node.
func (dsp *DistSQLPlanner) isOnlyOnGateway(plan *PhysicalPlan) bool {
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
	planCtx *PlanningCtx, n *unionNode,
) (PhysicalPlan, error) {
	leftLogicalPlan := n.left
	leftPlan, err := dsp.createPlanForNode(planCtx, n.left)
	if err != nil {
		return PhysicalPlan{}, err
	}
	rightLogicalPlan := n.right
	rightPlan, err := dsp.createPlanForNode(planCtx, n.right)
	if err != nil {
		return PhysicalPlan{}, err
	}
	if n.inverted {
		leftPlan, rightPlan = rightPlan, leftPlan
		leftLogicalPlan, rightLogicalPlan = rightLogicalPlan, leftLogicalPlan
	}
	childPhysicalPlans := []*PhysicalPlan{&leftPlan, &rightPlan}
	childLogicalPlans := []planNode{leftLogicalPlan, rightLogicalPlan}

	// Check that the left and right side PlanToStreamColMaps are equivalent.
	// TODO(solon): Are there any valid UNION/INTERSECT/EXCEPT cases where these
	// differ? If we encounter any, we could handle them by adding a projection on
	// the unioned columns on each side, similar to how we handle mismatched
	// ResultTypes.
	if !reflect.DeepEqual(leftPlan.PlanToStreamColMap, rightPlan.PlanToStreamColMap) {
		return PhysicalPlan{}, errors.Errorf(
			"planToStreamColMap mismatch: %v, %v", leftPlan.PlanToStreamColMap,
			rightPlan.PlanToStreamColMap)
	}
	planToStreamColMap := leftPlan.PlanToStreamColMap
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
	var distinctSpecs [2]distsqlpb.ProcessorCoreUnion

	if !n.all {
		leftProps = leftProps.project(planCols)
		rightProps = rightProps.project(planCols)

		var distinctOrds [2]distsqlpb.Ordering
		distinctOrds[0] = distsqlpb.ConvertToMappedSpecOrdering(
			leftProps.ordering, leftPlan.PlanToStreamColMap,
		)
		distinctOrds[1] = distsqlpb.ConvertToMappedSpecOrdering(
			rightProps.ordering, rightPlan.PlanToStreamColMap,
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
			distinctSpec.Distinct = &distsqlpb.DistinctSpec{
				DistinctColumns: streamCols,
				OrderedColumns:  sortCols,
			}
			if !dsp.isOnlyOnGateway(plan) {
				// TODO(solon): We could skip this stage if there is a strong key on
				// the result columns.
				plan.AddNoGroupingStage(
					*distinctSpec, distsqlpb.PostProcessSpec{}, plan.ResultTypes, distinctOrds[side])
				plan.AddProjection(streamCols)
			}
		}
	}

	var p PhysicalPlan

	// Merge the plans' PlanToStreamColMap, which we know are equivalent.
	p.PlanToStreamColMap = planToStreamColMap

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
		mergeOrdering = distsqlpb.ConvertToMappedSpecOrdering(newOrdering, p.PlanToStreamColMap)

		var childResultTypes [2][]types.T
		for side, plan := range childPhysicalPlans {
			childResultTypes[side], err = getTypesForPlanResult(
				childLogicalPlans[side], plan.PlanToStreamColMap,
			)
			if err != nil {
				return PhysicalPlan{}, err
			}
		}
		resultTypes, err = distsqlplan.MergeResultTypes(childResultTypes[0], childResultTypes[1])
		if err != nil {
			return PhysicalPlan{}, err
		}
	} else if err != nil || !mergeOrdering.Equal(rightPlan.MergeOrdering) {
		// The result types or merge ordering can differ between the two sides in
		// pathological cases, like if they have incompatible ORDER BY clauses.
		// Resolve this by collecting results on a single node and adding a
		// projection to the results that will be unioned.
		for _, plan := range childPhysicalPlans {
			plan.AddSingleGroupStage(
				dsp.nodeDesc.NodeID,
				distsqlpb.ProcessorCoreUnion{Noop: &distsqlpb.NoopCoreSpec{}},
				distsqlpb.PostProcessSpec{},
				plan.ResultTypes)
			plan.AddProjection(streamCols)
		}

		// Result types should now be mergeable.
		resultTypes, err = distsqlplan.MergeResultTypes(leftPlan.ResultTypes, rightPlan.ResultTypes)
		if err != nil {
			return PhysicalPlan{}, err
		}
		mergeOrdering = distsqlpb.Ordering{}
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
			distinctSpec := distsqlpb.ProcessorCoreUnion{
				Distinct: &distsqlpb.DistinctSpec{DistinctColumns: streamCols},
			}
			p.AddSingleGroupStage(
				dsp.nodeDesc.NodeID, distinctSpec, distsqlpb.PostProcessSpec{}, p.ResultTypes)
		} else {
			// UNION ALL is special: it doesn't have any required downstream
			// processor, so its two inputs might have different post-processing
			// which would violate an assumption later down the line. Check for this
			// condition and add a no-op stage if it exists.
			if err := p.CheckLastStagePost(); err != nil {
				p.AddSingleGroupStage(
					dsp.nodeDesc.NodeID,
					distsqlpb.ProcessorCoreUnion{Noop: &distsqlpb.NoopCoreSpec{}},
					distsqlpb.PostProcessSpec{},
					p.ResultTypes,
				)
			}
		}
	} else {
		// We plan INTERSECT and EXCEPT queries with joiners. Get the appropriate
		// join type.
		joinType := distsqlSetOpJoinType(n.unionType)

		// Nodes where we will run the join processors.
		nodes := findJoinProcessorNodes(leftRouters, rightRouters, p.Processors)

		// Set up the equality columns.
		eqCols := streamCols

		// Project the left-side columns only.
		post := distsqlpb.PostProcessSpec{Projection: true}
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
		var core distsqlpb.ProcessorCoreUnion
		if !planMergeJoins.Get(&dsp.st.SV) || len(mergeOrdering.Columns) < len(streamCols) {
			core.HashJoiner = &distsqlpb.HashJoinerSpec{
				LeftEqColumns:  eqCols,
				RightEqColumns: eqCols,
				Type:           joinType,
			}
		} else {
			core.MergeJoiner = &distsqlpb.MergeJoinerSpec{
				LeftOrdering:  mergeOrdering,
				RightOrdering: mergeOrdering,
				Type:          joinType,
				NullEquality:  true,
			}
		}

		if n.all {
			p.AddJoinStage(
				nodes, core, post, eqCols, eqCols,
				leftPlan.ResultTypes, rightPlan.ResultTypes,
				leftPlan.MergeOrdering, rightPlan.MergeOrdering,
				leftRouters, rightRouters,
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
			mergeOrdering = distsqlpb.Ordering{}
		}

		p.ResultTypes = resultTypes
		p.SetMergeOrdering(mergeOrdering)
	}

	return p, nil
}

// createPlanForWindow creates a physical plan for computing window functions.
// We add a new stage of windower processors for each different partitioning
// scheme found in the query's window functions.
func (dsp *DistSQLPlanner) createPlanForWindow(
	planCtx *PlanningCtx, n *windowNode,
) (PhysicalPlan, error) {
	plan, err := dsp.createPlanForNode(planCtx, n.plan)
	if err != nil {
		return PhysicalPlan{}, err
	}

	numWindowFuncProcessed := 0
	windowPlanState := createWindowPlanState(n, planCtx, &plan)
	// Each iteration of this loop adds a new stage of windowers. The steps taken:
	// 1. find a set of unprocessed window functions that have the same PARTITION BY
	//    clause. All of these will be computed using the single stage of windowers.
	// 2. a) populate output types of the current stage of windowers. All input
	//       columns are being passed through, and windower will append output
	//       columns for each window function processed at the stage.
	//    b) create specs for all window functions in the set.
	// 3. decide whether to put windowers on a single or on multiple nodes.
	//    a) if we're putting windowers on multiple nodes, we'll put them onto
	//       every node that participated in the previous stage. We leverage hash
	//       routers to partition the data based on PARTITION BY clause of window
	//       functions in the set.
	for numWindowFuncProcessed < len(n.funcs) {
		samePartitionFuncs, partitionIdxs := windowPlanState.findUnprocessedWindowFnsWithSamePartition()
		numWindowFuncProcessed += len(samePartitionFuncs)
		windowerSpec := distsqlpb.WindowerSpec{
			PartitionBy: partitionIdxs,
			WindowFns:   make([]distsqlpb.WindowerSpec_WindowFn, len(samePartitionFuncs)),
		}

		newResultTypes := make([]types.T, len(plan.ResultTypes)+len(samePartitionFuncs))
		copy(newResultTypes, plan.ResultTypes)
		for windowFnSpecIdx, windowFn := range samePartitionFuncs {
			windowFnSpec, outputType, err := windowPlanState.createWindowFnSpec(windowFn)
			if err != nil {
				return PhysicalPlan{}, err
			}
			newResultTypes[windowFn.outputColIdx] = *outputType
			windowerSpec.WindowFns[windowFnSpecIdx] = windowFnSpec
		}

		// Check if the previous stage is all on one node.
		prevStageNode := plan.Processors[plan.ResultRouters[0]].Node
		for i := 1; i < len(plan.ResultRouters); i++ {
			if n := plan.Processors[plan.ResultRouters[i]].Node; n != prevStageNode {
				prevStageNode = 0
				break
			}
		}

		// Get all nodes from the previous stage.
		nodes := getNodesOfRouters(plan.ResultRouters, plan.Processors)
		if len(partitionIdxs) == 0 || len(nodes) == 1 {
			// No PARTITION BY or we have a single node. Use a single windower.
			// If the previous stage was all on a single node, put the windower
			// there. Otherwise, bring the results back on this node.
			node := dsp.nodeDesc.NodeID
			if len(nodes) == 1 {
				node = nodes[0]
			}
			plan.AddSingleGroupStage(
				node,
				distsqlpb.ProcessorCoreUnion{Windower: &windowerSpec},
				distsqlpb.PostProcessSpec{},
				newResultTypes,
			)
		} else {
			// Set up the output routers from the previous stage.
			// We use hash routers with hashing on the columns
			// from PARTITION BY clause of window functions
			// we're processing in the current stage.
			for _, resultProc := range plan.ResultRouters {
				plan.Processors[resultProc].Spec.Output[0] = distsqlpb.OutputRouterSpec{
					Type:        distsqlpb.OutputRouterSpec_BY_HASH,
					HashColumns: partitionIdxs,
				}
			}
			stageID := plan.NewStageID()

			// We put a windower on each node and we connect it
			// with all hash routers from the previous stage in
			// a such way that each node has its designated
			// SourceRouterSlot - namely, position in which
			// a node appears in nodes.
			prevStageRouters := plan.ResultRouters
			plan.ResultRouters = make([]distsqlplan.ProcessorIdx, 0, len(nodes))
			for bucket, nodeID := range nodes {
				proc := distsqlplan.Processor{
					Node: nodeID,
					Spec: distsqlpb.ProcessorSpec{
						Input: []distsqlpb.InputSyncSpec{{
							Type:        distsqlpb.InputSyncSpec_UNORDERED,
							ColumnTypes: plan.ResultTypes,
						}},
						Core: distsqlpb.ProcessorCoreUnion{Windower: &windowerSpec},
						Post: distsqlpb.PostProcessSpec{},
						Output: []distsqlpb.OutputRouterSpec{{
							Type: distsqlpb.OutputRouterSpec_PASS_THROUGH,
						}},
						StageID: stageID,
					},
				}
				pIdx := plan.AddProcessor(proc)

				for _, router := range prevStageRouters {
					plan.Streams = append(plan.Streams, distsqlplan.Stream{
						SourceProcessor:  router,
						SourceRouterSlot: bucket,
						DestProcessor:    pIdx,
						DestInput:        0,
					})
				}
				plan.ResultRouters = append(plan.ResultRouters, pIdx)
			}

			plan.ResultTypes = newResultTypes
		}
	}

	// We definitely added columns throughout all the stages of windowers, so we
	// need to update PlanToStreamColMap. We need to update the map before adding
	// rendering or projection because it is used there.
	plan.PlanToStreamColMap = identityMap(plan.PlanToStreamColMap, len(plan.ResultTypes))

	// windowers do not guarantee maintaining the order at the moment, so we
	// reset MergeOrdering. There shouldn't be an ordering here, but we reset it
	// defensively (see #35179).
	plan.SetMergeOrdering(distsqlpb.Ordering{})

	// After all window functions are computed, we need to add rendering or
	// projection.
	if err := windowPlanState.addRenderingOrProjection(); err != nil {
		return PhysicalPlan{}, err
	}

	if len(plan.ResultTypes) != len(plan.PlanToStreamColMap) {
		// We added/removed columns while rendering or projecting, so we need to
		// update PlanToStreamColMap.
		plan.PlanToStreamColMap = identityMap(plan.PlanToStreamColMap, len(plan.ResultTypes))
	}

	return plan, nil
}

// NewPlanningCtx returns a new PlanningCtx.
func (dsp *DistSQLPlanner) NewPlanningCtx(
	ctx context.Context, evalCtx *extendedEvalContext, txn *client.Txn,
) *PlanningCtx {
	planCtx := dsp.newLocalPlanningCtx(ctx, evalCtx)
	planCtx.spanIter = dsp.spanResolver.NewSpanResolverIterator(txn)
	planCtx.NodeAddresses = make(map[roachpb.NodeID]string)
	planCtx.NodeAddresses[dsp.nodeDesc.NodeID] = dsp.nodeDesc.Address.String()
	return planCtx
}

// newLocalPlanningCtx is a lightweight version of NewPlanningCtx that can be
// used when the caller knows plans will only be run on one node.
func (dsp *DistSQLPlanner) newLocalPlanningCtx(
	ctx context.Context, evalCtx *extendedEvalContext,
) *PlanningCtx {
	return &PlanningCtx{
		ctx:             ctx,
		ExtendedEvalCtx: evalCtx,
	}
}

// FinalizePlan adds a final "result" stage if necessary and populates the
// endpoints of the plan.
func (dsp *DistSQLPlanner) FinalizePlan(planCtx *PlanningCtx, plan *PhysicalPlan) {
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
			distsqlpb.ProcessorCoreUnion{Noop: &distsqlpb.NoopCoreSpec{}},
			distsqlpb.PostProcessSpec{},
			plan.ResultTypes,
		)
		if len(plan.ResultRouters) != 1 {
			panic(fmt.Sprintf("%d results after single group stage", len(plan.ResultRouters)))
		}
	}

	if len(metadataSenders) > 0 {
		plan.AddSingleGroupStage(
			thisNodeID,
			distsqlpb.ProcessorCoreUnion{
				MetadataTestReceiver: &distsqlpb.MetadataTestReceiverSpec{
					SenderIDs: metadataSenders,
				},
			},
			distsqlpb.PostProcessSpec{},
			plan.ResultTypes,
		)
	}

	// Set up the endpoints for p.streams.
	plan.PopulateEndpoints(planCtx.NodeAddresses)

	// Set up the endpoint for the final result.
	finalOut := &plan.Processors[plan.ResultRouters[0]].Spec.Output[0]
	finalOut.Streams = append(finalOut.Streams, distsqlpb.StreamEndpointSpec{
		Type: distsqlpb.StreamEndpointSpec_SYNC_RESPONSE,
	})

	// Assign processor IDs.
	for i := range plan.Processors {
		plan.Processors[i].Spec.ProcessorID = int32(i)
	}
}

func makeTableReaderSpans(spans roachpb.Spans) []distsqlpb.TableReaderSpan {
	trSpans := make([]distsqlpb.TableReaderSpan, len(spans))
	for i, span := range spans {
		trSpans[i].Span = span
	}

	return trSpans
}
