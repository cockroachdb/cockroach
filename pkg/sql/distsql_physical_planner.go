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
	"math/rand"
	"reflect"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan/replicaoracle"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
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
	planVersion execinfrapb.DistSQLVersion

	st *cluster.Settings
	// The SQLInstanceID of the gateway node that initiated this query.
	gatewaySQLInstanceID base.SQLInstanceID
	stopper              *stop.Stopper
	distSQLSrv           *distsql.ServerImpl
	spanResolver         physicalplan.SpanResolver

	// metadataTestTolerance is the minimum level required to plan metadata test
	// processors.
	metadataTestTolerance execinfra.MetadataTestLevel

	// runnerChan is used to send out requests (for running SetupFlow RPCs) to a
	// pool of workers.
	runnerChan chan runnerRequest

	// cancelFlowsCoordinator is responsible for batching up the requests to
	// cancel remote flows initiated on the behalf of the current node when the
	// local flows errored out.
	cancelFlowsCoordinator cancelFlowsCoordinator

	// gossip handle used to check node version compatibility.
	gossip gossip.OptionalGossip

	// nodeDialer handles communication between SQL and KV nodes.
	nodeDialer *nodedialer.Dialer

	// podNodeDialer handles communication between SQL nodes/pods.
	podNodeDialer *nodedialer.Dialer

	// nodeHealth encapsulates the various node health checks to avoid planning
	// on unhealthy nodes.
	nodeHealth distSQLNodeHealth

	// parallelLocalScansSem is a node-wide semaphore on the number of
	// additional goroutines that can be used to run concurrent TableReaders
	// for the same stage of the fully local physical plans.
	parallelLocalScansSem *quotapool.IntPool

	// distSender is used to construct the spanResolver upon SetSQLInstanceInfo.
	distSender *kvcoord.DistSender
	// nodeDescs is used to construct the spanResolver upon SetSQLInstanceInfo.
	nodeDescs kvcoord.NodeDescStore
	// rpcCtx is used to construct the spanResolver upon SetSQLInstanceInfo.
	rpcCtx *rpc.Context

	// sqlInstanceProvider has information about SQL instances in a non-system
	// tenant environment.
	sqlInstanceProvider sqlinstance.Provider

	// codec allows the DistSQLPlanner to determine whether it is creating plans
	// for a system tenant or non-system tenant.
	codec keys.SQLCodec
}

// DistributionType is an enum defining when a plan should be distributed.
type DistributionType int

const (
	// DistributionTypeNone does not distribute a plan across multiple instances.
	DistributionTypeNone = iota
	// DistributionTypeAlways distributes a plan across multiple instances whether
	// it is a system tenant or non-system tenant.
	DistributionTypeAlways
	// DistributionTypeSystemTenantOnly only distributes a plan if it is for a
	// system tenant. Plans on non-system tenants are not distributed.
	DistributionTypeSystemTenantOnly
)

// ReplicaOraclePolicy controls which policy the physical planner uses to choose
// a replica for a given range. It is exported so that it may be overwritten
// during initialization by CCL code to enable follower reads.
var ReplicaOraclePolicy = replicaoracle.BinPackingChoice

// If true, the plan diagram (in JSON) is logged for each plan (used for
// debugging).
var logPlanDiagram = envutil.EnvOrDefaultBool("COCKROACH_DISTSQL_LOG_PLAN", false)

// NewDistSQLPlanner initializes a DistSQLPlanner.
//
// sqlInstanceID is the ID of the node on which this planner runs. It is used to
// favor itself and other close-by nodes when planning. An invalid sqlInstanceID
// can be passed to aid bootstrapping, but then SetSQLInstanceInfo() needs to be called
// before this planner is used.
func NewDistSQLPlanner(
	ctx context.Context,
	planVersion execinfrapb.DistSQLVersion,
	st *cluster.Settings,
	sqlInstanceID base.SQLInstanceID,
	rpcCtx *rpc.Context,
	distSQLSrv *distsql.ServerImpl,
	distSender *kvcoord.DistSender,
	nodeDescs kvcoord.NodeDescStore,
	gw gossip.OptionalGossip,
	stopper *stop.Stopper,
	isAvailable func(base.SQLInstanceID) bool,
	nodeDialer *nodedialer.Dialer,
	podNodeDialer *nodedialer.Dialer,
	codec keys.SQLCodec,
	sqlInstanceProvider sqlinstance.Provider,
) *DistSQLPlanner {
	dsp := &DistSQLPlanner{
		planVersion:          planVersion,
		st:                   st,
		gatewaySQLInstanceID: sqlInstanceID,
		stopper:              stopper,
		distSQLSrv:           distSQLSrv,
		gossip:               gw,
		nodeDialer:           nodeDialer,
		podNodeDialer:        podNodeDialer,
		nodeHealth: distSQLNodeHealth{
			gossip:      gw,
			connHealth:  nodeDialer.ConnHealthTryDial,
			isAvailable: isAvailable,
		},
		distSender:            distSender,
		nodeDescs:             nodeDescs,
		rpcCtx:                rpcCtx,
		metadataTestTolerance: execinfra.NoExplain,
		sqlInstanceProvider:   sqlInstanceProvider,
		codec:                 codec,
	}

	dsp.parallelLocalScansSem = quotapool.NewIntPool("parallel local scans concurrency",
		uint64(localScansConcurrencyLimit.Get(&st.SV)))
	localScansConcurrencyLimit.SetOnChange(&st.SV, func(ctx context.Context) {
		dsp.parallelLocalScansSem.UpdateCapacity(uint64(localScansConcurrencyLimit.Get(&st.SV)))
	})
	if rpcCtx != nil {
		// rpcCtx might be nil in some tests.
		rpcCtx.Stopper.AddCloser(dsp.parallelLocalScansSem.Closer("stopper"))
	}

	dsp.initRunners(ctx)
	dsp.initCancelingWorkers(ctx)
	return dsp
}

func (dsp *DistSQLPlanner) shouldPlanTestMetadata() bool {
	return dsp.distSQLSrv.TestingKnobs.MetadataTestLevel >= dsp.metadataTestTolerance
}

// GetSQLInstanceInfo gets a node descriptor by node ID.
func (dsp *DistSQLPlanner) GetSQLInstanceInfo(
	sqlInstanceID base.SQLInstanceID,
) (*roachpb.NodeDescriptor, error) {
	return dsp.nodeDescs.GetNodeDescriptor(roachpb.NodeID(sqlInstanceID))
}

// SetSQLInstanceInfo sets the planner's node descriptor.
// The first call to SetSQLInstanceInfo leads to the construction of the SpanResolver.
func (dsp *DistSQLPlanner) SetSQLInstanceInfo(desc roachpb.NodeDescriptor) {
	dsp.gatewaySQLInstanceID = base.SQLInstanceID(desc.NodeID)
	if dsp.spanResolver == nil {
		sr := physicalplan.NewSpanResolver(dsp.st, dsp.distSender, dsp.nodeDescs, desc,
			dsp.rpcCtx, ReplicaOraclePolicy)
		dsp.SetSpanResolver(sr)
	}
}

// GatewayID returns the ID of the gateway.
func (dsp *DistSQLPlanner) GatewayID() base.SQLInstanceID {
	return dsp.gatewaySQLInstanceID
}

// SetSpanResolver switches to a different SpanResolver. It is the caller's
// responsibility to make sure the DistSQLPlanner is not in use.
func (dsp *DistSQLPlanner) SetSpanResolver(spanResolver physicalplan.SpanResolver) {
	dsp.spanResolver = spanResolver
}

// distSQLExprCheckVisitor is a tree.Visitor that checks if expressions
// contain things not supported by distSQL, like distSQL-blocklisted functions.
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
		if t.IsDistSQLBlocklist() {
			v.err = newQueryNotSupportedErrorf("function %s cannot be executed with distsql", t)
			return false, expr
		}
	case *tree.DOid:
		v.err = newQueryNotSupportedError("OID expressions are not supported by distsql")
		return false, expr
	case *tree.CastExpr:
		// TODO (rohany): I'm not sure why this CastExpr doesn't have a type
		//  annotation at this stage of processing...
		if typ, ok := tree.GetStaticallyKnownType(t.Type); ok {
			switch typ.Family() {
			case types.OidFamily:
				v.err = newQueryNotSupportedErrorf("cast to %s is not supported by distsql", t.Type)
				return false, expr
			}
		}
	case *tree.DArray:
		// We need to check for arrays of untyped tuples here since constant-folding
		// on builtin functions sometimes produces this. DecodeUntaggedDatum
		// requires that all the types of the tuple contents are known.
		if t.ResolvedType().ArrayContents() == types.AnyTuple {
			v.err = newQueryNotSupportedErrorf("array %s cannot be executed with distsql", t)
			return false, expr
		}
	case *tree.DTuple:
		if t.ResolvedType() == types.AnyTuple {
			v.err = newQueryNotSupportedErrorf("tuple %s cannot be executed with distsql", t)
			return false, expr
		}
	}
	return true, expr
}

func (v *distSQLExprCheckVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

// checkExpr verifies that an expression doesn't contain things that are not yet
// supported by distSQL, like distSQL-blocklisted functions.
func checkExpr(expr tree.Expr) error {
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

// planNodeNotSupportedErr is the catch-all error value returned from
// checkSupportForPlanNode when a planNode type does not support distributed
// execution.
var planNodeNotSupportedErr = newQueryNotSupportedError("unsupported node")

var cannotDistributeRowLevelLockingErr = newQueryNotSupportedError(
	"scans with row-level locking are not supported by distsql",
)

// mustWrapNode returns true if a node has no DistSQL-processor equivalent.
// This must be kept in sync with createPhysPlanForPlanNode.
// TODO(jordan): refactor these to use the observer pattern to avoid duplication.
func (dsp *DistSQLPlanner) mustWrapNode(planCtx *PlanningCtx, node planNode) bool {
	switch n := node.(type) {
	// Keep these cases alphabetized, please!
	case *distinctNode:
	case *exportNode:
	case *filterNode:
	case *groupNode:
	case *indexJoinNode:
	case *invertedFilterNode:
	case *invertedJoinNode:
	case *joinNode:
	case *limitNode:
	case *lookupJoinNode:
	case *ordinalityNode:
	case *projectSetNode:
	case *renderNode:
	case *scanNode:
	case *sortNode:
	case *topKNode:
	case *unaryNode:
	case *unionNode:
	case *valuesNode:
		return mustWrapValuesNode(planCtx, n.specifiedInQuery)
	case *windowNode:
	case *zeroNode:
	case *zigzagJoinNode:
	default:
		return true
	}
	return false
}

// mustWrapValuesNode returns whether a valuesNode must be wrapped into the
// physical plan which indicates that we cannot create a values processor. This
// method can be used before actually creating the valuesNode to decide whether
// that creation can be avoided or when we have existing valuesNode and need to
// decide whether we can create a corresponding values processor.
func mustWrapValuesNode(planCtx *PlanningCtx, specifiedInQuery bool) bool {
	// If a valuesNode wasn't specified in the query, it means that it was
	// autogenerated for things that we don't want to be distributing, like
	// populating values from a virtual table. So, we must wrap the valuesNode.
	//
	// If the plan is local, we also wrap the valuesNode to avoid pointless
	// serialization of the values, and also to avoid situations in which
	// expressions within the valuesNode were not distributable in the first
	// place.
	if !specifiedInQuery || planCtx.isLocal {
		return true
	}
	return false
}

// checkSupportForPlanNode returns a distRecommendation (as described above) or
// cannotDistribute and an error if the plan subtree is not distributable.
// The error doesn't indicate complete failure - it's instead the reason that
// this plan couldn't be distributed.
// TODO(radu): add tests for this.
func checkSupportForPlanNode(node planNode) (distRecommendation, error) {
	switch n := node.(type) {
	// Keep these cases alphabetized, please!
	case *distinctNode:
		return checkSupportForPlanNode(n.plan)

	case *exportNode:
		return checkSupportForPlanNode(n.source)

	case *filterNode:
		if err := checkExpr(n.filter); err != nil {
			return cannotDistribute, err
		}
		return checkSupportForPlanNode(n.source.plan)

	case *groupNode:
		rec, err := checkSupportForPlanNode(n.plan)
		if err != nil {
			return cannotDistribute, err
		}
		// Distribute aggregations if possible.
		return rec.compose(shouldDistribute), nil

	case *indexJoinNode:
		// n.table doesn't have meaningful spans, but we need to check support (e.g.
		// for any filtering expression).
		if _, err := checkSupportForPlanNode(n.table); err != nil {
			return cannotDistribute, err
		}
		return checkSupportForPlanNode(n.input)

	case *invertedFilterNode:
		return checkSupportForInvertedFilterNode(n)

	case *invertedJoinNode:
		if err := checkExpr(n.onExpr); err != nil {
			return cannotDistribute, err
		}
		rec, err := checkSupportForPlanNode(n.input)
		if err != nil {
			return cannotDistribute, err
		}
		return rec.compose(shouldDistribute), nil

	case *joinNode:
		if err := checkExpr(n.pred.onCond); err != nil {
			return cannotDistribute, err
		}
		recLeft, err := checkSupportForPlanNode(n.left.plan)
		if err != nil {
			return cannotDistribute, err
		}
		recRight, err := checkSupportForPlanNode(n.right.plan)
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

	case *limitNode:
		// Note that we don't need to check whether we support distribution of
		// n.countExpr or n.offsetExpr because those expressions are evaluated
		// locally, during the physical planning.
		return checkSupportForPlanNode(n.plan)

	case *lookupJoinNode:
		if n.table.lockingStrength != descpb.ScanLockingStrength_FOR_NONE {
			// Lookup joins that are performing row-level locking cannot
			// currently be distributed because their locks would not be
			// propagated back to the root transaction coordinator.
			// TODO(nvanbenschoten): lift this restriction.
			return cannotDistribute, cannotDistributeRowLevelLockingErr
		}

		if err := checkExpr(n.lookupExpr); err != nil {
			return cannotDistribute, err
		}
		if err := checkExpr(n.remoteLookupExpr); err != nil {
			return cannotDistribute, err
		}
		if err := checkExpr(n.onCond); err != nil {
			return cannotDistribute, err
		}
		rec, err := checkSupportForPlanNode(n.input)
		if err != nil {
			return cannotDistribute, err
		}
		return rec.compose(canDistribute), nil

	case *ordinalityNode:
		// WITH ORDINALITY never gets distributed so that the gateway node can
		// always number each row in order.
		return cannotDistribute, nil

	case *projectSetNode:
		return checkSupportForPlanNode(n.source)

	case *renderNode:
		for _, e := range n.render {
			if err := checkExpr(e); err != nil {
				return cannotDistribute, err
			}
		}
		return checkSupportForPlanNode(n.source.plan)

	case *scanNode:
		if n.lockingStrength != descpb.ScanLockingStrength_FOR_NONE {
			// Scans that are performing row-level locking cannot currently be
			// distributed because their locks would not be propagated back to
			// the root transaction coordinator.
			// TODO(nvanbenschoten): lift this restriction.
			return cannotDistribute, cannotDistributeRowLevelLockingErr
		}

		switch {
		case n.localityOptimized:
			// This is a locality optimized scan.
			return cannotDistribute, nil
		case n.isFull:
			// This is a full scan.
			return shouldDistribute, nil
		default:
			// Although we don't yet recommend distributing plans where soft limits
			// propagate to scan nodes because we don't have infrastructure to only
			// plan for a few ranges at a time, the propagation of the soft limits
			// to scan nodes has been added in 20.1 release, so to keep the
			// previous behavior we continue to ignore the soft limits for now.
			// TODO(yuzefovich): pay attention to the soft limits.
			return canDistribute, nil
		}

	case *sortNode:
		rec, err := checkSupportForPlanNode(n.plan)
		if err != nil {
			return cannotDistribute, err
		}
		return rec.compose(shouldDistribute), nil

	case *topKNode:
		rec, err := checkSupportForPlanNode(n.plan)
		if err != nil {
			return cannotDistribute, err
		}
		// If we have a top K sort, we can distribute the query.
		return rec.compose(canDistribute), nil

	case *unaryNode:
		return canDistribute, nil

	case *unionNode:
		recLeft, err := checkSupportForPlanNode(n.left)
		if err != nil {
			return cannotDistribute, err
		}
		recRight, err := checkSupportForPlanNode(n.right)
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
				if err := checkExpr(expr); err != nil {
					return cannotDistribute, err
				}
			}
		}
		return canDistribute, nil

	case *windowNode:
		return checkSupportForPlanNode(n.plan)

	case *zeroNode:
		return canDistribute, nil

	case *zigzagJoinNode:
		if err := checkExpr(n.onCond); err != nil {
			return cannotDistribute, err
		}
		return shouldDistribute, nil

	case *createStatsNode:
		if n.runAsJob {
			return cannotDistribute, planNodeNotSupportedErr
		}
		return shouldDistribute, nil

	default:
		return cannotDistribute, planNodeNotSupportedErr
	}
}

func checkSupportForInvertedFilterNode(n *invertedFilterNode) (distRecommendation, error) {
	rec, err := checkSupportForPlanNode(n.input)
	if err != nil {
		return cannotDistribute, err
	}
	// When filtering is a union of inverted spans, it is distributable: place
	// an inverted filterer on each node, which produce the primary keys in
	// arbitrary order, and de-duplicate the PKs at the next stage.
	// The expression is a union of inverted spans iff all the spans have been
	// promoted to FactoredUnionSpans, in which case the Left and Right
	// inverted.Expressions are nil.
	//
	// TODO(sumeer): Even if the filtering cannot be distributed, the
	// placement of the inverted filter could be optimized. Specifically, when
	// the input is a single processor (because the TableReader is reading
	// span(s) that are all on the same node), we can place the inverted
	// filterer on that input node. Currently, this approach fails because we
	// don't know whether the input is a single processor at this stage, and if
	// we blindly returned shouldDistribute, we encounter situations where
	// remote TableReaders are feeding an inverted filterer which runs into an
	// encoding problem with inverted columns. The remote code tries to decode
	// the inverted column as the original type (e.g. for geospatial, tries to
	// decode the int cell-id as a geometry) which obviously fails -- this is
	// related to #50659. Fix this in the distSQLSpecExecFactory.
	filterRec := cannotDistribute
	if n.expression.Left == nil && n.expression.Right == nil {
		filterRec = shouldDistribute
	}
	return rec.compose(filterRec), nil
}

//go:generate stringer -type=NodeStatus

// NodeStatus represents a node's health and compatibility in the context of
// physical planning for a query.
type NodeStatus int

const (
	// NodeOK means that the node can be used for planning.
	NodeOK NodeStatus = iota
	// NodeUnhealthy means that the node should be avoided because
	// it's not healthy.
	NodeUnhealthy
	// NodeDistSQLVersionIncompatible means that the node should be avoided
	// because it's DistSQL version is not compatible.
	NodeDistSQLVersionIncompatible
)

// PlanningCtx contains data used and updated throughout the planning process of
// a single query.
type PlanningCtx struct {
	ctx             context.Context
	ExtendedEvalCtx *extendedEvalContext
	spanIter        physicalplan.SpanResolverIterator
	// NodesStatuses contains info for all SQLInstanceIDs that are referenced by
	// any PhysicalPlan we generate with this context.
	NodeStatuses map[base.SQLInstanceID]NodeStatus

	infra physicalplan.PhysicalInfrastructure

	// isLocal is set to true if we're planning this query on a single node.
	isLocal bool
	planner *planner
	// ignoreClose, when set to true, will prevent the closing of the planner's
	// current plan. Only the top-level query needs to close it, but everything
	// else (like sub- and postqueries, or EXPLAIN ANALYZE) should set this to
	// true to avoid double closes of the planNode tree.
	ignoreClose bool
	stmtType    tree.StatementReturnType
	// planDepth is set to the current depth of the planNode tree. It's used to
	// keep track of whether it's valid to run a root node in a special fast path
	// mode.
	planDepth int

	// If set, the flows for the physical plan will be passed to this function.
	// The flows are not safe for use past the lifetime of the saveFlows function.
	saveFlows func(map[base.SQLInstanceID]*execinfrapb.FlowSpec, execinfra.OpChains) error

	// If set, we will record the mapping from planNode to tracing metadata to
	// later allow associating statistics with the planNode.
	traceMetadata execNodeTraceMetadata

	// If set, statement execution stats should be collected.
	collectExecStats bool

	// parallelizeScansIfLocal indicates whether we might want to create
	// multiple table readers if the physical plan ends up being fully local.
	// This value is determined based on whether there are any mutations in the
	// plan (which prohibit all concurrency) and whether all parts of the plan
	// are supported natively by the vectorized engine.
	parallelizeScansIfLocal bool

	// onFlowCleanup contains non-nil functions that will be called after the
	// local flow finished running and is being cleaned up. It allows us to
	// release the resources that are acquired during the physical planning and
	// are being hold onto throughout the whole flow lifecycle.
	onFlowCleanup []func()
}

var _ physicalplan.ExprContext = &PlanningCtx{}

// NewPhysicalPlan creates an empty PhysicalPlan, backed by the
// PlanInfrastructure in the planning context.
//
// Note that any processors created in the physical plan cannot be discarded;
// they have to be part of the final plan.
func (p *PlanningCtx) NewPhysicalPlan() *PhysicalPlan {
	return &PhysicalPlan{
		PhysicalPlan: physicalplan.MakePhysicalPlan(&p.infra),
	}
}

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

// getDefaultSaveFlowsFunc returns the default function used to save physical
// plans and their diagrams.
func (p *PlanningCtx) getDefaultSaveFlowsFunc(
	ctx context.Context, planner *planner, typ planComponentType,
) func(map[base.SQLInstanceID]*execinfrapb.FlowSpec, execinfra.OpChains) error {
	return func(flows map[base.SQLInstanceID]*execinfrapb.FlowSpec, opChains execinfra.OpChains) error {
		var diagram execinfrapb.FlowDiagram
		if planner.instrumentation.shouldSaveDiagrams() {
			diagramFlags := execinfrapb.DiagramFlags{
				MakeDeterministic: planner.execCfg.TestingKnobs.DeterministicExplain,
			}
			var err error
			diagram, err = p.flowSpecsToDiagram(ctx, flows, diagramFlags)
			if err != nil {
				return err
			}
		}
		var explainVec []string
		var explainVecVerbose []string
		if planner.instrumentation.collectBundle && planner.curPlan.flags.IsSet(planFlagVectorized) {
			flowCtx := newFlowCtxForExplainPurposes(p, planner)
			getExplain := func(verbose bool) []string {
				explain, cleanup, err := colflow.ExplainVec(
					ctx, flowCtx, flows, p.infra.LocalProcessors, opChains,
					planner.extendedEvalCtx.DistSQLPlanner.gatewaySQLInstanceID,
					verbose, planner.curPlan.flags.IsDistributed(),
				)
				cleanup()
				if err != nil {
					// In some edge cases (like when subqueries are present or
					// when certain component doesn't implement execinfra.OpNode
					// interface) an error might occur. In such scenario, we
					// don't want to fail the collection of the bundle, so we
					// deliberately ignoring the error.
					explain = nil
				}
				return explain
			}
			explainVec = getExplain(false /* verbose */)
			explainVecVerbose = getExplain(true /* verbose */)
		}
		planner.curPlan.distSQLFlowInfos = append(
			planner.curPlan.distSQLFlowInfos,
			flowInfo{
				typ:               typ,
				diagram:           diagram,
				explainVec:        explainVec,
				explainVecVerbose: explainVecVerbose,
				flowsMetadata:     execstats.NewFlowsMetadata(flows),
			},
		)
		return nil
	}
}

// flowSpecsToDiagram is a helper function used to convert flowSpecs into a
// FlowDiagram using this PlanningCtx's information.
func (p *PlanningCtx) flowSpecsToDiagram(
	ctx context.Context,
	flows map[base.SQLInstanceID]*execinfrapb.FlowSpec,
	diagramFlags execinfrapb.DiagramFlags,
) (execinfrapb.FlowDiagram, error) {
	log.VEvent(ctx, 1, "creating plan diagram")
	var stmtStr string
	if p.planner != nil && p.planner.stmt.AST != nil {
		stmtStr = p.planner.stmt.String()
	}
	diagram, err := execinfrapb.GeneratePlanDiagram(
		stmtStr, flows, diagramFlags,
	)
	if err != nil {
		return nil, err
	}
	return diagram, nil
}

// getCleanupFunc returns a non-nil function that needs to be called after the
// local flow finished running. This can be called only after the physical
// planning has been completed.
func (p *PlanningCtx) getCleanupFunc() func() {
	return func() {
		for _, r := range p.onFlowCleanup {
			r()
		}
	}
}

// PhysicalPlan is a partial physical plan which corresponds to a planNode
// (partial in that it can correspond to a planNode subtree and not necessarily
// to the entire planNode for a given query).
//
// It augments physicalplan.PhysicalPlan with information relating the physical
// plan to a planNode subtree.
//
// These plans are built recursively on a planNode tree.
type PhysicalPlan struct {
	physicalplan.PhysicalPlan

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
	// Before the query is run, the physical plan must be finalized, and during
	// the finalization a projection is added to the plan so that
	// DistSQLReceiver gets rows of the desired schema from the output
	// processor.
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

// SpanPartition associates a subset of spans with a specific SQL instance,
// chosen to have the most efficient access to those spans. In the single-tenant
// case, the instance is the one running on the same node as the leaseholder for
// those spans.
type SpanPartition struct {
	SQLInstanceID base.SQLInstanceID
	Spans         roachpb.Spans
}

type distSQLNodeHealth struct {
	gossip      gossip.OptionalGossip
	isAvailable func(base.SQLInstanceID) bool
	connHealth  func(roachpb.NodeID, rpc.ConnectionClass) error
}

func (h *distSQLNodeHealth) check(ctx context.Context, sqlInstanceID base.SQLInstanceID) error {
	{
		// NB: as of #22658, ConnHealth does not work as expected; see the
		// comment within. We still keep this code for now because in
		// practice, once the node is down it will prevent using this node
		// 90% of the time (it gets used around once per second as an
		// artifact of rpcContext's reconnection mechanism at the time of
		// writing). This is better than having it used in 100% of cases
		// (until the liveness check below kicks in).
		err := h.connHealth(roachpb.NodeID(sqlInstanceID), rpc.DefaultClass)
		if err != nil && !errors.Is(err, rpc.ErrNotHeartbeated) {
			// This host is known to be unhealthy. Don't use it (use the gateway
			// instead). Note: this can never happen for our sqlInstanceID (which
			// always has its address in the nodeMap).
			log.VEventf(ctx, 1, "marking n%d as unhealthy for this plan: %v", sqlInstanceID, err)
			return err
		}
	}
	if !h.isAvailable(sqlInstanceID) {
		return pgerror.Newf(pgcode.CannotConnectNow, "not using n%d since it is not available", sqlInstanceID)
	}

	// Check that the node is not draining.
	if g, ok := h.gossip.Optional(distsql.MultiTenancyIssueNo); ok {
		drainingInfo := &execinfrapb.DistSQLDrainingInfo{}
		if err := g.GetInfoProto(gossip.MakeDistSQLDrainingKey(sqlInstanceID), drainingInfo); err != nil {
			// Because draining info has no expiration, an error
			// implies that we have not yet received a node's
			// draining information. Since this information is
			// written on startup, the most likely scenario is
			// that the node is ready. We therefore return no
			// error.
			// TODO(ajwerner): Determine the expected error types and only filter those.
			return nil //nolint:returnerrcheck
		}

		if drainingInfo.Draining {
			err := errors.Newf("not using n%d because it is draining", sqlInstanceID)
			log.VEventf(ctx, 1, "%v", err)
			return err
		}
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
	if dsp.codec.ForSystemTenant() {
		return dsp.partitionSpansSystem(planCtx, spans)
	}
	return dsp.partitionSpansTenant(planCtx, spans)
}

// partitionSpansSystem finds node owners for ranges touching the given spans
// for a system tenant.
func (dsp *DistSQLPlanner) partitionSpansSystem(
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
			SpanPartition{dsp.gatewaySQLInstanceID, spans})
		return partitions, nil
	}
	// nodeMap maps a SQLInstanceID to an index inside the partitions array.
	nodeMap := make(map[base.SQLInstanceID]int)
	it := planCtx.spanIter
	for i := range spans {

		span := spans[i]
		noEndKey := false
		if len(span.EndKey) == 0 {
			// If we see a span to partition that has no end key, it means that
			// we're going to do a point lookup on the start key of this span.
			//
			// The code below us doesn't really tolerate spans without an
			// EndKey, so we manufacture a single-key span for this case. Note
			// that we still, however, will preserve the point lookup.
			span = roachpb.Span{
				Key:    span.Key,
				EndKey: span.Key.Next(),
			}
			noEndKey = true
		}

		// rSpan is the span we are currently partitioning.
		rSpan, err := keys.SpanAddr(span)
		if err != nil {
			return nil, err
		}

		var lastSQLInstanceID base.SQLInstanceID
		// lastKey maintains the EndKey of the last piece of `span`.
		lastKey := rSpan.Key
		if log.V(1) {
			log.Infof(ctx, "partitioning span %s", span)
		}
		// We break up rSpan into its individual ranges (which may or
		// may not be on separate nodes). We then create "partitioned
		// spans" using the end keys of these individual ranges.
		for it.Seek(ctx, span, kvcoord.Ascending); ; it.Next(ctx) {
			if !it.Valid() {
				return nil, it.Error()
			}
			replDesc, err := it.ReplicaInfo(ctx)
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
			if rSpan.EndKey.Less(endKey) {
				endKey = rSpan.EndKey
			}

			sqlInstanceID := base.SQLInstanceID(replDesc.NodeID)
			partitionIdx, inNodeMap := nodeMap[sqlInstanceID]
			if !inNodeMap {
				// This is the first time we are seeing this sqlInstanceID for these
				// spans. Check its health.
				status := dsp.CheckInstanceHealthAndVersion(planCtx, sqlInstanceID)
				// If the node is unhealthy or its DistSQL version is incompatible, use
				// the gateway to process this span instead of the unhealthy host.
				// An empty address indicates an unhealthy host.
				if status != NodeOK {
					log.Eventf(ctx, "not planning on node %d: %s", sqlInstanceID, status)
					sqlInstanceID = dsp.gatewaySQLInstanceID
					partitionIdx, inNodeMap = nodeMap[sqlInstanceID]
				}

				if !inNodeMap {
					partitionIdx = len(partitions)
					partitions = append(partitions, SpanPartition{SQLInstanceID: sqlInstanceID})
					nodeMap[sqlInstanceID] = partitionIdx
				}
			}
			partition := &partitions[partitionIdx]

			if noEndKey {
				// The original span had no EndKey, and we want to preserve it
				// so that we could use a GetRequest.
				partition.Spans = append(partition.Spans, roachpb.Span{
					Key: lastKey.AsRawKey(),
				})
				break
			}

			if lastSQLInstanceID == sqlInstanceID {
				// Two consecutive ranges on the same node, merge the spans.
				partition.Spans[len(partition.Spans)-1].EndKey = endKey.AsRawKey()
			} else {
				partition.Spans = append(partition.Spans, roachpb.Span{
					Key:    lastKey.AsRawKey(),
					EndKey: endKey.AsRawKey(),
				})
			}

			if !endKey.Less(rSpan.EndKey) {
				// Done.
				break
			}

			lastKey = endKey
			lastSQLInstanceID = sqlInstanceID
		}
	}
	return partitions, nil
}

// partitionSpansTenant assigns SQL instances in a tenant to spans. Currently
// assignments are made to all available instances in a round-robin fashion.
func (dsp *DistSQLPlanner) partitionSpansTenant(
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
			SpanPartition{dsp.gatewaySQLInstanceID, spans})
		return partitions, nil
	}
	if dsp.sqlInstanceProvider == nil {
		return nil, errors.New("sql instance provider not available in multi-tenant environment")
	}
	// GetAllInstances only returns healthy instances.
	instances, err := dsp.sqlInstanceProvider.GetAllInstances(ctx)
	if err != nil {
		return nil, err
	}
	// Randomize the order in which we assign partitions, so that work is
	// allocated fairly across queries.
	rand.Shuffle(len(instances), func(i, j int) {
		instances[i], instances[j] = instances[j], instances[i]
	})

	// nodeMap maps a SQLInstanceID to an index inside the partitions array.
	nodeMap := make(map[base.SQLInstanceID]int)
	for i := range spans {
		span := spans[i]
		if log.V(1) {
			log.Infof(ctx, "partitioning span %s", span)
		}
		sqlInstanceID := instances[i%len(instances)].InstanceID
		partitionIdx, inNodeMap := nodeMap[sqlInstanceID]
		if !inNodeMap {
			partitionIdx = len(partitions)
			partitions = append(partitions, SpanPartition{SQLInstanceID: sqlInstanceID})
			nodeMap[sqlInstanceID] = partitionIdx
		}
		partition := &partitions[partitionIdx]
		partition.Spans = append(partition.Spans, span)
	}
	return partitions, nil
}

// nodeVersionIsCompatible decides whether a particular node's DistSQL version
// is compatible with dsp.planVersion. It uses gossip to find out the node's
// version range.
func (dsp *DistSQLPlanner) nodeVersionIsCompatible(sqlInstanceID base.SQLInstanceID) bool {
	g, ok := dsp.gossip.Optional(distsql.MultiTenancyIssueNo)
	if !ok {
		return true // no gossip - always compatible; only a single gateway running in Phase 2
	}
	var v execinfrapb.DistSQLVersionGossipInfo
	if err := g.GetInfoProto(gossip.MakeDistSQLNodeVersionKey(sqlInstanceID), &v); err != nil {
		return false
	}
	return distsql.FlowVerIsCompatible(dsp.planVersion, v.MinAcceptedVersion, v.Version)
}

func getIndexIdx(index catalog.Index, desc catalog.TableDescriptor) (uint32, error) {
	if index.Public() {
		return uint32(index.Ordinal()), nil
	}
	return 0, errors.Errorf("invalid index %v (table %s)", index, desc.GetName())
}

// initTableReaderSpecTemplate initializes a TableReaderSpec/PostProcessSpec
// that corresponds to a scanNode, except for the following fields:
//  - Spans
//  - Parallelize
//  - BatchBytesLimit
// The generated specs will be used as templates for planning potentially
// multiple TableReaders.
func initTableReaderSpecTemplate(
	n *scanNode, codec keys.SQLCodec,
) (*execinfrapb.TableReaderSpec, execinfrapb.PostProcessSpec, error) {
	if n.isCheck {
		return nil, execinfrapb.PostProcessSpec{}, errors.AssertionFailedf("isCheck no longer supported")
	}
	colIDs := make([]descpb.ColumnID, len(n.cols))
	for i := range n.cols {
		colIDs[i] = n.cols[i].GetID()
	}
	s := physicalplan.NewTableReaderSpec()
	*s = execinfrapb.TableReaderSpec{
		Reverse:                         n.reverse,
		TableDescriptorModificationTime: n.desc.GetModificationTime(),
		LockingStrength:                 n.lockingStrength,
		LockingWaitPolicy:               n.lockingWaitPolicy,
	}
	if err := rowenc.InitIndexFetchSpec(&s.FetchSpec, codec, n.desc, n.index, colIDs); err != nil {
		return nil, execinfrapb.PostProcessSpec{}, err
	}

	var post execinfrapb.PostProcessSpec
	if n.hardLimit != 0 {
		post.Limit = uint64(n.hardLimit)
	} else if n.softLimit != 0 {
		s.LimitHint = n.softLimit
	}
	return s, post, nil
}

// tableOrdinal returns the index of a column with the given ID.
func tableOrdinal(desc catalog.TableDescriptor, colID descpb.ColumnID) int {
	col, _ := desc.FindColumnWithID(colID)
	if col == nil {
		panic(errors.AssertionFailedf("column %d not in desc.Columns", colID))
	}
	return col.Ordinal()
}

// convertOrdering maps the columns in props.ordering to the output columns of a
// processor.
func (dsp *DistSQLPlanner) convertOrdering(
	reqOrdering ReqOrdering, planToStreamColMap []int,
) execinfrapb.Ordering {
	if len(reqOrdering) == 0 {
		return execinfrapb.Ordering{}
	}
	result := execinfrapb.Ordering{
		Columns: make([]execinfrapb.Ordering_Column, len(reqOrdering)),
	}
	for i, o := range reqOrdering {
		streamColIdx := o.ColIdx
		if planToStreamColMap != nil {
			streamColIdx = planToStreamColMap[o.ColIdx]
		}
		if streamColIdx == -1 {
			panic("column in ordering not part of processor output")
		}
		result.Columns[i].ColIdx = uint32(streamColIdx)
		dir := execinfrapb.Ordering_Column_ASC
		if o.Direction == encoding.Descending {
			dir = execinfrapb.Ordering_Column_DESC
		}
		result.Columns[i].Direction = dir
	}
	return result
}

// getInstanceIDForScan retrieves the SQL Instance ID where the single table reader
// should reside for a limited scan. Ideally this is the lease holder for the
// first range in the specified spans. But if that node is unhealthy or
// incompatible, we use the gateway node instead.
func (dsp *DistSQLPlanner) getInstanceIDForScan(
	planCtx *PlanningCtx, spans []roachpb.Span, reverse bool,
) (base.SQLInstanceID, error) {
	if len(spans) == 0 {
		panic("no spans")
	}

	// Determine the node ID for the first range to be scanned.
	it := planCtx.spanIter
	if reverse {
		it.Seek(planCtx.ctx, spans[len(spans)-1], kvcoord.Descending)
	} else {
		it.Seek(planCtx.ctx, spans[0], kvcoord.Ascending)
	}
	if !it.Valid() {
		return 0, it.Error()
	}
	replDesc, err := it.ReplicaInfo(planCtx.ctx)
	if err != nil {
		return 0, err
	}

	sqlInstanceID := base.SQLInstanceID(replDesc.NodeID)
	status := dsp.CheckInstanceHealthAndVersion(planCtx, sqlInstanceID)
	if status != NodeOK {
		log.Eventf(planCtx.ctx, "not planning on node %d: %s", sqlInstanceID, status)
		return dsp.gatewaySQLInstanceID, nil
	}
	return sqlInstanceID, nil
}

// CheckInstanceHealthAndVersion returns a information about a node's health and
// compatibility. The info is also recorded in planCtx.Nodes.
func (dsp *DistSQLPlanner) CheckInstanceHealthAndVersion(
	planCtx *PlanningCtx, sqlInstanceID base.SQLInstanceID,
) NodeStatus {
	if status, ok := planCtx.NodeStatuses[sqlInstanceID]; ok {
		return status
	}

	var status NodeStatus
	if err := dsp.nodeHealth.check(planCtx.ctx, sqlInstanceID); err != nil {
		status = NodeUnhealthy
	} else if !dsp.nodeVersionIsCompatible(sqlInstanceID) {
		status = NodeDistSQLVersionIncompatible
	} else {
		status = NodeOK
	}
	planCtx.NodeStatuses[sqlInstanceID] = status
	return status
}

// createTableReaders generates a plan consisting of table reader processors,
// one for each node that has spans that we are reading.
func (dsp *DistSQLPlanner) createTableReaders(
	planCtx *PlanningCtx, n *scanNode,
) (*PhysicalPlan, error) {
	spec, post, err := initTableReaderSpecTemplate(n, planCtx.ExtendedEvalCtx.Codec)
	if err != nil {
		return nil, err
	}

	p := planCtx.NewPhysicalPlan()
	err = dsp.planTableReaders(
		planCtx,
		p,
		&tableReaderPlanningInfo{
			spec:              spec,
			post:              post,
			desc:              n.desc,
			spans:             n.spans,
			reverse:           n.reverse,
			parallelize:       n.parallelize,
			estimatedRowCount: n.estimatedRowCount,
			reqOrdering:       n.reqOrdering,
		},
	)
	return p, err
}

// tableReaderPlanningInfo is a utility struct that contains the information
// needed to perform the physical planning of table readers once the specs have
// been created. See scanNode to get more context on some of the fields.
type tableReaderPlanningInfo struct {
	spec              *execinfrapb.TableReaderSpec
	post              execinfrapb.PostProcessSpec
	desc              catalog.TableDescriptor
	spans             []roachpb.Span
	reverse           bool
	parallelize       bool
	estimatedRowCount uint64
	reqOrdering       ReqOrdering
}

const defaultLocalScansConcurrencyLimit = 1024

// localScansConcurrencyLimit determines the number of additional goroutines
// that can be used to run parallel TableReaders when the plans are local. By
// "additional" we mean having more processors than one in the same stage of the
// physical plan.
var localScansConcurrencyLimit = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.local_scans.concurrency_limit",
	"maximum number of additional goroutines for performing scans in local plans",
	defaultLocalScansConcurrencyLimit,
	settings.NonNegativeInt,
)

// maybeParallelizeLocalScans check whether we are planning such a TableReader
// for the local flow that would benefit (and is safe) to parallelize.
func (dsp *DistSQLPlanner) maybeParallelizeLocalScans(
	planCtx *PlanningCtx, info *tableReaderPlanningInfo,
) (spanPartitions []SpanPartition, parallelizeLocal bool) {
	// For local plans, if:
	// - there is no required ordering,
	// - the scan is safe to parallelize, and
	// - the parallelization of scans in local flows is allowed,
	// - there is still quota for running more parallel local TableReaders,
	// then we will split all spans according to the leaseholder boundaries and
	// will create a separate TableReader for each node.
	sd := planCtx.ExtendedEvalCtx.EvalContext.SessionData()
	// If we have locality optimized search enabled and we won't use the
	// vectorized engine, using the parallel scans might actually be
	// significantly worse, so we prohibit it. This is the case because if we
	// have a local region hit, we would still execute all lookups into the
	// remote regions and would block until all come back in the row-based flow.
	prohibitParallelScans := sd.LocalityOptimizedSearch && sd.VectorizeMode == sessiondatapb.VectorizeOff
	if len(info.reqOrdering) == 0 &&
		info.parallelize &&
		planCtx.parallelizeScansIfLocal &&
		!prohibitParallelScans &&
		dsp.parallelLocalScansSem.ApproximateQuota() > 0 &&
		planCtx.spanIter != nil { // This condition can only be false in tests.
		parallelizeLocal = true
		// Temporarily unset isLocal so that PartitionSpans divides all spans
		// according to the respective leaseholders.
		planCtx.isLocal = false
		var err error
		spanPartitions, err = dsp.PartitionSpans(planCtx, info.spans)
		planCtx.isLocal = true
		if err != nil {
			// For some reason we couldn't partition the spans - fallback to
			// having a single TableReader.
			spanPartitions = []SpanPartition{{dsp.gatewaySQLInstanceID, info.spans}}
			parallelizeLocal = false
			return spanPartitions, parallelizeLocal
		}
		for i := range spanPartitions {
			spanPartitions[i].SQLInstanceID = dsp.gatewaySQLInstanceID
		}
		if len(spanPartitions) > 1 {
			// We're touching ranges that have leaseholders on multiple nodes,
			// so it'd be beneficial to parallelize such a scan.
			//
			// Determine the desired concurrency. The concurrency is limited by
			// the number of partitions as well as maxConcurrency constant (the
			// upper bound). We then try acquiring the quota for all additional
			// goroutines, and if the quota isn't available, we reduce the
			// proposed concurrency by 1. If in the end we didn't manage to
			// acquire the quota even for a single additional goroutine, we
			// won't have parallel TableReaders.
			const maxConcurrency = 64
			actualConcurrency := len(spanPartitions)
			if actualConcurrency > maxConcurrency {
				actualConcurrency = maxConcurrency
			}
			if quota := int(dsp.parallelLocalScansSem.ApproximateQuota()); actualConcurrency > quota {
				actualConcurrency = quota
			}
			// alloc will be non-nil only if actualConcurrency remains above 1.
			var alloc *quotapool.IntAlloc
			for actualConcurrency > 1 {
				alloc, err = dsp.parallelLocalScansSem.TryAcquire(planCtx.ctx, uint64(actualConcurrency-1))
				if err == nil {
					break
				}
				actualConcurrency--
			}

			if actualConcurrency > 1 {
				// We will have at least two concurrent TableReaders.
				//
				// Now we might need to merge some span partitions together. We
				// will keep first actualConcurrency partitions and will append
				// into them all "extra" span partitions in an alternating
				// fashion.
				for extraPartitionIdx := actualConcurrency; extraPartitionIdx < len(spanPartitions); extraPartitionIdx++ {
					mergeIntoIdx := extraPartitionIdx % actualConcurrency
					spanPartitions[mergeIntoIdx].Spans = append(spanPartitions[mergeIntoIdx].Spans, spanPartitions[extraPartitionIdx].Spans...)
				}
				spanPartitions = spanPartitions[:actualConcurrency]
				planCtx.onFlowCleanup = append(planCtx.onFlowCleanup, alloc.Release)
			} else {
				// We weren't able to acquire the quota for any additional
				// goroutines, so we will fallback to having a single
				// TableReader.
				spanPartitions = []SpanPartition{{dsp.gatewaySQLInstanceID, info.spans}}
			}
		}
		if len(spanPartitions) == 1 {
			// If all spans are assigned to a single partition, then there will
			// be no parallelism, so we want to elide the redundant
			// synchronizer.
			parallelizeLocal = false
		}
	} else {
		spanPartitions = []SpanPartition{{dsp.gatewaySQLInstanceID, info.spans}}
	}
	return spanPartitions, parallelizeLocal
}

func (dsp *DistSQLPlanner) planTableReaders(
	planCtx *PlanningCtx, p *PhysicalPlan, info *tableReaderPlanningInfo,
) error {
	var (
		spanPartitions   []SpanPartition
		parallelizeLocal bool
		err              error
	)
	if planCtx.isLocal {
		spanPartitions, parallelizeLocal = dsp.maybeParallelizeLocalScans(planCtx, info)
	} else if info.post.Limit == 0 {
		// No hard limit - plan all table readers where their data live. Note
		// that we're ignoring soft limits for now since the TableReader will
		// still read too eagerly in the soft limit case. To prevent this we'll
		// need a new mechanism on the execution side to modulate table reads.
		// TODO(yuzefovich): add that mechanism.
		spanPartitions, err = dsp.PartitionSpans(planCtx, info.spans)
		if err != nil {
			return err
		}
	} else {
		// If the scan has a hard limit, use a single TableReader to avoid
		// reading more rows than necessary.
		sqlInstanceID, err := dsp.getInstanceIDForScan(planCtx, info.spans, info.reverse)
		if err != nil {
			return err
		}
		spanPartitions = []SpanPartition{{sqlInstanceID, info.spans}}
	}

	corePlacement := make([]physicalplan.ProcessorCorePlacement, len(spanPartitions))
	for i, sp := range spanPartitions {
		var tr *execinfrapb.TableReaderSpec
		if i == 0 {
			// For the first span partition, we can just directly use the spec we made
			// above.
			tr = info.spec
		} else {
			// For the rest, we have to copy the spec into a fresh spec.
			tr = physicalplan.NewTableReaderSpec()
			*tr = *info.spec
		}
		// TODO(yuzefovich): figure out how we could reuse the Spans slice if we
		// kept the reference to it in TableReaderSpec (rather than allocating
		// new slices in generateScanSpans and PartitionSpans).
		tr.Spans = sp.Spans

		tr.Parallelize = info.parallelize
		if !tr.Parallelize {
			tr.BatchBytesLimit = dsp.distSQLSrv.TestingKnobs.TableReaderBatchBytesLimit
		}
		p.TotalEstimatedScannedRows += info.estimatedRowCount

		corePlacement[i].SQLInstanceID = sp.SQLInstanceID
		corePlacement[i].EstimatedRowCount = info.estimatedRowCount
		corePlacement[i].Core.TableReader = tr
	}

	typs := make([]*types.T, len(info.spec.FetchSpec.FetchedColumns))
	for i := range typs {
		typs[i] = info.spec.FetchSpec.FetchedColumns[i].Type
	}

	// Note: we will set a merge ordering below.
	p.AddNoInputStage(corePlacement, info.post, typs, execinfrapb.Ordering{})

	p.PlanToStreamColMap = identityMap(make([]int, len(typs)), len(typs))
	p.SetMergeOrdering(dsp.convertOrdering(info.reqOrdering, p.PlanToStreamColMap))

	if parallelizeLocal {
		// If we planned multiple table readers, we need to merge the streams
		// into one.
		p.AddSingleGroupStage(dsp.gatewaySQLInstanceID, execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}}, execinfrapb.PostProcessSpec{}, p.GetResultTypes())
	}

	return nil
}

// createPlanForRender takes a PhysicalPlan and updates it to produce results
// corresponding to the render node. An evaluator stage is added if the render
// node has any expressions which are not just simple column references.
func (dsp *DistSQLPlanner) createPlanForRender(
	p *PhysicalPlan, n *renderNode, planCtx *PlanningCtx,
) error {
	typs, err := getTypesForPlanResult(n, nil /* planToStreamColMap */)
	if err != nil {
		return err
	}
	if n.serialize {
		// We need to serialize the physical plan by forcing all streams to be
		// merged into one on the gateway node. However, it is beneficial to
		// apply the rendering (or the projection) before merging streams in
		// order to not send unnecessary data across the network. This
		// optimization is possible if we have an empty merge ordering on the
		// plan.
		deferSerialization := len(p.MergeOrdering.Columns) == 0
		if deferSerialization {
			defer p.EnsureSingleStreamOnGateway()
		} else {
			p.EnsureSingleStreamOnGateway()
		}
	}
	newColMap := identityMap(p.PlanToStreamColMap, len(n.render))
	newMergeOrdering := dsp.convertOrdering(n.reqOrdering, newColMap)
	err = p.AddRendering(
		n.render, planCtx, p.PlanToStreamColMap, typs, newMergeOrdering,
	)
	if err != nil {
		return err
	}
	p.PlanToStreamColMap = newColMap
	return nil
}

// addSorters adds sorters corresponding to the ordering and updates the plan
// accordingly. When alreadyOrderedPrefix is non-zero, the input is already
// ordered on the prefix ordering[:alreadyOrderedPrefix].
func (dsp *DistSQLPlanner) addSorters(
	p *PhysicalPlan, ordering colinfo.ColumnOrdering, alreadyOrderedPrefix int, limit int64,
) {
	// Sorting is needed; we add a stage of sorting processors.
	outputOrdering := execinfrapb.ConvertToMappedSpecOrdering(ordering, p.PlanToStreamColMap)

	p.AddNoGroupingStage(
		execinfrapb.ProcessorCoreUnion{
			Sorter: &execinfrapb.SorterSpec{
				OutputOrdering:   outputOrdering,
				OrderingMatchLen: uint32(alreadyOrderedPrefix),
				Limit:            limit,
			},
		},
		execinfrapb.PostProcessSpec{},
		p.GetResultTypes(),
		outputOrdering,
	)

	// Add a node to limit the number of output rows to the top K if a limit is
	// required and there are multiple routers.
	if limit > 0 && len(p.ResultRouters) > 1 {
		post := execinfrapb.PostProcessSpec{
			Limit: uint64(limit),
		}
		p.AddSingleGroupStage(
			p.GatewaySQLInstanceID,
			execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
			post,
			p.GetResultTypes(),
		)
	}
}

// aggregatorPlanningInfo is a utility struct that contains the information
// needed to perform the physical planning of aggregators once the specs have
// been created.
type aggregatorPlanningInfo struct {
	aggregations             []execinfrapb.AggregatorSpec_Aggregation
	argumentsColumnTypes     [][]*types.T
	isScalar                 bool
	groupCols                []int
	groupColOrdering         colinfo.ColumnOrdering
	inputMergeOrdering       execinfrapb.Ordering
	reqOrdering              ReqOrdering
	allowPartialDistribution bool
}

// addAggregators adds aggregators corresponding to a groupNode and updates the plan to
// reflect the groupNode.
func (dsp *DistSQLPlanner) addAggregators(
	planCtx *PlanningCtx, p *PhysicalPlan, n *groupNode,
) error {
	aggregations := make([]execinfrapb.AggregatorSpec_Aggregation, len(n.funcs))
	argumentsColumnTypes := make([][]*types.T, len(n.funcs))
	for i, fholder := range n.funcs {
		funcIdx, err := execinfrapb.GetAggregateFuncIdx(fholder.funcName)
		if err != nil {
			return err
		}
		aggregations[i].Func = execinfrapb.AggregatorSpec_Func(funcIdx)
		aggregations[i].Distinct = fholder.isDistinct
		for _, renderIdx := range fholder.argRenderIdxs {
			aggregations[i].ColIdx = append(aggregations[i].ColIdx, uint32(p.PlanToStreamColMap[renderIdx]))
		}
		if fholder.hasFilter() {
			col := uint32(p.PlanToStreamColMap[fholder.filterRenderIdx])
			aggregations[i].FilterColIdx = &col
		}
		aggregations[i].Arguments = make([]execinfrapb.Expression, len(fholder.arguments))
		argumentsColumnTypes[i] = make([]*types.T, len(fholder.arguments))
		for j, argument := range fholder.arguments {
			var err error
			aggregations[i].Arguments[j], err = physicalplan.MakeExpression(argument, planCtx, nil)
			if err != nil {
				return err
			}
			argumentsColumnTypes[i][j] = argument.ResolvedType()
		}
	}

	return dsp.planAggregators(planCtx, p, &aggregatorPlanningInfo{
		aggregations:         aggregations,
		argumentsColumnTypes: argumentsColumnTypes,
		isScalar:             n.isScalar,
		groupCols:            n.groupCols,
		groupColOrdering:     n.groupColOrdering,
		inputMergeOrdering:   dsp.convertOrdering(planReqOrdering(n.plan), p.PlanToStreamColMap),
		reqOrdering:          n.reqOrdering,
	})
}

// planAggregators plans the aggregator processors. An evaluator stage is added
// if necessary.
// Invariants assumed:
//  - There is strictly no "pre-evaluation" necessary. If the given query is
//  'SELECT COUNT(k), v + w FROM kv GROUP BY v + w', the evaluation of the first
//  'v + w' is done at the source of the groupNode.
//  - We only operate on the following expressions:
//      - ONLY aggregation functions, with arguments pre-evaluated. So for
//        COUNT(k + v), we assume a stream of evaluated 'k + v' values.
//      - Expressions that CONTAIN an aggregation function, e.g. 'COUNT(k) + 1'.
//        These are set as render expressions in the post-processing spec and
//        are evaluated on the rows that the aggregator returns.
//      - Expressions that also appear verbatim in the GROUP BY expressions.
//        For 'SELECT k GROUP BY k', the aggregation function added is IDENT,
//        therefore k just passes through unchanged.
//    All other expressions simply pass through unchanged, for e.g. '1' in
//    'SELECT 1 GROUP BY k'.
func (dsp *DistSQLPlanner) planAggregators(
	planCtx *PlanningCtx, p *PhysicalPlan, info *aggregatorPlanningInfo,
) error {
	aggType := execinfrapb.AggregatorSpec_NON_SCALAR
	if info.isScalar {
		aggType = execinfrapb.AggregatorSpec_SCALAR
	}

	inputTypes := p.GetResultTypes()

	groupCols := make([]uint32, len(info.groupCols))
	for i, idx := range info.groupCols {
		groupCols[i] = uint32(p.PlanToStreamColMap[idx])
	}
	orderedGroupCols := make([]uint32, len(info.groupColOrdering))
	var orderedGroupColSet util.FastIntSet
	for i, c := range info.groupColOrdering {
		orderedGroupCols[i] = uint32(p.PlanToStreamColMap[c.ColIdx])
		orderedGroupColSet.Add(c.ColIdx)
	}

	// We can have a local stage of distinct processors if all aggregation
	// functions are distinct.
	allDistinct := true
	for _, e := range info.aggregations {
		if !e.Distinct {
			allDistinct = false
			break
		}
	}
	if allDistinct {
		var distinctColumnsSet util.FastIntSet
		for _, e := range info.aggregations {
			for _, colIdx := range e.ColIdx {
				distinctColumnsSet.Add(int(colIdx))
			}
		}
		if distinctColumnsSet.Len() > 0 {
			// We only need to plan distinct processors if we have non-empty
			// set of argument columns.
			distinctColumns := make([]uint32, 0, distinctColumnsSet.Len())
			distinctColumnsSet.ForEach(func(i int) {
				distinctColumns = append(distinctColumns, uint32(i))
			})
			ordering := info.inputMergeOrdering.Columns
			orderedColumns := make([]uint32, 0, len(ordering))
			for _, ord := range ordering {
				if distinctColumnsSet.Contains(int(ord.ColIdx)) {
					// Ordered columns must be a subset of distinct columns, so
					// we only include such into orderedColumns slice.
					orderedColumns = append(orderedColumns, ord.ColIdx)
				}
			}
			sort.Slice(orderedColumns, func(i, j int) bool { return orderedColumns[i] < orderedColumns[j] })
			sort.Slice(distinctColumns, func(i, j int) bool { return distinctColumns[i] < distinctColumns[j] })
			distinctSpec := execinfrapb.ProcessorCoreUnion{
				Distinct: dsp.createDistinctSpec(
					distinctColumns,
					orderedColumns,
					false, /* nullsAreDistinct */
					"",    /* errorOnDup */
					p.MergeOrdering,
				),
			}
			// Add distinct processors local to each existing current result
			// processor.
			p.AddNoGroupingStage(distinctSpec, execinfrapb.PostProcessSpec{}, inputTypes, p.MergeOrdering)
		}
	}

	// Check if the previous stage is all on one node.
	prevStageNode := p.Processors[p.ResultRouters[0]].SQLInstanceID
	for i := 1; i < len(p.ResultRouters); i++ {
		if n := p.Processors[p.ResultRouters[i]].SQLInstanceID; n != prevStageNode {
			prevStageNode = 0
			break
		}
	}

	// We either have a local stage on each stream followed by a final stage, or
	// just a final stage. We only use a local stage if:
	//  - the previous stage is distributed on multiple nodes, and
	//  - all aggregation functions support it, and
	//  - no function is performing distinct aggregation.
	//  TODO(radu): we could relax this by splitting the aggregation into two
	//  different paths and joining on the results.
	multiStage := prevStageNode == 0
	if multiStage {
		for _, e := range info.aggregations {
			if e.Distinct {
				multiStage = false
				break
			}
			// Check that the function supports a local stage.
			if _, ok := physicalplan.DistAggregationTable[e.Func]; !ok {
				multiStage = false
				break
			}
		}
	}

	var finalAggsSpec execinfrapb.AggregatorSpec
	var finalAggsPost execinfrapb.PostProcessSpec

	// Note that we pass in nil as the second argument because we will have a
	// simple 1-to-1 PlanToStreamColMap in the end.
	finalOutputOrdering := dsp.convertOrdering(info.reqOrdering, nil /* planToStreamColMap */)

	if !multiStage {
		finalAggsSpec = execinfrapb.AggregatorSpec{
			Type:             aggType,
			Aggregations:     info.aggregations,
			GroupCols:        groupCols,
			OrderedGroupCols: orderedGroupCols,
			OutputOrdering:   finalOutputOrdering,
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
		for _, e := range info.aggregations {
			info := physicalplan.DistAggregationTable[e.Func]
			nLocalAgg += len(info.LocalStage)
			nFinalAgg += len(info.FinalStage)
			if info.FinalRendering != nil {
				needRender = true
			}
		}

		// We alloc the maximum possible number of unique local and final
		// aggregations but do not initialize any aggregations
		// since we can de-duplicate equivalent local and final aggregations.
		localAggs := make([]execinfrapb.AggregatorSpec_Aggregation, 0, nLocalAgg+len(groupCols))
		intermediateTypes := make([]*types.T, 0, nLocalAgg+len(groupCols))
		finalAggs := make([]execinfrapb.AggregatorSpec_Aggregation, 0, nFinalAgg)
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
		for _, e := range info.aggregations {
			info := physicalplan.DistAggregationTable[e.Func]

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
				localAgg := execinfrapb.AggregatorSpec_Aggregation{
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
					argTypes := make([]*types.T, len(e.ColIdx))
					for j, c := range e.ColIdx {
						argTypes[j] = inputTypes[c]
					}
					_, outputType, err := execinfrapb.GetAggregateInfo(localFunc, argTypes...)
					if err != nil {
						return err
					}
					intermediateTypes = append(intermediateTypes, outputType)
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
				finalAgg := execinfrapb.AggregatorSpec_Aggregation{
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
						argTypes := make([]*types.T, len(finalInfo.LocalIdxs))
						for i := range finalInfo.LocalIdxs {
							// Map the corresponding local
							// aggregation output types for
							// the current aggregation e.
							argTypes[i] = intermediateTypes[argIdxs[i]]
						}
						_, outputType, err := execinfrapb.GetAggregateInfo(finalInfo.Fn, argTypes...)
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
			agg := execinfrapb.AggregatorSpec_Aggregation{
				Func:   execinfrapb.AnyNotNull,
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
			if orderedGroupColSet.Contains(info.groupCols[i]) {
				finalOrderedGroupCols = append(finalOrderedGroupCols, uint32(idx))
			}
		}

		// Create the merge ordering for the local stage (this will be maintained
		// for results going into the final stage).
		ordCols := make([]execinfrapb.Ordering_Column, len(info.groupColOrdering))
		for i, o := range info.groupColOrdering {
			// Find the group column.
			found := false
			for j, col := range info.groupCols {
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
				ordCols[i].Direction = execinfrapb.Ordering_Column_DESC
			} else {
				ordCols[i].Direction = execinfrapb.Ordering_Column_ASC
			}
		}

		localAggsSpec := execinfrapb.AggregatorSpec{
			Type:             aggType,
			Aggregations:     localAggs,
			GroupCols:        groupCols,
			OrderedGroupCols: orderedGroupCols,
			OutputOrdering:   execinfrapb.Ordering{Columns: ordCols},
		}

		p.AddNoGroupingStage(
			execinfrapb.ProcessorCoreUnion{Aggregator: &localAggsSpec},
			execinfrapb.PostProcessSpec{},
			intermediateTypes,
			execinfrapb.Ordering{Columns: ordCols},
		)

		finalAggsSpec = execinfrapb.AggregatorSpec{
			Type:             aggType,
			Aggregations:     finalAggs,
			GroupCols:        finalGroupCols,
			OrderedGroupCols: finalOrderedGroupCols,
			OutputOrdering:   finalOutputOrdering,
		}

		if needRender {
			// Build rendering expressions.
			renderExprs := make([]execinfrapb.Expression, len(info.aggregations))
			h := tree.MakeTypesOnlyIndexedVarHelper(finalPreRenderTypes)
			// finalIdx is an index inside finalAggs. It is used to
			// keep track of the finalAggs results that correspond
			// to each aggregation.
			finalIdx := 0
			for i, e := range info.aggregations {
				info := physicalplan.DistAggregationTable[e.Func]
				if info.FinalRendering == nil {
					// mappedIdx corresponds to the index
					// location of the result for this
					// final aggregation in finalAggs. This
					// is necessary since we re-use final
					// aggregations if they are equivalent
					// across and within stages.
					mappedIdx := int(finalIdxMap[finalIdx])
					var err error
					renderExprs[i], err = physicalplan.MakeExpression(
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
					renderExprs[i], err = physicalplan.MakeExpression(
						expr, planCtx,
						nil /* indexVarMap */)
					if err != nil {
						return err
					}
				}
				finalIdx += len(info.FinalStage)
			}
			finalAggsPost.RenderExprs = renderExprs
		} else if len(finalAggs) < len(info.aggregations) {
			// We have removed some duplicates, so we need to add a projection.
			finalAggsPost.Projection = true
			finalAggsPost.OutputColumns = finalIdxMap
		}
	}

	// Set up the final stage.

	finalOutTypes := make([]*types.T, len(info.aggregations))
	for i, agg := range info.aggregations {
		argTypes := make([]*types.T, len(agg.ColIdx)+len(agg.Arguments))
		for j, c := range agg.ColIdx {
			argTypes[j] = inputTypes[c]
		}
		copy(argTypes[len(agg.ColIdx):], info.argumentsColumnTypes[i])
		var err error
		_, returnTyp, err := execinfrapb.GetAggregateInfo(agg.Func, argTypes...)
		if err != nil {
			return err
		}
		finalOutTypes[i] = returnTyp
	}

	// Update p.PlanToStreamColMap; we will have a simple 1-to-1 mapping of
	// planNode columns to stream columns because the aggregator
	// has been programmed to produce the same columns as the groupNode.
	p.PlanToStreamColMap = identityMap(p.PlanToStreamColMap, len(info.aggregations))

	if len(finalAggsSpec.GroupCols) == 0 || len(p.ResultRouters) == 1 {
		// No GROUP BY, or we have a single stream. Use a single final aggregator.
		// If the previous stage was all on a single node, put the final
		// aggregator there. Otherwise, bring the results back on this node.
		node := dsp.gatewaySQLInstanceID
		if prevStageNode != 0 {
			node = prevStageNode
		}
		p.AddSingleGroupStage(
			node,
			execinfrapb.ProcessorCoreUnion{Aggregator: &finalAggsSpec},
			finalAggsPost,
			finalOutTypes,
		)
	} else {
		// We distribute (by group columns) to multiple processors.

		// Set up the output routers from the previous stage.
		for _, resultProc := range p.ResultRouters {
			p.Processors[resultProc].Spec.Output[0] = execinfrapb.OutputRouterSpec{
				Type:        execinfrapb.OutputRouterSpec_BY_HASH,
				HashColumns: finalAggsSpec.GroupCols,
			}
		}

		// We have multiple streams, so we definitely have a processor planned
		// on a remote node.
		stageID := p.NewStage(true /* containsRemoteProcessor */, info.allowPartialDistribution)

		// We have one final stage processor for each result router. This is a
		// somewhat arbitrary decision; we could have a different number of nodes
		// working on the final stage.
		pIdxStart := physicalplan.ProcessorIdx(len(p.Processors))
		prevStageResultTypes := p.GetResultTypes()
		for _, resultProc := range p.ResultRouters {
			proc := physicalplan.Processor{
				SQLInstanceID: p.Processors[resultProc].SQLInstanceID,
				Spec: execinfrapb.ProcessorSpec{
					Input: []execinfrapb.InputSyncSpec{{
						// The other fields will be filled in by mergeResultStreams.
						ColumnTypes: prevStageResultTypes,
					}},
					Core: execinfrapb.ProcessorCoreUnion{Aggregator: &finalAggsSpec},
					Post: finalAggsPost,
					Output: []execinfrapb.OutputRouterSpec{{
						Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
					}},
					StageID:     stageID,
					ResultTypes: finalOutTypes,
				},
			}
			p.AddProcessor(proc)
		}

		// Connect the streams.
		for bucket := 0; bucket < len(p.ResultRouters); bucket++ {
			pIdx := pIdxStart + physicalplan.ProcessorIdx(bucket)
			p.MergeResultStreams(p.ResultRouters, bucket, p.MergeOrdering, pIdx, 0, false /* forceSerialization */)
		}

		// Set the new result routers.
		for i := 0; i < len(p.ResultRouters); i++ {
			p.ResultRouters[i] = pIdxStart + physicalplan.ProcessorIdx(i)
		}

		p.SetMergeOrdering(dsp.convertOrdering(info.reqOrdering, p.PlanToStreamColMap))
	}

	return nil
}

func (dsp *DistSQLPlanner) createPlanForIndexJoin(
	planCtx *PlanningCtx, n *indexJoinNode,
) (*PhysicalPlan, error) {
	plan, err := dsp.createPhysPlanForPlanNode(planCtx, n.input)
	if err != nil {
		return nil, err
	}

	// In "index-join mode", the join reader assumes that the PK cols are a prefix
	// of the input stream columns (see #40749). We need a projection to make that
	// happen. The other columns are not used by the join reader.
	pkCols := make([]uint32, len(n.keyCols))
	for i := range n.keyCols {
		streamColOrd := plan.PlanToStreamColMap[n.keyCols[i]]
		if streamColOrd == -1 {
			panic("key column not in planToStreamColMap")
		}
		pkCols[i] = uint32(streamColOrd)
	}
	// Note that we're using an empty merge ordering because we know for sure
	// that we won't join streams before the next stage: below, we either call
	// - AddNoGroupingStage, which doesn't join the streams, if we have multiple
	//   streams, or
	// - AddSingleGroupStage, if we have a single stream.
	// The former does set the correct new merge ordering after the index join
	// planning is done.
	plan.AddProjection(pkCols, execinfrapb.Ordering{})

	joinReaderSpec := execinfrapb.JoinReaderSpec{
		Type:              descpb.InnerJoin,
		LockingStrength:   n.table.lockingStrength,
		LockingWaitPolicy: n.table.lockingWaitPolicy,
		MaintainOrdering:  len(n.reqOrdering) > 0,
	}

	fetchColIDs := make([]descpb.ColumnID, len(n.cols))
	var fetchOrdinals util.FastIntSet
	for i := range n.cols {
		fetchColIDs[i] = n.cols[i].GetID()
		fetchOrdinals.Add(n.cols[i].Ordinal())
	}
	index := n.table.desc.GetPrimaryIndex()
	if err := rowenc.InitIndexFetchSpec(
		&joinReaderSpec.FetchSpec,
		planCtx.ExtendedEvalCtx.Codec,
		n.table.desc,
		index,
		fetchColIDs,
	); err != nil {
		return nil, err
	}

	splitter := span.MakeSplitter(n.table.desc, index, fetchOrdinals)
	joinReaderSpec.SplitFamilyIDs = splitter.FamilyIDs()

	plan.PlanToStreamColMap = identityMap(plan.PlanToStreamColMap, len(fetchColIDs))

	types, err := getTypesForPlanResult(n, plan.PlanToStreamColMap)
	if err != nil {
		return nil, err
	}
	if len(plan.ResultRouters) > 1 {
		// Instantiate one join reader for every stream.
		plan.AddNoGroupingStage(
			execinfrapb.ProcessorCoreUnion{JoinReader: &joinReaderSpec},
			execinfrapb.PostProcessSpec{},
			types,
			dsp.convertOrdering(n.reqOrdering, plan.PlanToStreamColMap),
		)
	} else {
		// We have a single stream, so use a single join reader on that node.
		plan.AddSingleGroupStage(
			plan.Processors[plan.ResultRouters[0]].SQLInstanceID,
			execinfrapb.ProcessorCoreUnion{JoinReader: &joinReaderSpec},
			execinfrapb.PostProcessSpec{},
			types,
		)
	}
	return plan, nil
}

// createPlanForLookupJoin creates a distributed plan for a lookupJoinNode.
func (dsp *DistSQLPlanner) createPlanForLookupJoin(
	planCtx *PlanningCtx, n *lookupJoinNode,
) (*PhysicalPlan, error) {
	plan, err := dsp.createPhysPlanForPlanNode(planCtx, n.input)
	if err != nil {
		return nil, err
	}

	joinReaderSpec := execinfrapb.JoinReaderSpec{
		Type:              n.joinType,
		LockingStrength:   n.table.lockingStrength,
		LockingWaitPolicy: n.table.lockingWaitPolicy,
		// TODO(sumeer): specifying ordering here using isFirstJoinInPairedJoiner
		// is late in the sense that the cost of this has not been taken into
		// account. Make this decision earlier in CustomFuncs.GenerateLookupJoins.
		MaintainOrdering:                  len(n.reqOrdering) > 0 || n.isFirstJoinInPairedJoiner,
		LeftJoinWithPairedJoiner:          n.isSecondJoinInPairedJoiner,
		OutputGroupContinuationForLeftRow: n.isFirstJoinInPairedJoiner,
		LookupBatchBytesLimit:             dsp.distSQLSrv.TestingKnobs.JoinReaderBatchBytesLimit,
	}

	fetchColIDs := make([]descpb.ColumnID, len(n.table.cols))
	var fetchOrdinals util.FastIntSet
	for i := range n.table.cols {
		fetchColIDs[i] = n.table.cols[i].GetID()
		fetchOrdinals.Add(n.table.cols[i].Ordinal())
	}
	if err := rowenc.InitIndexFetchSpec(
		&joinReaderSpec.FetchSpec,
		planCtx.ExtendedEvalCtx.Codec,
		n.table.desc,
		n.table.index,
		fetchColIDs,
	); err != nil {
		return nil, err
	}

	splitter := span.MakeSplitter(n.table.desc, n.table.index, fetchOrdinals)
	joinReaderSpec.SplitFamilyIDs = splitter.FamilyIDs()

	joinReaderSpec.LookupColumns = make([]uint32, len(n.eqCols))
	for i, col := range n.eqCols {
		if plan.PlanToStreamColMap[col] == -1 {
			panic("lookup column not in planToStreamColMap")
		}
		joinReaderSpec.LookupColumns[i] = uint32(plan.PlanToStreamColMap[col])
	}
	joinReaderSpec.LookupColumnsAreKey = n.eqColsAreKey

	inputTypes := plan.GetResultTypes()
	fetchedColumns := joinReaderSpec.FetchSpec.FetchedColumns
	numOutCols := len(inputTypes) + len(fetchedColumns)
	if n.isFirstJoinInPairedJoiner {
		// We will add a continuation column.
		numOutCols++
	}

	var outTypes []*types.T
	var planToStreamColMap []int
	if !n.joinType.ShouldIncludeRightColsInOutput() {
		if n.isFirstJoinInPairedJoiner {
			return nil, errors.AssertionFailedf("continuation column without right columns")
		}
		outTypes = inputTypes
		planToStreamColMap = plan.PlanToStreamColMap
	} else {
		outTypes = make([]*types.T, numOutCols)
		copy(outTypes, inputTypes)
		planToStreamColMap = plan.PlanToStreamColMap
		for i := range fetchedColumns {
			outTypes[len(inputTypes)+i] = fetchedColumns[i].Type
			planToStreamColMap = append(planToStreamColMap, len(inputTypes)+i)
		}
		if n.isFirstJoinInPairedJoiner {
			outTypes[numOutCols-1] = types.Bool
			planToStreamColMap = append(planToStreamColMap, numOutCols-1)
		}
	}

	// Set the lookup condition.
	if n.lookupExpr != nil {
		var err error
		joinReaderSpec.LookupExpr, err = physicalplan.MakeExpression(
			n.lookupExpr, planCtx, nil, /* indexVarMap */
		)
		if err != nil {
			return nil, err
		}
	}
	if n.remoteLookupExpr != nil {
		if n.lookupExpr == nil {
			return nil, errors.AssertionFailedf("remoteLookupExpr is set but lookupExpr is not")
		}
		var err error
		joinReaderSpec.RemoteLookupExpr, err = physicalplan.MakeExpression(
			n.remoteLookupExpr, planCtx, nil, /* indexVarMap */
		)
		if err != nil {
			return nil, err
		}
	}

	// Set the ON condition.
	if n.onCond != nil {
		var err error
		joinReaderSpec.OnExpr, err = physicalplan.MakeExpression(
			n.onCond, planCtx, nil, /* indexVarMap */
		)
		if err != nil {
			return nil, err
		}
	}

	// Instantiate one join reader for every stream. This is also necessary for
	// correctness of paired-joins where this join is the second join -- it is
	// necessary to have a one-to-one relationship between the first and second
	// join processor.
	plan.AddNoGroupingStage(
		execinfrapb.ProcessorCoreUnion{JoinReader: &joinReaderSpec},
		execinfrapb.PostProcessSpec{},
		outTypes,
		dsp.convertOrdering(planReqOrdering(n), planToStreamColMap),
	)
	plan.PlanToStreamColMap = planToStreamColMap
	return plan, nil
}

// mappingHelperForLookupJoins creates slices etc. for the columns of
// lookup-style joins (that involve an input that is used to lookup from a
// table).
func mappingHelperForLookupJoins(
	plan *PhysicalPlan, input planNode, table *scanNode, addContinuationCol bool,
) (
	numInputNodeCols int,
	planToStreamColMap []int,
	post execinfrapb.PostProcessSpec,
	outTypes []*types.T,
) {
	// The n.table node can be configured with an arbitrary set of columns. Apply
	// the corresponding projection.
	// The internal schema of the join reader is:
	//    <input columns>... <table columns>...[continuation col]
	inputTypes := plan.GetResultTypes()
	numLeftCols := len(inputTypes)
	numOutCols := numLeftCols + len(table.cols)
	if addContinuationCol {
		numOutCols++
	}
	post = execinfrapb.PostProcessSpec{Projection: true}

	post.OutputColumns = make([]uint32, numOutCols)
	outTypes = make([]*types.T, numOutCols)

	for i := 0; i < numLeftCols; i++ {
		outTypes[i] = inputTypes[i]
		post.OutputColumns[i] = uint32(i)
	}
	for i := range table.cols {
		outTypes[numLeftCols+i] = table.cols[i].GetType()
		ord := tableOrdinal(table.desc, table.cols[i].GetID())
		post.OutputColumns[numLeftCols+i] = uint32(numLeftCols + ord)
	}
	if addContinuationCol {
		outTypes[numOutCols-1] = types.Bool
		post.OutputColumns[numOutCols-1] = uint32(numLeftCols + len(table.desc.DeletableColumns()))
	}

	// Map the columns of the lookupJoinNode to the result streams of the
	// JoinReader.
	numInputNodeCols = len(planColumns(input))
	lenPlanToStreamColMap := numInputNodeCols + len(table.cols)
	if addContinuationCol {
		lenPlanToStreamColMap++
	}
	planToStreamColMap = makePlanToStreamColMap(lenPlanToStreamColMap)
	copy(planToStreamColMap, plan.PlanToStreamColMap)
	for i := range table.cols {
		planToStreamColMap[numInputNodeCols+i] = numLeftCols + i
	}
	if addContinuationCol {
		planToStreamColMap[lenPlanToStreamColMap-1] = numLeftCols + len(table.cols)
	}
	return numInputNodeCols, planToStreamColMap, post, outTypes
}

func makeIndexVarMapForLookupJoins(
	numInputNodeCols int, table *scanNode, plan *PhysicalPlan, post *execinfrapb.PostProcessSpec,
) (indexVarMap []int) {
	// Note that (regardless of the join type or the OutputColumns projection)
	// the inverted expression and ON condition refers to the input columns with
	// var indexes 0 to numInputNodeCols-1 and to table columns with var indexes
	// starting from numInputNodeCols.
	indexVarMap = makePlanToStreamColMap(numInputNodeCols + len(table.cols))
	copy(indexVarMap, plan.PlanToStreamColMap)
	numLeftCols := len(plan.GetResultTypes())
	for i := range table.cols {
		indexVarMap[numInputNodeCols+i] = int(post.OutputColumns[numLeftCols+i])
	}
	return indexVarMap
}

func truncateToInputForLookupJoins(
	numInputNodeCols int, planToStreamColMap []int, outputColumns []uint32, outTypes []*types.T,
) ([]int, []uint32, []*types.T) {
	planToStreamColMap = planToStreamColMap[:numInputNodeCols]
	outputColumns = outputColumns[:numInputNodeCols]
	outTypes = outTypes[:numInputNodeCols]
	return planToStreamColMap, outputColumns, outTypes
}

func (dsp *DistSQLPlanner) createPlanForInvertedJoin(
	planCtx *PlanningCtx, n *invertedJoinNode,
) (*PhysicalPlan, error) {
	plan, err := dsp.createPhysPlanForPlanNode(planCtx, n.input)
	if err != nil {
		return nil, err
	}

	invertedJoinerSpec := execinfrapb.InvertedJoinerSpec{
		Table:                             *n.table.desc.TableDesc(),
		Type:                              n.joinType,
		MaintainOrdering:                  len(n.reqOrdering) > 0,
		OutputGroupContinuationForLeftRow: n.isFirstJoinInPairedJoiner,
	}
	invertedJoinerSpec.IndexIdx, err = getIndexIdx(n.table.index, n.table.desc)
	if err != nil {
		return nil, err
	}

	numInputNodeCols, planToStreamColMap, post, types :=
		mappingHelperForLookupJoins(plan, n.input, n.table, n.isFirstJoinInPairedJoiner)

	invertedJoinerSpec.PrefixEqualityColumns = make([]uint32, len(n.prefixEqCols))
	for i, col := range n.prefixEqCols {
		if plan.PlanToStreamColMap[col] == -1 {
			panic("lookup column not in planToStreamColMap")
		}
		invertedJoinerSpec.PrefixEqualityColumns[i] = uint32(plan.PlanToStreamColMap[col])
	}

	indexVarMap := makeIndexVarMapForLookupJoins(numInputNodeCols, n.table, plan, &post)
	if invertedJoinerSpec.InvertedExpr, err = physicalplan.MakeExpression(
		n.invertedExpr, planCtx, indexVarMap,
	); err != nil {
		return nil, err
	}
	// Set the ON condition.
	if n.onExpr != nil {
		if invertedJoinerSpec.OnExpr, err = physicalplan.MakeExpression(
			n.onExpr, planCtx, indexVarMap,
		); err != nil {
			return nil, err
		}
	}

	if !n.joinType.ShouldIncludeRightColsInOutput() {
		planToStreamColMap, post.OutputColumns, types = truncateToInputForLookupJoins(
			numInputNodeCols, planToStreamColMap, post.OutputColumns, types)
	}

	// Instantiate one inverted joiner for every stream.
	plan.AddNoGroupingStage(
		execinfrapb.ProcessorCoreUnion{InvertedJoiner: &invertedJoinerSpec},
		post,
		types,
		dsp.convertOrdering(planReqOrdering(n), planToStreamColMap),
	)
	plan.PlanToStreamColMap = planToStreamColMap
	return plan, nil
}

// createPlanForZigzagJoin creates a distributed plan for a zigzagJoinNode.
func (dsp *DistSQLPlanner) createPlanForZigzagJoin(
	planCtx *PlanningCtx, n *zigzagJoinNode,
) (plan *PhysicalPlan, err error) {

	sides := make([]zigzagPlanningSide, len(n.sides))

	for i, side := range n.sides {
		// The fixed values are represented as a Values node with one tuple.
		typs := getTypesFromResultColumns(side.fixedVals.columns)
		valuesSpec, err := dsp.createValuesSpecFromTuples(planCtx, side.fixedVals.tuples, typs)
		if err != nil {
			return nil, err
		}

		sides[i] = zigzagPlanningSide{
			desc:        side.scan.desc,
			index:       side.scan.index,
			cols:        side.scan.cols,
			eqCols:      side.eqCols,
			fixedValues: valuesSpec,
		}
	}

	return dsp.planZigzagJoin(planCtx, zigzagPlanningInfo{
		sides:       sides,
		columns:     n.columns,
		onCond:      n.onCond,
		reqOrdering: n.reqOrdering,
	})
}

type zigzagPlanningSide struct {
	desc        catalog.TableDescriptor
	index       catalog.Index
	cols        []catalog.Column
	eqCols      []int
	fixedValues *execinfrapb.ValuesCoreSpec
}

type zigzagPlanningInfo struct {
	sides       []zigzagPlanningSide
	columns     colinfo.ResultColumns
	onCond      tree.TypedExpr
	reqOrdering ReqOrdering
}

func (dsp *DistSQLPlanner) planZigzagJoin(
	planCtx *PlanningCtx, pi zigzagPlanningInfo,
) (plan *PhysicalPlan, err error) {

	plan = planCtx.NewPhysicalPlan()
	tables := make([]descpb.TableDescriptor, len(pi.sides))
	indexOrdinals := make([]uint32, len(pi.sides))
	cols := make([]execinfrapb.Columns, len(pi.sides))
	fixedValues := make([]*execinfrapb.ValuesCoreSpec, len(pi.sides))

	for i, side := range pi.sides {
		tables[i] = *side.desc.TableDesc()
		indexOrdinals[i], err = getIndexIdx(side.index, side.desc)
		if err != nil {
			return nil, err
		}

		cols[i].Columns = make([]uint32, len(side.eqCols))
		for j, col := range side.eqCols {
			cols[i].Columns[j] = uint32(col)
		}
		fixedValues[i] = side.fixedValues
	}

	// The zigzag join node only represents inner joins, so hardcode Type to
	// InnerJoin.
	zigzagJoinerSpec := execinfrapb.ZigzagJoinerSpec{
		Tables:        tables,
		EqColumns:     cols,
		IndexOrdinals: indexOrdinals,
		FixedValues:   fixedValues,
		Type:          descpb.InnerJoin,
	}

	// The internal schema of the zigzag joiner is:
	//    <side 1 table columns> ... <side 2 table columns> ...
	// with only the columns in the specified index populated.
	//
	// The schema of the zigzagJoinNode is:
	//    <side 1 index columns> ... <side 2 index columns> ...
	// so the planToStreamColMap has to basically map index ordinals
	// to table ordinals.
	post := execinfrapb.PostProcessSpec{Projection: true}
	numOutCols := len(pi.columns)
	post.OutputColumns = make([]uint32, numOutCols)
	types := make([]*types.T, numOutCols)
	planToStreamColMap := makePlanToStreamColMap(numOutCols)
	colOffset := 0
	i := 0

	// Populate post.OutputColumns (the implicit projection), result types,
	// and the planToStreamColMap for index columns from all sides.
	for _, side := range pi.sides {
		// Note that the side's scanNode only contains the columns from that
		// index that are also in n.columns. This is because we generated
		// colCfg.wantedColumns for only the necessary columns in
		// opt/exec/execbuilder/relational_builder.go, similar to lookup joins.
		for _, col := range side.cols {
			ord := tableOrdinal(side.desc, col.GetID())
			post.OutputColumns[i] = uint32(colOffset + ord)
			types[i] = col.GetType()
			planToStreamColMap[i] = i
			i++
		}
		colOffset += len(side.desc.PublicColumns())
	}

	// Set the ON condition.
	if pi.onCond != nil {
		// Note that the ON condition refers to the *internal* columns of the
		// processor (before the OutputColumns projection).
		indexVarMap := makePlanToStreamColMap(len(pi.columns))
		for i := 0; i < len(pi.columns); i++ {
			indexVarMap[i] = int(post.OutputColumns[i])
		}
		zigzagJoinerSpec.OnExpr, err = physicalplan.MakeExpression(
			pi.onCond, planCtx, indexVarMap,
		)
		if err != nil {
			return nil, err
		}
	}

	// Figure out the node where this zigzag joiner goes.
	//
	// TODO(itsbilal): Add support for restricting the Zigzag joiner to a
	// certain set of spans on one side. Once that's done, we can split this
	// processor across multiple nodes here. Until then, schedule on the current
	// node.
	corePlacement := []physicalplan.ProcessorCorePlacement{{
		SQLInstanceID: dsp.gatewaySQLInstanceID,
		Core:          execinfrapb.ProcessorCoreUnion{ZigzagJoiner: &zigzagJoinerSpec},
	}}

	plan.AddNoInputStage(corePlacement, post, types, execinfrapb.Ordering{})
	plan.PlanToStreamColMap = planToStreamColMap

	return plan, nil
}

func (dsp *DistSQLPlanner) createPlanForInvertedFilter(
	planCtx *PlanningCtx, n *invertedFilterNode,
) (*PhysicalPlan, error) {
	plan, err := dsp.createPhysPlanForPlanNode(planCtx, n.input)
	if err != nil {
		return nil, err
	}
	invertedFiltererSpec := &execinfrapb.InvertedFiltererSpec{
		InvertedColIdx: uint32(n.invColumn),
		InvertedExpr:   *n.expression.ToProto(),
	}
	if n.preFiltererExpr != nil {
		invertedFiltererSpec.PreFiltererSpec = &execinfrapb.InvertedFiltererSpec_PreFiltererSpec{
			Type: n.preFiltererType,
		}
		if invertedFiltererSpec.PreFiltererSpec.Expression, err = physicalplan.MakeExpression(
			n.preFiltererExpr, planCtx, nil); err != nil {
			return nil, err
		}
	}

	// Cases:
	// - Last stage is a single processor (local or remote): Place the inverted
	//   filterer on that last stage node. Due to the behavior of
	//   checkSupportForInvertedFilterNode, the remote case can only happen for
	//   a distributable filter.
	// - Last stage has multiple processors that are on different nodes: Filtering
	//   must be distributable. Place an inverted filterer on each node, which
	//   produces the primary keys in arbitrary order, and de-duplicate the PKs
	//   at the next stage.
	if len(plan.ResultRouters) == 1 {
		// Last stage is a single processor.
		lastSQLInstanceID := plan.Processors[plan.ResultRouters[0]].SQLInstanceID
		plan.AddSingleGroupStage(lastSQLInstanceID,
			execinfrapb.ProcessorCoreUnion{
				InvertedFilterer: invertedFiltererSpec,
			},
			execinfrapb.PostProcessSpec{}, plan.GetResultTypes())
		return plan, nil
	}
	// Must be distributable.
	distributable := n.expression.Left == nil && n.expression.Right == nil
	if !distributable {
		return nil, errors.Errorf("expected distributable inverted filterer")
	}
	reqOrdering := execinfrapb.Ordering{}
	// Instantiate one inverted filterer for every stream.
	plan.AddNoGroupingStage(
		execinfrapb.ProcessorCoreUnion{InvertedFilterer: invertedFiltererSpec},
		execinfrapb.PostProcessSpec{}, plan.GetResultTypes(), reqOrdering,
	)
	// De-duplicate the PKs. Note that the inverted filterer output includes
	// the inverted column always set to NULL, so we exclude it from the
	// distinct columns.
	distinctColumns := make([]uint32, 0, len(n.resultColumns)-1)
	for i := 0; i < len(n.resultColumns); i++ {
		if i == n.invColumn {
			continue
		}
		distinctColumns = append(distinctColumns, uint32(i))
	}
	plan.AddSingleGroupStage(
		dsp.gatewaySQLInstanceID,
		execinfrapb.ProcessorCoreUnion{
			Distinct: dsp.createDistinctSpec(
				distinctColumns,
				[]uint32{}, /* orderedColumns */
				false,      /* nullsAreDistinct */
				"",         /* errorOnDup */
				reqOrdering,
			),
		},
		execinfrapb.PostProcessSpec{},
		plan.GetResultTypes(),
	)
	return plan, nil
}

func getTypesFromResultColumns(cols colinfo.ResultColumns) []*types.T {
	typs := make([]*types.T, len(cols))
	for i, col := range cols {
		typs[i] = col.Typ
	}
	return typs
}

// getTypesForPlanResult returns the types of the elements in the result streams
// of a plan that corresponds to a given planNode. If planToStreamColMap is nil,
// a 1-1 mapping is assumed.
func getTypesForPlanResult(node planNode, planToStreamColMap []int) ([]*types.T, error) {
	nodeColumns := planColumns(node)
	if planToStreamColMap == nil {
		// No remapping.
		return getTypesFromResultColumns(nodeColumns), nil
	}
	numCols := 0
	for _, streamCol := range planToStreamColMap {
		if numCols <= streamCol {
			numCols = streamCol + 1
		}
	}
	types := make([]*types.T, numCols)
	for nodeCol, streamCol := range planToStreamColMap {
		if streamCol != -1 {
			types[streamCol] = nodeColumns[nodeCol].Typ
		}
	}
	return types, nil
}

func (dsp *DistSQLPlanner) createPlanForJoin(
	planCtx *PlanningCtx, n *joinNode,
) (*PhysicalPlan, error) {
	leftPlan, err := dsp.createPhysPlanForPlanNode(planCtx, n.left.plan)
	if err != nil {
		return nil, err
	}
	rightPlan, err := dsp.createPhysPlanForPlanNode(planCtx, n.right.plan)
	if err != nil {
		return nil, err
	}

	leftMap, rightMap := leftPlan.PlanToStreamColMap, rightPlan.PlanToStreamColMap
	helper := &joinPlanningHelper{
		numLeftOutCols:          n.pred.numLeftCols,
		numRightOutCols:         n.pred.numRightCols,
		numAllLeftCols:          len(leftPlan.GetResultTypes()),
		leftPlanToStreamColMap:  leftMap,
		rightPlanToStreamColMap: rightMap,
	}
	post, joinToStreamColMap := helper.joinOutColumns(n.pred.joinType, n.columns)
	onExpr, err := helper.remapOnExpr(planCtx, n.pred.onCond)
	if err != nil {
		return nil, err
	}

	// We initialize these properties of the joiner. They will then be used to
	// fill in the processor spec. See descriptions for HashJoinerSpec.
	// Set up the equality columns and the merge ordering.
	leftEqCols := eqCols(n.pred.leftEqualityIndices, leftMap)
	rightEqCols := eqCols(n.pred.rightEqualityIndices, rightMap)
	leftMergeOrd := distsqlOrdering(n.mergeJoinOrdering, leftEqCols)
	rightMergeOrd := distsqlOrdering(n.mergeJoinOrdering, rightEqCols)

	joinResultTypes, err := getTypesForPlanResult(n, joinToStreamColMap)
	if err != nil {
		return nil, err
	}

	info := joinPlanningInfo{
		leftPlan:           leftPlan,
		rightPlan:          rightPlan,
		joinType:           n.pred.joinType,
		joinResultTypes:    joinResultTypes,
		onExpr:             onExpr,
		post:               post,
		joinToStreamColMap: joinToStreamColMap,
		leftEqCols:         leftEqCols,
		rightEqCols:        rightEqCols,
		leftEqColsAreKey:   n.pred.leftEqKey,
		rightEqColsAreKey:  n.pred.rightEqKey,
		leftMergeOrd:       leftMergeOrd,
		rightMergeOrd:      rightMergeOrd,
		// In the old execFactory we can only have either local or fully
		// distributed plans, so checking the last stage is sufficient to get
		// the distribution of the whole plans.
		leftPlanDistribution:  leftPlan.GetLastStageDistribution(),
		rightPlanDistribution: rightPlan.GetLastStageDistribution(),
	}
	return dsp.planJoiners(planCtx, &info, n.reqOrdering), nil
}

func (dsp *DistSQLPlanner) planJoiners(
	planCtx *PlanningCtx, info *joinPlanningInfo, reqOrdering ReqOrdering,
) *PhysicalPlan {
	// Outline of the planning process for joins when given PhysicalPlans for
	// the left and right side (with each plan having a set of output routers
	// with result that will serve as input for the join).
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

	p := planCtx.NewPhysicalPlan()
	physicalplan.MergePlans(
		&p.PhysicalPlan, &info.leftPlan.PhysicalPlan, &info.rightPlan.PhysicalPlan,
		info.leftPlanDistribution, info.rightPlanDistribution, info.allowPartialDistribution,
	)
	leftRouters := info.leftPlan.ResultRouters
	rightRouters := info.rightPlan.ResultRouters

	// Instances where we will run the join processors.
	var sqlInstances []base.SQLInstanceID
	if numEq := len(info.leftEqCols); numEq != 0 {
		sqlInstances = findJoinProcessorNodes(leftRouters, rightRouters, p.Processors)
	} else {
		// Without column equality, we cannot distribute the join. Run a
		// single processor.
		sqlInstances = []base.SQLInstanceID{dsp.gatewaySQLInstanceID}

		// If either side has a single stream, put the processor on that node. We
		// prefer the left side because that is processed first by the hash joiner.
		if len(leftRouters) == 1 {
			sqlInstances[0] = p.Processors[leftRouters[0]].SQLInstanceID
		} else if len(rightRouters) == 1 {
			sqlInstances[0] = p.Processors[rightRouters[0]].SQLInstanceID
		}
	}

	p.AddJoinStage(
		sqlInstances, info.makeCoreSpec(), info.post,
		info.leftEqCols, info.rightEqCols,
		info.leftPlan.GetResultTypes(), info.rightPlan.GetResultTypes(),
		info.leftMergeOrd, info.rightMergeOrd,
		leftRouters, rightRouters, info.joinResultTypes,
	)

	p.PlanToStreamColMap = info.joinToStreamColMap

	// Joiners may guarantee an ordering to outputs, so we ensure that
	// ordering is propagated through the input synchronizer of the next stage.
	p.SetMergeOrdering(dsp.convertOrdering(reqOrdering, p.PlanToStreamColMap))
	return p
}

// createPhysPlan creates a PhysicalPlan as well as returns a non-nil cleanup
// function that must be called after the flow has been cleaned up.
func (dsp *DistSQLPlanner) createPhysPlan(
	planCtx *PlanningCtx, plan planMaybePhysical,
) (physPlan *PhysicalPlan, cleanup func(), err error) {
	if plan.isPhysicalPlan() {
		// TODO(yuzefovich): figure out how to propagate
		// planCtx.getCleanupFunc() from the experimental DistSQL spec factory.
		return plan.physPlan.PhysicalPlan, func() {}, nil
	}
	physPlan, err = dsp.createPhysPlanForPlanNode(planCtx, plan.planNode)
	return physPlan, planCtx.getCleanupFunc(), err
}

func (dsp *DistSQLPlanner) createPhysPlanForPlanNode(
	planCtx *PlanningCtx, node planNode,
) (plan *PhysicalPlan, err error) {
	planCtx.planDepth++

	switch n := node.(type) {
	// Keep these cases alphabetized, please!
	case *distinctNode:
		plan, err = dsp.createPlanForDistinct(planCtx, n)

	case *exportNode:
		plan, err = dsp.createPlanForExport(planCtx, n)

	case *filterNode:
		plan, err = dsp.createPhysPlanForPlanNode(planCtx, n.source.plan)
		if err != nil {
			return nil, err
		}

		if err := plan.AddFilter(n.filter, planCtx, plan.PlanToStreamColMap); err != nil {
			return nil, err
		}

	case *groupNode:
		plan, err = dsp.createPhysPlanForPlanNode(planCtx, n.plan)
		if err != nil {
			return nil, err
		}

		if err := dsp.addAggregators(planCtx, plan, n); err != nil {
			return nil, err
		}

	case *indexJoinNode:
		plan, err = dsp.createPlanForIndexJoin(planCtx, n)

	case *invertedFilterNode:
		plan, err = dsp.createPlanForInvertedFilter(planCtx, n)

	case *invertedJoinNode:
		plan, err = dsp.createPlanForInvertedJoin(planCtx, n)

	case *joinNode:
		plan, err = dsp.createPlanForJoin(planCtx, n)

	case *limitNode:
		plan, err = dsp.createPhysPlanForPlanNode(planCtx, n.plan)
		if err != nil {
			return nil, err
		}
		var count, offset int64
		if count, offset, err = evalLimit(planCtx.EvalContext(), n.countExpr, n.offsetExpr); err != nil {
			return nil, err
		}
		if err := plan.AddLimit(count, offset, planCtx); err != nil {
			return nil, err
		}

	case *lookupJoinNode:
		plan, err = dsp.createPlanForLookupJoin(planCtx, n)

	case *ordinalityNode:
		plan, err = dsp.createPlanForOrdinality(planCtx, n)

	case *projectSetNode:
		plan, err = dsp.createPlanForProjectSet(planCtx, n)

	case *renderNode:
		plan, err = dsp.createPhysPlanForPlanNode(planCtx, n.source.plan)
		if err != nil {
			return nil, err
		}
		err = dsp.createPlanForRender(plan, n, planCtx)
		if err != nil {
			return nil, err
		}

	case *scanNode:
		plan, err = dsp.createTableReaders(planCtx, n)

	case *sortNode:
		plan, err = dsp.createPhysPlanForPlanNode(planCtx, n.plan)
		if err != nil {
			return nil, err
		}

		dsp.addSorters(plan, n.ordering, n.alreadyOrderedPrefix, 0 /* limit */)

	case *topKNode:
		plan, err = dsp.createPhysPlanForPlanNode(planCtx, n.plan)
		if err != nil {
			return nil, err
		}

		if n.k <= 0 {
			return nil, errors.New("negative or zero value for LIMIT")
		}
		dsp.addSorters(plan, n.ordering, n.alreadyOrderedPrefix, n.k)

	case *unaryNode:
		plan, err = dsp.createPlanForUnary(planCtx, n)

	case *unionNode:
		plan, err = dsp.createPlanForSetOp(planCtx, n)

	case *valuesNode:
		if mustWrapValuesNode(planCtx, n.specifiedInQuery) {
			plan, err = dsp.wrapPlan(planCtx, n, false /* allowPartialDistribution */)
		} else {
			colTypes := getTypesFromResultColumns(n.columns)
			var spec *execinfrapb.ValuesCoreSpec
			spec, err = dsp.createValuesSpecFromTuples(planCtx, n.tuples, colTypes)
			if err != nil {
				return nil, err
			}
			plan, err = dsp.createValuesPlan(planCtx, spec, colTypes)
		}

	case *windowNode:
		plan, err = dsp.createPlanForWindow(planCtx, n)

	case *zeroNode:
		plan, err = dsp.createPlanForZero(planCtx, n)

	case *zigzagJoinNode:
		plan, err = dsp.createPlanForZigzagJoin(planCtx, n)

	case *createStatsNode:
		if n.runAsJob {
			plan, err = dsp.wrapPlan(planCtx, n, false /* allowPartialDistribution */)
		} else {
			// Create a job record but don't actually start the job.
			var record *jobs.Record
			record, err = n.makeJobRecord(planCtx.ctx)
			if err != nil {
				return nil, err
			}
			plan, err = dsp.createPlanForCreateStats(planCtx, 0, /* jobID */
				record.Details.(jobspb.CreateStatsDetails))
		}

	default:
		// Can't handle a node? We wrap it and continue on our way.
		plan, err = dsp.wrapPlan(planCtx, n, false /* allowPartialDistribution */)
	}

	if err != nil {
		return plan, err
	}

	if planCtx.traceMetadata != nil {
		processors := make(execComponents, len(plan.ResultRouters))
		for i, resultProcIdx := range plan.ResultRouters {
			processors[i] = execinfrapb.ProcessorComponentID(
				plan.Processors[resultProcIdx].SQLInstanceID,
				execinfrapb.FlowID{UUID: planCtx.infra.FlowID},
				int32(resultProcIdx),
			)
		}
		planCtx.traceMetadata.associateNodeWithComponents(node, processors)
	}

	if dsp.shouldPlanTestMetadata() {
		if err := plan.CheckLastStagePost(); err != nil {
			log.Fatalf(planCtx.ctx, "%v", err)
		}
		plan.AddNoGroupingStageWithCoreFunc(
			func(_ int, _ *physicalplan.Processor) execinfrapb.ProcessorCoreUnion {
				return execinfrapb.ProcessorCoreUnion{
					MetadataTestSender: &execinfrapb.MetadataTestSenderSpec{
						ID: uuid.MakeV4().String(),
					},
				}
			},
			execinfrapb.PostProcessSpec{},
			plan.GetResultTypes(),
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
func (dsp *DistSQLPlanner) wrapPlan(
	planCtx *PlanningCtx, n planNode, allowPartialDistribution bool,
) (*PhysicalPlan, error) {
	useFastPath := planCtx.planDepth == 1 && planCtx.stmtType == tree.RowsAffected

	// First, we search the planNode tree we're trying to wrap for the first
	// DistSQL-enabled planNode in the tree. If we find one, we ask the planner to
	// continue the DistSQL planning recursion on that planNode.
	seenTop := false
	nParents := uint32(0)
	p := planCtx.NewPhysicalPlan()
	// This will be set to first DistSQL-enabled planNode we find, if any. We'll
	// modify its parent later to connect its source to the DistSQL-planned
	// subtree.
	var firstNotWrapped planNode
	if err := walkPlan(planCtx.ctx, n, planObserver{
		enterNode: func(ctx context.Context, nodeName string, plan planNode) (bool, error) {
			switch plan.(type) {
			case *explainVecNode, *explainPlanNode, *explainDDLNode:
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
				p, err = dsp.createPhysPlanForPlanNode(planCtx, plan)
				if err != nil {
					return false, err
				}
				nParents++
				return false, nil
			}
			return true, nil
		},
	}); err != nil {
		return nil, err
	}
	if nParents > 1 {
		return nil, errors.Errorf("can't wrap plan %v %T with more than one input", n, n)
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
		return nil, err
	}
	wrapper.firstNotWrapped = firstNotWrapped

	localProcIdx := p.AddLocalProcessor(wrapper)
	var input []execinfrapb.InputSyncSpec
	if firstNotWrapped != nil {
		// We found a DistSQL-plannable subtree - create an input spec for it.
		input = []execinfrapb.InputSyncSpec{{
			Type:        execinfrapb.InputSyncSpec_PARALLEL_UNORDERED,
			ColumnTypes: p.GetResultTypes(),
		}}
	}
	name := nodeName(n)
	proc := physicalplan.Processor{
		SQLInstanceID: dsp.gatewaySQLInstanceID,
		Spec: execinfrapb.ProcessorSpec{
			Input: input,
			Core: execinfrapb.ProcessorCoreUnion{LocalPlanNode: &execinfrapb.LocalPlanNodeSpec{
				RowSourceIdx: uint32(localProcIdx),
				NumInputs:    nParents,
				Name:         name,
			}},
			Post: execinfrapb.PostProcessSpec{},
			Output: []execinfrapb.OutputRouterSpec{{
				Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
			}},
			// This stage consists of a single processor planned on the gateway.
			StageID:     p.NewStage(false /* containsRemoteProcessor */, allowPartialDistribution),
			ResultTypes: wrapper.outputTypes,
		},
	}
	pIdx := p.AddProcessor(proc)
	if firstNotWrapped != nil {
		// If we found a DistSQL-plannable subtree, we need to add a result stream
		// between it and the physicalPlan we're creating here.
		p.MergeResultStreams(p.ResultRouters, 0, p.MergeOrdering, pIdx, 0, false /* forceSerialization */)
	}
	// ResultRouters gets overwritten each time we add a new PhysicalPlan. We will
	// just have a single result router, since local processors aren't
	// distributed, so make sure that p.ResultRouters has at least 1 slot and
	// write the new processor index there.
	if cap(p.ResultRouters) < 1 {
		p.ResultRouters = make([]physicalplan.ProcessorIdx, 1)
	} else {
		p.ResultRouters = p.ResultRouters[:1]
	}
	p.ResultRouters[0] = pIdx
	p.PlanToStreamColMap = identityMapInPlace(make([]int, len(p.GetResultTypes())))
	return p, nil
}

// createValuesSpec creates a ValuesCoreSpec with the given schema and encoded
// data.
func (dsp *DistSQLPlanner) createValuesSpec(
	planCtx *PlanningCtx, resultTypes []*types.T, numRows int, rawBytes [][]byte,
) *execinfrapb.ValuesCoreSpec {
	numColumns := len(resultTypes)
	s := &execinfrapb.ValuesCoreSpec{
		Columns: make([]execinfrapb.DatumInfo, numColumns),
	}

	for i, t := range resultTypes {
		s.Columns[i].Encoding = descpb.DatumEncoding_VALUE
		s.Columns[i].Type = t
	}

	s.NumRows = uint64(numRows)
	s.RawBytes = rawBytes

	return s
}

// createValuesPlan creates a plan with a single Values processor
// located on the gateway node.
func (dsp *DistSQLPlanner) createValuesPlan(
	planCtx *PlanningCtx, spec *execinfrapb.ValuesCoreSpec, resultTypes []*types.T,
) (*PhysicalPlan, error) {
	p := planCtx.NewPhysicalPlan()

	pIdx := p.AddProcessor(physicalplan.Processor{
		// TODO: find a better node to place processor at
		SQLInstanceID: dsp.gatewaySQLInstanceID,
		Spec: execinfrapb.ProcessorSpec{
			Core:        execinfrapb.ProcessorCoreUnion{Values: spec},
			Output:      []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
			ResultTypes: resultTypes,
		},
	})
	p.ResultRouters = []physicalplan.ProcessorIdx{pIdx}
	p.Distribution = physicalplan.LocalPlan
	p.PlanToStreamColMap = identityMapInPlace(make([]int, len(resultTypes)))

	return p, nil
}

// createValuesSpecFromTuples creates a ValuesCoreSpec from the results of
// evaluating the given tuples.
func (dsp *DistSQLPlanner) createValuesSpecFromTuples(
	planCtx *PlanningCtx, tuples [][]tree.TypedExpr, resultTypes []*types.T,
) (*execinfrapb.ValuesCoreSpec, error) {
	var a tree.DatumAlloc
	evalCtx := &planCtx.ExtendedEvalCtx.EvalContext
	numRows := len(tuples)
	if len(resultTypes) == 0 {
		// Optimization for zero-column sets.
		spec := dsp.createValuesSpec(planCtx, resultTypes, numRows, nil /* rawBytes */)
		return spec, nil
	}
	rawBytes := make([][]byte, numRows)
	for rowIdx, tuple := range tuples {
		var buf []byte
		for colIdx, typedExpr := range tuple {
			datum, err := typedExpr.Eval(evalCtx)
			if err != nil {
				return nil, err
			}
			encDatum := rowenc.DatumToEncDatum(resultTypes[colIdx], datum)
			buf, err = encDatum.Encode(resultTypes[colIdx], &a, descpb.DatumEncoding_VALUE, buf)
			if err != nil {
				return nil, err
			}
		}
		rawBytes[rowIdx] = buf
	}
	spec := dsp.createValuesSpec(planCtx, resultTypes, numRows, rawBytes)
	return spec, nil
}

func (dsp *DistSQLPlanner) createPlanForUnary(
	planCtx *PlanningCtx, n *unaryNode,
) (*PhysicalPlan, error) {
	types, err := getTypesForPlanResult(n, nil /* planToStreamColMap */)
	if err != nil {
		return nil, err
	}

	spec := dsp.createValuesSpec(planCtx, types, 1 /* numRows */, nil /* rawBytes */)
	return dsp.createValuesPlan(planCtx, spec, types)
}

func (dsp *DistSQLPlanner) createPlanForZero(
	planCtx *PlanningCtx, n *zeroNode,
) (*PhysicalPlan, error) {
	types, err := getTypesForPlanResult(n, nil /* planToStreamColMap */)
	if err != nil {
		return nil, err
	}

	spec := dsp.createValuesSpec(planCtx, types, 0 /* numRows */, nil /* rawBytes */)
	return dsp.createValuesPlan(planCtx, spec, types)
}

func (dsp *DistSQLPlanner) createDistinctSpec(
	distinctColumns []uint32,
	orderedColumns []uint32,
	nullsAreDistinct bool,
	errorOnDup string,
	outputOrdering execinfrapb.Ordering,
) *execinfrapb.DistinctSpec {
	return &execinfrapb.DistinctSpec{
		OrderedColumns:   orderedColumns,
		DistinctColumns:  distinctColumns,
		NullsAreDistinct: nullsAreDistinct,
		ErrorOnDup:       errorOnDup,
		OutputOrdering:   outputOrdering,
	}
}

func (dsp *DistSQLPlanner) createPlanForDistinct(
	planCtx *PlanningCtx, n *distinctNode,
) (*PhysicalPlan, error) {
	plan, err := dsp.createPhysPlanForPlanNode(planCtx, n.plan)
	if err != nil {
		return nil, err
	}
	spec := dsp.createDistinctSpec(
		convertFastIntSetToUint32Slice(n.distinctOnColIdxs),
		convertFastIntSetToUint32Slice(n.columnsInOrder),
		n.nullsAreDistinct,
		n.errorOnDup,
		dsp.convertOrdering(n.reqOrdering, plan.PlanToStreamColMap),
	)
	dsp.addDistinctProcessors(plan, spec)
	return plan, nil
}

func (dsp *DistSQLPlanner) addDistinctProcessors(
	plan *PhysicalPlan, spec *execinfrapb.DistinctSpec,
) {
	distinctSpec := execinfrapb.ProcessorCoreUnion{
		Distinct: spec,
	}

	// Add distinct processors local to each existing current result processor.
	plan.AddNoGroupingStage(distinctSpec, execinfrapb.PostProcessSpec{}, plan.GetResultTypes(), plan.MergeOrdering)
	if !plan.IsLastStageDistributed() {
		return
	}

	sqlInstanceIDs := getSQLInstanceIDsOfRouters(plan.ResultRouters, plan.Processors)
	plan.AddStageOnNodes(
		sqlInstanceIDs, distinctSpec, execinfrapb.PostProcessSpec{},
		distinctSpec.Distinct.DistinctColumns, plan.GetResultTypes(),
		plan.GetResultTypes(), plan.MergeOrdering, plan.ResultRouters,
	)
	plan.SetMergeOrdering(spec.OutputOrdering)
}

func (dsp *DistSQLPlanner) createPlanForOrdinality(
	planCtx *PlanningCtx, n *ordinalityNode,
) (*PhysicalPlan, error) {
	plan, err := dsp.createPhysPlanForPlanNode(planCtx, n.source)
	if err != nil {
		return nil, err
	}

	ordinalitySpec := execinfrapb.ProcessorCoreUnion{
		Ordinality: &execinfrapb.OrdinalitySpec{},
	}

	plan.PlanToStreamColMap = append(plan.PlanToStreamColMap, len(plan.GetResultTypes()))
	outputTypes := append(plan.GetResultTypes(), types.Int)

	// WITH ORDINALITY never gets distributed so that the gateway node can
	// always number each row in order.
	plan.AddSingleGroupStage(dsp.gatewaySQLInstanceID, ordinalitySpec, execinfrapb.PostProcessSpec{}, outputTypes)

	return plan, nil
}

func createProjectSetSpec(
	planCtx *PlanningCtx, n *projectSetPlanningInfo, indexVarMap []int,
) (*execinfrapb.ProjectSetSpec, error) {
	spec := execinfrapb.ProjectSetSpec{
		Exprs:            make([]execinfrapb.Expression, len(n.exprs)),
		GeneratedColumns: make([]*types.T, len(n.columns)-n.numColsInSource),
		NumColsPerGen:    make([]uint32, len(n.exprs)),
	}
	for i, expr := range n.exprs {
		var err error
		spec.Exprs[i], err = physicalplan.MakeExpression(expr, planCtx, indexVarMap)
		if err != nil {
			return nil, err
		}
	}
	for i, col := range n.columns[n.numColsInSource:] {
		spec.GeneratedColumns[i] = col.Typ
	}
	for i, n := range n.numColsPerGen {
		spec.NumColsPerGen[i] = uint32(n)
	}
	return &spec, nil
}

func (dsp *DistSQLPlanner) createPlanForProjectSet(
	planCtx *PlanningCtx, n *projectSetNode,
) (*PhysicalPlan, error) {
	plan, err := dsp.createPhysPlanForPlanNode(planCtx, n.source)
	if err != nil {
		return nil, err
	}
	err = dsp.addProjectSet(plan, planCtx, &n.projectSetPlanningInfo)
	return plan, err
}

// addProjectSet adds a grouping stage consisting of a single
// projectSetProcessor that is planned on the gateway.
func (dsp *DistSQLPlanner) addProjectSet(
	plan *PhysicalPlan, planCtx *PlanningCtx, info *projectSetPlanningInfo,
) error {
	numResults := len(plan.GetResultTypes())

	// Create the project set processor spec.
	projectSetSpec, err := createProjectSetSpec(planCtx, info, plan.PlanToStreamColMap)
	if err != nil {
		return err
	}
	spec := execinfrapb.ProcessorCoreUnion{
		ProjectSet: projectSetSpec,
	}

	// Since ProjectSet tends to be a late stage which produces more rows than its
	// source, we opt to perform it only on the gateway node. If we encounter
	// cases in the future where this is non-optimal (perhaps if its output is
	// filtered), we could try to detect these cases and use AddNoGroupingStage
	// instead.
	outputTypes := append(plan.GetResultTypes(), projectSetSpec.GeneratedColumns...)
	plan.AddSingleGroupStage(dsp.gatewaySQLInstanceID, spec, execinfrapb.PostProcessSpec{}, outputTypes)

	// Add generated columns to PlanToStreamColMap.
	for i := range projectSetSpec.GeneratedColumns {
		plan.PlanToStreamColMap = append(plan.PlanToStreamColMap, numResults+i)
	}
	return nil
}

// isOnlyOnGateway returns true if a physical plan is executed entirely on the
// gateway node.
func (dsp *DistSQLPlanner) isOnlyOnGateway(plan *PhysicalPlan) bool {
	if len(plan.ResultRouters) == 1 {
		processorIdx := plan.ResultRouters[0]
		if plan.Processors[processorIdx].SQLInstanceID == dsp.gatewaySQLInstanceID {
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
) (*PhysicalPlan, error) {
	leftLogicalPlan := n.left
	leftPlan, err := dsp.createPhysPlanForPlanNode(planCtx, n.left)
	if err != nil {
		return nil, err
	}
	rightLogicalPlan := n.right
	rightPlan, err := dsp.createPhysPlanForPlanNode(planCtx, n.right)
	if err != nil {
		return nil, err
	}
	if n.inverted {
		leftPlan, rightPlan = rightPlan, leftPlan
		leftLogicalPlan, rightLogicalPlan = rightLogicalPlan, leftLogicalPlan
	}
	childPhysicalPlans := []*PhysicalPlan{leftPlan, rightPlan}

	// Check that the left and right side PlanToStreamColMaps are equivalent.
	// TODO(solon): Are there any valid UNION/INTERSECT/EXCEPT cases where these
	// differ? If we encounter any, we could handle them by adding a projection on
	// the unioned columns on each side, similar to how we handle mismatched
	// ResultTypes.
	if !reflect.DeepEqual(leftPlan.PlanToStreamColMap, rightPlan.PlanToStreamColMap) {
		return nil, errors.Errorf(
			"planToStreamColMap mismatch: %v, %v", leftPlan.PlanToStreamColMap,
			rightPlan.PlanToStreamColMap)
	}
	planToStreamColMap := leftPlan.PlanToStreamColMap
	streamCols := make([]uint32, 0, len(planToStreamColMap))
	for _, streamCol := range planToStreamColMap {
		if streamCol < 0 {
			continue
		}
		streamCols = append(streamCols, uint32(streamCol))
	}

	var distinctSpecs [2]execinfrapb.ProcessorCoreUnion

	if !n.all {
		var distinctOrds [2]execinfrapb.Ordering
		distinctOrds[0] = execinfrapb.ConvertToMappedSpecOrdering(
			planReqOrdering(leftLogicalPlan), leftPlan.PlanToStreamColMap,
		)
		distinctOrds[1] = execinfrapb.ConvertToMappedSpecOrdering(
			planReqOrdering(rightLogicalPlan), rightPlan.PlanToStreamColMap,
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
			distinctSpecs[side].Distinct = dsp.createDistinctSpec(
				streamCols,
				sortCols,
				false, /* nullsAreDistinct */
				"",    /* errorOnDup */
				distinctOrds[side],
			)
			if !dsp.isOnlyOnGateway(plan) {
				// TODO(solon): We could skip this stage if there is a strong key on
				// the result columns.
				plan.AddNoGroupingStage(distinctSpecs[side], execinfrapb.PostProcessSpec{}, plan.GetResultTypes(), distinctOrds[side])
			}
		}
	}

	p := planCtx.NewPhysicalPlan()
	p.SetRowEstimates(&leftPlan.PhysicalPlan, &rightPlan.PhysicalPlan)

	// Merge the plans' PlanToStreamColMap, which we know are equivalent.
	p.PlanToStreamColMap = planToStreamColMap

	// Merge the plans' result types and merge ordering.
	resultTypes, err := mergeResultTypesForSetOp(leftPlan, rightPlan)
	if err != nil {
		return nil, err
	}

	// Set the merge ordering.
	mergeOrdering := dsp.convertOrdering(n.streamingOrdering, p.PlanToStreamColMap)

	// Merge processors, streams, result routers, and stage counter.
	leftRouters := leftPlan.ResultRouters
	rightRouters := rightPlan.ResultRouters
	physicalplan.MergePlans(
		&p.PhysicalPlan, &leftPlan.PhysicalPlan, &rightPlan.PhysicalPlan,
		// In the old execFactory we can only have either local or fully
		// distributed plans, so checking the last stage is sufficient to get
		// the distribution of the whole plans.
		leftPlan.GetLastStageDistribution(),
		rightPlan.GetLastStageDistribution(),
		false, /* allowPartialDistribution */
	)

	if n.unionType == tree.UnionOp {
		// We just need to append the left and right streams together, so append
		// the left and right output routers.
		p.ResultRouters = append(leftRouters, rightRouters...)

		p.SetMergeOrdering(mergeOrdering)

		if !n.all {
			if n.hardLimit != 0 {
				return nil, errors.AssertionFailedf("a hard limit is not supported for UNION (only for UNION ALL)")
			}

			// TODO(abhimadan): use columns from mergeOrdering to fill in the
			// OrderingColumns field in DistinctSpec once the unused columns
			// are projected out.
			distinctSpec := execinfrapb.ProcessorCoreUnion{
				Distinct: dsp.createDistinctSpec(
					streamCols,
					[]uint32{}, /* orderedColumns */
					false,      /* nullsAreDistinct */
					"",         /* errorOnDup */
					mergeOrdering,
				),
			}
			p.AddSingleGroupStage(dsp.gatewaySQLInstanceID, distinctSpec, execinfrapb.PostProcessSpec{}, resultTypes)
		} else {
			// With UNION ALL, we can end up with multiple streams on the same node.
			// We don't want to have unnecessary routers and cross-node streams, so
			// merge these streams now.
			//
			// More importantly, we need to guarantee that if everything is planned
			// on a single node (which is always the case when there are mutations),
			// we can fuse everything so there are no concurrent KV operations (see
			// #40487, #41307).

			if n.hardLimit == 0 {
				// In order to disable auto-parallelism that could occur when merging
				// multiple streams on the same node, we force the serialization of the
				// merge operation (otherwise, it would be possible that we have a
				// source of unbounded parallelism, see #51548).
				p.EnsureSingleStreamPerNode(true /* forceSerialization */, execinfrapb.PostProcessSpec{})
			} else {
				if p.GetLastStageDistribution() != physicalplan.LocalPlan {
					return nil, errors.AssertionFailedf("we expect that limited UNION ALL queries are only planned locally")
				}
				if len(p.MergeOrdering.Columns) != 0 {
					return nil, errors.AssertionFailedf(
						"we expect that limited UNION ALL queries do not require a specific ordering",
					)
				}
				// Force the serialization between the two streams so that the
				// serial unordered synchronizer is used which has exactly the
				// behavior that we want (in particular, it won't execute the
				// right child if the limit is reached by the left child).
				p.EnsureSingleStreamPerNode(
					true, /* forceSerialization */
					execinfrapb.PostProcessSpec{Limit: n.hardLimit},
				)
			}

			// UNION ALL is special: it doesn't have any required downstream
			// processor, so its two inputs might have different post-processing
			// which would violate an assumption later down the line. Check for this
			// condition and add a no-op stage if it exists.
			if err := p.CheckLastStagePost(); err != nil {
				p.EnsureSingleStreamOnGateway()
			}
		}
	} else {
		if n.hardLimit != 0 {
			return nil, errors.AssertionFailedf("a hard limit is not supported for INTERSECT or EXCEPT")
		}

		// We plan INTERSECT and EXCEPT queries with joiners. Get the appropriate
		// join type.
		joinType := distsqlSetOpJoinType(n.unionType)

		// Nodes where we will run the join processors.
		nodes := findJoinProcessorNodes(leftRouters, rightRouters, p.Processors)

		// Set up the equality columns.
		eqCols := streamCols

		// Project the left-side columns only.
		post := execinfrapb.PostProcessSpec{Projection: true}
		post.OutputColumns = make([]uint32, len(streamCols))
		copy(post.OutputColumns, streamCols)

		// Create the Core spec.
		var core execinfrapb.ProcessorCoreUnion
		if len(mergeOrdering.Columns) == 0 {
			core.HashJoiner = &execinfrapb.HashJoinerSpec{
				LeftEqColumns:  eqCols,
				RightEqColumns: eqCols,
				Type:           joinType,
			}
		} else {
			if len(mergeOrdering.Columns) < len(streamCols) {
				return nil, errors.AssertionFailedf("the merge ordering must include all stream columns")
			}
			core.MergeJoiner = &execinfrapb.MergeJoinerSpec{
				LeftOrdering:  mergeOrdering,
				RightOrdering: mergeOrdering,
				Type:          joinType,
				NullEquality:  true,
			}
		}

		if n.all {
			p.AddJoinStage(
				nodes, core, post, eqCols, eqCols,
				leftPlan.GetResultTypes(), rightPlan.GetResultTypes(),
				leftPlan.MergeOrdering, rightPlan.MergeOrdering,
				leftRouters, rightRouters, resultTypes,
			)
		} else {
			p.AddDistinctSetOpStage(
				nodes, core, distinctSpecs[:], post, eqCols,
				leftPlan.GetResultTypes(), rightPlan.GetResultTypes(),
				leftPlan.MergeOrdering, rightPlan.MergeOrdering,
				leftRouters, rightRouters, resultTypes,
			)
		}

		p.SetMergeOrdering(mergeOrdering)
	}

	return p, nil
}

// createPlanForWindow creates a physical plan for computing window functions.
// We add a new stage of windower processors for each different partitioning
// scheme found in the query's window functions.
func (dsp *DistSQLPlanner) createPlanForWindow(
	planCtx *PlanningCtx, n *windowNode,
) (*PhysicalPlan, error) {
	plan, err := dsp.createPhysPlanForPlanNode(planCtx, n.plan)
	if err != nil {
		return nil, err
	}

	numWindowFuncProcessed := 0
	windowPlanState := createWindowPlanState(n, planCtx, plan)
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
		windowerSpec := execinfrapb.WindowerSpec{
			PartitionBy: partitionIdxs,
			WindowFns:   make([]execinfrapb.WindowerSpec_WindowFn, len(samePartitionFuncs)),
		}

		newResultTypes := make([]*types.T, len(plan.GetResultTypes())+len(samePartitionFuncs))
		copy(newResultTypes, plan.GetResultTypes())
		for windowFnSpecIdx, windowFn := range samePartitionFuncs {
			windowFnSpec, outputType, err := windowPlanState.createWindowFnSpec(windowFn)
			if err != nil {
				return nil, err
			}
			newResultTypes[windowFn.outputColIdx] = outputType
			windowerSpec.WindowFns[windowFnSpecIdx] = windowFnSpec
		}

		// Check if the previous stage is all on one node.
		prevStageNode := plan.Processors[plan.ResultRouters[0]].SQLInstanceID
		for i := 1; i < len(plan.ResultRouters); i++ {
			if n := plan.Processors[plan.ResultRouters[i]].SQLInstanceID; n != prevStageNode {
				prevStageNode = 0
				break
			}
		}

		// Get all sqlInstanceIDs from the previous stage.
		sqlInstanceIDs := getSQLInstanceIDsOfRouters(plan.ResultRouters, plan.Processors)
		if len(partitionIdxs) == 0 || len(sqlInstanceIDs) == 1 {
			// No PARTITION BY or we have a single node. Use a single windower.
			// If the previous stage was all on a single node, put the windower
			// there. Otherwise, bring the results back on this node.
			sqlInstanceID := dsp.gatewaySQLInstanceID
			if len(sqlInstanceIDs) == 1 {
				sqlInstanceID = sqlInstanceIDs[0]
			}
			plan.AddSingleGroupStage(
				sqlInstanceID,
				execinfrapb.ProcessorCoreUnion{Windower: &windowerSpec},
				execinfrapb.PostProcessSpec{},
				newResultTypes,
			)
		} else {
			// Set up the output routers from the previous stage.
			// We use hash routers with hashing on the columns
			// from PARTITION BY clause of window functions
			// we're processing in the current stage.
			for _, resultProc := range plan.ResultRouters {
				plan.Processors[resultProc].Spec.Output[0] = execinfrapb.OutputRouterSpec{
					Type:        execinfrapb.OutputRouterSpec_BY_HASH,
					HashColumns: partitionIdxs,
				}
			}
			// We have multiple streams, so we definitely have a processor planned
			// on a remote node.
			stageID := plan.NewStage(true /* containsRemoteProcessor */, false /* allowPartialDistribution */)

			// We put a windower on each node and we connect it
			// with all hash routers from the previous stage in
			// a such way that each node has its designated
			// SourceRouterSlot - namely, position in which
			// a node appears in nodes.
			prevStageRouters := plan.ResultRouters
			prevStageResultTypes := plan.GetResultTypes()
			plan.ResultRouters = make([]physicalplan.ProcessorIdx, 0, len(sqlInstanceIDs))
			for bucket, sqlInstanceID := range sqlInstanceIDs {
				proc := physicalplan.Processor{
					SQLInstanceID: sqlInstanceID,
					Spec: execinfrapb.ProcessorSpec{
						Input: []execinfrapb.InputSyncSpec{{
							Type:        execinfrapb.InputSyncSpec_PARALLEL_UNORDERED,
							ColumnTypes: prevStageResultTypes,
						}},
						Core: execinfrapb.ProcessorCoreUnion{Windower: &windowerSpec},
						Post: execinfrapb.PostProcessSpec{},
						Output: []execinfrapb.OutputRouterSpec{{
							Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
						}},
						StageID:     stageID,
						ResultTypes: newResultTypes,
					},
				}
				pIdx := plan.AddProcessor(proc)

				for _, router := range prevStageRouters {
					plan.Streams = append(plan.Streams, physicalplan.Stream{
						SourceProcessor:  router,
						SourceRouterSlot: bucket,
						DestProcessor:    pIdx,
						DestInput:        0,
					})
				}
				plan.ResultRouters = append(plan.ResultRouters, pIdx)
			}
		}
	}

	// We definitely added columns throughout all the stages of windowers, so we
	// need to update PlanToStreamColMap. We need to update the map before adding
	// rendering or projection because it is used there.
	plan.PlanToStreamColMap = identityMap(plan.PlanToStreamColMap, len(plan.GetResultTypes()))

	// windowers do not guarantee maintaining the order at the moment, so we
	// reset MergeOrdering. There shouldn't be an ordering here, but we reset it
	// defensively (see #35179).
	plan.SetMergeOrdering(execinfrapb.Ordering{})

	// After all window functions are computed, we need to add rendering or
	// projection.
	if err := windowPlanState.addRenderingOrProjection(); err != nil {
		return nil, err
	}

	if len(plan.GetResultTypes()) != len(plan.PlanToStreamColMap) {
		// We added/removed columns while rendering or projecting, so we need to
		// update PlanToStreamColMap.
		plan.PlanToStreamColMap = identityMap(plan.PlanToStreamColMap, len(plan.GetResultTypes()))
	}

	return plan, nil
}

// createPlanForExport creates a physical plan for EXPORT.
// We add a new stage of CSV/Parquet Writer processors to the input plan.
func (dsp *DistSQLPlanner) createPlanForExport(
	planCtx *PlanningCtx, n *exportNode,
) (*PhysicalPlan, error) {
	plan, err := dsp.createPhysPlanForPlanNode(planCtx, n.source)
	if err != nil {
		return nil, err
	}

	var core execinfrapb.ProcessorCoreUnion
	core.Exporter = &execinfrapb.ExportSpec{
		Destination: n.destination,
		NamePattern: n.fileNamePattern,
		Format:      n.format,
		ChunkRows:   int64(n.chunkRows),
		ChunkSize:   n.chunkSize,
		ColNames:    n.colNames,
		UserProto:   planCtx.planner.User().EncodeProto(),
	}

	resTypes := make([]*types.T, len(colinfo.ExportColumns))
	for i := range colinfo.ExportColumns {
		resTypes[i] = colinfo.ExportColumns[i].Typ
	}
	plan.AddNoGroupingStage(
		core, execinfrapb.PostProcessSpec{}, resTypes, execinfrapb.Ordering{},
	)

	// The CSVWriter produces the same columns as the EXPORT statement.
	plan.PlanToStreamColMap = identityMap(plan.PlanToStreamColMap, len(colinfo.ExportColumns))
	return plan, nil
}

// checkScanParallelizationIfLocal returns whether the plan contains scanNodes
// that can be parallelized and is such that it is safe to do so.
//
// This method performs a walk over the plan to make sure that only planNodes
// that allow for the scan parallelization are present (this is a limitation
// of the vectorized engine). Namely, the plan is allowed to contain only those
// things that are natively supported by the vectorized engine; if there is a
// planNode that will be handled by wrapping a row-by-row processor into the
// vectorized flow, we might get an error during the query execution because the
// processors eagerly move into the draining state which will cancel the context
// of parallel TableReaders which might "poison" the transaction.
func checkScanParallelizationIfLocal(
	ctx context.Context, plan *planComponents,
) (prohibitParallelization, hasScanNodeToParallelize bool) {
	if plan.main.planNode == nil || len(plan.cascades) != 0 || len(plan.checkPlans) != 0 {
		// We either used the experimental DistSQL spec factory or have
		// cascades/checks; both of these conditions - for now - prohibit
		// the scan parallelization.
		return true, false
	}
	o := planObserver{
		enterNode: func(ctx context.Context, nodeName string, plan planNode) (bool, error) {
			if prohibitParallelization {
				return false, nil
			}
			switch n := plan.(type) {
			case *distinctNode:
				return true, nil
			case *explainPlanNode:
				// walkPlan doesn't recurse into explainPlanNode, so we have to
				// manually walk over the wrapped plan.
				plan := n.plan.WrappedPlan.(*planComponents)
				prohibit, has := checkScanParallelizationIfLocal(ctx, plan)
				prohibitParallelization = prohibitParallelization || prohibit
				hasScanNodeToParallelize = hasScanNodeToParallelize || has
				return false, nil
			case *explainVecNode:
				return true, nil
			case *filterNode:
				// Some filter expressions might be handled by falling back to
				// the wrapped processors, so we choose to be safe.
				prohibitParallelization = true
				return false, nil
			case *groupNode:
				for _, f := range n.funcs {
					prohibitParallelization = f.hasFilter()
				}
				return true, nil
			case *indexJoinNode:
				return true, nil
			case *joinNode:
				prohibitParallelization = n.pred.onCond != nil
				return true, nil
			case *limitNode:
				return true, nil
			case *ordinalityNode:
				return true, nil
			case *renderNode:
				// Only support projections since render expressions might be
				// handled via a wrapped row-by-row processor.
				for _, e := range n.render {
					if _, isIVar := e.(*tree.IndexedVar); !isIVar {
						prohibitParallelization = true
					}
				}
				return true, nil
			case *scanNode:
				if len(n.reqOrdering) == 0 && n.parallelize {
					hasScanNodeToParallelize = true
				}
				return true, nil
			case *sortNode:
				return true, nil
			case *unionNode:
				return true, nil
			case *valuesNode:
				return true, nil
			default:
				prohibitParallelization = true
				return false, nil
			}
		},
	}
	_ = walkPlan(ctx, plan.main.planNode, o)
	for _, s := range plan.subqueryPlans {
		_ = walkPlan(ctx, s.plan.planNode, o)
	}
	return prohibitParallelization, hasScanNodeToParallelize
}

// NewPlanningCtx returns a new PlanningCtx. When distribute is false, a
// lightweight version PlanningCtx is returned that can be used when the caller
// knows plans will only be run on one node. On SQL tenants, the plan is only
// distributed if tenantDistributionEnabled is true. planner argument can be
// left nil.
func (dsp *DistSQLPlanner) NewPlanningCtx(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	planner *planner,
	txn *kv.Txn,
	distributionType DistributionType,
) *PlanningCtx {
	distribute := distributionType == DistributionTypeAlways || (distributionType == DistributionTypeSystemTenantOnly && evalCtx.Codec.ForSystemTenant())
	planCtx := &PlanningCtx{
		ctx:             ctx,
		ExtendedEvalCtx: evalCtx,
		infra:           physicalplan.MakePhysicalInfrastructure(uuid.FastMakeV4(), dsp.gatewaySQLInstanceID),
		isLocal:         !distribute,
		planner:         planner,
	}
	if !distribute {
		if planner == nil || dsp.spanResolver == nil || planner.curPlan.flags.IsSet(planFlagContainsMutation) {
			// Don't parallelize the scans if we have a local plan if
			// - we don't have a planner which is the case when we are not on
			// the main query path;
			// - we don't have a span resolver (this can happen only in tests);
			// - the plan contains a mutation operation - we currently don't
			// support any parallelism when mutations are present.
			return planCtx
		}
		prohibitParallelization, hasScanNodeToParallelize := checkScanParallelizationIfLocal(ctx, &planner.curPlan.planComponents)
		if prohibitParallelization || !hasScanNodeToParallelize {
			return planCtx
		}
		// We might decide to parallelize scans, and although the plan is local,
		// we still need to instantiate a full planning context.
		planCtx.parallelizeScansIfLocal = true
	}
	planCtx.spanIter = dsp.spanResolver.NewSpanResolverIterator(txn)
	planCtx.NodeStatuses = make(map[base.SQLInstanceID]NodeStatus)
	planCtx.NodeStatuses[dsp.gatewaySQLInstanceID] = NodeOK
	return planCtx
}

// maybeMoveSingleFlowToGateway checks whether plan consists of a single flow
// on the remote node and would benefit from bringing that flow to the gateway.
func maybeMoveSingleFlowToGateway(planCtx *PlanningCtx, plan *PhysicalPlan, rowCount int64) {
	if !planCtx.isLocal && planCtx.ExtendedEvalCtx.SessionData().DistSQLMode != sessiondatapb.DistSQLAlways {
		// If we chose to distribute this plan, yet we created only a single
		// remote flow, it might be a good idea to bring that whole flow back
		// to the gateway.
		//
		// This comes from the limitation of pinning each flow based on the
		// physical planning of table readers. However, if later stages of the
		// plan contain other processors, e.g. joinReaders, the whole flow can
		// become quite expensive. With high enough frequency of such flows, the
		// node having the lease for the ranges of the table readers becomes the
		// hot spot. In such a scenario we might choose to run the flow locally
		// to distribute the load on the cluster better (assuming that the
		// queries are issued against all nodes with equal frequency).

		// If we estimate that the plan reads far more rows than it returns in
		// the output, it probably makes sense to keep the flow as distributed
		// (i.e. keep the computation where the data is). Therefore, when we
		// don't have a good estimate (rowCount is negative) or the data
		// cardinality is reduced significantly (the reduction ratio is at least
		// 10), we will keep the plan as is.
		const rowReductionRatio = 10
		keepPlan := rowCount <= 0 || float64(plan.TotalEstimatedScannedRows)/float64(rowCount) >= rowReductionRatio
		if keepPlan {
			return
		}
		singleFlow := true
		moveFlowToGateway := false
		sqlInstanceID := plan.Processors[0].SQLInstanceID
		for _, p := range plan.Processors[1:] {
			if p.SQLInstanceID != sqlInstanceID {
				if p.SQLInstanceID != plan.GatewaySQLInstanceID || p.Spec.Core.Noop == nil {
					// We want to ignore the noop processors planned on the
					// gateway because their job is to simply communicate the
					// results back to the client. If, however, there is another
					// non-noop processor on the gateway, then we'll correctly
					// treat the plan as having multiple flows.
					singleFlow = false
					break
				}
			}
			core := p.Spec.Core
			if core.JoinReader != nil || core.MergeJoiner != nil || core.HashJoiner != nil ||
				core.ZigzagJoiner != nil || core.InvertedJoiner != nil {
				// We want to move the flow when it contains a processor that
				// might increase the cardinality of the data flowing through it
				// or that performs the KV work.
				moveFlowToGateway = true
			}
		}
		if singleFlow && moveFlowToGateway {
			for i := range plan.Processors {
				plan.Processors[i].SQLInstanceID = plan.GatewaySQLInstanceID
			}
			planCtx.isLocal = true
			planCtx.planner.curPlan.flags.Unset(planFlagFullyDistributed)
			planCtx.planner.curPlan.flags.Unset(planFlagPartiallyDistributed)
			plan.Distribution = physicalplan.LocalPlan
		}
	}
}

// FinalizePlan adds a final "result" stage and a final projection if necessary
// as well as populates the endpoints of the plan.
func (dsp *DistSQLPlanner) FinalizePlan(planCtx *PlanningCtx, plan *PhysicalPlan) {
	dsp.finalizePlanWithRowCount(planCtx, plan, -1 /* rowCount */)
}

// finalizePlanWithRowCount adds a final "result" stage and a final projection
// if necessary as well as populates the endpoints of the plan.
// - rowCount is the estimated number of rows that the plan outputs. Use a
// negative number if the stats were not available to make an estimate.
func (dsp *DistSQLPlanner) finalizePlanWithRowCount(
	planCtx *PlanningCtx, plan *PhysicalPlan, rowCount int64,
) {
	// Find all MetadataTestSenders in the plan, so that the MetadataTestReceiver
	// knows how many sender IDs it should expect.
	var metadataSenders []string
	for _, proc := range plan.Processors {
		if proc.Spec.Core.MetadataTestSender != nil {
			metadataSenders = append(metadataSenders, proc.Spec.Core.MetadataTestSender.ID)
		}
	}

	maybeMoveSingleFlowToGateway(planCtx, plan, rowCount)

	// Add a final "result" stage if necessary.
	plan.EnsureSingleStreamOnGateway()

	// Add a final projection so that DistSQLReceiver gets the rows of the
	// desired schema.
	projection := make([]uint32, 0, len(plan.GetResultTypes()))
	for _, outputCol := range plan.PlanToStreamColMap {
		if outputCol >= 0 {
			projection = append(projection, uint32(outputCol))
		}
	}
	plan.AddProjection(projection, execinfrapb.Ordering{})
	// PlanToStreamColMap is no longer necessary.
	plan.PlanToStreamColMap = nil

	if len(metadataSenders) > 0 {
		plan.AddSingleGroupStage(
			dsp.gatewaySQLInstanceID,
			execinfrapb.ProcessorCoreUnion{
				MetadataTestReceiver: &execinfrapb.MetadataTestReceiverSpec{
					SenderIDs: metadataSenders,
				},
			},
			execinfrapb.PostProcessSpec{},
			plan.GetResultTypes(),
		)
	}

	// Set up the endpoints for plan.Streams.
	plan.PopulateEndpoints()

	// Set up the endpoint for the final result.
	finalOut := &plan.Processors[plan.ResultRouters[0]].Spec.Output[0]
	finalOut.Streams = append(finalOut.Streams, execinfrapb.StreamEndpointSpec{
		Type: execinfrapb.StreamEndpointSpec_SYNC_RESPONSE,
	})

	// Assign processor IDs.
	for i := range plan.Processors {
		plan.Processors[i].Spec.ProcessorID = int32(i)
	}
}
