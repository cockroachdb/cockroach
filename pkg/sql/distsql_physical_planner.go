// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execagg"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan/replicaoracle"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// DistSQLPlanner is used to generate distributed plans from logical
// plans. A rough overview of the process:
//
//   - the plan is based on a planNode tree (in the future it will be based on an
//     intermediate representation tree). Only a subset of the possible trees is
//     supported (this can be checked via CheckSupport).
//
//   - we generate a PhysicalPlan for the planNode tree recursively. The
//     PhysicalPlan consists of a network of processors and streams, with a set
//     of unconnected "result routers". The PhysicalPlan also has information on
//     ordering and on the mapping planNode columns to columns in the result
//     streams (all result routers output streams with the same schema).
//
//     The PhysicalPlan for a scanNode leaf consists of TableReaders, one for each node
//     that has one or more ranges.
//
//   - for each an internal planNode we start with the plan of the child node(s)
//     and add processing stages (connected to the result routers of the children
//     node).
type DistSQLPlanner struct {
	st *cluster.Settings
	// The SQLInstanceID of the gateway node that initiated this query.
	gatewaySQLInstanceID base.SQLInstanceID
	stopper              *stop.Stopper
	distSQLSrv           *distsql.ServerImpl
	spanResolver         physicalplan.SpanResolver

	// runnerCoordinator is used to send out requests (for running SetupFlow
	// RPCs) to a pool of workers.
	runnerCoordinator runnerCoordinator

	// cancelFlowsCoordinator is responsible for batching up the requests to
	// cancel remote flows initiated on the behalf of the current node when the
	// local flows errored out.
	cancelFlowsCoordinator cancelFlowsCoordinator

	// gossip handle used to check node version compatibility.
	gossip gossip.OptionalGossip

	// sqlInstanceDialer handles communication between SQL nodes/pods.
	sqlInstanceDialer *nodedialer.Dialer

	// nodeHealth encapsulates the various node health checks to avoid planning
	// on unhealthy nodes.
	nodeHealth distSQLNodeHealth

	// parallelLocalScansSem is a node-wide semaphore on the number of
	// additional goroutines that can be used to run concurrent TableReaders
	// for the same stage of the fully local physical plans.
	parallelLocalScansSem *quotapool.IntPool
	// parallelChecksSem is a node-wide semaphore on the number of additional
	// goroutines that can be used to run check postqueries (FK and UNIQUE
	// constraint checks) in parallel.
	parallelChecksSem *quotapool.IntPool

	// distSender is used to construct the spanResolver upon SetSQLInstanceInfo.
	distSender *kvcoord.DistSender
	// nodeDescs is used to construct the spanResolver upon SetSQLInstanceInfo.
	nodeDescs kvclient.NodeDescStore
	// rpcCtx is used to construct the spanResolver upon SetSQLInstanceInfo.
	rpcCtx *rpc.Context

	// sqlAddressResolver has information about SQL instances in a non-system
	// tenant environment.
	sqlAddressResolver sqlinstance.AddressResolver

	// codec allows the DistSQLPlanner to determine whether it is creating plans
	// for a system tenant or non-system tenant.
	codec keys.SQLCodec

	clock *hlc.Clock
}

// DistributionType is an enum defining when a plan should be distributed.
type DistributionType int

const (
	// LocalDistribution does not distribute a plan across multiple SQL
	// instances.
	LocalDistribution = iota
	// FullDistribution distributes a plan across multiple SQL instances whether
	// it is a system tenant or non-system tenant.
	FullDistribution
)

// ReplicaOraclePolicy controls which policy the physical planner uses to choose
// a replica for a given range. It is exported so that it may be overwritten
// during initialization by CCL code to enable follower reads.
var ReplicaOraclePolicy = replicaoracle.BinPackingChoice

// NewDistSQLPlanner initializes a DistSQLPlanner.
//
// sqlInstanceID is the ID of the node on which this planner runs. It is used to
// favor itself and other close-by nodes when planning. An invalid sqlInstanceID
// can be passed to aid bootstrapping, but then SetSQLInstanceInfo() needs to be called
// before this planner is used.
func NewDistSQLPlanner(
	ctx context.Context,
	st *cluster.Settings,
	sqlInstanceID base.SQLInstanceID,
	rpcCtx *rpc.Context,
	distSQLSrv *distsql.ServerImpl,
	distSender *kvcoord.DistSender,
	nodeDescs kvclient.NodeDescStore,
	gw gossip.OptionalGossip,
	stopper *stop.Stopper,
	isAvailable func(base.SQLInstanceID) bool,
	connHealthCheckerSystem func(roachpb.NodeID, rpc.ConnectionClass) error, // will only be used by the system tenant
	instanceConnHealthChecker func(base.SQLInstanceID, string) error,
	sqlInstanceDialer *nodedialer.Dialer,
	codec keys.SQLCodec,
	sqlAddressResolver sqlinstance.AddressResolver,
	clock *hlc.Clock,
) *DistSQLPlanner {
	dsp := &DistSQLPlanner{
		st:                   st,
		gatewaySQLInstanceID: sqlInstanceID,
		stopper:              stopper,
		distSQLSrv:           distSQLSrv,
		gossip:               gw,
		sqlInstanceDialer:    sqlInstanceDialer,
		nodeHealth: distSQLNodeHealth{
			gossip:             gw,
			connHealthSystem:   connHealthCheckerSystem,
			connHealthInstance: instanceConnHealthChecker,
			isAvailable:        isAvailable,
		},
		distSender:         distSender,
		nodeDescs:          nodeDescs,
		rpcCtx:             rpcCtx,
		sqlAddressResolver: sqlAddressResolver,
		codec:              codec,
		clock:              clock,
	}

	dsp.parallelLocalScansSem = quotapool.NewIntPool("parallel local scans concurrency",
		uint64(localScansConcurrencyLimit.Get(&st.SV)))
	localScansConcurrencyLimit.SetOnChange(&st.SV, func(ctx context.Context) {
		dsp.parallelLocalScansSem.UpdateCapacity(uint64(localScansConcurrencyLimit.Get(&st.SV)))
	})
	dsp.parallelChecksSem = quotapool.NewIntPool("parallel checks concurrency",
		uint64(parallelChecksConcurrencyLimit.Get(&st.SV)))
	parallelChecksConcurrencyLimit.SetOnChange(&st.SV, func(ctx context.Context) {
		dsp.parallelChecksSem.UpdateCapacity(uint64(parallelChecksConcurrencyLimit.Get(&st.SV)))
	})
	if rpcCtx != nil {
		// rpcCtx might be nil in some tests.
		rpcCtx.Stopper.AddCloser(dsp.parallelLocalScansSem.Closer("stopper"))
		rpcCtx.Stopper.AddCloser(dsp.parallelChecksSem.Closer("stopper"))
	}

	dsp.runnerCoordinator.init(ctx, stopper, &st.SV)
	dsp.initCancelingWorkers(ctx)
	return dsp
}

// GetAllInstancesByLocality lists all instances that match the passed locality
// filters.
func (dsp *DistSQLPlanner) GetAllInstancesByLocality(
	ctx context.Context, filter roachpb.Locality,
) ([]sqlinstance.InstanceInfo, error) {
	all, err := dsp.sqlAddressResolver.GetAllInstances(ctx)
	if err != nil {
		return nil, err
	}
	all, _ = dsp.filterUnhealthyInstances(all, nil /* nodeStatusesCache */)

	log.VEventf(ctx, 2, "resolved sql instances: %v", all)
	var pos int
	for _, n := range all {
		if ok, _ := n.Locality.Matches(filter); ok {
			all[pos] = n
			pos++
		}
	}
	log.VEventf(ctx, 2, "found %d instances matching locality filter %s; matching instances: %v",
		pos, filter, all[:pos])
	if pos == 0 {
		return nil, errors.Newf("no instances found matching locality filter %s", filter.String())
	}
	return all[:pos], nil
}

// GetSQLInstanceInfo gets a node descriptor by node ID.
func (dsp *DistSQLPlanner) GetSQLInstanceInfo(
	sqlInstanceID base.SQLInstanceID,
) (*roachpb.NodeDescriptor, error) {
	return dsp.nodeDescs.GetNodeDescriptor(roachpb.NodeID(sqlInstanceID))
}

// ReplicaOracleConfig returns the DSP's replicaoracle.Config.
func (dsp *DistSQLPlanner) ReplicaOracleConfig(loc roachpb.Locality) replicaoracle.Config {
	return replicaoracle.Config{
		NodeDescs:   dsp.nodeDescs,
		NodeID:      roachpb.NodeID(dsp.gatewaySQLInstanceID),
		Locality:    loc,
		Settings:    dsp.st,
		Clock:       dsp.clock,
		RPCContext:  dsp.rpcCtx,
		LatencyFunc: dsp.distSender.LatencyFunc(),
		HealthFunc:  dsp.distSender.HealthFunc(),
	}
}

// ConstructAndSetSpanResolver constructs and sets the planner's
// SpanResolver if it is unset. It's a no-op otherwise.
func (dsp *DistSQLPlanner) ConstructAndSetSpanResolver(
	ctx context.Context, nodeID roachpb.NodeID, locality roachpb.Locality,
) {
	if dsp.spanResolver != nil {
		log.Fatal(ctx, "trying to construct and set span resolver when one already exists")
	}
	sr := physicalplan.NewSpanResolver(dsp.st, dsp.distSender, dsp.nodeDescs, nodeID, locality,
		dsp.clock, dsp.rpcCtx, ReplicaOraclePolicy)
	dsp.SetSpanResolver(sr)
}

// SetGatewaySQLInstanceID sets the planner's SQL instance ID.
func (dsp *DistSQLPlanner) SetGatewaySQLInstanceID(id base.SQLInstanceID) {
	dsp.gatewaySQLInstanceID = id
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
	case *tree.RoutineExpr:
		v.err = newQueryNotSupportedErrorf("user-defined routine %s cannot be executed with distsql", t)
		return false, expr
	case *tree.DOid:
		v.err = newQueryNotSupportedError("OID expressions are not supported by distsql")
		return false, expr
	case *tree.Subquery:
		if hasOidType(t.ResolvedType()) {
			// If a subquery results in a DOid datum, the datum will get a type
			// annotation (because DOids are ambiguous) when serializing the
			// render expression involving the result of the subquery. As a
			// result, we might need to perform a cast on a remote node which
			// might fail, thus we prohibit the distribution of the main query.
			v.err = newQueryNotSupportedError("OID expressions are not supported by distsql")
			return false, expr
		}
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
		if t.ResolvedType().ArrayContents().Identical(types.AnyTuple) {
			v.err = newQueryNotSupportedErrorf("array %s cannot be executed with distsql", t)
			return false, expr
		}
	case *tree.DTuple:
		if t.ResolvedType().Identical(types.AnyTuple) {
			v.err = newQueryNotSupportedErrorf("tuple %s cannot be executed with distsql", t)
			return false, expr
		}
	case *tree.DJsonpath:
		// TODO(normanchenn): We currently do not have an encoding for jsonpath
		// thus do not support it within distsql
		v.err = newQueryNotSupportedErrorf("jsonpath %s cannot be executed with distsql", t)
		return false, expr
	}
	return true, expr
}

func (v *distSQLExprCheckVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

// hasOidType returns whether t or its contents include an OID type.
func hasOidType(t *types.T) bool {
	switch t.Family() {
	case types.OidFamily:
		return true
	case types.ArrayFamily:
		return hasOidType(t.ArrayContents())
	case types.TupleFamily:
		for _, typ := range t.TupleContents() {
			if hasOidType(typ) {
				return true
			}
		}
	}
	return false
}

// checkExprForDistSQL verifies that an expression doesn't contain things that
// are not yet supported by distSQL, like distSQL-blocklisted functions.
func checkExprForDistSQL(expr tree.Expr, distSQLVisitor *distSQLExprCheckVisitor) error {
	if expr == nil {
		return nil
	}
	distSQLVisitor.err = nil
	tree.WalkExprConst(distSQLVisitor, expr)
	return distSQLVisitor.err
}

type distRecommendation int

const (
	// cannotDistribute indicates that a plan cannot be distributed.
	cannotDistribute distRecommendation = iota

	// canDistribute indicates that a plan can be distributed, but it's not
	// clear whether it'll benefit from that.
	canDistribute

	// shouldDistribute indicates that a plan will likely benefit if distributed.
	shouldDistribute
)

// compose returns the recommendation for a plan given recommendations for two
// parts of it.
func (a distRecommendation) compose(b distRecommendation) distRecommendation {
	if a == cannotDistribute || b == cannotDistribute {
		return cannotDistribute
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

var (
	// planNodeNotSupportedErr is the catch-all error value returned from
	// checkSupportForPlanNode when a planNode type does not support distributed
	// execution.
	planNodeNotSupportedErr            = newQueryNotSupportedError("unsupported node")
	cannotDistributeRowLevelLockingErr = newQueryNotSupportedError(
		"scans with row-level locking are not supported by distsql",
	)
	invertedFilterNotDistributableErr = newQueryNotSupportedError(
		"inverted filter is only distributable when it's a union of spans",
	)
	localityOptimizedOpNotDistributableErr = newQueryNotSupportedError(
		"locality-optimized operation cannot be distributed",
	)
	ordinalityNotDistributableErr = newQueryNotSupportedError(
		"ordinality operation cannot be distributed",
	)
	cannotDistributeVectorSearchErr = newQueryNotSupportedError(
		"vector search operation cannot be distributed",
	)
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
	case *vectorMutationSearchNode:
	case *vectorSearchNode:
	case *windowNode:
	case *zeroNode:
	case *zigzagJoinNode:
	default:
		return true
	}
	return false
}

func shouldWrapPlanNodeForExecStats(planCtx *PlanningCtx, node planNode) bool {
	if !planCtx.collectExecStats {
		// If execution stats aren't being collected, there is no point in
		// having the overhead of wrappers.
		return false
	}
	// Wrapping batchedPlanNodes breaks some assumptions (namely that Start is
	// called on the processor-adapter) because it's executed in a special "fast
	// path" way, so we exempt these from wrapping.
	_, ok := node.(batchedPlanNode)
	return !ok
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
	// place. Unless the plan is a vector insert where we are inserting a preformed
	// coldata.Batch.
	return !specifiedInQuery || (planCtx.isLocal && !planCtx.isVectorInsert)
}

// checkSupportForPlanNode returns a distRecommendation (as described above) or
// cannotDistribute and an error if the plan subtree is not distributable.
// The error doesn't indicate complete failure - it's instead the reason that
// this plan couldn't be distributed.
// TODO(radu): add tests for this.
func checkSupportForPlanNode(
	ctx context.Context,
	node planNode,
	distSQLVisitor *distSQLExprCheckVisitor,
	sd *sessiondata.SessionData,
) (retRec distRecommendation, retErr error) {
	if buildutil.CrdbTestBuild {
		defer func() {
			if retRec == cannotDistribute && retErr == nil {
				panic(errors.AssertionFailedf("all 'cannotDistribute' recommendations must be accompanied by an error"))
			}
		}()
	}
	switch n := node.(type) {
	// Keep these cases alphabetized, please!
	case *createStatsNode:
		if n.runAsJob {
			return cannotDistribute, planNodeNotSupportedErr
		}
		return shouldDistribute, nil

	case *distinctNode:
		return checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd)

	case *exportNode:
		return checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd)

	case *filterNode:
		if err := checkExprForDistSQL(n.filter, distSQLVisitor); err != nil {
			return cannotDistribute, err
		}
		return checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd)

	case *groupNode:
		rec, err := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd)
		if err != nil {
			return cannotDistribute, err
		}
		for _, agg := range n.funcs {
			if agg.distsqlBlocklist {
				return cannotDistribute, newQueryNotSupportedErrorf("aggregate %q cannot be executed with distsql", agg.funcName)
			}
		}
		// Don't force distribution if we expect to process small number of
		// rows.
		aggRec := canDistribute
		if n.estimatedInputRowCount == 0 {
			log.VEventf(ctx, 2, "aggregation (no stats) recommends plan distribution")
			aggRec = shouldDistribute
		} else if n.estimatedInputRowCount >= sd.DistributeGroupByRowCountThreshold {
			log.VEventf(ctx, 2, "large aggregation recommends plan distribution")
			aggRec = shouldDistribute
		}
		return rec.compose(aggRec), nil

	case *indexJoinNode:
		if n.table.lockingStrength != descpb.ScanLockingStrength_FOR_NONE {
			// Index joins that are performing row-level locking cannot
			// currently be distributed because their locks would not be
			// propagated back to the root transaction coordinator.
			// TODO(nvanbenschoten): lift this restriction.
			return cannotDistribute, cannotDistributeRowLevelLockingErr
		}
		// n.table doesn't have meaningful spans, but we need to check support (e.g.
		// for any filtering expression).
		if _, err := checkSupportForPlanNode(ctx, n.table, distSQLVisitor, sd); err != nil {
			return cannotDistribute, err
		}
		return checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd)

	case *invertedFilterNode:
		return checkSupportForInvertedFilterNode(ctx, n, distSQLVisitor, sd)

	case *invertedJoinNode:
		if n.table.lockingStrength != descpb.ScanLockingStrength_FOR_NONE {
			// Inverted joins that are performing row-level locking cannot
			// currently be distributed because their locks would not be
			// propagated back to the root transaction coordinator.
			// TODO(nvanbenschoten): lift this restriction.
			return cannotDistribute, cannotDistributeRowLevelLockingErr
		}
		if err := checkExprForDistSQL(n.onExpr, distSQLVisitor); err != nil {
			return cannotDistribute, err
		}
		rec, err := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd)
		if err != nil {
			return cannotDistribute, err
		}
		// TODO(yuzefovich): we might want to be smarter about this and don't
		// force distribution with small inputs.
		log.VEventf(ctx, 2, "inverted join recommends plan distribution")
		return rec.compose(shouldDistribute), nil

	case *joinNode:
		if err := checkExprForDistSQL(n.pred.onCond, distSQLVisitor); err != nil {
			return cannotDistribute, err
		}
		recLeft, err := checkSupportForPlanNode(ctx, n.left, distSQLVisitor, sd)
		if err != nil {
			return cannotDistribute, err
		}
		recRight, err := checkSupportForPlanNode(ctx, n.right, distSQLVisitor, sd)
		if err != nil {
			return cannotDistribute, err
		}
		rec := recLeft.compose(recRight)
		if len(n.pred.leftEqualityIndices) > 0 {
			// We can partition both streams on the equality columns.
			if n.estimatedLeftRowCount == 0 && n.estimatedRightRowCount == 0 {
				log.VEventf(ctx, 2, "join (no stats) recommends plan distribution")
				rec = rec.compose(shouldDistribute)
			} else if n.estimatedLeftRowCount+n.estimatedRightRowCount >= sd.DistributeJoinRowCountThreshold {
				log.VEventf(ctx, 2, "large join recommends plan distribution")
				rec = rec.compose(shouldDistribute)
			}
		}
		return rec, nil

	case *limitNode:
		// Note that we don't need to check whether we support distribution of
		// n.countExpr or n.offsetExpr because those expressions are evaluated
		// locally, during the physical planning.
		return checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd)

	case *lookupJoinNode:
		if n.remoteLookupExpr != nil || n.remoteOnlyLookups {
			// This is a locality optimized join.
			return cannotDistribute, localityOptimizedOpNotDistributableErr
		}
		if n.table.lockingStrength != descpb.ScanLockingStrength_FOR_NONE {
			// Lookup joins that are performing row-level locking cannot
			// currently be distributed because their locks would not be
			// propagated back to the root transaction coordinator.
			// TODO(nvanbenschoten): lift this restriction.
			return cannotDistribute, cannotDistributeRowLevelLockingErr
		}

		if err := checkExprForDistSQL(n.lookupExpr, distSQLVisitor); err != nil {
			return cannotDistribute, err
		}
		if err := checkExprForDistSQL(n.remoteLookupExpr, distSQLVisitor); err != nil {
			return cannotDistribute, err
		}
		if err := checkExprForDistSQL(n.onCond, distSQLVisitor); err != nil {
			return cannotDistribute, err
		}
		rec, err := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd)
		if err != nil {
			return cannotDistribute, err
		}
		return rec.compose(canDistribute), nil

	case *ordinalityNode:
		// WITH ORDINALITY never gets distributed so that the gateway node can
		// always number each row in order.
		return cannotDistribute, ordinalityNotDistributableErr

	case *projectSetNode:
		for i := range n.exprs {
			if err := checkExprForDistSQL(n.exprs[i], distSQLVisitor); err != nil {
				return cannotDistribute, err
			}
		}
		return checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd)

	case *renderNode:
		for _, e := range n.render {
			if err := checkExprForDistSQL(e, distSQLVisitor); err != nil {
				return cannotDistribute, err
			}
		}
		return checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd)

	case *scanNode:
		if n.lockingStrength != descpb.ScanLockingStrength_FOR_NONE {
			// Scans that are performing row-level locking cannot currently be
			// distributed because their locks would not be propagated back to
			// the root transaction coordinator.
			// TODO(nvanbenschoten): lift this restriction.
			return cannotDistribute, cannotDistributeRowLevelLockingErr
		}

		if n.localityOptimized {
			// This is a locality optimized scan.
			return cannotDistribute, localityOptimizedOpNotDistributableErr
		}
		// TODO(yuzefovich): consider using the soft limit in making a decision
		// here.
		scanRec := canDistribute
		if n.estimatedRowCount != 0 && n.estimatedRowCount >= sd.DistributeScanRowCountThreshold {
			log.VEventf(ctx, 2, "large scan recommends plan distribution")
			scanRec = shouldDistribute
		}
		if n.isFull && (n.estimatedRowCount == 0 || sd.AlwaysDistributeFullScans) {
			// In the absence of table stats, we default to always distributing
			// full scans.
			log.VEventf(ctx, 2, "full scan recommends plan distribution")
			scanRec = shouldDistribute
		}
		return scanRec, nil

	case *sortNode:
		rec, err := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd)
		if err != nil {
			return cannotDistribute, err
		}
		// Don't force distribution if we expect to process small number of
		// rows.
		sortRec := canDistribute
		if n.estimatedInputRowCount == 0 {
			log.VEventf(ctx, 2, "sort (no stats) recommends plan distribution")
			sortRec = shouldDistribute
		} else if n.estimatedInputRowCount >= sd.DistributeSortRowCountThreshold {
			log.VEventf(ctx, 2, "large sort recommends plan distribution")
			sortRec = shouldDistribute
		}
		return rec.compose(sortRec), nil

	case *topKNode:
		rec, err := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd)
		if err != nil {
			return cannotDistribute, err
		}
		// Don't force distribution if we expect to process small number of
		// rows.
		topKRec := canDistribute
		if n.estimatedInputRowCount == 0 {
			log.VEventf(ctx, 2, "top k (no stats) recommends plan distribution")
			topKRec = shouldDistribute
		} else if n.estimatedInputRowCount >= sd.DistributeSortRowCountThreshold {
			log.VEventf(ctx, 2, "large top k recommends plan distribution")
			topKRec = shouldDistribute
		}
		return rec.compose(topKRec), nil

	case *unaryNode:
		return canDistribute, nil

	case *unionNode:
		recLeft, err := checkSupportForPlanNode(ctx, n.left, distSQLVisitor, sd)
		if err != nil {
			return cannotDistribute, err
		}
		recRight, err := checkSupportForPlanNode(ctx, n.right, distSQLVisitor, sd)
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
				if err := checkExprForDistSQL(expr, distSQLVisitor); err != nil {
					return cannotDistribute, err
				}
			}
		}
		return canDistribute, nil

	case *vectorSearchNode, *vectorMutationSearchNode:
		// Don't allow distribution for vector search operators, for now.
		return cannotDistribute, cannotDistributeVectorSearchErr

	case *windowNode:
		rec, err := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd)
		if err != nil {
			return cannotDistribute, err
		}
		for _, f := range n.funcs {
			if len(f.partitionIdxs) > 0 {
				// If at least one function has PARTITION BY clause, then we
				// should distribute the execution.
				// TODO(yuzefovich): we might want to be smarter about this and
				// don't force distribution with small inputs.
				log.VEventf(ctx, 2, "window function with PARTITION BY recommends plan distribution")
				return rec.compose(shouldDistribute), nil
			}
		}
		return rec.compose(canDistribute), nil

	case *zeroNode:
		return canDistribute, nil

	case *zigzagJoinNode:
		for _, side := range n.sides {
			if side.scan.lockingStrength != descpb.ScanLockingStrength_FOR_NONE {
				// ZigZag joins that are performing row-level locking cannot
				// currently be distributed because their locks would not be
				// propagated back to the root transaction coordinator.
				// TODO(nvanbenschoten): lift this restriction.
				return cannotDistribute, cannotDistributeRowLevelLockingErr
			}
		}
		if err := checkExprForDistSQL(n.onCond, distSQLVisitor); err != nil {
			return cannotDistribute, err
		}
		// TODO(yuzefovich): we might want to be smarter about this and don't
		// force distribution with small inputs.
		log.VEventf(ctx, 2, "zigzag join recommends plan distribution")
		return shouldDistribute, nil

	default:
		return cannotDistribute, planNodeNotSupportedErr
	}
}

func checkSupportForInvertedFilterNode(
	ctx context.Context,
	n *invertedFilterNode,
	distSQLVisitor *distSQLExprCheckVisitor,
	sd *sessiondata.SessionData,
) (distRecommendation, error) {
	rec, err := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd)
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
	if n.expression.Left != nil || n.expression.Right != nil {
		return cannotDistribute, invertedFilterNotDistributableErr
	}
	// TODO(yuzefovich): we might want to be smarter about this and don't force
	// distribution with small inputs.
	log.VEventf(ctx, 2, "inverted filter (union of inverted spans) recommends plan distribution")
	return rec.compose(shouldDistribute), nil
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
	// NodeDraining means that the node should be avoided because
	// it's draining.
	NodeDraining
)

// spanPartitionState captures information about the current state of the
// partitioning that has occurred during the planning process.
type spanPartitionState struct {
	// partitionSpanDecisions is a mapping from a SpanPartitionReason to the number of
	// times we have picked an instance for that reason.
	partitionSpanDecisions [SpanPartitionReasonMax]int

	// partitionSpans is a mapping from a SQLInstanceID to the number of
	// partition spans that have been assigned to that node.
	partitionSpans map[base.SQLInstanceID]int

	// totalPartitionSpans is the total number of partitions that have been processed
	// so far.
	totalPartitionSpans int

	testingOverrideRandomSelection func() base.SQLInstanceID
}

// update updates the spanPartitionState with the information about the new span partition.
func (p *spanPartitionState) update(
	partitionNode base.SQLInstanceID, partitionReason SpanPartitionReason,
) {
	p.totalPartitionSpans++
	p.partitionSpanDecisions[partitionReason]++
	p.partitionSpans[partitionNode]++
}

// PlanningCtx contains data used and updated throughout the planning process of
// a single query.
type PlanningCtx struct {
	ExtendedEvalCtx *extendedEvalContext

	localityFilter roachpb.Locality

	// spanPartitionState captures information about the current state of the
	// partitioning that has occurred during the planning process.
	spanPartitionState *spanPartitionState

	spanIter physicalplan.SpanResolverIterator
	// nodeStatuses contains info for all SQLInstanceIDs that are referenced by
	// any PhysicalPlan we generate with this context.
	nodeStatuses map[base.SQLInstanceID]NodeStatus

	infra *physicalplan.PhysicalInfrastructure

	// isLocal is set to true if we're planning this query on a single node.
	isLocal bool
	// distSQLProhibitedErr, if set, indicates why the plan couldn't be
	// distributed.
	distSQLProhibitedErr error
	planner              *planner

	stmtType tree.StatementReturnType
	// planDepth is set to the current depth of the planNode tree. It's used to
	// keep track of whether it's valid to run a root node in a special fast path
	// mode.
	planDepth int

	// If set, the DistSQL diagram is **not** generated and not added to the
	// trace (when the tracing is enabled). We generally want to include it, but
	// on the main query path the overhead becomes too large while we also have
	// other ways to get the diagram.
	skipDistSQLDiagramGeneration bool

	// stmtForDistSQLDiagram, if set, will be used when generating the DistSQL
	// diagram to specify the SQL stmt that this plan corresponds to.
	stmtForDistSQLDiagram string

	// If set, the flows for the physical plan will be passed to this function.
	// The flows are not safe for use past the lifetime of the saveFlows function.
	saveFlows SaveFlowsFunc

	// If set, we will record the mapping from planNode to tracing metadata to
	// later allow associating statistics with the planNode.
	associateNodeWithComponents func(exec.Node, execComponents)

	// If set, statement execution stats should be collected.
	collectExecStats bool

	// parallelizeScansIfLocal indicates whether we might want to create
	// multiple table readers if the physical plan ends up being fully local.
	// This value is determined based on whether there are any mutations in the
	// plan (which prohibit all concurrency) and whether all parts of the plan
	// are supported natively by the vectorized engine.
	parallelizeScansIfLocal bool

	// Set if this is either a subquery or a postquery (i.e. not the "main"
	// query).
	subOrPostQuery bool

	// mustUseLeafTxn, if set, indicates that this PlanningCtx is used to handle
	// one of the plans that will run in parallel with other plans. As such, the
	// DistSQL planner will need to use the LeafTxn (even if it's not needed
	// based on other "regular" factors).
	mustUseLeafTxn bool

	// onFlowCleanup contains non-nil functions that will be called after the
	// local flow finished running and is being cleaned up. It allows us to
	// release the resources that are acquired during the physical planning and
	// are being held onto throughout the whole flow lifecycle.
	// Note that these functions might not be called in some cases (when
	// DistSQLPlanner.Run is not called), but that is ok since on the main query
	// path it will get called.
	onFlowCleanup []func()

	// This is true if plan is a simple insert that can be vectorized.
	isVectorInsert bool

	// OverridePlannerMon, if set, will be used instead of the Planner.Mon() as
	// the parent monitor for the DistSQL flow.
	OverridePlannerMon *mon.BytesMonitor
}

var _ physicalplan.ExprContext = &PlanningCtx{}

// NewPhysicalPlan creates an empty PhysicalPlan, backed by the
// PlanInfrastructure in the planning context.
//
// Note that any processors created in the physical plan cannot be discarded;
// they have to be part of the final plan.
func (p *PlanningCtx) NewPhysicalPlan() *PhysicalPlan {
	return &PhysicalPlan{
		PhysicalPlan: physicalplan.MakePhysicalPlan(p.infra),
	}
}

// EvalContext returns the associated EvalContext, or nil if there isn't one.
func (p *PlanningCtx) EvalContext() *eval.Context {
	if p.ExtendedEvalCtx == nil {
		return nil
	}
	return &p.ExtendedEvalCtx.Context
}

// IsLocal returns true if this PlanningCtx is being used to plan a query that
// has no remote flows.
func (p *PlanningCtx) IsLocal() bool {
	return p.isLocal
}

// getPortalPauseInfo returns the portal pause info if the current planner is
// for a pausable portal. Otherwise, returns nil.
func (p *PlanningCtx) getPortalPauseInfo() *portalPauseInfo {
	if p.planner != nil && p.planner.pausablePortal != nil && p.planner.pausablePortal.pauseInfo != nil {
		return p.planner.pausablePortal.pauseInfo
	}
	return nil
}

// setUpForMainQuery updates the PlanningCtx for the main query path.
func (p *PlanningCtx) setUpForMainQuery(
	ctx context.Context, planner *planner, recv *DistSQLReceiver,
) {
	p.stmtType = recv.stmtType
	// Skip the diagram generation since on this "main" query path we can get it
	// via the statement bundle.
	p.skipDistSQLDiagramGeneration = true
	if planner.execCfg.TestingKnobs.TestingSaveFlows != nil {
		p.saveFlows = planner.execCfg.TestingKnobs.TestingSaveFlows(planner.stmt.SQL)
	} else if planner.instrumentation.ShouldSaveFlows() {
		p.saveFlows = getDefaultSaveFlowsFunc(ctx, planner, planComponentTypeMainQuery)
	}
	p.associateNodeWithComponents = planner.instrumentation.getAssociateNodeWithComponentsFn()
	p.collectExecStats = planner.instrumentation.ShouldCollectExecStats()
}

// SaveFlowsFunc is the signature for a function used to examine the physical
// plan for a query. Implementations may not be concurrency-safe.
type SaveFlowsFunc func(
	flows map[base.SQLInstanceID]*execinfrapb.FlowSpec,
	opChains execopnode.OpChains,
	localProcessors []execinfra.LocalProcessor,
	vectorized bool,
) error

// getDefaultSaveFlowsFunc returns the default function used to save physical
// plans and their diagrams. The returned function is **not** concurrency-safe.
func getDefaultSaveFlowsFunc(
	ctx context.Context, planner *planner, typ planComponentType,
) SaveFlowsFunc {
	return func(flows map[base.SQLInstanceID]*execinfrapb.FlowSpec, opChains execopnode.OpChains, localProcessors []execinfra.LocalProcessor, vectorized bool) error {
		var diagram execinfrapb.FlowDiagram
		if planner.instrumentation.shouldSaveDiagrams() {
			diagramFlags := execinfrapb.DiagramFlags{
				MakeDeterministic: planner.execCfg.TestingKnobs.DeterministicExplain,
			}
			var err error
			diagram, err = flowSpecsToDiagram(planner, flows, diagramFlags)
			if err != nil {
				return err
			}
		}
		var explainVec []string
		var explainVecVerbose []string
		if planner.instrumentation.collectBundle && vectorized {
			flowCtx := newFlowCtxForExplainPurposes(ctx, planner)
			flowCtx.Local = !planner.curPlan.flags.IsDistributed()
			getExplain := func(verbose bool) []string {
				gatewaySQLInstanceID := planner.extendedEvalCtx.DistSQLPlanner.gatewaySQLInstanceID
				// When we're collecting the bundle, we're always recording the
				// stats.
				const recordingStats = true
				explain, err := colflow.ExplainVec(
					ctx, flowCtx, flows, localProcessors, opChains,
					gatewaySQLInstanceID, verbose, recordingStats,
				)
				if err != nil {
					// In some edge cases (like when subqueries are present or
					// when certain component doesn't implement execopnode.OpNode
					// interface) an error might occur. In such scenario, we
					// don't want to fail the collection of the bundle, so we're
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
// FlowDiagram.
func flowSpecsToDiagram(
	p *planner,
	flows map[base.SQLInstanceID]*execinfrapb.FlowSpec,
	diagramFlags execinfrapb.DiagramFlags,
) (execinfrapb.FlowDiagram, error) {
	var stmtStr string
	if p != nil && p.stmt.AST != nil {
		stmtStr = p.stmt.String()
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
	if len(p.onFlowCleanup) == 0 {
		// Make sure to release the physical infrastructure after the execution
		// finishes.
		return p.infra.Release
	}
	return func() {
		p.infra.Release()
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
//
// PhysicalPlan is immutable after its finalization.
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

// SpanPartitionReason is the reason why a span was assigned to a particular
// node or SQL Instance ID.
type SpanPartitionReason int32

const (
	// SpanPartitionReason_UNSPECIFIED is reported when the reason is unspecified.
	SpanPartitionReason_UNSPECIFIED SpanPartitionReason = iota
	// SpanPartitionReason_GATEWAY_TARGET_UNHEALTHY is reported when the target
	// node is unhealthy and so we default to the gateway node.
	SpanPartitionReason_GATEWAY_TARGET_UNHEALTHY
	// SpanPartitionReason_GATEWAY_NO_HEALTHY_INSTANCES is reported when there are
	// no healthy instances and so we default to the gateway node.
	SpanPartitionReason_GATEWAY_NO_HEALTHY_INSTANCES
	// SpanPartitionReason_GATEWAY_ON_ERROR is reported when there is an error and
	// so we default to the gateway node.
	SpanPartitionReason_GATEWAY_ON_ERROR
	// SpanPartitionReason_TARGET_HEALTHY is reported when the target node is
	// healthy.
	SpanPartitionReason_TARGET_HEALTHY
	// SpanPartitionReason_CLOSEST_LOCALITY_MATCH is reported when we picked an
	// instance with the closest match to the provided locality filter.
	SpanPartitionReason_CLOSEST_LOCALITY_MATCH
	// SpanPartitionReason_GATEWAY_NO_LOCALITY_MATCH is reported when there is no
	// match to the provided locality filter and so we default to the gateway.
	SpanPartitionReason_GATEWAY_NO_LOCALITY_MATCH
	// SpanPartitionReason_LOCALITY_FILTERED_RANDOM is reported when there is no
	// match to the provided locality filter and the gateway is not eligible. In
	// this case we pick a random available instance.
	SpanPartitionReason_LOCALITY_FILTERED_RANDOM
	// SpanPartitionReason_ROUND_ROBIN is reported when there is no locality info
	// on any of the instances and so we default to a naive round-robin strategy.
	SpanPartitionReason_ROUND_ROBIN
	// SpanPartitionReason_GOSSIP_GATEWAY_TARGET_UNHEALTHY is reported when the
	// target node retrieved via gossip is deemed unhealthy. In this case we
	// default to the gateway node.
	SpanPartitionReason_GOSSIP_GATEWAY_TARGET_UNHEALTHY
	// SpanPartitionReason_GOSSIP_TARGET_HEALTHY is reported when the
	// target node retrieved via gossip is deemed healthy.
	SpanPartitionReason_GOSSIP_TARGET_HEALTHY
	// SpanPartitionReason_LOCALITY_FILTERED_RANDOM_GATEWAY_OVERLOADED is reported
	// when there is no match to the provided locality filter and the gateway is
	// eligible but overloaded with other partitions. In this case we pick a
	// random instance apart from the gateway.
	SpanPartitionReason_LOCALITY_FILTERED_RANDOM_GATEWAY_OVERLOADED
	// SpanPartitionReasonMax tracks the number of SpanPartitionReason objects.
	SpanPartitionReasonMax
)

func (r SpanPartitionReason) String() string {
	switch r {
	case SpanPartitionReason_UNSPECIFIED:
		return "unspecified"
	case SpanPartitionReason_GATEWAY_TARGET_UNHEALTHY:
		return "gateway-target-unhealthy"
	case SpanPartitionReason_GATEWAY_NO_HEALTHY_INSTANCES:
		return "gateway-no-healthy-instances"
	case SpanPartitionReason_GATEWAY_ON_ERROR:
		return "gateway-on-error"
	case SpanPartitionReason_TARGET_HEALTHY:
		return "target-healthy"
	case SpanPartitionReason_CLOSEST_LOCALITY_MATCH:
		return "closest-locality-match"
	case SpanPartitionReason_GATEWAY_NO_LOCALITY_MATCH:
		return "gateway-no-locality-match"
	case SpanPartitionReason_LOCALITY_FILTERED_RANDOM:
		return "locality-filtered-random"
	case SpanPartitionReason_ROUND_ROBIN:
		return "round-robin"
	case SpanPartitionReason_GOSSIP_GATEWAY_TARGET_UNHEALTHY:
		return "gossip-gateway-target-unhealthy"
	case SpanPartitionReason_GOSSIP_TARGET_HEALTHY:
		return "gossip-target-healthy"
	case SpanPartitionReason_LOCALITY_FILTERED_RANDOM_GATEWAY_OVERLOADED:
		return "locality-filtered-random-gateway-overloaded"
	default:
		return "unknown"
	}
}

// SpanPartition associates a subset of spans with a specific SQL instance,
// chosen to have the most efficient access to those spans. In the single-tenant
// case, the instance is the one running on the same node as the leaseholder for
// those spans.
type SpanPartition struct {
	SQLInstanceID base.SQLInstanceID
	Spans         roachpb.Spans

	haveRangeInfo bool
	numRanges     int
}

// NumRanges returns the number of ranges in a partition only if this
// information is present. The returned bool indicates if the result is correct.
// Note that the returned count is not for distinct ranges. If there are two
// spans belonging to the same range in the partition (ex. two disjoint spans),
// this counts as two ranges.
func (sp *SpanPartition) NumRanges() (int, bool) {
	if !sp.haveRangeInfo {
		return 0, false
	}
	return sp.numRanges, true
}

// MakeSpanPartitionWithRangeCount constructs a SpanPartition with the
// specified count of ranges.
func MakeSpanPartitionWithRangeCount(
	instanceID base.SQLInstanceID, spans roachpb.Spans, numRanges int,
) SpanPartition {
	return SpanPartition{
		SQLInstanceID: instanceID,
		Spans:         spans,
		haveRangeInfo: true,
		numRanges:     numRanges,
	}
}

type distSQLNodeHealth struct {
	gossip             gossip.OptionalGossip
	isAvailable        func(base.SQLInstanceID) bool
	connHealthSystem   func(roachpb.NodeID, rpc.ConnectionClass) error
	connHealthInstance func(base.SQLInstanceID, string) error
}

func (h *distSQLNodeHealth) checkSystem(
	ctx context.Context, sqlInstanceID base.SQLInstanceID,
) error {
	{
		// NB: as of #22658, connHealthSystem does not work as expected; see the
		// comment within. We still keep this code for now because in
		// practice, once the node is down it will prevent using this node
		// 90% of the time (it gets used around once per second as an
		// artifact of rpcContext's reconnection mechanism at the time of
		// writing). This is better than having it used in 100% of cases
		// (until the liveness check below kicks in).
		if err := h.connHealthSystem(roachpb.NodeID(sqlInstanceID), rpc.DefaultClass); err != nil {
			// This host isn't known to be healthy. Don't use it (use the gateway
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
	g, ok := h.gossip.Optional(distsql.MultiTenancyIssueNo)
	if !ok {
		return errors.AssertionFailedf("gossip is expected to be available for the system tenant")
	}
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

	return nil
}

// checkInstanceHealthAndVersionSystem returns information about a node's health
// and compatibility. The info is also recorded in planCtx.nodeStatuses. It
// should only be used by the system tenant.
func (dsp *DistSQLPlanner) checkInstanceHealthAndVersionSystem(
	ctx context.Context, planCtx *PlanningCtx, sqlInstanceID base.SQLInstanceID,
) NodeStatus {
	if status, ok := planCtx.nodeStatuses[sqlInstanceID]; ok {
		return status
	}

	status := NodeOK
	if err := dsp.nodeHealth.checkSystem(ctx, sqlInstanceID); err != nil {
		status = NodeUnhealthy
	}
	planCtx.nodeStatuses[sqlInstanceID] = status
	return status
}

// PartitionSpansBoundGranularity indicates how "granular" boundaries between
// spans passed to PartitionSpans are.
type PartitionSpansBoundGranularity int

const (
	// PartitionSpansBoundDefault indicates that consecutive spans passed to
	// PartitionSpans can always be processed on different instances. This is
	// the case when the spans boundaries are at SQL row level or higher (index,
	// table, tenant, etc).
	PartitionSpansBoundDefault PartitionSpansBoundGranularity = iota
	// PartitionSpansBoundCFWithinRow indicates that consecutive spans passed to
	// PartitionSpans _might_ be part of the same SQL row (when they correspond
	// to different column families within a single row), and in that case those
	// spans need to be assigned the same instance.
	PartitionSpansBoundCFWithinRow
)

// PartitionSpans finds out which nodes are owners for ranges touching the
// given spans, and splits the spans according to owning nodes. The result is a
// set of SpanPartitions (guaranteed one for each relevant node), which form a
// partitioning of the spans (i.e. they are non-overlapping and their union is
// exactly the original set of spans).
//
// PartitionSpans does its best to not assign ranges on nodes that are known to
// be unhealthy. The ranges owned by such nodes are assigned to the gateway.
func (dsp *DistSQLPlanner) PartitionSpans(
	ctx context.Context,
	planCtx *PlanningCtx,
	spans roachpb.Spans,
	bound PartitionSpansBoundGranularity,
) ([]SpanPartition, error) {
	partitions, _, err := dsp.partitionSpansEx(ctx, planCtx, spans, bound)
	return partitions, err
}

// partitionSpansEx is the same as PartitionSpans but additionally returns a
// boolean indicating whether the misplanned ranges metadata should not be
// generated.
func (dsp *DistSQLPlanner) partitionSpansEx(
	ctx context.Context,
	planCtx *PlanningCtx,
	spans roachpb.Spans,
	bound PartitionSpansBoundGranularity,
) (_ []SpanPartition, ignoreMisplannedRanges bool, _ error) {
	if len(spans) == 0 {
		return nil, false, errors.AssertionFailedf("no spans")
	}
	if planCtx.isLocal {
		// If we're planning locally, map all spans to the gateway. Note that we
		// always ignore misplanned ranges for local-only plans, and we choose
		// to return `true` to explicitly highlight this fact, yet the boolean
		// doesn't really matter.
		return []SpanPartition{{SQLInstanceID: dsp.gatewaySQLInstanceID, Spans: spans}},
			true /* ignoreMisplannedRanges */, nil
	}
	if dsp.useGossipPlanning(ctx, planCtx) {
		return dsp.deprecatedPartitionSpansSystem(ctx, planCtx, spans)
	}
	return dsp.partitionSpans(ctx, planCtx, spans, bound)
}

// partitionSpan takes a single span and splits it up according to the owning
// nodes (if the span touches multiple ranges).
//
//   - partitions is the set of SpanPartitions so far. The updated set is
//     returned.
//   - nodeMap maps a SQLInstanceID to an index inside the partitions array. If
//     the SQL instance chosen for the span is not in this map, then a new
//     SpanPartition is appended to partitions and nodeMap is updated accordingly.
//   - getSQLInstanceIDForKVNodeID is a resolver from the KV node ID to the SQL
//     instance ID.
//
// The updated array of SpanPartitions is returned as well as the index into
// that array pointing to the SpanPartition that included the last part of the
// span.
func (dsp *DistSQLPlanner) partitionSpan(
	ctx context.Context,
	planCtx *PlanningCtx,
	span roachpb.Span,
	partitions []SpanPartition,
	nodeMap map[base.SQLInstanceID]int,
	getSQLInstanceIDForKVNodeID func(roachpb.NodeID) (base.SQLInstanceID, SpanPartitionReason),
	ignoreMisplannedRanges *bool,
) (_ []SpanPartition, lastPartitionIdx int, _ error) {
	it := planCtx.spanIter
	// rSpan is the span we are currently partitioning.
	rSpan, err := keys.SpanAddr(span)
	if err != nil {
		return nil, 0, err
	}

	var lastSQLInstanceID base.SQLInstanceID
	// lastKey maintains the EndKey of the last piece of `span`.
	lastKey := rSpan.Key
	if log.V(1) {
		log.Infof(ctx, "partitioning span %s", span)
	}
	// We break up rSpan into its individual ranges (which may or may not be on
	// separate nodes). We then create "partitioned spans" using the end keys of
	// these individual ranges.
	for it.Seek(ctx, span, kvcoord.Ascending); ; it.Next(ctx) {
		if !it.Valid() {
			return nil, 0, it.Error()
		}
		replDesc, ignore, err := it.ReplicaInfo(ctx)
		if err != nil {
			return nil, 0, err
		}
		*ignoreMisplannedRanges = *ignoreMisplannedRanges || ignore
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

		sqlInstanceID, reason := getSQLInstanceIDForKVNodeID(replDesc.NodeID)
		planCtx.spanPartitionState.update(sqlInstanceID, reason)
		partitionIdx, inNodeMap := nodeMap[sqlInstanceID]
		if !inNodeMap {
			partitionIdx = len(partitions)
			partitions = append(partitions, SpanPartition{SQLInstanceID: sqlInstanceID, numRanges: 0, haveRangeInfo: true})
			nodeMap[sqlInstanceID] = partitionIdx
		}
		partition := &partitions[partitionIdx]
		lastPartitionIdx = partitionIdx

		if len(span.EndKey) == 0 {
			// If we see a span to partition that has no end key, it means that
			// we're going to do a point lookup on the start key of this span.
			// Thus, we include the span into partition.Spans without trying to
			// merge it with the last span.
			partition.Spans = append(partition.Spans, span)
			partition.numRanges += 1
			if log.ExpensiveLogEnabled(ctx, 2) {
				log.VEventf(ctx, 2, "partition span: %s, instance ID: %d, reason: %s",
					span, sqlInstanceID, reason)
			}
			break
		}

		// Limit the end key to the end of the span we are resolving.
		endKey := desc.EndKey
		if rSpan.EndKey.Less(endKey) {
			endKey = rSpan.EndKey
		}

		partitionedSpan := roachpb.Span{Key: lastKey.AsRawKey(), EndKey: endKey.AsRawKey()}
		if log.ExpensiveLogEnabled(ctx, 2) {
			log.VEventf(ctx, 2, "partition span: %s, instance ID: %d, reason: %s",
				partitionedSpan.String(), sqlInstanceID, reason.String())
		}

		if lastSQLInstanceID == sqlInstanceID {
			// Two consecutive ranges on the same node, merge the spans.
			partition.Spans[len(partition.Spans)-1].EndKey = endKey.AsRawKey()
		} else {
			partition.Spans = append(partition.Spans, partitionedSpan)
		}
		partition.numRanges += 1

		if !endKey.Less(rSpan.EndKey) {
			// Done.
			break
		}

		lastKey = endKey
		lastSQLInstanceID = sqlInstanceID
	}
	return partitions, lastPartitionIdx, nil
}

// deprecatedPartitionSpansSystem finds node owners for ranges touching the given spans
// for a system tenant.
func (dsp *DistSQLPlanner) deprecatedPartitionSpansSystem(
	ctx context.Context, planCtx *PlanningCtx, spans roachpb.Spans,
) (partitions []SpanPartition, ignoreMisplannedRanges bool, _ error) {
	nodeMap := make(map[base.SQLInstanceID]int)
	resolver := func(nodeID roachpb.NodeID) (base.SQLInstanceID, SpanPartitionReason) {
		return dsp.deprecatedHealthySQLInstanceIDForKVNodeIDSystem(ctx, planCtx, nodeID)
	}
	for _, span := range spans {
		var err error
		partitions, _, err = dsp.partitionSpan(
			ctx, planCtx, span, partitions, nodeMap, resolver, &ignoreMisplannedRanges,
		)
		if err != nil {
			return nil, false, err
		}
	}
	return partitions, ignoreMisplannedRanges, nil
}

// partitionSpans assigns SQL instances to spans. In mixed sql and KV mode it
// generally assigns each span to the instance hosted on the KV node chosen by
// the configured replica oracle, while in clusters operating with standalone
// SQL instances it performs locality-aware physical planning among all
// available SQL instances if the locality info is available on at least some of
// the instances, and it falls back to naive round-robin assignment if not.
func (dsp *DistSQLPlanner) partitionSpans(
	ctx context.Context,
	planCtx *PlanningCtx,
	spans roachpb.Spans,
	bound PartitionSpansBoundGranularity,
) (partitions []SpanPartition, ignoreMisplannedRanges bool, _ error) {
	resolver, err := dsp.makeInstanceResolver(ctx, planCtx)
	if err != nil {
		return nil, false, err
	}
	nodeMap := make(map[base.SQLInstanceID]int)
	var lastKey roachpb.Key
	var lastPartitionIdx int
	for _, span := range spans {
		if bound == PartitionSpansBoundCFWithinRow {
			// Rows with column families may have been split into different
			// spans. These spans should be assigned the same pod so that the
			// pod can stitch together the rows correctly. Split rows are in
			// adjacent spans.
			// TODO(112702): here we can stitch together spans in more cases
			// then necessary because we currently can't distinguish some valid
			// keys corresponding to a full SQL row (i.e. single CF case) vs
			// those that are part of a SQL row (i.e. multiple CFs case). If we
			// address #112702, we'll be able to make the check here precise.
			if safeKey, err := keys.EnsureSafeSplitKey(span.Key); err == nil && len(safeKey) > 0 {
				if safeKey.Equal(lastKey) {
					if log.V(1) {
						log.Infof(ctx, "stitching span %s into the previous span partition", span)
					}
					// TODO(yuzefovich): we're not updating
					// SpanPartition.numRanges as well as spanPartitionState
					// here. At the moment, that info is only used with
					// PartitionSpansBoundDefault, which makes this code
					// unreachable. If that ever changes, we'll need to be
					// smarter here.
					partition := &partitions[lastPartitionIdx]
					partition.Spans = append(partition.Spans, span)
					continue
				}
				lastKey = safeKey
			}
		}
		partitions, lastPartitionIdx, err = dsp.partitionSpan(
			ctx, planCtx, span, partitions, nodeMap, resolver, &ignoreMisplannedRanges,
		)
		if err != nil {
			return nil, false, err
		}
	}
	return partitions, ignoreMisplannedRanges, nil
}

// deprecatedHealthySQLInstanceIDForKVNodeIDSystem returns the SQL instance that
// should handle the range with the given node ID when planning is done on
// behalf of the system tenant. It ensures that the chosen SQL instance is
// healthy and of the compatible DistSQL version.
func (dsp *DistSQLPlanner) deprecatedHealthySQLInstanceIDForKVNodeIDSystem(
	ctx context.Context, planCtx *PlanningCtx, nodeID roachpb.NodeID,
) (base.SQLInstanceID, SpanPartitionReason) {
	sqlInstanceID := base.SQLInstanceID(nodeID)
	status := dsp.checkInstanceHealthAndVersionSystem(ctx, planCtx, sqlInstanceID)
	// If the node is unhealthy, use the gateway to process this span instead of
	// the unhealthy host. An empty address indicates an unhealthy host.
	reason := SpanPartitionReason_GOSSIP_TARGET_HEALTHY
	if status != NodeOK {
		log.VEventf(ctx, 2, "not planning on node %d: %s", sqlInstanceID, status)
		sqlInstanceID = dsp.gatewaySQLInstanceID
		reason = SpanPartitionReason_GOSSIP_GATEWAY_TARGET_UNHEALTHY
	}
	return sqlInstanceID, reason
}

// checkInstanceHealth returns the instance health status by dialing the node.
// It also caches the result to avoid redialing for a query.
func (dsp *DistSQLPlanner) checkInstanceHealth(
	instanceID base.SQLInstanceID,
	instanceRPCAddr string,
	isDraining bool,
	nodeStatusesCache map[base.SQLInstanceID]NodeStatus,
) NodeStatus {
	if nodeStatusesCache != nil {
		if status, ok := nodeStatusesCache[instanceID]; ok {
			return status
		}
	}
	status := NodeOK
	if isDraining {
		status = NodeDraining
	} else if err := dsp.nodeHealth.connHealthInstance(instanceID, instanceRPCAddr); err != nil {
		if errors.Is(err, rpc.ErrNotHeartbeated) {
			// Consider ErrNotHeartbeated as a temporary error (see its description) and
			// avoid caching its result, as it can resolve to a more accurate result soon.
			// It's more reasonable to plan on using this node, and if such a node is
			// unhealthy, the retry-as-local mechanism would retry the operation on the
			// gateway.
			return NodeOK
		}
		status = NodeUnhealthy
	}

	if nodeStatusesCache != nil {
		nodeStatusesCache[instanceID] = status
	}
	return status
}

// healthySQLInstanceIDForKVNodeHostedInstanceResolver returns the SQL instance
// ID for an instance that is hosted in the process of a KV node. Currently SQL
// instances that run in KV node processes have IDs fixed to be equal to the KV
// nodes' IDs, and all of the SQL instances for a given tenant are _either_ run
// in this mixed mode or standalone, meaning if this server is in mixed mode, we
// can safely assume every other server is as well, and thus has IDs matching
// node IDs.
//
// If the given node is not healthy, the gateway node is returned.
func (dsp *DistSQLPlanner) healthySQLInstanceIDForKVNodeHostedInstanceResolver(
	ctx context.Context, planCtx *PlanningCtx,
) func(nodeID roachpb.NodeID) (base.SQLInstanceID, SpanPartitionReason) {
	allInstances, err := dsp.sqlAddressResolver.GetAllInstances(ctx)
	if err != nil {
		log.Warningf(ctx, "could not get all instances: %v", err)
		return dsp.alwaysUseGatewayWithReason(SpanPartitionReason_GATEWAY_ON_ERROR)
	}

	if log.ExpensiveLogEnabled(ctx, 2) {
		log.VEventf(ctx, 2, "all SQL instances available for distributed planning: %v", allInstances)
	}

	instances := make(map[base.SQLInstanceID]sqlinstance.InstanceInfo, len(allInstances))
	for _, n := range allInstances {
		instances[n.InstanceID] = n
	}

	return func(nodeID roachpb.NodeID) (base.SQLInstanceID, SpanPartitionReason) {
		sqlInstance := base.SQLInstanceID(nodeID)
		if n, ok := instances[sqlInstance]; ok {
			if status := dsp.checkInstanceHealth(
				sqlInstance, n.InstanceRPCAddr, n.IsDraining, planCtx.nodeStatuses); status == NodeOK {
				return sqlInstance, SpanPartitionReason_TARGET_HEALTHY
			}
		}
		log.VWarningf(ctx, 1, "not planning on node %d", sqlInstance)
		return dsp.gatewaySQLInstanceID, SpanPartitionReason_GATEWAY_TARGET_UNHEALTHY
	}
}

func (dsp *DistSQLPlanner) alwaysUseGatewayWithReason(
	reason SpanPartitionReason,
) func(nodeID roachpb.NodeID) (base.SQLInstanceID, SpanPartitionReason) {
	return func(nodeID roachpb.NodeID) (base.SQLInstanceID, SpanPartitionReason) {
		return dsp.gatewaySQLInstanceID, reason
	}
}

var noInstancesMatchingLocalityFilterErr = errors.New(
	"no healthy sql instances available matching locality requirement",
)

// shouldPickGateway determines whether the gateway node should be picked for a
// particular partition.
func (dsp *DistSQLPlanner) shouldPickGateway(
	planCtx *PlanningCtx, instances []sqlinstance.InstanceInfo,
) bool {
	numEligibleInstancesExcludingGateway := len(instances) - 1
	if numEligibleInstancesExcludingGateway <= 0 {
		return true
	}

	partitionsOnGateway := planCtx.spanPartitionState.partitionSpans[dsp.gatewaySQLInstanceID]
	averageDistributionOnNonGatewayInstances :=
		(planCtx.spanPartitionState.totalPartitionSpans - partitionsOnGateway) / numEligibleInstancesExcludingGateway

	// If the gateway does not have very many partitions yet, we should use the
	// gateway. This is to avoid the situation where we are partitioning spans to
	// remote nodes even when the overall number of partitions is not that high.
	minPartitionsOnGateway := 10
	if dsp.distSQLSrv.TestingKnobs.MinimumNumberOfGatewayPartitions != 0 {
		minPartitionsOnGateway = dsp.distSQLSrv.TestingKnobs.MinimumNumberOfGatewayPartitions
	}
	if partitionsOnGateway < minPartitionsOnGateway {
		return true
	}

	// If the gateway has span partitions >= twice (by default) the average span
	// partitions across other nodes we should distribute the partition to another
	// node.
	bias := int(planCtx.ExtendedEvalCtx.SessionData().DistsqlPlanGatewayBias)
	return partitionsOnGateway < bias*averageDistributionOnNonGatewayInstances
}

// filterUnhealthyInstances dials the instances and filters out unhealthy
// instances.
func (dsp *DistSQLPlanner) filterUnhealthyInstances(
	instances []sqlinstance.InstanceInfo, nodeStatusesCache map[base.SQLInstanceID]NodeStatus,
) ([]sqlinstance.InstanceInfo, []sqlinstance.InstanceInfo) {
	var unhealthyInstances []sqlinstance.InstanceInfo
	// In-place filter out unhealthy instances
	var j int
	for _, n := range instances {
		// Gateway is always considered healthy
		if n.InstanceID == dsp.gatewaySQLInstanceID ||
			dsp.checkInstanceHealth(n.InstanceID, n.InstanceRPCAddr,
				n.IsDraining, nodeStatusesCache) == NodeOK {
			instances[j] = n
			j++
		} else {
			unhealthyInstances = append(unhealthyInstances, n)
		}
	}
	return instances[:j], unhealthyInstances
}

// makeInstanceResolver returns a function that can choose the SQL instance ID
// for a provided KV node ID.
func (dsp *DistSQLPlanner) makeInstanceResolver(
	ctx context.Context, planCtx *PlanningCtx,
) (func(roachpb.NodeID) (base.SQLInstanceID, SpanPartitionReason), error) {
	_, mixedProcessMode := dsp.distSQLSrv.NodeID.OptionalNodeID()
	locFilter := planCtx.localityFilter

	var mixedProcessSameNodeResolver func(nodeID roachpb.NodeID) (base.SQLInstanceID, SpanPartitionReason)
	if mixedProcessMode {
		mixedProcessSameNodeResolver = dsp.healthySQLInstanceIDForKVNodeHostedInstanceResolver(ctx, planCtx)
	}

	if mixedProcessMode && locFilter.Empty() {
		return mixedProcessSameNodeResolver, nil
	}

	// GetAllInstances returns mostly healthy nodes, except those that have
	// recently gone down and not yet been updated in the sql_instances cache. The
	// filtering out of these nodes is deferred to the resolver using the
	// filterUnhealthyInstances function.
	instances, err := dsp.sqlAddressResolver.GetAllInstances(ctx)
	if err != nil {
		return nil, err
	}
	if len(instances) == 0 {
		// For whatever reason, we think that we don't have any healthy
		// instances (one example is someone explicitly removing the rows from
		// the sql_instances table), but we always have the gateway pod to
		// execute on, so we'll use it (unless we have a locality filter).
		if locFilter.NonEmpty() {
			return nil, noInstancesMatchingLocalityFilterErr
		}
		log.Warningf(ctx, "no healthy sql instances available for planning, only using the gateway")
		return dsp.alwaysUseGatewayWithReason(SpanPartitionReason_GATEWAY_NO_HEALTHY_INSTANCES), nil
	}

	rng, _ := randutil.NewPseudoRand()

	instancesHaveLocality := false

	var gatewayIsEligible bool
	if locFilter.NonEmpty() {
		eligible := make([]sqlinstance.InstanceInfo, 0, len(instances))
		for i := range instances {
			if ok, _ := instances[i].Locality.Matches(locFilter); ok {
				eligible = append(eligible, instances[i])
				if instances[i].InstanceID == dsp.gatewaySQLInstanceID {
					gatewayIsEligible = true
				}
			}
		}
		if len(eligible) == 0 {
			return nil, noInstancesMatchingLocalityFilterErr
		}
		instances = eligible
		instancesHaveLocality = true
	} else {
		for i := range instances {
			if instances[i].Locality.NonEmpty() {
				instancesHaveLocality = true
				break
			}
		}
		gatewayIsEligible = true
	}

	if log.ExpensiveLogEnabled(ctx, 2) {
		log.VEventf(ctx, 2, "healthy SQL instances available for distributed planning: %v", instances)
	}

	filterUnhealthyInstances := func() []sqlinstance.InstanceInfo {
		// Instances that have gone down might still be present in the sql_instances
		// cache. Therefore, we filter out these unhealthy nodes by dialing them.
		healthy, unhealthy := dsp.filterUnhealthyInstances(instances, planCtx.nodeStatuses)
		if len(unhealthy) != 0 && log.ExpensiveLogEnabled(ctx, 2) {
			log.Eventf(ctx, "not planning on unhealthy instances : %v", unhealthy)
		}
		return healthy
	}

	// If we were able to determine the locality information for at least some
	// instances, use the locality-aware resolver.
	if instancesHaveLocality {
		resolver := func(nodeID roachpb.NodeID) (base.SQLInstanceID, SpanPartitionReason) {
			instances = filterUnhealthyInstances()
			if len(instances) == 0 {
				log.Eventf(ctx, "no healthy sql instances available for planning, using the gateway")
				return dsp.gatewaySQLInstanceID, SpanPartitionReason_GATEWAY_NO_HEALTHY_INSTANCES
			}
			// Lookup the node localities to compare to the instance localities.
			nodeDesc, err := dsp.nodeDescs.GetNodeDescriptor(nodeID)
			if err != nil {
				log.Eventf(ctx, "unable to get node descriptor for KV node %s", nodeID)
				return dsp.gatewaySQLInstanceID, SpanPartitionReason_GATEWAY_ON_ERROR
			}

			// If we're in mixed-mode, check if the picked node already matches the
			// locality filter in which case we can just use it.
			if mixedProcessMode {
				if ok, _ := nodeDesc.Locality.Matches(locFilter); ok {
					return mixedProcessSameNodeResolver(nodeID)
				} else {
					log.VEventf(ctx, 2,
						"node %d locality %s does not match locality filter %s, finding alternative placement...",
						nodeID, nodeDesc.Locality, locFilter,
					)
				}
			}

			// TODO(dt): Pre-compute / cache this result, e.g. in the instance reader.
			if closest, _ := ClosestInstances(instances,
				nodeDesc.Locality); len(closest) > 0 {
				return closest[rng.Intn(len(closest))], SpanPartitionReason_CLOSEST_LOCALITY_MATCH
			}

			// No instances had any locality tiers in common with the node locality.
			// At this point we pick the gateway if it is eligible, otherwise we pick
			// a random instance from the eligible instances.
			if !gatewayIsEligible {
				return instances[rng.Intn(len(instances))].InstanceID, SpanPartitionReason_LOCALITY_FILTERED_RANDOM
			}
			if dsp.shouldPickGateway(planCtx, instances) {
				return dsp.gatewaySQLInstanceID, SpanPartitionReason_GATEWAY_NO_LOCALITY_MATCH
			} else {
				// If the gateway has a disproportionate number of partitions pick a
				// random instance that is not the gateway.
				if planCtx.spanPartitionState.testingOverrideRandomSelection != nil {
					return planCtx.spanPartitionState.testingOverrideRandomSelection(),
						SpanPartitionReason_LOCALITY_FILTERED_RANDOM_GATEWAY_OVERLOADED
				}
				// NB: This random selection may still pick the gateway but that is
				// alright as we are more interested in a uniform distribution rather
				// than avoiding the gateway.
				id := instances[rng.Intn(len(instances))].InstanceID
				return id, SpanPartitionReason_LOCALITY_FILTERED_RANDOM_GATEWAY_OVERLOADED
			}
		}
		return resolver, nil
	}

	// If no sql instances have locality information, fallback to a naive
	// round-robin strategy that is completely locality-ignorant. Randomize the
	// order in which we choose instances so that work is allocated fairly across
	// queries.
	rng.Shuffle(len(instances), func(i, j int) {
		instances[i], instances[j] = instances[j], instances[i]
	})
	var i int
	resolver := func(roachpb.NodeID) (base.SQLInstanceID, SpanPartitionReason) {
		instances := filterUnhealthyInstances()
		if len(instances) == 0 {
			log.Eventf(ctx, "no healthy sql instances available for planning, only using the gateway")
			return dsp.gatewaySQLInstanceID, SpanPartitionReason_GATEWAY_NO_HEALTHY_INSTANCES
		}

		id := instances[i%len(instances)].InstanceID
		i++
		return id, SpanPartitionReason_ROUND_ROBIN
	}
	return resolver, nil
}

type InstanceLocalityGetter interface {
	sqlinstance.InstanceInfo | InstanceLocality
	GetInstanceID() base.SQLInstanceID
	GetLocality() roachpb.Locality
}

type InstanceLocality struct {
	id       base.SQLInstanceID
	locality roachpb.Locality
}

func MakeInstanceLocality(id base.SQLInstanceID, locality roachpb.Locality) InstanceLocality {
	return InstanceLocality{id: id, locality: locality}
}

func (il InstanceLocality) GetInstanceID() base.SQLInstanceID {
	return il.id
}

func (ii InstanceLocality) GetLocality() roachpb.Locality {
	return ii.locality
}

// ClosestInstances returns the subset of instances which are closest to the
// passed locality, i.e. those which jointly have the longest shared prefix of
// at least length 1 and the shared prefix length. Returns nil, rather than the
// entire input, if no instances have *any* shared locality prefix.
func ClosestInstances[instance InstanceLocalityGetter](
	instances []instance, loc roachpb.Locality,
) ([]base.SQLInstanceID, int) {
	best := 1
	var res []base.SQLInstanceID
	for _, i := range instances {
		if l := i.GetLocality().SharedPrefix(loc); l > best {
			best = l
			res = append(res[:0], i.GetInstanceID())
		} else if l == best {
			res = append(res, i.GetInstanceID())
		}
	}
	if len(res) == 0 {
		best = 0
	}
	return res, best
}

// getInstanceIDForScan retrieves the SQL Instance ID where the single table
// reader should reside for a limited scan. Ideally this is the lease holder for
// the first range in the specified spans. But if that node is unhealthy, we use
// the gateway node instead.
func (dsp *DistSQLPlanner) getInstanceIDForScan(
	ctx context.Context, planCtx *PlanningCtx, spans []roachpb.Span, reverse bool,
) (base.SQLInstanceID, error) {
	if len(spans) == 0 {
		return 0, errors.AssertionFailedf("no spans")
	}

	// Determine the node ID for the first range to be scanned.
	it := planCtx.spanIter
	if reverse {
		it.Seek(ctx, spans[len(spans)-1], kvcoord.Descending)
	} else {
		it.Seek(ctx, spans[0], kvcoord.Ascending)
	}
	if !it.Valid() {
		return 0, it.Error()
	}
	// Note that regardless of the second return value from ReplicaInfo method,
	// ignoreMisplannedRanges will be set to true by the caller because the
	// spans might cover multiple ranges, yet we're planning the scan only on a
	// single node.
	replDesc, _, err := it.ReplicaInfo(ctx)
	if err != nil {
		return 0, err
	}

	if dsp.useGossipPlanning(ctx, planCtx) {
		sqlInstanceID, _ := dsp.deprecatedHealthySQLInstanceIDForKVNodeIDSystem(ctx, planCtx, replDesc.NodeID)
		return sqlInstanceID, nil
	}
	resolver, err := dsp.makeInstanceResolver(ctx, planCtx)
	if err != nil {
		return 0, err
	}
	sqlInstanceID, reason := resolver(replDesc.NodeID)
	planCtx.spanPartitionState.update(sqlInstanceID, reason)
	return sqlInstanceID, nil
}

// TODO(yuzefovich): retire this setting altogether in 25.3 release.
var useGossipPlanning = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.distsql_planning.use_gossip.enabled",
	"if enabled, the DistSQL physical planner falls back to gossip-based planning",
	false,
)

func (dsp *DistSQLPlanner) useGossipPlanning(_ context.Context, planCtx *PlanningCtx) bool {
	var gossipPlanningEnabled bool
	// Some of the planCtx fields can be left unset in tests.
	if planCtx.ExtendedEvalCtx != nil && planCtx.ExtendedEvalCtx.Settings != nil {
		gossipPlanningEnabled = useGossipPlanning.Get(&planCtx.ExtendedEvalCtx.Settings.SV)
	}
	return dsp.codec.ForSystemTenant() && planCtx.localityFilter.Empty() && gossipPlanningEnabled
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

// initTableReaderSpecTemplate initializes a TableReaderSpec/PostProcessSpec
// that corresponds to a scanNode, except for the following fields:
//   - Spans
//   - Parallelize
//   - BatchBytesLimit
//
// The generated specs will be used as templates for planning potentially
// multiple TableReaders.
func initTableReaderSpecTemplate(
	n *scanNode, codec keys.SQLCodec,
) (*execinfrapb.TableReaderSpec, execinfrapb.PostProcessSpec, error) {
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
		LockingDurability:               n.lockingDurability,
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

// createTableReaders generates a plan consisting of table reader processors,
// one for each node that has spans that we are reading.
func (dsp *DistSQLPlanner) createTableReaders(
	ctx context.Context, planCtx *PlanningCtx, n *scanNode,
) (*PhysicalPlan, error) {
	spec, post, err := initTableReaderSpecTemplate(n, planCtx.ExtendedEvalCtx.Codec)
	if err != nil {
		return nil, err
	}

	p := planCtx.NewPhysicalPlan()
	err = dsp.planTableReaders(
		ctx,
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
	settings.ApplicationLevel,
	"sql.local_scans.concurrency_limit",
	"maximum number of additional goroutines for performing scans in local plans",
	defaultLocalScansConcurrencyLimit,
	settings.NonNegativeInt,
)

// maybeParallelizeLocalScans check whether we are planning such a TableReader
// for the local flow that would benefit (and is safe) to parallelize.
func (dsp *DistSQLPlanner) maybeParallelizeLocalScans(
	ctx context.Context, planCtx *PlanningCtx, info *tableReaderPlanningInfo,
) (spanPartitions []SpanPartition, parallelizeLocal bool) {
	// For local plans, if:
	// - there is no required ordering,
	// - the scan is safe to parallelize, and
	// - the parallelization of scans in local flows is allowed,
	// - there is still quota for running more parallel local TableReaders,
	// then we will split all spans according to the leaseholder boundaries and
	// will create a separate TableReader for each node.
	sd := planCtx.ExtendedEvalCtx.SessionData()
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
		// Do a quick check whether we will touch at least two ranges. If we
		// only touch a single range, then we'll end up with a single span
		// partition, so we can skip the PartitionSpans call below.
		it, rangeID, foundTwoRanges := planCtx.spanIter, int64(0), false
		for i := 0; i < len(info.spans) && !foundTwoRanges; i++ {
			sp := info.spans[i]
			for it.Seek(ctx, sp, kvcoord.Ascending); it.Valid(); it.Next(ctx) {
				rID := int64(it.Desc().RangeID)
				if rangeID == 0 {
					rangeID = rID
				} else if rangeID != rID {
					foundTwoRanges = true
					break
				}
				if sp.EndKey == nil || !it.NeedAnother() {
					break
				}
			}
		}
		if !foundTwoRanges {
			spanPartitions = []SpanPartition{{SQLInstanceID: dsp.gatewaySQLInstanceID, Spans: info.spans}}
			parallelizeLocal = false
			return spanPartitions, parallelizeLocal
		}
		parallelizeLocal = true
		// Temporarily unset isLocal so that PartitionSpans divides all spans
		// according to the respective leaseholders.
		planCtx.isLocal = false
		bound := PartitionSpansBoundDefault
		if info.desc.NumFamilies() > 1 {
			bound = PartitionSpansBoundCFWithinRow
		}
		var err error
		spanPartitions, err = dsp.PartitionSpans(ctx, planCtx, info.spans, bound)
		planCtx.isLocal = true
		if err != nil {
			// For some reason we couldn't partition the spans - fallback to
			// having a single TableReader.
			spanPartitions = []SpanPartition{{SQLInstanceID: dsp.gatewaySQLInstanceID, Spans: info.spans}}
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
				alloc, err = dsp.parallelLocalScansSem.TryAcquire(ctx, uint64(actualConcurrency-1))
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
				spanPartitions = []SpanPartition{{SQLInstanceID: dsp.gatewaySQLInstanceID, Spans: info.spans}}
			}
		}
		if len(spanPartitions) == 1 {
			// If all spans are assigned to a single partition, then there will
			// be no parallelism, so we want to elide the redundant
			// synchronizer.
			parallelizeLocal = false
		}
	} else {
		spanPartitions = []SpanPartition{{SQLInstanceID: dsp.gatewaySQLInstanceID, Spans: info.spans}}
	}
	return spanPartitions, parallelizeLocal
}

func (dsp *DistSQLPlanner) planTableReaders(
	ctx context.Context, planCtx *PlanningCtx, p *PhysicalPlan, info *tableReaderPlanningInfo,
) error {
	var (
		spanPartitions         []SpanPartition
		parallelizeLocal       bool
		ignoreMisplannedRanges bool
		err                    error
	)
	if planCtx.isLocal {
		spanPartitions, parallelizeLocal = dsp.maybeParallelizeLocalScans(ctx, planCtx, info)
	} else if info.post.Limit == 0 {
		// No hard limit - plan all table readers where their data live. Note
		// that we're ignoring soft limits for now since the TableReader will
		// still read too eagerly in the soft limit case. To prevent this we'll
		// need a new mechanism on the execution side to modulate table reads.
		// TODO(yuzefovich): add that mechanism.
		bound := PartitionSpansBoundDefault
		if info.desc.NumFamilies() > 1 {
			bound = PartitionSpansBoundCFWithinRow
		}
		spanPartitions, ignoreMisplannedRanges, err = dsp.partitionSpansEx(ctx, planCtx, info.spans, bound)
		if err != nil {
			return err
		}
	} else {
		// If the scan has a hard limit, use a single TableReader to avoid
		// reading more rows than necessary.
		sqlInstanceID, err := dsp.getInstanceIDForScan(ctx, planCtx, info.spans, info.reverse)
		if err != nil {
			return err
		}
		spanPartitions = []SpanPartition{{SQLInstanceID: sqlInstanceID, Spans: info.spans}}
		// The spans to scan might actually live on different nodes, so we don't
		// want to create "misplanned ranges" metadata since it can result in
		// false positives.
		ignoreMisplannedRanges = true
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

		// In some cases, sp.Spans might be an alias to scanNode.spans, and in
		// order to protect the latter from modification (which we must do in
		// case the query might need to be retried-as-local), we might need to
		// perform a copy. In particular, we need to care about the scenario
		// when the following conditions are met:
		// - 1. the query might be distributed (if it's not distributed, then
		//      retry-as-local mechanism won't kick in)
		// - 2. this SpanPartition is assigned to the local node (for all remote
		//      nodes the spans will be serialized and sent over the wire, so it
		//      doesn't matter if the remote node modifies them - it'll have its
		//      own copy).
		//
		// NB: not making a copy for local plans means that scanNode.spans might
		// be modified during the execution. At the time of writing, this
		// doesn't matter, so we choose the performance angle.
		if !planCtx.isLocal && sp.SQLInstanceID == dsp.gatewaySQLInstanceID {
			// Note that we might be copying the spans in more cases than
			// strictly necessary (e.g. because we allocated a fresh slice in
			// partitionSpansEx above), but we choose to be a bit conservative
			// here.
			tr.Spans = make(roachpb.Spans, len(sp.Spans))
			copy(tr.Spans, sp.Spans)
		}

		tr.Parallelize = info.parallelize
		if !tr.Parallelize {
			tr.BatchBytesLimit = dsp.distSQLSrv.TestingKnobs.TableReaderBatchBytesLimit
		}
		tr.IgnoreMisplannedRanges = ignoreMisplannedRanges
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
		p.AddSingleGroupStage(ctx, dsp.gatewaySQLInstanceID, execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}}, execinfrapb.PostProcessSpec{}, p.GetResultTypes())
	}

	return nil
}

// createPlanForRender takes a PhysicalPlan and updates it to produce results
// corresponding to the render node. An evaluator stage is added if the render
// node has any expressions which are not just simple column references.
func (dsp *DistSQLPlanner) createPlanForRender(
	ctx context.Context, p *PhysicalPlan, n *renderNode, planCtx *PlanningCtx,
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
			defer p.EnsureSingleStreamOnGateway(ctx)
		} else {
			p.EnsureSingleStreamOnGateway(ctx)
		}
	}
	newColMap := identityMap(p.PlanToStreamColMap, len(n.render))
	newMergeOrdering := dsp.convertOrdering(n.reqOrdering, newColMap)
	err = p.AddRendering(
		ctx, n.render, planCtx, p.PlanToStreamColMap, typs, newMergeOrdering,
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
	ctx context.Context,
	p *PhysicalPlan,
	ordering colinfo.ColumnOrdering,
	alreadyOrderedPrefix int,
	limit int64,
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
			ctx,
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
	groupCols                []exec.NodeColumnOrdinal
	groupColOrdering         colinfo.ColumnOrdering
	inputMergeOrdering       execinfrapb.Ordering
	reqOrdering              ReqOrdering
	allowPartialDistribution bool
	estimatedRowCount        uint64
}

// addAggregators adds aggregators corresponding to a groupNode and updates the plan to
// reflect the groupNode.
func (dsp *DistSQLPlanner) addAggregators(
	ctx context.Context, planCtx *PlanningCtx, p *PhysicalPlan, n *groupNode,
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
		var ef physicalplan.ExprFactory
		ef.Init(ctx, planCtx, nil /* indexVarMap */)
		for j, argument := range fholder.arguments {
			var err error
			aggregations[i].Arguments[j], err = ef.Make(argument)
			if err != nil {
				return err
			}
			argumentsColumnTypes[i][j] = argument.ResolvedType()
		}
	}

	return dsp.planAggregators(ctx, planCtx, p, &aggregatorPlanningInfo{
		aggregations:         aggregations,
		argumentsColumnTypes: argumentsColumnTypes,
		isScalar:             n.isScalar,
		groupCols:            n.groupCols,
		groupColOrdering:     n.groupColOrdering,
		inputMergeOrdering:   dsp.convertOrdering(planReqOrdering(n.input), p.PlanToStreamColMap),
		reqOrdering:          n.reqOrdering,
		estimatedRowCount:    n.estimatedRowCount,
	})
}

// planAggregators plans the aggregator processors. An evaluator stage is added
// if necessary.
// Invariants assumed:
//   - There is strictly no "pre-evaluation" necessary. If the given query is
//     'SELECT COUNT(k), v + w FROM kv GROUP BY v + w', the evaluation of the first
//     'v + w' is done at the source of the groupNode.
//   - We only operate on the following expressions:
//   - ONLY aggregation functions, with arguments pre-evaluated. So for
//     COUNT(k + v), we assume a stream of evaluated 'k + v' values.
//   - Expressions that CONTAIN an aggregation function, e.g. 'COUNT(k) + 1'.
//     These are set as render expressions in the post-processing spec and
//     are evaluated on the rows that the aggregator returns.
//   - Expressions that also appear verbatim in the GROUP BY expressions.
//     For 'SELECT k GROUP BY k', the aggregation function added is IDENT,
//     therefore k just passes through unchanged.
//     All other expressions simply pass through unchanged, for e.g. '1' in
//     'SELECT 1 GROUP BY k'.
func (dsp *DistSQLPlanner) planAggregators(
	ctx context.Context, planCtx *PlanningCtx, p *PhysicalPlan, info *aggregatorPlanningInfo,
) error {
	aggType := execinfrapb.AggregatorSpec_NON_SCALAR
	if info.isScalar {
		aggType = execinfrapb.AggregatorSpec_SCALAR
	}

	inputTypes := p.GetResultTypes()
	// argTypes will be lazily allocated whenever we need to fetch the output
	// type of the aggregate function.
	var argTypes []*types.T

	groupCols := make([]uint32, len(info.groupCols))
	for i, idx := range info.groupCols {
		groupCols[i] = uint32(p.PlanToStreamColMap[idx])
	}
	orderedGroupCols := make([]uint32, len(info.groupColOrdering))
	var orderedGroupColSet intsets.Fast
	for i, c := range info.groupColOrdering {
		orderedGroupCols[i] = uint32(p.PlanToStreamColMap[c.ColIdx])
		orderedGroupColSet.Add(c.ColIdx)
	}

	// planHashGroupJoin tracks whether we should plan a hash group-join for the
	// first stage of aggregators (either local if multi-stage or final if
	// single-stage), which is the case when
	//   1. the corresponding session variable is enabled
	//   2. the input stage are hash joiners
	//      2.1. there is no ON expression
	//      2.2. the PostProcessSpec can only be a pass-through or a simple
	//           projection (i.e. no rendering, limits, nor offsets)
	//      2.3. only inner and outer joins are supported at the moment
	//   3. the join's equality columns are exactly the same as the
	//      aggregation's grouping columns
	//      3.1. the join's equality columns must be non-empty
	//   4. the first stage we're planning are hash aggregators
	//   5. the distribution of the joiners and the first stage of aggregators
	//      is the same (i.e. both either local or distributed).
	//      TODO(yuzefovich): we could consider lifting the condition 5. by
	//      changing the distribution of the hash joiner stager.
	planHashGroupJoin := planCtx.ExtendedEvalCtx.SessionData().ExperimentalHashGroupJoinEnabled
	if planHashGroupJoin { // condition 1.
		planHashGroupJoin = func() bool {
			prevStageProc := p.Processors[p.ResultRouters[0]].Spec
			hjSpec := prevStageProc.Core.HashJoiner
			if hjSpec == nil {
				return false // condition 2.
			}
			if !hjSpec.OnExpr.Empty() {
				return false // condition 2.1.
			}
			pps := prevStageProc.Post
			if pps.RenderExprs != nil || pps.Limit != 0 || pps.Offset != 0 {
				return false // condition 2.2.
			}
			switch hjSpec.Type {
			case descpb.InnerJoin, descpb.LeftOuterJoin, descpb.RightOuterJoin, descpb.FullOuterJoin:
			default:
				return false // condition 2.3.
			}
			if len(hjSpec.LeftEqColumns) != len(groupCols) {
				return false // condition 3.
			}
			if len(hjSpec.LeftEqColumns) == 0 {
				return false // condition 3.1.
			}
			if len(groupCols) == len(orderedGroupCols) {
				// We will plan streaming aggregation. We shouldn't really ever
				// get here since the hash join doesn't provide any ordering,
				// but we want to be safe.
				return false // condition 4.
			}
			// Join's equality columns refer to columns from the join's inputs
			// whereas the grouping columns refer to the join's output columns.
			// Thus, we need to translate one or the other to the common
			// denominator, and we choose to map the grouping columns to the
			// ordinals of the join's inputs.
			//
			// If we define `m` and `n` as the number of columns from the
			// left and right inputs of the join, respectively, then columns
			// 0, 1, ..., m-1 refer to the corresponding "left" columns whereas
			// m, m+1, ..., m+n-1 refer to the "right" ones.
			var joinEqCols intsets.Fast
			m := len(prevStageProc.Input[0].ColumnTypes)
			for _, leftEqCol := range hjSpec.LeftEqColumns {
				joinEqCols.Add(int(leftEqCol))
			}
			for _, rightEqCol := range hjSpec.RightEqColumns {
				joinEqCols.Add(m + int(rightEqCol))
			}
			for _, groupCol := range groupCols {
				mappedCol := groupCol
				if pps.OutputColumns != nil {
					mappedCol = pps.OutputColumns[groupCol]
				}
				if !joinEqCols.Contains(int(mappedCol)) {
					return false // condition 3.
				}
			}
			return true
		}()
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
		var distinctColumnsSet intsets.Fast
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

			// The condition 4. above is not satisfied since we've just added
			// a distinct stage before the first aggregation one.
			// TODO(yuzefovich): re-evaluate whether it is worth skipping this
			// local distinct stage in favor of getting hash group-join planned.
			planHashGroupJoin = false
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
		if !multiStage {
			// The joiners are distributed whereas the aggregation cannot be
			// distributed which fails condition 5.
			planHashGroupJoin = false
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

					// Keep track of the new local aggregation's output type.
					argTypes = argTypes[:0]
					for _, c := range e.ColIdx {
						argTypes = append(argTypes, inputTypes[c])
					}
					outputType, err := execagg.GetAggregateOutputType(localFunc, argTypes)
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
						argTypes = argTypes[:0]
						for i := range finalInfo.LocalIdxs {
							// Map the corresponding local aggregation output
							// types for the current aggregation e.
							argTypes = append(argTypes, intermediateTypes[argIdxs[i]])
						}
						outputType, err := execagg.GetAggregateOutputType(finalInfo.Fn, argTypes)
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
			if orderedGroupColSet.Contains(int(info.groupCols[i])) {
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
				if int(col) == o.ColIdx {
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

		if planHashGroupJoin {
			prevStageProc := p.Processors[p.ResultRouters[0]].Spec
			hjSpec := prevStageProc.Core.HashJoiner
			pps := prevStageProc.Post
			hgjSpec := execinfrapb.HashGroupJoinerSpec{
				HashJoinerSpec:    *hjSpec,
				JoinOutputColumns: pps.OutputColumns,
				AggregatorSpec:    localAggsSpec,
			}
			p.ReplaceLastStage(
				execinfrapb.ProcessorCoreUnion{HashGroupJoiner: &hgjSpec},
				execinfrapb.PostProcessSpec{},
				intermediateTypes,
				execinfrapb.Ordering{Columns: ordCols},
			)
			// Let the final aggregation be planned as normal.
			planHashGroupJoin = false
		} else {
			p.AddNoGroupingStage(
				execinfrapb.ProcessorCoreUnion{Aggregator: &localAggsSpec},
				execinfrapb.PostProcessSpec{},
				intermediateTypes,
				execinfrapb.Ordering{Columns: ordCols},
			)
		}

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
			h := tree.MakeIndexedVarHelperWithTypes(finalPreRenderTypes)
			// finalIdx is an index inside finalAggs. It is used to
			// keep track of the finalAggs results that correspond
			// to each aggregation.
			finalIdx := 0
			var ef physicalplan.ExprFactory
			ef.Init(ctx, planCtx, nil /* indexVarMap */)
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
					renderExprs[i], err = ef.Make(h.IndexedVar(mappedIdx))
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
					renderExprs[i], err = ef.Make(expr)
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
		argTypes = argTypes[:0]
		for _, c := range agg.ColIdx {
			argTypes = append(argTypes, inputTypes[c])
		}
		argTypes = append(argTypes, info.argumentsColumnTypes[i]...)
		returnTyp, err := execagg.GetAggregateOutputType(agg.Func, argTypes)
		if err != nil {
			return err
		}
		finalOutTypes[i] = returnTyp
	}

	// Update p.PlanToStreamColMap; we will have a simple 1-to-1 mapping of
	// planNode columns to stream columns because the aggregator
	// has been programmed to produce the same columns as the groupNode.
	p.PlanToStreamColMap = identityMap(p.PlanToStreamColMap, len(info.aggregations))

	if planHashGroupJoin {
		prevStageProc := p.Processors[p.ResultRouters[0]].Spec
		hjSpec := prevStageProc.Core.HashJoiner
		pps := prevStageProc.Post
		hgjSpec := execinfrapb.HashGroupJoinerSpec{
			HashJoinerSpec:    *hjSpec,
			JoinOutputColumns: pps.OutputColumns,
			AggregatorSpec:    finalAggsSpec,
		}
		p.ReplaceLastStage(
			execinfrapb.ProcessorCoreUnion{HashGroupJoiner: &hgjSpec},
			execinfrapb.PostProcessSpec{},
			finalOutTypes,
			dsp.convertOrdering(info.reqOrdering, p.PlanToStreamColMap),
		)
	} else if len(finalAggsSpec.GroupCols) == 0 || len(p.ResultRouters) == 1 {
		// No GROUP BY, or we have a single stream. Use a single final aggregator.
		// If the previous stage was all on a single node, put the final
		// aggregator there. Otherwise, bring the results back on this node.
		node := dsp.gatewaySQLInstanceID
		if prevStageNode != 0 {
			node = prevStageNode
		}
		p.AddSingleGroupStage(
			ctx,
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
			p.MergeResultStreams(ctx, p.ResultRouters, bucket, p.MergeOrdering, pIdx, 0, false, /* forceSerialization */
				physicalplan.SerialStreamErrorSpec{},
			)
		}

		// Set the new result routers.
		for i := 0; i < len(p.ResultRouters); i++ {
			p.ResultRouters[i] = pIdxStart + physicalplan.ProcessorIdx(i)
		}

		p.SetMergeOrdering(dsp.convertOrdering(info.reqOrdering, p.PlanToStreamColMap))
	}

	// Set the estimated output row count if we have it available.
	for _, pIdx := range p.ResultRouters {
		p.Processors[pIdx].Spec.EstimatedRowCount = info.estimatedRowCount
	}

	return nil
}

func (dsp *DistSQLPlanner) createPlanForIndexJoin(
	ctx context.Context, planCtx *PlanningCtx, n *indexJoinNode,
) (*PhysicalPlan, error) {
	plan, err := dsp.createPhysPlanForPlanNode(ctx, planCtx, n.input)
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
		LockingDurability: n.table.lockingDurability,
		MaintainOrdering:  len(n.reqOrdering) > 0,
		LimitHint:         n.limitHint,
	}

	fetchColIDs := make([]descpb.ColumnID, len(n.cols))
	var fetchOrdinals intsets.Fast
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
			ctx,
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
	ctx context.Context, planCtx *PlanningCtx, n *lookupJoinNode,
) (*PhysicalPlan, error) {
	plan, err := dsp.createPhysPlanForPlanNode(ctx, planCtx, n.input)
	if err != nil {
		return nil, err
	}

	// If any of the ordering columns originate from the lookup table, this is a
	// case where we are ordering on a prefix of input columns followed by the
	// lookup columns.
	var maintainLookupOrdering bool
	numInputCols := len(plan.GetResultTypes())
	for i := range n.reqOrdering {
		if n.reqOrdering[i].ColIdx >= numInputCols {
			// We need to maintain the index ordering on each lookup.
			maintainLookupOrdering = true
			break
		}
	}

	joinReaderSpec := execinfrapb.JoinReaderSpec{
		Type:              n.joinType,
		LockingStrength:   n.table.lockingStrength,
		LockingWaitPolicy: n.table.lockingWaitPolicy,
		LockingDurability: n.table.lockingDurability,
		// TODO(sumeer): specifying ordering here using isFirstJoinInPairedJoiner
		// is late in the sense that the cost of this has not been taken into
		// account. Make this decision earlier in CustomFuncs.GenerateLookupJoins.
		MaintainOrdering:                  len(n.reqOrdering) > 0 || n.isFirstJoinInPairedJoiner,
		MaintainLookupOrdering:            maintainLookupOrdering,
		LeftJoinWithPairedJoiner:          n.isSecondJoinInPairedJoiner,
		OutputGroupContinuationForLeftRow: n.isFirstJoinInPairedJoiner,
		LookupBatchBytesLimit:             dsp.distSQLSrv.TestingKnobs.JoinReaderBatchBytesLimit,
		LimitHint:                         n.limitHint,
		RemoteOnlyLookups:                 n.remoteOnlyLookups,
	}

	fetchColIDs := make([]descpb.ColumnID, len(n.table.cols))
	var fetchOrdinals intsets.Fast
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

	var splitter span.Splitter
	if joinReaderSpec.LockingStrength != descpb.ScanLockingStrength_FOR_NONE &&
		planCtx.ExtendedEvalCtx.TxnIsoLevel != isolation.Serializable {
		splitter = span.MakeSplitterForSideEffect(n.table.desc, n.table.index, fetchOrdinals)
	} else {
		splitter = span.MakeSplitter(n.table.desc, n.table.index, fetchOrdinals)
	}
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

	var ef physicalplan.ExprFactory
	ef.Init(ctx, planCtx, nil /* indexVarMap */)

	// Set the lookup condition.
	if n.lookupExpr != nil {
		var err error
		joinReaderSpec.LookupExpr, err = ef.Make(n.lookupExpr)
		if err != nil {
			return nil, err
		}
	}
	if n.remoteLookupExpr != nil {
		if n.lookupExpr == nil {
			return nil, errors.AssertionFailedf("remoteLookupExpr is set but lookupExpr is not")
		}
		var err error
		joinReaderSpec.RemoteLookupExpr, err = ef.Make(n.remoteLookupExpr)
		if err != nil {
			return nil, err
		}
	}

	// Set the ON condition.
	if n.onCond != nil {
		var err error
		joinReaderSpec.OnExpr, err = ef.Make(n.onCond)
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

func (dsp *DistSQLPlanner) createPlanForInvertedJoin(
	ctx context.Context, planCtx *PlanningCtx, n *invertedJoinNode,
) (*PhysicalPlan, error) {
	plan, err := dsp.createPhysPlanForPlanNode(ctx, planCtx, n.input)
	if err != nil {
		return nil, err
	}

	invertedJoinerSpec := execinfrapb.InvertedJoinerSpec{
		Type:                              n.joinType,
		MaintainOrdering:                  len(n.reqOrdering) > 0,
		OutputGroupContinuationForLeftRow: n.isFirstJoinInPairedJoiner,
		LockingStrength:                   n.table.lockingStrength,
		LockingWaitPolicy:                 n.table.lockingWaitPolicy,
		LockingDurability:                 n.table.lockingDurability,
	}

	fetchColIDs := make([]descpb.ColumnID, len(n.table.cols))
	for i := range n.table.cols {
		fetchColIDs[i] = n.table.cols[i].GetID()
	}
	if err := rowenc.InitIndexFetchSpec(
		&invertedJoinerSpec.FetchSpec,
		planCtx.ExtendedEvalCtx.Codec,
		n.table.desc,
		n.table.index,
		fetchColIDs,
	); err != nil {
		return nil, err
	}

	invCol, err := catalog.MustFindColumnByID(n.table.desc, n.table.index.InvertedColumnID())
	if err != nil {
		return nil, err
	}
	invertedJoinerSpec.InvertedColumnOriginalType = invCol.GetType()

	invertedJoinerSpec.PrefixEqualityColumns = make([]uint32, len(n.prefixEqCols))
	for i, col := range n.prefixEqCols {
		if plan.PlanToStreamColMap[col] == -1 {
			panic("lookup column not in planToStreamColMap")
		}
		invertedJoinerSpec.PrefixEqualityColumns[i] = uint32(plan.PlanToStreamColMap[col])
	}

	var ef physicalplan.ExprFactory
	ef.Init(ctx, planCtx, nil /* indexVarMap */)
	if invertedJoinerSpec.InvertedExpr, err = ef.Make(n.invertedExpr); err != nil {
		return nil, err
	}
	// Set the ON condition.
	if n.onExpr != nil {
		if invertedJoinerSpec.OnExpr, err = ef.Make(n.onExpr); err != nil {
			return nil, err
		}
	}

	inputTypes := plan.GetResultTypes()
	fetchedColumns := invertedJoinerSpec.FetchSpec.FetchedColumns

	outTypes := inputTypes
	planToStreamColMap := plan.PlanToStreamColMap
	if n.joinType.ShouldIncludeRightColsInOutput() {
		outTypes = make([]*types.T, len(inputTypes)+len(fetchedColumns))
		copy(outTypes, inputTypes)
		for i := range fetchedColumns {
			outTypes[len(inputTypes)+i] = fetchedColumns[i].Type
			planToStreamColMap = append(planToStreamColMap, len(inputTypes)+i)
		}
	}
	if n.isFirstJoinInPairedJoiner {
		outTypes = append(outTypes, types.Bool)
		planToStreamColMap = append(planToStreamColMap, len(outTypes)-1)
	}

	// Instantiate one inverted joiner for every stream.
	plan.AddNoGroupingStage(
		execinfrapb.ProcessorCoreUnion{InvertedJoiner: &invertedJoinerSpec},
		execinfrapb.PostProcessSpec{},
		outTypes,
		dsp.convertOrdering(planReqOrdering(n), planToStreamColMap),
	)
	plan.PlanToStreamColMap = planToStreamColMap
	return plan, nil
}

// createPlanForZigzagJoin creates a distributed plan for a zigzagJoinNode.
func (dsp *DistSQLPlanner) createPlanForZigzagJoin(
	ctx context.Context, planCtx *PlanningCtx, n *zigzagJoinNode,
) (plan *PhysicalPlan, err error) {

	sides := make([]zigzagPlanningSide, len(n.sides))

	for i, side := range n.sides {
		// The fixed values are represented as a Values node with one tuple.
		typs := getTypesFromResultColumns(side.fixedVals.columns)
		valuesSpec, err := dsp.createValuesSpecFromTuples(ctx, planCtx, side.fixedVals.tuples, typs)
		if err != nil {
			return nil, err
		}

		sides[i] = zigzagPlanningSide{
			desc:              side.scan.desc,
			index:             side.scan.index,
			cols:              side.scan.cols,
			eqCols:            side.eqCols,
			fixedValues:       valuesSpec,
			lockingStrength:   side.scan.lockingStrength,
			lockingWaitPolicy: side.scan.lockingWaitPolicy,
			lockingDurability: side.scan.lockingDurability,
		}
	}

	return dsp.planZigzagJoin(ctx, planCtx, zigzagPlanningInfo{
		sides:       sides,
		columns:     n.columns,
		onCond:      n.onCond,
		reqOrdering: n.reqOrdering,
	})
}

type zigzagPlanningSide struct {
	desc              catalog.TableDescriptor
	index             catalog.Index
	cols              []catalog.Column
	eqCols            []int
	fixedValues       *execinfrapb.ValuesCoreSpec
	lockingStrength   descpb.ScanLockingStrength
	lockingWaitPolicy descpb.ScanLockingWaitPolicy
	lockingDurability descpb.ScanLockingDurability
}

type zigzagPlanningInfo struct {
	sides       []zigzagPlanningSide
	columns     colinfo.ResultColumns
	onCond      tree.TypedExpr
	reqOrdering ReqOrdering
}

func (dsp *DistSQLPlanner) planZigzagJoin(
	ctx context.Context, planCtx *PlanningCtx, pi zigzagPlanningInfo,
) (plan *PhysicalPlan, err error) {

	plan = planCtx.NewPhysicalPlan()

	sides := make([]execinfrapb.ZigzagJoinerSpec_Side, len(pi.sides))
	for i, side := range pi.sides {
		s := &sides[i]
		fetchColIDs := make([]descpb.ColumnID, len(side.cols))
		for i := range side.cols {
			fetchColIDs[i] = side.cols[i].GetID()
		}
		if err := rowenc.InitIndexFetchSpec(
			&s.FetchSpec,
			planCtx.ExtendedEvalCtx.Codec,
			side.desc,
			side.index,
			fetchColIDs,
		); err != nil {
			return nil, err
		}

		s.EqColumns.Columns = make([]uint32, len(side.eqCols))
		for j, col := range side.eqCols {
			s.EqColumns.Columns[j] = uint32(col)
		}
		s.FixedValues = *side.fixedValues
	}

	// The zigzag join node only represents inner joins, so hardcode Type to
	// InnerJoin.
	zigzagJoinerSpec := execinfrapb.ZigzagJoinerSpec{
		Sides: sides,
		Type:  descpb.InnerJoin,
	}

	// Set the ON condition.
	if pi.onCond != nil {
		zigzagJoinerSpec.OnExpr, err = physicalplan.MakeExpression(
			ctx, pi.onCond, planCtx, nil, /* indexVarMap */
		)
		if err != nil {
			return nil, err
		}
	}
	// The internal schema of the zigzag joiner matches the zigzagjoinNode columns:
	//    <side 1 columns> ... <side 2 columns> ...
	types := make([]*types.T, len(pi.columns))
	for i := range types {
		types[i] = pi.columns[i].Typ
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

	plan.AddNoInputStage(corePlacement, execinfrapb.PostProcessSpec{}, types, execinfrapb.Ordering{})
	plan.PlanToStreamColMap = identityMap(nil /* buf */, len(pi.columns))

	return plan, nil
}

func (dsp *DistSQLPlanner) createPlanForInvertedFilter(
	ctx context.Context, planCtx *PlanningCtx, n *invertedFilterNode,
) (*PhysicalPlan, error) {
	plan, err := dsp.createPhysPlanForPlanNode(ctx, planCtx, n.input)
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
			ctx, n.preFiltererExpr, planCtx, nil,
		); err != nil {
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
		plan.AddSingleGroupStage(ctx, lastSQLInstanceID,
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
		ctx,
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
	ctx context.Context, planCtx *PlanningCtx, n *joinNode,
) (*PhysicalPlan, error) {
	leftPlan, err := dsp.createPhysPlanForPlanNode(ctx, planCtx, n.left)
	if err != nil {
		return nil, err
	}
	rightPlan, err := dsp.createPhysPlanForPlanNode(ctx, planCtx, n.right)
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
	onExpr, err := helper.remapOnExpr(ctx, planCtx, n.pred.onCond)
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
	return dsp.planJoiners(ctx, planCtx, &info, n.reqOrdering), nil
}

func (dsp *DistSQLPlanner) planJoiners(
	ctx context.Context, planCtx *PlanningCtx, info *joinPlanningInfo, reqOrdering ReqOrdering,
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

		// If either side has a single stream, put the processor on that node.
		// We prefer the right side because that is processed first by the hash
		// joiner.
		if len(rightRouters) == 1 {
			sqlInstances[0] = p.Processors[rightRouters[0]].SQLInstanceID
		} else if len(leftRouters) == 1 {
			sqlInstances[0] = p.Processors[leftRouters[0]].SQLInstanceID
		}
	}

	p.AddJoinStage(
		ctx, sqlInstances, info.makeCoreSpec(), info.post,
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
	ctx context.Context, planCtx *PlanningCtx, plan planMaybePhysical,
) (physPlan *PhysicalPlan, cleanup func(), err error) {
	if plan.isPhysicalPlan() {
		// Note that planCtx.getCleanupFunc() is already set in
		// plan.physPlan.onClose, so here we return a noop cleanup function.
		return plan.physPlan.PhysicalPlan, func() {}, nil
	}
	physPlan, err = dsp.createPhysPlanForPlanNode(ctx, planCtx, plan.planNode)
	return physPlan, planCtx.getCleanupFunc(), err
}

func (dsp *DistSQLPlanner) createPhysPlanForPlanNode(
	ctx context.Context, planCtx *PlanningCtx, node planNode,
) (plan *PhysicalPlan, err error) {
	planCtx.planDepth++

	switch n := node.(type) {
	// Keep these cases alphabetized, please!
	case *createStatsNode:
		if n.runAsJob {
			plan, err = dsp.wrapPlan(ctx, planCtx, n, false /* allowPartialDistribution */)
		} else {
			// Create a job record but don't actually start the job.
			var record *jobs.Record
			record, err = n.makeJobRecord(ctx)
			if err != nil {
				return nil, err
			}
			details := record.Details.(jobspb.CreateStatsDetails)

			numIndexes := 1
			if details.UsingExtremes {
				// Partial stats collections scan a different index for each column.
				numIndexes = len(details.ColumnStats)
			}
			plan, err = dsp.createPlanForCreateStats(
				ctx, planCtx, planCtx.planner.SemaCtx(), 0 /* jobID */, details,
				numIndexes, 0, /* curIndex */
			)
		}

	case *distinctNode:
		plan, err = dsp.createPlanForDistinct(ctx, planCtx, n)

	case *exportNode:
		plan, err = dsp.createPlanForExport(ctx, planCtx, n)

	case *filterNode:
		plan, err = dsp.createPhysPlanForPlanNode(ctx, planCtx, n.input)
		if err != nil {
			return nil, err
		}

		if err := plan.AddFilter(ctx, n.filter, planCtx, plan.PlanToStreamColMap); err != nil {
			return nil, err
		}

	case *groupNode:
		plan, err = dsp.createPhysPlanForPlanNode(ctx, planCtx, n.input)
		if err != nil {
			return nil, err
		}

		if err := dsp.addAggregators(ctx, planCtx, plan, n); err != nil {
			return nil, err
		}

	case *indexJoinNode:
		plan, err = dsp.createPlanForIndexJoin(ctx, planCtx, n)

	case *insertNode:
		if planCtx.isVectorInsert {
			plan, err = dsp.createPlanForInsert(ctx, planCtx, n)
		} else {
			plan, err = dsp.wrapPlan(ctx, planCtx, n, false /* allowPartialDistribution */)
		}

	case *invertedFilterNode:
		plan, err = dsp.createPlanForInvertedFilter(ctx, planCtx, n)

	case *invertedJoinNode:
		plan, err = dsp.createPlanForInvertedJoin(ctx, planCtx, n)

	case *joinNode:
		plan, err = dsp.createPlanForJoin(ctx, planCtx, n)

	case *limitNode:
		plan, err = dsp.createPhysPlanForPlanNode(ctx, planCtx, n.input)
		if err != nil {
			return nil, err
		}
		var count, offset int64
		if count, offset, err = evalLimit(ctx, planCtx.EvalContext(), n.countExpr, n.offsetExpr); err != nil {
			return nil, err
		}
		if err := plan.AddLimit(ctx, count, offset, planCtx); err != nil {
			return nil, err
		}

	case *lookupJoinNode:
		plan, err = dsp.createPlanForLookupJoin(ctx, planCtx, n)

	case *ordinalityNode:
		plan, err = dsp.createPlanForOrdinality(ctx, planCtx, n)

	case *projectSetNode:
		plan, err = dsp.createPlanForProjectSet(ctx, planCtx, n)

	case *renderNode:
		plan, err = dsp.createPhysPlanForPlanNode(ctx, planCtx, n.input)
		if err != nil {
			return nil, err
		}
		err = dsp.createPlanForRender(ctx, plan, n, planCtx)
		if err != nil {
			return nil, err
		}

	case *rowCountNode:
		if in, ok := n.source.(*insertNode); ok {
			// Skip over any renderNodes.
			nod := in.input
			for r, ok := nod.(*renderNode); ok; r, ok = r.input.(*renderNode) {
				nod = r.input
			}
			if v, ok := nod.(*valuesNode); ok {
				if v.coldataBatch != nil {
					planCtx.isVectorInsert = true
				}
			}
		}

		if planCtx.isVectorInsert {
			plan, err = dsp.createPlanForRowCount(ctx, planCtx, n)
		} else {
			plan, err = dsp.wrapPlan(ctx, planCtx, n, false /* allowPartialDistribution */)
		}

	case *scanNode:
		plan, err = dsp.createTableReaders(ctx, planCtx, n)

	case *sortNode:
		plan, err = dsp.createPhysPlanForPlanNode(ctx, planCtx, n.input)
		if err != nil {
			return nil, err
		}

		dsp.addSorters(ctx, plan, n.ordering, n.alreadyOrderedPrefix, 0 /* limit */)

	case *topKNode:
		plan, err = dsp.createPhysPlanForPlanNode(ctx, planCtx, n.input)
		if err != nil {
			return nil, err
		}

		if n.k <= 0 {
			return nil, errors.New("negative or zero value for LIMIT")
		}
		dsp.addSorters(ctx, plan, n.ordering, n.alreadyOrderedPrefix, n.k)

	case *unaryNode:
		plan, err = dsp.createPlanForUnary(planCtx, n)

	case *unionNode:
		plan, err = dsp.createPlanForSetOp(ctx, planCtx, n)

	case *valuesNode:
		if mustWrapValuesNode(planCtx, n.specifiedInQuery) {
			plan, err = dsp.wrapPlan(ctx, planCtx, n, false /* allowPartialDistribution */)
		} else {
			colTypes := getTypesFromResultColumns(n.columns)
			var spec *execinfrapb.ValuesCoreSpec
			spec, err = dsp.createValuesSpecFromTuples(ctx, planCtx, n.tuples, colTypes)
			if err != nil {
				return nil, err
			}
			var idx physicalplan.ProcessorIdx
			plan, idx, err = dsp.createValuesPlan(planCtx, spec, colTypes)
			if n.coldataBatch != nil {
				if plan.LocalVectorSources == nil {
					plan.LocalVectorSources = make(map[int32]any)
				}
				plan.LocalVectorSources[int32(idx)] = n.coldataBatch
			}
		}

	case *vectorMutationSearchNode:
		plan, err = dsp.createPlanForVectorMutationSearch(ctx, planCtx, n)

	case *vectorSearchNode:
		plan, err = dsp.createPlanForVectorSearch(planCtx, n)

	case *windowNode:
		plan, err = dsp.createPlanForWindow(ctx, planCtx, n)

	case *zeroNode:
		plan, err = dsp.createPlanForZero(planCtx, n)

	case *zigzagJoinNode:
		plan, err = dsp.createPlanForZigzagJoin(ctx, planCtx, n)

	default:
		// Can't handle a node? We wrap it and continue on our way.
		plan, err = dsp.wrapPlan(ctx, planCtx, n, false /* allowPartialDistribution */)
	}

	if err != nil {
		return plan, err
	}

	if planCtx.associateNodeWithComponents != nil {
		processors := make(execComponents, len(plan.ResultRouters))
		for i, resultProcIdx := range plan.ResultRouters {
			processors[i] = execinfrapb.ProcessorComponentID(
				plan.Processors[resultProcIdx].SQLInstanceID,
				execinfrapb.FlowID{UUID: planCtx.infra.FlowID},
				int32(resultProcIdx),
			)
		}
		planCtx.associateNodeWithComponents(node, processors)
	}

	return plan, err
}

// wrapPlan produces a DistSQL processor for an arbitrary planNode. This is
// invoked when a particular planNode can't be distributed for some reason. It
// will create a planNodeToRowSource wrapper for the sub-tree that's not
// plannable by DistSQL. If that sub-tree has DistSQL-plannable sources, they
// will be planned by DistSQL and connected to the wrapper.
func (dsp *DistSQLPlanner) wrapPlan(
	ctx context.Context, planCtx *PlanningCtx, n planNode, allowPartialDistribution bool,
) (*PhysicalPlan, error) {
	useFastPath := planCtx.planDepth == 1 && planCtx.stmtType == tree.RowsAffected

	// First, we search the planNode tree we're trying to wrap for the first
	// DistSQL-enabled planNode in the tree. If we find one, we ask the planner to
	// continue the DistSQL planning recursion on that planNode.
	seenTop := false
	var planFirstDistSQLNode func(plan planNode) (planNode, *PhysicalPlan, error)
	planFirstDistSQLNode = func(plan planNode) (planNode, *PhysicalPlan, error) {
		switch plan.(type) {
		case *explainVecNode, *explainPlanNode, *explainDDLNode:
			// Don't continue recursing into explain nodes - they need to be left
			// alone since they handle their own planning later.
			return nil, nil, nil
		}
		if !seenTop {
			seenTop = true
		} else if !dsp.mustWrapNode(planCtx, plan) || shouldWrapPlanNodeForExecStats(planCtx, plan) {
			p, err := dsp.createPhysPlanForPlanNode(ctx, planCtx, plan)
			if err != nil {
				return nil, nil, err
			}
			return plan, p, nil
		}
		switch plan.InputCount() {
		case 0:
			return nil, nil, nil
		case 1:
			// Continue walking until we find a node that has a DistSQL
			// representation - that's when we'll quit the wrapping process and
			// hand control of planning back to the DistSQL physical planner.
			//
			// However, if we're collecting execution stats, then we'll surround
			// each planNode with a pair of planNodeToRowSource and
			// rowSourceToPlanNode adapters so that the execution statistics
			// are collected for each planNode independently. This should have
			// low enough overhead.
			input, err := plan.Input(0)
			if err != nil {
				return nil, nil, err
			}
			return planFirstDistSQLNode(input)
		default:
			// Can't wrap plans with more than 1 input.
			return nil, nil, nil
		}
	}
	firstNotWrapped, p, err := planFirstDistSQLNode(n)
	if err != nil {
		return nil, err
	}
	if p == nil {
		p = planCtx.NewPhysicalPlan()
	}

	// Copy the evalCtx.
	evalCtx := *planCtx.ExtendedEvalCtx
	// We permit the planNodeToRowSource to trigger the wrapped planNode's fast
	// path if its the very first node in the flow, and if the statement type we're
	// expecting is in fact RowsAffected. RowsAffected statements return a single
	// row with the number of rows affected by the statement, and are the only
	// types of statement where it's valid to invoke a plan's fast path.
	wrapper := newPlanNodeToRowSource(
		n,
		runParams{
			extendedEvalCtx: &evalCtx,
			p:               planCtx.planner,
		},
		useFastPath,
		firstNotWrapped,
	)

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
				NumInputs:    uint32(len(input)),
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
		p.MergeResultStreams(ctx, p.ResultRouters, 0, p.MergeOrdering, pIdx, 0, false, /* forceSerialization */
			physicalplan.SerialStreamErrorSpec{},
		)
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
		s.Columns[i].Encoding = catenumpb.DatumEncoding_VALUE
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
) (*PhysicalPlan, physicalplan.ProcessorIdx, error) {
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

	return p, pIdx, nil
}

// createValuesSpecFromTuples creates a ValuesCoreSpec from the results of
// evaluating the given tuples.
func (dsp *DistSQLPlanner) createValuesSpecFromTuples(
	ctx context.Context, planCtx *PlanningCtx, tuples [][]tree.TypedExpr, resultTypes []*types.T,
) (*execinfrapb.ValuesCoreSpec, error) {
	var a tree.DatumAlloc
	evalCtx := &planCtx.ExtendedEvalCtx.Context
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
			datum, err := eval.Expr(ctx, evalCtx, typedExpr)
			if err != nil {
				return nil, err
			}
			encDatum := rowenc.DatumToEncDatum(resultTypes[colIdx], datum)
			buf, err = encDatum.Encode(resultTypes[colIdx], &a, catenumpb.DatumEncoding_VALUE, buf)
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
	plan, _, err := dsp.createValuesPlan(planCtx, spec, types)
	return plan, err
}

func (dsp *DistSQLPlanner) createPlanForZero(
	planCtx *PlanningCtx, n *zeroNode,
) (*PhysicalPlan, error) {
	types, err := getTypesForPlanResult(n, nil /* planToStreamColMap */)
	if err != nil {
		return nil, err
	}

	spec := dsp.createValuesSpec(planCtx, types, 0 /* numRows */, nil /* rawBytes */)
	plan, _, err := dsp.createValuesPlan(planCtx, spec, types)
	return plan, err
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
	ctx context.Context, planCtx *PlanningCtx, n *distinctNode,
) (*PhysicalPlan, error) {
	plan, err := dsp.createPhysPlanForPlanNode(ctx, planCtx, n.input)
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
	dsp.addDistinctProcessors(ctx, plan, spec)
	return plan, nil
}

func (dsp *DistSQLPlanner) addDistinctProcessors(
	ctx context.Context, plan *PhysicalPlan, spec *execinfrapb.DistinctSpec,
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
		ctx, sqlInstanceIDs, distinctSpec, execinfrapb.PostProcessSpec{},
		distinctSpec.Distinct.DistinctColumns, plan.GetResultTypes(),
		plan.GetResultTypes(), plan.MergeOrdering, plan.ResultRouters,
	)
	plan.SetMergeOrdering(spec.OutputOrdering)
}

func (dsp *DistSQLPlanner) createPlanForVectorSearch(
	planCtx *PlanningCtx, n *vectorSearchNode,
) (*PhysicalPlan, error) {
	var queryVector vector.T
	switch t := n.queryVector.(type) {
	case *tree.DPGVector:
		queryVector = t.T
	default:
		return nil, errors.AssertionFailedf("unexpected query vector type: %T", t)
	}

	p := planCtx.NewPhysicalPlan()
	colTypes := getTypesFromResultColumns(n.resultCols)
	spec := &execinfrapb.VectorSearchSpec{
		PrefixKey:           n.prefixKey,
		QueryVector:         queryVector,
		TargetNeighborCount: n.targetNeighborCount,
	}
	fetchCols := make([]descpb.ColumnID, len(n.cols))
	for i, col := range n.cols {
		fetchCols[i] = col.GetID()
	}
	if err := rowenc.InitIndexFetchSpec(
		&spec.FetchSpec,
		planCtx.ExtendedEvalCtx.Codec,
		n.table,
		n.index,
		fetchCols,
	); err != nil {
		return nil, err
	}

	// Execute the vector search on the gateway node.
	corePlacement := []physicalplan.ProcessorCorePlacement{{
		SQLInstanceID: dsp.gatewaySQLInstanceID,
		Core:          execinfrapb.ProcessorCoreUnion{VectorSearch: spec},
	}}
	p.AddNoInputStage(corePlacement, execinfrapb.PostProcessSpec{}, colTypes, execinfrapb.Ordering{})
	p.PlanToStreamColMap = identityMap(make([]int, len(colTypes)), len(colTypes))
	return p, nil
}

func (dsp *DistSQLPlanner) createPlanForVectorMutationSearch(
	ctx context.Context, planCtx *PlanningCtx, n *vectorMutationSearchNode,
) (*PhysicalPlan, error) {
	plan, err := dsp.createPhysPlanForPlanNode(ctx, planCtx, n.input)
	if err != nil {
		return nil, err
	}
	// Add the partition column. Also add the quantized vector column for index
	// puts.
	inputTypes := plan.GetResultTypes()
	outputTypes := append(inputTypes, types.Int)
	plan.PlanToStreamColMap = append(plan.PlanToStreamColMap, len(inputTypes))
	if n.isIndexPut {
		outputTypes = append(outputTypes, types.Bytes)
		plan.PlanToStreamColMap = append(plan.PlanToStreamColMap, len(inputTypes)+1)
	}
	// Retrieve the prefix and suffix index columns.
	prefixKeyColumnOrdinals := make([]uint32, len(n.prefixKeyCols))
	for i, col := range n.prefixKeyCols {
		prefixKeyColumnOrdinals[i] = uint32(plan.PlanToStreamColMap[col])
	}
	suffixKeyColumnOrdinals := make([]uint32, len(n.suffixKeyCols))
	for i, col := range n.suffixKeyCols {
		suffixKeyColumnOrdinals[i] = uint32(plan.PlanToStreamColMap[col])
	}
	keyAndSuffixCols := n.table.IndexFetchSpecKeyAndSuffixColumns(n.index)
	prefixKeyCols := keyAndSuffixCols[n.index.NumKeyColumns()-1:]
	suffixKeyCols := keyAndSuffixCols[n.index.NumKeyColumns():]
	spec := &execinfrapb.VectorMutationSearchSpec{
		PrefixKeyColumnOrdinals:  prefixKeyColumnOrdinals,
		PrefixKeyColumns:         prefixKeyCols,
		QueryVectorColumnOrdinal: uint32(plan.PlanToStreamColMap[n.queryVectorCol]),
		SuffixKeyColumnOrdinals:  suffixKeyColumnOrdinals,
		SuffixKeyColumns:         suffixKeyCols,
		IsIndexPut:               n.isIndexPut,
	}
	// VectorMutationSearch operators materialize partition and quantized-vec
	// columns rather than fetching from the table, so leave fetchCols empty.
	var fetchCols []descpb.ColumnID
	if err := rowenc.InitIndexFetchSpec(
		&spec.FetchSpec,
		planCtx.ExtendedEvalCtx.Codec,
		n.table,
		n.index,
		fetchCols,
	); err != nil {
		return nil, err
	}

	// The vector mutation search can be conducted for each row independently, so
	// it's fine to instantiate one instance for each stream.
	pSpec := execinfrapb.ProcessorCoreUnion{VectorMutationSearch: spec}
	plan.AddNoGroupingStage(pSpec, execinfrapb.PostProcessSpec{}, outputTypes, execinfrapb.Ordering{})
	return plan, nil
}

func (dsp *DistSQLPlanner) createPlanForOrdinality(
	ctx context.Context, planCtx *PlanningCtx, n *ordinalityNode,
) (*PhysicalPlan, error) {
	plan, err := dsp.createPhysPlanForPlanNode(ctx, planCtx, n.input)
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
	plan.AddSingleGroupStage(ctx, dsp.gatewaySQLInstanceID, ordinalitySpec, execinfrapb.PostProcessSpec{}, outputTypes)

	return plan, nil
}

func createProjectSetSpec(
	ctx context.Context, planCtx *PlanningCtx, n *projectSetPlanningInfo, indexVarMap []int,
) (*execinfrapb.ProjectSetSpec, error) {
	spec := execinfrapb.ProjectSetSpec{
		Exprs:                 make([]execinfrapb.Expression, len(n.exprs)),
		GeneratedColumns:      make([]*types.T, len(n.columns)-n.numColsInSource),
		GeneratedColumnLabels: make([]string, len(n.columns)-n.numColsInSource),
		NumColsPerGen:         make([]uint32, len(n.exprs)),
	}
	var ef physicalplan.ExprFactory
	ef.Init(ctx, planCtx, indexVarMap)
	// TODO(mgartner): Consider using ef.IndexedVarsHint here.
	for i, expr := range n.exprs {
		var err error
		spec.Exprs[i], err = ef.Make(expr)
		if err != nil {
			return nil, err
		}
	}
	for i, col := range n.columns[n.numColsInSource:] {
		spec.GeneratedColumns[i] = col.Typ
		spec.GeneratedColumnLabels[i] = col.Name
	}
	for i, n := range n.numColsPerGen {
		spec.NumColsPerGen[i] = uint32(n)
	}
	return &spec, nil
}

func (dsp *DistSQLPlanner) createPlanForProjectSet(
	ctx context.Context, planCtx *PlanningCtx, n *projectSetNode,
) (*PhysicalPlan, error) {
	plan, err := dsp.createPhysPlanForPlanNode(ctx, planCtx, n.input)
	if err != nil {
		return nil, err
	}
	err = dsp.addProjectSet(ctx, plan, planCtx, &n.projectSetPlanningInfo)
	return plan, err
}

// addProjectSet adds a grouping stage consisting of a single
// projectSetProcessor that is planned on the gateway.
func (dsp *DistSQLPlanner) addProjectSet(
	ctx context.Context, plan *PhysicalPlan, planCtx *PlanningCtx, info *projectSetPlanningInfo,
) error {
	numResults := len(plan.GetResultTypes())

	// Create the project set processor spec.
	projectSetSpec, err := createProjectSetSpec(ctx, planCtx, info, plan.PlanToStreamColMap)
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
	plan.AddSingleGroupStage(ctx, dsp.gatewaySQLInstanceID, spec, execinfrapb.PostProcessSpec{}, outputTypes)

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
//
//   - Query: ( VALUES (1), (2), (2) ) UNION ( VALUES (2), (3) )
//     Plan:
//     VALUES        VALUES
//     |             |
//     -------------
//     |
//     DISTINCT
//
//   - Query: ( VALUES (1), (2), (2) ) INTERSECT ALL ( VALUES (2), (3) )
//     Plan:
//     VALUES        VALUES
//     |             |
//     -------------
//     |
//     JOIN
//
//   - Query: ( VALUES (1), (2), (2) ) EXCEPT ( VALUES (2), (3) )
//     Plan:
//     VALUES        VALUES
//     |             |
//     DISTINCT       DISTINCT
//     |             |
//     -------------
//     |
//     JOIN
func (dsp *DistSQLPlanner) createPlanForSetOp(
	ctx context.Context, planCtx *PlanningCtx, n *unionNode,
) (*PhysicalPlan, error) {
	leftLogicalPlan := n.left
	leftPlan, err := dsp.createPhysPlanForPlanNode(ctx, planCtx, n.left)
	if err != nil {
		return nil, err
	}
	rightLogicalPlan := n.right
	rightPlan, err := dsp.createPhysPlanForPlanNode(ctx, planCtx, n.right)
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
			p.AddSingleGroupStage(ctx, dsp.gatewaySQLInstanceID, distinctSpec, execinfrapb.PostProcessSpec{}, resultTypes)
		} else {
			var serialStreamErrorSpec physicalplan.SerialStreamErrorSpec
			if n.enforceHomeRegion {
				// When inputIdx of the SerialUnorderedSynchronizer is incremented to 1,
				// error out.
				serialStreamErrorSpec.SerialInputIdxExclusiveUpperBound = 1
				serialStreamErrorSpec.ExceedsInputIdxExclusiveUpperBoundError =
					pgerror.Newf(pgcode.QueryHasNoHomeRegion,
						"Query has no home region. Try using a lower LIMIT value or running the query from a different region. %s",
						sqlerrors.EnforceHomeRegionFurtherInfo,
					)
			}
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
				p.EnsureSingleStreamPerNode(ctx, true /* forceSerialization */, execinfrapb.PostProcessSpec{}, serialStreamErrorSpec)
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
					ctx,
					true, /* forceSerialization */
					execinfrapb.PostProcessSpec{Limit: n.hardLimit},
					serialStreamErrorSpec,
				)
			}

			// UNION ALL is special: it doesn't have any required downstream
			// processor, so its two inputs might have different post-processing
			// which would violate an assumption later down the line. Check for this
			// condition and add a no-op stage if it exists.
			if err := p.CheckLastStagePost(); err != nil {
				p.EnsureSingleStreamOnGateway(ctx)
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
				ctx, nodes, core, post, eqCols, eqCols,
				leftPlan.GetResultTypes(), rightPlan.GetResultTypes(),
				leftPlan.MergeOrdering, rightPlan.MergeOrdering,
				leftRouters, rightRouters, resultTypes,
			)
		} else {
			p.AddDistinctSetOpStage(
				ctx, nodes, core, distinctSpecs[:], post, eqCols,
				leftPlan.GetResultTypes(), rightPlan.GetResultTypes(),
				leftPlan.MergeOrdering, rightPlan.MergeOrdering,
				leftRouters, rightRouters, resultTypes,
			)
		}

		p.SetMergeOrdering(mergeOrdering)
	}

	return p, nil
}

// createPlanForWindow creates a physical plan for computing window functions
// that have the same PARTITION BY and ORDER BY clauses.
func (dsp *DistSQLPlanner) createPlanForWindow(
	ctx context.Context, planCtx *PlanningCtx, n *windowNode,
) (*PhysicalPlan, error) {
	plan, err := dsp.createPhysPlanForPlanNode(ctx, planCtx, n.input)
	if err != nil {
		return nil, err
	}

	if len(n.funcs) == 0 {
		// If we don't have any window functions to compute, then all input
		// columns are simply passed-through, so we don't need to plan the
		// windower. This shouldn't really happen since the optimizer should
		// eliminate such a window node, but if some of the optimizer's rules
		// are disabled (in tests), it could happen.
		return plan, nil
	}

	partitionIdxs := make([]uint32, len(n.funcs[0].partitionIdxs))
	for i := range partitionIdxs {
		partitionIdxs[i] = uint32(n.funcs[0].partitionIdxs[i])
	}

	// Check that all window functions have the same PARTITION BY and ORDER BY
	// clauses. We can assume that because the optbuilder ensures that all
	// window functions in the windowNode have the same PARTITION BY and ORDER
	// BY clauses.
	for _, f := range n.funcs[1:] {
		if !n.funcs[0].samePartition(f) {
			return nil, errors.AssertionFailedf(
				"PARTITION BY clauses of window functions handled by the same "+
					"windowNode are different: %v, %v", n.funcs[0].partitionIdxs, f.partitionIdxs,
			)
		}
		if !n.funcs[0].columnOrdering.Equal(f.columnOrdering) {
			return nil, errors.AssertionFailedf(
				"ORDER BY clauses of window functions handled by the same "+
					"windowNode are different: %v, %v", n.funcs[0].columnOrdering, f.columnOrdering,
			)
		}
	}

	// All window functions in the windowNode will be computed using the single
	// stage of windowers (because they have the same PARTITION BY and ORDER BY
	// clauses). All input columns are being passed through, and windower will
	// append output columns for each window function processed at the current
	// stage.
	windowerSpec := execinfrapb.WindowerSpec{
		PartitionBy: partitionIdxs,
		WindowFns:   make([]execinfrapb.WindowerSpec_WindowFn, len(n.funcs)),
	}

	newResultTypes := make([]*types.T, len(plan.GetResultTypes())+len(n.funcs))
	copy(newResultTypes, plan.GetResultTypes())
	for windowFnSpecIdx, windowFn := range n.funcs {
		windowFnSpec, outputType, err := createWindowFnSpec(ctx, planCtx, plan, windowFn)
		if err != nil {
			return nil, err
		}
		newResultTypes[windowFn.outputColIdx] = outputType
		windowerSpec.WindowFns[windowFnSpecIdx] = windowFnSpec
	}

	// Get all sqlInstanceIDs from the previous stage.
	sqlInstanceIDs := getSQLInstanceIDsOfRouters(plan.ResultRouters, plan.Processors)
	if len(partitionIdxs) == 0 || len(sqlInstanceIDs) == 1 {
		// No PARTITION BY or we have a single node. Use a single windower. If
		// the previous stage was all on a single node, put the windower there.
		// Otherwise, bring the results back on this node.
		sqlInstanceID := dsp.gatewaySQLInstanceID
		if len(sqlInstanceIDs) == 1 {
			sqlInstanceID = sqlInstanceIDs[0]
		}
		plan.AddSingleGroupStage(
			ctx,
			sqlInstanceID,
			execinfrapb.ProcessorCoreUnion{Windower: &windowerSpec},
			execinfrapb.PostProcessSpec{},
			newResultTypes,
		)
	} else {
		// Set up the output routers from the previous stage. We use hash
		// routers with hashing on the columns from PARTITION BY clause of
		// window functions we're processing in the current stage.
		for _, resultProc := range plan.ResultRouters {
			plan.Processors[resultProc].Spec.Output[0] = execinfrapb.OutputRouterSpec{
				Type:        execinfrapb.OutputRouterSpec_BY_HASH,
				HashColumns: partitionIdxs,
			}
		}
		// We have multiple streams, so we definitely have a processor planned
		// on a remote node.
		stageID := plan.NewStage(true /* containsRemoteProcessor */, false /* allowPartialDistribution */)

		// We put a windower on each node and we connect it with all hash
		// routers from the previous stage in a such way that each node has its
		// designated SourceRouterSlot - namely, position in which a node
		// appears in nodes.
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

	// We definitely added new columns, so we need to update PlanToStreamColMap.
	plan.PlanToStreamColMap = identityMap(plan.PlanToStreamColMap, len(plan.GetResultTypes()))

	// windowers do not guarantee maintaining the order at the moment, so we
	// reset MergeOrdering. There shouldn't be an ordering here, but we reset it
	// defensively (see #35179).
	plan.SetMergeOrdering(execinfrapb.Ordering{})

	return plan, nil
}

// createPlanForExport creates a physical plan for EXPORT.
// We add a new stage of CSV/Parquet Writer processors to the input plan.
func (dsp *DistSQLPlanner) createPlanForExport(
	ctx context.Context, planCtx *PlanningCtx, n *exportNode,
) (*PhysicalPlan, error) {
	plan, err := dsp.createPhysPlanForPlanNode(ctx, planCtx, n.input)
	if err != nil {
		return nil, err
	}

	if err = logAndSanitizeExportDestination(ctx, n.destination); err != nil {
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

	plan.AddNoGroupingStage(
		core, execinfrapb.PostProcessSpec{}, colinfo.ExportColumnTypes, execinfrapb.Ordering{},
	)

	// The CSVWriter produces the same columns as the EXPORT statement.
	plan.PlanToStreamColMap = identityMap(plan.PlanToStreamColMap, len(colinfo.ExportColumns))
	return plan, nil
}

func logAndSanitizeExportDestination(ctx context.Context, dest string) error {
	clean, err := cloud.SanitizeExternalStorageURI(dest, nil)
	if err != nil {
		return err
	}
	log.Ops.Infof(ctx, "export planning to connect to destination %v", redact.Safe(clean))
	return nil
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
	ctx context.Context, plan *planComponents, c *localScanParallelizationChecker,
) (prohibitParallelization, hasScanNodeToParallelize bool) {
	if plan.main.planNode == nil || len(plan.cascades) != 0 ||
		len(plan.checkPlans) != 0 || len(plan.triggers) != 0 {
		// We either used the experimental DistSQL spec factory or have
		// cascades/checks; both of these conditions - for now - prohibit
		// the scan parallelization.
		return true, false
	}
	*c = localScanParallelizationChecker{}
	o := planObserver{enterNode: c.enterNode}
	_ = walkPlan(ctx, plan.main.planNode, o)
	for _, s := range plan.subqueryPlans {
		_ = walkPlan(ctx, s.plan.planNode, o)
	}
	return c.prohibitParallelization, c.hasScanNodeToParallelize
}

type localScanParallelizationChecker struct {
	prohibitParallelization  bool
	hasScanNodeToParallelize bool
}

func (c *localScanParallelizationChecker) enterNode(
	ctx context.Context, _ string, plan planNode,
) (bool, error) {
	if c.prohibitParallelization {
		return false, nil
	}
	switch n := plan.(type) {
	case *distinctNode:
		return true, nil
	case *explainPlanNode:
		// walkPlan doesn't recurse into explainPlanNode, so we have to manually
		// walk over the wrapped plan.
		plan := n.plan.WrappedPlan.(*planComponents)
		prohibit, has := checkScanParallelizationIfLocal(ctx, plan, c)
		c.prohibitParallelization = c.prohibitParallelization || prohibit
		c.hasScanNodeToParallelize = c.hasScanNodeToParallelize || has
		return false, nil
	case *explainVecNode:
		return true, nil
	case *filterNode:
		// Some filter expressions might be handled by falling back to the
		// wrapped processors, so we choose to be safe.
		c.prohibitParallelization = true
		return false, nil
	case *groupNode:
		for _, f := range n.funcs {
			c.prohibitParallelization = f.hasFilter()
		}
		return true, nil
	case *indexJoinNode:
		return true, nil
	case *joinNode:
		c.prohibitParallelization = n.pred.onCond != nil
		return true, nil
	case *limitNode:
		return true, nil
	case *ordinalityNode:
		return true, nil
	case *renderNode:
		// Only support projections since render expressions might be handled
		// via a wrapped row-by-row processor.
		for _, e := range n.render {
			if _, isIVar := e.(*tree.IndexedVar); !isIVar {
				c.prohibitParallelization = true
			}
		}
		return true, nil
	case *scanNode:
		if len(n.reqOrdering) == 0 && n.parallelize {
			c.hasScanNodeToParallelize = true
		}
		return true, nil
	case *sortNode:
		return true, nil
	case *unionNode:
		return true, nil
	case *valuesNode:
		return true, nil
	default:
		c.prohibitParallelization = true
		return false, nil
	}
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
	return dsp.NewPlanningCtxWithOracle(
		ctx, evalCtx, planner, txn, distributionType, physicalplan.DefaultReplicaChooser, roachpb.Locality{},
	)
}

// NewPlanningCtxWithOracle is a variant of NewPlanningCtx that allows passing a
// replica choice oracle as well.
func (dsp *DistSQLPlanner) NewPlanningCtxWithOracle(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	planner *planner,
	txn *kv.Txn,
	distributionType DistributionType,
	oracle replicaoracle.Oracle,
	localityFiler roachpb.Locality,
) *PlanningCtx {
	distribute := distributionType == FullDistribution
	// Note: infra.Release is not added to the planCtx's onFlowCleanup
	// functions below. It is instead called directly in the function
	// returned by getCleanupFunc.
	infra := physicalplan.NewPhysicalInfrastructure(uuid.MakeV4(), dsp.gatewaySQLInstanceID)
	planCtx := &PlanningCtx{
		ExtendedEvalCtx: evalCtx,
		localityFilter:  localityFiler,
		infra:           infra,
		isLocal:         !distribute,
		planner:         planner,
	}
	if !distribute {
		if planner == nil ||
			evalCtx.SessionData().Internal ||
			planner.curPlan.flags.IsSet(planFlagContainsMutation) ||
			planner.curPlan.flags.IsSet(planFlagContainsLocking) {
			// Don't parallelize the scans if we have a local plan if
			// - we don't have a planner which is the case when we are not on
			// the main query path;
			// - we're in the internal executor context - it's unlikely that any
			// of the internal queries will benefit from this parallelization,
			// and returning early in this function allows us to avoid the race
			// on dsp.spanResolver in fakedist logic test configs without adding
			// any synchronization (see #116039);
			// - the plan contains a mutation operation - we currently don't
			// support any parallelism when mutations are present;
			// - the plan uses locking (see #94290).
			return planCtx
		}
		prohibitParallelization, hasScanNodeToParallelize := checkScanParallelizationIfLocal(
			ctx, &planner.curPlan.planComponents, &planner.parallelizationChecker,
		)
		if prohibitParallelization || !hasScanNodeToParallelize {
			return planCtx
		}
		// We might decide to parallelize scans, and although the plan is local,
		// we still need to instantiate a full planning context.
		planCtx.parallelizeScansIfLocal = true
	}
	planCtx.spanIter = dsp.spanResolver.NewSpanResolverIterator(txn, oracle)
	planCtx.nodeStatuses = make(map[base.SQLInstanceID]NodeStatus)
	planCtx.nodeStatuses[dsp.gatewaySQLInstanceID] = NodeOK
	planCtx.spanPartitionState = &spanPartitionState{
		partitionSpans: make(map[base.SQLInstanceID]int),
	}
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
func FinalizePlan(ctx context.Context, planCtx *PlanningCtx, plan *PhysicalPlan) {
	finalizePlanWithRowCount(ctx, planCtx, plan, -1 /* rowCount */)
}

// finalizePlanWithRowCount adds a final "result" stage and a final projection
// if necessary as well as populates the endpoints of the plan.
// - rowCount is the estimated number of rows that the plan outputs. Use a
// negative number if the stats were not available to make an estimate.
func finalizePlanWithRowCount(
	ctx context.Context, planCtx *PlanningCtx, plan *PhysicalPlan, rowCount int64,
) {
	maybeMoveSingleFlowToGateway(planCtx, plan, rowCount)

	// Add a final "result" stage if necessary.
	plan.EnsureSingleStreamOnGateway(ctx)

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

	// Set up the endpoints for plan.Streams.
	plan.PopulateEndpoints()

	// Set up the endpoint for the final result.
	finalOut := &plan.Processors[plan.ResultRouters[0]].Spec.Output[0]
	finalOut.Streams = append(finalOut.Streams, execinfrapb.StreamEndpointSpec{
		Type: execinfrapb.StreamEndpointSpec_SYNC_RESPONSE,
	})

	for i, p := range plan.Processors {
		// Assign processor IDs.
		plan.Processors[i].Spec.ProcessorID = int32(i)
		// Double check that our reliance on ProcessorID == index is good.
		if _, ok := plan.LocalVectorSources[int32(i)]; ok {
			// Ensure processor is a values spec.
			if p.Spec.Core.Values == nil {
				panic(errors.AssertionFailedf("expected processor to be Values"))
			}
		}
		// Prevent the type schema corruption as found in #130402.
		//
		// Namely, during the vectorized operator planning we often use the type
		// slice from the input spec to create the type schema of an operator.
		// However, it is possible that the same type slice is shared by
		// multiple stages of processors. If it just so happens that there is
		// free capacity in the slice, and we append to it when planning
		// operators for both stages, we might corrupt the type schema captured
		// by the operators for the earlier stage. In order to prevent such type
		// schema corruption we cap the slice to force creation of a fresh copy
		// on the first append.
		//
		// We can't do this capping later (during the vectorized planning)
		// because the physical plan is immutable once finalized.
		for j := range p.Spec.Input {
			inputSpec := &p.Spec.Input[j]
			inputSpec.ColumnTypes = inputSpec.ColumnTypes[:len(inputSpec.ColumnTypes):len(inputSpec.ColumnTypes)]
		}
	}
}

// TODO(cucaroach): this doesn't work, get it working as part of effort to make
// distsql inserts handle general inserts.
func (dsp *DistSQLPlanner) createPlanForRowCount(
	ctx context.Context, planCtx *PlanningCtx, n *rowCountNode,
) (*PhysicalPlan, error) {
	plan, err := dsp.createPhysPlanForPlanNode(ctx, planCtx, n.source)
	plan.PlanToStreamColMap = identityMap(nil, 1)
	// fn := newAggregateFuncHolder(
	// 	execinfrapb.AggregatorSpec_Func_name[int32(execinfrapb.AggregatorSpec_COUNT_ROWS)],
	// 	[]int{0},
	// 	nil,   /* arguments */
	// 	false, /* isDistinct */
	// )
	// gn := groupNode{
	// 	columns:   []colinfo.ResultColumn{{Name: "rowCount", Typ: types.Int}},
	// 	plan:      n,
	// 	groupCols: []int{0},
	// 	isScalar:  true,
	// 	funcs:     []*aggregateFuncHolder{fn},
	// }
	// // This errors:  no builtin aggregate for COUNT_ROWS on [int]
	// if err := dsp.addAggregators(ctx, planCtx, plan, &gn); err != nil {
	// 	return nil, err
	// }
	return plan, err
}

func (dsp *DistSQLPlanner) createPlanForInsert(
	ctx context.Context, planCtx *PlanningCtx, n *insertNode,
) (*PhysicalPlan, error) {
	plan, err := dsp.createPhysPlanForPlanNode(ctx, planCtx, n.input)
	if err != nil {
		return nil, err
	}
	insColIDs := make([]descpb.ColumnID, len(n.run.insertCols))
	for i, c := range n.run.insertCols {
		insColIDs[i] = c.GetID()
	}
	var buff bytes.Buffer
	if !n.run.checkOrds.Empty() {
		if err := n.run.checkOrds.Encode(&buff); err != nil {
			return nil, err
		}
	}
	insertSpec := execinfrapb.InsertSpec{
		Table:      *n.run.ti.tableDesc().TableDesc(),
		ColumnIDs:  insColIDs,
		CheckOrds:  buff.Bytes(),
		AutoCommit: n.run.ti.autoCommit == autoCommitEnabled,
	}
	var typs []*types.T
	if len(n.columns) > 0 {
		return nil, errors.AssertionFailedf("distsql insert doesn't support RETURNING")
	} else {
		typs = []*types.T{types.Int}
	}
	plan.AddNoGroupingStage(
		execinfrapb.ProcessorCoreUnion{Insert: &insertSpec},
		execinfrapb.PostProcessSpec{},
		typs,
		execinfrapb.Ordering{})
	return plan, nil
}

func (dsp *DistSQLPlanner) NodeDescStore() kvclient.NodeDescStore {
	return dsp.nodeDescs
}
