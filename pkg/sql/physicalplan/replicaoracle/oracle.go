// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package replicaoracle provides functionality for physicalplan to choose a
// replica for a range.
package replicaoracle

import (
	"context"
	"math"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

// Policy determines how an Oracle should select a replica.
type Policy byte

var (
	// RandomChoice chooses lease replicas randomly.
	RandomChoice = RegisterPolicy(newRandomOracleFactory)
	// BinPackingChoice bin-packs the choices.
	BinPackingChoice = RegisterPolicy(newBinPackingOracleFactory)
	// ClosestChoice chooses the node closest to the current node.
	ClosestChoice = RegisterPolicy(newClosestOracleFactory)
)

// Config is used to construct an OracleFactory.
type Config struct {
	NodeDescs     kvcoord.NodeDescStore
	HealthChecker nodedialer.HealthChecker
	NodeDesc      roachpb.NodeDescriptor // current node
	Settings      *cluster.Settings
	RPCContext    *rpc.Context
}

// Oracle is used to choose the lease holder for ranges. This
// interface was extracted so we can experiment with different choosing
// policies.
// Note that choices that start out random can act as self-fulfilling prophecies
// - if there's no active lease, the node that will be asked to execute part of
// the query (the chosen node) will acquire a new lease.
type Oracle interface {
	// ChoosePreferredReplica returns a choice for one range. Implementors are
	// free to use the QueryState param, which has info about the number of	ranges
	// already handled by each node for the current SQL query. The state is not
	// updated with the result of this method; the caller is in charge of that.
	//
	// When the range's leaseholder is known, leaseHolder is passed in; leaseHolder
	// is nil otherwise. Implementors are free to use it, or ignore it if they
	// don't care about the leaseholder (e.g. when we're planning for follower
	// reads).
	//
	// A RangeUnavailableError can be returned if there's no information in gossip
	// about any of the nodes that might be tried.
	ChoosePreferredReplica(
		ctx context.Context, rng *roachpb.RangeDescriptor, leaseholder *roachpb.ReplicaDescriptor, qState QueryState,
	) (roachpb.ReplicaDescriptor, error)
}

// OracleFactory creates an oracle for a Txn.
type OracleFactory interface {
	Oracle(*kv.Txn) Oracle
}

// OracleFactoryFunc creates an OracleFactory from a Config.
type OracleFactoryFunc func(Config) OracleFactory

// NewOracleFactory creates an oracle with the given policy.
func NewOracleFactory(policy Policy, cfg Config) OracleFactory {
	ff, ok := oracleFactoryFuncs[policy]
	if !ok {
		panic(errors.Errorf("unknown Policy %v", policy))
	}
	return ff(cfg)
}

// RegisterPolicy creates a new policy given a function which constructs an
// OracleFactory. RegisterPolicy is intended to be called only during init and
// is not safe for concurrent use.
func RegisterPolicy(f OracleFactoryFunc) Policy {
	if len(oracleFactoryFuncs) == 255 {
		panic("Can only register 255 Policy instances")
	}
	r := Policy(len(oracleFactoryFuncs))
	oracleFactoryFuncs[r] = f
	return r
}

var oracleFactoryFuncs = map[Policy]OracleFactoryFunc{}

// QueryState encapsulates the history of assignments of ranges to nodes
// done by an oracle on behalf of one particular query.
type QueryState struct {
	RangesPerNode  map[roachpb.NodeID]int
	AssignedRanges map[roachpb.RangeID]roachpb.ReplicaDescriptor
}

// MakeQueryState creates an initialized QueryState.
func MakeQueryState() QueryState {
	return QueryState{
		RangesPerNode:  make(map[roachpb.NodeID]int),
		AssignedRanges: make(map[roachpb.RangeID]roachpb.ReplicaDescriptor),
	}
}

// randomOracle is a Oracle that chooses the lease holder randomly
// among the replicas in a range descriptor.
type randomOracle struct {
	nodeDescs     kvcoord.NodeDescStore
	healthChecker nodedialer.HealthChecker
}

var _ OracleFactory = &randomOracle{}

func newRandomOracleFactory(cfg Config) OracleFactory {
	return &randomOracle{nodeDescs: cfg.NodeDescs, healthChecker: cfg.HealthChecker}
}

func (o *randomOracle) Oracle(_ *kv.Txn) Oracle {
	return o
}

func (o *randomOracle) ChoosePreferredReplica(
	ctx context.Context, desc *roachpb.RangeDescriptor, _ *roachpb.ReplicaDescriptor, _ QueryState,
) (roachpb.ReplicaDescriptor, error) {
	replicas, err := orderReplicasOrErr(
		ctx, kvcoord.DontOrderByLatency,
		nil, /* curNode - we don't want ordering so we don't care */
		o.nodeDescs,
		o.healthChecker, nil /* latencyFunc */, desc,
		roachpb.ReplicaDescriptor{} /* leaseholder - we don't want ordering so we don't care */)
	if err != nil {
		return roachpb.ReplicaDescriptor{}, err
	}
	return replicas[rand.Intn(len(replicas))], nil
}

type closestOracle struct {
	nodeDescs kvcoord.NodeDescStore
	// nodeDesc is the descriptor of the current node. It will be used to give
	// preference to the current node and others "close" to it.
	nodeDesc      roachpb.NodeDescriptor
	latencyFunc   kvcoord.LatencyFunc
	healthChecker nodedialer.HealthChecker
}

func newClosestOracleFactory(cfg Config) OracleFactory {
	return &closestOracle{
		nodeDescs:     cfg.NodeDescs,
		nodeDesc:      cfg.NodeDesc,
		latencyFunc:   latencyFunc(cfg.RPCContext),
		healthChecker: cfg.HealthChecker,
	}
}

func (o *closestOracle) Oracle(_ *kv.Txn) Oracle {
	return o
}

func (o *closestOracle) ChoosePreferredReplica(
	ctx context.Context, desc *roachpb.RangeDescriptor, _ *roachpb.ReplicaDescriptor, _ QueryState,
) (roachpb.ReplicaDescriptor, error) {
	replicas, err := orderReplicasOrErr(
		ctx, kvcoord.OrderByLatency, &o.nodeDesc, o.nodeDescs,
		o.healthChecker, o.latencyFunc, desc,
		roachpb.ReplicaDescriptor{}, /* leaseholder - this oracle ignores it */
	)
	if err != nil {
		return roachpb.ReplicaDescriptor{}, err
	}
	return replicas[0], nil
}

// maxPreferredRangesPerLeaseHolder applies to the binPackingOracle.
// When choosing lease holders, we try to choose the same node for all the
// ranges applicable, until we hit this limit. The rationale is that maybe a
// bunch of those ranges don't have an active lease, so our choice is going to
// be self-fulfilling. If so, we want to collocate the lease holders. But above
// some limit, we prefer to take the parallelism and distribute to multiple
// nodes. The actual number used is based on nothing.
const maxPreferredRangesPerLeaseHolder = 10

// binPackingOracle coalesces choices together, so it gives preference to
// replicas on nodes that are already assumed to be lease holders for some other
// ranges that are going to be part of a single query.
// Secondarily, it gives preference to replicas that are "close" to the current
// node.
// Finally, it tries not to overload any node.
type binPackingOracle struct {
	maxPreferredRangesPerLeaseHolder int
	nodeDescs                        kvcoord.NodeDescStore
	// nodeDesc is the descriptor of the current node. It will be used to give
	// preference to the current node and others "close" to it.
	nodeDesc      roachpb.NodeDescriptor
	latencyFunc   kvcoord.LatencyFunc
	healthChecker nodedialer.HealthChecker
}

func newBinPackingOracleFactory(cfg Config) OracleFactory {
	return &binPackingOracle{
		maxPreferredRangesPerLeaseHolder: maxPreferredRangesPerLeaseHolder,
		nodeDescs:                        cfg.NodeDescs,
		nodeDesc:                         cfg.NodeDesc,
		latencyFunc:                      latencyFunc(cfg.RPCContext),
		healthChecker:                    cfg.HealthChecker,
	}
}

var _ OracleFactory = &binPackingOracle{}

func (o *binPackingOracle) Oracle(_ *kv.Txn) Oracle {
	return o
}

func (o *binPackingOracle) ChoosePreferredReplica(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	leaseholder *roachpb.ReplicaDescriptor,
	queryState QueryState,
) (roachpb.ReplicaDescriptor, error) {
	// If we know the leaseholder, we choose it.
	if leaseholder != nil {
		return *leaseholder, nil
	}

	replicas, err := orderReplicasOrErr(
		ctx, kvcoord.OrderByLatency, &o.nodeDesc, o.nodeDescs,
		o.healthChecker, o.latencyFunc, desc,
		roachpb.ReplicaDescriptor{} /* leaseholder - if it were known, we'd have returned above */)
	if err != nil {
		return roachpb.ReplicaDescriptor{}, err
	}

	// Look for a replica that has been assigned some ranges, but it's not yet full.
	minLoad := int(math.MaxInt32)
	var leastLoadedIdx int
	for i, repl := range replicas {
		assignedRanges := queryState.RangesPerNode[repl.NodeID]
		if assignedRanges != 0 && assignedRanges < o.maxPreferredRangesPerLeaseHolder {
			return repl, nil
		}
		if assignedRanges < minLoad {
			leastLoadedIdx = i
			minLoad = assignedRanges
		}
	}
	// Either no replica was assigned any previous ranges, or all replicas are
	// full. Use the least-loaded one (if all the load is 0, then the closest
	// replica is returned).
	return replicas[leastLoadedIdx], nil
}

// orderReplicasOrErr orders the replicas to be tried for contacting a range. If
// no replicas are available, a RangeUnavailableError is returned.
//
// TODO(andrei): Consider returning RangeUnavailableError directly from
// kvcoord.OrderReplicas.
func orderReplicasOrErr(
	ctx context.Context,
	orderOpt kvcoord.ReorderReplicasOpt,
	curNode *roachpb.NodeDescriptor,
	nodeDescStore kvcoord.NodeDescStore,
	healthChecker nodedialer.HealthChecker,
	latencyFn kvcoord.LatencyFunc,
	desc *roachpb.RangeDescriptor,
	leaseholder roachpb.ReplicaDescriptor,
) ([]roachpb.ReplicaDescriptor, error) {
	replicas, err := kvcoord.OrderReplicas(
		ctx, orderOpt, curNode, nodeDescStore,
		healthChecker, latencyFn, desc, leaseholder)
	if err != nil {
		return nil, sqlerrors.NewRangeUnavailableError(desc.RangeID, err)
	}
	return replicas, nil
}

// latencyFunc returns a kv.LatencyFunc for use with OrderReplicas.
func latencyFunc(rpcCtx *rpc.Context) kvcoord.LatencyFunc {
	if rpcCtx != nil {
		return rpcCtx.RemoteClocks.Latency
	}
	return nil
}
