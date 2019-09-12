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

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
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
	NodeDesc         roachpb.NodeDescriptor
	Settings         *cluster.Settings
	Gossip           *gossip.Gossip
	RPCContext       *rpc.Context
	LeaseHolderCache *kv.LeaseHolderCache
}

// Oracle is used to choose the lease holder for ranges. This
// interface was extracted so we can experiment with different choosing
// policies.
// Note that choices that start out random can act as self-fulfilling prophecies
// - if there's no active lease, the node that will be asked to execute part of
// the query (the chosen node) will acquire a new lease.
type Oracle interface {
	// ChoosePreferredReplica returns a choice for one range. Implementors are
	// free to use the queryState param, which has info about the number of	ranges
	// already handled by each node for the current SQL query. The state is not
	// updated with the result of this method; the caller is in charge of  that.
	//
	// A RangeUnavailableError can be returned if there's no information in gossip
	// about any of the nodes that might be tried.
	ChoosePreferredReplica(
		context.Context, roachpb.RangeDescriptor, QueryState,
	) (kv.ReplicaInfo, error)
}

// OracleFactory creates an oracle for a Txn.
type OracleFactory interface {
	Oracle(*client.Txn) Oracle
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
	AssignedRanges map[roachpb.RangeID]kv.ReplicaInfo
}

// MakeQueryState creates an initialized QueryState.
func MakeQueryState() QueryState {
	return QueryState{
		RangesPerNode:  make(map[roachpb.NodeID]int),
		AssignedRanges: make(map[roachpb.RangeID]kv.ReplicaInfo),
	}
}

// randomOracle is a Oracle that chooses the lease holder randomly
// among the replicas in a range descriptor.
type randomOracle struct {
	gossip *gossip.Gossip
}

var _ OracleFactory = &randomOracle{}

func newRandomOracleFactory(cfg Config) OracleFactory {
	return &randomOracle{gossip: cfg.Gossip}
}

func (o *randomOracle) Oracle(_ *client.Txn) Oracle {
	return o
}

func (o *randomOracle) ChoosePreferredReplica(
	ctx context.Context, desc roachpb.RangeDescriptor, _ QueryState,
) (kv.ReplicaInfo, error) {
	replicas, err := replicaSliceOrErr(desc, o.gossip)
	if err != nil {
		return kv.ReplicaInfo{}, err
	}
	return replicas[rand.Intn(len(replicas))], nil
}

type closestOracle struct {
	gossip      *gossip.Gossip
	latencyFunc kv.LatencyFunc
	// nodeDesc is the descriptor of the current node. It will be used to give
	// preference to the current node and others "close" to it.
	nodeDesc roachpb.NodeDescriptor
}

func newClosestOracleFactory(cfg Config) OracleFactory {
	return &closestOracle{
		latencyFunc: latencyFunc(cfg.RPCContext),
		gossip:      cfg.Gossip,
		nodeDesc:    cfg.NodeDesc,
	}
}

func (o *closestOracle) Oracle(_ *client.Txn) Oracle {
	return o
}

func (o *closestOracle) ChoosePreferredReplica(
	ctx context.Context, desc roachpb.RangeDescriptor, queryState QueryState,
) (kv.ReplicaInfo, error) {
	replicas, err := replicaSliceOrErr(desc, o.gossip)
	if err != nil {
		return kv.ReplicaInfo{}, err
	}
	replicas.OptimizeReplicaOrder(&o.nodeDesc, o.latencyFunc)
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
	leaseHolderCache                 *kv.LeaseHolderCache
	maxPreferredRangesPerLeaseHolder int
	gossip                           *gossip.Gossip
	latencyFunc                      kv.LatencyFunc
	// nodeDesc is the descriptor of the current node. It will be used to give
	// preference to the current node and others "close" to it.
	nodeDesc roachpb.NodeDescriptor
}

func newBinPackingOracleFactory(cfg Config) OracleFactory {
	return &binPackingOracle{
		maxPreferredRangesPerLeaseHolder: maxPreferredRangesPerLeaseHolder,
		gossip:                           cfg.Gossip,
		nodeDesc:                         cfg.NodeDesc,
		leaseHolderCache:                 cfg.LeaseHolderCache,
		latencyFunc:                      latencyFunc(cfg.RPCContext),
	}
}

var _ OracleFactory = &binPackingOracle{}

func (o *binPackingOracle) Oracle(_ *client.Txn) Oracle {
	return o
}

func (o *binPackingOracle) ChoosePreferredReplica(
	ctx context.Context, desc roachpb.RangeDescriptor, queryState QueryState,
) (kv.ReplicaInfo, error) {
	// Attempt to find a cached lease holder and use it if found.
	// If an error occurs, ignore it and proceed to choose a replica below.
	if storeID, ok := o.leaseHolderCache.Lookup(ctx, desc.RangeID); ok {
		var repl kv.ReplicaInfo
		repl.ReplicaDescriptor = roachpb.ReplicaDescriptor{StoreID: storeID}
		// Fill in the node descriptor.
		nodeID, err := o.gossip.GetNodeIDForStoreID(storeID)
		if err != nil {
			log.VEventf(ctx, 2, "failed to lookup store %d: %s", storeID, err)
		} else if nd, err := o.gossip.GetNodeDescriptor(nodeID); err != nil {
			log.VEventf(ctx, 2, "failed to resolve node %d: %s", nodeID, err)
		} else {
			repl.ReplicaDescriptor.NodeID = nodeID
			repl.NodeDesc = nd
			return repl, nil
		}
	}

	// If we've assigned the range before, return that assignment.
	if repl, ok := queryState.AssignedRanges[desc.RangeID]; ok {
		return repl, nil
	}

	replicas, err := replicaSliceOrErr(desc, o.gossip)
	if err != nil {
		return kv.ReplicaInfo{}, err
	}
	replicas.OptimizeReplicaOrder(&o.nodeDesc, o.latencyFunc)

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

// replicaSliceOrErr returns a ReplicaSlice for the given range descriptor.
// ReplicaSlices are restricted to replicas on nodes for which a NodeDescriptor
// is available in gossip. If no nodes are available, a RangeUnavailableError is
// returned.
func replicaSliceOrErr(desc roachpb.RangeDescriptor, gsp *gossip.Gossip) (kv.ReplicaSlice, error) {
	// Learner replicas won't serve reads/writes, so send only to the `Voters`
	// replicas. This is just an optimization to save a network hop, everything
	// would still work if we had `All` here.
	voterReplicas := desc.Replicas().Voters()
	replicas := kv.NewReplicaSlice(gsp, voterReplicas)
	if len(replicas) == 0 {
		// We couldn't get node descriptors for any replicas.
		var nodeIDs []roachpb.NodeID
		for _, r := range voterReplicas {
			nodeIDs = append(nodeIDs, r.NodeID)
		}
		return kv.ReplicaSlice{}, sqlbase.NewRangeUnavailableError(
			desc.RangeID, errors.Errorf("node info not available in gossip"), nodeIDs...)
	}
	return replicas, nil
}

// latencyFunc returns a kv.LatencyFunc for use with
// Replicas.OptimizeReplicaOrder.
func latencyFunc(rpcCtx *rpc.Context) kv.LatencyFunc {
	if rpcCtx != nil {
		return rpcCtx.RemoteClocks.Latency
	}
	return nil
}
