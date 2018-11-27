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

package distsqlplan

import (
	"context"
	"math"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// SpanResolver resolves key spans to their respective ranges and lease holders.
// Used for planning physical execution of distributed SQL queries.
//
// Sample usage for resolving a bunch of spans:
//
// func resolveSpans(
//   ctx context.Context,
//   it *distsql.SpanResolverIterator,
//   spans ...spanWithDir,
// ) ([][]kv.ReplicaInfo, error) {
//   lr := distsql.NewSpanResolver(
//     distSender, gossip, nodeDescriptor,
//     distsql.BinPackingLeaseHolderChoice)
//   it := lr.NewSpanResolverIterator(nil)
//   res := make([][]kv.ReplicaInfo, 0)
//   for _, span := range spans {
//     repls := make([]kv.ReplicaInfo, 0)
//     for it.Seek(ctx, span.Span, span.dir); ; it.Next(ctx) {
//       if !it.Valid() {
//         return nil, it.Error()
//       }
//       repl, err := it.ReplicaInfo(ctx)
//       if err != nil {
//         return nil, err
//       }
//       repls = append(repls, repl)
//       if !it.NeedAnother() {
//         break
//       }
//     }
//     res = append(res, repls)
//   }
//   return res, nil
// }
//
//
type SpanResolver interface {
	// NewSpanResolverIterator creates a new SpanResolverIterator.
	// The txn is only used by the "fake" implementation (used for testing).
	NewSpanResolverIterator(txn *client.Txn) SpanResolverIterator
}

// SpanResolverIterator is used to iterate over the ranges composing a key span.
type SpanResolverIterator interface {
	// Seek positions the iterator on the start of a span (span.Key or
	// span.EndKey, depending on ScanDir). Note that span.EndKey is exclusive,
	// regardless of scanDir.
	//
	// After calling this, ReplicaInfo() will return information about the range
	// containing the start key of the span (or the end key, if the direction is
	// Descending).
	//
	// NeedAnother() will return true until the iterator is positioned on or after
	// the end of the span.  Possible errors encountered should be checked for
	// with Valid().
	//
	// Seek can be called repeatedly on the same iterator. To make optimal uses of
	// caches, Seek()s should be performed on spans sorted according to the
	// scanDir (if Descending, then the span with the highest keys should be
	// Seek()ed first).
	//
	// scanDir changes the direction in which Next() will advance the iterator.
	Seek(ctx context.Context, span roachpb.Span, scanDir kv.ScanDirection)

	// NeedAnother returns true if the current range is not the last for the span
	// that was last Seek()ed.
	NeedAnother() bool

	// Next advances the iterator to the next range. The next range contains the
	// last range's end key (but it does not necessarily start there, because of
	// asynchronous range splits and caching effects).
	// Possible errors encountered should be checked for with Valid().
	Next(ctx context.Context)

	// Valid returns false if an error was encountered by the last Seek() or Next().
	Valid() bool

	// Error returns any error encountered by the last Seek() or Next().
	Error() error

	// Desc returns the current RangeDescriptor.
	Desc() roachpb.RangeDescriptor

	// ReplicaInfo returns information about the replica that has been picked for
	// the current range.
	// A RangeUnavailableError is returned if there's no information in gossip
	// about any of the replicas.
	ReplicaInfo(ctx context.Context) (kv.ReplicaInfo, error)
}

// When choosing lease holders, we try to choose the same node for all the
// ranges applicable, until we hit this limit. The rationale is that maybe a
// bunch of those ranges don't have an active lease, so our choice is going to
// be self-fulfilling. If so, we want to collocate the lease holders. But above
// some limit, we prefer to take the parallelism and distribute to multiple
// nodes. The actual number used is based on nothing.
const maxPreferredRangesPerLeaseHolder = 10

// spanResolver implements SpanResolver.
type spanResolver struct {
	gossip     *gossip.Gossip
	distSender *kv.DistSender
	oracle     leaseHolderOracle

	// nodeDesc is the descriptor of the current node. It might be used to give
	// preference to the current node and others "close" to it when choosing lease
	// holders.
	nodeDesc roachpb.NodeDescriptor
}

var _ SpanResolver = &spanResolver{}

// LeaseHolderChoosingPolicy enumerates the implementors of leaseHolderOracle.
type LeaseHolderChoosingPolicy byte

const (
	// RandomLeaseHolderChoice chooses lease holders randomly.
	RandomLeaseHolderChoice LeaseHolderChoosingPolicy = iota
	// BinPackingLeaseHolderChoice bin-packs the choices.
	BinPackingLeaseHolderChoice
)

// NewSpanResolver creates a new spanResolver.
func NewSpanResolver(
	distSender *kv.DistSender,
	gossip *gossip.Gossip,
	nodeDesc roachpb.NodeDescriptor,
	choosingPolicy LeaseHolderChoosingPolicy,
) SpanResolver {
	var oracle leaseHolderOracle
	switch choosingPolicy {
	case RandomLeaseHolderChoice:
		oracle = &randomOracle{gossip: gossip}
	case BinPackingLeaseHolderChoice:
		oracle = &binPackingOracle{
			maxPreferredRangesPerLeaseHolder: maxPreferredRangesPerLeaseHolder,
			gossip:   gossip,
			nodeDesc: nodeDesc,
		}
	}
	return &spanResolver{
		distSender: distSender,
		oracle:     oracle,
		gossip:     gossip,
		nodeDesc:   nodeDesc,
	}
}

// oracleQueryState encapsulates the history of assignments of ranges to nodes
// done by an oracle on behalf of one particular query.
type oracleQueryState struct {
	rangesPerNode  map[roachpb.NodeID]int
	assignedRanges map[roachpb.RangeID]kv.ReplicaInfo
}

func makeOracleQueryState() oracleQueryState {
	return oracleQueryState{
		rangesPerNode:  make(map[roachpb.NodeID]int),
		assignedRanges: make(map[roachpb.RangeID]kv.ReplicaInfo),
	}
}

// spanResolverIterator implements the SpanResolverIterator interface.
type spanResolverIterator struct {
	// it is a wrapper RangeIterator.
	it *kv.RangeIterator
	// gossip is used to resolve NodeIds to addresses and node attributes, used to
	// giving preference to close-by replicas.
	gossip *gossip.Gossip
	// oracle is used to choose a lease holders for ranges when one isn't present
	// in the cache.
	oracle leaseHolderOracle

	curSpan roachpb.RSpan
	// dir is the direction set by the last Seek()
	dir kv.ScanDirection
	// queryState accumulates information about the assigned lease holders.
	queryState oracleQueryState

	err error
}

var _ SpanResolverIterator = &spanResolverIterator{}

// NewSpanResolverIterator creates a new SpanResolverIterator.
func (sr *spanResolver) NewSpanResolverIterator(_ *client.Txn) SpanResolverIterator {
	return &spanResolverIterator{
		gossip:     sr.gossip,
		it:         kv.NewRangeIterator(sr.distSender),
		oracle:     sr.oracle,
		queryState: makeOracleQueryState(),
	}
}

// Valid is part of the SpanResolverIterator interface.
func (it *spanResolverIterator) Valid() bool {
	return it.err == nil && it.it.Valid()
}

// Error is part of the SpanResolverIterator interface.
func (it *spanResolverIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	// TODO(andrei): make the DistSender iterator return error, not pErr
	return it.it.Error().GoError()
}

// Seek is part of the SpanResolverIterator interface.
func (it *spanResolverIterator) Seek(
	ctx context.Context, span roachpb.Span, scanDir kv.ScanDirection,
) {
	var key, endKey roachpb.RKey
	var err error
	if key, err = keys.Addr(span.Key); err != nil {
		it.err = err
		return
	}
	if endKey, err = keys.Addr(span.EndKey); err != nil {
		it.err = err
		return
	}
	oldDir := it.dir
	it.curSpan = roachpb.RSpan{
		Key:    key,
		EndKey: endKey,
	}
	it.dir = scanDir

	var seekKey roachpb.RKey
	if scanDir == kv.Ascending {
		seekKey = it.curSpan.Key
	} else {
		seekKey = it.curSpan.EndKey
	}

	// Check if the start of the span falls within the descriptor on which we're
	// already positioned. If so, and if the direction also corresponds, there's
	// no need to change the underlying iterator's state.
	if it.dir == oldDir && it.it.Valid() {
		reverse := (it.dir == kv.Descending)
		desc := it.it.Desc()
		if (reverse && desc.ContainsKeyInverted(seekKey)) ||
			(!reverse && desc.ContainsKey(seekKey)) {
			if log.V(1) {
				log.Infof(ctx, "not seeking (key=%s); existing descriptor %s", seekKey, desc)
			}
			return
		}
	}
	if log.V(1) {
		log.Infof(ctx, "seeking (key=%s)", seekKey)
	}
	it.it.Seek(ctx, seekKey, scanDir)
}

// Next is part of the SpanResolverIterator interface.
func (it *spanResolverIterator) Next(ctx context.Context) {
	if !it.Valid() {
		panic(it.Error())
	}
	it.it.Next(ctx)
}

// NeedAnother is part of the SpanResolverIterator interface.
func (it *spanResolverIterator) NeedAnother() bool {
	return it.it.NeedAnother(it.curSpan)
}

// Desc is part of the SpanResolverIterator interface.
func (it *spanResolverIterator) Desc() roachpb.RangeDescriptor {
	return *it.it.Desc()
}

// ReplicaInfo is part of the SpanResolverIterator interface.
func (it *spanResolverIterator) ReplicaInfo(ctx context.Context) (kv.ReplicaInfo, error) {
	if !it.Valid() {
		panic(it.Error())
	}

	resolvedLH := false
	var repl kv.ReplicaInfo
	if storeID, ok := it.it.LeaseHolderStoreID(ctx); ok {
		repl.ReplicaDescriptor = roachpb.ReplicaDescriptor{StoreID: storeID}
		// Fill in the node descriptor.
		nodeID, err := it.gossip.GetNodeIDForStoreID(storeID)
		if err != nil {
			log.VEventf(ctx, 2, "failed to lookup store %d: %s", storeID, err)
		} else {
			nd, err := it.gossip.GetNodeDescriptor(nodeID)
			if err != nil {
				// Ignore the error; ask the oracle to pick another replica below.
				log.VEventf(ctx, 2, "failed to resolve node %d: %s", nodeID, err)
			} else {
				repl.ReplicaDescriptor.NodeID = nodeID
				repl.NodeDesc = nd
				resolvedLH = true
			}
		}

	}
	if !resolvedLH {
		leaseHolder, err := it.oracle.ChoosePreferredLeaseHolder(
			*it.it.Desc(), it.queryState)
		if err != nil {
			return kv.ReplicaInfo{}, err
		}
		repl = leaseHolder
	}
	it.queryState.rangesPerNode[repl.NodeID]++
	it.queryState.assignedRanges[it.it.Desc().RangeID] = repl
	return repl, nil
}

// leaseHolderOracle is used to choose the lease holder for ranges. This
// interface was extracted so we can experiment with different choosing
// policies.
// Note that choices that start out random can act as self-fulfilling prophecies
// - if there's no active lease, the node that will be asked to execute part of
// the query (the chosen node) will acquire a new lease.
type leaseHolderOracle interface {
	// ChoosePreferredLeaseHolder returns a choice for one range. Implementors are free to
	// use the queryState param, which has info about the number of
	// ranges already handled by each node for the current SQL query. The state is
	// not updated with the result of this method; the caller is in charge of
	// that.
	//
	// A RangeUnavailableError can be returned if there's no information in gossip
	// about any of the nodes that might be tried.
	ChoosePreferredLeaseHolder(
		desc roachpb.RangeDescriptor, queryState oracleQueryState,
	) (kv.ReplicaInfo, error)
}

// randomOracle is a leaseHolderOracle that chooses the lease holder randomly
// among the replicas in a range descriptor.
// TODO(andrei): consider implementing also an oracle that prefers the "closest"
// replica.
type randomOracle struct {
	gossip *gossip.Gossip
}

var _ leaseHolderOracle = &randomOracle{}

func (o *randomOracle) ChoosePreferredLeaseHolder(
	desc roachpb.RangeDescriptor, _ oracleQueryState,
) (kv.ReplicaInfo, error) {
	replicas, err := replicaSliceOrErr(desc, o.gossip)
	if err != nil {
		return kv.ReplicaInfo{}, err
	}
	return replicas[rand.Intn(len(replicas))], nil
}

// binPackingOracle coalesces choices together, so it gives preference to
// replicas on nodes that are already assumed to be lease holders for some other
// ranges that are going to be part of a single query.
// Secondarily, it gives preference to replicas that are "close" to the current
// node.
// Finally, it tries not to overload any node.
type binPackingOracle struct {
	maxPreferredRangesPerLeaseHolder int
	gossip                           *gossip.Gossip
	// nodeDesc is the descriptor of the current node. It will be used to give
	// preference to the current node and others "close" to it.
	nodeDesc roachpb.NodeDescriptor
}

var _ leaseHolderOracle = &binPackingOracle{}

func (o *binPackingOracle) ChoosePreferredLeaseHolder(
	desc roachpb.RangeDescriptor, queryState oracleQueryState,
) (kv.ReplicaInfo, error) {
	// If we've assigned the range before, return that assignment.
	if repl, ok := queryState.assignedRanges[desc.RangeID]; ok {
		return repl, nil
	}

	replicas, err := replicaSliceOrErr(desc, o.gossip)
	if err != nil {
		return kv.ReplicaInfo{}, err
	}

	replicas.OptimizeReplicaOrder(&o.nodeDesc, nil /* TODO(andrei): plumb rpc context and remote clocks for latency */)

	// Look for a replica that has been assigned some ranges, but it's not yet full.
	minLoad := int(math.MaxInt32)
	var leastLoadedIdx int
	for i, repl := range replicas {
		assignedRanges := queryState.rangesPerNode[repl.NodeID]
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
	replicas := kv.NewReplicaSlice(gsp, &desc)
	if len(replicas) == 0 {
		// We couldn't get node descriptors for any replicas.
		var nodeIDs []roachpb.NodeID
		for _, r := range desc.Replicas {
			nodeIDs = append(nodeIDs, r.NodeID)
		}
		return kv.ReplicaSlice{}, sqlbase.NewRangeUnavailableError(
			desc.RangeID, errors.Errorf("node info not available in gossip"), nodeIDs...)
	}
	return replicas, nil
}
