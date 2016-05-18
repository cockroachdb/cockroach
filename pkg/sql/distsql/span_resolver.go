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
// Author: Andrei Matei (andreimatei1@gmail.com)

package distsql

import (
	"math"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// When guessing lease holders, we try to guess the same node for all the ranges
// applicable, until we hit this limit. The rationale is that maybe a bunch of
// those ranges don't have an active lease, so our guess is going to be
// self-fulfilling. If so, we want to collocate the lease holders. But above
// some limit, we prefer to take the parallelism and distribute to multiple
// nodes. The actual number used is based on nothing.
const maxPreferredRangesPerLeaseHolder = 10

// SpanResolver resolves key spans to their respective ranges and lease holders.
// Used for planning physical execution of distributed SQL queries.
//
// All public methods are thread-safe.
type SpanResolver struct {
	gossip     *gossip.Gossip
	distSender *kv.DistSender
	stopper    *stop.Stopper
	oracle     leaseHolderOracle

	// nodeDesc is the descriptor of the current node. It might be used to give
	// preference to the current node and others "close" to it when guessing lease
	// holders.
	// TODO(andrei): can the descriptor change at runtime?
	nodeDesc roachpb.NodeDescriptor
}

// LeaseHolderGuessingPolicy enumerates the implementors of leaseHolderOracle.
type LeaseHolderGuessingPolicy byte

const (
	// RandomLHGuessingPolicy guesses randomly.
	RandomLHGuessingPolicy LeaseHolderGuessingPolicy = iota
	// BinPackingLHGuessingPolicy bin-packs the guesses.
	BinPackingLHGuessingPolicy
)

// ScanType determines defines how a key span should be resolved by the
// SpanResolver (which end of key span is inclusive and which isn't).
type ScanType byte

const (
	// Ascending means Key is inclusive and EndKey is exclusive.
	Ascending ScanType = iota
	// Descending means Key is exclusive and EndKey is inclusive.
	Descending
)

// NewSpanResolver creates a new SpanResolver.
func NewSpanResolver(
	distSender *kv.DistSender,
	gossip *gossip.Gossip,
	nodeDesc roachpb.NodeDescriptor,
	stopper *stop.Stopper,
	guessingPolicy LeaseHolderGuessingPolicy,
) *SpanResolver {
	var oracle leaseHolderOracle
	switch guessingPolicy {
	case RandomLHGuessingPolicy:
		oracle = &randomOracle{gossip: gossip}
	case BinPackingLHGuessingPolicy:
		oracle = &binPackingOracle{
			// This number is based on nothing.
			maxPreferredRangesPerLeaseHolder: maxPreferredRangesPerLeaseHolder,
			gossip:   gossip,
			nodeDesc: nodeDesc,
		}
	}
	return &SpanResolver{
		distSender: distSender,
		oracle:     oracle,
		gossip:     gossip,
		stopper:    stopper,
		nodeDesc:   nodeDesc,
	}
}

func (sr *SpanResolver) resolveSpan(
	ctx context.Context, span roachpb.Span, scanType ScanType, rangesPerNode map[roachpb.NodeID]uint,
) ([]kv.ReplicaInfo, error) {

	var key, endKey roachpb.RKey
	var err error
	if key, err = keys.Addr(span.Key); err != nil {
		return nil, err
	}
	if endKey, err = keys.Addr(span.EndKey); err != nil {
		return nil, err
	}
	rspan := roachpb.RSpan{
		Key:    key,
		EndKey: endKey,
	}
	var seekKey roachpb.RKey
	if scanType == Ascending {
		seekKey = rspan.Key
	} else {
		seekKey = rspan.EndKey
	}

	var leaseHolders []kv.ReplicaInfo
	ri := kv.NewRangeIterator(sr.distSender, scanType == Descending)
	for ri.Seek(ctx, seekKey); ri.Valid(); ri.Next(ctx) {
		var repl kv.ReplicaInfo
		if lh, ok := ri.LeaseHolder(ctx); ok {
			repl.ReplicaDescriptor = lh
		} else {
			leaseHolder, err := sr.oracle.GuessLeaseHolder(
				*ri.Desc(), rangesPerNode)
			if err != nil {
				return nil, err
			}
			repl = leaseHolder
		}
		// Fill in the node descriptor.
		nd, err := sr.gossip.GetNodeDescriptor(repl.NodeID)
		if err != nil {
			return nil, sqlbase.NewRangeUnavailableError(
				ri.Desc().RangeID, err, repl.NodeID)
		}
		repl.NodeDesc = nd

		leaseHolders = append(leaseHolders, repl)
		rangesPerNode[repl.NodeID]++

		if !ri.NeedAnother(rspan) {
			break
		}
	}
	return leaseHolders, nil
}

// ResolveSpans takes a list of spans and returns information about how those
// spans are distributed across kv ranges.
// For each input span it returns an array of results containing information
// about all the ranges spanned by the respective span. Each range is
// represented by a RangeInfo, which identifies the replica that is believed
// to be the current lease holder of the range. Information about connecting to
// the node owning that replica is also included.
//
// The spans need to be disjoint; they also need to be sorted so that the
// prefetching in the range descriptor cache helps us.
//
// If scanType is Ascending, the descriptors returned cover the StartKey of the
// spans but not necessarily the EndKey (so StartKey is inclusive, EndKey is
// exclusive). If it's Descending then the opposite holds. Regardless of
// scanType, the input spans still need to be sorted ascendingly.
//
// A note on ranges and lease holders: ResolveSpans uses local caches for
// figuring out ranges and lease holders. These caches might have stale
// information (or the information can also become stale by the time it's used).
// Also, in the case of lease holders, if the cache doesn't have information
// about a range, the lease holder is guessed. So users should treat these
// results as best-effort.
func (sr *SpanResolver) ResolveSpans(
	ctx context.Context, scanType ScanType, spans ...roachpb.Span,
) ([][]kv.ReplicaInfo, error) {
	leaseHolders := make([][]kv.ReplicaInfo, len(spans))
	if len(spans) == 0 {
		return nil, nil
	}

	// Keep track of how many ranges we assigned to each node; the
	// leaseHolderOracle can use this to coalesce guesses.
	rangesPerNode := make(map[roachpb.NodeID]uint)
	if scanType == Ascending {
		for i, span := range spans {
			var err error
			leaseHolders[i], err = sr.resolveSpan(ctx, span, scanType, rangesPerNode)
			if err != nil {
				return nil, err
			}
		}
	} else {
		// Iterate backwards so that prefetching helps us across spans.
		for i := len(spans) - 1; i >= 0; i-- {
			var err error
			leaseHolders[i], err = sr.resolveSpan(ctx, spans[i], scanType, rangesPerNode)
			if err != nil {
				return nil, err
			}
		}
	}
	return leaseHolders, nil
}

// leaseHolderOracle is used to guess the lease holder for ranges. This
// interface was extracted so we can experiment with different guessing
// policies.
// Note that guesses can act as self-fulfilling prophecies - if there's no
// active lease, the node that will be asked to execute part of the query (the
// guessed node) will acquire a new lease.
type leaseHolderOracle interface {
	// GuessLeaseHolder returns a guess for one range. Implementors are free to
	// use the rangesPerLeaseHolder param, which has info about the number of
	// ranges already handled by each node for the current SQL query. The map is
	// not updated with the result of this method; the caller is in charge of
	// that.
	//
	// A RangeUnavailableError can be returned if there's no information in gossip
	// about any of the nodes that might be tried.
	GuessLeaseHolder(
		desc roachpb.RangeDescriptor, rangesPerLeaseHolder map[roachpb.NodeID]uint,
	) (kv.ReplicaInfo, error)
}

// randomOracle is a leaseHolderOracle that guesses the lease holder randomly
// among the replicas in a range descriptor.
// TODO(andrei): consider implementing also an oracle that prefers the "closest"
// replica.
type randomOracle struct {
	gossip *gossip.Gossip
}

var _ leaseHolderOracle = &randomOracle{}

func (o *randomOracle) GuessLeaseHolder(
	desc roachpb.RangeDescriptor, _ map[roachpb.NodeID]uint,
) (kv.ReplicaInfo, error) {
	replicas, err := replicaSliceOrErr(desc, o.gossip)
	if err != nil {
		return kv.ReplicaInfo{}, err
	}
	return replicas[rand.Intn(len(replicas))], nil
}

// binPackingOracle coalesces guesses together, so it gives preference to
// replicas on nodes that are already assumed to be lease holders for some other
// ranges that are going to be part of a single query.
// Secondarily, it gives preference to replicas that are "close" to the current
// node.
// Finally, it tries not to overload any node.
type binPackingOracle struct {
	maxPreferredRangesPerLeaseHolder uint
	gossip                           *gossip.Gossip
	// nodeDesc is the descriptor of the current node. It will be used to give
	// preference to the current node and others "close" to it.
	nodeDesc roachpb.NodeDescriptor
}

var _ leaseHolderOracle = &binPackingOracle{}

func (o *binPackingOracle) GuessLeaseHolder(
	desc roachpb.RangeDescriptor, rangesPerLeaseHolder map[roachpb.NodeID]uint,
) (kv.ReplicaInfo, error) {
	replicas, err := replicaSliceOrErr(desc, o.gossip)
	if err != nil {
		return kv.ReplicaInfo{}, err
	}
	replicas.OptimizeReplicaOrder(&o.nodeDesc)

	// Look for a replica that has been assigned some ranges, but it's not yet full.
	minLoad := uint(math.MaxUint32)
	var leastLoadedIdx int
	for i, repl := range replicas {
		assignedRanges := rangesPerLeaseHolder[repl.NodeID]
		if assignedRanges != 0 && assignedRanges < o.maxPreferredRangesPerLeaseHolder {
			return repl, nil
		}
		if assignedRanges < minLoad {
			leastLoadedIdx = i
			minLoad = assignedRanges
		}
	}
	// Either no replica was assigned any previous ranges, or all replicas are
	// full. Use the least-loaded one.
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
