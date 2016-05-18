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
// Sample usage for resolving a bunch of spans:
//
// func resolveSpans(
//   ctx context.Context,
//   it *distsql.SpanResolverIterator,
//   spans ...spanWithDir,
// ) ([][]kv.ReplicaInfo, error) {
//   lr := distsql.NewSpanResolver(
//		 distSender, gossip, nodeDescriptor,
//     distsql.BinPackingLHGuessingPolicy)
//   it := lr.NewSpanResolverIterator()
//   res := make([][]kv.ReplicaInfo, 0)
//   for _, span := range spans {
//     repls := make([]kv.ReplicaInfo, 0)
//     for it.Seek(ctx, span.Span, span.dir); it.Valid(); it.Next(ctx) {
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
type SpanResolver struct {
	gossip     *gossip.Gossip
	distSender *kv.DistSender
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

// NewSpanResolver creates a new SpanResolver.
func NewSpanResolver(
	distSender *kv.DistSender,
	gossip *gossip.Gossip,
	nodeDesc roachpb.NodeDescriptor,
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
		nodeDesc:   nodeDesc,
	}
}

type oracleQueryState struct {
	rangesPerNode map[roachpb.NodeID]int
}

func makeOracleQueryState() oracleQueryState {
	return oracleQueryState{
		rangesPerNode: make(map[roachpb.NodeID]int),
	}
}

// SpanResolverIterator iterates over the ranges composing a key span.
type SpanResolverIterator struct {
	it     *kv.RangeIterator
	gossip *gossip.Gossip
	oracle leaseHolderOracle

	curSpan roachpb.RSpan
	// dir is the direction set by the last Seek()
	dir        kv.ScanDirection
	queryState oracleQueryState

	err error
}

// NewSpanResolverIterator creates a new SpanResolverIterator.
func (sr *SpanResolver) NewSpanResolverIterator() *SpanResolverIterator {
	it := SpanResolverIterator{
		gossip:     sr.gossip,
		it:         kv.NewRangeIterator(sr.distSender),
		oracle:     sr.oracle,
		queryState: makeOracleQueryState(),
	}
	return &it
}

// Valid returns false if an error was encoutered by the last Seek() or Next().
func (it *SpanResolverIterator) Valid() bool {
	return it.err == nil && it.it.Valid()
}

func (it *SpanResolverIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	// !!! make the DistSender iterator return error, not pErr
	return it.it.Error().GoError()
}

// Seek positions the iterator on the start of a span.
// After calling this, ReplicaInfo() will return information about the first
// range of the span. NeedAnother() will return true until the iterator is
// positioned on the end of the span.
// Possible errors encountered should be checked for with Valid().
//
// Seek can be called repeatedly on the same iterator. To make optimal uses of
// caches, Seek()s should be performed in the on spans sorted according to the
// scanDir (if Descending, then the span with the highest keys should be
// Seek()ed first).
//
// scanDir influences the inclusive or exclusive character of the span's Key and
// EndKey; see ScanDirection comments.
func (it *SpanResolverIterator) Seek(
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
	oldSpan := it.curSpan
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
	if it.dir == oldDir && oldSpan.ContainsKey(seekKey) && it.it.Valid() {
		return
	}
	it.it.Seek(ctx, seekKey, scanDir)
}

// Next advances the iterator to the next range.
// Possible errors encountered should be checked for with Valid().
func (it *SpanResolverIterator) Next(ctx context.Context) {
	if !it.Valid() {
		panic(it.Error())
	}
	it.it.Next(ctx)
}

// NeedAnother returns true if the current range is not the last for the span
// that was last Seek()ed.
func (it *SpanResolverIterator) NeedAnother() bool {
	return it.it.NeedAnother(it.curSpan)
}

func (it *SpanResolverIterator) ReplicaInfo(ctx context.Context) (kv.ReplicaInfo, error) {
	if !it.Valid() {
		panic(it.Error())
	}

	var repl kv.ReplicaInfo
	if lh, ok := it.it.LeaseHolder(ctx); ok {
		repl.ReplicaDescriptor = lh
	} else {
		leaseHolder, err := it.oracle.GuessLeaseHolder(
			*it.it.Desc(), it.queryState)
		if err != nil {
			return kv.ReplicaInfo{}, err
		}
		repl = leaseHolder
	}
	// Fill in the node descriptor.
	nd, err := it.gossip.GetNodeDescriptor(repl.NodeID)
	if err != nil {
		return kv.ReplicaInfo{}, sqlbase.NewRangeUnavailableError(
			it.it.Desc().RangeID, err, repl.NodeID)
	}
	repl.NodeDesc = nd

	it.queryState.rangesPerNode[repl.NodeID]++

	return repl, nil
}

// leaseHolderOracle is used to guess the lease holder for ranges. This
// interface was extracted so we can experiment with different guessing
// policies.
// Note that guesses can act as self-fulfilling prophecies - if there's no
// active lease, the node that will be asked to execute part of the query (the
// guessed node) will acquire a new lease.
type leaseHolderOracle interface {
	// GuessLeaseHolder returns a guess for one range. Implementors are free to
	// use the queryState param, which has info about the number of
	// ranges already handled by each node for the current SQL query. The state is
	// not updated with the result of this method; the caller is in charge of
	// that.
	//
	// A RangeUnavailableError can be returned if there's no information in gossip
	// about any of the nodes that might be tried.
	GuessLeaseHolder(
		desc roachpb.RangeDescriptor, queryState oracleQueryState,
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
	desc roachpb.RangeDescriptor, _ oracleQueryState,
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
	maxPreferredRangesPerLeaseHolder int
	gossip                           *gossip.Gossip
	// nodeDesc is the descriptor of the current node. It will be used to give
	// preference to the current node and others "close" to it.
	nodeDesc roachpb.NodeDescriptor
}

var _ leaseHolderOracle = &binPackingOracle{}

func (o *binPackingOracle) GuessLeaseHolder(
	desc roachpb.RangeDescriptor, queryState oracleQueryState,
) (kv.ReplicaInfo, error) {
	replicas, err := replicaSliceOrErr(desc, o.gossip)
	if err != nil {
		return kv.ReplicaInfo{}, err
	}
	replicas.OptimizeReplicaOrder(&o.nodeDesc)

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
