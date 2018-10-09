// Copyright 2017 The Cockroach Authors.
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
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

const avgRangesPerNode = 5

// fakeSpanResolver is a SpanResovler which splits spans and distributes them to
// nodes randomly. Each Seek() call generates a random distribution with
// expected avgRangesPerNode ranges for each node.
type fakeSpanResolver struct {
	nodes []*roachpb.NodeDescriptor
}

var _ SpanResolver = &fakeSpanResolver{}

// NewFakeSpanResolver creates a fake span resolver.
func NewFakeSpanResolver(nodes []*roachpb.NodeDescriptor) SpanResolver {
	return &fakeSpanResolver{
		nodes: nodes,
	}
}

// fakeSplit indicates that a range starting at key is owned by a certain node.
type fakeSplit struct {
	key      roachpb.Key
	nodeDesc *roachpb.NodeDescriptor
}

type fakeSpanResolverIterator struct {
	fsr *fakeSpanResolver
	// the fake span resolver needs to perform scans as part of Seek(); these
	// scans are performed in the context of this txn - the same one using the
	// results of the resolver - so that using the resolver doesn't introduce
	// conflicts.
	txn *client.Txn
	err error
	// splits are ordered by the key; the first one is the beginning of the
	// current range and the last one is the end of the queried span.
	splits []fakeSplit
}

// NewSpanResolverIterator is part of the SpanResolver interface.
func (fsr *fakeSpanResolver) NewSpanResolverIterator(txn *client.Txn) SpanResolverIterator {
	return &fakeSpanResolverIterator{fsr: fsr, txn: txn}
}

// Seek is part of the SpanResolverIterator interface. Each Seek call generates
// a random distribution of the given span.
func (fit *fakeSpanResolverIterator) Seek(
	ctx context.Context, span roachpb.Span, scanDir kv.ScanDirection,
) {
	// Scan the range and keep a list of all potential split keys.
	kvs, err := fit.txn.Scan(ctx, span.Key, span.EndKey, 0)
	if err != nil {
		log.Errorf(ctx, "Error in fake span resolver scan: %s", err)
		fit.err = err
		return
	}

	// Populate splitKeys with potential split keys; all keys are strictly
	// between span.Key and span.EndKey.
	var splitKeys []roachpb.Key
	lastKey := span.Key
	for _, kv := range kvs {
		// Extract the key for the row.
		splitKey, err := keys.EnsureSafeSplitKey(kv.Key)
		if err != nil {
			fit.err = err
			return
		}
		if !splitKey.Equal(lastKey) && span.ContainsKey(splitKey) {
			splitKeys = append(splitKeys, splitKey)
			lastKey = splitKey
		}
	}

	// Generate fake splits. The number of splits is selected randomly between 0
	// and a maximum value; we want to generate
	//   x = #nodes * avgRangesPerNode
	// splits on average, so the maximum number is 2x:
	//   Expected[ rand(2x+1) ] = (0 + 1 + 2 + .. + 2x) / (2x + 1) = x.
	maxSplits := 2 * len(fit.fsr.nodes) * avgRangesPerNode
	if maxSplits > len(splitKeys) {
		maxSplits = len(splitKeys)
	}
	numSplits := rand.Intn(maxSplits + 1)

	// Use Robert Floyd's algorithm to generate numSplits distinct integers
	// between 0 and len(splitKeys), just because it's so cool!
	chosen := make(map[int]struct{})
	for j := len(splitKeys) - numSplits; j < len(splitKeys); j++ {
		t := rand.Intn(j + 1)
		if _, alreadyChosen := chosen[t]; !alreadyChosen {
			// Insert T.
			chosen[t] = struct{}{}
		} else {
			// Insert J.
			chosen[j] = struct{}{}
		}
	}

	fit.splits = make([]fakeSplit, 0, numSplits+2)
	fit.splits = append(fit.splits, fakeSplit{key: span.Key})
	for i := range splitKeys {
		if _, ok := chosen[i]; ok {
			fit.splits = append(fit.splits, fakeSplit{key: splitKeys[i]})
		}
	}
	fit.splits = append(fit.splits, fakeSplit{key: span.EndKey})

	// Assign nodes randomly.
	for i := range fit.splits {
		fit.splits[i].nodeDesc = fit.fsr.nodes[rand.Intn(len(fit.fsr.nodes))]
	}

	if scanDir == kv.Descending {
		// Reverse the order of the splits.
		for i := 0; i < len(fit.splits)/2; i++ {
			j := len(fit.splits) - i - 1
			fit.splits[i], fit.splits[j] = fit.splits[j], fit.splits[i]
		}
	}
}

// Valid is part of the SpanResolverIterator interface.
func (fit *fakeSpanResolverIterator) Valid() bool {
	return fit.err == nil
}

// Error is part of the SpanResolverIterator interface.
func (fit *fakeSpanResolverIterator) Error() error {
	return fit.err
}

// NeedAnother is part of the SpanResolverIterator interface.
func (fit *fakeSpanResolverIterator) NeedAnother() bool {
	return len(fit.splits) > 2
}

// Next is part of the SpanResolverIterator interface.
func (fit *fakeSpanResolverIterator) Next(_ context.Context) {
	if len(fit.splits) <= 2 {
		panic("Next called with no more ranges")
	}
	fit.splits = fit.splits[1:]
}

// Desc is part of the SpanResolverIterator interface.
func (fit *fakeSpanResolverIterator) Desc() roachpb.RangeDescriptor {
	return roachpb.RangeDescriptor{
		StartKey: roachpb.RKey(fit.splits[0].key),
		EndKey:   roachpb.RKey(fit.splits[1].key),
	}
}

// ReplicaInfo is part of the SpanResolverIterator interface.
func (fit *fakeSpanResolverIterator) ReplicaInfo(_ context.Context) (kv.ReplicaInfo, error) {
	n := fit.splits[0].nodeDesc
	return kv.ReplicaInfo{
		ReplicaDescriptor: roachpb.ReplicaDescriptor{NodeID: n.NodeID},
		NodeDesc:          n,
	}, nil
}
