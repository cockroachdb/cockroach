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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsqlplan

import (
	"math/rand"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type fakeSpanResolver struct {
	nodes []*roachpb.NodeDescriptor
	db    *client.DB
}

var _ SpanResolver = &fakeSpanResolver{}

// NewFakeSpanResolver creates a SpanResolver which splits spans and distributes
// them to nodes randomly.
func NewFakeSpanResolver(nodes []*roachpb.NodeDescriptor, db *client.DB) SpanResolver {
	return &fakeSpanResolver{
		nodes: nodes,
		db:    db,
	}
}

// fakeSplit indicates that a range starting at key is owned by a certain node.
type fakeSplit struct {
	key      roachpb.Key
	nodeDesc *roachpb.NodeDescriptor
}

type fakeSpanResolverIterator struct {
	fsr *fakeSpanResolver
	err error
	// splits are ordered by the key; the first one is the beginning of the
	// current range and the last one is the end of the queried span.
	splits []fakeSplit
}

const avgRangesPerNode = 5

// NewSpanResolverIterator is part of the SpanResolver interface.
func (fsr *fakeSpanResolver) NewSpanResolverIterator() SpanResolverIterator {
	return &fakeSpanResolverIterator{
		fsr: fsr,
	}
}

// Seek is part of the SpanResolverIterator interface.
func (fit *fakeSpanResolverIterator) Seek(
	ctx context.Context, span roachpb.Span, scanDir kv.ScanDirection,
) {
	if scanDir != kv.Ascending {
		panic("descending not implemented")
	}

	// Scan the range and keep a list of all potential split keys.
	var splitKeys []roachpb.Key

	if err := fit.fsr.db.Txn(ctx, func(txn *client.Txn) error {
		b := &client.Batch{}
		b.Scan(span.Key, span.EndKey)

		if err := txn.Run(b); err != nil {
			return err
		}
		if err := txn.CommitOrCleanup(); err != nil {
			return err
		}
		// Populate splitKeys with potential split keys; all keys are strictly
		// between span.Key and span.EndKey.
		splitKeys = nil
		lastKey := span.Key
		for _, kv := range b.Results[0].Rows {
			// Extract the key for the row.
			splitKey, err := keys.EnsureSafeSplitKey(kv.Key)
			if err != nil {
				return err
			}
			if !splitKey.Equal(lastKey) {
				splitKeys = append(splitKeys, splitKey)
				lastKey = splitKey
			}
		}
		return nil
	}); err != nil {
		fit.err = err
		return
	}

	// Generate fake splits.
	maxSplits := 2 * len(fit.fsr.nodes) * avgRangesPerNode
	if maxSplits > len(splitKeys) {
		maxSplits = len(splitKeys)
	}
	numSplits := rand.Intn(maxSplits)

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
