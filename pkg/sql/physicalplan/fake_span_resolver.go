// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package physicalplan

import (
	"bytes"
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
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

// fakeRange indicates that a range between startKey and endKey is owned by a
// certain node.
type fakeRange struct {
	startKey roachpb.Key
	endKey   roachpb.Key
	replica  *roachpb.NodeDescriptor
}

type fakeSpanResolverIterator struct {
	fsr *fakeSpanResolver
	// the fake span resolver needs to perform scans as part of Seek(); these
	// scans are performed in the context of this txn - the same one using the
	// results of the resolver - so that using the resolver doesn't introduce
	// conflicts.
	txn *kv.Txn
	err error

	// ranges are ordered by the key; the start key of the first one is the
	// beginning of the current range and the end key of the last one is the end
	// of the queried span.
	ranges []fakeRange
}

// NewSpanResolverIterator is part of the SpanResolver interface.
func (fsr *fakeSpanResolver) NewSpanResolverIterator(txn *kv.Txn) SpanResolverIterator {
	return &fakeSpanResolverIterator{fsr: fsr, txn: txn}
}

// Seek is part of the SpanResolverIterator interface. Each Seek call generates
// a random distribution of the given span.
func (fit *fakeSpanResolverIterator) Seek(
	ctx context.Context, span roachpb.Span, scanDir kvcoord.ScanDirection,
) {
	// Set aside the last range from the previous seek.
	var prevRange fakeRange
	if fit.ranges != nil {
		prevRange = fit.ranges[len(fit.ranges)-1]
	}

	// Scan the range and keep a list of all potential split keys.
	// TODO(asubiotto): this scan can have undesired side effects. For example,
	// it can change when contention occurs and swallows tracing payloads, leading
	// to unexpected test outcomes as observed in:
	//
	// https://github.com/cockroachdb/cockroach/pull/61438
	// This should use an inconsistent span outside of the txn instead.
	kvs, err := fit.txn.Scan(ctx, span.Key, span.EndKey, 0)
	if err != nil {
		log.Errorf(ctx, "error in fake span resolver scan: %s", err)
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

	splits := make([]roachpb.Key, 0, numSplits+2)
	splits = append(splits, span.Key)
	for i := range splitKeys {
		if _, ok := chosen[i]; ok {
			splits = append(splits, splitKeys[i])
		}
	}
	splits = append(splits, span.EndKey)

	if scanDir == kvcoord.Descending {
		// Reverse the order of the splits.
		for i := 0; i < len(splits)/2; i++ {
			j := len(splits) - i - 1
			splits[i], splits[j] = splits[j], splits[i]
		}
	}

	// Build ranges corresponding to the fake splits and assign them random
	// replicas.
	fit.ranges = make([]fakeRange, len(splits)-1)
	for i := range fit.ranges {
		fit.ranges[i] = fakeRange{
			startKey: splits[i],
			endKey:   splits[i+1],
			replica:  fit.fsr.nodes[rand.Intn(len(fit.fsr.nodes))],
		}
	}

	// Check for the case where the last range of the previous Seek() describes
	// the same row as this seek. In this case we'll assign the same replica so we
	// don't "split" column families of the same row across different replicas.
	if prevRange.endKey != nil {
		prefix, err := keys.EnsureSafeSplitKey(span.Key)
		// EnsureSafeSplitKey returns an error for keys which do not specify a
		// column family. In this case we don't need to worry about splitting the
		// row.
		if err == nil && len(prevRange.endKey) >= len(prefix) &&
			bytes.Equal(prefix, prevRange.endKey[:len(prefix)]) {
			fit.ranges[0].replica = prevRange.replica
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
	return len(fit.ranges) > 1
}

// Next is part of the SpanResolverIterator interface.
func (fit *fakeSpanResolverIterator) Next(_ context.Context) {
	if len(fit.ranges) <= 1 {
		panic("Next called with no more ranges")
	}
	fit.ranges = fit.ranges[1:]
}

// Desc is part of the SpanResolverIterator interface.
func (fit *fakeSpanResolverIterator) Desc() roachpb.RangeDescriptor {
	return roachpb.RangeDescriptor{
		StartKey: roachpb.RKey(fit.ranges[0].startKey),
		EndKey:   roachpb.RKey(fit.ranges[0].endKey),
	}
}

// ReplicaInfo is part of the SpanResolverIterator interface.
func (fit *fakeSpanResolverIterator) ReplicaInfo(
	_ context.Context,
) (roachpb.ReplicaDescriptor, error) {
	n := fit.ranges[0].replica
	return roachpb.ReplicaDescriptor{NodeID: n.NodeID}, nil
}
